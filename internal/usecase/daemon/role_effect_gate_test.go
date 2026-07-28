package daemon

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/pkg/renderer"
)

func releaseTestGate(t *testing.T, ch chan struct{}) func() {
	t.Helper()
	var once sync.Once
	release := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return release
}

type blockedRenderReplacementTransport struct {
	sendEntered chan struct{}
	closed      chan struct{}
	sendOnce    sync.Once
	closeOnce   sync.Once
	locksFree   chan bool
	d           *Daemon
	sess        *session
}

func newBlockedRenderReplacementTransport(d *Daemon, sess *session) *blockedRenderReplacementTransport {
	return &blockedRenderReplacementTransport{
		sendEntered: make(chan struct{}),
		closed:      make(chan struct{}),
		locksFree:   make(chan bool, 1),
		d:           d,
		sess:        sess,
	}
}

func (t *blockedRenderReplacementTransport) Send(ports.Frame) error {
	t.sendOnce.Do(func() { close(t.sendEntered) })
	<-t.closed
	return errors.New("transport closed")
}

func (*blockedRenderReplacementTransport) Recv() (ports.Frame, error) {
	return ports.Frame{}, io.EOF
}

func (t *blockedRenderReplacementTransport) Close() error {
	locksFree := t.d.mu.TryLock()
	if locksFree {
		t.d.mu.Unlock()
	}
	if locksFree {
		locksFree = t.d.notices.routingMu.TryLock()
		if locksFree {
			t.d.notices.routingMu.Unlock()
		}
	}
	if locksFree {
		locksFree = t.sess.mu.TryLock()
		if locksFree {
			t.sess.mu.Unlock()
		}
	}
	if locksFree {
		if rc := t.sess.renderCoordinator(); rc != nil {
			locksFree = rc.mu.TryLock()
			if locksFree {
				rc.mu.Unlock()
			}
		}
	}
	t.closeOnce.Do(func() {
		t.locksFree <- locksFree
		close(t.closed)
	})
	return nil
}

type teardownBlockingTransport struct {
	sendEntered chan struct{}
	closed      chan struct{}
	sendOnce    sync.Once
	closeOnce   sync.Once
}

func newTeardownBlockingTransport() *teardownBlockingTransport {
	return &teardownBlockingTransport{sendEntered: make(chan struct{}), closed: make(chan struct{})}
}

func (t *teardownBlockingTransport) Send(ports.Frame) error {
	t.sendOnce.Do(func() { close(t.sendEntered) })
	<-t.closed
	return errors.New("transport closed")
}

func (*teardownBlockingTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }

func (t *teardownBlockingTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func (t *teardownBlockingTransport) Closed() bool {
	select {
	case <-t.closed:
		return true
	default:
		return false
	}
}

func startBlockedRoleSend(t *testing.T, token attachmentRoleToken) <-chan struct{} {
	t.Helper()
	ticket, admitted := token.ac.beginRoleEffect(token)
	require.True(t, admitted)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ticket.End()
		_ = token.ac.sendExpectedTransportForRole(token.transport, framePong(), ticket)
	}()
	return done
}

func requireRoleGateRetired(t *testing.T, ac *attachedClient) {
	t.Helper()
	ac.roleEffects.mu.Lock()
	defer ac.roleEffects.mu.Unlock()
	require.Equal(t, roleEffectsStable, ac.roleEffects.phase)
	require.Zero(t, ac.roleEffects.inFlight)
	require.Empty(t, ac.roleEffects.transportEffects)
}

func roleGatePublicationSnapshot(ac *attachedClient) (roleEffectPhase, roleCapability) {
	ac.roleEffects.mu.Lock()
	defer ac.roleEffects.mu.Unlock()
	return ac.roleEffects.phase, ac.roleEffects.capability
}

func TestKillSessionAcquisitionTimeoutDoesNotPublishPartialInvalidation(t *testing.T) {
	d, sess, first, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	firstTransport := &closeTrackingTransport{}
	first.replaceTransport(firstTransport)

	blockedTransport := &closeTrackingTransport{}
	blocked := &attachedClient{tr: blockedTransport, output: newOutputStateStream(), size: first.size}
	blocked.initOverlays()
	blocked.setSession(sess)
	laterTransport := &closeTrackingTransport{}
	later := &attachedClient{tr: laterTransport, output: newOutputStateStream(), size: first.size}
	later.initOverlays()
	later.setSession(sess)
	sess.mu.Lock()
	sess.addSnatchedLocked(blocked)
	sess.addSnatchedLocked(later)
	sess.mu.Unlock()

	rc := d.attachCoordinator(sess, nil, first, true)
	firstToken := sess.attachmentToken(first, firstTransport)
	firstToken.lease = rc.attachmentLease(first)
	first.publishRoleCapability(firstToken)
	blockedToken := sess.attachmentToken(blocked, blockedTransport)
	blocked.publishRoleCapability(blockedToken)
	laterToken := sess.attachmentToken(later, laterTransport)
	later.publishRoleCapability(laterToken)

	// Fix the canonical order so teardown acquires first, waits behind the gate
	// owned by this test, and never reaches the still-stable later participant.
	first.roleEffects.immutableOrder()
	blocked.roleEffects.immutableOrder()
	later.roleEffects.immutableOrder()
	require.Less(t, first.roleEffects.order.Load(), blocked.roleEffects.order.Load())
	require.Less(t, blocked.roleEffects.order.Load(), later.roleEffects.order.Load())

	blockedOwner := freezeRoleEffectGates(blocked)
	blockedOwnerHeld := true
	t.Cleanup(func() {
		if blockedOwnerHeld {
			blockedOwner.unfreeze()
		}
	})

	firstPhase, firstCapability := roleGatePublicationSnapshot(first)
	blockedPhase, blockedCapability := roleGatePublicationSnapshot(blocked)
	laterPhase, laterCapability := roleGatePublicationSnapshot(later)
	require.Equal(t, roleEffectsStable, firstPhase)
	require.Equal(t, roleEffectsFrozen, blockedPhase)
	require.Equal(t, roleEffectsStable, laterPhase)
	firstGeneration := first.roleGeneration.Load()
	blockedGeneration := blocked.roleGeneration.Load()
	laterGeneration := later.roleGeneration.Load()
	firstSnapshot := first.transportSnapshot()
	blockedSnapshot := blocked.transportSnapshot()
	laterSnapshot := later.transportSnapshot()

	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clock
	frozenByKill := make(chan *attachedClient, 3)
	d.afterRoleEffectGateFrozen = func(_ string, ac *attachedClient) { frozenByKill <- ac }
	killDone := make(chan error, 1)
	go func() { killDone <- d.killSession(sess, ports.ReasonSessionKilled, false) }()

	require.Same(t, first, awaitTestValue(t, frozenByKill, "teardown did not acquire the first role gate"))
	deadline := awaitTestValue(t, clock.timers, "teardown did not arm the gate acquisition deadline")
	require.Equal(t, detachNotifyTimeout, deadline.duration)
	deadline.ch <- time.Now()
	select {
	case err := <-killDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("teardown did not abort after partial gate acquisition timed out")
	}
	require.Empty(t, frozenByKill, "teardown acquired a participant after its deadline expired")

	d.mu.Lock()
	require.Same(t, sess, d.sessions[sess.id], "partial gate acquisition removed the session registry owner")
	d.mu.Unlock()
	sess.mu.Lock()
	require.Same(t, first, sess.client)
	require.Contains(t, sess.snatched, blocked)
	require.Contains(t, sess.snatched, later)
	sess.mu.Unlock()
	for _, ac := range []*attachedClient{first, blocked, later} {
		require.Same(t, sess, ac.currentSession(), "partial gate acquisition cleared attachment ownership")
	}
	require.Equal(t, firstGeneration, first.roleGeneration.Load())
	require.Equal(t, blockedGeneration, blocked.roleGeneration.Load())
	require.Equal(t, laterGeneration, later.roleGeneration.Load())
	require.True(t, first.transportSnapshotCurrent(firstSnapshot))
	require.True(t, blocked.transportSnapshotCurrent(blockedSnapshot))
	require.True(t, later.transportSnapshotCurrent(laterSnapshot))
	require.False(t, firstTransport.Closed())
	require.False(t, blockedTransport.Closed())
	require.False(t, laterTransport.Closed())

	firstPhase, gotFirstCapability := roleGatePublicationSnapshot(first)
	blockedPhase, gotBlockedCapability := roleGatePublicationSnapshot(blocked)
	laterPhase, gotLaterCapability := roleGatePublicationSnapshot(later)
	require.Equal(t, roleEffectsStable, firstPhase, "partially acquired gate was not rolled back")
	require.Equal(t, roleEffectsFrozen, blockedPhase, "teardown released a gate owned by another transition")
	require.Equal(t, roleEffectsStable, laterPhase, "unacquired later gate was frozen or invalidated")
	require.Equal(t, firstCapability, gotFirstCapability)
	require.Equal(t, blockedCapability, gotBlockedCapability)
	require.Equal(t, laterCapability, gotLaterCapability)

	blockedOwner.unfreeze()
	blockedOwnerHeld = false
	complete := freezeRoleEffectGates(first, blocked, later)
	require.True(t, complete.acquired, "rolled-back gate set could not be acquired again")
	require.True(t, complete.drained)
	complete.unfreeze()
	for _, token := range []attachmentRoleToken{firstToken, blockedToken, laterToken} {
		ticket, admitted := token.ac.beginRoleEffect(token)
		require.True(t, admitted, "pre-publication abort changed a role capability")
		ticket.End()
		requireRoleGateRetired(t, token.ac)
	}
}

func TestKillSessionInterruptsOnlyExactParticipantBlockedSends(t *testing.T) {
	d, sess, snatched, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	snatchedTransport := newTeardownBlockingTransport()
	snatched.replaceTransport(snatchedTransport)
	d.attachCoordinator(sess, nil, snatched, true)
	snatched.publishRoleCapability(sess.attachmentToken(snatched, snatchedTransport))

	activeTransport := newTeardownBlockingTransport()
	active := &attachedClient{tr: activeTransport, output: newOutputStateStream(), size: snatched.size}
	active.initOverlays()
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: active, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: active.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	defer d.deferAttachmentTransitionCleanups(transition)

	unrelatedTransport := &closeTrackingTransport{}
	unrelatedClient := &attachedClient{tr: unrelatedTransport, output: newOutputStateStream(), size: active.size}
	unrelatedClient.initOverlays()
	unrelated := &session{id: "unrelated", name: "unrelated", ctx: sess.ctx, cancel: func() {}, client: unrelatedClient}
	unrelatedClient.setSession(unrelated)
	d.mu.Lock()
	d.sessions[unrelated.id] = unrelated
	d.mu.Unlock()
	d.attachCoordinator(unrelated, nil, unrelatedClient, true)

	snatchedToken := sess.attachmentToken(snatched, snatchedTransport)
	activeToken := sess.attachmentToken(active, activeTransport)
	snatchedDone := startBlockedRoleSend(t, snatchedToken)
	activeDone := startBlockedRoleSend(t, activeToken)
	awaitTestCompletion(t, snatchedTransport.sendEntered, "snatched send did not block")
	awaitTestCompletion(t, activeTransport.sendEntered, "active send did not block")

	killDone := make(chan error, 1)
	go func() { killDone <- d.killSession(sess, ports.ReasonSessionKilled, false) }()
	select {
	case err := <-killDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		_ = snatchedTransport.Close()
		_ = activeTransport.Close()
		t.Fatal("session kill remained pinned behind admitted participant sends")
	}
	awaitTestCompletion(t, snatchedDone, "snatched role ticket did not retire")
	awaitTestCompletion(t, activeDone, "active role ticket did not retire")
	d.waitNotifies()

	require.True(t, snatchedTransport.Closed())
	require.True(t, activeTransport.Closed())
	require.False(t, unrelatedTransport.Closed(), "unrelated session transport was interrupted")
	require.Same(t, unrelatedClient, unrelated.client)
	require.Same(t, unrelated, unrelatedClient.currentSession())
	requireRoleGateRetired(t, snatched)
	requireRoleGateRetired(t, active)
}

func TestKillSessionInterruptsCapturedSendWithoutClosingNewerIncarnation(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	stale := newTeardownBlockingTransport()
	ac.replaceTransport(stale)
	d.attachCoordinator(sess, nil, ac, true)
	ac.publishRoleCapability(sess.attachmentToken(ac, stale))

	token := sess.attachmentToken(ac, stale)
	sendDone := startBlockedRoleSend(t, token)
	awaitTestCompletion(t, stale.sendEntered, "captured incarnation send did not block")
	fresh := &closeTrackingTransport{}
	d.afterRoleEffectParticipantsSnapshotted = func(string, []*attachedClient) {
		ac.replaceTransport(fresh)
	}

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
	awaitTestCompletion(t, sendDone, "captured incarnation role ticket did not retire")
	require.True(t, stale.Closed(), "exact captured in-flight transport was not interrupted")
	require.False(t, fresh.Closed(), "newer transport incarnation was interrupted")
	require.Same(t, fresh, ac.transport())
	d.mu.Lock()
	require.Same(t, sess, d.sessions[sess.id], "transport incarnation change should abort stale teardown publication")
	d.mu.Unlock()
	sess.mu.Lock()
	require.Same(t, ac, sess.client)
	sess.mu.Unlock()
	requireRoleGateRetired(t, ac)
}

func TestShutdownInterruptsBlockedParticipantSend(t *testing.T) {
	d, sess, active, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	blocked := newTeardownBlockingTransport()
	active.replaceTransport(blocked)
	d.attachCoordinator(sess, nil, active, true)
	active.publishRoleCapability(sess.attachmentToken(active, blocked))

	token := sess.attachmentToken(active, blocked)
	sendDone := startBlockedRoleSend(t, token)
	awaitTestCompletion(t, blocked.sendEntered, "active send did not block")

	shutdownDone := make(chan bool, 1)
	go func() { shutdownDone <- d.shutdownAll(ports.ReasonServerShutdown) }()
	select {
	case incomplete := <-shutdownDone:
		require.False(t, incomplete)
	case <-time.After(500 * time.Millisecond):
		_ = blocked.Close()
		t.Fatal("shutdown remained pinned behind admitted participant send")
	}
	awaitTestCompletion(t, sendDone, "shutdown-interrupted role ticket did not retire")
	d.waitNotifies()
	require.True(t, blocked.Closed())
	requireRoleGateRetired(t, active)
}

type renderFailureTransport struct {
	mu     sync.Mutex
	closed bool
}

func (*renderFailureTransport) Send(ports.Frame) error     { return errors.New("render send failed") }
func (*renderFailureTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (t *renderFailureTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}
func (t *renderFailureTransport) Closed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func TestRenderSendFailureCleanupRejectsReplacedAndResumedIncarnation(t *testing.T) {
	d, sess, original, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	failedTransport := &renderFailureTransport{}
	original.replaceTransport(failedTransport)
	rc := d.attachCoordinator(sess, nil, original, true)
	originalToken := sess.attachmentToken(original, failedTransport)
	originalToken.lease = rc.attachmentLease(original)
	original.publishRoleCapability(originalToken)

	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	cleanupDone := make(chan struct{})
	d.beforeRoleSendErrorCleanup = func(token attachmentRoleToken) {
		require.Equal(t, originalToken.generation, token.generation)
		require.Equal(t, originalToken.transport, token.transport)
		require.Same(t, originalToken.lease, token.lease)
		close(cleanupStarted)
		<-releaseCleanup
	}
	d.afterRoleSendErrorCleanup = func() { close(cleanupDone) }

	d.paint(sess, original, true, originalToken.lease)
	awaitTestCompletion(t, cleanupStarted, "render failure cleanup did not retain the captured role token")
	cleanupTracked := waitGroupDone(&d.attachmentCleanupWg)
	select {
	case <-cleanupTracked:
		t.Fatal("paused render failure cleanup was not tracked by attachmentCleanupWg")
	default:
	}

	replacementTransport := &closeTrackingTransport{}
	replacement := &attachedClient{tr: replacementTransport, output: newOutputStateStream(), size: original.size}
	replacement.initOverlays()
	displaced, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: replacement, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: replacement.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)

	resumed, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: original, expectedRole: attachmentSnatched, targetRole: attachmentActive,
		expectedTransport: original.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	resumedGeneration := original.roleGeneration.Load()
	resumedLease := resumed.published.lease
	require.NotSame(t, originalToken.lease, resumedLease)

	close(releaseCleanup)
	awaitTestCompletion(t, cleanupDone, "render failure cleanup did not finish")
	awaitTestCompletion(t, cleanupTracked, "tracked render failure cleanup did not retire")

	sess.mu.Lock()
	require.Same(t, original, sess.client, "stale render cleanup detached the resumed incarnation")
	sess.mu.Unlock()
	require.Equal(t, resumedGeneration, original.roleGeneration.Load())
	require.True(t, resumed.published.activeCurrent())
	require.False(t, failedTransport.Closed(), "stale cleanup closed the resumed incarnation's transport")

	d.deferAttachmentTransitionCleanups(displaced)
	d.deferAttachmentTransitionCleanups(resumed)
	d.attachmentCleanupWg.Wait()
}

func TestRenderSendFailureCleanupIsDeadlineBoundedWhileRoleGateIsBusy(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	transport := &closeTrackingTransport{}
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(sess, nil, ac, true)
	token := sess.attachmentToken(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.publishRoleCapability(token)

	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clock
	frozen := freezeRoleEffectGates(ac)
	defer frozen.unfreeze()
	cleanupDone := make(chan struct{})
	d.afterRoleSendErrorCleanup = func() { close(cleanupDone) }

	launchCleanup := d.reserveRoleSendErrorCleanup(token, transport)
	launchCleanup()
	deadline := awaitTestValue(t, clock.timers, "render cleanup did not arm its deadline")
	require.Equal(t, detachNotifyTimeout, deadline.duration)
	deadline.ch <- time.Now()
	awaitTestCompletion(t, cleanupDone, "deadline did not release blocked render cleanup")
	awaitTestCompletion(t, waitGroupDone(&d.attachmentCleanupWg), "bounded render cleanup leaked its tracked goroutine")

	sess.mu.Lock()
	require.Same(t, ac, sess.client)
	sess.mu.Unlock()
	require.False(t, transport.Closed())
}

func TestRoleEffectGateReplacementInterruptsBlockedOldRenderBeforePublication(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newBlockedRenderReplacementTransport(d, sess)
	old.replaceTransport(oldTransport)
	rc := d.attachCoordinator(sess, nil, old, true)
	lease := rc.attachmentLease(old)
	token := sess.attachmentToken(old, oldTransport)
	token.lease = lease
	old.publishRoleCapability(token)

	paintDone := make(chan struct{})
	go func() {
		d.paint(sess, old, true, lease)
		close(paintDone)
	}()
	<-oldTransport.sendEntered

	newTransport := &closeTrackingTransport{}
	next := &attachedClient{tr: newTransport, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	transitionDone := make(chan attachmentTransitionResult, 1)
	transitionErr := make(chan error, 1)
	go func() {
		result, err := d.transitionAttachment(attachmentTransitionRequest{
			target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
			expectedTransport: next.transportSnapshot(), ready: true,
		})
		transitionDone <- result
		transitionErr <- err
	}()

	var result attachmentTransitionResult
	select {
	case result = <-transitionDone:
	case <-time.After(time.Second):
		t.Fatal("replacement publication remained blocked behind the old render send")
	}
	require.NoError(t, <-transitionErr)
	require.True(t, <-oldTransport.locksFree, "transport close ran while an architecture lock was held")
	select {
	case <-paintDone:
	case <-time.After(time.Second):
		t.Fatal("closing the exact old transport did not unblock its render")
	}
	require.Same(t, next, sess.client)
	require.False(t, newTransport.Closed(), "replacement transport was affected by old-link interruption")

	d.deferAttachmentTransitionCleanups(result)
	d.attachmentCleanupWg.Wait()
	require.Nil(t, old.transport(), "the unhealthy old link was not retired")
	require.Same(t, next, sess.client)
	require.False(t, newTransport.Closed())
}

func TestDisplacedBlockedRenderCleanupWaitsForStaleDetachFreeze(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newBlockedRenderReplacementTransport(d, sess)
	old.replaceTransport(oldTransport)
	rc := d.attachCoordinator(sess, nil, old, true)
	lease := rc.attachmentLease(old)
	token := sess.attachmentToken(old, oldTransport)
	token.lease = lease
	old.publishRoleCapability(token)
	old.sendMu.Lock()
	old.captureFrames = map[*pane]capturedPaneRenderState{sess.activeTab().focusedPane(): {}}
	old.sendMu.Unlock()

	staleDetachFrozen := make(chan struct{})
	releaseStaleDetach := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseStaleDetach) }) })
	d.afterDetachRoleEffectsFrozen = func() {
		close(staleDetachFrozen)
		<-releaseStaleDetach
	}
	cleanupStarted := make(chan struct{})
	d.afterDisplacedCleanupStarted = func() { close(cleanupStarted) }

	paintDone := make(chan struct{})
	go func() {
		d.paint(sess, old, true, lease)
		close(paintDone)
	}()
	awaitTestCompletion(t, oldTransport.sendEntered, "old render did not block in transport send")

	newTransport := &closeTrackingTransport{}
	next := &attachedClient{tr: newTransport, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	transitionDone := make(chan attachmentTransitionResult, 1)
	transitionErr := make(chan error, 1)
	go func() {
		result, err := d.transitionAttachment(attachmentTransitionRequest{
			target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
			expectedTransport: next.transportSnapshot(), ready: true,
		})
		transitionDone <- result
		transitionErr <- err
	}()

	var result attachmentTransitionResult
	select {
	case result = <-transitionDone:
		require.NoError(t, <-transitionErr)
	case <-time.After(time.Second):
		t.Fatal("replacement did not interrupt the blocked render")
	}
	awaitTestCompletion(t, paintDone, "interrupted render did not retire")
	awaitTestCompletion(t, staleDetachFrozen, "stale send-error detach did not freeze the role gate")

	d.deferAttachmentTransitionCleanups(result)
	awaitTestCompletion(t, cleanupStarted, "displaced cleanup did not start during stale detach freeze")
	releaseOnce.Do(func() { close(releaseStaleDetach) })
	select {
	case <-waitGroupDone(&d.attachmentCleanupWg):
	case <-time.After(time.Second):
		t.Fatal("displaced cleanup deadlocked behind stale detach")
	}

	sess.mu.Lock()
	require.Same(t, next, sess.client)
	require.NotContains(t, sess.snatched, old, "closed displaced incarnation remained registered")
	sess.mu.Unlock()
	require.Equal(t, attachmentActive, sess.attachmentRole(next))
	require.Nil(t, old.transport(), "closed displaced transport remained attached")
	require.Nil(t, old.currentSession(), "terminal displaced cleanup retained session ownership")
	old.sendMu.Lock()
	require.Empty(t, old.captureFrames, "terminal displaced cleanup retained captures")
	old.sendMu.Unlock()
	require.False(t, newTransport.Closed(), "stale cleanup affected the new active owner")
}

func TestInterruptedDisplacedCleanupRejectsNewerReclaimedIncarnation(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	staleTransport := &closeTrackingTransport{}
	old.replaceTransport(staleTransport)
	d.attachCoordinator(sess, nil, old, true)

	nextTransport := &closeTrackingTransport{}
	next := &attachedClient{tr: nextTransport, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	displaced, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: next.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	stale := displaced.displaced

	_, err = d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: old, expectedRole: attachmentSnatched, targetRole: attachmentActive,
		expectedTransport: old.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	freshTransport := &closeTrackingTransport{}
	old.replaceTransport(freshTransport)
	_, err = d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: old, expectedRole: attachmentActive, targetRole: attachmentActive,
		expectedTransport: old.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	require.NoError(t, staleTransport.Close())

	require.False(t, d.cleanupInterruptedSnatchedAttachment(stale))
	sess.mu.Lock()
	require.Same(t, old, sess.client)
	require.Contains(t, sess.snatched, next)
	sess.mu.Unlock()
	require.Equal(t, attachmentActive, sess.attachmentRole(old))
	require.Same(t, freshTransport, old.transport())
	require.False(t, freshTransport.Closed(), "stale terminal cleanup closed the newer transport")
	require.False(t, nextTransport.Closed(), "stale terminal cleanup affected the newer displaced owner")
}

func TestRoleEffectGateAdmittedEffectCompletesBeforeConflictingTransition(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)
	rc := d.attachCoordinator(sess, nil, old, true)
	token := sess.attachmentToken(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.publishRoleCapability(token)

	ticket, ok := old.beginRoleEffect(token)
	require.True(t, ok)

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	done := make(chan error, 1)
	go func() {
		_, err := d.transitionAttachment(attachmentTransitionRequest{
			target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
			expectedTransport: next.transportSnapshot(), ready: true,
		})
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("transition committed before admitted effect ended: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	ticket.End()
	require.NoError(t, <-done)
}

func TestRoleEffectGateAdmittedActiveEffectsFinishBeforeReplacement(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*Daemon, *session, *attachedClient, *transactionalResizePTY)
		frame  ports.Frame
		assert func(*testing.T, *attachedClient, *transactionalResizePTY)
	}{
		{
			name:  "PTY input",
			frame: frameInput([]byte("reserved")),
			assert: func(t *testing.T, _ *attachedClient, pty *transactionalResizePTY) {
				require.Equal(t, [][]byte{[]byte("reserved")}, pty.writes())
			},
		},
		{
			name: "mouse input",
			setup: func(_ *Daemon, sess *session, _ *attachedClient, _ *transactionalResizePTY) {
				p := sess.activeTab().focusedPane()
				p.mu.Lock()
				p.screen.Write([]byte("\x1b[?1000h\x1b[?1006h"))
				p.mu.Unlock()
			},
			frame: frameInput([]byte("\x1b[<0;1;2M")),
			assert: func(t *testing.T, _ *attachedClient, pty *transactionalResizePTY) {
				require.NotEmpty(t, pty.writes())
			},
		},
		{
			name:  "key action",
			frame: frameInput([]byte("\x1b ")),
			assert: func(t *testing.T, ac *attachedClient, _ *transactionalResizePTY) {
				require.True(t, ac.overlays.paletteActive())
			},
		},
		{
			name: "overlay mutation",
			setup: func(d *Daemon, sess *session, ac *attachedClient, _ *transactionalResizePTY) {
				d.enterPrompt(sess, ac, "Rename", "", func(string) error { return nil })
			},
			frame: frameInput([]byte("x")),
			assert: func(t *testing.T, ac *attachedClient, _ *transactionalResizePTY) {
				ac.overlays.promptMu.Lock()
				defer ac.overlays.promptMu.Unlock()
				require.Equal(t, "x", ac.overlays.prompt.Value())
			},
		},
		{
			name: "theme",
			frame: ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(ports.Theme{
				HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
				HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
			})},
			assert: func(t *testing.T, ac *attachedClient, _ *transactionalResizePTY) {
				require.True(t, ac.getClientTheme().HasFG)
			},
		},
		{
			name: "ACK",
			setup: func(_ *Daemon, _ *session, ac *attachedClient, _ *transactionalResizePTY) {
				ac.output.next = 3
			},
			frame: ports.Frame{Type: ports.MsgAck, Payload: ports.MarshalAck(ports.Ack{AckedStateNum: 1})},
			assert: func(t *testing.T, ac *attachedClient, _ *transactionalResizePTY) {
				ac.sendMu.Lock()
				defer ac.sendMu.Unlock()
				require.Equal(t, uint64(1), ac.output.acked)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pty := &transactionalResizePTY{}
			d, sess, old, _ := newManualSessionWithPTYs(t, pty)
			oldTransport := newDatagramTestTransport()
			old.replaceTransport(oldTransport)
			old.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: old}, &d.bindings)
			rc := d.attachCoordinator(sess, nil, old, true)
			token := sess.attachmentToken(old, oldTransport)
			token.lease = rc.attachmentLease(old)
			if tt.setup != nil {
				tt.setup(d, sess, old, pty)
			}

			admitted := make(chan struct{})
			release := make(chan struct{})
			d.afterRoleEffectAdmitted = func(attachmentRoleToken) {
				close(admitted)
				<-release
			}
			effectDone := make(chan struct{})
			go func() {
				d.handleActiveClientFrame(token, tt.frame)
				close(effectDone)
			}()
			<-admitted

			next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
			next.initOverlays()
			transitionDone := make(chan error, 1)
			go func() {
				_, err := d.transitionAttachment(attachmentTransitionRequest{
					target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
					expectedTransport: next.transportSnapshot(), ready: true,
				})
				transitionDone <- err
			}()
			select {
			case err := <-transitionDone:
				t.Fatalf("transition overtook admitted effect: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			<-effectDone
			require.NoError(t, <-transitionDone)
			tt.assert(t, old, pty)
		})
	}
}

func TestJumpAttentionDoesNotMutateSourceAfterInitiatorReplacement(t *testing.T) {
	d, sess, old, _, releases := newManualTabSession(t, 2)
	defer releaseAll(releases)
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(1, 0)
	sess.mu.Unlock()

	oldTransport := old.transport()
	rc := d.attachCoordinator(sess, nil, old, true)
	token := sess.attachmentToken(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.publishRoleCapability(token)

	admissionEnded := make(chan struct{})
	releaseAction := make(chan struct{})
	release := releaseTestGate(t, releaseAction)
	var admissionEndedOnce sync.Once
	d.afterActionRoleEffectEnded = func(action string) {
		if action != "jump-attention" {
			return
		}
		admissionEndedOnce.Do(func() { close(admissionEnded) })
		<-releaseAction
	}
	actionDone := make(chan struct{})
	go func() {
		d.handleActiveClientFrame(token, frameInput([]byte("\x1ba")))
		close(actionDone)
	}()
	<-admissionEnded

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: next.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	sess.mu.Lock()
	sess.active = 0
	sess.mu.Unlock()
	release()
	<-actionDone

	require.Equal(t, 0, activeTabIndex(sess), "the replaced initiator mutated source focus after losing its role")
	d.deferAttachmentTransitionCleanups(result)
}

func TestJumpAttentionAdmittedHandoffCrossesSessions(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	p3, release3 := newBlockingPTY(t)
	defer release1()
	defer release2()
	defer release3()
	d, source, ac, _ := newManualSessionWithPTYs(t, p1)
	target := &session{id: "target", name: "target", ctx: source.ctx, cancel: func() {}, tabs: []*tab{
		newTab(p2, domain.Size{Cols: 80, Rows: 23}),
		newTab(p3, domain.Size{Cols: 80, Rows: 23}),
	}}
	target.tabs[1].attention = true
	target.tabs[1].attentionAt = time.Unix(1, 0)
	d.sessions[target.id] = target

	rc := d.attachCoordinator(source, nil, ac, true)
	token := source.attachmentToken(ac, ac.transport())
	token.lease = rc.attachmentLease(ac)
	ac.publishRoleCapability(token)

	d.handleActiveClientFrame(token, frameInput([]byte("\x1ba")))

	require.Same(t, target, ac.currentSession())
	require.Equal(t, 1, activeTabIndex(target))
}

func TestJumpAttentionHandoffRevalidatesInitiatorAfterAdmissionEnds(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	p3, release3 := newBlockingPTY(t)
	defer release1()
	defer release2()
	defer release3()
	d, source, old, _ := newManualSessionWithPTYs(t, p1)
	target := &session{id: "target", name: "target", ctx: source.ctx, cancel: func() {}, tabs: []*tab{
		newTab(p2, domain.Size{Cols: 80, Rows: 23}),
		newTab(p3, domain.Size{Cols: 80, Rows: 23}),
	}}
	target.tabs[1].attention = true
	target.tabs[1].attentionAt = time.Unix(1, 0)
	d.sessions[target.id] = target

	oldTransport := old.transport()
	rc := d.attachCoordinator(source, nil, old, true)
	token := source.attachmentToken(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.publishRoleCapability(token)

	admissionEnded := make(chan struct{})
	releaseAction := make(chan struct{})
	release := releaseTestGate(t, releaseAction)
	var admissionEndedOnce sync.Once
	d.afterActionRoleEffectEnded = func(action string) {
		if action == "jump-attention" {
			admissionEndedOnce.Do(func() { close(admissionEnded) })
			<-releaseAction
		}
	}
	actionDone := make(chan struct{})
	go func() {
		d.handleActiveClientFrame(token, frameInput([]byte("\x1ba")))
		close(actionDone)
	}()
	<-admissionEnded

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target: source, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: next.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	release()
	<-actionDone

	require.Equal(t, 0, activeTabIndex(target), "stale handoff changed target focus")
	target.mu.Lock()
	targetClient := target.client
	target.mu.Unlock()
	require.Nil(t, targetClient, "stale handoff attached the replaced initiator")
	d.deferAttachmentTransitionCleanups(result)
}

func TestPickerDeleteDoesNotDeleteSourceAfterInitiatorReplacement(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, old, _ := newManualSessionWithPTYs(t, p)
	d.enterPicker(sess, old)

	oldTransport := old.transport()
	rc := d.attachCoordinator(sess, nil, old, true)
	token := sess.attachmentToken(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.publishRoleCapability(token)

	admissionEnded := make(chan struct{})
	releaseAction := make(chan struct{})
	release := releaseTestGate(t, releaseAction)
	var admissionEndedOnce sync.Once
	d.afterActionRoleEffectEnded = func(action string) {
		if action != "picker-delete" {
			return
		}
		admissionEndedOnce.Do(func() { close(admissionEnded) })
		<-releaseAction
	}
	actionDone := make(chan struct{})
	go func() {
		d.handleActiveClientFrame(token, frameInput([]byte("x")))
		close(actionDone)
	}()
	<-admissionEnded

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: next.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	release()
	<-actionDone

	d.mu.Lock()
	registered := d.sessions[sess.id]
	d.mu.Unlock()
	require.Same(t, sess, registered, "the replaced initiator deleted its former source session")
	d.deferAttachmentTransitionCleanups(result)
}

func TestPickerDeleteOtherSessionRevalidatesInitiatorAfterAdmissionEnds(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	defer release1()
	defer release2()
	d, source, old, _ := newManualSessionWithPTYs(t, p1)
	source.name = "source"
	target := &session{id: "target", name: "target", ctx: source.ctx, cancel: func() {}, tabs: []*tab{newTab(p2, domain.Size{Cols: 80, Rows: 23})}}
	d.sessions[target.id] = target
	d.enterPicker(source, old)
	old.overlays.pickerMu.Lock()
	for {
		selected, ok := old.overlays.picker.Selected()
		if ok && selected.Session == target.id {
			break
		}
		old.overlays.picker.Down()
	}
	old.overlays.pickerMu.Unlock()

	oldTransport := old.transport()
	rc := d.attachCoordinator(source, nil, old, true)
	token := source.attachmentToken(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.publishRoleCapability(token)

	admissionEnded := make(chan struct{})
	releaseAction := make(chan struct{})
	release := releaseTestGate(t, releaseAction)
	var admissionEndedOnce sync.Once
	d.afterActionRoleEffectEnded = func(action string) {
		if action == "picker-delete" {
			admissionEndedOnce.Do(func() { close(admissionEnded) })
			<-releaseAction
		}
	}
	actionDone := make(chan struct{})
	go func() {
		d.handleActiveClientFrame(token, frameInput([]byte("x")))
		close(actionDone)
	}()
	<-admissionEnded

	d.mu.Lock()
	_, targetStillRegistered := d.sessions[target.id]
	d.mu.Unlock()
	require.True(t, targetStillRegistered, "picker deletion mutated the target before source-token preflight")

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target: source, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: next.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	release()
	<-actionDone
	d.mu.Lock()
	_, targetStillRegistered = d.sessions[target.id]
	d.mu.Unlock()
	require.True(t, targetStillRegistered, "stale picker deletion mutated the target session")
	d.deferAttachmentTransitionCleanups(result)
}

func TestPickerDeleteSourceForCurrentInitiatorDoesNotDeadlock(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	d.enterPicker(sess, ac)
	rc := d.attachCoordinator(sess, nil, ac, true)
	token := sess.attachmentToken(ac, ac.transport())
	token.lease = rc.attachmentLease(ac)
	ac.publishRoleCapability(token)

	d.handleActiveClientFrame(token, frameInput([]byte("x")))

	d.mu.Lock()
	_, registered := d.sessions[sess.id]
	d.mu.Unlock()
	require.False(t, registered)
}

func TestDelayedKeyCallbackAdmittedBeforeReplacementCompletesFirst(t *testing.T) {
	writes := make(chan []byte, 1)
	pty := &transactionalResizePTY{onWrite: func(data []byte) { writes <- data }}
	d, sess, old, _ := newManualSessionWithPTYs(t, pty)
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clock
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)
	old.keys = keys.NewRouter(clock, daemonKeyHandler{d: d, ac: old}, &d.bindings)
	rc := d.attachCoordinator(sess, nil, old, true)
	token := sess.attachmentToken(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.publishRoleCapability(token)

	d.handleActiveClientFrame(token, frameInput([]byte{keys.ESC}))
	timer := <-clock.timers
	admitted := make(chan struct{})
	release := make(chan struct{})
	d.afterRoleEffectAdmitted = func(attachmentRoleToken) {
		close(admitted)
		<-release
	}
	timer.ch <- time.Time{}
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("delayed key callback did not acquire a fresh role-effect ticket")
	}

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	transitionDone := make(chan error, 1)
	go func() {
		_, err := d.transitionAttachment(attachmentTransitionRequest{
			target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
			expectedTransport: next.transportSnapshot(), ready: true,
		})
		transitionDone <- err
	}()
	select {
	case err := <-transitionDone:
		t.Fatalf("replacement overtook admitted delayed key callback: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	require.Equal(t, []byte{keys.ESC}, <-writes)
	require.NoError(t, <-transitionDone)
}

func TestDelayedKeyCallbackAfterReplacementDropsStaleWork(t *testing.T) {
	writes := make(chan []byte, 1)
	pty := &transactionalResizePTY{onWrite: func(data []byte) { writes <- data }}
	d, sess, old, _ := newManualSessionWithPTYs(t, pty)
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clock
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)
	old.keys = keys.NewRouter(clock, daemonKeyHandler{d: d, ac: old}, &d.bindings)
	rc := d.attachCoordinator(sess, nil, old, true)
	token := sess.attachmentToken(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.publishRoleCapability(token)

	d.handleActiveClientFrame(token, frameInput([]byte{keys.ESC}))
	timer := <-clock.timers
	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	_, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: next.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)

	attempted := make(chan bool, 1)
	d.afterDelayedKeyEffectAttempt = func(admitted bool) { attempted <- admitted }
	timer.ch <- time.Time{}
	require.False(t, <-attempted, "stale delayed callback acquired authority after replacement")
	select {
	case got := <-writes:
		t.Fatalf("stale delayed key callback reached the PTY: %q", got)
	default:
	}
}

func TestRoleEffectGateAdmittedSnatchedPanelFinishesBeforePromotion(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)
	d.attachCoordinator(sess, nil, old, true)

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	replaced, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: next.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	token := replaced.displaced

	admitted := make(chan struct{})
	release := make(chan struct{})
	d.afterRoleEffectAdmitted = func(attachmentRoleToken) {
		close(admitted)
		<-release
	}
	effectDone := make(chan struct{})
	wantSize := domain.Size{Cols: 100, Rows: 30}
	go func() {
		d.handleSnatchedClientFrame(token, ports.Frame{Type: ports.MsgResize, Payload: ports.MarshalResize(ports.Resize{Size: wantSize})})
		close(effectDone)
	}()
	<-admitted

	transitionDone := make(chan error, 1)
	go func() {
		_, err := d.transitionAttachment(attachmentTransitionRequest{
			target: sess, next: old, expectedRole: attachmentSnatched, targetRole: attachmentActive,
			expectedTransport: old.transportSnapshot(), ready: true,
		})
		transitionDone <- err
	}()
	select {
	case err := <-transitionDone:
		t.Fatalf("promotion overtook admitted snatched panel: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-effectDone
	require.NoError(t, <-transitionDone)
	old.sendMu.Lock()
	gotSize := old.size
	old.sendMu.Unlock()
	require.Equal(t, wantSize, gotSize)
}

func TestRoleEffectGateAdmittedFirstPaintFinishesBeforeReplacement(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)
	rc := d.attachCoordinator(sess, nil, old, true)
	token := sess.attachmentToken(old, oldTransport)
	token.lease = rc.attachmentLease(old)

	admitted := make(chan struct{})
	release := make(chan struct{})
	d.afterRoleEffectAdmitted = func(attachmentRoleToken) {
		close(admitted)
		<-release
	}
	paintDone := make(chan bool, 1)
	go func() { paintDone <- d.firstPaintForTransition(token) }()
	<-admitted

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	transitionDone := make(chan error, 1)
	go func() {
		_, err := d.transitionAttachment(attachmentTransitionRequest{
			target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
			expectedTransport: next.transportSnapshot(), ready: true,
		})
		transitionDone <- err
	}()
	select {
	case err := <-transitionDone:
		t.Fatalf("replacement overtook admitted first paint: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	require.True(t, <-paintDone)
	require.NoError(t, <-transitionDone)
}

func TestRoleEffectGateReversedConcurrentTransitionsDoNotDeadlock(t *testing.T) {
	d, first, a, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	aTransport := newDatagramTestTransport()
	a.replaceTransport(aTransport)
	d.attachCoordinator(first, nil, a, true)
	first.attachmentToken(a, aTransport)

	bTransport := newDatagramTestTransport()
	b := &attachedClient{tr: bTransport, output: newOutputStateStream(), size: a.size}
	b.initOverlays()
	second := &session{id: "second", name: "second", client: b, snatched: make(map[*attachedClient]struct{})}
	b.setSession(second)
	d.mu.Lock()
	d.sessions[second.id] = second
	d.mu.Unlock()
	d.attachCoordinator(second, nil, b, true)
	second.attachmentToken(b, bTransport)

	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, err := d.transitionAttachment(attachmentTransitionRequest{
			source: first, target: second, next: a,
			expectedRole: attachmentActive, targetRole: attachmentActive,
			expectedTransport: a.transportSnapshot(), ready: true,
		})
		results <- err
	}()
	go func() {
		<-start
		_, err := d.transitionAttachment(attachmentTransitionRequest{
			source: second, target: first, next: b,
			expectedRole: attachmentActive, targetRole: attachmentActive,
			expectedTransport: b.transportSnapshot(), ready: true,
		})
		results <- err
	}()
	close(start)

	errs := make([]error, 0, 2)
	for range 2 {
		select {
		case err := <-results:
			errs = append(errs, err)
		case <-time.After(time.Second):
			t.Fatal("reversed transitions deadlocked")
		}
	}
	successes := 0
	for _, err := range errs {
		if err == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)
}

func TestPickerDeleteReversedWithTargetTransitionDoesNotDeadlock(t *testing.T) {
	d, source, a, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	aTransport := newDatagramTestTransport()
	a.replaceTransport(aTransport)
	aCoordinator := d.attachCoordinator(source, nil, a, true)

	bTransport := newDatagramTestTransport()
	b := &attachedClient{tr: bTransport, output: newOutputStateStream(), size: a.size}
	b.initOverlays()
	target := &session{
		id: "picker-delete-target", name: "picker-delete-target", ctx: source.ctx, cancel: func() {},
		client: b, snatched: make(map[*attachedClient]struct{}),
	}
	b.setSession(target)
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()
	d.attachCoordinator(target, nil, b, true)

	// Give B the earlier immutable identity. The reverse transition therefore
	// freezes B then A, while the old picker-delete path froze A separately
	// before discovering and freezing B.
	b.roleEffects.immutableOrder()
	a.roleEffects.immutableOrder()
	require.Less(t, b.roleEffects.order.Load(), a.roleEffects.order.Load())

	token := source.attachmentToken(a, aTransport)
	token.lease = aCoordinator.attachmentLease(a)
	a.publishRoleCapability(token)
	effect, admitted := a.beginRoleEffect(token)
	require.True(t, admitted)
	token = effect.roleToken()

	deleteDiscovered := make(chan []*attachedClient, 1)
	transitionDiscovered := make(chan []*attachedClient, 1)
	releaseDelete := make(chan struct{})
	releaseTransition := make(chan struct{})
	transitionBFrozen := make(chan struct{})
	deleteFrozen := make(chan *attachedClient, 2)
	d.afterRoleEffectParticipantsSnapshotted = func(action string, participants []*attachedClient) {
		snapshot := append([]*attachedClient(nil), participants...)
		switch action {
		case "picker-delete":
			deleteDiscovered <- snapshot
			<-releaseDelete
		case "reverse-delete-transition":
			transitionDiscovered <- snapshot
			<-releaseTransition
		}
	}
	d.afterRoleEffectGateFrozen = func(action string, ac *attachedClient) {
		switch action {
		case "reverse-delete-transition":
			if ac == b {
				close(transitionBFrozen)
			}
		case "picker-delete":
			deleteFrozen <- ac
		}
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- d.killPickerTargetForRole(picker.Target{Session: target.id}, token)
	}()
	deleteParticipants := <-deleteDiscovered
	require.ElementsMatch(t, []*attachedClient{a, b}, deleteParticipants)

	transitionDone := make(chan error, 1)
	go func() {
		result, err := d.transitionAttachment(attachmentTransitionRequest{
			source: target, target: source, next: b,
			expectedRole: attachmentActive, targetRole: attachmentActive,
			expectedTransport: b.transportSnapshot(), action: "reverse-delete-transition", ready: true,
		})
		if err == nil {
			d.deferAttachmentTransitionCleanups(result)
		}
		transitionDone <- err
	}()
	transitionParticipants := <-transitionDiscovered
	require.ElementsMatch(t, []*attachedClient{a, b}, transitionParticipants)

	close(releaseTransition)
	select {
	case <-transitionBFrozen:
	case <-time.After(time.Second):
		t.Fatal("reverse transition did not freeze B first")
	}
	close(releaseDelete)

	select {
	case err := <-transitionDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("reverse transition deadlocked with picker deletion")
	}
	select {
	case err := <-deleteDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("picker deletion deadlocked with reverse transition")
	}
	frozenByDelete := make([]*attachedClient, 0, 2)
	for range 2 {
		select {
		case ac := <-deleteFrozen:
			frozenByDelete = append(frozenByDelete, ac)
		case <-time.After(time.Second):
			t.Fatal("picker deletion did not freeze every participant")
		}
	}
	require.ElementsMatch(t, []*attachedClient{a, b}, frozenByDelete, "picker deletion must freeze each deduplicated participant exactly once")
	require.Empty(t, deleteFrozen)

	d.mu.Lock()
	registered := d.sessions[target.id]
	d.mu.Unlock()
	require.Same(t, target, registered, "stale picker deletion mutated its target lifecycle")
	require.Equal(t, attachmentSnatched, source.attachmentRole(a))
	require.Equal(t, attachmentActive, source.attachmentRole(b))
}

func TestRoleEffectGateFrozenTransitionRejectsLateEffect(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)
	rc := d.attachCoordinator(sess, nil, old, true)
	token := sess.attachmentToken(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.publishRoleCapability(token)

	frozen := make(chan struct{})
	release := make(chan struct{})
	d.afterRoleEffectsFrozen = func() {
		close(frozen)
		<-release
	}

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	done := make(chan error, 1)
	go func() {
		_, err := d.transitionAttachment(attachmentTransitionRequest{
			target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
			expectedTransport: next.transportSnapshot(), ready: true,
		})
		done <- err
	}()
	<-frozen

	_, ok := old.beginRoleEffect(token)
	require.False(t, ok, "a frozen capability must admit no new effect")
	close(release)
	require.NoError(t, <-done)
}
