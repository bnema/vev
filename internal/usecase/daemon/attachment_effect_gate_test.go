package daemon

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
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

func startBlockedAttachmentSend(t *testing.T, token attachmentCapability) <-chan struct{} {
	t.Helper()
	ticket, admitted := token.ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ticket.End()
		_ = token.ac.sendExpectedTransportForAttachment(token.transport, framePong(), ticket)
	}()
	return done
}

func requireAttachmentEffectGateRetired(t *testing.T, ac *attachedClient) {
	t.Helper()
	ac.lifecycle.mu.Lock()
	defer ac.lifecycle.mu.Unlock()
	require.Equal(t, attachmentEffectsStable, ac.lifecycle.phase)
	require.Zero(t, ac.lifecycle.inFlight)
	require.Empty(t, ac.lifecycle.transportEffects)
}

func TestBeginCurrentAttachmentEffectFailsClosedOnStableCapabilityMismatch(t *testing.T) {
	_, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	current := sess.captureAttachmentCapability(ac, ac.transport())
	stale := current
	stale.generation++
	ac.installTestAttachmentCapability(stale)

	result := make(chan bool, 1)
	go func() {
		_, ticket, admitted := ac.beginCurrentAttachmentEffect(sess, ac.transport())
		if ticket != nil {
			ticket.End()
		}
		result <- admitted
	}()
	select {
	case admitted := <-result:
		require.False(t, admitted)
	case <-time.After(time.Second):
		t.Fatal("stable capability mismatch spun instead of failing closed")
	}
}

func TestBeginAttachmentLeaseEffectRejectsForeignLease(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	rc := d.attachCoordinator(sess, nil, ac, true)
	foreign := &attachmentLease{attachment: &attachedClient{}}
	_, admitted := beginAttachmentLeaseEffect(sess, ac, foreign)
	require.False(t, admitted)
	require.NotNil(t, rc.attachmentLease(ac))
}

func attachmentEffectGatePublicationSnapshot(ac *attachedClient) (attachmentEffectPhase, attachmentCapability) {
	ac.lifecycle.mu.Lock()
	defer ac.lifecycle.mu.Unlock()
	return ac.lifecycle.phase, ac.lifecycle.capability
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
	sess.registerAttachmentLocked(blocked)
	sess.registerAttachmentLocked(later)
	sess.mu.Unlock()

	rc := d.attachCoordinator(sess, nil, first, true)
	firstToken := sess.captureAttachmentCapability(first, firstTransport)
	firstToken.lease = rc.attachmentLease(first)
	first.installTestAttachmentCapability(firstToken)
	blockedToken := sess.captureAttachmentCapability(blocked, blockedTransport)
	blocked.installTestAttachmentCapability(blockedToken)
	laterToken := sess.captureAttachmentCapability(later, laterTransport)
	later.installTestAttachmentCapability(laterToken)

	// Fix the canonical order so teardown acquires first, waits behind the gate
	// owned by this test, and never reaches the still-stable later participant.
	first.lifecycle.immutableOrder()
	blocked.lifecycle.immutableOrder()
	later.lifecycle.immutableOrder()
	require.Less(t, first.lifecycle.order.Load(), blocked.lifecycle.order.Load())
	require.Less(t, blocked.lifecycle.order.Load(), later.lifecycle.order.Load())

	blockedOwner := freezeAttachmentEffectGates(blocked)
	blockedOwnerHeld := true
	t.Cleanup(func() {
		if blockedOwnerHeld {
			blockedOwner.unfreeze()
		}
	})

	firstPhase, firstCapability := attachmentEffectGatePublicationSnapshot(first)
	blockedPhase, blockedCapability := attachmentEffectGatePublicationSnapshot(blocked)
	laterPhase, laterCapability := attachmentEffectGatePublicationSnapshot(later)
	require.Equal(t, attachmentEffectsStable, firstPhase)
	require.Equal(t, attachmentEffectsFrozen, blockedPhase)
	require.Equal(t, attachmentEffectsStable, laterPhase)
	firstGeneration := first.lifecycle.generationValue()
	blockedGeneration := blocked.lifecycle.generationValue()
	laterGeneration := later.lifecycle.generationValue()
	firstSnapshot := first.transportSnapshot()
	blockedSnapshot := blocked.transportSnapshot()
	laterSnapshot := later.transportSnapshot()

	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clock
	frozenByKill := make(chan *attachedClient, 3)
	d.afterAttachmentEffectGateFrozen = func(_ string, ac *attachedClient) { frozenByKill <- ac }
	killDone := make(chan error, 1)
	go func() { killDone <- d.killSession(sess, ports.ReasonSessionKilled, false) }()

	require.Same(t, first, awaitTestValue(t, frozenByKill, "teardown did not acquire the first attachment effect gate"))
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
	require.Contains(t, sess.snapshotAttachmentsLocked(), first)
	require.Contains(t, sess.snapshotAttachmentsLocked(), blocked)
	require.Contains(t, sess.snapshotAttachmentsLocked(), later)
	sess.mu.Unlock()
	for _, ac := range []*attachedClient{first, blocked, later} {
		require.Same(t, sess, ac.currentSession(), "partial gate acquisition cleared attachment ownership")
	}
	require.Equal(t, firstGeneration, first.lifecycle.generationValue())
	require.Equal(t, blockedGeneration, blocked.lifecycle.generationValue())
	require.Equal(t, laterGeneration, later.lifecycle.generationValue())
	require.True(t, first.transportSnapshotCurrent(firstSnapshot))
	require.True(t, blocked.transportSnapshotCurrent(blockedSnapshot))
	require.True(t, later.transportSnapshotCurrent(laterSnapshot))
	require.False(t, firstTransport.Closed())
	require.False(t, blockedTransport.Closed())
	require.False(t, laterTransport.Closed())

	firstPhase, gotFirstCapability := attachmentEffectGatePublicationSnapshot(first)
	blockedPhase, gotBlockedCapability := attachmentEffectGatePublicationSnapshot(blocked)
	laterPhase, gotLaterCapability := attachmentEffectGatePublicationSnapshot(later)
	require.Equal(t, attachmentEffectsStable, firstPhase, "partially acquired gate was not rolled back")
	require.Equal(t, attachmentEffectsFrozen, blockedPhase, "teardown released a gate owned by another transition")
	require.Equal(t, attachmentEffectsStable, laterPhase, "unacquired later gate was frozen or invalidated")
	require.Equal(t, firstCapability, gotFirstCapability)
	require.Equal(t, blockedCapability, gotBlockedCapability)
	require.Equal(t, laterCapability, gotLaterCapability)

	blockedOwner.unfreeze()
	blockedOwnerHeld = false
	complete := freezeAttachmentEffectGates(first, blocked, later)
	require.True(t, complete.acquired, "rolled-back gate set could not be acquired again")
	require.True(t, complete.drained)
	complete.unfreeze()
	for _, token := range []attachmentCapability{firstToken, blockedToken, laterToken} {
		ticket, admitted := token.ac.beginAttachmentEffect(token)
		require.True(t, admitted, "pre-publication abort changed an attachment capability")
		ticket.End()
		requireAttachmentEffectGateRetired(t, token.ac)
	}
}

func TestKillSessionInterruptsOnlyExactParticipantBlockedSends(t *testing.T) {
	d, sess, snatched, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	snatchedTransport := newTeardownBlockingTransport()
	snatched.replaceTransport(snatchedTransport)
	d.attachCoordinator(sess, nil, snatched, true)
	snatched.installTestAttachmentCapability(sess.captureAttachmentCapability(snatched, snatchedTransport))

	activeTransport := newTeardownBlockingTransport()
	active := &attachedClient{tr: activeTransport, output: newOutputStateStream(), size: snatched.size}
	active.initOverlays()
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: active, expectedTransport: active.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	defer d.deferAttachmentTransitionCleanups(transition)

	unrelatedTransport := &closeTrackingTransport{}
	unrelatedClient := &attachedClient{tr: unrelatedTransport, output: newOutputStateStream(), size: active.size}
	unrelatedClient.initOverlays()
	unrelated := &session{sessionCore: sessionCore{id: "unrelated", name: "unrelated", attachments: map[*attachedClient]struct{}{unrelatedClient: {}}}, ctx: sess.ctx, cancel: func() {}}
	unrelatedClient.setSession(unrelated)
	d.mu.Lock()
	d.sessions[unrelated.id] = unrelated
	d.mu.Unlock()
	d.attachCoordinator(unrelated, nil, unrelatedClient, true)

	snatchedToken := sess.captureAttachmentCapability(snatched, snatchedTransport)
	activeToken := sess.captureAttachmentCapability(active, activeTransport)
	snatchedDone := startBlockedAttachmentSend(t, snatchedToken)
	activeDone := startBlockedAttachmentSend(t, activeToken)
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
	awaitTestCompletion(t, snatchedDone, "snatched attachment effect ticket did not retire")
	awaitTestCompletion(t, activeDone, "active attachment effect ticket did not retire")
	d.waitNotifies()

	require.True(t, snatchedTransport.Closed())
	require.True(t, activeTransport.Closed())
	require.False(t, unrelatedTransport.Closed(), "unrelated session transport was interrupted")
	require.Contains(t, unrelated.snapshotAttachments(), unrelatedClient)
	require.Same(t, unrelated, unrelatedClient.currentSession())
	requireAttachmentEffectGateRetired(t, snatched)
	requireAttachmentEffectGateRetired(t, active)
}

func TestKillSessionInterruptsCapturedSendWithoutClosingNewerIncarnation(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	stale := newTeardownBlockingTransport()
	ac.replaceTransport(stale)
	d.attachCoordinator(sess, nil, ac, true)
	ac.installTestAttachmentCapability(sess.captureAttachmentCapability(ac, stale))

	token := sess.captureAttachmentCapability(ac, stale)
	sendDone := startBlockedAttachmentSend(t, token)
	awaitTestCompletion(t, stale.sendEntered, "captured incarnation send did not block")
	fresh := &closeTrackingTransport{}
	d.afterAttachmentEffectParticipantsSnapshotted = func(string, []*attachedClient) {
		ac.replaceTransport(fresh)
	}

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
	awaitTestCompletion(t, sendDone, "captured incarnation attachment effect ticket did not retire")
	require.True(t, stale.Closed(), "exact captured in-flight transport was not interrupted")
	require.False(t, fresh.Closed(), "newer transport incarnation was interrupted")
	require.Same(t, fresh, ac.transport())
	d.mu.Lock()
	require.Same(t, sess, d.sessions[sess.id], "transport incarnation change should abort stale teardown publication")
	d.mu.Unlock()
	sess.mu.Lock()
	require.Contains(t, sess.snapshotAttachmentsLocked(), ac)
	sess.mu.Unlock()
	requireAttachmentEffectGateRetired(t, ac)
}

func TestShutdownSignalsServeWhenParticipantGateAcquisitionTimesOut(t *testing.T) {
	d, sess, active, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	transport := &closeTrackingTransport{}
	active.replaceTransport(transport)
	d.attachCoordinator(sess, nil, active, true)
	active.installTestAttachmentCapability(sess.captureAttachmentCapability(active, transport))

	owner := freezeAttachmentEffectGates(active)
	require.True(t, owner.acquired)
	defer owner.unfreeze()

	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clock
	shutdownDone := make(chan bool, 1)
	go func() { shutdownDone <- d.shutdownAll(ports.ReasonServerShutdown) }()

	deadline := awaitTestValue(t, clock.timers, "shutdown did not arm the gate acquisition deadline")
	deadline.ch <- clock.Now()
	require.False(t, awaitTestValue(t, shutdownDone, "shutdown did not return after its gate acquisition deadline"))

	d.mu.Lock()
	registered := d.sessions[sess.id]
	d.mu.Unlock()
	require.Same(t, sess, registered, "timed-out initial teardown should leave the session for Serve's shutdown pass")
	select {
	case <-d.done:
	case <-time.After(time.Second):
		t.Fatal("shutdown request did not signal Serve after the initial teardown pass timed out")
	}
}

func TestShutdownInterruptsBlockedParticipantSend(t *testing.T) {
	d, sess, active, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	blocked := newTeardownBlockingTransport()
	active.replaceTransport(blocked)
	d.attachCoordinator(sess, nil, active, true)
	active.installTestAttachmentCapability(sess.captureAttachmentCapability(active, blocked))

	token := sess.captureAttachmentCapability(active, blocked)
	sendDone := startBlockedAttachmentSend(t, token)
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
	awaitTestCompletion(t, sendDone, "shutdown-interrupted attachment effect ticket did not retire")
	d.waitNotifies()
	require.True(t, blocked.Closed())
	requireAttachmentEffectGateRetired(t, active)
}

func TestRenderSendFailureCleanupIsDeadlineBoundedWhileAttachmentEffectGateIsBusy(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	transport := &closeTrackingTransport{}
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)

	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clock
	frozen := freezeAttachmentEffectGates(ac)
	defer frozen.unfreeze()
	cleanupDone := make(chan struct{})
	d.afterAttachmentSendErrorCleanup = func() { close(cleanupDone) }

	launchCleanup := d.reserveAttachmentSendErrorCleanup(token, transport)
	launchCleanup()
	deadline := awaitTestValue(t, clock.timers, "render cleanup did not arm its deadline")
	require.Equal(t, detachNotifyTimeout, deadline.duration)
	deadline.ch <- time.Now()
	awaitTestCompletion(t, cleanupDone, "deadline did not release blocked render cleanup")
	awaitTestCompletion(t, waitGroupDone(&d.attachmentCleanupWg), "bounded render cleanup leaked its tracked goroutine")

	sess.mu.Lock()
	require.Contains(t, sess.snapshotAttachmentsLocked(), ac)
	sess.mu.Unlock()
	require.False(t, transport.Closed())
}

func TestAttachmentEffectGateReplacementInterruptsBlockedOldRenderBeforePublication(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newBlockedRenderReplacementTransport(d, sess)
	old.replaceTransport(oldTransport)
	rc := d.attachCoordinator(sess, nil, old, true)
	lease := rc.attachmentLease(old)
	token := sess.captureAttachmentCapability(old, oldTransport)
	token.lease = lease
	old.installTestAttachmentCapability(token)

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
			target: sess, next: next, expectedTransport: next.transportSnapshot(), ready: true,
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
	require.Contains(t, sess.snapshotAttachments(), next)
	require.Contains(t, sess.snapshotAttachments(), old)
	require.False(t, newTransport.Closed(), "new attachment transport was affected by publication")

	// Generic attachment publication does not interrupt or retire another
	// registered connection. Release the test transport explicitly.
	require.NoError(t, oldTransport.Close())
	select {
	case <-paintDone:
	case <-time.After(time.Second):
		t.Fatal("closing the test transport did not unblock its render")
	}
	d.deferAttachmentTransitionCleanups(result)
	d.attachmentCleanupWg.Wait()
	require.Same(t, oldTransport, old.transport(), "unrelated attachment link was retired")
	require.False(t, newTransport.Closed())
}

func TestAttachmentEffectGateAdmittedEffectCompletesBeforeConflictingTransition(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)
	rc := d.attachCoordinator(sess, nil, old, true)
	token := sess.captureAttachmentCapability(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.installTestAttachmentCapability(token)

	ticket, ok := old.beginAttachmentEffect(token)
	require.True(t, ok)

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	done := make(chan error, 1)
	go func() {
		_, err := d.transitionAttachment(attachmentTransitionRequest{
			target: sess, next: next, expectedTransport: next.transportSnapshot(), ready: true,
		})
		done <- err
	}()

	// Adding an independent attachment does not wait on another connection's
	// effect gate.
	require.NoError(t, <-done)
	ticket.End()
}

func TestAttachmentEffectGateAdmittedActiveEffectsFinishBeforeReplacement(t *testing.T) {
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
				p := testAttachmentTab(sess).focusedPane()
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
			frame: ports.Frame{Type: ports.MsgAck, Payload: mustMarshalAck(ports.Ack{Epoch: 1, State: 1})},
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
			token := sess.captureAttachmentCapability(old, oldTransport)
			token.lease = rc.attachmentLease(old)
			old.installTestAttachmentCapability(token)
			if tt.setup != nil {
				tt.setup(d, sess, old, pty)
			}

			admitted := make(chan struct{})
			release := make(chan struct{})
			d.afterAttachmentEffectAdmitted = func(attachmentCapability) {
				close(admitted)
				<-release
			}
			effectDone := make(chan struct{})
			go func() {
				d.handleAttachmentClientFrame(token, tt.frame)
				close(effectDone)
			}()
			<-admitted

			next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
			next.initOverlays()
			transitionDone := make(chan error, 1)
			go func() {
				_, err := d.transitionAttachment(attachmentTransitionRequest{
					target: sess, next: next, expectedTransport: next.transportSnapshot(), ready: true,
				})
				transitionDone <- err
			}()
			require.NoError(t, <-transitionDone)
			close(release)
			<-effectDone
			tt.assert(t, old, pty)
		})
	}
}

func TestAttachedRouteSnapshotIsAcceptedAsAnAttachmentValue(t *testing.T) {
	d, source, ac, _ := newManualSessionWithPTYs(t, nil)
	transport := &closeTrackingTransport{}
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(source, nil, ac, true)
	token := source.captureAttachmentCapability(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)

	activeRef := ports.RouteRef{Key: 8, Generation: 4}
	activeTarget := testRouteTarget(source.name, 8)
	snapshot := ports.RecentRouteSnapshot{
		Generation:  4,
		Active:      activeRef,
		ActiveEntry: ports.RecentRouteEntry{Key: 8, Generation: 4, Target: activeTarget, Name: source.name, Kind: ports.RouteKindLocal},
		Home:        activeRef,
		Entries:     []ports.RecentRouteEntry{testRouteEntry(7, 3, "previous", 7, ports.RouteKindLocal)},
	}
	payload, err := ports.MarshalRecentRouteSnapshot(snapshot)
	require.NoError(t, err)

	require.False(t, d.handleAttachmentClientFrame(token, ports.Frame{Type: ports.MsgRecentRouteSnapshot, Payload: payload}))
	require.Equal(t, snapshot, ac.routeSnapshotCopy())
}

func TestAttachedNavigationCommandSendsResultAfterLocalTransition(t *testing.T) {
	d, source, ac, _ := newManualSessionWithPTYs(t, nil)
	target := &session{sessionCore: sessionCore{id: "target", name: "target"}, ctx: source.ctx, cancel: func() {}, tabs: []*tab{
		newTab(nil, domain.Size{Cols: 80, Rows: 23}),
	}}
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{
		Generation:  2,
		Active:      ports.RouteRef{Key: 2, Generation: 2},
		ActiveEntry: testRouteEntry(2, 2, source.name, 2, ports.RouteKindLocal),
		Previous:    ports.RouteRef{Key: 1, Generation: 1},
		Entries:     []ports.RecentRouteEntry{testRouteEntry(1, 1, "target", 1, ports.RouteKindLocal)},
	})

	transport := &closeTrackingTransport{}
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(source, nil, ac, true)
	token := source.captureAttachmentCapability(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)
	payload, err := ports.MarshalCommandRequest(ports.CommandRequest{
		Version: ports.ProtocolVersion, RequestID: 1, Slug: "back-session", Attached: true,
	})
	require.NoError(t, err)

	require.False(t, d.handleAttachmentClientFrame(token, ports.Frame{Type: ports.MsgCommand, Payload: payload}))
	require.Same(t, source, ac.currentAttachmentSession())
	frames := transport.Sends()
	require.NotEmpty(t, frames)
	var result ports.CommandResult
	for _, frame := range frames {
		if frame.Type != ports.MsgCommandResult {
			continue
		}
		result, err = ports.UnmarshalCommandResult(frame.Payload)
		require.NoError(t, err)
		break
	}
	require.True(t, result.OK, result.Text)
	var action ports.RouteNavigationAction
	for _, frame := range frames {
		if frame.Type != ports.MsgNavigateRecentRoute {
			continue
		}
		action, err = ports.UnmarshalRouteNavigationAction(frame.Payload)
		require.NoError(t, err)
	}
	require.Equal(t, ports.RouteNavigationAction{SnapshotGeneration: 2, Key: 1, Generation: 1}, action)
	require.False(t, transport.Closed(), "client navigation must not tear down the attachment")
}

func TestJumpAttentionAdmittedHandoffCrossesSessions(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	p3, release3 := newBlockingPTY(t)
	defer release1()
	defer release2()
	defer release3()
	d, source, ac, _ := newManualSessionWithPTYs(t, p1)
	target := &session{sessionCore: sessionCore{id: "target", name: "target"}, ctx: source.ctx, cancel: func() {}, tabs: []*tab{
		newTab(p2, domain.Size{Cols: 80, Rows: 23}),
		newTab(p3, domain.Size{Cols: 80, Rows: 23}),
	}}
	target.tabs[1].attention = true
	target.tabs[1].attentionAt = time.Unix(1, 0)
	d.sessions[target.id] = target

	rc := d.attachCoordinator(source, nil, ac, true)
	token := source.captureAttachmentCapability(ac, ac.transport())
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)

	d.handleAttachmentClientFrame(token, frameInput([]byte("\x1ba")))

	require.Same(t, target, ac.currentSession())
	require.Equal(t, 1, testAttachmentTabIndex(target))
}

func TestPickerDeleteDoesNotDeleteSourceAfterInitiatorReplacement(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, old, _ := newManualSessionWithPTYs(t, p)
	d.enterPicker(sess, old)

	oldTransport := old.transport()
	rc := d.attachCoordinator(sess, nil, old, true)
	token := sess.captureAttachmentCapability(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.installTestAttachmentCapability(token)

	admissionEnded := make(chan struct{})
	releaseAction := make(chan struct{})
	release := releaseTestGate(t, releaseAction)
	var admissionEndedOnce sync.Once
	d.afterActionAttachmentEffectEnded = func(action string) {
		if action != "picker-delete" {
			return
		}
		admissionEndedOnce.Do(func() { close(admissionEnded) })
		<-releaseAction
	}
	actionDone := make(chan struct{})
	go func() {
		d.handleAttachmentClientFrame(token, frameInput([]byte("x")))
		close(actionDone)
	}()
	<-admissionEnded

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: next, expectedTransport: next.transportSnapshot(), ready: true,
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

func TestPickerDeleteSourceForCurrentInitiatorDoesNotDeadlock(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	d.enterPicker(sess, ac)
	rc := d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, ac.transport())
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)

	d.handleAttachmentClientFrame(token, frameInput([]byte("x")))

	d.mu.Lock()
	_, registered := d.sessions[sess.id]
	d.mu.Unlock()
	require.False(t, registered)
}

func TestAttachmentEffectGateAdmittedFirstPaintFinishesBeforeReplacement(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)
	rc := d.attachCoordinator(sess, nil, old, true)
	token := sess.captureAttachmentCapability(old, oldTransport)
	token.lease = rc.attachmentLease(old)
	old.installTestAttachmentCapability(token)

	admitted := make(chan struct{})
	release := make(chan struct{})
	d.afterAttachmentEffectAdmitted = func(attachmentCapability) {
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
			target: sess, next: next, expectedTransport: next.transportSnapshot(), ready: true,
		})
		transitionDone <- err
	}()
	// An independent attachment publication does not wait on this connection's
	// admitted first paint.
	require.NoError(t, <-transitionDone)
	close(release)
	require.True(t, <-paintDone)
}

func TestAttachmentEffectGateReversedConcurrentTransitionsDoNotDeadlock(t *testing.T) {
	d, first, a, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	aTransport := newDatagramTestTransport()
	a.replaceTransport(aTransport)
	d.attachCoordinator(first, nil, a, true)
	first.captureAttachmentCapability(a, aTransport)

	bTransport := newDatagramTestTransport()
	b := &attachedClient{tr: bTransport, output: newOutputStateStream(), size: a.size}
	b.initOverlays()
	second := &session{sessionCore: sessionCore{id: "second", name: "second", attachments: map[*attachedClient]struct{}{b: {}}}}
	b.setSession(second)
	d.mu.Lock()
	d.sessions[second.id] = second
	d.mu.Unlock()
	d.attachCoordinator(second, nil, b, true)
	second.captureAttachmentCapability(b, bTransport)

	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, err := d.transitionAttachment(attachmentTransitionRequest{
			source: first, target: second, next: a,

			expectedTransport: a.transportSnapshot(), ready: true,
		})
		results <- err
	}()
	go func() {
		<-start
		_, err := d.transitionAttachment(attachmentTransitionRequest{
			source: second, target: first, next: b,

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
	require.Equal(t, 2, successes)
}

func TestAttachmentEffectGateFrozenConnectionRejectsLateEffect(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	transport := newDatagramTestTransport()
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(sess, nil, ac, true)
	token := sess.captureAttachmentCapability(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)

	frozen := freezeAttachmentEffectGates(ac)
	require.True(t, frozen.acquired)
	require.True(t, frozen.drained)
	_, ok := ac.beginAttachmentEffect(token)
	require.False(t, ok, "a frozen capability must admit no new effect")
	frozen.unfreeze()
}
