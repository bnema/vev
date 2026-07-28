package daemon

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

type failingReclaimTransport struct {
	mu     sync.Mutex
	closed bool
}

func (*failingReclaimTransport) Send(ports.Frame) error { return errors.New("send failed") }
func (*failingReclaimTransport) Recv() (ports.Frame, error) {
	return ports.Frame{}, errors.New("closed")
}
func (t *failingReclaimTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}
func (t *failingReclaimTransport) Closed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func newAtomicReclaimFixture(t *testing.T, clock ports.Clock, waitingTransport, activeTransport ports.Transport) (*Daemon, *session, *attachedClient, *attachedClient) {
	t.Helper()
	d := newTestDaemon(t, nil, clock)
	waiting := &attachedClient{tr: waitingTransport, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	active := &attachedClient{tr: activeTransport, output: newOutputStateStream(), size: waiting.size}
	waiting.initOverlays()
	active.initOverlays()
	sess := &session{
		id: "atomic-reclaim", client: active,
		snatched: map[*attachedClient]struct{}{waiting: {}},
	}
	waiting.setSession(sess)
	active.setSession(sess)
	d.sessions[sess.id] = sess
	d.attachCoordinator(sess, nil, active, true)
	return d, sess, waiting, active
}

func requireSingleOwnerAndLease(t *testing.T, sess *session, clients ...*attachedClient) {
	t.Helper()
	sess.mu.Lock()
	owner := sess.client
	snatched := make(map[*attachedClient]struct{}, len(sess.snatched))
	for ac := range sess.snatched {
		snatched[ac] = struct{}{}
	}
	sess.mu.Unlock()
	require.NotNil(t, owner)

	activeCount := 0
	for _, ac := range clients {
		if ac == owner {
			activeCount++
			continue
		}
		_, waiting := snatched[ac]
		require.True(t, waiting, "every non-owner must remain snatched")
	}
	require.Equal(t, 1, activeCount)

	rc := sess.renderCoordinator()
	rc.mu.Lock()
	lease := rc.lease
	rc.mu.Unlock()
	require.NotNil(t, lease)
	require.True(t, lease.active)
	require.Same(t, owner, lease.attachment)
}

func TestReclaimSnatchedSwapsOwnerLeaseRebasesAndAppliesTheme(t *testing.T) {
	d, sess, requester, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	requesterTransport := &closeTrackingTransport{}
	requester.replaceTransport(requesterTransport)
	d.attachCoordinator(sess, nil, requester, true)

	active := &attachedClient{
		tr:     &closeTrackingTransport{},
		output: newOutputStateStream(),
		size:   requester.size,
	}
	active.initOverlays()
	displaced, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: active,
		expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: active.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(displaced)
	d.attachmentCleanupWg.Wait()

	requester.sendMu.Lock()
	beforeReclaimState := requester.output.next
	requester.sendMu.Unlock()
	requesterTheme := themeui.Theme{
		Foreground: renderer.RGB{R: 11, G: 22, B: 33},
		Background: renderer.RGB{R: 44, G: 55, B: 66},
		HasFG:      true, HasBG: true, Known: true, TrueColor: true,
	}
	requester.setClientTheme(requesterTheme)

	token := sess.attachmentToken(requester, requesterTransport)
	require.Equal(t, attachmentSnatched, token.role)
	require.False(t, d.handleSnatchedClientFrame(token, ports.Frame{
		Type:    ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{Data: []byte{'r'}}),
	}))

	sess.mu.Lock()
	require.Same(t, requester, sess.client)
	_, activeIsSnatched := sess.snatched[active]
	_, requesterIsSnatched := sess.snatched[requester]
	sess.mu.Unlock()
	require.True(t, activeIsSnatched)
	require.False(t, requesterIsSnatched)
	rc := sess.renderCoordinator()
	lease := rc.attachmentLease(requester)
	require.NotNil(t, lease)
	require.Nil(t, rc.attachmentLease(active))
	require.True(t, lease.active)

	requester.sendMu.Lock()
	require.Greater(t, requester.output.next, beforeReclaimState, "reclaim must emit a reset first paint")
	require.GreaterOrEqual(t, requester.output.acked, beforeReclaimState, "reclaim must rebase the old panel output chain")
	requester.sendMu.Unlock()
	assertSessionDefaultColors(t, sess, requesterTheme.Foreground, requesterTheme.Background)

	d.attachmentCleanupWg.Wait()
	require.Equal(t, attachmentSnatched, sess.attachmentRole(active))
	activeTransport, ok := active.transport().(*closeTrackingTransport)
	require.True(t, ok, "active transport has unexpected type")
	require.False(t, activeTransport.Closed())
	require.Equal(t, domain.Size{Cols: 80, Rows: 24}, requester.size)
}

func TestFailedReclaimBeforeCommitPreservesOwnerAndShowsUnavailable(t *testing.T) {
	waitingTransport := &closeTrackingTransport{}
	activeTransport := &closeTrackingTransport{}
	d, sess, waiting, active := newAtomicReclaimFixture(t, stubClock{}, waitingTransport, activeTransport)
	token := sess.attachmentToken(waiting, waitingTransport)

	d.mu.Lock()
	delete(d.sessions, sess.id)
	d.mu.Unlock()
	d.handleSnatchedClientFrame(token, ports.Frame{
		Type:    ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{Data: []byte{'r'}}),
	})

	requireSingleOwnerAndLease(t, sess, waiting, active)
	require.Same(t, active, sess.client)
	require.False(t, waitingTransport.Closed())
	frames := waitingTransport.Sends()
	require.Len(t, frames, 1)
	output, err := ports.UnmarshalOutput(frames[0].Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "Session is no longer available.")
}

func TestPostcommitPanelFailureRetiresOnlyDisplacedClient(t *testing.T) {
	waitingTransport := &closeTrackingTransport{}
	activeTransport := &failingReclaimTransport{}
	d, sess, waiting, active := newAtomicReclaimFixture(t, stubClock{}, waitingTransport, activeTransport)
	token := sess.attachmentToken(waiting, waitingTransport)

	d.handleSnatchedClientFrame(token, ports.Frame{
		Type:    ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{Data: []byte{'r'}}),
	})
	d.attachmentCleanupWg.Wait()

	require.Same(t, waiting, sess.client)
	require.Equal(t, attachmentActive, sess.attachmentRole(waiting))
	require.Equal(t, attachmentDetached, sess.attachmentRole(active))
	require.NotNil(t, sess.renderCoordinator().attachmentLease(waiting))
	require.Nil(t, sess.renderCoordinator().attachmentLease(active))
	require.False(t, waitingTransport.Closed())
	require.True(t, activeTransport.Closed())
}

func TestBlockedRequesterActivationDeadlinePreservesOwnerLease(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 2)}
	waitingTransport := newSnatchedCloseSignalTransport()
	activeTransport := &closeTrackingTransport{}
	d, sess, waiting, active := newAtomicReclaimFixture(t, clock, waitingTransport, activeTransport)

	waiting.sendMu.Lock()
	locked := true
	defer func() {
		if locked {
			waiting.sendMu.Unlock()
		}
	}()
	done := make(chan struct{})
	go func() {
		token := sess.attachmentToken(waiting, waitingTransport)
		d.handleSnatchedClientFrame(token, ports.Frame{
			Type:    ports.MsgInput,
			Payload: ports.MarshalInput(ports.Input{Data: []byte{'r'}}),
		})
		close(done)
	}()

	timer := awaitTestValue(t, clock.timers, "reclaim did not install its activation deadline")
	require.Equal(t, detachNotifyTimeout, timer.duration)
	timer.ch <- time.Time{}
	awaitTestCompletion(t, done, "deadline did not release blocked reclaim")
	require.Same(t, active, sess.client)
	require.Same(t, active, sess.renderCoordinator().attachmentLease(active).attachment)
	require.True(t, waitingTransport.Closed())

	waiting.sendMu.Unlock()
	locked = false
	d.attachmentCleanupWg.Wait()
	require.Equal(t, attachmentDetached, sess.attachmentRole(waiting))
	require.Equal(t, attachmentActive, sess.attachmentRole(active))
}

func TestQuitInvalidatesPausedSnatchedFinalizerWithoutLatePanel(t *testing.T) {
	waitingTransport := &closeTrackingTransport{}
	activeTransport := &closeTrackingTransport{}
	d, sess, waiting, active := newAtomicReclaimFixture(t, stubClock{}, waitingTransport, activeTransport)

	finalizerStarted := make(chan struct{})
	releaseFinalizer := make(chan struct{})
	d.afterDisplacedCleanupStarted = func() {
		close(finalizerStarted)
		<-releaseFinalizer
	}
	waitingToken := sess.attachmentToken(waiting, waitingTransport)
	d.handleSnatchedClientFrame(waitingToken, ports.Frame{
		Type:    ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{Data: []byte{'r'}}),
	})
	awaitTestCompletion(t, finalizerStarted, "displaced finalizer did not pause")

	activeToken := sess.attachmentToken(active, activeTransport)
	require.Equal(t, attachmentSnatched, activeToken.role)
	require.True(t, d.handleSnatchedClientFrame(activeToken, ports.Frame{
		Type:    ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{Data: []byte{'q'}}),
	}))
	close(releaseFinalizer)
	d.attachmentCleanupWg.Wait()

	require.Equal(t, attachmentDetached, sess.attachmentRole(active))
	frames := activeTransport.Sends()
	require.Len(t, frames, 1, "stale finalizer emitted output after quit")
	require.Equal(t, ports.MsgDetached, frames[0].Type)
}

func TestReclaimOrdersDisplacedPanelAfterAdmittedOldPaint(t *testing.T) {
	d, sess, requester, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	requesterTransport := &closeTrackingTransport{}
	requester.replaceTransport(requesterTransport)
	d.attachCoordinator(sess, nil, requester, true)

	activeTransport := &closeTrackingTransport{}
	active := &attachedClient{tr: activeTransport, output: newOutputStateStream(), size: requester.size}
	active.initOverlays()
	initial, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: active,
		expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: active.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(initial)
	d.attachmentCleanupWg.Wait()
	baselineActiveFrames := len(activeTransport.Sends())

	paintAdmitted := make(chan struct{})
	releasePaint := make(chan struct{})
	d.afterRoleEffectAdmitted = func(token attachmentRoleToken) {
		if token.ac == active && token.role == attachmentActive {
			close(paintAdmitted)
			<-releasePaint
		}
	}
	activeToken := sess.attachmentToken(active, activeTransport)
	paintDone := make(chan bool, 1)
	go func() { paintDone <- d.firstPaintForTransition(activeToken) }()
	awaitTestCompletion(t, paintAdmitted, "old active paint was not admitted")

	reclaimFrozen := make(chan struct{})
	var frozenOnce sync.Once
	d.afterRoleEffectGateFrozen = func(action string, _ *attachedClient) {
		if action == "reclaim-snatched" {
			frozenOnce.Do(func() { close(reclaimFrozen) })
		}
	}
	reclaimDone := make(chan struct{})
	go func() {
		token := sess.attachmentToken(requester, requesterTransport)
		d.handleSnatchedClientFrame(token, ports.Frame{
			Type:    ports.MsgInput,
			Payload: ports.MarshalInput(ports.Input{Data: []byte{'r'}}),
		})
		close(reclaimDone)
	}()
	awaitTestCompletion(t, reclaimFrozen, "reclaim did not freeze role effects behind old paint")
	close(releasePaint)
	require.True(t, awaitTestValue(t, paintDone, "old paint did not finish"))
	awaitTestCompletion(t, reclaimDone, "reclaim did not finish after old paint")
	d.attachmentCleanupWg.Wait()

	frames := activeTransport.Sends()[baselineActiveFrames:]
	require.NotEmpty(t, frames)
	last := frames[len(frames)-1]
	require.Equal(t, ports.MsgOutput, last.Type)
	output, err := ports.UnmarshalOutput(last.Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "Session snatched", "old paint overtook the displaced reset panel")
	requireSingleOwnerAndLease(t, sess, requester, active)
}

func TestLaterReclaimMakesPausedFirstPaintStale(t *testing.T) {
	d, sess, first, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	firstTransport := &closeTrackingTransport{}
	first.replaceTransport(firstTransport)
	d.attachCoordinator(sess, nil, first, true)

	secondTransport := &closeTrackingTransport{}
	second := &attachedClient{tr: secondTransport, output: newOutputStateStream(), size: first.size}
	second.initOverlays()
	initial, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: second,
		expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: second.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(initial)
	d.attachmentCleanupWg.Wait()
	baselineFirstFrames := len(firstTransport.Sends())

	firstPaintPaused := make(chan struct{})
	releaseFirstPaint := make(chan struct{})
	d.beforeReclaimFirstPaint = func(token attachmentRoleToken) {
		if token.ac != first {
			return
		}
		close(firstPaintPaused)
		<-releaseFirstPaint
	}
	firstDone := make(chan struct{})
	go func() {
		token := sess.attachmentToken(first, firstTransport)
		d.handleSnatchedClientFrame(token, ports.Frame{
			Type:    ports.MsgInput,
			Payload: ports.MarshalInput(ports.Input{Data: []byte{'r'}}),
		})
		close(firstDone)
	}()
	awaitTestCompletion(t, firstPaintPaused, "first reclaim did not pause before first paint")
	require.Same(t, first, sess.client, "first reclaim was not committed before its paint")

	secondToken := sess.attachmentToken(second, secondTransport)
	require.Equal(t, attachmentSnatched, secondToken.role)
	d.handleSnatchedClientFrame(secondToken, ports.Frame{
		Type:    ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{Data: []byte{'r'}}),
	})
	require.Same(t, second, sess.client)
	close(releaseFirstPaint)
	awaitTestCompletion(t, firstDone, "stale first paint did not return")
	d.attachmentCleanupWg.Wait()

	newFirstFrames := firstTransport.Sends()[baselineFirstFrames:]
	require.Len(t, newFirstFrames, 1, "stale first paint emitted after a later reclaim")
	require.Equal(t, ports.MsgOutput, newFirstFrames[0].Type)
	output, err := ports.UnmarshalOutput(newFirstFrames[0].Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "Session snatched")
	requireSingleOwnerAndLease(t, sess, first, second)
}

func TestAnyWaiterCanReclaimAndConcurrentRequestsKeepSingleOwnerLease(t *testing.T) {
	d, sess, first, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	firstTransport := &closeTrackingTransport{}
	first.replaceTransport(firstTransport)
	d.attachCoordinator(sess, nil, first, true)

	secondTransport := &closeTrackingTransport{}
	second := &attachedClient{tr: secondTransport, output: newOutputStateStream(), size: first.size}
	second.initOverlays()
	toSecond, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: second,
		expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: second.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(toSecond)
	d.attachmentCleanupWg.Wait()

	thirdTransport := &closeTrackingTransport{}
	third := &attachedClient{tr: thirdTransport, output: newOutputStateStream(), size: first.size}
	third.initOverlays()
	toThird, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: third,
		expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: third.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(toThird)
	d.attachmentCleanupWg.Wait()
	requireSingleOwnerAndLease(t, sess, first, second, third)

	firstToken := sess.attachmentToken(first, firstTransport)
	secondToken := sess.attachmentToken(second, secondTransport)
	require.Equal(t, attachmentSnatched, firstToken.role)
	require.Equal(t, attachmentSnatched, secondToken.role)
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	for _, token := range []attachmentRoleToken{firstToken, secondToken} {
		go func() {
			<-start
			d.handleSnatchedClientFrame(token, ports.Frame{
				Type:    ports.MsgInput,
				Payload: ports.MarshalInput(ports.Input{Data: []byte{'r'}}),
			})
			done <- struct{}{}
		}()
	}
	close(start)
	awaitTestCompletion(t, done, "first concurrent reclaim did not return")
	awaitTestCompletion(t, done, "second concurrent reclaim did not return")
	d.attachmentCleanupWg.Wait()

	requireSingleOwnerAndLease(t, sess, first, second, third)
}
