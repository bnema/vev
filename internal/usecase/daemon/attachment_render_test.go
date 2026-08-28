package daemon

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

type blockedAttachmentTransport struct {
	entered chan struct{}
	release chan struct{}
	sends   chan wire.Frame
	blocked atomic.Bool
	once    sync.Once
}

func (t *blockedAttachmentTransport) Send(frame wire.Frame) error {
	if frame.Type == wire.MsgOutput && t.blocked.Load() {
		t.once.Do(func() { close(t.entered) })
		<-t.release
	}
	t.sends <- frame
	return nil
}
func (*blockedAttachmentTransport) Recv() (wire.Frame, error) { return wire.Frame{}, io.EOF }
func (*blockedAttachmentTransport) Close() error              { return nil }

func TestStaleCapacityReadinessRequeuesWithoutAnotherInvalidation(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	rc := d.attachCoordinator(sess, nil, ac, true)
	require.True(t, sess.repairAttachmentView(ac))
	rc.opts.clock = nil // drive fire explicitly; the interleaving is the test.

	firstObserved := make(chan struct{})
	retryObserved := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseRetry := make(chan struct{})
	var probes atomic.Int32
	rc.opts.ackReadyFor = func(*attachedClient) bool {
		switch probes.Add(1) {
		case 1:
			close(firstObserved)
			<-releaseFirst
		case 2:
			close(retryObserved)
			<-releaseRetry
		}
		return true
	}

	pane := sess.tabs[0].focusedPane()
	pane.mu.Lock()
	pane.screen.Write([]byte("capacity race"))
	pane.mu.Unlock()
	require.True(t, rc.invalidate(renderInvalidation{class: invalidateUrgent, producer: "capacity-race-test"}))
	fireDone := make(chan struct{})
	go func() {
		rc.fireCurrent(false)
		close(fireDone)
	}()
	awaitTestCompletion(t, firstObserved, "readiness probe did not start")

	// Readiness was observed while the window had room. Fill it before the
	// wake reaches paint; paint must report the failed final capacity check.
	ac.sendMu.Lock()
	ac.output.maxOutstanding = 1
	ac.output.next = 1
	ac.output.acked = 0
	ac.output.syncCapacityLocked()
	ac.sendMu.Unlock()
	close(releaseFirst)
	awaitTestCompletion(t, retryObserved, "capacity failure did not schedule an internal retry")

	// The retry is already in flight. Restore capacity without publishing any
	// new invalidation or ACK notification, then let that retry paint.
	ac.sendMu.Lock()
	ac.output.maxOutstanding = 2
	ac.output.syncCapacityLocked()
	ac.sendMu.Unlock()
	close(releaseRetry)
	frame := awaitFrame(t, sends, wire.MsgOutput)
	output, err := wire.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "capacity race")
	awaitTestCompletion(t, fireDone, "initial fire did not finish")

	rc.mu.Lock()
	requeuedPending := rc.pending
	rc.mu.Unlock()
	require.False(t, requeuedPending, "successful internal retry must consume the mutation")
	require.Equal(t, int32(2), probes.Load(), "retry must not require another external wake")
}

func TestAttachmentResizeKeepsPeerWindowAndExpandsSharedContent(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	firstTransport, _ := newCapturingTransport(t)
	sess, first, err := d.route(protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentNew, Name: "work",
		Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{1},
	}, firstTransport)
	require.NoError(t, err)
	secondTransport, _ := newCapturingTransport(t)
	_, second, err := d.route(protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: "work",
		Size: domain.Size{Cols: 100, Rows: 40}, ClientID: [16]byte{2},
	}, secondTransport)
	require.NoError(t, err)
	require.True(t, sess.repairAttachmentView(second))

	tb := sess.tabs[0]
	firstSize := first.size
	secondRevision := second.viewSnapshot().revision
	secondEpoch := second.output.currentEpoch()

	rc := sess.renderCoordinator()
	lease := rc.attachmentLease(second)
	require.True(t, rc.markAttachmentReady(lease))
	token := sess.captureAttachmentCapability(second, secondTransport)
	token.lease = lease
	second.installTestAttachmentCapability(token)
	effect, admitted := second.beginAttachmentEffect(token)
	require.True(t, admitted)
	require.True(t, d.resizeAttachmentForLease(effect, domain.Size{Cols: 120, Rows: 50}))
	effect.End()

	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 120, Rows: 48}, tb.size)
	tb.mu.Unlock()
	require.Equal(t, firstSize, first.size)
	require.Equal(t, domain.Size{Cols: 120, Rows: 50}, second.size)
	require.Equal(t, secondRevision+1, second.viewSnapshot().revision)
	require.Greater(t, second.output.currentEpoch(), secondEpoch)
}

func TestAttachmentFirstPaintDoesNotWaitForBlockedPeer(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	clock := &signalClock{timers: make(chan *signalTimer, 16)}
	d := newTestDaemon(t, newFactory(t, pty), clock)
	oldTransport := &blockedAttachmentTransport{entered: make(chan struct{}), release: make(chan struct{}), sends: make(chan wire.Frame, 8)}
	sess, old, err := d.route(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentNew, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{1}}, oldTransport)
	require.NoError(t, err)
	newTransport := &blockedAttachmentTransport{entered: make(chan struct{}), release: make(chan struct{}), sends: make(chan wire.Frame, 8)}
	_, fresh, err := d.route(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{2}}, newTransport)
	require.NoError(t, err)

	rc := sess.renderCoordinator()
	oldLease := rc.attachmentLease(old)
	freshLease := rc.attachmentLease(fresh)
	require.True(t, rc.markAttachmentReady(oldLease))
	oldTransport.blocked.Store(true)
	pane := sess.tabs[0].focusedPane()
	pane.mu.Lock()
	pane.screen.Write([]byte("slow peer"))
	pane.mu.Unlock()
	require.True(t, rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: true, producer: "test"}))
	rc.fireCurrent(false)
	awaitTestCompletion(t, oldTransport.entered, "slow attachment did not begin its blocked output")

	require.True(t, rc.markAttachmentReady(freshLease))
	token := sess.captureAttachmentCapability(fresh, newTransport)
	token.lease = freshLease
	painted := make(chan bool, 1)
	go func() { painted <- d.firstPaintForTransition(token) }()
	frame := awaitTestValue(t, newTransport.sends, "healthy attachment first paint was gated by slow peer")
	require.Equal(t, wire.MsgOutput, frame.Type)
	require.True(t, <-painted)
	close(oldTransport.release)
}

func TestMultiAttachmentHandshakeFirstPaintNotGatedByBlockedPeer(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	clock := &signalClock{timers: make(chan *signalTimer, 16)}
	d := newTestDaemon(t, newFactory(t, pty), clock)
	oldTransport := &blockedAttachmentTransport{entered: make(chan struct{}), release: make(chan struct{}), sends: make(chan wire.Frame, 8)}
	sess, old, err := d.route(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentNew, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{1}}, oldTransport)
	require.NoError(t, err)
	rc := sess.renderCoordinator()
	require.True(t, rc.markAttachmentReady(rc.attachmentLease(old)))

	oldTransport.blocked.Store(true)
	pane := sess.tabs[0].focusedPane()
	pane.mu.Lock()
	pane.screen.Write([]byte("slow peer"))
	pane.mu.Unlock()
	require.True(t, rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: true, producer: "test"}))
	fireDone := make(chan struct{})
	go func() {
		rc.fireCurrent(false)
		close(fireDone)
	}()
	awaitTestCompletion(t, oldTransport.entered, "slow attachment did not begin its blocked output")

	hello := protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{2}}
	tr, sends, releaseConn := newConn(t, wire.Frame{Type: wire.MsgHello, Payload: wire.MarshalHello(hello)})
	defer releaseConn()
	handshakeDone := make(chan struct{})
	go func() {
		d.handleConn(tr)
		close(handshakeDone)
	}()
	awaitFrame(t, sends, wire.MsgWelcome)
	firstPaint := awaitFrame(t, sends, wire.MsgOutput)
	require.Equal(t, wire.MsgOutput, firstPaint.Type)
	releaseConn()
	awaitTestCompletion(t, handshakeDone, "multi-attachment handshake did not complete after first paint")
	close(oldTransport.release)
	awaitTestCompletion(t, fireDone, "slow peer output did not finish after release")
}

func TestAttachmentPaintFanoutDoesNotWaitForBlockedPeer(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	firstTransport := &blockedAttachmentTransport{entered: make(chan struct{}), release: make(chan struct{}), sends: make(chan wire.Frame, 8)}
	sess, _, err := d.route(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentNew, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{1}}, firstTransport)
	require.NoError(t, err)
	secondTransport := &blockedAttachmentTransport{entered: make(chan struct{}), release: make(chan struct{}), sends: make(chan wire.Frame, 8)}
	_, _, err = d.route(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{2}}, secondTransport)
	require.NoError(t, err)
	rc := sess.renderCoordinator()
	for _, ac := range sess.snapshotAttachments() {
		require.True(t, rc.markAttachmentReady(rc.attachmentLease(ac)))
	}
	firstTransport.blocked.Store(true)

	sess.tabs[0].focusedPane().screen.Write([]byte("shared"))
	rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: true, producer: "test"})
	rc.fireCurrent(false)
	select {
	case <-firstTransport.entered:
	case <-time.After(time.Second):
		t.Fatal("first attachment did not reach blocked send")
	}
	select {
	case frame := <-secondTransport.sends:
		require.Equal(t, wire.MsgOutput, frame.Type)
		out, err := wire.UnmarshalOutput(frame.Payload)
		require.NoError(t, err)
		require.True(t, out.Full, "fan-out peers must send a fresh first frame even after another attachment acknowledges shared damage")
	case <-time.After(time.Second):
		t.Fatal("second attachment waited for first attachment transport")
	}
	close(firstTransport.release)
}

func TestAttachmentResizeUsesLatestClaimedSessionGeometry(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	firstTransport, _ := newCapturingTransport(t)
	sess, first, err := d.route(protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentNew, Name: "work",
		Size: domain.Size{Cols: 80, Rows: 24}, ClientID: [16]byte{1},
	}, firstTransport)
	require.NoError(t, err)
	secondTransport, _ := newCapturingTransport(t)
	_, second, err := d.route(protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: "work",
		Size: domain.Size{Cols: 100, Rows: 40}, ClientID: [16]byte{2},
	}, secondTransport)
	require.NoError(t, err)
	d.firstPaint(sess, second)

	tb := sess.tabs[0]
	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 100, Rows: 38}, tb.size, "the newest attachment claim must win")
	tb.mu.Unlock()

	rc := sess.renderCoordinator()
	firstLease := rc.attachmentLease(first)
	secondLease := rc.attachmentLease(second)
	require.True(t, rc.markAttachmentReady(firstLease))
	require.True(t, rc.markAttachmentReady(secondLease))
	resize := func(ac *attachedClient, tr ports.ServerConnection, size domain.Size) {
		token := sess.captureAttachmentCapability(ac, tr)
		d.handleAttachmentClientFrame(token, wire.Frame{
			Type:    wire.MsgResize,
			Payload: mustMarshalResize(protocol.Resize{Size: size}),
		})
	}
	resize(second, secondTransport, domain.Size{Cols: 120, Rows: 50})

	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 120, Rows: 48}, tb.size)
	tb.mu.Unlock()
	require.Equal(t, domain.Size{Cols: 80, Rows: 24}, first.size)
	require.Equal(t, domain.Size{Cols: 120, Rows: 50}, second.size)

	// A same-size resize is still a claim, so the smaller peer can take
	// authority without changing its local window state first.
	resize(second, secondTransport, domain.Size{Cols: 90, Rows: 30})
	resize(first, firstTransport, domain.Size{Cols: 80, Rows: 24})
	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 80, Rows: 22}, tb.size)
	tb.mu.Unlock()

	// The latest resize wins even when it is smaller than a live peer.
	resize(first, firstTransport, domain.Size{Cols: 70, Rows: 20})
	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 70, Rows: 18}, tb.size)
	tb.mu.Unlock()
	require.Equal(t, domain.Size{Cols: 70, Rows: 20}, first.size)
	require.Equal(t, domain.Size{Cols: 90, Rows: 30}, second.size)

	// Detaching the latest claimant falls back to the most recent remaining
	// attachment claim rather than reverting to the historical maximum.
	d.clientGone(sess, first, first.transport(), true)
	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 90, Rows: 28}, tb.size)
	tb.mu.Unlock()
}

func TestStaleAttachmentGeometryClaimCannotCommit(t *testing.T) {
	d, sess, first, _ := newManualSessionWithPTYs(t, newQuietPTY())
	second := &attachedClient{output: newOutputStateStream()}
	second.setSize(domain.Size{Cols: 90, Rows: 30})
	second.setSession(sess)
	require.True(t, sess.registerAttachment(second))

	first.setSize(domain.Size{Cols: 120, Rows: 40})
	var superseded bool
	d.beforeSessionResizePublication = func() {
		if superseded {
			return
		}
		superseded = true
		sess.geometry.claimAttachment(sess, second)
	}
	require.False(t, sess.geometry.reconcile(d, sess, first), "a newer attachment claim must reject the stale layout commit")

	tb := sess.tabs[0]
	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 80, Rows: 23}, tb.size)
	tb.mu.Unlock()

	d.beforeSessionResizePublication = nil
	require.True(t, sess.geometry.reconcile(d, sess, nil))
	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 90, Rows: 28}, tb.size)
	tb.mu.Unlock()
}

func TestAttachmentMoveReconcilesSourceGeometryAfterOwnerRemoval(t *testing.T) {
	d, source, moved, _ := newManualSessionWithPTYs(t, newQuietPTY())
	peer := &attachedClient{output: newOutputStateStream()}
	peer.setSize(domain.Size{Cols: 80, Rows: 24})
	peer.setSession(source)
	require.True(t, source.registerAttachment(peer))

	moved.setSize(domain.Size{Cols: 120, Rows: 40})
	_, claimed := source.geometry.claimAttachment(source, moved)
	require.True(t, claimed)
	require.True(t, source.geometry.reconcile(d, source, nil))

	target := &session{
		sessionCore: sessionCore{id: "target-geometry", name: "target-geometry"},
		ctx:         source.ctx,
		cancel:      func() {},
		tabs:        []*tab{newTab(nil, domain.Size{Cols: 80, Rows: 23})},
	}
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()

	result, err := d.transitionAttachment(attachmentTransitionRequest{
		source:            source,
		target:            target,
		next:              moved,
		expectedTransport: moved.transportSnapshot(),
		ready:             true,
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(result)
	d.firstPaint(target, moved)

	target.mu.Lock()
	targetTab := target.tabs[0]
	target.mu.Unlock()
	targetTab.mu.Lock()
	require.Equal(t, domain.Size{Cols: 120, Rows: 38}, targetTab.size, "target geometry must use the moved attachment claim")
	targetTab.mu.Unlock()

	tb := source.tabs[0]
	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 80, Rows: 22}, tb.size, "source geometry must fall back after a moved owner leaves")
	tb.mu.Unlock()
	require.Same(t, source, peer.currentSession())
	require.Same(t, target, moved.currentSession())
}
