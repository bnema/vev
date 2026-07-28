package daemon

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
)

type snatchedInputTimer struct {
	ch chan time.Time
}

func newSnatchedInputTimer() *snatchedInputTimer {
	return &snatchedInputTimer{ch: make(chan time.Time, 1)}
}

func (t *snatchedInputTimer) C() <-chan time.Time    { return t.ch }
func (*snatchedInputTimer) Reset(time.Duration) bool { return false }
func (*snatchedInputTimer) Stop() bool               { return true }
func (t *snatchedInputTimer) Fire()                  { t.ch <- time.Time{} }

type snatchedInputClock struct {
	timers chan *snatchedInputTimer
}

func newSnatchedInputClock() *snatchedInputClock {
	return &snatchedInputClock{timers: make(chan *snatchedInputTimer, 8)}
}

func (*snatchedInputClock) Now() time.Time { return time.Time{} }
func (c *snatchedInputClock) NewTimer(delay time.Duration) ports.Timer {
	if delay != keys.ESCDelay {
		return stubTimer{}
	}
	timer := newSnatchedInputTimer()
	c.timers <- timer
	return timer
}

type snatchedCloseSignalTransport struct {
	mu        sync.Mutex
	closed    bool
	closedCh  chan struct{}
	closeOnce sync.Once
}

func newSnatchedCloseSignalTransport() *snatchedCloseSignalTransport {
	return &snatchedCloseSignalTransport{closedCh: make(chan struct{})}
}

func (*snatchedCloseSignalTransport) Send(ports.Frame) error { return nil }
func (*snatchedCloseSignalTransport) Recv() (ports.Frame, error) {
	return ports.Frame{}, errors.New("closed")
}
func (t *snatchedCloseSignalTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.closeOnce.Do(func() { close(t.closedCh) })
	return nil
}
func (t *snatchedCloseSignalTransport) Closed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

type blockedSnatchedQuitTransport struct {
	recv        chan ports.Frame
	sendStarted chan struct{}
	sendDone    chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
	sendOnce    sync.Once
	mu          sync.Mutex
	isClosed    bool
}

func newBlockedSnatchedQuitTransport() *blockedSnatchedQuitTransport {
	return &blockedSnatchedQuitTransport{
		recv:        make(chan ports.Frame, 1),
		sendStarted: make(chan struct{}),
		sendDone:    make(chan struct{}),
		closed:      make(chan struct{}),
	}
}

func (t *blockedSnatchedQuitTransport) Send(frame ports.Frame) error {
	if frame.Type != ports.MsgDetached {
		return nil
	}
	t.sendOnce.Do(func() { close(t.sendStarted) })
	<-t.closed
	close(t.sendDone)
	return io.ErrClosedPipe
}

func (t *blockedSnatchedQuitTransport) Recv() (ports.Frame, error) {
	select {
	case frame := <-t.recv:
		return frame, nil
	case <-t.closed:
		return ports.Frame{}, io.ErrClosedPipe
	}
}

func (t *blockedSnatchedQuitTransport) Close() error {
	t.mu.Lock()
	t.isClosed = true
	t.mu.Unlock()
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func (t *blockedSnatchedQuitTransport) Closed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.isClosed
}

func newSnatchedInputFixture(t *testing.T, clock ports.Clock) (*Daemon, *session, *attachedClient, *attachedClient, *snatchedCloseSignalTransport) {
	t.Helper()
	d := newTestDaemon(t, nil, clock)
	waitingTransport := newSnatchedCloseSignalTransport()
	waiting := &attachedClient{
		tr: waitingTransport, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24},
	}
	active := &attachedClient{
		tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: waiting.size,
	}
	waiting.initOverlays()
	active.initOverlays()
	sess := &session{
		id: "strict-input", client: active, snatched: map[*attachedClient]struct{}{waiting: {}},
	}
	waiting.setSession(sess)
	active.setSession(sess)
	d.sessions[sess.id] = sess
	d.attachCoordinator(sess, nil, active, true)
	return d, sess, waiting, active, waitingTransport
}

func sendSnatchedInput(d *Daemon, sess *session, ac *attachedClient, data string) bool {
	tr := ac.transport()
	token := sess.attachmentToken(ac, tr)
	return d.handleSnatchedClientFrame(token, ports.Frame{
		Type:    ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{Data: []byte(data)}),
	})
}

func TestSnatchedInputStrictActions(t *testing.T) {
	tests := []struct {
		name       string
		chunks     []string
		fireEscape bool
		wantRole   attachmentRole
		wantClosed bool
	}{
		{name: "lowercase reclaim", chunks: []string{"r"}, wantRole: attachmentActive},
		{name: "uppercase reclaim", chunks: []string{"R"}, wantRole: attachmentActive},
		{name: "lowercase quit", chunks: []string{"q"}, wantRole: attachmentDetached, wantClosed: true},
		{name: "uppercase quit", chunks: []string{"Q"}, wantRole: attachmentDetached, wantClosed: true},
		{name: "standalone escape quits after delay", chunks: []string{"\x1b"}, fireEscape: true, wantRole: attachmentDetached, wantClosed: true},
		{name: "coalesced reclaim ignored", chunks: []string{"rr"}, wantRole: attachmentSnatched},
		{name: "paste containing quit ignored", chunks: []string{"paste q"}, wantRole: attachmentSnatched},
		{name: "complete CSI ignored", chunks: []string{"\x1b[r"}, wantRole: attachmentSnatched},
		{name: "complete SS3 ignored", chunks: []string{"\x1bOR"}, wantRole: attachmentSnatched},
		{name: "complete sequence tail ignored", chunks: []string{"\x1b[Aq"}, wantRole: attachmentSnatched},
		{name: "split CSI final reclaim ignored", chunks: []string{"\x1b[", "r"}, wantRole: attachmentSnatched},
		{name: "split SS3 final reclaim ignored", chunks: []string{"\x1bO", "R"}, wantRole: attachmentSnatched},
		{name: "split sequence frame tail ignored", chunks: []string{"\x1b[", "Aq"}, wantRole: attachmentSnatched},
		{name: "escape followed by action ignored as alt chord", chunks: []string{"\x1b", "q"}, wantRole: attachmentSnatched},
		{name: "CSI prefix remains inert", chunks: []string{"\x1b[", "12;"}, wantRole: attachmentSnatched},
		{name: "overflow drain consumes final and frame tail", chunks: []string{"\x1b[", strings.Repeat(";", 80), "rQ"}, wantRole: attachmentSnatched},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newSnatchedInputClock()
			d, sess, waiting, active, waitingTransport := newSnatchedInputFixture(t, clock)
			for _, chunk := range tt.chunks {
				sendSnatchedInput(d, sess, waiting, chunk)
			}
			if tt.fireEscape {
				require.Equal(t, attachmentSnatched, sess.attachmentRole(waiting), "escape acted before its delay")
				require.False(t, waitingTransport.Closed(), "escape closed the transport before its delay")
				timer := awaitTestValue(t, clock.timers, "standalone escape did not create a timer")
				timer.Fire()
				awaitTestCompletion(t, waitingTransport.closedCh, "standalone escape did not quit")
			}
			d.attachmentCleanupWg.Wait()

			require.Equal(t, tt.wantRole, sess.attachmentRole(waiting))
			require.Equal(t, tt.wantClosed, waitingTransport.Closed())
			if tt.wantRole == attachmentActive {
				require.Same(t, waiting, sess.client)
				require.Equal(t, attachmentSnatched, sess.attachmentRole(active))
			} else {
				require.Same(t, active, sess.client)
			}
		})
	}
}

func TestBlockedSnatchedQuitDeadlineRetiresOnlyExactWaiter(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 2)}
	waitingTransport := newBlockedSnatchedQuitTransport()
	defer func() { _ = waitingTransport.Close() }()
	activeTransport := &closeTrackingTransport{}
	d, sess, waiting, active := newAtomicReclaimFixture(t, clock, waitingTransport, activeTransport)

	otherTransport := &closeTrackingTransport{}
	other := &attachedClient{tr: otherTransport, output: newOutputStateStream(), size: waiting.size}
	other.initOverlays()
	other.setSession(sess)
	sess.mu.Lock()
	sess.addSnatchedLocked(other)
	sess.mu.Unlock()
	otherToken := sess.attachmentToken(other, otherTransport)
	require.Equal(t, attachmentSnatched, otherToken.role)

	ownerLease := sess.renderCoordinator().attachmentLease(active)
	require.NotNil(t, ownerLease)
	activeGeneration := active.roleGeneration.Load()
	otherGeneration := other.roleGeneration.Load()

	loopDone := make(chan struct{})
	go func() {
		d.runConnLoop(waiting)
		close(loopDone)
	}()
	waitingTransport.recv <- ports.Frame{
		Type:    ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{Data: []byte{'q'}}),
	}
	awaitTestCompletion(t, waitingTransport.sendStarted, "snatched quit did not attempt its detach acknowledgement")
	select {
	case <-loopDone:
		t.Fatal("snatched quit returned before its acknowledgement deadline")
	default:
	}

	timer := awaitTestValue(t, clock.timers, "snatched quit did not install its acknowledgement deadline")
	require.Equal(t, detachNotifyTimeout, timer.duration)
	timer.ch <- time.Time{}
	awaitTestCompletion(t, waitingTransport.sendDone, "closing the captured link did not release its blocked send")
	awaitTestCompletion(t, loopDone, "snatched connection parser did not retire after the acknowledgement deadline")

	require.True(t, waitingTransport.Closed())
	requireRoleGateRetired(t, waiting)
	require.Equal(t, attachmentDetached, sess.attachmentRole(waiting))
	require.Nil(t, waiting.currentSession())
	require.Nil(t, waiting.transport())
	require.Same(t, active, sess.client)
	require.Equal(t, attachmentActive, sess.attachmentRole(active))
	require.Same(t, ownerLease, sess.renderCoordinator().attachmentLease(active))
	require.Equal(t, activeGeneration, active.roleGeneration.Load())
	require.False(t, activeTransport.Closed())
	require.Equal(t, attachmentSnatched, sess.attachmentRole(other))
	require.Equal(t, otherGeneration, other.roleGeneration.Load())
	require.Same(t, otherTransport, other.transport())
	require.False(t, otherTransport.Closed())
}

func TestBlockedSnatchedQuitCleanupRejectsReclaimedTransportGeneration(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 2)}
	staleTransport := newBlockedSnatchedQuitTransport()
	defer func() { _ = staleTransport.Close() }()
	ownerTransport := &closeTrackingTransport{}
	d, sess, waiting, owner := newAtomicReclaimFixture(t, clock, staleTransport, ownerTransport)

	ticketEnded := make(chan struct{})
	releaseQuit := make(chan struct{})
	d.afterActionRoleEffectEnded = func(action string) {
		if action == "quit-snatched" {
			close(ticketEnded)
			<-releaseQuit
		}
	}
	loopDone := make(chan struct{})
	go func() {
		d.runConnLoop(waiting)
		close(loopDone)
	}()
	staleTransport.recv <- ports.Frame{
		Type:    ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{Data: []byte{'Q'}}),
	}
	awaitTestCompletion(t, staleTransport.sendStarted, "snatched quit did not block in its captured transport")
	timer := awaitTestValue(t, clock.timers, "snatched quit did not install its acknowledgement deadline")
	timer.ch <- time.Time{}
	awaitTestCompletion(t, staleTransport.sendDone, "deadline close did not retire the captured send")
	awaitTestCompletion(t, ticketEnded, "snatched quit did not end its role ticket before cleanup")

	freshTransport := &closeTrackingTransport{}
	waiting.replaceTransport(freshTransport)
	reclaimed, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: waiting, expectedRole: attachmentSnatched, targetRole: attachmentActive,
		expectedTransport: waiting.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(reclaimed)
	freshGeneration := reclaimed.published.generation
	freshLease := reclaimed.published.lease

	close(releaseQuit)
	awaitTestCompletion(t, loopDone, "stale quit parser did not retire after reclaim")
	d.attachmentCleanupWg.Wait()

	require.True(t, staleTransport.Closed())
	requireRoleGateRetired(t, waiting)
	require.Same(t, waiting, sess.client)
	require.Equal(t, attachmentActive, sess.attachmentRole(waiting))
	require.Equal(t, freshGeneration, waiting.roleGeneration.Load())
	require.Same(t, freshTransport, waiting.transport())
	require.False(t, freshTransport.Closed(), "stale quit cleanup closed the reclaimed transport")
	require.Same(t, freshLease, sess.renderCoordinator().attachmentLease(waiting))
	require.Equal(t, attachmentSnatched, sess.attachmentRole(owner))
	require.False(t, ownerTransport.Closed(), "stale quit cleanup affected the displaced owner")
}

func TestSnatchedInputTimerCannotAffectNewRoleOrTransport(t *testing.T) {
	clock := newSnatchedInputClock()
	d, sess, waiting, _, oldTransport := newSnatchedInputFixture(t, clock)

	sendSnatchedInput(d, sess, waiting, "\x1b")
	timer := awaitTestValue(t, clock.timers, "standalone escape did not create a timer")
	accepted := make(chan struct{})
	release := make(chan struct{})
	attempted := make(chan bool, 1)
	d.afterSnatchedEscapeAccepted = func() {
		close(accepted)
		<-release
	}
	d.afterSnatchedEscapeAttempt = func(admitted bool) { attempted <- admitted }
	timer.Fire()
	awaitTestCompletion(t, accepted, "escape timer callback did not consume parser state")

	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: waiting, expectedRole: attachmentSnatched, targetRole: attachmentActive,
		expectedTransport: waiting.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(result)
	newTransport := newSnatchedCloseSignalTransport()
	waiting.replaceTransport(newTransport)
	close(release)
	require.False(t, awaitTestValue(t, attempted, "escape callback did not revalidate its stale role"))
	d.attachmentCleanupWg.Wait()

	require.Equal(t, attachmentActive, sess.attachmentRole(waiting))
	require.Same(t, newTransport, waiting.transport())
	require.False(t, newTransport.Closed(), "stale timer closed the replacement transport")
	require.False(t, oldTransport.Closed(), "stale timer closed the old transport after role activation")
}
