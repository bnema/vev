package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/client"
)

func init() {
	// Keep client diagnostics out of the test runner's stderr.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// realClock is a stdlib-backed ports.Clock for tests that exercise the stdin
// pump end to end; the paste coalescer's flush timer runs on wall time here.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) NewTimer(d time.Duration) ports.Timer {
	return &realTimer{t: time.NewTimer(d)}
}

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }

type reconnectTestClock struct {
	mu     sync.Mutex
	timers []*reconnectTestTimer
}

func (c *reconnectTestClock) Now() time.Time { return time.Time{} }
func (c *reconnectTestClock) NewTimer(d time.Duration) ports.Timer {
	t := &reconnectTestTimer{ch: make(chan time.Time, 1), duration: d}
	c.mu.Lock()
	c.timers = append(c.timers, t)
	c.mu.Unlock()
	return t
}

func (c *reconnectTestClock) fireReconnect(t *testing.T) {
	t.Helper()
	c.fireDuration(t, 100*time.Millisecond)
}

func (c *reconnectTestClock) fireDuration(t *testing.T, duration time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, timer := range c.timers {
			timer.mu.Lock()
			if timer.duration != duration {
				timer.mu.Unlock()
				continue
			}
			if !timer.stopped && !timer.fired {
				timer.fired = true
				select {
				case timer.ch <- time.Time{}:
				default:
					timer.fired = false
					timer.mu.Unlock()
					continue
				}
				timer.mu.Unlock()
				return true
			}
			timer.mu.Unlock()
		}
		return false
	}, time.Second, time.Millisecond)
}

type reconnectTestTimer struct {
	mu       sync.Mutex
	ch       chan time.Time
	duration time.Duration
	stopped  bool
	fired    bool
}

func (t *reconnectTestTimer) C() <-chan time.Time { return t.ch }
func (t *reconnectTestTimer) Reset(d time.Duration) bool {
	t.mu.Lock()
	t.duration = d
	t.fired = false
	t.stopped = false
	t.mu.Unlock()
	return true
}
func (t *reconnectTestTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasStopped := t.stopped
	t.stopped = true
	return !wasStopped
}

// recvItem is one scripted Recv result.
type recvItem struct {
	f    wire.Frame
	err  error
	wait <-chan struct{}
}

type markedDatagramTransport struct{ wire.Transport }

func (markedDatagramTransport) DatagramTransport() {}

// scriptRecv wires a MockTransport's Recv to yield the given items in order,
// then block forever (simulating a live but idle connection). It returns a
// cleanup that unblocks the parked reader.
func scriptRecv(tr *mockClientConnection, items ...recvItem) func() {
	ch := make(chan recvItem, len(items))
	for _, it := range items {
		ch <- it
	}
	done := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (wire.Frame, error) {
		select {
		case it := <-ch:
			if it.wait != nil {
				select {
				case <-it.wait:
				case <-done:
					return wire.Frame{}, io.EOF
				}
			}
			return it.f, it.err
		case <-done:
			return wire.Frame{}, io.EOF
		}
	}).Maybe()
	return func() { close(done) }
}

func frameOf(t wire.MsgType, payload []byte) wire.Frame {
	return wire.Frame{Type: t, Payload: payload}
}

// blockingReader blocks on Read until closed, then returns EOF. Stands in
// for a terminal's stdin that produces nothing during the test.
type blockingReader struct{ ch chan struct{} }

func newBlockingReader() *blockingReader { return &blockingReader{ch: make(chan struct{})} }

func (b *blockingReader) Read(p []byte) (int, error) {
	<-b.ch
	return 0, io.EOF
}
func (b *blockingReader) unblock() { close(b.ch) }

type oneShotBlockingReader struct {
	data []byte
	done chan struct{}
	once sync.Once
}

func newOneShotBlockingReader(data []byte) *oneShotBlockingReader {
	return &oneShotBlockingReader{data: data, done: make(chan struct{})}
}

func (r *oneShotBlockingReader) Read(p []byte) (int, error) {
	read := false
	r.once.Do(func() {
		copy(p, r.data)
		read = true
	})
	if read {
		return len(r.data), nil
	}
	<-r.done
	return 0, io.EOF
}

func (r *oneShotBlockingReader) unblock() { close(r.done) }

type chunkedBlockingReader struct {
	chunks [][]byte
	done   chan struct{}
	mu     sync.Mutex
	next   int
}

func newChunkedBlockingReader(chunks ...[]byte) *chunkedBlockingReader {
	return &chunkedBlockingReader{chunks: chunks, done: make(chan struct{})}
}

func (r *chunkedBlockingReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.next < len(r.chunks) {
		chunk := r.chunks[r.next]
		r.next++
		return copy(p, chunk), nil
	}
	<-r.done
	return 0, io.EOF
}

func (r *chunkedBlockingReader) unblock() { close(r.done) }

func newHappyTerminal(t *testing.T, out *bytes.Buffer, restoreCount *atomic.Int32, resizeCh chan domain.Geometry) (*portsmocks.MockTerminal, *blockingReader) {
	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error {
		restoreCount.Add(1)
		return nil
	}, nil).Once()
	in := newBlockingReader()
	tm.EXPECT().In().Return(in).Maybe()
	tm.EXPECT().Out().Return(out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()
	return tm, in
}

func isType(typ wire.MsgType) any {
	return mock.MatchedBy(func(f wire.Frame) bool { return f.Type == typ })
}

// transportDialer adapts one already-open test transport to the Runner API.
type transportDialer struct{ transport wire.Transport }

func (d transportDialer) Dial(context.Context) (ports.ClientConnection, error) {
	return &rawClientConnection{raw: d.transport}, nil
}

func runTestClient(ctx context.Context, deps client.Dependencies, request client.AttachRequest) error {
	return client.NewRunner(deps).Run(ctx, request)
}

func testDependencies(dialer ports.ClientDialer, terminal ports.Terminal, clock ports.Clock, clipboard ports.ClipboardReader, observer ports.SerializedRuntimeObserver) client.Dependencies {
	return client.Dependencies{
		Dialer:                 dialer,
		Terminal:               terminal,
		Clock:                  clock,
		DisableCapabilityProbe: true,
		Clipboard:              clipboard,
		Logger:                 slog.New(slog.DiscardHandler),
		RuntimeObserver:        observer,
	}
}

func attachTestDependencies(transport wire.Transport, terminal ports.Terminal, clock ports.Clock) client.Dependencies {
	return testDependencies(transportDialer{transport: transport}, terminal, clock, nil, nil)
}

const testPreWelcomeTimeout = protocol.HandshakeTimeout

func newHandshakeClock(t *testing.T, timerCount int) (*portsmocks.MockClock, <-chan chan time.Time) {
	t.Helper()
	created := make(chan chan time.Time, timerCount)
	clk := portsmocks.NewMockClock(t)
	clk.EXPECT().NewTimer(testPreWelcomeTimeout).RunAndReturn(func(time.Duration) ports.Timer {
		timerC := make(chan time.Time, 1)
		timer := portsmocks.NewMockTimer(t)
		timer.EXPECT().C().Return((<-chan time.Time)(timerC)).Maybe()
		timer.EXPECT().Stop().Return(true).Once()
		created <- timerC
		return timer
	}).Times(timerCount)
	return clk, created
}

func TestRunBoundsBlockedDial(t *testing.T) {
	clk, createdTimers := newHandshakeClock(t, 1)
	started := make(chan struct{})
	dialer := newMockClientDialer(t)
	dialer.EXPECT().Dial(mock.Anything).RunAndReturn(func(ctx context.Context) (wire.Transport, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}).Once()

	errCh := make(chan error, 1)
	go func() {
		errCh <- runTestClient(context.Background(), testDependencies(dialer, nil, clk, nil, nil), client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "main"})
	}()
	timerC := <-createdTimers
	<-started
	timerC <- time.Time{}
	require.ErrorIs(t, <-errCh, context.DeadlineExceeded)
}

func TestRunBoundsPreWelcomeOperations(t *testing.T) {
	tests := []struct {
		name  string
		phase string
		end   string
		want  error
	}{
		{name: "cancel while sending hello", phase: "send", end: "cancel", want: context.Canceled},
		{name: "timeout while sending hello", phase: "send", end: "timeout", want: context.DeadlineExceeded},
		{name: "cancel while receiving welcome", phase: "recv", end: "cancel", want: context.Canceled},
		{name: "timeout while receiving welcome", phase: "recv", end: "timeout", want: context.DeadlineExceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			clk, createdTimers := newHandshakeClock(t, 1)

			term := portsmocks.NewMockTerminal(t)
			term.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Once()
			// No EnterRaw expectation: it must not run before Welcome.

			started := make(chan struct{})
			closed := make(chan struct{})
			var closeOnce sync.Once
			operationExited := make(chan struct{})
			tr := newMockClientConnection(t)
			tr.EXPECT().Close().Run(func() { closeOnce.Do(func() { close(closed) }) }).Return(nil).Maybe()

			block := func() error {
				close(started)
				<-closed
				close(operationExited)
				return io.ErrClosedPipe
			}
			if tt.phase == "send" {
				tr.EXPECT().Send(isType(wire.MsgHello)).RunAndReturn(func(wire.Frame) error {
					return block()
				}).Once()
			} else {
				tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
				tr.EXPECT().Recv().RunAndReturn(func() (wire.Frame, error) {
					return wire.Frame{}, block()
				}).Once()
			}
			dialer := newMockClientDialer(t)
			dialer.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()

			errCh := make(chan error, 1)
			go func() {
				errCh <- runTestClient(ctx, testDependencies(dialer, term, clk, nil, nil), client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "main", Remote: false})
			}()

			var timerC chan time.Time
			select {
			case timerC = <-createdTimers:
			case <-time.After(time.Second):
				t.Fatal("handshake timer was not created")
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("handshake operation did not start")
			}
			if tt.end == "cancel" {
				cancel()
			} else {
				timerC <- time.Time{}
			}

			select {
			case err := <-errCh:
				require.ErrorIs(t, err, tt.want)
			case <-time.After(time.Second):
				t.Fatal("Run did not return after handshake cancellation")
			}
			select {
			case <-operationExited:
			case <-time.After(time.Second):
				t.Fatal("handshake operation leaked after Run returned")
			}
			tr.AssertNotCalled(t, "Send", isType(wire.MsgTheme))
		})
	}
}

func TestAttachHelloIncludesTrueColor(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("COLORTERM", "truecolor")

	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	gotHello := make(chan protocol.Hello, 1)
	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).RunAndReturn(func(f wire.Frame) error {
		hello, err := wire.UnmarshalHello(f.Payload)
		require.NoError(t, err)
		gotHello <- hello
		return nil
	}).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: protocol.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)

	select {
	case hello := <-gotHello:
		require.Equal(t, "xterm-256color", hello.TermEnv)
		require.True(t, hello.TrueColor)
		require.Equal(t, uint8(8), hello.MaxOutputInFlight)
	case <-time.After(2 * time.Second):
		t.Fatal("hello frame was not sent")
	}
}

func TestAttachPublishesCommittedRouteSnapshot(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	lifecycle := domain.SessionLifecycleID{9, 8, 7}
	tr := &recordingTransport{recvs: []recvItem{
		{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{
			SessionID:   "s1",
			SessionName: "main",
			CommittedIdentity: &protocol.CommittedRouteIdentity{
				Target: protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "main"},
			},
		}))},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}

	err := runTestClient(context.Background(), attachTestDependencies(tr, term, realClock{}), client.AttachRequest{
		Intent:      protocol.IntentAttach,
		SessionName: "main",
		Origin:      protocol.RouteOriginLocal,
		OriginKey:   "local",
	})
	require.NoError(t, err)

	var snapshot protocol.RecentRouteSnapshot
	var subscription protocol.RouteAttentionSubscription
	foundSnapshot := false
	foundSubscription := false
	for _, frame := range tr.Sends() {
		switch frame.Type {
		case wire.MsgRecentRouteSnapshot:
			var err error
			snapshot, err = wire.UnmarshalRecentRouteSnapshot(frame.Payload)
			require.NoError(t, err)
			foundSnapshot = true
		case wire.MsgRouteAttentionSubscription:
			var err error
			subscription, err = wire.UnmarshalRouteAttentionSubscription(frame.Payload)
			require.NoError(t, err)
			foundSubscription = true
		}
	}
	require.True(t, foundSnapshot, "successful Welcome must publish a route snapshot")
	require.True(t, foundSubscription, "successful Welcome must publish a route attention subscription")
	require.Empty(t, subscription.Targets, "the active route itself needs no status subscription")
	require.NotZero(t, snapshot.Generation)
	require.NotZero(t, snapshot.Active.Key)
	require.Empty(t, snapshot.Entries, "the active route is metadata-only")
}

func TestLocalOnlyHandoffBetweenLocalSessionsKeepsClientRunning(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()

	firstLifecycle := domain.SessionLifecycleID{1}
	secondLifecycle := domain.SessionLifecycleID{2}
	welcome := func(name string, lifecycle domain.SessionLifecycleID) wire.Frame {
		return frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{
			SessionID: name, SessionName: name, ResumeToken: 1, Capabilities: protocol.CapabilityResume,
			CommittedIdentity: &protocol.CommittedRouteIdentity{
				Target: protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: name},
			},
		}))
	}
	secondTarget := protocol.ExactSessionTarget{LifecycleID: secondLifecycle, SessionName: "second"}
	first := &recordingTransport{recvs: []recvItem{
		{f: welcome("first", firstLifecycle)},
		{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
			Session: "second", Intent: protocol.IntentAttach, ExactTarget: &secondTarget,
			EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, SamePeer: true,
		}))},
		{f: frameOf(wire.MsgCommittedRouteIdentity, mustMarshalCommittedIdentity(protocol.CommittedRouteIdentity{Target: secondTarget}))},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	dialer := &sequenceDialer{trs: []wire.Transport{first}}

	err := runTestClient(context.Background(), testDependencies(dialer, term, realClock{}, nil, nil), client.AttachRequest{
		Intent: protocol.IntentAttach, SessionName: "first",
		Origin: protocol.RouteOriginLocal, OriginKey: "local",
		EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
	})

	require.NoError(t, err)
	require.Equal(t, int32(1), dialer.calls.Load(), "same-peer switching must not dial a replacement transport")
}

func TestAttachHelloPreservesCompleteAttachRequest(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	lifecycle := domain.SessionLifecycleID{1, 2, 3}
	target := &domain.RemoteSessionTarget{
		Endpoint: "remote.example", DisplayOrigin: "remote.example", LifecycleID: lifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}
	request := client.AttachRequest{
		Intent: protocol.IntentAttach, SessionName: "work", Remote: true,
		RemoteTarget: target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		NavigationCapabilities: protocol.NavigationCapabilityHomePicker,
		StartupOverlay:         protocol.StartupOverlayNone,
	}
	tr := &recordingTransport{recvs: []recvItem{
		{f: welcomeFrame(0)},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}

	require.NoError(t, runTestClient(context.Background(), attachTestDependencies(tr, term, realClock{}), request))
	hello := helloFromSend(t, tr)
	require.Equal(t, request.Intent, hello.Intent)
	require.Equal(t, request.SessionName, hello.Name)
	require.Equal(t, target, hello.RemoteTarget)
	require.Equal(t, request.EnvironmentPolicy, hello.EnvironmentPolicy)
	require.Equal(t, request.NavigationCapabilities, hello.NavigationCapabilities)
	require.Equal(t, request.StartupOverlay, hello.StartupOverlay)
}

func TestStoppedLocalHandoffDialsReplacementTransport(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()

	sourceLifecycle := domain.SessionLifecycleID{1}
	targetLifecycle := domain.SessionLifecycleID{2}
	target := protocol.ExactSessionTarget{LifecycleID: targetLifecycle, SessionName: "stopped"}
	welcome := func(name string, lifecycle domain.SessionLifecycleID) wire.Frame {
		return frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{
			SessionID: name, SessionName: name, ResumeToken: 1, Capabilities: protocol.CapabilityResume,
			CommittedIdentity: &protocol.CommittedRouteIdentity{
				Target: protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: name},
			},
		}))
	}
	first := &recordingTransport{recvs: []recvItem{
		{f: welcome("source", sourceLifecycle)},
		{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
			Session: "stopped", Intent: protocol.IntentAttach, ExactTarget: &target,
			EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		}))},
	}}
	second := &recordingTransport{recvs: []recvItem{
		{f: welcome("stopped", targetLifecycle)},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	dialer := &sequenceDialer{trs: []wire.Transport{first, second}}

	err := runTestClient(context.Background(), testDependencies(dialer, term, realClock{}, nil, nil), client.AttachRequest{
		Intent: protocol.IntentAttach, SessionName: "source",
		Origin: protocol.RouteOriginLocal, OriginKey: "local",
		EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
	})

	require.NoError(t, err)
	require.Equal(t, int32(2), dialer.calls.Load())
	require.Equal(t, &target, helloFromSend(t, second).ExactTarget)
}

func TestAcceptedHomeActionUsesCapturedRouteAfterRequestMetadataRebase(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	localLifecycle := domain.SessionLifecycleID{1}
	remoteLifecycle := domain.SessionLifecycleID{2}
	welcome := func(name string, lifecycle domain.SessionLifecycleID) wire.Frame {
		return frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{
			SessionID: name, SessionName: name, ResumeToken: 1, Capabilities: protocol.CapabilityResume,
			CommittedIdentity: &protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: name}},
		}))
	}
	remoteTarget := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: remoteLifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}
	local1 := &recordingTransport{recvs: []recvItem{
		{f: welcome("local", localLifecycle)},
		{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
			Endpoint: "remote", Session: "work", Intent: protocol.IntentAttach,
			RemoteTarget: &remoteTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		}))},
	}}
	remote := &recordingTransport{recvs: []recvItem{
		{f: welcome("work", remoteLifecycle)},
		{f: navigationDirectiveFrame(protocol.NavigationOpenHomePicker)},
	}}
	local2 := &recordingTransport{recvs: []recvItem{
		{f: welcome("local", localLifecycle)},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	localDialer := &sequenceDialer{trs: []wire.Transport{local1, local2}}
	remoteDialer := &sequenceDialer{trs: []wire.Transport{remote}}
	deps := testDependencies(localDialer, term, realClock{}, nil, nil)
	deps.AttachHandoff = func(protocol.AttachTarget) (ports.ClientDialer, client.AttachRequest, error) {
		target := protocol.ExactSessionTarget{LifecycleID: remoteLifecycle, SessionName: "work"}
		return remoteDialer, client.AttachRequest{
			Intent: protocol.IntentAttach, SessionName: "work", Remote: true,
			Origin: protocol.RouteOriginRemote, OriginKey: "remote", ExactTarget: &target,
			EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
			// Deliberately omit the copied Home capability. The accepted server
			// action remains valid because Runner retained the concrete home route.
		}, nil
	}

	require.NoError(t, runTestClient(context.Background(), deps, client.AttachRequest{
		Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local",
	}))
	require.Equal(t, protocol.StartupOverlaySessionPicker, helloFromSend(t, local2).StartupOverlay)
}

func hybridWelcomeFrame(name string, lifecycle domain.SessionLifecycleID) wire.Frame {
	return frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{
		SessionID: name, SessionName: name, ResumeToken: 1, Capabilities: protocol.CapabilityResume,
		CommittedIdentity: &protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: name}},
	}))
}

func hybridLocalBootstrap(lifecycle domain.SessionLifecycleID, target domain.RemoteSessionTarget) *recordingTransport {
	return &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("local", lifecycle)},
		{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
			Endpoint: target.Endpoint, Session: target.SessionName, Intent: protocol.IntentAttach,
			RemoteTarget: &target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		}))},
	}}
}

func hybridPickerDependencies(localDialer ports.ClientDialer, term ports.Terminal, clock ports.Clock, remoteByEndpoint map[string]ports.ClientDialer) client.Dependencies {
	deps := testDependencies(localDialer, term, clock, nil, nil)
	deps.AttachHandoff = func(target protocol.AttachTarget) (ports.ClientDialer, client.AttachRequest, error) {
		if target.RemoteTarget == nil {
			return nil, client.AttachRequest{}, errors.New("hybrid picker target is not remote")
		}
		dialer := remoteByEndpoint[target.Endpoint]
		if dialer == nil {
			return nil, client.AttachRequest{}, errors.New("hybrid picker endpoint has no dialer")
		}
		selection := *target.RemoteTarget
		return dialer, client.AttachRequest{
			Intent: protocol.IntentAttach, SessionName: target.Session, Remote: true,
			Origin: protocol.RouteOriginDiscovery, OriginKey: target.Endpoint,
			RemoteTarget: &selection, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		}, nil
	}
	return deps
}

func hybridParkedRequestHandler(requests chan<- protocol.ParkedRouteRequest, requestErrors chan<- error, signals map[protocol.ParkedRouteAction]chan struct{}) func(wire.Frame) {
	once := make(map[protocol.ParkedRouteAction]*sync.Once, len(signals))
	for action := range signals {
		once[action] = &sync.Once{}
	}
	return func(frame wire.Frame) {
		if frame.Type != wire.MsgParkedRouteRequest {
			return
		}
		request, err := wire.UnmarshalParkedRouteRequest(frame.Payload)
		if err != nil {
			if requestErrors != nil {
				requestErrors <- err
			}
			return
		}
		if requests != nil {
			requests <- request
		}
		if signal := signals[request.Action]; signal != nil {
			once[request.Action].Do(func() { close(signal) })
		}
	}
}

func TestHybridPickerSameHostSwitchReusesRemoteTransport(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()

	localLifecycle := domain.SessionLifecycleID{1}
	remoteSourceLifecycle := domain.SessionLifecycleID{2}
	remoteTargetLifecycle := domain.SessionLifecycleID{3}
	sourceTarget := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: remoteSourceLifecycle,
		SessionName: "source", LiveTabID: "source-tab",
	}
	targetTarget := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: remoteTargetLifecycle,
		SessionName: "target", LiveTabID: "target-tab",
	}
	localInitial := hybridLocalBootstrap(localLifecycle, sourceTarget)
	parkSent := make(chan struct{})
	switchSent := make(chan struct{})
	parkedRequests := make(chan protocol.ParkedRouteRequest, 2)
	parkedRequestErrors := make(chan error, 2)
	remote := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("source", remoteSourceLifecycle)},
		{f: navigationDirectiveFrame(protocol.NavigationOpenHomePicker)},
		{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 1, Status: protocol.ParkedRouteReady})), wait: parkSent},
		{f: frameOf(wire.MsgCommittedRouteIdentity, mustMarshalCommittedIdentity(protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: remoteTargetLifecycle, SessionName: "target"}})), wait: switchSent},
		{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 2, Status: protocol.ParkedRouteSwitched}))},
		{f: frameOf(wire.MsgOutput, mustMarshalOutput(protocol.Output{Epoch: 2, New: 1, Size: domain.Size{Cols: 80, Rows: 24}, Full: true, Data: []byte("target frame")}))},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	remote.onSend = hybridParkedRequestHandler(parkedRequests, parkedRequestErrors, map[protocol.ParkedRouteAction]chan struct{}{
		protocol.ParkedRoutePrepare: parkSent,
		protocol.ParkedRouteSwitch:  switchSent,
	})
	localPicker := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("local", localLifecycle)},
		{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
			Endpoint: "remote", Session: "target", Intent: protocol.IntentAttach,
			RemoteTarget: &targetTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		}))},
	}}
	var resizeOnce sync.Once
	localPicker.onSend = func(frame wire.Frame) {
		if frame.Type == wire.MsgHello {
			resizeOnce.Do(func() { term.setSize(domain.Size{Cols: 100, Rows: 40}) })
		}
	}
	localDialer := &sequenceDialer{trs: []wire.Transport{localInitial, localPicker}}
	remoteDialer := &sequenceDialer{trs: []wire.Transport{markedDatagramTransport{Transport: remote}}}
	deps := hybridPickerDependencies(localDialer, term, realClock{}, map[string]ports.ClientDialer{"remote": remoteDialer})

	err := runTestClient(context.Background(), deps, client.AttachRequest{
		Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local",
	})

	require.NoError(t, err)
	select {
	case requestErr := <-parkedRequestErrors:
		require.NoError(t, requestErr)
	default:
	}
	prepareRequest := <-parkedRequests
	switchRequest := <-parkedRequests
	require.Equal(t, protocol.ParkedRoutePrepare, prepareRequest.Action)
	require.Equal(t, protocol.ParkedRouteSwitch, switchRequest.Action)
	require.NotNil(t, switchRequest.Target)
	require.Equal(t, targetTarget, *switchRequest.Target)
	frames := remote.Sends()
	switchIndex := -1
	for i, frame := range frames {
		if frame.Type != wire.MsgParkedRouteRequest {
			continue
		}
		request, requestErr := wire.UnmarshalParkedRouteRequest(frame.Payload)
		require.NoError(t, requestErr)
		if request.Action == protocol.ParkedRouteSwitch {
			switchIndex = i
			break
		}
	}
	require.Positive(t, switchIndex)
	require.Equal(t, wire.MsgResize, frames[switchIndex-1].Type, "current size must precede the parked switch")
	resize, resizeErr := wire.UnmarshalResize(frames[switchIndex-1].Payload)
	require.NoError(t, resizeErr)
	require.Equal(t, domain.Size{Cols: 100, Rows: 40}, resize.Size)
	require.Equal(t, int32(1), remoteDialer.calls.Load(), "same-host picker switch must retain the authenticated remote transport")
}

func TestHybridPickerLocalSelectionCommitsLocalRoute(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()

	localHomeLifecycle := domain.SessionLifecycleID{1}
	localMiscLifecycle := domain.SessionLifecycleID{2}
	remoteSourceLifecycle := domain.SessionLifecycleID{3}
	remoteMiscLifecycle := domain.SessionLifecycleID{4}
	remoteVev := protocol.ExactSessionTarget{LifecycleID: remoteSourceLifecycle, SessionName: "vev"}
	remoteMisc := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "arch", LifecycleID: remoteMiscLifecycle,
		SessionName: "misc", LiveTabID: "misc-tab",
	}
	localInitial := hybridLocalBootstrap(localHomeLifecycle, remoteMisc)
	switchSent := make(chan struct{})
	prepareSent := make(chan struct{})
	resumeOrClosed := make(chan struct{})
	remoteClosed := make(chan struct{})
	var remoteCloseOnce sync.Once
	var resumeOrCloseOnce sync.Once
	remote := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("misc", remoteMiscLifecycle)},
		{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
			Session: "vev", Intent: protocol.IntentAttach,
			ExactTarget:       &remoteVev,
			EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, SamePeer: true,
		}))},
		{f: frameOf(wire.MsgCommittedRouteIdentity, mustMarshalCommittedIdentity(protocol.CommittedRouteIdentity{
			Target: remoteVev,
		})), wait: switchSent},
		{f: navigationDirectiveFrame(protocol.NavigationOpenHomePicker)},
		{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 1, Status: protocol.ParkedRouteReady})), wait: prepareSent},
		{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 2, Status: protocol.ParkedRouteResumed})), wait: resumeOrClosed},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}, stall: remoteClosed}
	remote.onClose = func() {
		remoteCloseOnce.Do(func() { close(remoteClosed) })
		resumeOrCloseOnce.Do(func() { close(resumeOrClosed) })
	}
	parkedHandler := hybridParkedRequestHandler(nil, nil, map[protocol.ParkedRouteAction]chan struct{}{
		protocol.ParkedRoutePrepare: prepareSent,
	})
	var switchOnce sync.Once
	remote.onSend = func(frame wire.Frame) {
		parkedHandler(frame)
		if frame.Type == wire.MsgSamePeerSwitchRequest {
			switchOnce.Do(func() { close(switchSent) })
		}
		if frame.Type != wire.MsgParkedRouteRequest {
			return
		}
		request, err := wire.UnmarshalParkedRouteRequest(frame.Payload)
		if err == nil && request.Action == protocol.ParkedRouteResume {
			resumeOrCloseOnce.Do(func() { close(resumeOrClosed) })
		}
	}
	localTarget := protocol.ExactSessionTarget{LifecycleID: localMiscLifecycle, SessionName: "misc"}
	localPickerClosed := make(chan struct{})
	var localPickerCloseOnce sync.Once
	localPicker := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("local", localHomeLifecycle)},
		{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
			Session: "misc", Intent: protocol.IntentAttach, ExactTarget: &localTarget,
			EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, SamePeer: true,
		}))},
	}, stall: localPickerClosed}
	localPicker.onClose = func() { localPickerCloseOnce.Do(func() { close(localPickerClosed) }) }
	localMisc := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("misc", localMiscLifecycle)},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	localDialer := &sequenceDialer{trs: []wire.Transport{localInitial, localPicker, localMisc}}
	remoteDialer := &sequenceDialer{trs: []wire.Transport{markedDatagramTransport{Transport: remote}}}
	deps := hybridPickerDependencies(localDialer, term, realClock{}, map[string]ports.ClientDialer{"remote": remoteDialer})

	err := runTestClient(t.Context(), deps, client.AttachRequest{
		Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local",
	})
	require.NoError(t, err)
	require.Equal(t, int32(3), localDialer.calls.Load())
	require.Equal(t, int32(1), remoteDialer.calls.Load())
	var remoteSwitches []protocol.SamePeerSwitchRequest
	for _, sent := range remote.Sends() {
		if sent.Type != wire.MsgSamePeerSwitchRequest {
			continue
		}
		switchRequest, err := wire.UnmarshalSamePeerSwitchRequest(sent.Payload)
		require.NoError(t, err)
		remoteSwitches = append(remoteSwitches, switchRequest)
	}
	require.Len(t, remoteSwitches, 1)
	require.Equal(t, remoteVev, remoteSwitches[0].Target, "the local misc target must never be switched on the remote peer")

	var snapshot protocol.RecentRouteSnapshot
	for _, sent := range localMisc.Sends() {
		if sent.Type != wire.MsgRecentRouteSnapshot {
			continue
		}
		var err error
		snapshot, err = wire.UnmarshalRecentRouteSnapshot(sent.Payload)
		require.NoError(t, err)
	}
	require.Equal(t, localTarget, snapshot.ActiveEntry.Target)
	require.Equal(t, protocol.RouteKindLocal, snapshot.ActiveEntry.Kind)
	require.Empty(t, snapshot.ActiveEntry.HostLabel)
	remoteMiscFound := false
	for _, entry := range snapshot.Entries {
		if entry.Target != (protocol.ExactSessionTarget{LifecycleID: remoteMiscLifecycle, SessionName: "misc"}) {
			continue
		}
		remoteMiscFound = true
		require.Equal(t, "arch", entry.HostLabel)
		require.Equal(t, protocol.RouteKindRemote, entry.Kind)
	}
	require.True(t, remoteMiscFound, "the distinct remote misc route must remain qualified in history")
}

func TestHybridPickerExpiredSwitchFallsBackToNewDial(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()

	localLifecycle := domain.SessionLifecycleID{1}
	sourceLifecycle := domain.SessionLifecycleID{2}
	targetLifecycle := domain.SessionLifecycleID{3}
	sourceTarget := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: sourceLifecycle,
		SessionName: "source", LiveTabID: "source-tab",
	}
	targetTarget := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: targetLifecycle,
		SessionName: "target", LiveTabID: "target-tab",
	}
	localInitial := hybridLocalBootstrap(localLifecycle, sourceTarget)
	prepareSent := make(chan struct{})
	switchSent := make(chan struct{})
	parkedRequestErrors := make(chan error, 1)
	sourceRemote := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("source", sourceLifecycle)},
		{f: navigationDirectiveFrame(protocol.NavigationOpenHomePicker)},
		{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 1, Status: protocol.ParkedRouteReady})), wait: prepareSent},
		{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 2, Status: protocol.ParkedRouteExpired})), wait: switchSent},
	}}
	sourceRemote.onSend = hybridParkedRequestHandler(nil, parkedRequestErrors, map[protocol.ParkedRouteAction]chan struct{}{
		protocol.ParkedRoutePrepare: prepareSent,
		protocol.ParkedRouteSwitch:  switchSent,
	})
	localPicker := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("local", localLifecycle)},
		{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
			Endpoint: "remote", Session: "target", Intent: protocol.IntentAttach,
			RemoteTarget: &targetTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		}))},
	}}
	targetRemote := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("target", targetLifecycle)},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	localDialer := &sequenceDialer{trs: []wire.Transport{localInitial, localPicker}}
	remoteDialer := &sequenceDialer{trs: []wire.Transport{
		markedDatagramTransport{Transport: sourceRemote}, targetRemote,
	}}
	deps := hybridPickerDependencies(localDialer, term, realClock{}, map[string]ports.ClientDialer{"remote": remoteDialer})

	require.NoError(t, runTestClient(context.Background(), deps, client.AttachRequest{
		Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local",
	}))
	select {
	case requestErr := <-parkedRequestErrors:
		require.NoError(t, requestErr)
	default:
	}
	require.Equal(t, int32(2), remoteDialer.calls.Load(), "an expired parked lease must use the traditional dial path")
}

func TestHybridPickerPrepareResponseTimeoutClosesRetainedTransport(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	prepareSent := make(chan struct{})
	type deadlineTimer struct {
		ch      chan time.Time
		stopped atomic.Bool
	}
	var deadlineMu sync.Mutex
	var deadlineRecords []*deadlineTimer
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().Now().Return(time.Time{}).Maybe()
	clock.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(duration time.Duration) ports.Timer {
		timer := portsmocks.NewMockTimer(t)
		ch := make(chan time.Time, 1)
		timer.EXPECT().C().Return(ch).Maybe()
		timer.EXPECT().Reset(mock.Anything).Return(true).Maybe()
		if duration == protocol.HandshakeTimeout {
			record := &deadlineTimer{ch: ch}
			timer.EXPECT().Stop().Run(func() { record.stopped.Store(true) }).Return(true).Maybe()
			deadlineMu.Lock()
			deadlineRecords = append(deadlineRecords, record)
			deadlineMu.Unlock()
		} else {
			timer.EXPECT().Stop().Return(true).Maybe()
		}
		return timer
	}).Maybe()

	localLifecycle := domain.SessionLifecycleID{1}
	remoteLifecycle := domain.SessionLifecycleID{2}
	remoteTarget := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: remoteLifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}
	localInitial := hybridLocalBootstrap(localLifecycle, remoteTarget)
	remoteClosed := make(chan struct{})
	var closeOnce sync.Once
	remote := &recordingTransport{
		recvs: []recvItem{
			{f: hybridWelcomeFrame("work", remoteLifecycle)},
			{f: navigationDirectiveFrame(protocol.NavigationOpenHomePicker)},
		},
		stall: remoteClosed,
	}
	remote.onSend = hybridParkedRequestHandler(nil, nil, map[protocol.ParkedRouteAction]chan struct{}{
		protocol.ParkedRoutePrepare: prepareSent,
	})

	localDialer := &sequenceDialer{trs: []wire.Transport{localInitial}}
	remoteDialer := &sequenceDialer{trs: []wire.Transport{markedDatagramTransport{Transport: remote}}}
	deps := hybridPickerDependencies(localDialer, term, clock, map[string]ports.ClientDialer{"remote": remoteDialer})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	remote.onClose = func() {
		closeOnce.Do(func() { close(remoteClosed) })
		cancel()
	}
	result := make(chan error, 1)
	go func() {
		result <- runTestClient(ctx, deps, client.AttachRequest{
			Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local",
		})
	}()
	select {
	case <-prepareSent:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for parked Prepare request")
	}
	var deadline chan time.Time
	require.Eventually(t, func() bool {
		deadlineMu.Lock()
		defer deadlineMu.Unlock()
		var active []*deadlineTimer
		for _, record := range deadlineRecords {
			if !record.stopped.Load() {
				active = append(active, record)
			}
		}
		if len(active) != 1 {
			return false
		}
		deadline = active[0].ch
		return true
	}, time.Second, time.Millisecond, "expected exactly one active parked-response deadline")
	deadline <- time.Time{}
	select {
	case err := <-result:
		require.ErrorContains(t, err, "timed out waiting for parked-route response")
	case <-time.After(time.Second):
		t.Fatal("client did not leave the parked request after its deadline")
	}
	require.Positive(t, remote.closed.Load())
}

func TestHybridPickerBackResumesRetainedRemoteTransport(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()

	localLifecycle := domain.SessionLifecycleID{1}
	remoteLifecycle := domain.SessionLifecycleID{2}
	remoteTarget := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: remoteLifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}
	localInitial := hybridLocalBootstrap(localLifecycle, remoteTarget)
	parkSent := make(chan struct{})
	resumeSent := make(chan struct{})
	parkedRequestErrors := make(chan error, 2)
	remote := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("work", remoteLifecycle)},
		{f: navigationDirectiveFrame(protocol.NavigationOpenHomePicker)},
		{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 1, Status: protocol.ParkedRouteReady})), wait: parkSent},
		{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 2, Status: protocol.ParkedRouteResumed})), wait: resumeSent},
		{f: frameOf(wire.MsgOutput, mustMarshalOutput(protocol.Output{Epoch: 1, New: 1, Size: domain.Size{Cols: 80, Rows: 24}, Full: true, Data: []byte("source frame")}))},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	remote.onSend = hybridParkedRequestHandler(nil, parkedRequestErrors, map[protocol.ParkedRouteAction]chan struct{}{
		protocol.ParkedRoutePrepare: parkSent,
		protocol.ParkedRouteResume:  resumeSent,
	})
	localPicker := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("local", localLifecycle)},
		{f: navigationDirectiveFrame(protocol.NavigationBack)},
	}}
	localDialer := &sequenceDialer{trs: []wire.Transport{localInitial, localPicker}}
	remoteDialer := &sequenceDialer{trs: []wire.Transport{markedDatagramTransport{Transport: remote}}}
	deps := hybridPickerDependencies(localDialer, term, realClock{}, map[string]ports.ClientDialer{"remote": remoteDialer})

	require.NoError(t, runTestClient(context.Background(), deps, client.AttachRequest{
		Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local",
	}))
	select {
	case requestErr := <-parkedRequestErrors:
		require.NoError(t, requestErr)
	default:
	}
	require.Equal(t, int32(1), remoteDialer.calls.Load())
}

func TestHybridPickerDifferentHostFallsBackToNewRemoteDial(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()

	localLifecycle := domain.SessionLifecycleID{1}
	sourceLifecycle := domain.SessionLifecycleID{2}
	targetLifecycle := domain.SessionLifecycleID{3}
	sourceTarget := domain.RemoteSessionTarget{
		Endpoint: "source-host", DisplayOrigin: "source-host", LifecycleID: sourceLifecycle,
		SessionName: "source", LiveTabID: "source-tab",
	}
	targetTarget := domain.RemoteSessionTarget{
		Endpoint: "target-host", DisplayOrigin: "target-host", LifecycleID: targetLifecycle,
		SessionName: "target", LiveTabID: "target-tab",
	}
	localInitial := hybridLocalBootstrap(localLifecycle, sourceTarget)
	parkSent := make(chan struct{})
	keepSourceOpen := make(chan struct{})
	parkedRequestErrors := make(chan error, 1)
	defer close(keepSourceOpen)
	sourceRemote := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("source", sourceLifecycle)},
		{f: navigationDirectiveFrame(protocol.NavigationOpenHomePicker)},
		{f: frameOf(wire.MsgParkedRouteResponse, wire.MarshalParkedRouteResponse(protocol.ParkedRouteResponse{RequestID: 1, Status: protocol.ParkedRouteReady})), wait: parkSent},
		{err: io.EOF, wait: keepSourceOpen},
	}}
	sourceRemote.onSend = hybridParkedRequestHandler(nil, parkedRequestErrors, map[protocol.ParkedRouteAction]chan struct{}{
		protocol.ParkedRoutePrepare: parkSent,
	})
	localPicker := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("local", localLifecycle)},
		{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(protocol.AttachTarget{
			Endpoint: targetTarget.Endpoint, Session: targetTarget.SessionName, Intent: protocol.IntentAttach,
			RemoteTarget: &targetTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		}))},
	}}
	targetRemote := &recordingTransport{recvs: []recvItem{
		{f: hybridWelcomeFrame("target", targetLifecycle)},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	localDialer := &sequenceDialer{trs: []wire.Transport{localInitial, localPicker}}
	sourceDialer := &sequenceDialer{trs: []wire.Transport{markedDatagramTransport{Transport: sourceRemote}}}
	targetDialer := &sequenceDialer{trs: []wire.Transport{targetRemote}}
	deps := hybridPickerDependencies(localDialer, term, realClock{}, map[string]ports.ClientDialer{
		sourceTarget.Endpoint: sourceDialer,
		targetTarget.Endpoint: targetDialer,
	})

	require.NoError(t, runTestClient(context.Background(), deps, client.AttachRequest{
		Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local",
	}))
	select {
	case requestErr := <-parkedRequestErrors:
		require.NoError(t, requestErr)
	default:
	}
	require.Equal(t, int32(1), sourceDialer.calls.Load())
	require.Equal(t, int32(1), targetDialer.calls.Load())
	require.Positive(t, sourceRemote.closed.Load())
}

func TestRouteNavigationPreservesRemoteHomePickerAcrossLocalReturn(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()

	localLifecycle := domain.SessionLifecycleID{1, 1, 1}
	remoteLifecycle := domain.SessionLifecycleID{2, 2, 2}
	newLifecycle := domain.SessionLifecycleID{3, 3, 3}
	remoteNewLifecycle := domain.SessionLifecycleID{4, 4, 4}
	welcome := func(name string, lifecycle domain.SessionLifecycleID, token uint64) wire.Frame {
		return frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{
			SessionID:    name + "-id",
			SessionName:  name,
			ResumeToken:  token,
			Capabilities: protocol.CapabilityResume,
			CommittedIdentity: &protocol.CommittedRouteIdentity{
				Target: protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: name},
			},
		}))
	}
	remoteTarget := domain.RemoteSessionTarget{
		Endpoint:      "remote",
		DisplayOrigin: "remote",
		LifecycleID:   remoteLifecycle,
		SessionName:   "remote-manual",
		LiveTabID:     "tab-1",
	}
	remoteHandoff := protocol.AttachTarget{
		Endpoint: "remote", Session: "remote-manual", Intent: protocol.IntentAttach,
		RemoteTarget: &remoteTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}

	local1 := &recordingTransport{recvs: []recvItem{
		{f: welcome("test", localLifecycle, 11)},
		{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(remoteHandoff))},
	}}
	local2 := &recordingTransport{recvs: []recvItem{
		{f: welcome("test", localLifecycle, 11)},
		{f: frameOf(wire.MsgCommittedRouteIdentity, mustMarshalCommittedIdentity(protocol.CommittedRouteIdentity{
			Target: protocol.ExactSessionTarget{LifecycleID: newLifecycle, SessionName: "new"},
		}))},
		{f: frameOf(wire.MsgNavigateRecentRoute, mustMarshalRouteAction(protocol.RouteNavigationAction{SnapshotGeneration: 4, Key: 2, Generation: 2}))},
	}}
	local3 := &recordingTransport{recvs: []recvItem{
		{f: welcome("test", localLifecycle, 11)},
		{f: navigationDirectiveFrame(protocol.NavigationBack)},
	}}
	remote1 := &recordingTransport{recvs: []recvItem{
		{f: welcome("remote-manual", remoteLifecycle, 22)},
		{f: frameOf(wire.MsgNavigateRecentRoute, mustMarshalRouteAction(protocol.RouteNavigationAction{SnapshotGeneration: 2, Key: 1, Generation: 1}))},
	}}
	remote2 := &recordingTransport{recvs: []recvItem{
		{f: welcome("remote-manual", remoteLifecycle, 22)},
		{f: frameOf(wire.MsgCommittedRouteIdentity, mustMarshalCommittedIdentity(protocol.CommittedRouteIdentity{
			Target: protocol.ExactSessionTarget{LifecycleID: remoteNewLifecycle, SessionName: "remote-new"},
		}))},
		{f: frameOf(wire.MsgRoutePosition, mustMarshalRoutePosition(protocol.RoutePosition{
			Target: protocol.ExactSessionTarget{LifecycleID: remoteNewLifecycle, SessionName: "remote-new"}, ActiveTabID: "tab-2",
		}))},
		{f: navigationDirectiveFrame(protocol.NavigationOpenHomePicker)},
	}}
	remote3 := &recordingTransport{recvs: []recvItem{
		{f: welcome("remote-new", remoteNewLifecycle, 22)},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	localDialer := &sequenceDialer{trs: []wire.Transport{local1, local2, local3}}
	remoteDialer := &sequenceDialer{trs: []wire.Transport{remote1, remote2, remote3}}

	deps := testDependencies(localDialer, term, realClock{}, nil, nil)
	deps.AttachHandoff = func(target protocol.AttachTarget) (ports.ClientDialer, client.AttachRequest, error) {
		require.Equal(t, remoteHandoff, target)
		return remoteDialer, client.AttachRequest{
			Intent: protocol.IntentAttach, SessionName: target.Session, Remote: true,
			Origin: protocol.RouteOriginDiscovery, OriginKey: target.Endpoint,
			RemoteTarget: &remoteTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		}, nil
	}

	err := runTestClient(context.Background(), deps, client.AttachRequest{
		Intent: protocol.IntentAttach, SessionName: "test", Origin: protocol.RouteOriginLocal, OriginKey: "local",
	})
	remoteHello := helloFromSend(t, remote2)
	require.NoError(t, err)

	require.Equal(t, protocol.NavigationCapabilityHomePicker, remoteHello.NavigationCapabilities)
	remoteNewHello := helloFromSend(t, remote3)
	require.Equal(t, "remote-new", remoteNewHello.Name)
	require.Equal(t, &protocol.ExactSessionTarget{LifecycleID: remoteNewLifecycle, SessionName: "remote-new"}, remoteNewHello.ExactTarget)
}

func navigationDirectiveFrame(action protocol.NavigationAction) wire.Frame {
	directive := protocol.NavigationDirective{Action: action}
	if action == protocol.NavigationOpenHomePicker {
		directive.LeaseID = protocol.ParkedRouteLeaseID{1}
	}
	return frameOf(wire.MsgNavigationAction, wire.MarshalNavigationDirective(directive))
}

func mustMarshalCommittedIdentity(identity protocol.CommittedRouteIdentity) []byte {
	payload, err := wire.MarshalCommittedRouteIdentity(identity)
	if err != nil {
		panic(err)
	}
	return payload
}

func mustMarshalRouteAction(action protocol.RouteNavigationAction) []byte {
	payload, err := wire.MarshalRouteNavigationAction(action)
	if err != nil {
		panic(err)
	}
	return payload
}

func mustMarshalRoutePosition(position protocol.RoutePosition) []byte {
	payload, err := wire.MarshalRoutePosition(position)
	if err != nil {
		panic(err)
	}
	return payload
}

func TestAttachTargetHandoffReturnsValidatedTargetAndClosesTransport(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	target := protocol.AttachTarget{Endpoint: "remote.example", Session: "work", Intent: protocol.IntentAttach}
	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "local"}))},
		recvItem{f: frameOf(wire.MsgAttachTarget, wire.MarshalAttachTarget(target))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "work"})
	var handoff *client.AttachTargetError
	require.ErrorAs(t, err, &handoff)
	require.Equal(t, target, handoff.Target)
	require.Equal(t, int32(1), restoreCount.Load(), "handoff must restore the terminal after closing the old connection")
}

func TestAttachHelloIncludesCompleteLocalEnvironment(t *testing.T) {
	t.Setenv("VEV_CLIENT_ENV_TEST", "TOKEN=a=b=c")

	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	gotHello := make(chan protocol.Hello, 1)
	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).RunAndReturn(func(f wire.Frame) error {
		hello, err := wire.UnmarshalHello(f.Payload)
		if err != nil {
			return err
		}
		gotHello <- hello
		return nil
	}).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	require.NoError(t, runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: protocol.IntentEphemeral}))
	require.Equal(t, os.Environ(), (<-gotHello).Env)
}

func TestAttachHelloRequestsSingleOutputForDatagramTransport(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).RunAndReturn(func(f wire.Frame) error {
		hello, err := wire.UnmarshalHello(f.Payload)
		require.NoError(t, err)
		require.Equal(t, uint8(1), hello.MaxOutputInFlight)
		return nil
	}).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	require.NoError(t, runTestClient(context.Background(), attachTestDependencies(markedDatagramTransport{tr}, tm, realClock{}), client.AttachRequest{Intent: protocol.IntentEphemeral}))
}

func TestAttachHappyPath(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1", SessionName: "main"}))},
		recvItem{f: frameOf(wire.MsgOutput, mustMarshalOutput(protocol.Output{Epoch: 1, Size: domain.Size{Cols: 1, Rows: 1}, Data: []byte("hello world")}))},
		recvItem{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: protocol.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)
	require.Equal(t, "\x1b]10;?\x07\x1b]11;?\x07\x1b]4;0;?;1;?;2;?;3;?;4;?;5;?;6;?;7;?;8;?;9;?;10;?;11;?;12;?;13;?;14;?;15;?\x07\x1b[?2031$phello world", out.String())
	require.Equal(t, int32(1), restoreCount.Load(), "restore must run exactly once")
}

func TestAttachVersionMismatch(t *testing.T) {
	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Once()
	// EnterRaw must NOT be called on the error-before-welcome path.

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	tr.EXPECT().Recv().Return(
		frameOf(wire.MsgError, wire.MarshalErrorMsg(protocol.ErrorMsg{Code: protocol.ErrVersionMismatch, Text: "version mismatch"})),
		nil,
	).Once()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "main"})
	require.Error(t, err)
	var pe *client.ProtocolError
	require.True(t, errors.As(err, &pe), "want *client.ProtocolError, got %T", err)
	require.Equal(t, protocol.ErrVersionMismatch, pe.Code)
	tr.AssertNotCalled(t, "Send", isType(wire.MsgTheme))
}

func TestAttachRestoredOnRecvErrorMidStream(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	boom := errors.New("connection reset")
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
		recvItem{err: boom},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: protocol.IntentEphemeral, SessionName: ""})
	require.Error(t, err)
	require.Equal(t, int32(1), restoreCount.Load(), "restore must run after mid-stream Recv error")
}

func TestAttachDaemonVanishedOnEOF(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
		recvItem{err: io.EOF},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: protocol.IntentEphemeral, SessionName: ""})
	require.Error(t, err)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int32(1), restoreCount.Load())
}

func TestAttachStdinForwardsSGRMouseReportAsSingleFrame(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	input := newOneShotBlockingReader([]byte("\x1b[<0;1;1M"))
	defer input.unblock()

	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error { restoreCount.Add(1); return nil }, nil).Once()
	tm.EXPECT().In().Return(input).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()

	gotInput := make(chan []byte, 2)
	allowDetach := make(chan struct{})
	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	tr.EXPECT().Send(isType(wire.MsgInput)).RunAndReturn(func(f wire.Frame) error {
		in, err := wire.UnmarshalInput(f.Payload)
		require.NoError(t, err)
		gotInput <- in.Data
		close(allowDetach)
		return nil
	}).Once()
	welcome := frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))
	detached := frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))
	recvCh := make(chan recvItem, 1)
	recvCh <- recvItem{f: welcome}
	closed := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (wire.Frame, error) {
		select {
		case it := <-recvCh:
			return it.f, it.err
		case <-allowDetach:
			select {
			case <-closed:
				return wire.Frame{}, io.EOF
			default:
				close(closed)
				return detached, nil
			}
		case <-closed:
			return wire.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: protocol.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)
	select {
	case got := <-gotInput:
		require.Equal(t, []byte("\x1b[<0;1;1M"), got, "SGR mouse report must arrive as one intact MsgInput frame")
	case <-time.After(2 * time.Second):
		t.Fatal("mouse report input was not sent")
	}
}

func TestAttachStdinCoalescesSplitBracketedPaste(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	paste := []byte("\x1b[200~hello\nworld\x1b[201~")
	input := newChunkedBlockingReader([]byte("\x1b[200~hello\n"), []byte("world\x1b[201~"))
	defer input.unblock()

	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error { restoreCount.Add(1); return nil }, nil).Once()
	tm.EXPECT().In().Return(input).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()

	gotInput := make(chan []byte, 1)
	allowDetach := make(chan struct{})
	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	tr.EXPECT().Send(isType(wire.MsgInput)).RunAndReturn(func(f wire.Frame) error {
		in, err := wire.UnmarshalInput(f.Payload)
		require.NoError(t, err)
		gotInput <- in.Data
		close(allowDetach)
		return nil
	}).Once()
	welcome := frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))
	detached := frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))
	recvCh := make(chan recvItem, 1)
	recvCh <- recvItem{f: welcome}
	closed := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (wire.Frame, error) {
		select {
		case it := <-recvCh:
			return it.f, it.err
		case <-allowDetach:
			select {
			case <-closed:
				return wire.Frame{}, io.EOF
			default:
				close(closed)
				return detached, nil
			}
		case <-closed:
			return wire.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: protocol.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)
	select {
	case got := <-gotInput:
		require.Equal(t, paste, got, "split bracketed paste must arrive as one intact MsgInput frame")
	case <-time.After(2 * time.Second):
		t.Fatal("split bracketed paste input was not sent")
	}
}

func TestAttachForwardsResize(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	detachAfterResize := make(chan struct{})
	firstRecv := make(chan struct{})
	var firstRecvOnce sync.Once

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	// The resize frame is forwarded via the sender goroutine.
	gotResize := make(chan protocol.Resize, 1)
	tr.EXPECT().Send(isType(wire.MsgResize)).RunAndReturn(func(f wire.Frame) error {
		r, _ := wire.UnmarshalResize(f.Payload)
		gotResize <- r
		close(detachAfterResize)
		return nil
	}).Once()

	welcome := frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))
	detached := frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))
	recvCh := make(chan recvItem, 2)
	recvCh <- recvItem{f: welcome}
	done := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (wire.Frame, error) {
		firstRecvOnce.Do(func() { close(firstRecv) })
		select {
		case it := <-recvCh:
			return it.f, it.err
		case <-detachAfterResize:
			// Deliver the detach once the resize has been observed.
			select {
			case <-done:
				return wire.Frame{}, io.EOF
			default:
				close(done)
				return detached, nil
			}
		case <-done:
			return wire.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	// Push a resize event once attach begins receiving daemon frames.
	go func() {
		<-firstRecv
		resizeCh <- domain.Geometry{Size: domain.Size{Cols: 120, Rows: 40}}
	}()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: protocol.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)
	select {
	case r := <-gotResize:
		require.Equal(t, domain.Size{Cols: 120, Rows: 40}, r.Size)
	default:
		t.Fatal("resize was not forwarded")
	}
}

func TestRunRestoresRawModeAfterAttachError(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	boom := errors.New("connection reset")
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
		recvItem{err: boom},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()
	d := newMockClientDialer(t)
	d.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()

	err := runTestClient(context.Background(), testDependencies(d, tm, realClock{}, nil, nil), client.AttachRequest{Intent: protocol.IntentEphemeral, SessionName: "", Remote: false})
	require.Error(t, err)
	require.Equal(t, int32(1), restoreCount.Load(), "Run must restore raw mode after attach attempt errors")
}

func TestRunDoesNotEnterRawBeforePreWelcomeError(t *testing.T) {
	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Once()
	// EnterRaw must NOT be called when the daemon rejects Hello before Welcome.

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	tr.EXPECT().Recv().Return(
		frameOf(wire.MsgError, wire.MarshalErrorMsg(protocol.ErrorMsg{Code: protocol.ErrVersionMismatch, Text: "version mismatch"})),
		nil,
	).Once()
	tr.EXPECT().Close().Return(nil).Once()
	d := newMockClientDialer(t)
	d.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()

	err := runTestClient(context.Background(), testDependencies(d, tm, realClock{}, nil, nil), client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "main", Remote: false})
	require.Error(t, err)
	var pe *client.ProtocolError
	require.True(t, errors.As(err, &pe), "want *client.ProtocolError, got %T", err)
	tr.AssertNotCalled(t, "Send", isType(wire.MsgTheme))
}

func TestRunPhaseASingleAttempt(t *testing.T) {
	dialErr := errors.New("dial failed")
	d := newMockClientDialer(t)
	d.EXPECT().Dial(mock.Anything).Return(nil, dialErr).Once()
	tm := portsmocks.NewMockTerminal(t)

	err := runTestClient(context.Background(), testDependencies(d, tm, realClock{}, nil, nil), client.AttachRequest{Intent: protocol.IntentEphemeral, SessionName: "", Remote: false})
	require.ErrorIs(t, err, dialErr)
}

type sequenceDialer struct {
	trs   []wire.Transport
	errs  []error
	calls atomic.Int32
}

func (d *sequenceDialer) Dial(context.Context) (ports.ClientConnection, error) {
	i := int(d.calls.Add(1)) - 1
	if i < len(d.errs) && d.errs[i] != nil {
		return nil, d.errs[i]
	}
	if i >= len(d.trs) {
		return nil, io.EOF
	}
	return &rawClientConnection{raw: d.trs[i]}, nil
}

type recordingTransport struct {
	mu      sync.Mutex
	recvs   []recvItem
	sends   []wire.Frame
	closed  atomic.Int32
	stall   <-chan struct{}
	onSend  func(wire.Frame)
	onClose func()
}

func (t *recordingTransport) Send(f wire.Frame) error {
	t.mu.Lock()
	t.sends = append(t.sends, f)
	onSend := t.onSend
	t.mu.Unlock()
	if onSend != nil {
		onSend(f)
	}
	return nil
}

func (t *recordingTransport) Sends() []wire.Frame {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]wire.Frame(nil), t.sends...)
}

func (t *recordingTransport) Recv() (wire.Frame, error) {
	if len(t.recvs) == 0 {
		if t.stall != nil {
			<-t.stall
		}
		return wire.Frame{}, io.EOF
	}
	it := t.recvs[0]
	t.recvs = t.recvs[1:]
	if it.wait != nil {
		<-it.wait
	}
	return it.f, it.err
}

func (t *recordingTransport) Close() error {
	t.closed.Add(1)
	if t.onClose != nil {
		t.onClose()
	}
	return nil
}

type runTerminal struct {
	in           *blockingReader
	out          bytes.Buffer
	rawCount     atomic.Int32
	restoreCount atomic.Int32
	resizeCh     chan domain.Geometry
	geometryMu   sync.Mutex
	geometry     domain.Geometry
}

func newRunTerminal() *runTerminal {
	return &runTerminal{
		in:       newBlockingReader(),
		resizeCh: make(chan domain.Geometry),
		geometry: domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}},
	}
}

func (t *runTerminal) EnterRaw() (func() error, error) {
	t.rawCount.Add(1)
	return func() error { t.restoreCount.Add(1); return nil }, nil
}
func (t *runTerminal) Geometry() (domain.Geometry, error) {
	t.geometryMu.Lock()
	defer t.geometryMu.Unlock()
	return t.geometry, nil
}
func (t *runTerminal) setSize(size domain.Size) {
	t.geometryMu.Lock()
	t.geometry.Size = size
	t.geometryMu.Unlock()
}
func (t *runTerminal) ResizeEvents() <-chan domain.Geometry { return t.resizeCh }
func (t *runTerminal) In() io.Reader                        { return t.in }
func (t *runTerminal) Out() io.Writer                       { return &t.out }
func (t *runTerminal) Flush() error                         { return nil }

func helloFromSend(t *testing.T, tr *recordingTransport) protocol.Hello {
	t.Helper()
	sends := tr.Sends()
	require.NotEmpty(t, sends)
	require.Equal(t, wire.MsgHello, sends[0].Type)
	h, err := wire.UnmarshalHello(sends[0].Payload)
	require.NoError(t, err)
	return h
}

func welcomeFrame(token uint64) wire.Frame {
	return frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1", SessionName: "main", ResumeToken: token, Capabilities: protocol.CapabilityResume}))
}

func TestRunReconnectsWithRotatedTokenAndSameClientID(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	tr1 := &recordingTransport{recvs: []recvItem{{f: welcomeFrame(11)}, {err: io.EOF}}}
	tr2 := &recordingTransport{recvs: []recvItem{{f: welcomeFrame(22)}, {err: io.EOF}}}
	tr3 := &recordingTransport{recvs: []recvItem{{f: welcomeFrame(33)}, {f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))}}}
	d := &sequenceDialer{trs: []wire.Transport{tr1, tr2, tr3}}
	clock := &reconnectTestClock{}
	result := make(chan error, 1)
	go func() {
		result <- runTestClient(context.Background(), testDependencies(d, term, clock, nil, nil), client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "main", Remote: false})
	}()
	clock.fireReconnect(t)
	clock.fireReconnect(t)
	require.NoError(t, <-result)
	require.Equal(t, int32(3), d.calls.Load())
	require.Equal(t, int32(1), term.rawCount.Load())
	require.Equal(t, int32(1), term.restoreCount.Load())

	h1 := helloFromSend(t, tr1)
	h2 := helloFromSend(t, tr2)
	h3 := helloFromSend(t, tr3)
	require.Equal(t, protocol.IntentAttach, h1.Intent)
	require.Zero(t, h1.ResumeToken)
	require.Equal(t, protocol.IntentResume, h2.Intent)
	require.Equal(t, uint64(11), h2.ResumeToken)
	require.Equal(t, protocol.IntentResume, h3.Intent)
	require.Equal(t, uint64(22), h3.ResumeToken)
	require.Equal(t, h1.ClientID, h2.ClientID)
	require.Equal(t, h1.ClientID, h3.ClientID)
	require.Contains(t, term.out.String(), "reconnecting…")
	require.Contains(t, term.out.String(), "\x1b[2K")
}

func TestExpiredResumeFallsBackToFreshExactAttach(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	target := protocol.ExactSessionTarget{
		LifecycleID: domain.SessionLifecycleID{7, 8, 9},
		SessionName: "work",
	}
	remoteTarget := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: target.LifecycleID,
		SessionName: target.SessionName, LiveTabID: "stale-tab",
	}
	welcome := func(token uint64) wire.Frame {
		return frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{
			SessionID: "work", SessionName: "work", ResumeToken: token, Capabilities: protocol.CapabilityResume,
			CommittedIdentity: &protocol.CommittedRouteIdentity{Target: target},
		}))
	}
	tr1 := &recordingTransport{recvs: []recvItem{{f: welcome(11)}, {err: io.EOF}}}
	tr2 := &recordingTransport{recvs: []recvItem{{f: frameOf(wire.MsgError, wire.MarshalErrorMsg(protocol.ErrorMsg{
		Code: protocol.ErrNoSuchSession, Text: "resume token is no longer valid",
	}))}}}
	tr3 := &recordingTransport{recvs: []recvItem{
		{f: welcome(22)},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	dialer := &sequenceDialer{trs: []wire.Transport{tr1, tr2, tr3}}
	clock := &reconnectTestClock{}
	result := make(chan error, 1)
	go func() {
		result <- runTestClient(context.Background(), testDependencies(dialer, term, clock, nil, nil), client.AttachRequest{
			Intent: protocol.IntentAttach, SessionName: "work", Remote: true,
			Origin: protocol.RouteOriginDiscovery, OriginKey: "remote", RemoteTarget: &remoteTarget,
			EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		})
	}()

	clock.fireReconnect(t)
	require.NoError(t, <-result)
	require.Equal(t, int32(3), dialer.calls.Load())

	initialHello := helloFromSend(t, tr1)
	resumeHello := helloFromSend(t, tr2)
	fallbackHello := helloFromSend(t, tr3)
	require.Equal(t, protocol.IntentResume, resumeHello.Intent)
	require.Equal(t, uint64(11), resumeHello.ResumeToken)
	require.Equal(t, protocol.IntentAttach, fallbackHello.Intent)
	require.Zero(t, fallbackHello.ResumeToken)
	require.Equal(t, &target, fallbackHello.ExactTarget)
	require.Nil(t, fallbackHello.RemoteTarget)
	require.Equal(t, initialHello.ClientID, fallbackHello.ClientID)
}

func TestReconnectRestoresLastPublishedRouteTab(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	lifecycle := domain.SessionLifecycleID{7, 8, 9}
	target := protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "work"}
	welcome := func(token uint64) wire.Frame {
		return frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{
			SessionID: "work", SessionName: "work", ResumeToken: token, Capabilities: protocol.CapabilityResume,
			CommittedIdentity: &protocol.CommittedRouteIdentity{Target: target},
		}))
	}
	tr1 := &recordingTransport{recvs: []recvItem{
		{f: welcome(11)},
		{f: frameOf(wire.MsgRoutePosition, mustMarshalRoutePosition(protocol.RoutePosition{Target: target, ActiveTabID: "tab-2"}))},
		{err: io.EOF},
	}}
	tr2 := &recordingTransport{recvs: []recvItem{
		{f: welcome(22)},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	dialer := &sequenceDialer{trs: []wire.Transport{tr1, tr2}}
	clock := &reconnectTestClock{}
	result := make(chan error, 1)
	go func() {
		result <- runTestClient(context.Background(), testDependencies(dialer, term, clock, nil, nil), client.AttachRequest{
			Intent: protocol.IntentAttach, SessionName: "work", Origin: protocol.RouteOriginLocal, OriginKey: "local",
		})
	}()
	clock.fireReconnect(t)
	require.NoError(t, <-result)

	reconnectHello := helloFromSend(t, tr2)
	require.Equal(t, domain.TabStableID("tab-2"), reconnectHello.PreferredTabID)
}

func TestRemoteReconnectDropsStalePickerTargetAfterDaemonSessionSwitch(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	oldLifecycle := domain.SessionLifecycleID{1, 2, 3}
	newLifecycle := domain.SessionLifecycleID{4, 5, 6}
	remoteTarget := &domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: oldLifecycle,
		SessionName: "old", LiveTabID: "tab-1",
	}
	welcome := func(name string, lifecycle domain.SessionLifecycleID, token uint64) wire.Frame {
		return frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{
			SessionID: name, SessionName: name, ResumeToken: token, Capabilities: protocol.CapabilityResume,
			CommittedIdentity: &protocol.CommittedRouteIdentity{
				Target: protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: name},
			},
		}))
	}
	tr1 := &recordingTransport{recvs: []recvItem{
		{f: welcome("old", oldLifecycle, 11)},
		{f: frameOf(wire.MsgCommittedRouteIdentity, mustMarshalCommittedIdentity(protocol.CommittedRouteIdentity{
			Target: protocol.ExactSessionTarget{LifecycleID: newLifecycle, SessionName: "new"},
		}))},
		{err: io.EOF},
	}}
	tr2 := &recordingTransport{recvs: []recvItem{
		{f: welcome("new", newLifecycle, 22)},
		{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	}}
	dialer := &sequenceDialer{trs: []wire.Transport{tr1, tr2}}
	clock := &reconnectTestClock{}
	result := make(chan error, 1)
	go func() {
		result <- runTestClient(context.Background(), testDependencies(dialer, term, clock, nil, nil), client.AttachRequest{
			Intent: protocol.IntentAttach, SessionName: "old", Remote: true,
			Origin: protocol.RouteOriginDiscovery, OriginKey: "remote",
			RemoteTarget: remoteTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		})
	}()
	clock.fireReconnect(t)
	require.NoError(t, <-result)

	reconnectHello := helloFromSend(t, tr2)
	require.Equal(t, protocol.IntentResume, reconnectHello.Intent)
	require.Equal(t, "new", reconnectHello.Name)
	require.Equal(t, &protocol.ExactSessionTarget{LifecycleID: newLifecycle, SessionName: "new"}, reconnectHello.ExactTarget)
	require.Nil(t, reconnectHello.RemoteTarget)
}

func TestAttachRememberRemoteHost(t *testing.T) {
	tests := []struct {
		name             string
		request          client.AttachRequest
		remember         func() error
		recv             []recvItem
		preWelcomeReject bool
		wantCalled       bool
		wantErr          bool
	}{
		{
			name: "remote success",
			request: client.AttachRequest{
				Intent:      protocol.IntentAttach,
				SessionName: "main",
				Remote:      true,
			},
			remember: func() error { return nil },
			recv: []recvItem{
				{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
				{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
			},
			wantCalled: true,
		},
		{
			name: "callback failure does not fail attach",
			request: client.AttachRequest{
				Intent:      protocol.IntentAttach,
				SessionName: "main",
				Remote:      true,
			},
			remember: func() error { return errors.New("disk full") },
			recv: []recvItem{
				{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
				{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
			},
			wantCalled: true,
		},
		{
			name: "local attach does not call callback",
			request: client.AttachRequest{
				Intent:      protocol.IntentAttach,
				SessionName: "main",
				Remote:      false,
			},
			remember: func() error {
				t.Fatal("remember callback must not run for local attach")
				return nil
			},
			recv: []recvItem{
				{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
				{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
			},
		},
		{
			name: "rejected before welcome does not call callback",
			request: client.AttachRequest{
				Intent:      protocol.IntentAttach,
				SessionName: "main",
				Remote:      true,
			},
			remember: func() error {
				t.Fatal("remember callback must not run before welcome")
				return nil
			},
			recv: []recvItem{
				{f: frameOf(wire.MsgError, wire.MarshalErrorMsg(protocol.ErrorMsg{Code: protocol.ErrVersionMismatch, Text: "version mismatch"}))},
			},
			preWelcomeReject: true,
			wantErr:          true,
		},
		{
			name: "post-welcome error still uses happy terminal fixture",
			request: client.AttachRequest{
				Intent:      protocol.IntentAttach,
				SessionName: "main",
				Remote:      true,
			},
			remember: func() error { return nil },
			recv: []recvItem{
				{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
				{err: errors.New("connection reset")},
			},
			wantCalled: true,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tm *portsmocks.MockTerminal
			var unblockIn func()
			if tt.preWelcomeReject {
				tm = portsmocks.NewMockTerminal(t)
				tm.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Once()
				// EnterRaw must NOT be called when the daemon rejects Hello before Welcome.
			} else {
				var out bytes.Buffer
				var restoreCount atomic.Int32
				resizeCh := make(chan domain.Geometry)
				var in *blockingReader
				tm, in = newHappyTerminal(t, &out, &restoreCount, resizeCh)
				unblockIn = in.unblock
			}
			if unblockIn != nil {
				defer unblockIn()
			}

			var called atomic.Bool
			learnerDone := make(chan struct{})
			rememberHook := func() error {
				called.Store(true)
				defer close(learnerDone)
				if tt.remember != nil {
					return tt.remember()
				}
				return nil
			}

			tr := newMockClientConnection(t)
			if !tt.preWelcomeReject {
				tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
			}
			tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
			if tt.preWelcomeReject {
				tr.EXPECT().Recv().Return(tt.recv[0].f, tt.recv[0].err).Once()
			} else {
				unblock := scriptRecv(tr, tt.recv...)
				defer unblock()
			}
			tr.EXPECT().Close().Return(nil).Once()

			deps := attachTestDependencies(tr, tm, realClock{})
			learner := portsmocks.NewMockRemoteHostLearner(t)
			if tt.wantCalled {
				learner.EXPECT().RememberRemoteHost().RunAndReturn(rememberHook).Once()
			}
			deps.RemoteHostLearner = learner

			err := runTestClient(context.Background(), deps, tt.request)
			if tt.wantCalled {
				select {
				case <-learnerDone:
				case <-time.After(2 * time.Second):
					t.Fatal("remote host learner did not complete")
				}
			}
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantCalled, called.Load())
		})
	}
}

func TestRunRememberRemoteHostAtMostOnceAcrossReconnects(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()

	var rememberCalls atomic.Int32
	learnerDone := make(chan struct{})

	tr1 := &recordingTransport{recvs: []recvItem{{f: welcomeFrame(11)}, {err: io.EOF}}}
	tr2 := &recordingTransport{recvs: []recvItem{{f: welcomeFrame(22)}, {f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))}}}
	d := &sequenceDialer{trs: []wire.Transport{tr1, tr2}}

	deps := testDependencies(d, term, realClock{}, nil, nil)
	learner := portsmocks.NewMockRemoteHostLearner(t)
	learner.EXPECT().RememberRemoteHost().RunAndReturn(func() error {
		rememberCalls.Add(1)
		close(learnerDone)
		return nil
	}).Once()
	deps.RemoteHostLearner = learner
	err := runTestClient(context.Background(), deps, client.AttachRequest{
		Intent:      protocol.IntentAttach,
		SessionName: "main",
		Remote:      true,
	})
	require.NoError(t, err)
	select {
	case <-learnerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("remote host learner did not complete")
	}
	require.Equal(t, int32(1), rememberCalls.Load())
	require.Equal(t, int32(2), d.calls.Load())
}

func TestAttachRememberRemoteHostDoesNotBlockAfterWelcome(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Geometry)
	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Once()
	enteredRaw := make(chan struct{})
	tm.EXPECT().EnterRaw().Run(func() { close(enteredRaw) }).Return(func() error {
		restoreCount.Add(1)
		return nil
	}, nil).Once()
	in := newBlockingReader()
	tm.EXPECT().In().Return(in).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()
	defer in.unblock()

	learnerStarted := make(chan struct{})
	releaseLearner := make(chan struct{})
	learnerDone := make(chan struct{})

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	deps := attachTestDependencies(tr, tm, realClock{})
	learner := portsmocks.NewMockRemoteHostLearner(t)
	learner.EXPECT().RememberRemoteHost().RunAndReturn(func() error {
		close(learnerStarted)
		<-releaseLearner
		close(learnerDone)
		return nil
	}).Once()
	deps.RemoteHostLearner = learner

	errCh := make(chan error, 1)
	go func() {
		errCh <- runTestClient(context.Background(), deps, client.AttachRequest{
			Intent:      protocol.IntentAttach,
			SessionName: "main",
			Remote:      true,
		})
	}()

	select {
	case <-learnerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("remote host learner did not start")
	}
	select {
	case <-enteredRaw:
	case <-time.After(2 * time.Second):
		t.Fatal("attach did not enter raw mode while remote host learner was blocked")
	}
	select {
	case err := <-errCh:
		t.Fatalf("run returned before remote host learner completed: %v", err)
	default:
	}
	close(releaseLearner)
	select {
	case <-learnerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("remote host learner did not complete after release")
	}
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after remote host learner completed")
	}
}

func TestRunRestoresTerminalBeforeWaitingForLearner(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	restored := make(chan struct{})
	resizeCh := make(chan domain.Geometry)
	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error {
		restoreCount.Add(1)
		close(restored)
		return nil
	}, nil).Once()
	in := newBlockingReader()
	tm.EXPECT().In().Return(in).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()
	defer in.unblock()

	learnerStarted := make(chan struct{})
	releaseLearner := make(chan struct{})
	learnerDone := make(chan struct{})

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	deps := attachTestDependencies(tr, tm, realClock{})
	learner := portsmocks.NewMockRemoteHostLearner(t)
	learner.EXPECT().RememberRemoteHost().RunAndReturn(func() error {
		close(learnerStarted)
		<-releaseLearner
		close(learnerDone)
		return nil
	}).Once()
	deps.RemoteHostLearner = learner

	errCh := make(chan error, 1)
	go func() {
		errCh <- runTestClient(context.Background(), deps, client.AttachRequest{
			Intent:      protocol.IntentAttach,
			SessionName: "main",
			Remote:      true,
		})
	}()

	select {
	case <-learnerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("remote host learner did not start")
	}
	select {
	case <-restored:
	case <-time.After(2 * time.Second):
		t.Fatal("terminal was not restored while remote host learner was blocked")
	}
	require.Equal(t, int32(1), restoreCount.Load())
	select {
	case err := <-errCh:
		t.Fatalf("run returned before remote host learner completed: %v", err)
	default:
	}
	close(releaseLearner)
	select {
	case <-learnerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("remote host learner did not complete after release")
	}
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after remote host learner completed")
	}
}

func TestRunReturnsWhenRemoteHostLearnerStalls(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	restored := make(chan struct{})
	resizeCh := make(chan domain.Geometry)
	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error {
		restoreCount.Add(1)
		close(restored)
		return nil
	}, nil).Once()
	in := newBlockingReader()
	tm.EXPECT().In().Return(in).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()
	defer in.unblock()

	learnerStarted := make(chan struct{})
	blockLearner := make(chan struct{})
	defer close(blockLearner)

	shutdownTimerC := make(chan time.Time, 1)
	shutdownTimerCreated := make(chan struct{})
	shutdownTimer := portsmocks.NewMockTimer(t)
	shutdownTimer.EXPECT().C().Return((<-chan time.Time)(shutdownTimerC)).Once()
	shutdownTimer.EXPECT().Stop().Return(true).Once()
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().Now().Return(time.Now()).Maybe()
	clock.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(d time.Duration) ports.Timer {
		if d == time.Second {
			close(shutdownTimerCreated)
			return shutdownTimer
		}
		return realClock{}.NewTimer(d)
	}).Maybe()

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	deps := attachTestDependencies(tr, tm, clock)
	learner := portsmocks.NewMockRemoteHostLearner(t)
	learner.EXPECT().RememberRemoteHost().RunAndReturn(func() error {
		close(learnerStarted)
		<-blockLearner
		return nil
	}).Once()
	deps.RemoteHostLearner = learner

	errCh := make(chan error, 1)
	go func() {
		errCh <- runTestClient(context.Background(), deps, client.AttachRequest{
			Intent:      protocol.IntentAttach,
			SessionName: "main",
			Remote:      true,
		})
	}()

	select {
	case <-learnerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("remote host learner did not start")
	}
	select {
	case <-restored:
	case <-time.After(2 * time.Second):
		t.Fatal("terminal was not restored while remote host learner was stalled")
	}
	require.Equal(t, int32(1), restoreCount.Load())

	select {
	case <-shutdownTimerCreated:
	case <-time.After(2 * time.Second):
		t.Fatal("remote host learner shutdown timer was not created")
	}
	shutdownTimerC <- time.Now()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after remote host learner shutdown timer fired")
	}
}

func TestRunDoesNotRetryTerminalDetachedError(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	tr := &recordingTransport{recvs: []recvItem{{f: welcomeFrame(11)}, {f: frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonSessionKilled}))}}}
	d := &sequenceDialer{trs: []wire.Transport{tr}}

	err := runTestClient(context.Background(), testDependencies(d, term, realClock{}, nil, nil), client.AttachRequest{Intent: protocol.IntentAttach, SessionName: "main", Remote: false})
	require.Error(t, err)
	var de *client.DetachedError
	require.True(t, errors.As(err, &de))
	require.Equal(t, int32(1), d.calls.Load())
	require.Equal(t, int32(1), term.restoreCount.Load())
}
