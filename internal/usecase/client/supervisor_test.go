package client

import (
	"context"
	"errors"
	"io"
	"math"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// supervisorTestClock is a manually fired ports.Clock. NewTimer publishes each
// timer on a buffered channel so a test can fire it deterministically without a
// sleep, and Stop is recorded so a test can prove every retry timer was
// retired.
type supervisorTestClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*supervisorTestTimer
	created chan *supervisorTestTimer
}

func newSupervisorTestClock() *supervisorTestClock {
	return &supervisorTestClock{now: time.Unix(1000, 0), created: make(chan *supervisorTestTimer, 32)}
}

func (c *supervisorTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *supervisorTestClock) NewTimer(delay time.Duration) ports.Timer {
	timer := &supervisorTestTimer{ch: make(chan time.Time, 1), delay: delay}
	c.mu.Lock()
	c.timers = append(c.timers, timer)
	c.mu.Unlock()
	select {
	case c.created <- timer:
	default:
	}
	return timer
}

func (c *supervisorTestClock) awaitTimer(t *testing.T) *supervisorTestTimer {
	t.Helper()
	select {
	case timer := <-c.created:
		return timer
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not schedule a retry timer")
		return nil
	}
}

func (c *supervisorTestClock) timerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

func (c *supervisorTestClock) liveTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	live := 0
	for _, timer := range c.timers {
		if !timer.stopped() {
			live++
		}
	}
	return live
}

func (c *supervisorTestClock) allStopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, timer := range c.timers {
		if !timer.stopped() {
			return false
		}
	}
	return true
}

type supervisorTestTimer struct {
	ch          chan time.Time
	delay       time.Duration
	mu          sync.Mutex
	stoppedFlag bool
}

func (t *supervisorTestTimer) C() <-chan time.Time { return t.ch }

func (t *supervisorTestTimer) Reset(time.Duration) bool { return false }

func (t *supervisorTestTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stoppedFlag {
		return false
	}
	t.stoppedFlag = true
	return true
}

func (t *supervisorTestTimer) stopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stoppedFlag
}

// fire delivers the timer without blocking, so a test may fire before or after
// the supervisor reaches its select.
func (t *supervisorTestTimer) fire() {
	select {
	case t.ch <- time.Now():
	default:
	}
}

// supervisorTestTerminal records raw-mode enters and restores. Its reader is
// shared so tests can model input EOF and a live terminal.
type supervisorTestTerminal struct {
	in       io.Reader
	enterErr error

	mu       sync.Mutex
	restores int
}

func newSupervisorTestTerminal(in io.Reader) *supervisorTestTerminal {
	return &supervisorTestTerminal{in: in}
}

func (t *supervisorTestTerminal) EnterRaw() (func() error, error) {
	if t.enterErr != nil {
		return nil, t.enterErr
	}
	return func() error {
		t.mu.Lock()
		t.restores++
		t.mu.Unlock()
		return nil
	}, nil
}

func (t *supervisorTestTerminal) Geometry() (domain.Geometry, error) {
	return domain.Geometry{}, nil
}

func (t *supervisorTestTerminal) ResizeEvents() <-chan domain.Geometry { return nil }

func (t *supervisorTestTerminal) In() io.Reader { return t.in }

func (t *supervisorTestTerminal) Out() io.Writer { return io.Discard }

func (t *supervisorTestTerminal) Flush() error { return nil }

func (t *supervisorTestTerminal) restoreCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.restores
}

// supervisorTestConnector scripts connection attempts and records concurrency,
// so a test can prove attempts never overlap.
type supervisorTestConnector struct {
	connect func(ctx context.Context, call int) (ports.BrokerService, error)

	mu          sync.Mutex
	calls       int
	inFlight    int
	maxInFlight int
	started     chan int
}

func newSupervisorTestConnector(connect func(ctx context.Context, call int) (ports.BrokerService, error)) *supervisorTestConnector {
	return &supervisorTestConnector{connect: connect, started: make(chan int, 32)}
}

func (c *supervisorTestConnector) Connect(ctx context.Context) (ports.BrokerService, error) {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.inFlight++
	if c.inFlight > c.maxInFlight {
		c.maxInFlight = c.inFlight
	}
	c.mu.Unlock()
	select {
	case c.started <- call:
	default:
	}
	defer func() {
		c.mu.Lock()
		c.inFlight--
		c.mu.Unlock()
	}()
	if c.connect == nil {
		return nil, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "no script"}
	}
	return c.connect(ctx, call)
}

func (c *supervisorTestConnector) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *supervisorTestConnector) maxConcurrent() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxInFlight
}

func (c *supervisorTestConnector) inFlightCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inFlight
}

func (c *supervisorTestConnector) awaitStart(t *testing.T) int {
	t.Helper()
	select {
	case call := <-c.started:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not start a connection attempt")
		return 0
	}
}

// supervisorTestHub is one broker publication source. It conforms to
// ports.BrokerSubscription: each subscriber owns its own capacity-one channel,
// a publication offers a non-blocking wake to every live subscriber and never
// replaces or closes a channel, and a subscriber re-reads the newest snapshot
// on wake. A slow or parked subscriber therefore can never block the
// publisher or busy-spin.
type supervisorTestHub struct {
	mu       sync.Mutex
	snapshot ports.BrokerSnapshot
	subs     map[chan struct{}]struct{}
}

func newSupervisorTestHub() *supervisorTestHub {
	return &supervisorTestHub{subs: make(map[chan struct{}]struct{})}
}

func (h *supervisorTestHub) publish(snapshot ports.BrokerSnapshot) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snapshot = snapshot
	for changed := range h.subs {
		select {
		case changed <- struct{}{}:
		default:
		}
	}
}

func (h *supervisorTestHub) current() ports.BrokerSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshot
}

func (h *supervisorTestHub) subscribe(closeFn func()) ports.BrokerSubscription {
	changed := make(chan struct{}, 1)
	h.mu.Lock()
	h.subs[changed] = struct{}{}
	h.mu.Unlock()
	return supervisorTestSubscription{changed: changed, closeFn: func() {
		h.mu.Lock()
		delete(h.subs, changed)
		h.mu.Unlock()
		if closeFn != nil {
			closeFn()
		}
	}}
}

type supervisorTestSubscription struct {
	changed chan struct{}
	closeFn func()
}

func (s supervisorTestSubscription) Changed() <-chan struct{} { return s.changed }

func (s supervisorTestSubscription) Close() {
	if s.closeFn != nil {
		s.closeFn()
	}
}

// supervisorTestService is one scripted BrokerService. Close is observable so a
// test can prove a retired or late service was closed, and Close can be held open
// so a test can order a parent cancellation strictly before a settle decision that
// runs after the connection is released.
type supervisorTestService struct {
	id  ports.BrokerConnectionID
	hub *supervisorTestHub

	mu       sync.Mutex
	err      error
	doneOnce sync.Once
	done     chan struct{}

	closeOnce sync.Once
	closed    chan struct{}
	subClosed chan struct{}

	// closeGate, when non-nil, holds Close until releaseClose is called, and
	// closeEntered reports that Close began waiting on it.
	closeGate    chan struct{}
	closeEntered chan struct{}

	// openStream scripts logical-stream admission for the P5.3b attachment
	// path. When nil, OpenStream keeps the historical refusal. openCalls
	// records every exact request the supervisor admitted.
	openStream func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error)
	openCalls  []ports.BrokerOpenStreamRequest
}

func newSupervisorTestService(id ports.BrokerConnectionID) *supervisorTestService {
	return &supervisorTestService{
		id:        id,
		hub:       newSupervisorTestHub(),
		done:      make(chan struct{}),
		closed:    make(chan struct{}),
		subClosed: make(chan struct{}, 1),
	}
}

func (s *supervisorTestService) ConnectionID() ports.BrokerConnectionID { return s.id }

func (s *supervisorTestService) Done() <-chan struct{} { return s.done }

func (s *supervisorTestService) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *supervisorTestService) Snapshot() ports.BrokerSnapshot { return s.hub.current() }

func (s *supervisorTestService) Subscribe() (ports.BrokerSubscription, error) {
	return s.hub.subscribe(func() {
		select {
		case s.subClosed <- struct{}{}:
		default:
		}
	}), nil
}

func (s *supervisorTestService) OpenStream(ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	s.mu.Lock()
	s.openCalls = append(s.openCalls, request)
	open := s.openStream
	s.mu.Unlock()
	if open == nil {
		return nil, errors.New("supervisor test service does not support streams")
	}
	return open(ctx, request)
}

// setOpenStream installs the logical-stream admission all P5.3b tests drive.
func (s *supervisorTestService) setOpenStream(open func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.openStream = open
}

// openedRequests returns a copy of every exact stream request admitted so far.
func (s *supervisorTestService) openedRequests() []ports.BrokerOpenStreamRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ports.BrokerOpenStreamRequest(nil), s.openCalls...)
}

func (s *supervisorTestService) CloseStream(ports.BrokerConnectionID, ports.BrokerStreamID) error {
	return nil
}

func (s *supervisorTestService) AddHost(context.Context, string) error { return nil }

func (s *supervisorTestService) RemoveHost(context.Context, string) (bool, error) {
	return false, nil
}

func (s *supervisorTestService) RequestReconcile(string) {}

func (s *supervisorTestService) Close() error {
	s.closeOnce.Do(func() {
		if s.closeGate != nil {
			select {
			case s.closeEntered <- struct{}{}:
			default:
			}
			<-s.closeGate
		}
		close(s.closed)
		s.terminate(nil)
	})
	return nil
}

// holdClose makes Close block until releaseClose is called, so a test can order a
// parent cancellation strictly before a settle decision that runs after the
// connection is released. closingCh reports that the release was entered.
func (s *supervisorTestService) holdClose() {
	s.closeGate = make(chan struct{})
	s.closeEntered = make(chan struct{}, 1)
}

// releaseClose releases a held Close.
func (s *supervisorTestService) releaseClose() {
	if s.closeGate != nil {
		close(s.closeGate)
	}
}

// closingCh reports that the held release was entered.
func (s *supervisorTestService) closingCh() <-chan struct{} { return s.closeEntered }

// terminate removes the connection, recording the first cause exactly once.
func (s *supervisorTestService) terminate(err error) {
	if err != nil {
		s.mu.Lock()
		if s.err == nil {
			s.err = err
		}
		s.mu.Unlock()
	}
	s.doneOnce.Do(func() { close(s.done) })
}

// lose models an unexplained broker-side loss.
func (s *supervisorTestService) lose(err error) { s.terminate(err) }

func (s *supervisorTestService) publish(epoch ports.BrokerEpoch, revision ports.BrokerRevision) {
	s.hub.publish(ports.BrokerSnapshot{Epoch: epoch, Revision: revision})
}

// publishSnapshot publishes one complete broker snapshot, as a real service
// does, so a test can deliver a second-and-later catalogue publication.
func (s *supervisorTestService) publishSnapshot(snapshot ports.BrokerSnapshot) {
	s.hub.publish(snapshot)
}

func (s *supervisorTestService) closedCh() <-chan struct{} { return s.closed }

func mustSupervisor(t *testing.T, cfg SupervisorConfig) *Supervisor {
	t.Helper()
	sup, err := NewSupervisor(cfg)
	require.NoError(t, err)
	return sup
}

// supervisorTestReader blocks until unblocked, so a test controls exactly when
// the owned terminal input reaches EOF.
type supervisorTestReader struct {
	mu   sync.Mutex
	done chan struct{}
}

func newSupervisorTestReader() *supervisorTestReader {
	return &supervisorTestReader{done: make(chan struct{})}
}

func (r *supervisorTestReader) Read([]byte) (int, error) {
	<-r.done
	return 0, io.EOF
}

func (r *supervisorTestReader) unblock() {
	r.mu.Lock()
	defer r.mu.Unlock()
	select {
	case <-r.done:
	default:
		close(r.done)
	}
}

func TestSupervisorReducerTransitions(t *testing.T) {
	transient := errors.New("unavailable")
	incompatible := ports.BrokerError{Code: ports.BrokerErrorIncompatible, Text: "version"}
	exited := ports.BrokerError{Code: ports.BrokerErrorExplicitExit, Text: "exit"}
	picker := State{Presentation: PresentPicker, Connectivity: ConnectivityDisconnected}

	tests := []struct {
		name  string
		state State
		event supervisorEvent
		want  State
	}{
		{
			name:  "begin attempt claims a generation and connects",
			state: picker,
			event: supervisorEvent{kind: supervisorBeginAttempt},
			want:  State{Presentation: PresentPicker, Connectivity: ConnectivityConnectingBroker, Attempt: 0, Generation: 1},
		},
		{
			name:  "ready resets the retry cadence",
			state: State{Presentation: PresentPicker, Connectivity: ConnectivityConnectingBroker, Attempt: 3, Generation: 4, Err: transient},
			event: supervisorEvent{kind: supervisorReady},
			want:  State{Presentation: PresentPicker, Connectivity: ConnectivityReady, Attempt: 0, Generation: 4},
		},
		{
			name:  "transient failure waits with a bumped attempt",
			state: State{Presentation: PresentPicker, Connectivity: ConnectivityConnectingBroker, Attempt: 0, Generation: 2},
			event: supervisorEvent{kind: supervisorTransientFailure, err: transient},
			want:  State{Presentation: PresentPicker, Connectivity: ConnectivityRetryWait, Attempt: 1, Generation: 2, Err: transient},
		},
		{
			name:  "broker loss returns to picker retry wait",
			state: State{Presentation: PresentPicker, Connectivity: ConnectivityReady, Attempt: 0, Generation: 5},
			event: supervisorEvent{kind: supervisorBrokerLoss, err: transient},
			want:  State{Presentation: PresentPicker, Connectivity: ConnectivityRetryWait, Attempt: 1, Generation: 5, Err: transient, ReadyLost: true},
		},
		{
			name:  "non-retryable failure disconnects and stays on the picker",
			state: State{Presentation: PresentPicker, Connectivity: ConnectivityConnectingBroker, Attempt: 1, Generation: 3},
			event: supervisorEvent{kind: supervisorNonRetryable, err: incompatible},
			want:  State{Presentation: PresentPicker, Connectivity: ConnectivityDisconnected, Attempt: 1, Generation: 3, Err: incompatible},
		},
		{
			name:  "terminal failure terminates",
			state: State{Presentation: PresentPicker, Connectivity: ConnectivityReady, Generation: 7},
			event: supervisorEvent{kind: supervisorTerminal, err: exited},
			want:  State{Presentation: PresentTerminating, Connectivity: ConnectivityDisconnected, Generation: 7, Err: exited},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, reduceSupervisor(tt.state, tt.event))
		})
	}
}

func TestSupervisorBackoffCadence(t *testing.T) {
	zero := func() float64 { return 0 }
	half := func() float64 { return 0.5 }
	clamped := func() float64 { return 1.5 }
	nan := func() float64 { return math.NaN() }
	negInf := func() float64 { return math.Inf(-1) }
	posInf := func() float64 { return math.Inf(1) }

	tests := []struct {
		name    string
		attempt uint64
		jitter  func() float64
		want    time.Duration
	}{
		{"no attempt has no delay", 0, zero, 0},
		{"first failure halves 100ms", 1, zero, 50 * time.Millisecond},
		{"second failure halves 200ms", 2, zero, 100 * time.Millisecond},
		{"third failure halves 400ms", 3, zero, 200 * time.Millisecond},
		{"fourth failure halves 800ms", 4, zero, 400 * time.Millisecond},
		{"fifth failure halves 1.6s", 5, zero, 800 * time.Millisecond},
		{"cap reached at 2s", 6, zero, time.Second},
		{"cap stays at 2s", 9, zero, time.Second},
		{"equal jitter reaches three quarters", 1, half, 75 * time.Millisecond},
		{"equal jitter scales with the cap", 6, half, 1500 * time.Millisecond},
		{"full jitter reaches the cap", 1, clamped, 100 * time.Millisecond},
		{"full jitter stays capped", 6, clamped, 2 * time.Second},
		{"NaN jitter clamps to the minimum half", 1, nan, 50 * time.Millisecond},
		{"NaN jitter stays at half the cap", 6, nan, time.Second},
		{"negative jitter clamps to the minimum half", 1, negInf, 50 * time.Millisecond},
		{"negative jitter stays at half the cap", 6, negInf, time.Second},
		{"positive infinity clamps to the cap", 1, posInf, 100 * time.Millisecond},
		{"positive infinity stays capped", 6, posInf, 2 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, supervisorBackoffDelay(tt.attempt, tt.jitter))
		})
	}
}

func TestSupervisorPickerWithoutBroker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := newSupervisorTestClock()
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	terminal := newSupervisorTestTerminal(reader)
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		return nil, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "broker down"}
	})

	var (
		renderMu sync.Mutex
		renders  []State
	)
	sup := mustSupervisor(t, SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
		Render: func(state State) {
			renderMu.Lock()
			renders = append(renders, state)
			renderMu.Unlock()
		},
	})

	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()

	first := clock.awaitTimer(t)
	require.Equal(t, 50*time.Millisecond, first.delay, "the first retry is the equal-jitter half of 100ms")
	require.Equal(t, 1, connector.awaitStart(t))
	require.Equal(t, State{Presentation: PresentPicker, Connectivity: ConnectivityRetryWait, Attempt: 1, Generation: 1, Err: ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "broker down"}}, sup.State())

	first.fire()
	second := clock.awaitTimer(t)
	require.Equal(t, 100*time.Millisecond, second.delay, "the second retry doubles the cap")
	require.Equal(t, 2, connector.awaitStart(t))
	require.Equal(t, PresentPicker, sup.State().Presentation, "the picker stays up while disconnected")
	require.Equal(t, ConnectivityRetryWait, sup.State().Connectivity)
	require.Equal(t, uint64(2), sup.State().Generation)

	cancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
	require.Equal(t, 1, terminal.restoreCount())
	require.Equal(t, 1, connector.maxConcurrent())

	renderMu.Lock()
	defer renderMu.Unlock()
	require.NotEmpty(t, renders)
	require.Equal(t, PresentTerminating, renders[len(renders)-1].Presentation)
	for _, state := range renders[:len(renders)-1] {
		require.Equal(t, PresentPicker, state.Presentation, "P5.1a never leaves the picker")
	}
}

func TestSupervisorBrokerLossReconnectsWithFreshGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := newSupervisorTestClock()
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	terminal := newSupervisorTestTerminal(reader)

	first := newSupervisorTestService(ports.BrokerConnectionID{1})
	first.publish(1, 1)
	second := newSupervisorTestService(ports.BrokerConnectionID{2})

	connector := newSupervisorTestConnector(func(_ context.Context, call int) (ports.BrokerService, error) {
		switch call {
		case 1:
			return first, nil
		default:
			return second, nil
		}
	})

	sup := mustSupervisor(t, SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
	})

	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()

	require.Equal(t, 1, connector.awaitStart(t))
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)
	require.Equal(t, uint64(1), sup.State().Generation)
	require.Equal(t, uint64(0), sup.State().Attempt)

	// The idle broker connection is lost: the supervisor retires it and waits
	// the first cadence before reconnecting.
	first.lose(ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "idle loss"})
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityRetryWait }, 5*time.Second, time.Millisecond)
	require.Equal(t, uint64(1), sup.State().Attempt)
	select {
	case <-first.closedCh():
	default:
		t.Fatal("the lost connection was not retired")
	}
	select {
	case <-first.subClosed:
	default:
		t.Fatal("the lost subscription was not retired")
	}

	retry := clock.awaitTimer(t)
	require.Equal(t, 50*time.Millisecond, retry.delay)
	retry.fire()
	require.Equal(t, 2, connector.awaitStart(t))

	// Wait until the replacement is waiting for its first publication.
	require.Eventually(t, func() bool {
		return sup.State().Connectivity == ConnectivityConnectingBroker && sup.State().Generation == 2
	}, 5*time.Second, time.Millisecond)

	// A publication on the retired connection must never make the replacement
	// Ready; only the replacement's own fresh epoch does.
	first.publish(1, 2)
	require.Never(t, func() bool { return sup.State().Connectivity == ConnectivityReady }, 50*time.Millisecond, 5*time.Millisecond)

	second.publish(2, 1)
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)
	require.Equal(t, uint64(2), sup.State().Generation)
	require.Equal(t, uint64(0), sup.State().Attempt, "readiness resets the cadence")

	cancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
	require.Equal(t, 1, terminal.restoreCount())
	select {
	case <-second.closedCh():
	default:
		t.Fatal("the active connection was not retired on shutdown")
	}
}

func TestSupervisorCancellationClosesLateConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	clock := newSupervisorTestClock()
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	terminal := newSupervisorTestTerminal(reader)

	release := make(chan struct{})
	late := newSupervisorTestService(ports.BrokerConnectionID{9})
	late.publish(1, 1)
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		<-release
		return late, nil
	})

	sup := mustSupervisor(t, SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
	})

	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()
	require.Equal(t, 1, connector.awaitStart(t))

	// Cancel while the connector ignores cancellation, then let it return a
	// live service: the supervisor must close the late superseded success.
	cancel()
	close(release)

	require.ErrorIs(t, <-runErr, context.Canceled)
	select {
	case <-late.closedCh():
	default:
		t.Fatal("a late superseded success must be closed")
	}
	require.Equal(t, 1, terminal.restoreCount())
	require.Zero(t, connector.inFlightCount())
}

func TestSupervisorIncompatibleStopsReconnecting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := newSupervisorTestClock()
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	terminal := newSupervisorTestTerminal(reader)

	incompatible := ports.BrokerError{Code: ports.BrokerErrorIncompatible, Text: "protocol"}
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		return nil, incompatible
	})
	notified := make(chan error, 4)
	sup := mustSupervisor(t, SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
		Notify:    func(_ State, err error) { notified <- err },
	})

	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()

	select {
	case err := <-notified:
		var typed ports.BrokerError
		require.ErrorAs(t, err, &typed)
		require.Equal(t, ports.BrokerErrorIncompatible, typed.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("the incompatible failure was not surfaced")
	}

	require.Equal(t, State{Presentation: PresentPicker, Connectivity: ConnectivityDisconnected, Generation: 1, Err: incompatible}, sup.State())
	require.Never(t, func() bool { return connector.callCount() > 1 }, 50*time.Millisecond, 5*time.Millisecond)
	require.Zero(t, clock.timerCount(), "a non-retryable failure schedules no retry timer")

	cancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
	require.Equal(t, 1, terminal.restoreCount())
}

func TestSupervisorTerminalEOFRestoresRawOnce(t *testing.T) {
	clock := newSupervisorTestClock()
	terminal := newSupervisorTestTerminal(strings.NewReader(""))
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		return nil, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "down"}
	})

	sup := mustSupervisor(t, SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
	})

	require.NoError(t, sup.Run(context.Background()), "a clean terminal EOF is an orderly exit")
	require.Equal(t, 1, terminal.restoreCount())
	require.Equal(t, PresentTerminating, sup.State().Presentation)
}

func TestSupervisorRawModePrecedesBroker(t *testing.T) {
	clock := newSupervisorTestClock()
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	terminal := newSupervisorTestTerminal(reader)

	enteredAfterRaw := make(chan bool, 1)
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		enteredAfterRaw <- terminal.restoreCount() == 0
		return nil, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "down"}
	})

	sup := mustSupervisor(t, SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
	})

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()
	require.Equal(t, 1, connector.awaitStart(t))
	require.True(t, <-enteredAfterRaw, "raw mode must be entered before the first broker connection")
	cancel()
	<-runErr
}

func TestSupervisorSingleAttemptAndTimer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	clock := newSupervisorTestClock()
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	terminal := newSupervisorTestTerminal(reader)
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		return nil, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "down"}
	})

	sup := mustSupervisor(t, SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
	})

	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()

	first := clock.awaitTimer(t)
	first.fire()
	second := clock.awaitTimer(t)
	require.Equal(t, 100*time.Millisecond, second.delay)
	second.fire()
	clock.awaitTimer(t)

	require.Equal(t, 1, connector.maxConcurrent(), "attempts never overlap")
	require.Equal(t, 3, connector.callCount())
	require.Equal(t, 1, clock.liveTimers(), "exactly one retry timer is live")
	require.Zero(t, connector.inFlightCount(), "the connector call has returned before the next wait")

	cancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
	require.Eventually(t, clock.allStopped, time.Second, time.Millisecond, "every retry timer is retired")
	require.Equal(t, terminal.restoreCount(), 1)
	require.Zero(t, connector.inFlightCount())
}

// supervisorCancellationRaceRounds is how many times each cancellation race is
// replayed. Every round orders the parent cancellation strictly before the run's
// settle decision by construction, so the rounds are deterministic rather than
// timing-dependent; the repetition runs both settle points many times over.
const supervisorCancellationRaceRounds = 100

// TestSupervisorCancellationRacingSettlementIsTerminal pins the parent
// cancellation fence at both settle points: an attempt that settles at the same
// instant as the parent cancellation is retired rather than adopted, and an
// established-connection loss that settles at that instant is never turned into a
// retry. Every round must end terminating with ctx.Err without scheduling a retry
// cadence, rendering ConnectivityRetryWait, or notifying a connectivity failure
// for a settle the parent had already given up on.
//
// The ordering is enforced, never hoped for: each round parks the settle at a
// point the round itself releases (the state lock the run decides adoption under,
// or the connection release the loss settle runs after) and cancels there, so the
// decision sees the cancellation whichever branch the run's select took.
func TestSupervisorCancellationRacingSettlementIsTerminal(t *testing.T) {
	transient := ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "raced"}
	exited := ports.BrokerError{Code: ports.BrokerErrorExplicitExit, Text: "raced exit"}

	attempts := []struct {
		name    string
		attempt func() (*supervisorTestService, error)
	}{
		{name: "retryable failure", attempt: func() (*supervisorTestService, error) { return nil, transient }},
		{
			name: "ready connection",
			attempt: func() (*supervisorTestService, error) {
				service := newSupervisorTestService(ports.BrokerConnectionID{3})
				service.publish(1, 1)
				return service, nil
			},
		},
		{name: "terminal failure", attempt: func() (*supervisorTestService, error) { return nil, exited }},
	}

	for _, tc := range attempts {
		t.Run("attempt "+tc.name, func(t *testing.T) {
			// One carrier thread for the round: yielding after the attempt is
			// released then runs that goroutine to its settlement before this one
			// resumes, so the outcome is delivered while the parent is still live
			// and the run's select takes the settled attempt rather than ctx.Done.
			defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
			for round := 0; round < supervisorCancellationRaceRounds; round++ {
				release := make(chan struct{})
				var raced *supervisorTestService
				race := startCancellationRace(t, func(context.Context, int) (ports.BrokerService, error) {
					<-release
					service, err := tc.attempt()
					raced = service
					if service == nil {
						return nil, err
					}
					return service, err
				})
				require.Equal(t, 1, race.connector.awaitStart(t), "round %d", round)

				// Take the state lock the run decides adoption under, release the
				// attempt, let it settle, and cancel the parent while still holding
				// the lock: the decision is ordered strictly after the cancellation
				// by construction.
				race.supervisor.mu.Lock()
				close(release)
				runtime.Gosched()
				race.cancel()
				race.supervisor.mu.Unlock()

				race.assertSettledAsCancelled(t, round, false)
				if raced != nil {
					select {
					case <-raced.closedCh():
					default:
						t.Fatalf("round %d: an attempt that raced cancellation must be retired, not adopted", round)
					}
				}
			}
		})
	}

	t.Run("established loss", func(t *testing.T) {
		for round := 0; round < supervisorCancellationRaceRounds; round++ {
			losing := newSupervisorTestService(ports.BrokerConnectionID{4})
			losing.publish(1, 1)
			losing.holdClose()
			race := startCancellationRace(t, func(context.Context, int) (ports.BrokerService, error) {
				return losing, nil
			})
			require.Equal(t, 1, race.connector.awaitStart(t), "round %d", round)
			require.Eventually(t, func() bool { return race.supervisor.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond, "round %d", round)

			// The loss wakes the run while the parent is still live, and its
			// connection release then parks the run before the settle decision, so
			// the cancellation below is ordered strictly before that decision.
			losing.lose(ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "raced loss"})
			select {
			case <-losing.closingCh():
			case <-time.After(5 * time.Second):
				t.Fatalf("round %d: the run never released the lost connection", round)
			}
			race.cancel()
			losing.releaseClose()

			race.assertSettledAsCancelled(t, round, true)
			select {
			case <-losing.closedCh():
			case <-time.After(5 * time.Second):
				t.Fatalf("round %d: the lost connection must be retired", round)
			}
		}
	})
}

// cancellationRace is one scripted round of the parent-cancellation race: the
// running supervisor with its fake clock, terminal, and recorder, plus the run's
// result channel. A round cancels the parent itself, at the point that orders the
// cancellation before the settle decision under test.
type cancellationRace struct {
	cancel     context.CancelFunc
	clock      *supervisorTestClock
	terminal   *supervisorTestTerminal
	recorder   *cancellationRaceRecorder
	connector  *supervisorTestConnector
	supervisor *Supervisor
	runErr     chan error
}

// startCancellationRace starts one supervisor run over a scripted connector. The
// reader is shared and never unblocked until the test ends, so the run can only
// end through cancellation.
func startCancellationRace(t *testing.T, connect func(ctx context.Context, call int) (ports.BrokerService, error)) *cancellationRace {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)

	race := &cancellationRace{
		cancel:    cancel,
		clock:     newSupervisorTestClock(),
		terminal:  newSupervisorTestTerminal(reader),
		recorder:  &cancellationRaceRecorder{},
		connector: newSupervisorTestConnector(connect),
		runErr:    make(chan error, 1),
	}
	race.supervisor = mustSupervisor(t, SupervisorConfig{
		Connector: race.connector,
		Terminal:  race.terminal,
		Clock:     race.clock,
		Jitter:    func() float64 { return 0 },
		Render:    race.recorder.render,
		Notify:    race.recorder.notify,
	})
	go func() { race.runErr <- race.supervisor.Run(ctx) }()
	return race
}

// assertSettledAsCancelled asserts one round's cancellation invariants: the run
// ends terminating with ctx.Err, raw mode is restored exactly once, no retry
// cadence was scheduled or notified, and every earlier render is the attempt that
// was still in flight. established marks a round whose connection was ready
// before the race, so its earlier ready render is expected.
func (r *cancellationRace) assertSettledAsCancelled(t *testing.T, round int, established bool) {
	t.Helper()
	require.ErrorIs(t, <-r.runErr, context.Canceled, "round %d: parent cancellation must settle the run", round)
	renders, notified := r.recorder.recorded()
	require.Empty(t, notified, "round %d: cancellation must not notify a connectivity failure", round)
	require.Zero(t, r.clock.timerCount(), "round %d: cancellation must not schedule a retry timer", round)
	require.Equal(t, 1, r.terminal.restoreCount(), "round %d: raw mode must be restored exactly once", round)
	require.NotEmpty(t, renders, "round %d: the run must render its state", round)
	require.Equal(t, PresentTerminating, renders[len(renders)-1].Presentation, "round %d: the run must end terminating", round)
	for _, state := range renders[:len(renders)-1] {
		require.Equal(t, PresentPicker, state.Presentation, "round %d: the run must stay in the picker", round)
		require.NotEqual(t, ConnectivityRetryWait, state.Connectivity,
			"round %d: no retry cadence may be published after the cancellation", round)
		if established {
			// The connection was ready before the race, so its ready render is the
			// only settled state the round expects.
			continue
		}
		require.Equal(t, ConnectivityConnectingBroker, state.Connectivity,
			"round %d: no settled connection may be published for the cancelled attempt", round)
	}
}

// cancellationRaceRecorder records what one round's supervisor published.
type cancellationRaceRecorder struct {
	mu       sync.Mutex
	renders  []State
	notified []error
}

func (r *cancellationRaceRecorder) render(state State) {
	r.mu.Lock()
	r.renders = append(r.renders, state)
	r.mu.Unlock()
}

func (r *cancellationRaceRecorder) notify(_ State, err error) {
	r.mu.Lock()
	r.notified = append(r.notified, err)
	r.mu.Unlock()
}

func (r *cancellationRaceRecorder) recorded() ([]State, []error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]State(nil), r.renders...), append([]error(nil), r.notified...)
}

// supervisorNilSub is a typed-nil subscription: it satisfies the interface but
// is nil underneath, so only the reflection guard keeps the supervisor from
// dereferencing it.
type supervisorNilSub struct{}

func (*supervisorNilSub) Changed() <-chan struct{} { return nil }
func (*supervisorNilSub) Close()                   {}

// supervisorNilSubService overrides Subscribe so a test can exercise the
// supervisor's nil-subscription guard without a live subscription.
type supervisorNilSubService struct {
	*supervisorTestService
	sub ports.BrokerSubscription
}

func (s *supervisorNilSubService) Subscribe() (ports.BrokerSubscription, error) {
	return s.sub, nil
}

// TestSupervisorDefensiveGenerationFenceRejectsSupersededAttempt pins the
// defensive generation fence directly: a settled attempt whose generation is not
// the current one is retired and discarded, while the current generation is
// adopted unchanged. Run joins every attempt before it advances, so no
// superseded completion can reach adoptAttempt in the production driver today;
// pinning the fence here keeps a future concurrent driver from dropping it.
func TestSupervisorDefensiveGenerationFenceRejectsSupersededAttempt(t *testing.T) {
	clock := newSupervisorTestClock()
	terminal := newSupervisorTestTerminal(newSupervisorTestReader())
	sup := mustSupervisor(t, SupervisorConfig{
		Connector: newSupervisorTestConnector(nil),
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
	})

	// Claim a generation exactly as Run does before it launches the connector.
	sup.transition(supervisorEvent{kind: supervisorBeginAttempt})
	current := sup.State().Generation
	require.Equal(t, uint64(1), current)

	service := newSupervisorTestService(ports.BrokerConnectionID{1})
	subClosed := make(chan struct{})
	stale := supervisorAttempt{
		generation: current - 1,
		service:    service,
		sub:        supervisorTestSubscription{closeFn: func() { close(subClosed) }},
	}
	adopted, cause := sup.adoptAttempt(context.Background(), stale)
	require.False(t, adopted, "a superseded generation must not be adopted")
	require.NoError(t, cause, "a superseded generation is not a cancellation")
	// Run retires every attempt it does not adopt.
	stale.retire()
	select {
	case <-service.closedCh():
	default:
		t.Fatal("a superseded service must be closed")
	}
	select {
	case <-subClosed:
	default:
		t.Fatal("a superseded subscription must be closed")
	}

	// The current generation is adopted, so its live service is left untouched.
	live := newSupervisorTestService(ports.BrokerConnectionID{2})
	adopted, cause = sup.adoptAttempt(context.Background(), supervisorAttempt{generation: current, service: live})
	require.True(t, adopted, "the current generation must be adopted")
	require.NoError(t, cause)
	select {
	case <-live.closedCh():
		t.Fatal("the current attempt must be adopted, not retired")
	default:
	}
}

// TestSupervisorNormalizesUntypedConnectFailure proves an untyped connect
// failure becomes a typed unavailable failure that preserves its cause, and that
// the same typed value reaches the notifier.
func TestSupervisorNormalizesUntypedConnectFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := newSupervisorTestClock()
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	terminal := newSupervisorTestTerminal(reader)

	cause := errors.New("dial: connection refused")
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		return nil, cause
	})
	notified := make(chan error, 4)
	sup := mustSupervisor(t, SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
		Notify:    func(_ State, err error) { notified <- err },
	})

	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()
	clock.awaitTimer(t)

	var typed ports.BrokerError
	require.ErrorAs(t, sup.State().Err, &typed)
	require.Equal(t, ports.BrokerErrorUnavailable, typed.Code)
	require.ErrorIs(t, typed.Cause, cause, "the untyped cause is preserved")

	select {
	case err := <-notified:
		var surfaced ports.BrokerError
		require.ErrorAs(t, err, &surfaced)
		require.Equal(t, ports.BrokerErrorUnavailable, surfaced.Code)
		require.ErrorIs(t, surfaced.Cause, cause)
	case <-time.After(5 * time.Second):
		t.Fatal("the normalized failure was not surfaced")
	}

	cancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
	require.Equal(t, 1, terminal.restoreCount())
}

// TestSupervisorNormalizesUntypedLoss proves an untyped established-connection
// loss is normalized to a typed unavailable failure that preserves its cause.
func TestSupervisorNormalizesUntypedLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := newSupervisorTestClock()
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	terminal := newSupervisorTestTerminal(reader)

	service := newSupervisorTestService(ports.BrokerConnectionID{1})
	service.publish(1, 1)
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		return service, nil
	})
	sup := mustSupervisor(t, SupervisorConfig{
		Connector: connector,
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
	})

	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()
	require.Equal(t, 1, connector.awaitStart(t))
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)

	cause := errors.New("carriage reset")
	service.lose(cause)
	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityRetryWait }, 5*time.Second, time.Millisecond)

	var typed ports.BrokerError
	require.ErrorAs(t, sup.State().Err, &typed)
	require.Equal(t, ports.BrokerErrorUnavailable, typed.Code)
	require.ErrorIs(t, typed.Cause, cause, "the untyped loss cause is preserved")

	cancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
}

// TestSupervisorNilSubscriptionIsTypedUnavailable proves a service that reports
// no subscription (a nil interface or a typed nil) is closed and treated as a
// typed unavailable failure instead of panicking on the first Changed call.
func TestSupervisorNilSubscriptionIsTypedUnavailable(t *testing.T) {
	cases := []struct {
		name string
		sub  ports.BrokerSubscription
	}{
		{"nil interface", nil},
		{"typed nil", (*supervisorNilSub)(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			clock := newSupervisorTestClock()
			reader := newSupervisorTestReader()
			t.Cleanup(reader.unblock)
			terminal := newSupervisorTestTerminal(reader)

			base := newSupervisorTestService(ports.BrokerConnectionID{7})
			svc := &supervisorNilSubService{supervisorTestService: base, sub: tc.sub}
			connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
				return svc, nil
			})
			sup := mustSupervisor(t, SupervisorConfig{
				Connector: connector,
				Terminal:  terminal,
				Clock:     clock,
				Jitter:    func() float64 { return 0 },
			})

			runErr := make(chan error, 1)
			go func() { runErr <- sup.Run(ctx) }()
			clock.awaitTimer(t)

			var typed ports.BrokerError
			require.ErrorAs(t, sup.State().Err, &typed)
			require.Equal(t, ports.BrokerErrorUnavailable, typed.Code)
			select {
			case <-base.closedCh():
			default:
				t.Fatal("a service that returned no subscription must be closed")
			}

			cancel()
			require.ErrorIs(t, <-runErr, context.Canceled)
			require.Equal(t, 1, terminal.restoreCount())
		})
	}
}

// TestSupervisorTestHubModelsSubscriptionContract pins the fake against
// ports.BrokerSubscription: each subscriber owns a distinct capacity-one
// channel that is never replaced or closed on publish, and a publication is a
// non-blocking offer so a parked subscriber can never busy-spin the publisher.
func TestSupervisorTestHubModelsSubscriptionContract(t *testing.T) {
	hub := newSupervisorTestHub()
	first := hub.subscribe(nil)
	second := hub.subscribe(nil)
	require.NotEqual(t, first.Changed(), second.Changed(), "each subscriber owns its own channel")

	hub.publish(ports.BrokerSnapshot{Epoch: 1, Revision: 1})
	for name, sub := range map[string]ports.BrokerSubscription{"first": first, "second": second} {
		select {
		case <-sub.Changed():
		default:
			t.Fatalf("subscriber %s must receive the publication", name)
		}
	}

	// A second publication while a wake is still buffered is a non-blocking
	// drop, and the channel identity is never replaced.
	channel := first.Changed()
	hub.publish(ports.BrokerSnapshot{Epoch: 1, Revision: 2})
	hub.publish(ports.BrokerSnapshot{Epoch: 1, Revision: 3})
	require.Equal(t, channel, first.Changed())
	select {
	case <-first.Changed():
	default:
		t.Fatal("a publication after the drained wake must offer a new wake")
	}

	first.Close()
	hub.publish(ports.BrokerSnapshot{Epoch: 1, Revision: 4})
	select {
	case <-first.Changed():
		t.Fatal("a closed subscription must not receive further wakes")
	default:
	}
	require.Equal(t, ports.BrokerRevision(4), hub.current().Revision)
}
