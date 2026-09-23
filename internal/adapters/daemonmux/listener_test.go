package daemonmux

import (
	"context"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// listenerTestDeadline bounds every eventually/select assertion on the listener
// tests. It is wall time and generous: the handshake budget under test is spent
// by advancing the deterministic clock, never by waiting it out.
const listenerTestDeadline = 5 * time.Second

// listenerIdleBudget is the handshake budget of every test that must never
// observe an expiry: only advancing the deterministic clock past that budget
// can expire a stream, so an unexpected expiry is a real failure instead of a
// slow assertion.
const listenerIdleBudget = time.Hour

// listenerClock is a deterministic ports.Clock/ports.Timer pair: time only
// moves when Advance is called, so a stream's absolute handshake deadline and
// its expiry are asserted exactly instead of racing wall time. Timers that come
// due at the same instant fire in creation order.
type listenerClock struct {
	mu     sync.Mutex
	now    time.Time
	order  uint64
	timers map[*listenerTimer]struct{}
}

func newListenerClock(start time.Time) *listenerClock {
	return &listenerClock{now: start, timers: make(map[*listenerTimer]struct{})}
}

func (c *listenerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *listenerClock) NewTimer(d time.Duration) ports.Timer {
	timer := &listenerTimer{clock: c, ch: make(chan time.Time, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	timer.arm(d)
	return timer
}

// Advance moves time forward and fires every timer that came due.
func (c *listenerClock) Advance(d time.Duration) {
	if d < 0 {
		panic("daemonmux test: listenerClock.Advance requires a non-negative duration")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for timer := range c.timers {
		if timer.when.After(c.now) {
			continue
		}
		timer.active = false
		delete(c.timers, timer)
		select {
		case timer.ch <- c.now:
		default:
		}
	}
}

// SetNow moves the clock to now without firing any due timer, so a test can
// drive a consumer's own deadline check in isolation from the watchdog
// goroutine. A later Advance still fires any timer whose deadline passed.
func (c *listenerClock) SetNow(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// timerCount reports how many timers are currently armed, so a test can wait
// until the watchdog is blocked on a deadline before moving the clock without
// firing it.
func (c *listenerClock) timerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

type listenerTimer struct {
	clock  *listenerClock
	ch     chan time.Time
	when   time.Time
	order  uint64
	active bool
}

func (t *listenerTimer) C() <-chan time.Time { return t.ch }

func (t *listenerTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if !t.active {
		return false
	}
	t.active = false
	delete(t.clock.timers, t)
	return true
}

func (t *listenerTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	if t.active {
		t.active = false
		delete(t.clock.timers, t)
	}
	select {
	case <-t.ch:
	default:
	}
	t.arm(d)
	return wasActive
}

// arm schedules the timer relative to the clock's current time. The caller
// holds clock.mu.
func (t *listenerTimer) arm(d time.Duration) {
	t.when = t.clock.now.Add(d)
	t.order = t.clock.order
	t.clock.order++
	if d <= 0 {
		t.active = false
		select {
		case t.ch <- t.clock.now:
		default:
		}
		return
	}
	t.active = true
	t.clock.timers[t] = struct{}{}
}

// listenerHarness joins one paired pump set, one broker-side connector, one
// deterministic clock, and one daemon-side listener under test.
type listenerHarness struct {
	t          *testing.T
	brokerPump *Pump
	daemonPump *Pump
	connector  *LogicalConnector
	listener   *Listener
	clock      *listenerClock
}

// newListenerHarness builds a listener over a fresh physical pump pair under an
// explicit handshake budget and accept-queue bound, driven by a deterministic
// clock. The budget is deliberately long: a test that wants an expiry advances
// the clock instead of waiting. The listener is constructed before the pumps
// start, because it registers the daemon pump's admission observer, which the
// pump refuses after Start.
func newListenerHarness(t *testing.T, budget time.Duration, queueLimit int) *listenerHarness {
	t.Helper()
	return newListenerHarnessWithCeilings(t, budget, queueLimit, DefaultMuxCeilings())
}

// newListenerHarnessWithCeilings builds the same harness under an explicit
// ceiling set, so a test can tighten the stream-count ceiling and prove a
// closed stream genuinely released its admission slot.
func newListenerHarnessWithCeilings(t *testing.T, budget time.Duration, queueLimit int, ceilings MuxCeilings) *listenerHarness {
	t.Helper()
	brokerPump, daemonPump, start := newPairedPumpsUnstarted(t, ceilings)
	clock := newListenerClock(time.Now())
	listener, err := newListener(daemonPump, budget, queueLimit, clock, localAcceptance(logicalTestPolicy()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	start()
	return &listenerHarness{
		t:          t,
		brokerPump: brokerPump,
		daemonPump: daemonPump,
		connector:  mustConnector(t, brokerPump),
		listener:   listener,
		clock:      clock,
	}
}

// open admits one broker-side logical stream and waits for the daemon's
// Opened, which the listener sends at acceptance.
func (h *listenerHarness) open(stream int) (*LogicalConnection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return h.connector.Open(ctx, controlRequest(stream))
}

func (h *listenerHarness) mustOpen(stream int) *LogicalConnection {
	h.t.Helper()
	connection, err := h.open(stream)
	require.NoError(h.t, err, "open stream %d", stream)
	return connection
}

// pending reports the admitted streams waiting in the accept queue.
func (h *listenerHarness) pending() int {
	h.listener.mu.Lock()
	defer h.listener.mu.Unlock()
	return len(h.listener.queue)
}

func (h *listenerHarness) awaitPending(count int) {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return h.pending() == count }, listenerTestDeadline, time.Millisecond,
		"accept queue never held %d admitted stream(s)", count)
}

// activeWatchdog reports how many admitted streams the deadline watchdog still
// tracks.
func (h *listenerHarness) activeWatchdog() int {
	h.listener.mu.Lock()
	defer h.listener.mu.Unlock()
	return len(h.listener.active)
}

// moveClock moves the clock forward without firing any pending watchdog timer,
// so a test can observe a consumer's own deadline check in isolation from the
// watchdog goroutine.
func (h *listenerHarness) moveClock(now time.Time) {
	h.clock.SetNow(now)
}

type acceptOutcome struct {
	conn *ListenerConnection
	err  error
}

// acceptAsync starts one Accept call; the outcome is delivered exactly once.
func (h *listenerHarness) acceptAsync() <-chan acceptOutcome {
	out := make(chan acceptOutcome, 1)
	go func() {
		raw, err := h.listener.Accept()
		if err != nil {
			out <- acceptOutcome{err: err}
			return
		}
		out <- acceptOutcome{conn: raw.(*ListenerConnection)}
	}()
	return out
}

func (h *listenerHarness) awaitAccept(out <-chan acceptOutcome) *ListenerConnection {
	h.t.Helper()
	select {
	case outcome := <-out:
		require.NoError(h.t, outcome.err)
		return outcome.conn
	case <-time.After(listenerTestDeadline):
		h.t.Fatal("listener: Accept did not return an admitted stream")
		return nil
	}
}

// servePongs answers every typed Ping of one accepted connection with exactly
// one Pong until the stream ends.
func servePongs(conn ports.ServerConnection) error {
	for {
		message, err := conn.ReceiveClient()
		if err != nil {
			return err
		}
		if _, ok := message.(protocol.Ping); !ok {
			continue
		}
		if err := conn.SendServer(protocol.Pong{}); err != nil {
			return err
		}
	}
}

// listenerRoundTrip proves one typed exchange on a logical connection.
func listenerRoundTrip(t *testing.T, stream ports.BrokerEnvelopeStream) {
	t.Helper()
	conn := asTyped(stream)
	require.NoError(t, conn.SendClient(protocol.Ping{}))
	message, err := conn.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)
}

// requireStreamState waits until one stream of the daemon pump reached state.
func requireStreamState(t *testing.T, pump *Pump, id PhysicalStreamID, state StreamState) StreamStatus {
	t.Helper()
	var status StreamStatus
	require.Eventually(t, func() bool {
		current, ok := pump.Engine().Status(id)
		if !ok || current.State != state {
			return false
		}
		status = current
		return true
	}, listenerTestDeadline, time.Millisecond, "daemon stream %d never reached %s", id, state)
	return status
}

// listenerDaemon is the test's daemon accept loop: it takes connections from
// the listener in acceptance order and serves each one with servePongs.
type listenerDaemon struct {
	listener *Listener

	mu    sync.Mutex
	conns []*ListenerConnection
}

func startListenerDaemon(l *Listener) *listenerDaemon {
	daemon := &listenerDaemon{listener: l}
	go func() {
		for {
			raw, err := l.Accept()
			if err != nil {
				return
			}
			connection := raw.(*ListenerConnection)
			daemon.mu.Lock()
			daemon.conns = append(daemon.conns, connection)
			daemon.mu.Unlock()
			go func() { _ = servePongs(connection) }()
		}
	}()
	return daemon
}

func (d *listenerDaemon) connection(index int) *ListenerConnection {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.conns[index]
}

func (d *listenerDaemon) await(t *testing.T, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return len(d.conns) >= count
	}, listenerTestDeadline, time.Millisecond, "daemon accepted %d of %d streams", count, count)
}

func TestNewListenerValidation(t *testing.T) {
	local := localAcceptance(testPolicy())
	t.Run("nil pump", func(t *testing.T) {
		listener, err := NewListener(nil, local)
		require.ErrorIs(t, err, ErrListenerConfig)
		require.Nil(t, listener)
	})
	t.Run("broker-side pump is refused", func(t *testing.T) {
		pump, _ := newTestPump(t, DirectionServer)
		listener, err := NewListener(pump, local)
		require.ErrorIs(t, err, ErrListenerConfig)
		require.Nil(t, listener)
	})
	t.Run("invalid accepted entry is refused", func(t *testing.T) {
		pump, _ := newTestPump(t, DirectionClient)
		listener, err := NewListener(pump, ServerPolicyAdmission{})
		require.ErrorIs(t, err, ErrListenerConfig)
		require.Nil(t, listener)
	})
	t.Run("daemon-side pump is accepted", func(t *testing.T) {
		pump, _ := newTestPump(t, DirectionClient)
		listener, err := NewListener(pump, local)
		require.NoError(t, err)
		require.NotNil(t, listener)
		require.Equal(t, "daemonmux", listener.Addr())
		require.NoError(t, listener.Close())
		_, err = listener.Accept()
		require.ErrorIs(t, err, ErrListenerClosed)
	})
	t.Run("accept queue is bounded by the stream ceiling", func(t *testing.T) {
		require.Equal(t, 128, MaxAcceptQueue)
		require.LessOrEqual(t, MaxAcceptQueue, int(MaxMuxStreams))
	})
}

// TestPumpAdmittedObserver proves the pump notifies its single admission
// observer for every admitted inbound Open, and never for a refused one.
func TestPumpAdmittedObserver(t *testing.T) {
	pump, carrier := newTestPump(t, DirectionClient)
	observed := make(chan Open, 4)
	require.NoError(t, pump.OnAdmitted(func(open Open) { observed <- open }))
	pump.Start(context.Background())

	deliverClientFrame(t, carrier, openFor(1))
	select {
	case open := <-observed:
		require.Equal(t, testRef(1), open.Ref)
		require.Equal(t, openFor(1), open, "the observer receives the decoded admission request")
	case <-time.After(listenerTestDeadline):
		t.Fatal("admission observer did not run")
	}

	// A refused admission (a reused physical identity) never notifies.
	deliverClientFrame(t, carrier, openFor(1))
	select {
	case open := <-observed:
		t.Fatalf("refused admission notified the observer: %+v", open)
	case <-time.After(50 * time.Millisecond):
	}

	deliverClientFrame(t, carrier, openFor(2))
	select {
	case open := <-observed:
		require.Equal(t, testRef(2), open.Ref)
	case <-time.After(listenerTestDeadline):
		t.Fatal("admission observer did not run for a fresh stream")
	}
}

// TestListenerTwoTypedAdmissions proves two admitted streams become two
// independent typed session channels over one physical pair, each with its own
// lifecycle.
func TestListenerTwoTypedAdmissions(t *testing.T) {
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	daemon := startListenerDaemon(h.listener)

	first := h.mustOpen(1)
	daemon.await(t, 1)
	second := h.mustOpen(2)
	daemon.await(t, 2)

	require.NotEqual(t, daemon.connection(0).Ref().Physical, daemon.connection(1).Ref().Physical)
	listenerRoundTrip(t, first)
	listenerRoundTrip(t, second)

	// Independence: closing one stream leaves the sibling and the physical
	// connection untouched.
	require.NoError(t, first.Close())
	require.True(t, channelClosed(first.Done()))
	require.Nil(t, first.Err())
	require.False(t, channelClosed(second.Done()))
	listenerRoundTrip(t, second)
	require.False(t, channelClosed(h.brokerPump.Done()))
	require.False(t, channelClosed(h.daemonPump.Done()))
	require.Equal(t, 1, h.daemonPump.Engine().Live())
}

// TestListenerHundredTypedAdmissions concurrently admits 100 independent typed
// streams over one physical pair, proves them all live before traffic, and
// interleaves a typed round trip on every one.
func TestListenerHundredTypedAdmissions(t *testing.T) {
	const count = 100
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	daemon := startListenerDaemon(h.listener)

	connections := make([]*LogicalConnection, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			connections[i], errs[i] = h.open(i + 1)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "stream %d", i+1)
	}
	daemon.await(t, count)

	// Every stream is admitted and live before any traffic, and the physical
	// connection is not terminal.
	require.Equal(t, count, h.daemonPump.Engine().Live())
	require.False(t, channelClosed(h.brokerPump.Done()))
	require.False(t, channelClosed(h.daemonPump.Done()))
	for i, connection := range connections {
		require.False(t, channelClosed(connection.Done()), "stream %d settled before traffic", i+1)
	}

	for _, connection := range connections {
		listenerRoundTrip(t, connection)
	}

	for i, connection := range connections {
		require.NoError(t, connection.Close(), "stream %d", i+1)
	}
	require.Eventually(t, func() bool { return h.daemonPump.Engine().Live() == 0 }, listenerTestDeadline, time.Millisecond)
	for i, connection := range connections {
		require.True(t, channelClosed(connection.Done()), "stream %d", i+1)
	}
	require.False(t, channelClosed(h.daemonPump.Done()))
}

// TestListenerBlockedAcceptAndReaderSiblingProgress proves a blocked Accept and
// a blocked reader never stall a sibling: an established stream keeps its typed
// round trips while the accept queue is empty, and while another accepted
// stream's reader is parked.
func TestListenerBlockedAcceptAndReaderSiblingProgress(t *testing.T) {
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)

	firstAccept := h.acceptAsync()
	first := h.mustOpen(1)
	daemonFirst := h.awaitAccept(firstAccept)
	go func() { _ = servePongs(daemonFirst) }()
	listenerRoundTrip(t, first)

	// The accept queue is empty: Accept is blocked, and a sibling still
	// exchanges typed traffic.
	probe := h.acceptAsync()
	select {
	case outcome := <-probe:
		t.Fatalf("Accept returned with an empty queue: %+v", outcome)
	default:
	}
	listenerRoundTrip(t, first)
	listenerRoundTrip(t, first)
	select {
	case outcome := <-probe:
		t.Fatalf("Accept returned with an empty queue: %+v", outcome)
	default:
	}

	// A newly admitted stream wakes the blocked Accept in admission order and
	// is delivered as its own typed connection. Its reader is parked, so it
	// never answers, while the first stream keeps progressing.
	second := h.mustOpen(2)
	daemonSecond := h.awaitAccept(probe)
	require.Equal(t, PhysicalStreamID(2), daemonSecond.Ref().Physical)
	blockedReader := make(chan error, 1)
	go func() {
		_, err := daemonSecond.ReceiveClient()
		blockedReader <- err
	}()
	listenerRoundTrip(t, first)
	select {
	case err := <-blockedReader:
		t.Fatalf("parked reader returned early: %v", err)
	default:
	}

	// Closing the parked stream unblocks exactly its reader.
	require.NoError(t, second.Close())
	select {
	case err := <-blockedReader:
		require.Error(t, err)
	case <-time.After(listenerTestDeadline):
		t.Fatal("closing the stream did not unblock its reader")
	}
	require.Eventually(t, func() bool { return channelClosed(daemonSecond.Done()) }, listenerTestDeadline, time.Millisecond)
	listenerRoundTrip(t, first)
}

// TestListenerQueueFullRefusesOnlyOffendingStream proves a stream admitted
// while the accept queue is full is refused and reset on its own stream, while
// the queued sibling and the physical connection are untouched.
func TestListenerQueueFullRefusesOnlyOffendingStream(t *testing.T) {
	h := newListenerHarness(t, listenerTestDeadline, 1)

	// The first stream is admitted but never accepted: it fills the queue.
	opened := make(chan *LogicalConnection, 1)
	openErrs := make(chan error, 1)
	go func() {
		connection, err := h.open(1)
		if err != nil {
			openErrs <- err
			return
		}
		opened <- connection
	}()
	h.awaitPending(1)

	// The second stream is refused on its own stream: one Reset, no queue
	// growth, no physical effect.
	_, err := h.open(2)
	require.Error(t, err)
	require.False(t, channelClosed(h.daemonPump.Done()))
	require.False(t, channelClosed(h.brokerPump.Done()))

	refused := requireStreamState(t, h.daemonPump, 2, StreamTerminal)
	require.Equal(t, domain.RemoteFailureTransport, refused.FailureKind)
	var detail ports.BrokerError
	require.ErrorAs(t, refused.Err, &detail)
	require.Equal(t, acceptQueueFullText, detail.Text)
	require.Equal(t, 1, h.daemonPump.Engine().Live(), "the queued sibling must stay live")

	// The queued sibling is still deliverable and fully usable.
	first := h.awaitAccept(h.acceptAsync())
	go func() { _ = servePongs(first) }()
	select {
	case connection := <-opened:
		listenerRoundTrip(t, connection)
	case err := <-openErrs:
		t.Fatalf("queued stream did not open: %v", err)
	case <-time.After(listenerTestDeadline):
		t.Fatal("queued stream did not open")
	}
	require.Equal(t, PhysicalStreamID(1), first.Ref().Physical)
}

// TestListenerHandshakeExpiryResetsOnlyStream proves one stream whose session
// handshake never ran is reset exactly at its admission deadline, while a
// sibling that completed its handshake stays alive and usable.
func TestListenerHandshakeExpiryResetsOnlyStream(t *testing.T) {
	const budget = 30 * time.Second
	h := newListenerHarness(t, budget, MaxAcceptQueue)

	firstAccept := h.acceptAsync()
	first := h.mustOpen(1)
	daemonFirst := h.awaitAccept(firstAccept)
	secondAccept := h.acceptAsync()
	second := h.mustOpen(2)
	daemonSecond := h.awaitAccept(secondAccept)
	go func() { _ = servePongs(daemonSecond) }()
	listenerRoundTrip(t, second)
	require.Equal(t, 2, h.daemonPump.Engine().Live())

	// The first stream's session handshake never runs: its one absolute
	// deadline expires, and only that stream is reset.
	h.clock.Advance(budget + time.Second)

	expired := requireStreamState(t, h.daemonPump, 1, StreamTerminal)
	require.Equal(t, domain.RemoteFailureTransport, expired.FailureKind)
	var detail ports.BrokerError
	require.ErrorAs(t, expired.Err, &detail)
	require.Equal(t, handshakeDeadlineText, detail.Text)
	require.Eventually(t, func() bool { return channelClosed(daemonFirst.Done()) }, listenerTestDeadline, time.Millisecond)
	require.Error(t, daemonFirst.Err())
	require.Equal(t, domain.RemoteFailureTransport, daemonFirst.FailureKind())

	// The broker observes the stream-local loss with its exact scope.
	require.Eventually(t, func() bool { return channelClosed(first.Done()) }, listenerTestDeadline, time.Millisecond)
	var loss ports.BrokerStreamLost
	require.ErrorAs(t, first.Err(), &loss)
	require.Equal(t, ports.BrokerStreamID(1), loss.Stream)
	require.Equal(t, domain.RemoteFailureTransport, loss.Cause)

	// The sibling is untouched: still open, still serving, physical alive.
	require.Equal(t, 1, h.daemonPump.Engine().Live())
	require.Equal(t, StreamOpen, requireStreamState(t, h.daemonPump, 2, StreamOpen).State)
	require.False(t, channelClosed(second.Done()))
	listenerRoundTrip(t, second)
	require.False(t, channelClosed(daemonSecond.Done()))
	require.False(t, channelClosed(h.brokerPump.Done()))
	require.False(t, channelClosed(h.daemonPump.Done()))
}

// TestListenerHandshakeCompletionCancelsWatchdog proves a completed session
// handshake retires the stream's watchdog: advancing past the deadline never
// resets a stream that already handshook.
func TestListenerHandshakeCompletionCancelsWatchdog(t *testing.T) {
	const budget = 30 * time.Second
	h := newListenerHarness(t, budget, MaxAcceptQueue)

	probe := h.acceptAsync()
	connection := h.mustOpen(1)
	daemonConnection := h.awaitAccept(probe)
	go func() { _ = servePongs(daemonConnection) }()
	listenerRoundTrip(t, connection)

	completed := make(chan struct{}, 1)
	daemonConnection.OnHandshakeComplete(func() { completed <- struct{}{} })
	select {
	case <-completed:
	case <-time.After(listenerTestDeadline):
		t.Fatal("session handshake completion was not published")
	}

	h.clock.Advance(2 * budget)

	require.Equal(t, StreamOpen, requireStreamState(t, h.daemonPump, 1, StreamOpen).State)
	require.False(t, channelClosed(connection.Done()))
	require.False(t, channelClosed(daemonConnection.Done()))
	require.Nil(t, daemonConnection.Err())
	listenerRoundTrip(t, connection)
	require.False(t, channelClosed(h.daemonPump.Done()))
}

// TestListenerDeadlineConsumesQueueDelay proves the accepted absolute deadline
// is fixed at Open admission, so queue delay spends the one budget instead of
// starting a second one.
func TestListenerDeadlineConsumesQueueDelay(t *testing.T) {
	const budget = 30 * time.Second
	h := newListenerHarness(t, budget, MaxAcceptQueue)
	admittedAt := h.clock.Now()

	opened := make(chan *LogicalConnection, 1)
	go func() {
		connection, err := h.open(1)
		if err == nil {
			opened <- connection
		}
	}()
	h.awaitPending(1)

	// Half the budget is spent while the stream waits in the accept queue.
	const queued = 15 * time.Second
	h.clock.Advance(queued)
	daemonConnection := h.awaitAccept(h.acceptAsync())

	require.Equal(t, admittedAt.Add(budget), daemonConnection.HandshakeDeadline())
	require.Equal(t, daemonConnection.HandshakeDeadline(), daemonConnection.HandshakeDeadline(), "the deadline is stable")
	require.True(t, daemonConnection.HandshakeDeadline().Before(h.clock.Now().Add(budget)),
		"accepting late must not start a second full budget")

	select {
	case connection := <-opened:
		go func() { _ = servePongs(daemonConnection) }()
		listenerRoundTrip(t, connection)
	case <-time.After(listenerTestDeadline):
		t.Fatal("queued stream did not open")
	}
}

// TestListenerWithdrawnPendingStreamIsSkipped proves a pending stream the peer
// withdrew is never handed to the daemon: Accept skips it and delivers the next
// admitted stream.
func TestListenerWithdrawnPendingStreamIsSkipped(t *testing.T) {
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := h.connector.Open(ctx, controlRequest(1))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	h.awaitPending(1)
	requireStreamState(t, h.daemonPump, 1, StreamTerminal)

	// The withdrawn stream is skipped; the next admitted stream is delivered.
	probe := h.acceptAsync()
	second := h.mustOpen(2)
	daemonConnection := h.awaitAccept(probe)
	require.Equal(t, PhysicalStreamID(2), daemonConnection.Ref().Physical)
	go func() { _ = servePongs(daemonConnection) }()
	listenerRoundTrip(t, second)
}

// TestListenerMalformedStreamDoesNotStopAccept proves a malformed inner session
// frame resets exactly its own stream and never surfaces as a fatal Accept
// error that would stop the shared daemon accept loop.
func TestListenerMalformedStreamDoesNotStopAccept(t *testing.T) {
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	daemon := startListenerDaemon(h.listener)

	first := h.mustOpen(1)
	daemon.await(t, 1)

	// The peer speaks garbage on the first frame of its stream.
	require.NoError(t, h.brokerPump.Send(Data{Physical: 1, Data: []byte("not-a-session-frame")}))

	malformed := requireStreamState(t, h.daemonPump, 1, StreamTerminal)
	require.Equal(t, domain.RemoteFailureInvalidResponse, malformed.FailureKind)
	require.Eventually(t, func() bool { return channelClosed(first.Done()) }, listenerTestDeadline, time.Millisecond)

	// The accept loop survived: a fresh stream is admitted and served.
	second := h.mustOpen(2)
	daemon.await(t, 2)
	listenerRoundTrip(t, second)
	require.False(t, channelClosed(h.brokerPump.Done()))
	require.False(t, channelClosed(h.daemonPump.Done()))
}

// TestListenerPhysicalLossUnblocksAll proves a physical loss fails the blocked
// Accept with its stable fatal outcome and unblocks every delivered stream.
func TestListenerPhysicalLossUnblocksAll(t *testing.T) {
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)

	connections := make([]*LogicalConnection, 0, 2)
	daemonConnections := make([]*ListenerConnection, 0, 2)
	for i := 1; i <= 2; i++ {
		probe := h.acceptAsync()
		connection := h.mustOpen(i)
		daemonConnection := h.awaitAccept(probe)
		connections = append(connections, connection)
		daemonConnections = append(daemonConnections, daemonConnection)
	}

	readers := make([]chan error, 0, 2)
	for _, daemonConnection := range daemonConnections {
		blocked := make(chan error, 1)
		readers = append(readers, blocked)
		conn := daemonConnection
		go func() {
			_, err := conn.ReceiveClient()
			blocked <- err
		}()
	}
	blockedAccept := h.acceptAsync()

	// Failing the broker carrier is a physical loss on the daemon side.
	require.NoError(t, h.brokerPump.Close())
	require.Eventually(t, func() bool { return channelClosed(h.daemonPump.Done()) }, listenerTestDeadline, time.Millisecond)
	require.Error(t, h.daemonPump.Err(), "the daemon pump must publish the carrier failure")

	select {
	case outcome := <-blockedAccept:
		require.ErrorIs(t, outcome.err, ErrListenerLost)
		require.ErrorIs(t, outcome.err, h.daemonPump.Err(), "the fatal Accept outcome wraps the pump cause")
	case <-time.After(listenerTestDeadline):
		t.Fatal("physical loss did not unblock Accept")
	}
	for i, blocked := range readers {
		select {
		case err := <-blocked:
			require.Error(t, err, "stream %d", i+1)
		case <-time.After(listenerTestDeadline):
			t.Fatalf("physical loss did not unblock stream %d", i+1)
		}
		require.Eventually(t, func() bool { return channelClosed(daemonConnections[i].Done()) }, listenerTestDeadline, time.Millisecond, "stream %d", i+1)
		require.Error(t, daemonConnections[i].Err(), "stream %d", i+1)
		require.Equal(t, domain.RemoteFailureTransport, daemonConnections[i].FailureKind(), "stream %d", i+1)
	}
	for i, connection := range connections {
		require.Eventually(t, func() bool { return channelClosed(connection.Done()) }, listenerTestDeadline, time.Millisecond, "stream %d", i+1)
	}

	// The fatal outcome is stable, so a later Accept cannot restart the loop.
	_, err := h.listener.Accept()
	require.ErrorIs(t, err, ErrListenerLost)
}

// TestListenerCloseUnblocksAcceptAndJoinsGoroutines proves Close unblocks every
// blocked Accept, is idempotent, and leaves no listener goroutine or pump watch
// entry behind.
func TestListenerCloseUnblocksAcceptAndJoinsGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	probe := h.acceptAsync()
	connection := h.mustOpen(1)
	daemonConnection := h.awaitAccept(probe)
	go func() { _ = servePongs(daemonConnection) }()
	listenerRoundTrip(t, connection)

	blocked := h.acceptAsync()
	select {
	case outcome := <-blocked:
		t.Fatalf("Accept returned before Close: %+v", outcome)
	default:
	}

	require.NoError(t, h.listener.Close())
	require.NoError(t, h.listener.Close(), "Close is idempotent")
	select {
	case outcome := <-blocked:
		require.ErrorIs(t, outcome.err, ErrListenerClosed)
	case <-time.After(listenerTestDeadline):
		t.Fatal("Close did not unblock Accept")
	}
	_, err := h.listener.Accept()
	require.ErrorIs(t, err, ErrListenerClosed)

	require.NoError(t, connection.Close())
	require.Eventually(t, func() bool { return channelClosed(daemonConnection.Done()) }, listenerTestDeadline, time.Millisecond)
	require.NoError(t, h.brokerPump.Close())
	require.NoError(t, h.daemonPump.Close())

	require.Eventually(t, func() bool { return !watchRegistered(h.daemonPump, 1) }, listenerTestDeadline, time.Millisecond,
		"listener connection leaked a pump watch entry")
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+3 }, listenerTestDeadline, time.Millisecond,
		"listener goroutines leaked")
}

// TestListenerClosedListenerRefusesFreshStreams proves a listener closed while
// its physical connection is still live refuses a later admission on that
// stream alone.
func TestListenerClosedListenerRefusesFreshStreams(t *testing.T) {
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	require.NoError(t, h.listener.Close())

	_, err := h.open(1)
	require.Error(t, err)
	require.False(t, channelClosed(h.daemonPump.Done()))
	require.False(t, channelClosed(h.brokerPump.Done()))
	refused := requireStreamState(t, h.daemonPump, 1, StreamTerminal)
	require.Equal(t, domain.RemoteFailureTransport, refused.FailureKind)
	var detail ports.BrokerError
	require.ErrorAs(t, refused.Err, &detail)
	require.Equal(t, listenerClosedText, detail.Text)
	require.Equal(t, 0, h.daemonPump.Engine().Live())
}

// TestListenerConnectionAdoptsAdmissionDeadline proves the typed session
// connection of one accepted stream spends the stream's admission budget and
// never starts a second one: a stream whose session handshake never runs fails
// its preamble after the short admission budget, and only that stream is reset.
func TestListenerConnectionAdoptsAdmissionDeadline(t *testing.T) {
	const budget = 300 * time.Millisecond
	h := newListenerHarness(t, budget, MaxAcceptQueue)

	probe := h.acceptAsync()
	started := time.Now()
	connection := h.mustOpen(1)
	daemonConnection := h.awaitAccept(probe)

	// The peer never starts its session preamble, so the accepted connection's
	// own deadline is what ends the read.
	_, err := daemonConnection.ReceiveClient()
	require.ErrorIs(t, err, sessionwire.ErrPreambleTimeout)
	require.Less(t, time.Since(started), listenerTestDeadline,
		"the connection started a second handshake budget instead of adopting the admission deadline")

	malformed := requireStreamState(t, h.daemonPump, 1, StreamTerminal)
	require.Equal(t, domain.RemoteFailureInvalidResponse, malformed.FailureKind)
	require.Eventually(t, func() bool { return channelClosed(daemonConnection.Done()) }, listenerTestDeadline, time.Millisecond)
	require.Eventually(t, func() bool { return channelClosed(connection.Done()) }, listenerTestDeadline, time.Millisecond)
	require.False(t, channelClosed(h.brokerPump.Done()))
	require.False(t, channelClosed(h.daemonPump.Done()))

	// The failed handshake leaves no live stream and no watchdog entry behind.
	require.Equal(t, 0, h.daemonPump.Engine().Live())
	require.Eventually(t, func() bool { return h.activeWatchdog() == 0 }, listenerTestDeadline, time.Millisecond,
		"the failed handshake leaked a watchdog entry")
}

// TestListenerConnectionPreambleFailureLeavesNoResidue proves a session
// preamble that fails on the daemon side - the peer's first frame is not a
// valid preamble - settles only its own stream: no live stream and no watchdog
// entry are left behind, and a sibling stream is still accepted and served over
// the live physical connection.
func TestListenerConnectionPreambleFailureLeavesNoResidue(t *testing.T) {
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)

	probe := h.acceptAsync()
	connection := h.mustOpen(1)
	daemonConnection := h.awaitAccept(probe)

	// The peer's first frame is not a valid session preamble; the failed
	// handshake closes the stream's carriage.
	require.NoError(t, h.brokerPump.Send(Data{Physical: 1, Data: []byte("not-a-session-preamble")}))
	_, err := daemonConnection.ReceiveClient()
	require.Error(t, err)

	failed := requireStreamState(t, h.daemonPump, 1, StreamTerminal)
	require.Equal(t, domain.RemoteFailureInvalidResponse, failed.FailureKind)
	require.Eventually(t, func() bool { return channelClosed(daemonConnection.Done()) }, listenerTestDeadline, time.Millisecond)
	require.Eventually(t, func() bool { return channelClosed(connection.Done()) }, listenerTestDeadline, time.Millisecond)
	require.Equal(t, 0, h.daemonPump.Engine().Live())
	require.Eventually(t, func() bool { return h.activeWatchdog() == 0 }, listenerTestDeadline, time.Millisecond,
		"the failed handshake leaked a watchdog entry")

	siblingProbe := h.acceptAsync()
	sibling := h.mustOpen(2)
	siblingDaemon := h.awaitAccept(siblingProbe)
	go func() { _ = servePongs(siblingDaemon) }()
	listenerRoundTrip(t, sibling)
	require.False(t, channelClosed(h.brokerPump.Done()))
	require.False(t, channelClosed(h.daemonPump.Done()))
}

// TestListenerConnectionCarriageErrorsAreStreamLocal proves the typed
// connection's own error paths stay stream-local: a settled connection reports
// its stable cause, and an orderly peer close reports io.EOF.
func TestListenerConnectionCarriageErrorsAreStreamLocal(t *testing.T) {
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)

	probe := h.acceptAsync()
	connection := h.mustOpen(1)
	daemonConnection := h.awaitAccept(probe)

	// An orderly peer close unblocks the daemon reader with io.EOF and a nil
	// terminal cause.
	require.NoError(t, connection.Close())
	_, err := daemonConnection.ReceiveClient()
	require.ErrorIs(t, err, io.EOF)
	require.Eventually(t, func() bool { return channelClosed(daemonConnection.Done()) }, listenerTestDeadline, time.Millisecond)
	require.Nil(t, daemonConnection.Err())
	require.Equal(t, domain.RemoteFailureNone, daemonConnection.FailureKind())

	// The settled connection keeps reporting its stable outcome.
	_, err = daemonConnection.ReceiveClient()
	require.Error(t, err)
	require.NoError(t, daemonConnection.Close())
	require.NoError(t, daemonConnection.Close())
}

// TestListenerConnectionInternalCarriageCloseSettlesStream proves a listener
// connection whose inner session carriage closes on its own during the
// handshake - the sessionwire preamble failing closes the raw transport - is
// settled without any consumer read or Close: exactly one stream-local Reset is
// issued, the admission slot and watchdog are released, and a sibling stream is
// still accepted over the live physical connection.
func TestListenerConnectionInternalCarriageCloseSettlesStream(t *testing.T) {
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)

	probe := h.acceptAsync()
	connection := h.mustOpen(1)
	daemonConnection := h.awaitAccept(probe)

	// No consumer touches the connection: the internal carriage close alone must
	// settle it, publish its Done/Err, and release the slot and watchdog.
	require.NoError(t, daemonConnection.raw.Close())

	closed := requireStreamState(t, h.daemonPump, 1, StreamTerminal)
	require.Equal(t, domain.RemoteFailureInvalidResponse, closed.FailureKind)
	var detail ports.BrokerError
	require.ErrorAs(t, closed.Err, &detail)
	require.Equal(t, sessionCarriageClosedText, detail.Text)
	require.Eventually(t, func() bool { return channelClosed(daemonConnection.Done()) }, listenerTestDeadline, time.Millisecond)
	require.Error(t, daemonConnection.Err())
	require.Equal(t, domain.RemoteFailureInvalidResponse, daemonConnection.FailureKind())
	require.Equal(t, 0, h.daemonPump.Engine().Live())
	require.Eventually(t, func() bool { return h.activeWatchdog() == 0 }, listenerTestDeadline, time.Millisecond,
		"the closed carriage leaked a watchdog entry")

	// The peer observes the stream-local Reset and its Open fails, while the
	// physical connection stays live.
	require.Eventually(t, func() bool { return channelClosed(connection.Done()) }, listenerTestDeadline, time.Millisecond)
	require.Error(t, connection.Err())
	require.False(t, channelClosed(h.brokerPump.Done()))
	require.False(t, channelClosed(h.daemonPump.Done()))

	// A sibling is still accepted and served.
	probe2 := h.acceptAsync()
	sibling := h.mustOpen(2)
	siblingDaemon := h.awaitAccept(probe2)
	go func() { _ = servePongs(siblingDaemon) }()
	listenerRoundTrip(t, sibling)
}

// TestListenerConnectionCloseDiscardsQueuedInboundReleasesAccounting proves a
// daemon-side local Close first discards the inbound chunks the engine already
// accepted for its stream: the engine's orderly Close reaches its terminal
// state, releasing both the stream's admission slot and its queued aggregate
// bytes, retiring its watchdog, and leaving the broker with exactly one orderly
// mux Close instead of a leak or a Reset. Every cycle closes with one queued
// inbound chunk under a two-stream ceiling, so a leaked slot would refuse the
// third admission.
func TestListenerConnectionCloseDiscardsQueuedInboundReleasesAccounting(t *testing.T) {
	ceilings := DefaultMuxCeilings()
	ceilings.MaxStreams = 2
	h := newListenerHarnessWithCeilings(t, listenerIdleBudget, MaxAcceptQueue, ceilings)

	chunk := []byte("queued")
	const cycles = 3
	for i := 1; i <= cycles; i++ {
		id := PhysicalStreamID(i)
		probe := h.acceptAsync()
		connection := h.mustOpen(i)
		daemonConnection := h.awaitAccept(probe)

		// The broker sent one chunk the daemon never reads, so the stream holds
		// accepted inbound data when the local Close arrives.
		require.NoError(t, h.brokerPump.Send(Data{Physical: id, Data: chunk}))
		requireEngineEventually(t, h.daemonPump, func(e *StreamEngine) bool { return e.AggregateBytes() > 0 },
			"cycle %d: the accepted chunk was never queued", i)

		require.NoError(t, daemonConnection.Close())
		require.NoError(t, daemonConnection.Close(), "Close is idempotent")
		require.True(t, channelClosed(daemonConnection.Done()))
		require.NoError(t, daemonConnection.Err())

		closed := mustStatus(t, h.daemonPump.Engine(), id)
		require.Equal(t, StreamTerminal, closed.State, "cycle %d: the closed stream must be terminal", i)
		require.NoError(t, closed.Err, "cycle %d: an orderly local close carries no cause", i)
		require.Zero(t, closed.QueuedChunks, "cycle %d", i)
		require.Zero(t, h.daemonPump.Engine().Live(), "cycle %d: a closing stream leaked its admission slot", i)
		require.Zero(t, h.daemonPump.Engine().AggregateBytes(), "cycle %d: a closing stream leaked its queued bytes", i)
		require.Eventually(t, func() bool { return h.activeWatchdog() == 0 }, listenerTestDeadline, time.Millisecond,
			"cycle %d: the closed connection leaked a watchdog entry", i)

		// The broker observes one orderly mux Close, never a stream-local Reset,
		// and the physical connection keeps flowing into the next cycle.
		require.Eventually(t, func() bool { return channelClosed(connection.Done()) }, listenerTestDeadline, time.Millisecond,
			"cycle %d: the broker never observed the mux Close", i)
		require.NoError(t, connection.Err(), "cycle %d: the mux Close must stay orderly", i)
		require.False(t, channelClosed(h.brokerPump.Done()))
		require.False(t, channelClosed(h.daemonPump.Done()))
	}
}

// TestListenerDeliverSkipsExpiredAdmissionWithoutWatchdog proves Accept settles
// an admission whose absolute deadline already elapsed even when the watchdog
// goroutine has not run: the clock is moved past the deadline without firing the
// watchdog timer, yet delivering the stream still aborts it and accepts the
// next one.
func TestListenerDeliverSkipsExpiredAdmissionWithoutWatchdog(t *testing.T) {
	const budget = 30 * time.Second
	h := newListenerHarness(t, budget, MaxAcceptQueue)

	expired := make(chan error, 1)
	go func() {
		_, err := h.open(1)
		expired <- err
	}()
	h.awaitPending(1)

	// Wait until the watchdog is blocked on stream 1's timer, then move the
	// clock past the deadline without firing that timer: only the consumer's own
	// deadline check can settle the stream here.
	require.Eventually(t, func() bool { return h.clock.timerCount() == 1 }, listenerTestDeadline, time.Millisecond,
		"watchdog never armed the admission deadline")
	h.moveClock(h.clock.Now().Add(budget + time.Second))

	probe := h.acceptAsync()

	requireStreamState(t, h.daemonPump, 1, StreamTerminal)
	select {
	case err := <-expired:
		require.Error(t, err)
	case <-time.After(listenerTestDeadline):
		t.Fatal("an expired admission was never aborted")
	}
	expiredStatus := mustStatus(t, h.daemonPump.Engine(), 1)
	var detail ports.BrokerError
	require.ErrorAs(t, expiredStatus.Err, &detail)
	require.Equal(t, handshakeDeadlineText, detail.Text)
	require.Equal(t, 0, h.daemonPump.Engine().Live())
	require.Equal(t, 0, h.activeWatchdog())

	sibling := h.mustOpen(2)
	siblingDaemon := h.awaitAccept(probe)
	require.Equal(t, PhysicalStreamID(2), siblingDaemon.Ref().Physical)
	go func() { _ = servePongs(siblingDaemon) }()
	listenerRoundTrip(t, sibling)
}

// TestListenerCloseDrainsQueuedAdmissions proves Close settles every
// admitted-but-unaccepted stream on its own: each broker Open is unblocked with
// a stream-local error, each admission slot is released, the queue is cleared,
// and the watchdog holds nothing afterwards.
func TestListenerCloseDrainsQueuedAdmissions(t *testing.T) {
	const queued = 3
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)

	opens := make([]chan error, queued)
	for i := 0; i < queued; i++ {
		opens[i] = make(chan error, 1)
		stream := i + 1
		go func() {
			_, err := h.open(stream)
			opens[i] <- err
		}()
	}
	h.awaitPending(queued)

	require.NoError(t, h.listener.Close())

	for i := 0; i < queued; i++ {
		select {
		case err := <-opens[i]:
			require.Error(t, err, "queued open %d must fail when the listener closes", i+1)
		case <-time.After(listenerTestDeadline):
			t.Fatalf("Close did not unblock queued open %d", i+1)
		}
	}
	require.Equal(t, 0, h.daemonPump.Engine().Live(), "Close must release every admission slot")
	require.Equal(t, 0, h.activeWatchdog(), "Close must clear the deadline watchdog")
	h.listener.mu.Lock()
	require.Empty(t, h.listener.queue)
	h.listener.mu.Unlock()
	require.False(t, channelClosed(h.daemonPump.Done()))
	require.False(t, channelClosed(h.brokerPump.Done()))
}

// TestListenerConcurrentCloseDrainsOnce proves concurrent Close calls are all
// successful and drain the queued admissions exactly once.
func TestListenerConcurrentCloseDrainsOnce(t *testing.T) {
	const queued = 4
	h := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)

	opens := make([]chan error, queued)
	for i := 0; i < queued; i++ {
		opens[i] = make(chan error, 1)
		stream := i + 1
		go func() {
			_, err := h.open(stream)
			opens[i] <- err
		}()
	}
	h.awaitPending(queued)

	const closers = 8
	errs := make([]error, closers)
	var wg sync.WaitGroup
	for i := 0; i < closers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = h.listener.Close()
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "closer %d", i)
	}
	for i := 0; i < queued; i++ {
		select {
		case err := <-opens[i]:
			require.Error(t, err, "queued open %d must fail when the listener closes", i+1)
		case <-time.After(listenerTestDeadline):
			t.Fatalf("concurrent Close did not unblock queued open %d", i+1)
		}
	}
	require.Equal(t, 0, h.daemonPump.Engine().Live())
	require.Equal(t, 0, h.activeWatchdog())
}

// TestPumpOnAdmittedRejectsLateRegistration proves the pump refuses an
// admission-observer registration after Start, so a listener can never miss an
// already-admitted Open.
func TestPumpOnAdmittedRejectsLateRegistration(t *testing.T) {
	pump, _ := newTestPump(t, DirectionClient)
	require.NoError(t, pump.OnAdmitted(func(Open) {}))
	pump.Start(context.Background())
	require.ErrorIs(t, pump.OnAdmitted(func(Open) {}), ErrAdmissionObserverLate)
	require.ErrorIs(t, pump.OnAdmitted(nil), ErrAdmissionObserverLate)

	var nilPump *Pump
	require.ErrorIs(t, nilPump.OnAdmitted(func(Open) {}), ErrPumpConfig)
}

// TestNewListenerRefusesStartedPump proves the listener constructor surfaces a
// late admission-observer registration: it must be constructed before the pump
// starts.
func TestNewListenerRefusesStartedPump(t *testing.T) {
	pump, _ := newTestPump(t, DirectionClient)
	pump.Start(context.Background())

	listener, err := NewListener(pump, localAcceptance(testPolicy()))
	require.ErrorIs(t, err, ErrListenerConfig)
	require.ErrorIs(t, err, ErrAdmissionObserverLate)
	require.Nil(t, listener)
}
