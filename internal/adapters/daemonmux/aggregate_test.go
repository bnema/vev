package daemonmux

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

// aggregateSink is the test's daemon-style consumer: it takes typed connections
// from the aggregate until Accept fails and serves each one with servePongs.
type aggregateSink struct {
	mu    sync.Mutex
	conns []ports.ServerConnection
	errs  []error
}

func startAggregateSink(a *AggregateListener) *aggregateSink {
	sink := &aggregateSink{}
	go func() {
		for {
			connection, err := a.Accept()
			if err != nil {
				sink.mu.Lock()
				sink.errs = append(sink.errs, err)
				sink.mu.Unlock()
				return
			}
			sink.mu.Lock()
			sink.conns = append(sink.conns, connection)
			sink.mu.Unlock()
			go func() { _ = servePongs(connection) }()
		}
	}()
	return sink
}

func (s *aggregateSink) await(t *testing.T, count int) []ports.ServerConnection {
	t.Helper()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.conns) >= count
	}, listenerTestDeadline, time.Millisecond, "aggregate accepted %d of %d connections", count, count)
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ports.ServerConnection, len(s.conns))
	copy(out, s.conns)
	return out
}

func (s *aggregateSink) errors() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errs...)
}

// aggregateChildCount reports the children the aggregate still forwards from.
func aggregateChildCount(a *AggregateListener) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.children)
}

// aggregateQueued reports the accepted-but-undelivered connections the
// aggregate holds.
func aggregateQueued(a *AggregateListener) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.queue)
}

// TestNewAggregateListenerValidation covers the nil-receiver and registration
// guards without any physical listener.
func TestNewAggregateListenerValidation(t *testing.T) {
	var nilAggregate *AggregateListener
	_, err := nilAggregate.Accept()
	require.ErrorIs(t, err, ErrAggregateConfig)
	require.ErrorIs(t, nilAggregate.Register(nil), ErrAggregateConfig)
	require.ErrorIs(t, nilAggregate.Remove(nil), ErrAggregateConfig)
	require.NoError(t, nilAggregate.Close())

	a := newAggregateListener(MaxAggregateQueue)
	require.Equal(t, "daemonmux", a.Addr())
	require.Equal(t, MaxAggregateQueue, a.limit)
	require.ErrorIs(t, a.Register(nil), ErrAggregateConfig)

	first := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	second := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	require.NoError(t, a.Register(first.listener))
	require.ErrorIs(t, a.Register(first.listener), ErrAggregateChildTaken)
	require.NoError(t, a.Close())
	require.ErrorIs(t, a.Register(second.listener), ErrAggregateClosed)
	_, err = a.Accept()
	require.ErrorIs(t, err, ErrAggregateClosed)
}

// TestAggregateTwoPhysicalChildrenAdmitIndependently proves the aggregate
// delivers both children's independently accepted typed connections through one
// ports.ServerListener, and that one child's stream lifecycle never touches the
// other child or its physical carriage.
func TestAggregateTwoPhysicalChildrenAdmitIndependently(t *testing.T) {
	a := newAggregateListener(MaxAggregateQueue)
	t.Cleanup(func() { _ = a.Close() })

	first := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	second := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	require.NoError(t, a.Register(first.listener))
	require.NoError(t, a.Register(second.listener))

	sink := startAggregateSink(a)
	firstBroker := first.mustOpen(1)
	secondBroker := second.mustOpen(1)
	accepted := sink.await(t, 2)

	// Each physical child contributed exactly one typed daemonmux connection.
	pumps := map[*Pump]bool{}
	for _, connection := range accepted {
		listenerConnection, ok := connection.(*ListenerConnection)
		require.True(t, ok, "the aggregate must forward the child's typed daemonmux connection")
		pumps[listenerConnection.pump] = true
	}
	require.True(t, pumps[first.daemonPump], "the first child never contributed a connection")
	require.True(t, pumps[second.daemonPump], "the second child never contributed a connection")
	require.Len(t, pumps, 2)

	listenerRoundTrip(t, firstBroker)
	listenerRoundTrip(t, secondBroker)

	// One stream ending leaves its sibling and both physical carriages live.
	require.NoError(t, firstBroker.Close())
	require.True(t, channelClosed(firstBroker.Done()))
	listenerRoundTrip(t, secondBroker)
	require.False(t, channelClosed(first.daemonPump.Done()))
	require.False(t, channelClosed(second.daemonPump.Done()))
	require.Empty(t, sink.errors())
}

// TestAggregateChildLossDoesNotStopAccept proves a physical loss of one child
// is isolated: the aggregate deregisters it without a terminal error, keeps
// accepting from the surviving child, and accepts from a child registered after
// the loss.
func TestAggregateChildLossDoesNotStopAccept(t *testing.T) {
	a := newAggregateListener(MaxAggregateQueue)
	t.Cleanup(func() { _ = a.Close() })

	first := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	second := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	require.NoError(t, a.Register(first.listener))
	require.NoError(t, a.Register(second.listener))

	sink := startAggregateSink(a)
	firstBroker := first.mustOpen(1)
	secondBroker := second.mustOpen(1)
	sink.await(t, 2)
	listenerRoundTrip(t, firstBroker)
	listenerRoundTrip(t, secondBroker)

	// Failing the first physical carriage makes its child's Accept terminal.
	require.NoError(t, first.brokerPump.Close())
	require.Eventually(t, func() bool { return channelClosed(first.daemonPump.Done()) }, listenerTestDeadline, time.Millisecond)

	// The aggregate isolates the loss: the child is deregistered and the loss
	// is never surfaced to the shared accept loop.
	require.Eventually(t, func() bool { return aggregateChildCount(a) == 1 }, listenerTestDeadline, time.Millisecond,
		"the lost child was not deregistered")
	require.Empty(t, sink.errors(), "a child loss must not surface as an aggregate Accept error")

	// The surviving physical child still admits.
	survivorBroker := second.mustOpen(2)
	survivorDaemon := sink.await(t, 3)[2].(*ListenerConnection)
	require.Equal(t, second.daemonPump, survivorDaemon.pump)
	listenerRoundTrip(t, survivorBroker)

	// A child registered after the loss also admits.
	third := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	require.NoError(t, a.Register(third.listener))
	thirdBroker := third.mustOpen(1)
	thirdDaemon := sink.await(t, 4)[3].(*ListenerConnection)
	require.Equal(t, third.daemonPump, thirdDaemon.pump)
	listenerRoundTrip(t, thirdBroker)
	require.Empty(t, sink.errors())
}

// TestAggregateCloseDrainsQueuedConnections proves Close unblocks Accept,
// closes the owned child, joins the forwarder, and cleans every connection it
// was holding: the queued ones and the single in-flight one blocked on the
// bounded queue.
func TestAggregateCloseDrainsQueuedConnections(t *testing.T) {
	const limit = 2
	a := newAggregateListener(limit)
	child := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	require.NoError(t, a.Register(child.listener))

	// Three admitted connections, none accepted: the aggregate holds its
	// bounded two, and the forwarder holds the third while backpressured.
	first := child.mustOpen(1)
	second := child.mustOpen(2)
	third := child.mustOpen(3)
	require.Eventually(t, func() bool { return aggregateQueued(a) == limit }, listenerTestDeadline, time.Millisecond,
		"the aggregate queue never filled to its bound")

	blocked := make(chan error, 1)
	go func() {
		_, err := a.Accept()
		blocked <- err
	}()

	require.NoError(t, a.Close())
	require.NoError(t, a.Close(), "Close is idempotent")

	select {
	case err := <-blocked:
		require.ErrorIs(t, err, ErrAggregateClosed)
	case <-time.After(listenerTestDeadline):
		t.Fatal("Close did not unblock Accept")
	}
	_, err := a.Accept()
	require.ErrorIs(t, err, ErrAggregateClosed)
	require.Zero(t, aggregateQueued(a), "Close must clear the aggregate queue")
	require.Zero(t, aggregateChildCount(a), "Close must drop every owned child")

	// Every connection the aggregate held is released, including the one its
	// forwarder was holding while backpressured; the child itself is closed.
	for i, broker := range []*LogicalConnection{first, second, third} {
		require.Eventually(t, func() bool { return channelClosed(broker.Done()) }, listenerTestDeadline, time.Millisecond,
			"queued connection %d was not cleaned by Close", i+1)
	}
	_, err = child.listener.Accept()
	require.ErrorIs(t, err, ErrListenerClosed)
}

// TestAggregateRemoveStopsChildOnly proves Remove closes and joins exactly the
// removed child, leaves the aggregate accepting from a sibling, and reports an
// unknown child afterwards.
func TestAggregateRemoveStopsChildOnly(t *testing.T) {
	a := newAggregateListener(MaxAggregateQueue)
	t.Cleanup(func() { _ = a.Close() })

	first := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	second := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	require.NoError(t, a.Register(first.listener))
	require.NoError(t, a.Register(second.listener))
	sink := startAggregateSink(a)

	require.NoError(t, a.Remove(first.listener))
	require.Equal(t, 1, aggregateChildCount(a))
	_, err := first.listener.Accept()
	require.ErrorIs(t, err, ErrListenerClosed)
	require.ErrorIs(t, a.Remove(first.listener), ErrAggregateUnknownChild)

	broker := second.mustOpen(1)
	sink.await(t, 1)
	listenerRoundTrip(t, broker)
	require.Empty(t, sink.errors())
}

// TestAggregateRemoveUnblocksBackpressuredForwarder proves Remove does not
// deadlock when the removed child's forwarder is waiting for queue space: it
// releases the in-flight connection, while a connection already queued stays
// deliverable to the daemon.
func TestAggregateRemoveUnblocksBackpressuredForwarder(t *testing.T) {
	a := newAggregateListener(1)
	t.Cleanup(func() { _ = a.Close() })
	child := newListenerHarness(t, listenerIdleBudget, MaxAcceptQueue)
	require.NoError(t, a.Register(child.listener))

	queuedBroker := child.mustOpen(1)
	heldBroker := child.mustOpen(2)
	require.Eventually(t, func() bool { return aggregateQueued(a) == 1 }, listenerTestDeadline, time.Millisecond,
		"the aggregate queue never filled to its bound")

	require.NoError(t, a.Remove(child.listener))
	require.Zero(t, aggregateChildCount(a))

	// The connection the forwarder was holding is released; the connection
	// already queued from the removed child stays deliverable.
	require.Eventually(t, func() bool { return channelClosed(heldBroker.Done()) }, listenerTestDeadline, time.Millisecond,
		"the backpressured in-flight connection was not released")
	queued, err := a.Accept()
	require.NoError(t, err)
	queuedConnection, ok := queued.(*ListenerConnection)
	require.True(t, ok, "the aggregate must forward the child's typed daemonmux connection")
	require.False(t, channelClosed(queuedConnection.Done()), "a queued connection from a removed child must stay deliverable")
	require.NoError(t, queued.Close())
	require.True(t, channelClosed(queuedConnection.Done()))
	require.Eventually(t, func() bool { return channelClosed(queuedBroker.Done()) }, listenerTestDeadline, time.Millisecond)
}
