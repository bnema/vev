// Aggregate listener over multiple physical listeners (P3.2).
//
// AggregateListener is the daemon's global ports.ServerListener over several
// physical daemonmux connections. The daemon's shared accept loop must not stop
// when one physical carriage is lost: a daemonmux.Listener Accept fails with
// ErrListenerLost exactly when its one physical connection reached its terminal
// outcome, and a raw accept loop treats that as the end of service. The
// aggregate composes independent child listeners into one accept stream, so the
// loss or closure of one child is isolated - the aggregate deregisters it and
// keeps accepting from every other child and from children registered later -
// and never becomes the aggregate's own terminal error.
//
// Each registered child is owned: the aggregate runs exactly one forwarding
// goroutine per child (bounded by the number of registered children, never by
// the number of accepted streams) that repeatedly takes one typed
// ports.ServerConnection from the child and hands it to the aggregate's bounded
// FIFO. The FIFO is a mutex-guarded slice signalled by a sync.Cond
// (MaxAggregateQueue, 128, by default): a slow or stalled consumer applies
// backpressure to the child forwarders instead of growing memory, so the
// aggregate never holds more than its queue bound plus one in-flight connection
// per child forwarder, and each child's own bounded accept queue still refuses
// a stream whose slot it cannot hold.
//
// A child whose Accept fails for any terminal reason - ErrListenerLost,
// ErrListenerClosed, or a transport-specific error - is deregistered and its
// goroutine joined; the failure is swallowed rather than returned by the
// aggregate's Accept, because one physical carriage is not the daemon's whole
// listener. Register and Remove are safe for concurrent use, as are Accept and
// Close. Close unblocks every blocked Accept with ErrAggregateClosed, closes
// every owned child (which unblocks and joins that child's own accept loop),
// joins every forwarding goroutine, and closes every connection that was taken
// from a child but never handed to the daemon. Removing one child closes that
// child and joins its goroutine without touching a sibling, a queued connection
// already taken from it, or a connection already delivered to the daemon.
//
// The aggregate owns no framing, session conversion, or carrier: it consumes
// only the typed ports.ServerListener and ports.ServerConnection contracts, so
// it composes with the daemonmux Listener, the sessionwire listener, or any
// later physical listener implementation.
package daemonmux

import (
	"errors"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

// Aggregate-listener sentinels. They are typed so a caller can classify a
// configuration, lifecycle, or registration outcome without matching on
// message text.
var (
	// ErrAggregateConfig reports an aggregate listener operation on a nil
	// receiver, or a registration of a nil child.
	ErrAggregateConfig = errors.New("daemonmux: invalid aggregate listener configuration")
	// ErrAggregateClosed reports a registration on, or an Accept of, an
	// aggregate listener that was closed.
	ErrAggregateClosed = errors.New("daemonmux: aggregate listener is closed")
	// ErrAggregateChildTaken reports a registration of a child that is already
	// registered.
	ErrAggregateChildTaken = errors.New("daemonmux: aggregate child is already registered")
	// ErrAggregateUnknownChild reports a removal of a child that is not
	// registered, whether it was never registered or already deregistered
	// because its physical carriage reached its terminal outcome.
	ErrAggregateUnknownChild = errors.New("daemonmux: aggregate child is not registered")
)

// MaxAggregateQueue bounds the accepted-but-undelivered typed connections the
// aggregate listener holds across every child. It is the per-physical-listener
// accept bound, so the aggregate's own memory is never larger than one child's.
// A child forwarder may additionally hold one connection while the queue is
// full, so the aggregate's total retained connections are bounded by this value
// plus the number of registered children.
const MaxAggregateQueue = MaxAcceptQueue

// AggregateListener is the daemon's global ports.ServerListener over several
// independent physical listeners. It is safe for concurrent use.
type AggregateListener struct {
	limit int

	// mu guards the child registry, the accepted-but-undelivered FIFO, and the
	// closed flag. cond broadcasts every FIFO and lifecycle change so a blocked
	// Accept and a child forwarder waiting for space never miss a wakeup.
	mu       sync.Mutex
	cond     *sync.Cond
	children map[ports.ServerListener]*aggregateChild
	queue    []ports.ServerConnection
	closed   bool

	closeOnce sync.Once
}

// aggregateChild is one registered physical listener and the lifecycle of its
// single forwarding goroutine. quit is the per-child stop signal Remove and the
// forwarder's own shutdown observe; done closes when the forwarder returns.
type aggregateChild struct {
	listener ports.ServerListener
	quit     chan struct{}
	done     chan struct{}
}

var _ ports.ServerListener = (*AggregateListener)(nil)

// NewAggregateListener returns an empty aggregate listener under the package
// policy: the aggregate queue is bounded by MaxAggregateQueue. Children are
// registered and removed explicitly; the aggregate owns every registered child
// and closes it on Close.
func NewAggregateListener() *AggregateListener {
	return newAggregateListener(MaxAggregateQueue)
}

// newAggregateListener builds an aggregate listener from an explicit queue
// bound, so a test can tighten it and prove the bound and its backpressure
// without changing live behavior. A non-positive bound falls back to the
// package policy.
func newAggregateListener(limit int) *AggregateListener {
	if limit <= 0 {
		limit = MaxAggregateQueue
	}
	listener := &AggregateListener{
		limit:    limit,
		children: make(map[ports.ServerListener]*aggregateChild),
	}
	listener.cond = sync.NewCond(&listener.mu)
	return listener
}

// Addr names the aggregate's carriage for ports.ServerListener. The aggregate
// multiplexes several physical carriages into one accept stream, so it has no
// single per-child address.
func (a *AggregateListener) Addr() string { return "daemonmux" }

// Register takes ownership of one physical child listener and starts its single
// forwarding goroutine. Registering a nil child is refused with
// ErrAggregateConfig, a child that is already registered with
// ErrAggregateChildTaken, and any child after Close with ErrAggregateClosed. A
// child whose Accept is already terminal is deregistered by its forwarder as
// soon as it runs, without failing the aggregate. The child is identified by
// its interface value, so a child is a pointer-like, comparable listener as
// every ports.ServerListener implementation in this repository is.
func (a *AggregateListener) Register(child ports.ServerListener) error {
	if a == nil || child == nil {
		return ErrAggregateConfig
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ErrAggregateClosed
	}
	if _, exists := a.children[child]; exists {
		a.mu.Unlock()
		return ErrAggregateChildTaken
	}
	entry := &aggregateChild{
		listener: child,
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	a.children[child] = entry
	a.mu.Unlock()
	go a.forward(entry)
	return nil
}

// Remove closes one registered child and joins its forwarding goroutine without
// touching a sibling, the connections already queued from that child, or a
// connection already delivered to the daemon. It is safe to call concurrently
// with Accept, Close, and another Remove; a child that was never registered or
// already deregistered because its carriage was lost reports
// ErrAggregateUnknownChild. A removal that races Close may either win (and
// close the child) or lose (and report the child already gone); either way the
// child is closed exactly once.
func (a *AggregateListener) Remove(child ports.ServerListener) error {
	if a == nil || child == nil {
		return ErrAggregateConfig
	}
	a.mu.Lock()
	entry, ok := a.children[child]
	if !ok {
		a.mu.Unlock()
		return ErrAggregateUnknownChild
	}
	delete(a.children, child)
	close(entry.quit)
	// Wake a forwarder that is waiting for queue space, so it observes quit and
	// releases the connection it was holding instead of waiting forever.
	a.cond.Broadcast()
	a.mu.Unlock()

	_ = entry.listener.Close()
	<-entry.done
	return nil
}

// Accept returns the oldest typed connection accepted from any registered child,
// blocking until one is available, and fails with ErrAggregateClosed after
// Close. It never surfaces a child's Accept failure: a lost or closed physical
// carriage is isolated by its forwarder and cannot stop the aggregate's accept
// stream. A nil receiver is refused with ErrAggregateConfig.
func (a *AggregateListener) Accept() (ports.ServerConnection, error) {
	if a == nil {
		return nil, ErrAggregateConfig
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for len(a.queue) == 0 {
		if a.closed {
			return nil, ErrAggregateClosed
		}
		a.cond.Wait()
	}
	connection := a.queue[0]
	a.queue = a.queue[1:]
	return connection, nil
}

// Close stops accepting: it unblocks every blocked Accept with
// ErrAggregateClosed, clears every owned child (which unblocks and joins that
// child's own accept loop), joins every forwarding goroutine, and closes every
// connection that was taken from a child but never handed to the daemon. Close
// is idempotent and concurrent-safe; closeOnce guarantees a single drain across
// concurrent callers. It never touches a connection already handed to the
// daemon, which owns it from then on.
func (a *AggregateListener) Close() error {
	if a == nil {
		return nil
	}
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		queued := a.queue
		a.queue = nil
		children := make([]*aggregateChild, 0, len(a.children))
		for _, entry := range a.children {
			children = append(children, entry)
		}
		a.children = make(map[ports.ServerListener]*aggregateChild)
		// Wake every blocked Accept (which now returns ErrAggregateClosed) and
		// every forwarder waiting for queue space (which now releases its
		// in-flight connection).
		a.cond.Broadcast()
		a.mu.Unlock()

		// Close every owned child first: this unblocks a forwarder parked in
		// the child's Accept so it can observe the closed aggregate and exit.
		for _, entry := range children {
			_ = entry.listener.Close()
		}
		for _, entry := range children {
			<-entry.done
		}
		for _, connection := range queued {
			if connection != nil {
				_ = connection.Close()
			}
		}
	})
	return nil
}

// forward is one child's single forwarding goroutine. It takes one typed
// connection at a time from the child and hands it to the aggregate's bounded
// FIFO; when the child's Accept fails for any reason it deregisters the child
// and returns, so the failure is isolated from the aggregate and every sibling.
// When the aggregate is closed or the child is removed it releases the
// connection it is holding and returns.
func (a *AggregateListener) forward(entry *aggregateChild) {
	defer close(entry.done)
	for {
		connection, err := entry.listener.Accept()
		if err != nil {
			if connection != nil {
				_ = connection.Close()
			}
			a.deregister(entry)
			return
		}
		if !a.enqueue(entry, connection) {
			if connection != nil {
				_ = connection.Close()
			}
			a.deregister(entry)
			return
		}
	}
}

// enqueue appends one accepted connection to the bounded FIFO, waiting for
// space while the queue is full. It reports false when the aggregate was closed
// or the child was removed before the connection could be queued, so the caller
// releases it. It never blocks a child's own accept loop: while the queue is
// full the child is left to its own bounded queue and refuses a stream it
// cannot hold.
func (a *AggregateListener) enqueue(entry *aggregateChild, connection ports.ServerConnection) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for len(a.queue) >= a.limit && !a.closed && !childStopped(entry) {
		a.cond.Wait()
	}
	if a.closed || childStopped(entry) {
		return false
	}
	a.queue = append(a.queue, connection)
	a.cond.Broadcast()
	return true
}

// deregister removes one forwarder's child from the registry if it is still
// registered. It is a no-op for a child already removed by Remove or by Close,
// so a natural terminal outcome and an explicit removal never double-close.
func (a *AggregateListener) deregister(entry *aggregateChild) {
	a.mu.Lock()
	if a.children[entry.listener] == entry {
		delete(a.children, entry.listener)
	}
	a.mu.Unlock()
}

// childStopped reports whether one child's forwarder was asked to stop. The
// caller holds a.mu.
func childStopped(entry *aggregateChild) bool {
	select {
	case <-entry.quit:
		return true
	default:
		return false
	}
}
