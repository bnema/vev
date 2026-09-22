// Server supervisor over accepted raw physical carriers (P3.2c).
//
// ServerSupervisor is the daemon-side owner of accepted daemonmux physical
// connections. A daemon accept loop hands it one authenticated raw framed
// carriage per connection; the supervisor completes the daemonmux physical
// preamble under its immutable ServerBinding, then publishes a fully wired
// typed listener to the daemon's AggregateListener. It performs no listener
// policy of its own: the carriage must already be authenticated, and the
// binding is the only authority the handshake enforces.
//
// Each accepted carriage becomes one owned supervisedPhysical: a Listener, its
// Pump, and the framing bridge that carries it. The listener is always built
// before its pump starts, because it registers the pump's single admission
// observer and the pump refuses a late registration. The supervised child
// implements ports.ServerListener by delegating Accept to that listener, and
// its Close tears the listener, pump, and carrier down together, so the
// AggregateListener never owns a half-closed child.
//
// The accepted physical policy is enforced on every inbound Open before fresh
// stream admission: the supervised pump's engine is restricted to the binding
// policy, so a stream that names a policy the daemon did not accept is refused
// on its own stream and never reaches the daemon's accept stream.
//
// Admission is bounded: at most the configured number of connections may be
// inside dial-handshake-registration at once, so a burst of raw carriers can
// never start an unbounded number of handshakes or pump goroutines. Each child
// is reaped on its own: when its pump reaches a terminal outcome - a lost
// carriage, a closed listener, or a local teardown - one goroutine deregisters
// it from the aggregate and closes it, without touching a sibling or the
// supervisor. The supervisor's Close closes every owned child and joins the
// reapers; it does not close the AggregateListener, which its caller owns.
package daemonmux

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Server supervisor sentinels. They are typed so a caller can classify a
// configuration, admission, or lifecycle outcome without matching on message
// text.
var (
	// ErrSupervisorConfig reports a supervisor constructed without an
	// aggregate listener or binding, with an invalid ceiling advertisement, or
	// an Adopt call with a nil raw carriage.
	ErrSupervisorConfig = errors.New("daemonmux: invalid server supervisor configuration")
	// ErrSupervisorClosed reports a physical carriage offered to a supervisor
	// that was closed, or a registration attempted after Close.
	ErrSupervisorClosed = errors.New("daemonmux: server supervisor is closed")
)

// MaxSupervisedAdmissions bounds the accepted physical connections one
// supervisor may have in handshake-or-registration at once. It mirrors the
// per-connection stream ceiling, so a daemon never starts more handshakes than
// one connection could multiplex.
const MaxSupervisedAdmissions = int(MaxMuxStreams)

// ServerSupervisor accepts authenticated raw physical carriage and publishes
// owned typed listeners to one AggregateListener. It is safe for concurrent
// use.
type ServerSupervisor struct {
	aggregate *AggregateListener
	binding   ServerBinding
	ceilings  MuxCeilings
	clock     ports.Clock

	// slots bounds concurrent handshakes and admissions: one token is held
	// from the start of Adopt until the child is registered and its pump
	// started.
	slots chan struct{}

	// mu guards children and closed. children holds exactly the live owned
	// children, so Close and the reapers agree on what still needs teardown.
	mu       sync.Mutex
	children map[*supervisedPhysical]struct{}
	closed   bool

	done      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewServerSupervisor returns a supervisor that publishes every accepted
// physical connection to aggregate under binding, advertising ceilings in the
// physical preamble. admissionLimit bounds concurrent handshakes and
// admissions; a non-positive value falls back to MaxSupervisedAdmissions. A
// nil aggregate, an invalid binding, or an invalid ceiling advertisement is
// refused with ErrSupervisorConfig.
func NewServerSupervisor(aggregate *AggregateListener, binding ServerBinding, ceilings MuxCeilings, admissionLimit int) (*ServerSupervisor, error) {
	return newServerSupervisor(aggregate, binding, ceilings, admissionLimit, clock.New())
}

// newServerSupervisor builds a supervisor from an explicit time source, so a
// test can drive the per-stream handshake deadline deterministically. The
// exported constructor delegates with the wall clock.
func newServerSupervisor(aggregate *AggregateListener, binding ServerBinding, ceilings MuxCeilings, admissionLimit int, timeSource ports.Clock) (*ServerSupervisor, error) {
	if aggregate == nil {
		return nil, ErrSupervisorConfig
	}
	if err := binding.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSupervisorConfig, err)
	}
	if err := ceilings.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSupervisorConfig, err)
	}
	if admissionLimit <= 0 {
		admissionLimit = MaxSupervisedAdmissions
	}
	if timeSource == nil {
		timeSource = clock.New()
	}
	return &ServerSupervisor{
		aggregate: aggregate,
		binding:   binding,
		ceilings:  ceilings,
		clock:     timeSource,
		slots:     make(chan struct{}, admissionLimit),
		children:  make(map[*supervisedPhysical]struct{}),
		done:      make(chan struct{}),
	}, nil
}

// Adopt accepts one authenticated raw physical carriage: it bounds admission,
// completes the daemonmux physical preamble against the supervisor's binding,
// negotiates the effective ceilings, builds the pump and its typed listener,
// registers the owned child with the aggregate, and starts the pump. The
// caller's ctx bounds the handshake; the started connection is detached from it
// and owned by the supervisor.
//
// A failed preamble, a refused policy, or a closed supervisor closes the
// carriage and registers nothing. After a successful return the caller's only
// remaining role is to stop offering carriage (or Close the supervisor); the
// supervisor owns the carriage from then on.
func (s *ServerSupervisor) Adopt(ctx context.Context, raw RawFramedTransport) error {
	if s == nil {
		return ErrSupervisorConfig
	}
	if raw == nil {
		return ErrSupervisorConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.acquire(ctx); err != nil {
		_ = raw.Close()
		return err
	}
	defer s.release()

	bridge, err := NewPreambleCarrier(raw)
	if err != nil {
		_ = raw.Close()
		return fmt.Errorf("%w: %w", ErrSupervisorConfig, err)
	}

	result, err := RunServerHandshake(ctx, bridge, s.binding, s.ceilings)
	if err != nil {
		// The handshake already closed a refused carriage; Close is idempotent.
		_ = bridge.Close()
		return err
	}
	if err := bridge.Negotiate(result.Ceilings); err != nil {
		_ = bridge.Close()
		return err
	}

	pump, err := NewPump(bridge, DirectionClient, result.Ceilings)
	if err != nil {
		_ = bridge.Close()
		return err
	}
	// Enforce the accepted physical policy before any fresh inbound Open can be
	// admitted: the pump's engine must be restricted before the pump starts,
	// and the listener below is built before that start too. The accepted entry
	// carries both the exact policy the engine enforces and the locality origin
	// the listener stamps on every admitted session connection.
	accepted := ServerPolicyAdmission{Policy: result.Policy, Origin: result.Origin}
	if err := pump.Engine().RestrictAdmissions(accepted.Policy); err != nil {
		_ = pump.Close()
		return err
	}

	listener, err := newListener(pump, protocol.HandshakeTimeout, MaxAcceptQueue, s.clock, accepted)
	if err != nil {
		_ = pump.Close()
		return err
	}

	child := &supervisedPhysical{listener: listener, pump: pump}
	child.start(context.WithoutCancel(ctx))
	if err := s.register(child); err != nil {
		_ = child.Close()
		return err
	}
	return nil
}

// Children reports how many physical connections the supervisor currently
// owns. It is a diagnostic accessor; a lost child disappears once its reaper
// has closed it.
func (s *ServerSupervisor) Children() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.children)
}

// Close stops accepting physical carriage and tears every owned child down:
// it deregisters and closes each child - listener, pump, and carrier together -
// and joins every reaper. Close is idempotent and concurrent-safe. It leaves
// the caller-owned AggregateListener open; the daemon closes that itself.
func (s *ServerSupervisor) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		children := make([]*supervisedPhysical, 0, len(s.children))
		for child := range s.children {
			children = append(children, child)
		}
		s.mu.Unlock()
		close(s.done)
		for _, child := range children {
			// Remove closes the child and joins its aggregate forwarder. A
			// child its own reaper already deregistered reports
			// ErrAggregateUnknownChild and is closed below regardless.
			_ = s.aggregate.Remove(child)
			_ = child.Close()
			s.forget(child)
		}
		s.wg.Wait()
	})
	return nil
}

// acquire takes one admission slot, blocking until one is free or ctx or the
// supervisor ends. It refuses admission outright once the supervisor closed.
func (s *ServerSupervisor) acquire(ctx context.Context) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return ErrSupervisorClosed
	}
	select {
	case s.slots <- struct{}{}:
		return nil
	default:
	}
	select {
	case s.slots <- struct{}{}:
		return nil
	case <-s.done:
		return ErrSupervisorClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release returns one admission slot.
func (s *ServerSupervisor) release() { <-s.slots }

// register takes ownership of one fully wired, already started child and
// publishes it to the aggregate, then starts its reaper. It registers with the
// aggregate and takes the wait-group reference under the same lock that checks
// closed, so Close can never observe a child it did not also own and wait for,
// and no child is ever registered after Close saw the supervisor closed. A
// closed supervisor, or a closed aggregate, refuses the child, which its
// caller closes.
func (s *ServerSupervisor) register(child *supervisedPhysical) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSupervisorClosed
	}
	if err := s.aggregate.Register(child); err != nil {
		return err
	}
	s.children[child] = struct{}{}
	s.wg.Add(1)
	go s.reap(child)
	return nil
}

// forget drops one child from the owned set. It is idempotent.
func (s *ServerSupervisor) forget(child *supervisedPhysical) {
	s.mu.Lock()
	delete(s.children, child)
	s.mu.Unlock()
}

// reap watches one child's pump and closes the child once its carriage reaches
// a terminal outcome, isolating the loss from every sibling and from the
// supervisor. It deregisters the child from the aggregate first (a child whose
// own forwarder already observed the terminal Accept reports
// ErrAggregateUnknownChild) and then closes listener, pump, and carrier
// together. It exits when Close wakes it through done.
func (s *ServerSupervisor) reap(child *supervisedPhysical) {
	defer s.wg.Done()
	select {
	case <-child.pump.Done():
	case <-s.done:
	}
	_ = s.aggregate.Remove(child)
	_ = child.Close()
	s.forget(child)
}

// supervisedPhysical is one owned accepted physical connection. It implements
// ports.ServerListener by delegating Accept to its typed Listener, so the
// aggregate composes it exactly like any other physical listener, while Close
// owns the whole triple: listener, pump, and carrier.
type supervisedPhysical struct {
	listener *Listener
	pump     *Pump

	closeOnce sync.Once
	closeErr  error
}

var _ ports.ServerListener = (*supervisedPhysical)(nil)

// start launches the child's pump. The listener was already constructed and
// registered its admission observer, so no admitted start can be missed.
func (c *supervisedPhysical) start(ctx context.Context) {
	c.pump.Start(ctx)
}

// Addr names the child's carriage for ports.ServerListener.
func (c *supervisedPhysical) Addr() string {
	if c == nil || c.listener == nil {
		return "daemonmux"
	}
	return c.listener.Addr()
}

// Accept returns the next typed connection from the child's listener.
func (c *supervisedPhysical) Accept() (ports.ServerConnection, error) {
	if c == nil || c.listener == nil {
		return nil, ErrSupervisorConfig
	}
	return c.listener.Accept()
}

// Close tears the child down exactly once: it closes the listener (unblocking
// and joining its watchdog), then the pump (cancelling the carriage goroutines
// and closing the carrier). Every caller observes the same pump close error.
func (c *supervisedPhysical) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		if c.listener != nil {
			_ = c.listener.Close()
		}
		if c.pump != nil {
			c.closeErr = c.pump.Close()
		}
	})
	return c.closeErr
}
