package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

// Lifecycle supervisor.
//
// The supervisor is the single owner of the broker's idle lifetime. Exactly two
// kinds of work can pin the broker open:
//
//   - an admitted client lease: one accepted broker client connection, and
//   - an in-flight user operation lease: one operation a user asked the broker
//     to perform while it is admitted (for example opening a stream or editing
//     the configured-host set).
//
// Nothing else pins. Work the broker schedules for itself -- observation
// retries, durable snapshot writes, opening the store, or keeping physical
// transports warm in the idle pool -- is deliberately not represented here and
// can never extend the broker's life: a broker with no admitted client and no
// in-flight user operation shuts down on schedule even while those components
// run. Registered background components therefore receive the root context and
// an ordered drain hook, but never a lease.
//
// Idle shutdown is committed atomically. The idle timer is active exactly when
// both lease counts are zero. A lease admitted before the commit stops the timer
// and keeps the broker open; once the commit wins, every later admission is
// refused with the typed, non-terminal ports.BrokerAdmissionClosed so the caller
// reconnects to a fresh broker instead of treating the exit as fatal.
//
// Arming installs a fresh timer and claims a new arm number, and admission
// disarms it under the same mutex. The run goroutine therefore commits only for
// the arm it is still observing: a fire from a superseded arm can never shut the
// broker down out of turn, so an admission that wins the race always leaves the
// broker open for a full grace.
//
// Shutdown itself runs in a fixed order: the supervisor marks itself closed and
// stops/drains the idle timer under its mutex, cancels the root context, then
// drains every registered hook outside the mutex in reverse registration order
// (so a component registered later, which may depend on an earlier one, is
// drained first). The supervisor joins every hook before Done closes. Close runs
// the same sequence and is idempotent and concurrent-safe.

// DefaultIdleGrace is the idle grace granted before a broker with no admitted
// client lease and no in-flight user operation lease shuts itself down.
const DefaultIdleGrace = 5 * time.Minute

// SupervisorConfig configures a Supervisor. The zero value is valid and selects
// DefaultIdleGrace.
type SupervisorConfig struct {
	// IdleGrace overrides DefaultIdleGrace when positive.
	IdleGrace time.Duration
}

// ShutdownHook drains one registered broker-owned component during shutdown.
// It runs after the root context is cancelled and must return once the
// component has stopped; the supervisor joins every hook before Done closes.
// Hooks run outside the supervisor mutex, in reverse registration order. A hook
// must not call Close, which would wait for the very shutdown running it.
type ShutdownHook func(ctx context.Context) error

// Runner is the minimal seam for a broker-owned background component that runs
// until its context is cancelled (for example the observation Registry). Run
// must return once ctx is cancelled so the supervisor can join it.
type Runner interface {
	Run(ctx context.Context)
}

// Closeable is the minimal seam for a broker-owned resource closed at shutdown
// (for example the physical-transport Pool). The supervisor never inspects pool
// or registry internals; it only asks them to drain.
type Closeable interface {
	Close() error
}

// Lease is an idempotent admission handle. Release may be called any number of
// times, from any goroutine, and after the supervisor has shut down; only the
// first call releases the lease.
type Lease struct {
	once    sync.Once
	release func()
}

// Release returns the lease exactly once. A nil Lease is a no-op.
func (l *Lease) Release() {
	if l == nil || l.release == nil {
		return
	}
	l.once.Do(l.release)
}

type leaseKind uint8

const (
	leaseClient leaseKind = iota + 1
	leaseOperation
)

type registeredHook struct {
	name string
	hook ShutdownHook
}

// Supervisor owns the broker idle lifetime, the root context handed to
// background components, and the ordered shutdown drain.
type Supervisor struct {
	clock ports.Clock
	idle  time.Duration

	root   context.Context
	cancel context.CancelFunc

	// ready is closed once the idle timer is armed and the run goroutine is
	// accepting lifecycle signals. done is closed only after the root context is
	// cancelled and every hook has drained. stopped is closed as the run
	// goroutine exits. closing wakes the run goroutine on the committed shutdown,
	// and wake nudges it whenever a lease changes the timer state.
	ready   chan struct{}
	done    chan struct{}
	stopped chan struct{}
	closing chan struct{}
	wake    chan struct{}

	mu          sync.Mutex
	closed      bool
	committed   bool
	clients     int
	operations  int
	timer       ports.Timer
	timerArmed  bool
	arm         uint64
	hooks       []registeredHook
	shutdownErr error
}

// NewSupervisor starts the idle lifetime. The idle timer is armed immediately:
// a broker that serves nothing shuts down one idle grace after construction.
func NewSupervisor(clock ports.Clock, cfg SupervisorConfig) (*Supervisor, error) {
	if nilDependency(clock) {
		return nil, errors.New("broker: supervisor requires a clock")
	}
	grace := cfg.IdleGrace
	if grace <= 0 {
		grace = DefaultIdleGrace
	}
	root, cancel := context.WithCancel(context.Background())
	s := &Supervisor{
		clock: clock, idle: grace, root: root, cancel: cancel,
		ready: make(chan struct{}), done: make(chan struct{}),
		stopped: make(chan struct{}), closing: make(chan struct{}),
		wake: make(chan struct{}, 1),
	}
	go s.run()
	<-s.ready
	return s, nil
}

// RootContext returns the supervisor-owned root context. Background components
// must derive their work from it and must stop when it is cancelled.
func (s *Supervisor) RootContext() context.Context { return s.root }

// Done is closed once the root context is cancelled and every registered hook
// has been drained and joined.
func (s *Supervisor) Done() <-chan struct{} { return s.done }

// Register admits one background component with an ordered drain hook. The
// component must derive its work from RootContext and must stop when that
// context is cancelled; hook must return once it has. Registration never grants
// a lease, so a registered component cannot pin the broker open. Register is
// refused with ports.BrokerAdmissionClosed after shutdown commits and with
// ports.BrokerAdmissionInvalid for an empty name or nil hook.
func (s *Supervisor) Register(name string, hook ShutdownHook) error {
	if name == "" || hook == nil {
		return ports.BrokerAdmissionInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ports.BrokerAdmissionClosed
	}
	s.hooks = append(s.hooks, registeredHook{name: name, hook: hook})
	return nil
}

// RegisterRunner starts a broker-owned background component under the root
// context and joins it at shutdown: its drain hook waits for Run to return. The
// component must therefore honor cancellation promptly. Registration is refused
// exactly as Register is.
func (s *Supervisor) RegisterRunner(name string, runner Runner) error {
	if nilDependency(runner) {
		return ports.BrokerAdmissionInvalid
	}
	drained := make(chan struct{})
	if err := s.Register(name, func(context.Context) error {
		<-drained
		return nil
	}); err != nil {
		return err
	}
	ctx := s.root
	go func() {
		defer close(drained)
		runner.Run(ctx)
	}()
	return nil
}

// RegisterCloseable registers a broker-owned resource to be closed after the
// root context is cancelled. Its Close error is reported by Close and never
// aborts the remaining drains. Registration is refused exactly as Register is.
func (s *Supervisor) RegisterCloseable(name string, closeable Closeable) error {
	if nilDependency(closeable) {
		return ports.BrokerAdmissionInvalid
	}
	return s.Register(name, func(context.Context) error { return closeable.Close() })
}

// AdmitClient admits one client-connection lease. It returns
// ports.BrokerAdmissionClosed once shutdown has committed.
func (s *Supervisor) AdmitClient() (*Lease, error) { return s.admit(leaseClient) }

// AdmitOperation admits one in-flight user-operation lease. It returns
// ports.BrokerAdmissionClosed once shutdown has committed.
func (s *Supervisor) AdmitOperation() (*Lease, error) { return s.admit(leaseOperation) }

// Close commits shutdown and waits for the root context to be cancelled and
// every hook to drain. It is idempotent and concurrent-safe: every caller
// observes the same terminal state and the same joined hook error.
func (s *Supervisor) Close() error {
	s.mu.Lock()
	s.commitLocked()
	s.mu.Unlock()
	<-s.stopped
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shutdownErr
}

func (s *Supervisor) admit(kind leaseKind) (*Lease, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ports.BrokerAdmissionClosed
	}
	switch kind {
	case leaseClient:
		s.clients++
	case leaseOperation:
		s.operations++
	}
	// An admission that wins the commit race disarms the pending idle shutdown.
	s.disarmTimerLocked()
	s.mu.Unlock()
	s.signal()
	return &Lease{release: func() { s.release(kind) }}, nil
}

func (s *Supervisor) release(kind leaseKind) {
	s.mu.Lock()
	switch kind {
	case leaseClient:
		if s.clients > 0 {
			s.clients--
		}
	case leaseOperation:
		if s.operations > 0 {
			s.operations--
		}
	}
	if !s.closed && s.clients == 0 && s.operations == 0 {
		s.armTimerLocked()
	}
	s.mu.Unlock()
	s.signal()
}

// signal nudges the run goroutine to re-read the timer state. Signals coalesce.
func (s *Supervisor) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// run owns the idle timer and is the sole shutdown executor. It waits on the
// current arm's timer and finalizes exactly once, either when an un-superseded
// arm fires while the broker is still idle or when Close commits the shutdown.
func (s *Supervisor) run() {
	defer close(s.stopped)

	s.mu.Lock()
	s.armTimerLocked()
	s.mu.Unlock()
	close(s.ready)

	for {
		s.mu.Lock()
		timer, arm := s.timer, s.arm
		s.mu.Unlock()

		var fire <-chan time.Time
		if timer != nil {
			fire = timer.C()
		}
		select {
		case <-s.closing:
			s.finalize()
			return
		case <-s.wake:
			continue
		case <-fire:
			if s.tryCommitIdleExpiry(arm) {
				s.finalize()
				return
			}
		}
	}
}

// tryCommitIdleExpiry reports whether a fire from arm must commit the idle
// shutdown. It returns true only for the run goroutine that must finalize: a
// superseded or already disarmed arm, a committed supervisor, or a still-held
// lease all make the fire a no-op.
func (s *Supervisor) tryCommitIdleExpiry(arm uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.timerArmed || s.arm != arm {
		return false
	}
	if s.clients > 0 || s.operations > 0 {
		return false
	}
	return s.commitLocked()
}

// commitLocked marks the supervisor closed and disarms the idle timer. It
// returns true exactly once, for the caller that committed. Because admission
// and commitment share the mutex, an admission either disarms the pending expiry
// before the commit or observes the closed state after it.
func (s *Supervisor) commitLocked() bool {
	if s.committed {
		return false
	}
	s.committed = true
	s.closed = true
	s.disarmTimerLocked()
	close(s.closing)
	return true
}

// finalize cancels the root context, drains every hook outside the mutex in
// reverse registration order, and only then closes Done. It runs exactly once,
// on the run goroutine.
func (s *Supervisor) finalize() {
	s.cancel()
	s.mu.Lock()
	hooks := append([]registeredHook(nil), s.hooks...)
	s.mu.Unlock()

	var errs []error
	for i := len(hooks) - 1; i >= 0; i-- {
		if err := hooks[i].hook(s.root); err != nil {
			errs = append(errs, fmt.Errorf("broker: %s shutdown: %w", hooks[i].name, err))
		}
	}

	s.mu.Lock()
	s.shutdownErr = errors.Join(errs...)
	s.mu.Unlock()
	close(s.done)
}

// armTimerLocked installs a fresh idle timer and claims a new arm number. A
// fresh timer per idle cycle lets the run goroutine attribute a fire to the arm
// that produced it, so a fire from a superseded arm is ignored instead of
// shutting the broker down out of turn. Callers hold mu and have established
// that both lease counts are zero.
func (s *Supervisor) armTimerLocked() {
	if s.timerArmed || s.committed {
		return
	}
	s.timer = s.clock.NewTimer(s.idle)
	s.timerArmed = true
	s.arm++
}

// disarmTimerLocked stops and drains the idle timer, superseding its arm,
// supporting buffered timer adapters with pre-Go-1.23 reset semantics. Callers
// hold mu.
func (s *Supervisor) disarmTimerLocked() {
	s.timerArmed = false
	if s.timer == nil {
		return
	}
	if !s.timer.Stop() {
		select {
		case <-s.timer.C():
		default:
		}
	}
	s.timer = nil
}
