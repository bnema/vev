package app

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/brokerstore"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/logging"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/broker"
	"github.com/bnema/vev/pkg/safedir"
)

// Offline broker foreground composition (Plan 001 P3.4 slice C).
//
// `_broker-serve` is a hidden, isolated sandbox command. It takes one strict
// offline root, derives only sandbox runtime, state, and log paths from it, and
// composes the P3.4 broker core and P3.3 local IPC listener over the immutable
// config marker. It never reads or writes production runtime, state, or config
// and never changes an ordinary command, path, or factory.
//
// Nothing here spawns a detached process or daemon, reports status, or changes
// a production path. When the sandbox configuration provisions a local binding,
// the registry observes that existing Unix daemonmux route without opening a
// logical stream; it owns no remote membership and never reaches SSH or QUIC.
// The composed process is foreground: it owns one lifetime lock for its whole run, serves
// until a signal or its parent context ends it, or until the broker idles out,
// and then shuts down in a fixed order.

const brokerServeCommand = "_broker-serve"

// brokerServePoolLimits bound the offline broker pool. They are explicit, not
// derived from the environment, and small enough that a stalled peer is
// refused rather than absorbed.
var brokerServePoolLimits = broker.PoolLimits{
	Physical:         64,
	Clients:          brokeripc.DefaultMaxClients,
	Streams:          256,
	StreamsPerClient: 64,
	Idle:             5 * time.Minute,
}

// brokerServeOptions is the parsed hidden invocation.
type brokerServeOptions struct {
	offlineRoot string
	// idleGrace is zero when the operator did not override it; the supervisor
	// then applies its own 5m default.
	idleGrace time.Duration
}

// parseBrokerServeArgs strictly parses `_broker-serve --offline-root ABS
// [--idle-grace DURATION]`. Unknown flags, duplicate flags, positionals, a
// missing value, and a non-positive or unparsable duration are refused; the
// path itself is validated later against the filesystem.
func parseBrokerServeArgs(args []string) (command, error) {
	if len(args) == 0 {
		return command{}, usagef("`%s` requires --offline-root", brokerServeCommand)
	}
	options := brokerServeOptions{}
	var rootSet, graceSet bool
	for index := 0; index < len(args); {
		switch args[index] {
		case "--offline-root":
			if rootSet {
				return command{}, usagef("`%s` received duplicate --offline-root", brokerServeCommand)
			}
			if index+1 >= len(args) || args[index+1] == "" {
				return command{}, usagef("`--offline-root` requires a path")
			}
			options.offlineRoot = args[index+1]
			rootSet = true
			index += 2
		case "--idle-grace":
			if graceSet {
				return command{}, usagef("`%s` received duplicate --idle-grace", brokerServeCommand)
			}
			if index+1 >= len(args) {
				return command{}, usagef("`--idle-grace` requires a duration")
			}
			grace, err := time.ParseDuration(args[index+1])
			if err != nil {
				return command{}, usagef("`--idle-grace` %q is not a duration", args[index+1])
			}
			if grace <= 0 {
				return command{}, usagef("`--idle-grace` must be positive")
			}
			options.idleGrace = grace
			graceSet = true
			index += 2
		default:
			if strings.HasPrefix(args[index], "-") {
				return command{}, usagef("unknown flag %q for `%s`", args[index], brokerServeCommand)
			}
			return command{}, usagef("`%s` does not accept positional arguments", brokerServeCommand)
		}
	}
	if !rootSet {
		return command{}, usagef("`%s` requires --offline-root", brokerServeCommand)
	}
	return command{kind: kindBrokerServe, brokerServe: options}, nil
}

// brokerServeDeps are the composition seams. Production supplies the real
// adapters; tests override the clock, the owner lock, the logger, and the
// readiness hook so idle shutdown and cleanup are deterministic.
type brokerServeDeps struct {
	clock     func() ports.Clock
	acquire   func(string) (lifecycleOwnership, error)
	newLogger func(string) (*slog.Logger, io.Closer, error)
	newEpoch  func() (ports.BrokerEpoch, error)
	listen    func(string, ports.BrokerEpoch, ports.BrokerAuthority, brokeripc.Config) (ports.BrokerListener, error)
	onReady   func(socketPath string)
}

// defaultBrokerServeDeps builds the production seams.
func defaultBrokerServeDeps() brokerServeDeps {
	return brokerServeDeps{
		clock: func() ports.Clock { return clock.New() },
		acquire: func(runtimeDir string) (lifecycleOwnership, error) {
			return lifecycle.TryAcquire(runtimeDir)
		},
		newLogger: sandboxLogging,
		newEpoch:  newBrokerEpoch,
		listen: func(path string, epoch ports.BrokerEpoch, authority ports.BrokerAuthority, cfg brokeripc.Config) (ports.BrokerListener, error) {
			return brokeripc.Listen(path, epoch, authority, cfg)
		},
	}
}

// runBrokerServeCommand enters the hidden foreground process: it installs
// SIGINT/SIGTERM cancellation over the parent context and runs the sandbox.
func runBrokerServeCommand(ctx context.Context, options brokerServeOptions) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return runBrokerServe(ctx, options, defaultBrokerServeDeps())
}

// sandboxLogging configures broker logging inside the sandbox log directory.
// It never consults the production state directory.
func sandboxLogging(dir string) (*slog.Logger, io.Closer, error) {
	return logging.Setup(logging.Config{
		Dir:       dir,
		Component: logging.Daemon,
		Level:     logging.EnvLevel(),
		MaxBytes:  logging.DefaultMaxBytes,
	})
}

// newBrokerEpoch samples one cryptographically random non-zero broker epoch.
func newBrokerEpoch() (ports.BrokerEpoch, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0, fmt.Errorf("vev: sample broker epoch: %w", err)
	}
	epoch := ports.BrokerEpoch(binary.BigEndian.Uint64(raw[:]))
	if epoch == 0 {
		epoch = 1
	}
	return epoch, nil
}

// offlineLayout resolves the hidden offline sandbox layout for one operator
// root, refusing any root that overlaps the production runtime or state paths.
func offlineLayout(offlineRoot string) (brokerconfig.Layout, error) {
	return brokerconfig.ResolveLayout(offlineRoot, []string{ipc.SocketDir(), platform.StateDir()})
}

// effectiveIdleGrace resolves the sandbox idle grace from an explicit flag and
// the provisioned configuration. The provisioned value is the single
// deterministic source: a status probe cannot read a running broker's effective
// grace through the broker IPC protocol, so an explicit grace that conflicts
// with a provisioned one is diagnosed here rather than silently applied.
func effectiveIdleGrace(config *brokerconfig.Config, explicit time.Duration) (time.Duration, error) {
	provisioned, ok := config.IdleGrace()
	switch {
	case explicit <= 0:
		if ok {
			return provisioned, nil
		}
		// Zero lets the supervisor apply its own built-in default.
		return 0, nil
	case ok && explicit != provisioned:
		return 0, usagef("`--idle-grace` %s conflicts with the provisioned idle grace %s", explicit, provisioned)
	default:
		return explicit, nil
	}
}

// runBrokerServe runs one isolated offline broker in the foreground until ctx
// is cancelled or the broker idles out, then shuts down in the fixed order
// listener -> pool -> registry -> store -> log -> owner lock.
//
// Every resource is acquired in dependency order and released in reverse on
// any failure, so a partial setup never leaks a store lock, an owner lock, a
// bound socket, or a supervisor goroutine. The temporary startup lease pins the
// supervisor open from construction until the listener and its accept drain are
// ready, so the idle timer can never fire during setup.
func runBrokerServe(ctx context.Context, options brokerServeOptions, deps brokerServeDeps) (retErr error) {
	layout, err := offlineLayout(options.offlineRoot)
	if err != nil {
		return err
	}
	for _, dir := range []string{layout.Root, layout.Runtime, layout.State, layout.Log} {
		if err := safedir.EnsurePrivate(dir); err != nil {
			return fmt.Errorf("vev: secure broker sandbox directory %s: %w", dir, err)
		}
	}
	// ResolveLayout validates the root textually and stops at the first
	// component that does not exist yet; now that EnsurePrivate has created the
	// sandbox paths, re-walk every component so a component swapped for a
	// symlink in that window fails closed instead of being followed.
	if err := layout.VerifyCreated(); err != nil {
		return fmt.Errorf("vev: verify broker sandbox paths: %w", err)
	}
	config, err := brokerconfig.Load(layout)
	if err != nil {
		return err
	}
	idleGrace, err := effectiveIdleGrace(config, options.idleGrace)
	if err != nil {
		return err
	}
	owner, err := deps.acquire(layout.Runtime)
	if err != nil {
		return fmt.Errorf("vev: broker sandbox ownership: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, owner.Release()) }()

	log, logCloser, err := deps.newLogger(layout.Log)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, logCloser.Close()) }()

	store, err := brokerstore.Open(brokerstore.Options{Dir: layout.State})
	if err != nil {
		return fmt.Errorf("vev: open broker sandbox store: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, store.Close()) }()

	epoch, err := deps.newEpoch()
	if err != nil {
		return err
	}
	// One clock is shared by the supervisor, registry, and pool so an injected
	// clock (for example a manual test clock) governs every timer in the sandbox.
	clk := deps.clock()
	supervisor, err := broker.NewSupervisor(clk, broker.SupervisorConfig{IdleGrace: idleGrace})
	if err != nil {
		return err
	}
	// The supervisor owns the ordered drain; its deferred Close is a safety net
	// for every error path below. A successful run closes it explicitly, and the
	// flag suppresses the deferred duplicate join so one shutdown error is never
	// reported twice.
	supervisorClosed := false
	defer func() {
		if !supervisorClosed {
			retErr = errors.Join(retErr, supervisor.Close())
		}
	}()

	// The startup lease pins the broker open until the listener and its accept
	// drain are ready. It is released exactly once, and again on any error path.
	startupLease, err := supervisor.AdmitOperation()
	if err != nil {
		return err
	}
	defer startupLease.Release()

	registryConfig, err := offlineRegistryConfig(config, log)
	if err != nil {
		return err
	}
	connector, err := brokerMuxConnector(config, log)
	if err != nil {
		return err
	}
	resolver := config.Resolver()
	var remoteProbe ports.BrokerHostProbe
	if len(config.Endpoints()) != 0 {
		remoteProbe = &brokerRemoteProbe{
			epoch: epoch, resolver: resolver, connector: connector,
			policy: func(endpoint string) (ports.BrokerPolicy, bool) {
				registration, ok := config.Registration(endpoint)
				return registration.Policy, ok
			},
		}
	}
	registry, err := broker.NewRegistryWithConfig(epoch, store, remoteProbe, clk, log, registryConfig)
	if err != nil {
		return err
	}
	// Registration order is shutdown order reversed: registering the registry
	// first, then the pool, then the listener drains them listener -> pool ->
	// registry.
	if err := supervisor.RegisterRunner("registry", registry); err != nil {
		return err
	}
	pool, err := broker.NewPool(epoch, resolver, connector, clk, brokerServePoolLimits)
	if err != nil {
		return err
	}
	if err := supervisor.RegisterCloseable("pool", pool); err != nil {
		return err
	}
	authority, err := broker.NewAuthority(epoch, registry, pool, supervisor)
	if err != nil {
		return err
	}
	socketPath := brokeripc.SocketPath(layout.Runtime)
	listener, err := deps.listen(socketPath, epoch, authority, brokeripc.Config{})
	if err != nil {
		return fmt.Errorf("vev: listen on broker sandbox socket: %w", err)
	}
	if err := supervisor.RegisterCloseable("listener", listener); err != nil {
		_ = listener.Close()
		return err
	}

	started := make(chan struct{})
	drained := make(chan struct{})
	failed := make(chan struct{})
	go drainBrokerAccept(listener, log, started, drained, failed)
	<-started
	startupLease.Release()
	log.Info("broker_offline_ready", "socket", socketPath, "endpoints", len(config.Endpoints()))
	if deps.onReady != nil {
		deps.onReady(socketPath)
	}

	select {
	case <-ctx.Done():
	case <-supervisor.Done():
	case <-failed:
		// An unexpected terminal accept failure leaves the socket bound but
		// unable to admit work. Commit shutdown so the sandbox tears down in the
		// fixed order instead of wedging as a bound, unserving broker.
	}
	// Closing the supervisor commits shutdown, cancels the registry run, and
	// drains the listener, pool, and registry in order. The store, log, and
	// owner lock are released by the deferred cleanup afterwards.
	closeErr := supervisor.Close()
	supervisorClosed = true
	<-drained
	log.Info("broker_offline_stopped")
	return closeErr
}

// drainBrokerAccept continuously drains admitted broker sessions so the
// listener's bounded admission FIFO never stalls.
//
// A drained session is the listener's own connection-scoped service: closing it
// here would end a live client, so it is deliberately left open and owned by
// the listener. The drain stops when the listener closes or reports a terminal
// accept failure. A listener close is the expected, orderly end of the drain and
// is never logged; any other terminal accept failure is unexpected: it is logged
// as an error and reported on failed so the caller commits shutdown instead of
// leaving a bound broker that can no longer admit work.
func offlineRegistryConfig(config *brokerconfig.Config, log *slog.Logger) (broker.RegistryConfig, error) {
	if config == nil {
		return broker.RegistryConfig{}, errors.New("vev: offline registry requires configuration")
	}
	if _, ok := config.LocalBinding(); !ok {
		return broker.RegistryConfig{ObservationDisabled: true}, nil
	}
	local, err := brokerLocalObservation(config, log)
	if err != nil {
		return broker.RegistryConfig{}, err
	}
	return broker.RegistryConfig{Local: local}, nil
}

func drainBrokerAccept(listener ports.BrokerListener, log *slog.Logger, started chan<- struct{}, drained chan<- struct{}, failed chan<- struct{}) {
	defer close(drained)
	close(started)
	for {
		session, err := listener.Accept()
		if err != nil {
			if errors.Is(err, brokeripc.ErrListenerClosed) {
				return
			}
			log.Error("broker_offline_accept_failed", "error", err)
			close(failed)
			return
		}
		if session == nil {
			continue
		}
		log.Debug("broker_offline_connection", "connection", fmt.Sprintf("%x", session.ConnectionID()))
	}
}
