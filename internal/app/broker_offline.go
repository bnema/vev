package app

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/brokerstore"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/daemonidentity"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/logging"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/broker"
	"github.com/bnema/vev/pkg/safedir"
)

// Offline broker foreground composition.
//
// `_broker-serve` is a hidden, isolated sandbox command. It takes one strict
// offline root, derives only sandbox runtime, state, and log paths from it, and
// composes the broker core and local IPC listener over the immutable
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

const productionBrokerServeCommand = "_broker-production-serve"

// brokerServePoolLimits bound the offline broker pool. They are explicit, not
// derived from the environment, and small enough that a stalled peer is
// refused rather than absorbed.
var brokerServePoolLimits = broker.PoolLimits{
	Physical:         64,
	Clients:          2 * brokeripc.DefaultMaxClients,
	Streams:          256,
	StreamsPerClient: 64,
	Warm:             brokerconfig.DefaultWarmTransports,
}

// brokerPoolLimits applies the configured warm transport retention to the
// fixed pool bounds.
func brokerPoolLimits(config *brokerconfig.Config) broker.PoolLimits {
	limits := brokerServePoolLimits
	limits.Warm = config.WarmTransports()
	limits.Idle = config.WarmIdleTimeout()
	return limits
}

// brokerServeOptions is the parsed hidden invocation.
type brokerServeOptions struct {
	// idleGrace is zero when the operator did not override it; the supervisor
	// then applies its own 5m default.
	idleGrace time.Duration
}

// parseProductionBrokerServeArgs parses the argument-free hidden serve entry.
func parseProductionBrokerServeArgs(args []string) (command, error) {
	if len(args) != 0 {
		return command{}, usagef("`%s` does not accept arguments", productionBrokerServeCommand)
	}
	return command{kind: kindProductionBrokerServe, brokerServe: brokerServeOptions{}}, nil
}

// brokerServeDeps are the composition seams. Production supplies the real
// adapters; tests override the clock, the owner lock, the logger, and the
// readiness hook so idle shutdown and cleanup are deterministic.
type productionIdentityBinder struct {
	delegate ports.BrokerIdentityBinder
	stateDir string
	policy   ports.BrokerPolicy
}

func (b productionIdentityBinder) BindAuthenticatedIdentity(ctx context.Context, request ports.BrokerIdentityBindingRequest) (ports.BrokerDaemonIdentity, error) {
	if !request.Fence.Local {
		return b.delegate.BindAuthenticatedIdentity(ctx, request)
	}
	if request.Policy != b.policy {
		return "", errors.New("vev: local daemon policy authority changed")
	}
	identity, err := daemonidentity.Load(b.stateDir)
	if err != nil {
		return "", fmt.Errorf("vev: revalidate daemon authority: %w", err)
	}
	if identity != request.Identity {
		return "", errors.New("vev: local daemon identity authority changed")
	}
	return identity, nil
}

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
func runBrokerServeCommand(ctx context.Context, options brokerServeOptions) (retErr error) {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	observer, observerCloser, err := newPerformanceTrace(clock.New())
	if err != nil {
		return fmt.Errorf("vev: performance trace: %w", err)
	}
	if observer != nil {
		observer.ObserveRuntime(ports.NewRuntimeMark("broker", ports.RuntimeTransportDiagnostic, 0, true))
	}
	if observerCloser != nil {
		defer func() { retErr = errors.Join(retErr, observerCloser.Close()) }()
	}
	return runBrokerServe(ctx, options, defaultBrokerServeDeps())
}

// sandboxLogging configures broker logging inside the sandbox log directory.
// It never consults the production state directory.
func sandboxLogging(dir string) (*slog.Logger, io.Closer, error) {
	return logging.Setup(logging.Config{
		Dir:       dir,
		Component: logging.Broker,
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := ensureProductionBrokerConfig(); err != nil {
		return err
	}
	if err := safedir.EnsurePrivate(ipc.SocketDir()); err != nil {
		return fmt.Errorf("vev: secure broker runtime parent: %w", err)
	}
	layout := productionBrokerLayout()
	configPath := productionBrokerConfigPath()
	for _, dir := range []string{layout.Root, layout.Runtime, layout.State, layout.Log} {
		if err := safedir.EnsurePrivate(dir); err != nil {
			return fmt.Errorf("vev: secure broker sandbox directory %s: %w", dir, err)
		}
	}
	// Re-walk every component now that EnsurePrivate created them, so a
	// component swapped for a symlink in that window fails closed.
	if err := layout.VerifyCreated(); err != nil {
		return fmt.Errorf("vev: verify broker paths: %w", err)
	}
	// The broker is allowed to precede the daemon. Identity is therefore an
	// optional existing authority here; acquisition binds it after an
	// authenticated StartIfNeeded connection when the daemon is first born.
	identity, err := daemonidentity.Load(platform.StateDir())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("vev: load daemon authority: %w", err)
	}
	config, err := brokerconfig.LoadProduction(layout, configPath, identity, localDaemonPolicy(), daemonmux.SocketPath(ipc.SocketDir()))
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

	// The durable route schema delegates SSH trust to OpenSSH. Refuse route
	// overrides that OpenSSH configuration must represent instead of dropping
	// security inputs during import.
	for _, endpoint := range config.Endpoints() {
		registration, _ := config.Registration(endpoint)
		if registration.Route.KnownHostsFile() != "" || registration.Route.ConnectTimeout() != 0 {
			return errors.New("vev: broker routes require SSH trust and timeouts in OpenSSH configuration")
		}
	}
	initialHosts, err := configuredBrokerHosts(config)
	if err != nil {
		return err
	}
	store, err := brokerstore.Open(brokerstore.Options{Dir: layout.State, InitialHosts: initialHosts, InitialImportProvided: true})
	if err != nil {
		return fmt.Errorf("vev: open broker sandbox store: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, store.Close()) }()
	if err := upgradeBrokerHostVersions(store); err != nil {
		return err
	}

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

	// Membership is imported once, then only durable remote authority is used.
	resolver := &brokerRoutes{local: config.Resolver()}
	connector, err := brokerDynamicMuxConnector(config, resolver, log)
	if err != nil {
		return err
	}
	loadLocalIdentity := func() (ports.BrokerDaemonIdentity, error) {
		identity, loadErr := daemonidentity.Load(platform.StateDir())
		if errors.Is(loadErr, os.ErrNotExist) {
			return "", nil
		}
		return identity, loadErr
	}
	registryConfig, err := offlineRegistryConfig(config, epoch, connector, loadLocalIdentity)
	if err != nil {
		return err
	}
	remoteProbe := &brokerRemoteProbe{epoch: epoch, routes: resolver, connector: connector, codec: sessionwire.BrokerCodec{}}
	registry, err := broker.NewRegistryWithConfig(epoch, store, remoteProbe, clk, log, registryConfig)
	if err != nil {
		return err
	}
	resolver.hosts = registry
	binder := productionIdentityBinder{delegate: registry, stateDir: platform.StateDir(), policy: localDaemonPolicy()}
	remoteProbe.binder = binder
	remoteProbe.hosts = registry
	// RegisterRunner starts immediately: finish the cyclic composition before
	// publishing it to any goroutine. Shutdown remains listener -> pool -> registry.
	if err := supervisor.RegisterRunner("registry", registry); err != nil {
		return err
	}
	pool, err := broker.NewPool(epoch, resolver, binder, connector, clk, brokerPoolLimits(config))
	if err != nil {
		return err
	}
	// Observation borrows pooled transports rather than dialing its own.
	remoteProbe.shared.share(pool)
	if registryConfig.Local != nil {
		if local, ok := registryConfig.Local.Probe.(*localRouteProbe); ok {
			local.shared.share(pool)
		}
	}
	if err := supervisor.RegisterCloseable("pool", pool); err != nil {
		return err
	}
	authority, err := broker.NewAuthority(epoch, registry, pool, supervisor, sessionwire.BrokerCodec{})
	if err != nil {
		return err
	}
	socketPath := brokeripc.SocketPath(layout.Runtime)
	listener, err := deps.listen(socketPath, epoch, authority, brokeripc.Config{OnRetire: cancel})
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
	log.Info("broker_ready", "socket", socketPath, "endpoints", len(config.Endpoints()))
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
	log.Info("broker_stopped")
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
// configuredBrokerHosts converts the one-time configuration import into store
// input. brokerstore consumes it only while creating its first manifest; an
// existing (including deliberately empty) membership is never reseeded.
func configuredBrokerHosts(config *brokerconfig.Config) ([]ports.BrokerHostRecord, error) {
	records := make([]ports.BrokerHostRecord, 0, len(config.Endpoints()))
	for _, endpoint := range config.Endpoints() {
		registration, ok := config.Registration(endpoint)
		if !ok {
			return nil, fmt.Errorf("vev: missing configured broker registration %q", endpoint)
		}
		route, err := durableBrokerRoute(registration.Route)
		if err != nil {
			return nil, fmt.Errorf("vev: import configured broker host %q: %w", endpoint, err)
		}
		records = append(records, ports.BrokerHostRecord{Registration: registration.Registration, Pinned: true, Policy: registration.Policy, Identity: registration.Identity, Route: route})
	}
	return records, nil
}

func durableBrokerRoute(route brokerconfig.Route) (ports.BrokerRouteSpec, error) {
	spec := ports.BrokerRouteSpec{Path: route.Path(), Target: route.Target(), Argv: route.Argv()}
	switch route.Kind() {
	case brokerconfig.RouteUnix:
		spec.Kind = ports.BrokerRouteUnix
	case brokerconfig.RouteSSHStdio:
		spec.Kind = ports.BrokerRouteSSHStdio
	case brokerconfig.RouteSSHQUIC:
		spec.Kind = ports.BrokerRouteSSHQUIC
	default:
		return ports.BrokerRouteSpec{}, errors.New("unknown configured broker route kind")
	}
	if err := spec.Validate(); err != nil {
		return ports.BrokerRouteSpec{}, err
	}
	return spec, nil
}

// offlineRegistryConfig selects the registry's observation producers from one
// immutable configuration. A configuration that provisions a local binding
// always composes the broker-owned local producer over the broker's already
// owned endpoint connector, so the probe is dynamic: it derives its authority
// from the local route, the local daemon policy, and an identity loader that is
// re-read on every attempt, so the broker may precede the daemon and start
// observing the moment the daemon first publishes its identity. Such a
// composition is mutable: the broker owns host membership for its whole run,
// including a zero-host membership, so a caller can add and remove hosts.
//
// A configuration that provisions no local binding is an explicitly configured
// fixture: it is deliberately left observation-disabled, so a read-only
// snapshot owner issues no probes, arms no timers, and never reconciles. The
// disabled fixture's membership mode follows the endpoints it was provisioned
// with, exactly as before.
func offlineRegistryConfig(config *brokerconfig.Config, epoch ports.BrokerEpoch, connector ports.BrokerEndpointConnector, loaders ...localIdentityLoader) (broker.RegistryConfig, error) {
	if config == nil {
		return broker.RegistryConfig{}, errors.New("vev: offline registry requires configuration")
	}
	if _, ok := config.LocalBinding(); !ok {
		mode := broker.MembershipImmutable
		if len(config.Endpoints()) != 0 {
			mode = broker.MembershipMutable
		}
		return broker.RegistryConfig{ObservationDisabled: true, MembershipMode: mode}, nil
	}
	local, err := brokerLocalObservation(config, epoch, connector, loaders...)
	if err != nil {
		return broker.RegistryConfig{}, err
	}
	return broker.RegistryConfig{Local: local, MembershipMode: broker.MembershipMutable}, nil
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
