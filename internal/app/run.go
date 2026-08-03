// Package app wires vev's runtime together via dependency injection. It is
// the single place where usecases (client, daemon) are connected to concrete
// adapters (ipc transport/listener, terminal); usecases themselves never
// import adapters.
package app

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/bnema/vev/internal/adapters/clipboard"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/config"
	"github.com/bnema/vev/internal/adapters/dgram"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/adapters/noticefile"
	"github.com/bnema/vev/internal/adapters/observability"
	"github.com/bnema/vev/internal/adapters/pty"
	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/adapters/shellcmd"
	snapshotadapter "github.com/bnema/vev/internal/adapters/snapshot"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/logging"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/internal/usecase/recovery"
	pdgram "github.com/bnema/vev/pkg/dgram"
	"github.com/bnema/vev/pkg/safedir"
)

// cmdKind identifies which sub-command the CLI parsed.
type cmdKind int

const (
	kindAttach cmdKind = iota // ephemeral/new/attach — distinguished by intent
	kindList
	kindHost
	kindKill
	kindCmd
	kindDaemon
	kindDaemonLauncher
	kindStdio
	kindUDPBootstrap
	kindUDPProxy
	kindHelp
	kindVersion
)

// command is the parsed CLI invocation: what to do, plus the attach intent
// and session name where relevant.
type command struct {
	kind         cmdKind
	intent       uint8
	name         string
	remoteTarget string
	listHost     string
	listAll      bool
	hostAction   string
	hostTarget   string
	killAll      bool
	killDaemon   bool
	cmd          cmdInvocation
}

// usageError is a user-facing argument error; the app prints it (with usage)
// rather than a stack of wrapped internals.
type usageError struct{ msg string }

func (e *usageError) Error() string { return "vev: " + e.msg }

func usagef(format string, args ...any) error {
	return &usageError{msg: fmt.Sprintf(format, args...)}
}

const usageText = `vev — a terminal multiplexer

usage:
  vev                 attach to (or create) an ephemeral session
  vev new <name>      create and attach to a named session
  vev attach <name>   attach to an existing session (alias: a)
  vev attach user@host[:session]
                      attach through SSH to a remote vev daemon
  vev ls              list local sessions
  vev ls <host>       list sessions on a known remote host
  vev ls --all        list local and remote sessions
  vev host add <host> add a pinned remote host
  vev host rm <host>  remove a pinned remote host
  vev host list       list known remote hosts
  vev kill <name>     kill a session
  vev kill --all      kill all sessions and stop the daemon
  vev kill --daemon   stop the active vev daemon
  vev cmd <command>   run a control command (vev cmd --help)
  vev --help          show this help
  vev --version       show version`

// Build metadata. Defaults describe a plain `go build`; releases overwrite
// them via -ldflags "-X github.com/bnema/vev/internal/app.version=..." (and
// .commit / .date) — see .goreleaser.yaml.
var (
	version = "0.1.0-dev"
	commit  = "none"
	date    = "unknown"

	openCatalogue = persist.OpenOrCreate
	listenDaemon  = func(dir string, observer ports.SerializedRuntimeObserver) (ports.Listener, error) {
		return ipc.Listen(dir, ipc.WithRuntimeObserver(observer))
	}
)

// versionLine renders the --version output.
func versionLine() string {
	return fmt.Sprintf("vev %s (commit %s, built %s)", version, commit, date)
}

// Run is the entry point invoked by main. It parses args into a command and
// dispatches it.
func Run(args []string) error {
	cmd, err := parseArgs(args)
	if err != nil {
		return err
	}
	return dispatch(context.Background(), cmd)
}

// parseArgs turns the raw argv tail into a command. It is deliberately a
// pure function so the full dispatch table can be unit-tested without any
// I/O.
func parseArgs(args []string) (command, error) {
	if len(args) == 0 {
		return command{kind: kindAttach, intent: ports.IntentEphemeral}, nil
	}

	switch args[0] {
	case "--daemon":
		return command{kind: kindDaemon}, nil
	case "--daemon-launcher":
		return command{kind: kindDaemonLauncher}, nil
	case "_stdio":
		if len(args) > 2 {
			return command{}, usagef("`_stdio` accepts at most one session name")
		}
		cmd := command{kind: kindStdio}
		if len(args) == 2 {
			cmd.name = args[1]
		}
		return cmd, nil
	case "_udp-bootstrap":
		if len(args) > 2 {
			return command{}, usagef("`_udp-bootstrap` accepts at most one session name")
		}
		cmd := command{kind: kindUDPBootstrap}
		if len(args) == 2 {
			cmd.name = args[1]
		}
		return cmd, nil
	case "_udp-proxy":
		if len(args) > 2 {
			return command{}, usagef("`_udp-proxy` accepts at most one session name")
		}
		cmd := command{kind: kindUDPProxy}
		if len(args) == 2 {
			cmd.name = args[1]
		}
		return cmd, nil
	case "new":
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`new` requires a session name")
		}
		if len(args) > 2 {
			return command{}, usagef("`new` does not support command overrides")
		}
		if err := domain.ValidateSessionName(args[1]); err != nil {
			return command{}, err
		}
		return command{kind: kindAttach, intent: ports.IntentNew, name: args[1]}, nil
	case "attach", "a":
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`attach` requires a session name")
		}
		if len(args) > 2 {
			return command{}, usagef("`attach` accepts exactly one session name or remote target")
		}
		cmd := command{kind: kindAttach, intent: ports.IntentAttach, name: args[1]}
		if target, session, ok := parseRemoteAttachTarget(args[1]); ok {
			if err := domain.ValidateRemoteHostTarget(target); err != nil {
				return command{}, err
			}
			if session != "" {
				if err := domain.ValidateSessionName(session); err != nil {
					return command{}, err
				}
			}
			cmd.remoteTarget = target
			cmd.name = session
			if session == "" {
				cmd.intent = ports.IntentEphemeral
			}
		}
		return cmd, nil
	case "ls", "list":
		return parseListArgs(args[1:])
	case "host":
		return parseHostArgs(args[1:])
	case "cmd":
		invocation, err := parseCmdArgs(args[1:])
		if err != nil {
			return command{}, err
		}
		return command{kind: kindCmd, cmd: invocation}, nil
	case "kill":
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`kill` requires a session name, --all, or --daemon")
		}
		if args[1] == "--" {
			if len(args) != 3 || args[2] == "" {
				return command{}, usagef("`kill --` requires a session name")
			}
			return command{kind: kindKill, name: args[2]}, nil
		}
		if len(args) > 2 {
			return command{}, usagef("`kill` accepts exactly one session name, --all, or --daemon")
		}
		switch args[1] {
		case "--all":
			return command{kind: kindKill, killAll: true}, nil
		case "--daemon":
			return command{kind: kindKill, killDaemon: true}, nil
		default:
			return command{kind: kindKill, name: args[1]}, nil
		}
	case "-h", "--help", "help":
		return command{kind: kindHelp}, nil
	case "--version", "version":
		return command{kind: kindVersion}, nil
	default:
		return command{}, usagef("unknown command %q", args[0])
	}
}

// dispatch runs the parsed command.
func dispatch(ctx context.Context, cmd command) error {
	switch cmd.kind {
	case kindHelp:
		fmt.Println(usageText)
		return nil
	case kindVersion:
		fmt.Println(versionLine())
		return nil
	case kindDaemon:
		return runDaemon()
	case kindDaemonLauncher:
		return runDaemonLauncher()
	case kindStdio:
		return runStdio(ctx)
	case kindUDPBootstrap:
		return runUDPBootstrap(ctx, cmd.name)
	case kindUDPProxy:
		return runUDPProxy(ctx, cmd.name, os.Stdout)
	case kindList:
		return runList(ctx, cmd)
	case kindHost:
		return runHostCommand(ctx, cmd, defaultRemoteHostDeps())
	case kindKill:
		return runKill(ctx, cmd.name, cmd.killAll, cmd.killDaemon)
	case kindCmd:
		return runCmd(ctx, cmd.cmd)
	case kindAttach:
		return runAttach(ctx, cmd.intent, cmd.name, cmd.remoteTarget)
	default:
		return usagef("unhandled command")
	}
}

func parseListArgs(args []string) (command, error) {
	if len(args) == 0 {
		return command{kind: kindList}, nil
	}
	if len(args) > 1 {
		return command{}, usagef("`ls` accepts at most one host or --all")
	}
	switch args[0] {
	case "--all":
		return command{kind: kindList, listAll: true}, nil
	default:
		if strings.HasPrefix(args[0], "-") {
			return command{}, usagef("unknown flag %q for `ls`", args[0])
		}
		if err := domain.ValidateRemoteHostTarget(args[0]); err != nil {
			return command{}, err
		}
		return command{kind: kindList, listHost: args[0]}, nil
	}
}

func parseHostArgs(args []string) (command, error) {
	if len(args) == 0 {
		return command{}, usagef("`host` requires add, rm, or list")
	}
	switch args[0] {
	case hostActionAdd, hostActionRm:
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`host %s` requires a host target", args[0])
		}
		if len(args) > 2 {
			return command{}, usagef("`host %s` accepts exactly one host target", args[0])
		}
		if err := domain.ValidateRemoteHostTarget(args[1]); err != nil {
			return command{}, err
		}
		return command{kind: kindHost, hostAction: args[0], hostTarget: args[1]}, nil
	case hostActionList:
		if len(args) > 1 {
			return command{}, usagef("`host list` accepts no arguments")
		}
		return command{kind: kindHost, hostAction: hostActionList}, nil
	default:
		return command{}, usagef("unknown host action %q", args[0])
	}
}

// performanceTrace creates one serialized timestamp owner for this process.
// An empty trace environment leaves all production behavior and wire bytes
// unchanged.
// Composition-root factory seams keep observer propagation testable without
// opening real transports.
var (
	newPerformanceTrace                       = performanceTrace
	newRemoteHostStore                        = remoteadapter.NewFileHostStore
	newRemoteCatalogCache                     = remoteadapter.NewFileCatalogCache
	newRemoteCatalogClient                    = remoteadapter.NewCatalogClient
	newRemoteDialerFactoryWithRuntimeObserver = func(observer ports.SerializedRuntimeObserver) ports.RemoteDialerFactory {
		return remoteadapter.NewDialerFactoryWithRuntimeObserver(observer)
	}
	runClientWithDeps runClientFunc = func(
		ctx context.Context,
		deps client.Dependencies,
		request client.AttachRequest,
	) error {
		return client.NewRunner(deps).Run(ctx, request)
	}
)

func performanceTrace(clk ports.Clock) (ports.SerializedRuntimeObserver, io.Closer, error) {
	return performanceTraceWithFactories(clk, observability.NewJSONL, ports.NewRuntimeCorrelationObserver)
}

// performanceTraceWithFactories keeps setup rollback behavior directly
// testable without requiring an operating-system file close failure.
func performanceTraceWithFactories(
	clk ports.Clock,
	newSink func(string, ports.Clock, string) (ports.RuntimeObserver, io.Closer, error),
	newCorrelation func(ports.RuntimeObserver, ports.RuntimeCorrelationInputs) (ports.RuntimeObserver, error),
) (ports.SerializedRuntimeObserver, io.Closer, error) {
	path, processID := os.Getenv("VEV_PERF_TRACE"), os.Getenv("VEV_PERF_PROCESS_ID")
	if path == "" {
		return nil, nil, nil
	}
	observer, closer, err := newSink(path, clk, processID)
	if err != nil {
		return nil, nil, err
	}
	// The harness supplies these manifest fields to every launched role. Keep a
	// valid standalone trace for operators that set only the original trace
	// variables, while ensuring harness traces match their process mapping.
	inputs := ports.RuntimeCorrelationInputs{Scenario: os.Getenv("VEV_PERF_SCENARIO"), Run: 1}
	if inputs.Scenario == "" {
		inputs.Scenario = "runtime"
	}
	if rawRun := os.Getenv("VEV_PERF_RUN"); rawRun != "" {
		inputs.Run, err = strconv.ParseUint(rawRun, 10, 64)
		if err != nil {
			setupErr := fmt.Errorf("invalid VEV_PERF_RUN %q: %w", rawRun, err)
			return nil, nil, closeTraceAfterSetupFailure(closer, setupErr)
		}
		if inputs.Run == 0 {
			setupErr := fmt.Errorf("invalid VEV_PERF_RUN %q", rawRun)
			return nil, nil, closeTraceAfterSetupFailure(closer, setupErr)
		}
	}
	observer, err = newCorrelation(observer, inputs)
	if err != nil {
		setupErr := fmt.Errorf("configure runtime trace correlation: %w", err)
		return nil, nil, closeTraceAfterSetupFailure(closer, setupErr)
	}
	reporter := ports.NewSerializedRuntimeObserver(observer, runtimeTraceQueueDepth)
	return reporter, &runtimeTraceCloser{reporter: reporter, closer: closer}, nil
}

func closeTraceAfterSetupFailure(closer io.Closer, setupErr error) error {
	if closer == nil {
		return setupErr
	}
	if closeErr := closer.Close(); closeErr != nil {
		return errors.Join(setupErr, fmt.Errorf("close performance trace after setup failure: %w", closeErr))
	}
	return setupErr
}

// runtimeTraceQueueDepth bounds all process-local trace producer handoffs.
// A full queue emits the serialized diagnostic gap rather than delaying a
// terminal, transport, or ACK progress path.
const runtimeTraceQueueDepth = 256

// runtimeTraceCloser owns the two-stage trace shutdown: drain the reporter
// before closing the concrete timestamp/file owner. sync.Once makes every
// deferred and explicit close path safe without closing shared workers twice.
type runtimeTraceCloser struct {
	reporter ports.SerializedRuntimeObserver
	closer   io.Closer
	once     sync.Once
	err      error
}

func (c *runtimeTraceCloser) Close() error {
	if c == nil {
		return nil
	}
	c.once.Do(func() {
		if c.reporter != nil {
			c.reporter.Flush()
			c.reporter.Close()
		}
		if c.closer != nil {
			c.err = c.closer.Close()
		}
	})
	return c.err
}

func configureLogging(component logging.Component, rotateAtRuntime bool) (*slog.Logger, io.Closer, error) {
	return logging.Setup(logging.Config{
		Dir:             platform.StateDir(),
		Component:       component,
		Level:           logging.EnvLevel(),
		MaxBytes:        logging.DefaultMaxBytes,
		RotateAtRuntime: rotateAtRuntime,
	})
}

func logConfigWarnings(log *slog.Logger, warnings []domain.Warning) {
	for _, warning := range warnings {
		if warning.Line > 0 {
			log.Warn("config warning", "line", warning.Line, "msg", warning.Msg)
			continue
		}
		log.Warn("config warning", "msg", warning.Msg)
	}
}

func snapshotDir() string {
	return filepath.Join(platform.StateDir(), "snapshots")
}

func pprofAddrIsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type ownedDaemonStart func(context.Context) error

type lifecycleOwnership interface {
	Release() error
}

func joinLifecycleReleaseError(retErr *error, owner lifecycleOwnership) {
	*retErr = errors.Join(*retErr, owner.Release())
}

type lifecycleStartupDeps struct {
	ensurePrivate func(string) error
	acquire       func(context.Context, string, time.Duration) (lifecycleOwnership, error)
	log           *slog.Logger
}

const lifecycleAcquireRetry = 25 * time.Millisecond

func lifecycleStartupDependencies(log *slog.Logger) lifecycleStartupDeps {
	return lifecycleStartupDeps{
		ensurePrivate: safedir.EnsurePrivate,
		acquire: func(ctx context.Context, runtimeDir string, retry time.Duration) (lifecycleOwnership, error) {
			return lifecycle.Acquire(ctx, runtimeDir, retry)
		},
		log: log,
	}
}

func runWithLifecycleOwner(ctx context.Context, runtimeDir, stateDir string, start ownedDaemonStart) error {
	return runWithLifecycleOwnerDeps(ctx, runtimeDir, stateDir, start, lifecycleStartupDependencies(nil))
}

func runWithLifecycleOwnerDeps(ctx context.Context, runtimeDir, stateDir string, start ownedDaemonStart, deps lifecycleStartupDeps) (retErr error) {
	log := deps.log
	if log == nil {
		log = slog.Default()
	}
	if err := deps.ensurePrivate(runtimeDir); err != nil {
		return fmt.Errorf("vev: secure runtime directory: %w", err)
	}
	if err := deps.ensurePrivate(stateDir); err != nil {
		return fmt.Errorf("vev: secure state directory: %w", err)
	}
	lockPath := lifecycle.Path(runtimeDir)
	log.Info("lifecycle_owner_wait", "path", lockPath)
	owner, err := deps.acquire(ctx, runtimeDir, lifecycleAcquireRetry)
	if err != nil {
		log.Warn("lifecycle_owner_wait_failed", "path", lockPath, "reason_code", "acquire-failed")
		return fmt.Errorf("vev: acquire lifecycle ownership: %w", err)
	}
	log.Info("lifecycle_owner_acquired", "path", lockPath)
	defer func() {
		releaseErr := owner.Release()
		if releaseErr != nil {
			log.Error("lifecycle_owner_release_failed", "path", lockPath, "reason_code", "release-failed")
		} else {
			log.Info("lifecycle_owner_released", "path", lockPath)
		}
		retErr = errors.Join(retErr, releaseErr)
	}()
	if start == nil {
		return errors.New("vev: nil owned daemon startup")
	}
	return start(ctx)
}

// runDaemon runs the daemon in the foreground (the hidden --daemon path,
// entered by an auto-spawned child) while holding exclusive lifecycle
// ownership through complete teardown.
func runDaemon() (retErr error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	log, logCloser, err := configureLogging(logging.Daemon, true)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, logCloser.Close()) }()
	return runWithLifecycleOwnerDeps(ctx, ipc.SocketDir(), platform.StateDir(), func(ctx context.Context) error {
		return runDaemonOwnedWithLogger(ctx, log)
	}, lifecycleStartupDependencies(log))
}

func logCatalogueRecovery(log *slog.Logger, records []domain.CatalogueRecord, recoveryMode string) {
	log.Info("catalogue_validated", "records", len(records), "recovery", recoveryMode)
}

func logStartupRecoveryCounts(log *slog.Logger, records []domain.CatalogueRecord, restoring int) {
	healthy, fresh, broken := 0, 0, 0
	for _, record := range records {
		switch {
		case record.DegradedReason != "":
			broken++
		case record.Committed == nil:
			fresh++
		default:
			healthy++
		}
	}
	log.Info("daemon_startup_complete", "healthy", healthy, "fresh", fresh, "restoring", restoring, "broken", broken)
}

func constructDaemonBeforeSocketPublication(
	construct func() *daemon.Daemon,
	prepare func(*daemon.Daemon) error,
	listen func() (ports.Listener, error),
) (*daemon.Daemon, ports.Listener, error) {
	d := construct()
	if err := prepare(d); err != nil {
		return d, nil, err
	}
	ln, err := listen()
	return d, ln, err
}

func runDaemonOwned(ctx context.Context) (retErr error) {
	log, logCloser, err := configureLogging(logging.Daemon, true)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, logCloser.Close()) }()
	return runDaemonOwnedWithLogger(ctx, log)
}

func runDaemonOwnedWithLogger(ctx context.Context, log *slog.Logger) (retErr error) {
	clk := clock.New()
	observer, observerCloser, err := newPerformanceTrace(clk)
	if err != nil {
		return fmt.Errorf("vev: performance trace: %w", err)
	}
	if observerCloser != nil {
		defer func() { retErr = errors.Join(retErr, observerCloser.Close()) }()
	}
	remoteDiscoveryOpt, err := remoteDiscoveryDaemonOption(platform.StateDir(), observer, os.Getenv(envRemoteTransport))
	if err != nil {
		return err
	}
	if addr := os.Getenv("VEV_PPROF_ADDR"); addr != "" {
		if !pprofAddrIsLoopback(addr) {
			log.Warn("pprof bound to non-loopback address; /debug/pprof is unauthenticated", "addr", addr)
		}
		go func() {
			if err := http.ListenAndServe(addr, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("pprof server exited", "err", err)
			}
		}()
		log.Info("pprof enabled", "addr", addr)
	}

	daemonOpts := []daemon.Option{remoteDiscoveryOpt}
	if observer != nil {
		daemonOpts = append(daemonOpts, daemon.WithRuntimeObserver(observer))
	}
	configPath := platform.ConfigPath()
	cfg, warnings, err := config.Load(configPath)
	if err != nil {
		log.Warn("loading config failed; using defaults", "path", configPath, "err", err)
		cfg = domain.Defaults()
	}
	logConfigWarnings(log, warnings)
	daemonOpts = append(daemonOpts, daemon.WithConfig(cfg))
	daemonOpts = append(daemonOpts, daemon.WithBarScriptCommandRunner(shellcmd.New()))
	daemonOpts = append(daemonOpts, daemon.WithProcessInspector(platform.NewProcessInspector()), daemon.WithDirOrHome(platform.DirOrHome))
	snapshotRepository := snapshotadapter.NewRepositoryWithLogger(snapshotDir(), log)
	daemonOpts = append(daemonOpts, daemon.WithSnapshotRepository(snapshotRepository))
	noticeStore := noticefile.New(platform.StateDir())
	daemonOpts = append(daemonOpts, daemon.WithNoticeStore(noticeStore))
	stateDir := platform.StateDir()
	storePath := persist.StorePath(stateDir)
	opened, err := openCatalogue(stateDir)
	if err != nil {
		log.Error("catalogue_validation_failed", "path", storePath, "reason_code", "open-failed")
		if errors.Is(err, persist.ErrCatalogueUnreadable) {
			return unreadableCatalogueError(stateDir)
		}
		return fmt.Errorf("vev: open durable session state %s: %w", storePath, err)
	}
	recoveryMode := "current"
	if opened.NewInstall {
		recoveryMode = "new-install"
	}
	logCatalogueRecovery(log, opened.Records, recoveryMode)
	coordinator := recovery.NewCoordinator(opened.Catalogue, snapshotRepository, rand.Reader)
	daemonOpts = append(daemonOpts, daemon.WithRecoveryCoordinator(coordinator))
	daemonOpts = append(daemonOpts, daemon.WithDurableMaintenance(opened.Catalogue, snapshotRepository))
	log.Info("session persistence enabled", "path", storePath)
	daemonOpts = append(daemonOpts, daemon.WithCatalogue(opened.Catalogue, opened.Records))

	// Construct the catalogue-backed expected-session registry before socket
	// publication. Phase 3 snapshot restoration remains asynchronous in Serve.
	d, ln, err := constructDaemonBeforeSocketPublication(
		func() *daemon.Daemon { return daemon.New(pty.NewFactory(), clk, log, daemonOpts...) },
		func(d *daemon.Daemon) error {
			if err := d.CollectStartupGarbage(ctx); err != nil {
				// GC is best-effort, but it is fully finished before socket
				// publication. A failed pass leaves durable state untouched and
				// restoration retains its per-session failure isolation.
				log.Warn("snapshot_garbage_collection_failed", "err", err)
			}
			return nil
		},
		func() (ports.Listener, error) { return listenDaemon(ipc.SocketDir(), observer) },
	)
	if err != nil {
		closeErr := opened.Catalogue.Close()
		log.Error("daemon startup preparation failed", "socket_dir", ipc.SocketDir(), "err", err)
		return errors.Join(fmt.Errorf("vev: prepare daemon startup: %w", err), closeErr)
	}
	defer func() { _ = ln.Close() }()
	log.Info("daemon starting", "socket", ln.Addr())

	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	go func() {
		if err := config.Watch(watchCtx, clk, configPath, func(cfg domain.Config, warnings []domain.Warning) {
			logConfigWarnings(log, warnings)
			d.ApplyConfig(cfg)
		}); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("config watcher stopped", "path", configPath, "err", err)
		}
	}()
	if err := d.Serve(ctx, ln); err != nil {
		log.Error("daemon exited", "err", err)
		return err
	}
	log.Info("daemon exited cleanly")
	return nil
}

// runAttach dials (auto-spawning the daemon if needed) and runs the client
// attach loop. Logging goes to the shared file: the client must never write
// to the console while the terminal is raw.
func runAttach(ctx context.Context, intent uint8, name, remoteTarget string) (retErr error) {
	// Treat a controlling-terminal hangup or termination request as a graceful
	// detach rather than an abrupt process death. Cancelling the context unwinds
	// the client's pumps, closes its transport, and lets the deferred trace closer
	// flush the in-flight receive's end mark — so teardown never truncates a span.
	// Raw mode disables ISIG, so catching SIGINT here does not affect interactive
	// Ctrl+C, which the daemon delivers to the remote shell as a normal keystroke.
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	log, logCloser, err := configureLogging(logging.Client, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()

	clk := clock.New()
	observer, observerCloser, err := newPerformanceTrace(clk)
	if err != nil {
		return fmt.Errorf("vev: performance trace: %w", err)
	}
	if observerCloser != nil {
		defer func() { retErr = errors.Join(retErr, observerCloser.Close()) }()
	}
	return runAttachWithDeps(ctx, intent, name, remoteTarget, os.Getenv("VEV"), log, runAttachDeps{
		localDialer: func() ports.Dialer {
			return localDaemonDialer{dir: ipc.SocketDir(), observer: observer}
		},
		remoteDialerFactory:     newRemoteDialerFactoryWithRuntimeObserver(observer),
		selectedRemoteTransport: os.Getenv(envRemoteTransport),
		runClient:               runClientWithDeps,
		createDetached:          createDetachedLocalSession,
		clipboard:               clipboard.New(),
		runtimeObserver:         observer,
		stateDir:                platform.StateDir,
	})
}

const envRemoteTransport = "VEV_REMOTE_TRANSPORT"

func defaultLocalDialer() ports.Dialer { return localDaemonDialer{dir: ipc.SocketDir()} }

func defaultRemoteDialerFactory() ports.RemoteDialerFactory {
	return remoteadapter.NewDialerFactory()
}

type runClientFunc func(context.Context, client.Dependencies, client.AttachRequest) error

type runAttachDeps struct {
	localDialer             func() ports.Dialer
	remoteDialerFactory     ports.RemoteDialerFactory
	selectedRemoteTransport string
	runClient               runClientFunc
	createDetached          func(context.Context, string) error
	runtimeObserver         ports.SerializedRuntimeObserver
	// clipboard reads a clipboard image on a remote attach's Ctrl+V (see
	// docs/superpowers/specs/2026-07-04-clipboard-image-transfer-design.md).
	// Only used for the remote-dialer branch below; local attaches never
	// intercept Ctrl+V regardless of this field.
	clipboard ports.ClipboardReader
	// Optional remote-learning seams.
	stateDir  func() string
	hostStore ports.RemoteHostStore
}

func remoteTransportModeFromEnv(value string) (ports.RemoteTransportMode, error) {
	switch value {
	case "", string(ports.RemoteTransportUDP):
		return ports.RemoteTransportUDP, nil
	case string(ports.RemoteTransportStdio):
		return ports.RemoteTransportStdio, nil
	default:
		return "", fmt.Errorf("vev: invalid remote transport %q (want %q or %q)", value, ports.RemoteTransportUDP, ports.RemoteTransportStdio)
	}
}

func validateRemoteAttachHandoff(target ports.AttachTarget) error {
	if err := ports.ValidateAttachTarget(target); err != nil {
		return err
	}
	if err := domain.ValidateRemoteHostTarget(target.Endpoint); err != nil {
		return err
	}
	return domain.ValidateSessionName(target.Session)
}

// remoteDiscoveryDaemonOption constructs the daemon-owned discovery ports from
// the same validated transport selection used by direct remote attach.
func remoteDiscoveryDaemonOption(stateDir string, observer ports.SerializedRuntimeObserver, transport string) (daemon.Option, error) {
	mode, err := remoteTransportModeFromEnv(transport)
	if err != nil {
		return nil, err
	}
	return daemon.WithRemoteDiscovery(
		newRemoteHostStore(remoteadapter.HostStorePath(stateDir)),
		newRemoteCatalogClient(),
		newRemoteCatalogCache(remoteadapter.CatalogCachePath(stateDir)),
		newRemoteDialerFactoryWithRuntimeObserver(observer),
		mode,
	), nil
}

func runAttachWithDeps(ctx context.Context, intent uint8, name, remoteTarget, activeSession string, log *slog.Logger, deps runAttachDeps) error {
	if activeSession != "" {
		if remoteTarget == "" && intent == ports.IntentNew {
			return deps.createDetached(ctx, name)
		}
		return errors.New("vev: sessions should be nested with care; unset VEV to force")
	}

	runClient := deps.runClient
	if runClient == nil {
		runClient = runClientWithDeps
	}
	if remoteTarget != "" {
		mode, err := remoteTransportModeFromEnv(deps.selectedRemoteTransport)
		if err != nil {
			return err
		}
		factory := deps.remoteDialerFactory
		if factory == nil {
			factory = defaultRemoteDialerFactory()
		}
		for {
			if log != nil {
				log.Info("attaching to remote session", "target", remoteTarget, "name", name, "transport", string(mode))
			}
			dialer, err := factory.DialerForRemote(remoteTarget, name, mode, log)
			if err != nil {
				return err
			}
			err = runClient(ctx, client.Dependencies{
				Dialer:            dialer,
				Terminal:          term.New(),
				Clock:             clock.New(),
				Clipboard:         deps.clipboard,
				Logger:            log,
				RuntimeObserver:   deps.runtimeObserver,
				RemoteHostLearner: attachRememberLearner(deps, remoteTarget, log),
				Remote:            true,
			}, client.AttachRequest{Intent: intent, SessionName: name})
			var handoff *client.AttachTargetError
			if !errors.As(err, &handoff) {
				return err
			}
			if handoff == nil {
				return fmt.Errorf("vev: invalid remote attach handoff")
			}
			if err := validateRemoteAttachHandoff(handoff.Target); err != nil {
				return fmt.Errorf("vev: invalid remote attach handoff: %w", err)
			}
			remoteTarget = handoff.Target.Endpoint
			name = handoff.Target.Session
			intent = handoff.Target.Intent
		}
	}

	localDialer := deps.localDialer
	if localDialer == nil {
		localDialer = defaultLocalDialer
	}
	if log != nil {
		log.Info("attaching to local session", "intent", intent, "name", name)
	}
	return runClient(ctx, client.Dependencies{
		Dialer:          localDialer(),
		Terminal:        term.New(),
		Clock:           clock.New(),
		Logger:          log,
		RuntimeObserver: deps.runtimeObserver,
		Remote:          false,
	}, client.AttachRequest{Intent: intent, SessionName: name})
}

const daemonStopTimeout = 2 * time.Second

type localDaemonDialer struct {
	dir      string
	observer ports.SerializedRuntimeObserver
}

func (d localDaemonDialer) Dial(ctx context.Context) (ports.Transport, error) {
	dial := realDial
	if d.observer != nil {
		dial = func(ctx context.Context, dir string) (ports.Transport, error) {
			return ipc.DialContext(ctx, dir, ipc.WithRuntimeObserver(d.observer))
		}
	}
	return ensureDaemonWithLifecycle(ctx, d.dir, dial, realSpawn, defaultBackoff)
}

func detachedLocalHello(name, cwd string) ports.Hello {
	termEnv := os.Getenv("TERM")
	return ports.Hello{
		Version:   ports.ProtocolVersion,
		Intent:    ports.IntentNew,
		Name:      name,
		Size:      domain.Size{Cols: 80, Rows: 24},
		TermEnv:   termEnv,
		Cwd:       cwd,
		TrueColor: client.DetectTrueColor(termEnv, os.Getenv("COLORTERM")),
		Env:       os.Environ(),
	}
}

func createDetachedLocalSession(ctx context.Context, name string) error {
	transport, err := ensureDaemonWithLifecycle(ctx, ipc.SocketDir(), realDial, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	defer func() { _ = transport.Close() }()

	cwd, err := os.Getwd()
	if err != nil {
		cwd = ""
	}
	hello := detachedLocalHello(name, cwd)
	if err := transport.Send(ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)}); err != nil {
		return fmt.Errorf("vev: creating detached session: %w", err)
	}

	type detachedReply struct {
		frame ports.Frame
		err   error
	}
	replyCh := make(chan detachedReply, 1)
	go func() {
		f, err := transport.Recv()
		replyCh <- detachedReply{frame: f, err: err}
	}()

	select {
	case <-ctx.Done():
		_ = transport.Close()
		return ctx.Err()
	case reply := <-replyCh:
		if reply.err != nil {
			return fmt.Errorf("vev: awaiting detached session creation: %w", reply.err)
		}
		switch reply.frame.Type {
		case ports.MsgWelcome:
			return nil
		case ports.MsgError:
			em, derr := ports.UnmarshalErrorMsg(reply.frame.Payload)
			if derr != nil {
				return fmt.Errorf("vev: decoding error reply: %w", derr)
			}
			return &client.ProtocolError{Code: em.Code, Text: em.Text}
		default:
			return fmt.Errorf("vev: unexpected reply type %d to detached session creation", reply.frame.Type)
		}
	}
}

func requestDaemonStop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, daemonStopTimeout)
	defer cancel()

	transport, owner, err := waitForDaemonOrLifecycle(ctx, ipc.SocketDir(), realDial, defaultBackoff)
	if err != nil {
		return fmt.Errorf("vev: stopping daemon: %w", err)
	}
	if owner != nil {
		return errors.Join(errors.New("vev: no daemon running"), owner.Release())
	}
	defer func() { _ = transport.Close() }()
	if err := transport.Send(ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{All: true})}); err != nil {
		return fmt.Errorf("vev: requesting daemon stop: %w", err)
	}
	if _, err := transport.Recv(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("vev: reading daemon stop reply: %w", err)
	}

	owner, err = waitForLifecycleAvailability(ctx, ipc.SocketDir(), defaultBackoff)
	if err != nil {
		return fmt.Errorf("vev: waiting for daemon ownership transfer: %w", err)
	}
	return owner.Release()
}

// runStdio is the hidden remote-side mode used by `ssh host vev _stdio`: it
// connects to the per-user daemon (auto-spawning it if needed) and proxies the
// framed protocol between process stdio and the daemon socket.
func runStdio(ctx context.Context) (retErr error) {
	log, logCloser, err := configureLogging(logging.Stdio, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	log.Debug("stdio proxy starting")

	clk := clock.New()
	observer, observerCloser, err := newPerformanceTrace(clk)
	if err != nil {
		return fmt.Errorf("vev: performance trace: %w", err)
	}
	if observerCloser != nil {
		defer func() { retErr = errors.Join(retErr, observerCloser.Close()) }()
	}
	transport, err := ensureDaemonWithLifecycle(ctx, ipc.SocketDir(), func(ctx context.Context, dir string) (ports.Transport, error) {
		return ipc.DialContext(ctx, dir, ipc.WithRuntimeObserver(observer))
	}, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	defer func() { _ = transport.Close() }()
	stdio := sshstdio.NewTransport(os.Stdin, os.Stdout, nil, sshstdio.WithRuntimeObserver(observer))
	return runStdioProxy(ctx, stdio, transport, log)
}

var (
	udpBootstrapTimeout = 5 * time.Second
	udpProxyCommand     = func(ctx context.Context, exe string, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, exe, args...)
	}
)

// runUDPBootstrap starts a detached _udp-proxy, forwards its single readiness
// line, and exits so SSH stdio can close without owning the UDP proxy lifetime.
func runUDPBootstrap(ctx context.Context, _ string) error {
	// Always start a fresh proxy for a bootstrap request. A client attach begins
	// with MsgHello, which must reach the daemon handshake path; reusing an
	// already-running proxy would forward that Hello into the daemon connection's
	// post-handshake runConnLoop, where it is intentionally ignored. Every
	// bootstrap starts a fresh byte-only carriage.
	r, w, err := os.Pipe()
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()

	exe, err := os.Executable()
	if err != nil {
		_ = w.Close()
		return err
	}
	args := []string{"_udp-proxy"}
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		_ = w.Close()
		return err
	}
	defer func() { _ = devNull.Close() }()

	cmd := udpProxyCommand(ctx, exe, args...)
	// _udp-proxy writes diagnostics through configureLogging(logging.Stdio, false);
	// stdio is detached here so the bootstrap SSH channel can close.
	cmd.Stdin = devNull
	cmd.Stdout = w
	cmd.Stderr = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		_ = w.Close()
		return err
	}
	_ = w.Close()

	readyCtx, cancel := context.WithTimeout(ctx, udpBootstrapTimeout)
	defer cancel()
	lineCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(r).ReadString('\n')
		if err != nil {
			errCh <- err
			return
		}
		lineCh <- line
	}()
	select {
	case line := <-lineCh:
		if err := cmd.Process.Release(); err != nil {
			return err
		}
		_, err := fmt.Fprint(os.Stdout, line)
		return err
	case err := <-errCh:
		_ = cmd.Wait()
		return err
	case <-readyCtx.Done():
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("vev: udp bootstrap readiness: %w", readyCtx.Err())
	}
}

// runUDPProxy is the detached long-lived remote-side datagram proxy.
func runUDPProxy(ctx context.Context, _ string, ready io.Writer) (retErr error) {
	log, logCloser, err := configureLogging(logging.Stdio, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	log.Debug("udp carriage starting")

	observer, observerCloser, err := newPerformanceTrace(clock.New())
	if err != nil {
		return fmt.Errorf("vev: performance trace: %w", err)
	}
	if observerCloser != nil {
		defer func() { retErr = errors.Join(retErr, observerCloser.Close()) }()
	}
	portRange, err := parseUDPPortRange(os.Getenv(envUDPPortRange))
	if err != nil {
		return err
	}
	conn, err := listenUDPInRange(ctx, portRange)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	daemonTr, err := ensureDaemonWithLifecycle(ctx, ipc.SocketDir(), func(ctx context.Context, dir string) (ports.Transport, error) {
		return ipc.DialContext(ctx, dir, ipc.WithRuntimeObserver(observer))
	}, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	defer func() { _ = daemonTr.Close() }()
	udpOptions := udpProxyClientTransportOptions
	udpOptions.Observe = dgram.DiagnosticLogObserver(log)
	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return errors.New("vev: udp proxy did not get a UDP address")
	}
	var (
		dg       *dgram.Transport
		setupErr error
	)
	pdgram.SecretDo(func() {
		key := make([]byte, pdgram.KeySize)
		defer pdgram.Erase(key)
		if _, setupErr = rand.Read(key); setupErr != nil {
			return
		}
		dg, setupErr = dgram.NewTransportWithOptions(conn, nil, key, 2, 1, udpOptions, dgram.WithRuntimeObserver(observer))
		if setupErr != nil {
			return
		}
		readiness := append([]byte("VEV-UDP "), strconv.Itoa(addr.Port)...)
		readiness = append(readiness, ' ')
		readiness = base64.StdEncoding.AppendEncode(readiness, key)
		readiness = append(readiness, '\n')
		defer pdgram.Erase(readiness)
		_, setupErr = ready.Write(readiness)
		pdgram.Erase(readiness)
		pdgram.Erase(key)
	})
	if setupErr != nil {
		if dg != nil {
			_ = dg.Close()
		}
		return setupErr
	}
	defer func() { _ = dg.Close() }()
	return runUDPProxyRuntime(ctx, dg, daemonTr, log)
}

const udpProxyIdleTTL = 15 * time.Minute

var udpProxyClientTransportOptions = dgram.Options{
	MaxPending: 32,
	DeadAfter:  2 * udpProxyIdleTTL,
}

const (
	// envUDPPortRange configures the remote UDP proxy's listen port range so a
	// host firewall can allow a known range instead of a random ephemeral port.
	// Format: "START-END" (inclusive), a single "PORT", or "0" for a random
	// ephemeral port. Unset uses the default range below.
	envUDPPortRange     = "VEV_UDP_PORT_RANGE"
	defaultUDPPortStart = 61000
	defaultUDPPortEnd   = 61023
)

// udpPortRange is an inclusive [start, end] UDP port range. start == 0 means a
// random ephemeral port (the old ":0" behavior).
type udpPortRange struct {
	start int
	end   int
}

// parseUDPPortRange parses VEV_UDP_PORT_RANGE. Empty -> default range. "0" ->
// ephemeral. "N" -> single port N. "A-B" -> inclusive range. Ports must be in
// 1..65535 (or 0 for ephemeral) and end must be >= start.
func parseUDPPortRange(value string) (udpPortRange, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return udpPortRange{start: defaultUDPPortStart, end: defaultUDPPortEnd}, nil
	}
	startStr, endStr, hasRange := strings.Cut(value, "-")
	start, err := parseUDPPort(startStr)
	if err != nil {
		return udpPortRange{}, fmt.Errorf("invalid %s %q: %w", envUDPPortRange, value, err)
	}
	if start == 0 {
		if hasRange {
			return udpPortRange{}, fmt.Errorf("invalid %s %q: port 0 (ephemeral) cannot be combined with a range", envUDPPortRange, value)
		}
		return udpPortRange{start: 0, end: 0}, nil
	}
	end := start
	if hasRange {
		end, err = parseUDPPort(endStr)
		if err != nil {
			return udpPortRange{}, fmt.Errorf("invalid %s %q: %w", envUDPPortRange, value, err)
		}
	}
	if end < start {
		return udpPortRange{}, fmt.Errorf("invalid %s %q: end %d before start %d", envUDPPortRange, value, end, start)
	}
	return udpPortRange{start: start, end: end}, nil
}

func parseUDPPort(s string) (int, error) {
	p, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("port %q is not a number", strings.TrimSpace(s))
	}
	if p < 0 || p > 65535 {
		return 0, fmt.Errorf("port %d out of range 0-65535", p)
	}
	return p, nil
}

// listenUDPInRange binds a UDP packet conn on the first free port in the range,
// skipping busy ports. A zero-start range binds a random ephemeral port.
func listenUDPInRange(ctx context.Context, r udpPortRange) (net.PacketConn, error) {
	var lc net.ListenConfig
	if r.start == 0 {
		return lc.ListenPacket(ctx, "udp", ":0")
	}
	if r.start < 0 || r.end < r.start {
		return nil, fmt.Errorf("invalid UDP port range %d-%d", r.start, r.end)
	}
	var lastErr error
	for port := r.start; port <= r.end; port++ {
		conn, err := lc.ListenPacket(ctx, "udp", fmt.Sprintf(":%d", port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("no free UDP port in range %d-%d: %w", r.start, r.end, lastErr)
}

// runStdioProxy forwards the framed transport without interpreting protocol
// messages. Session selection remains in the Hello sent by the thin client.
func runStdioProxy(ctx context.Context, client, daemon ports.Transport, log *slog.Logger) error {
	return proxyTransports(ctx, client, daemon, log)
}

func runUDPProxyRuntime(ctx context.Context, client, daemon ports.Transport, log *slog.Logger) error {
	return dgram.ProxyRuntime{Client: client, Daemon: daemon, Log: log, IdleTTL: udpProxyIdleTTL}.Run(ctx)
}

func proxyTransports(ctx context.Context, a, b ports.Transport, log *slog.Logger) error {
	return dgram.ProxyRuntime{Client: a, Daemon: b, Log: log}.Run(ctx)
}

// runList prints the daemon's session listing. With no daemon running, it
// falls back to the persisted stopped-session records. Remote forms list one
// known host or local+remote when --all is set.
func runList(ctx context.Context, cmd command) (retErr error) {
	if cmd.listAll || cmd.listHost != "" {
		return runRemoteList(ctx, cmd, defaultRemoteHostDeps())
	}
	sessions, err := listLocalSessions(ctx)
	if sessions != nil {
		printSessions(os.Stdout, sessions)
	}
	if err != nil {
		return err
	}
	return nil
}

func listLocalSessions(ctx context.Context) (_ []ports.SessionInfo, retErr error) {
	transport, owner, err := waitForDaemonOrLifecycle(ctx, ipc.SocketDir(), realDial, defaultBackoff)
	if err != nil {
		return nil, fmt.Errorf("vev: waiting for durable session state: %w", err)
	}
	if owner != nil {
		defer joinLifecycleReleaseError(&retErr, owner)
		records, loadErr := persist.LoadReadOnly(platform.StateDir())
		if loadErr != nil {
			if errors.Is(loadErr, persist.ErrCatalogueUnreadable) {
				return nil, unreadableCatalogueError(platform.StateDir())
			}
			return nil, fmt.Errorf("vev: reading stored sessions: %w", loadErr)
		}
		infos := make([]ports.SessionInfo, 0, len(records))
		for _, r := range records {
			state := ports.SessionStopped
			if r.DegradedReason != "" {
				state = ports.SessionBroken
			}
			infos = append(infos, ports.SessionInfo{Name: r.Name, State: state})
		}
		return infos, nil
	}
	defer func() { _ = transport.Close() }()

	if err := transport.Send(ports.Frame{Type: ports.MsgList, Payload: ports.MarshalList(ports.List{})}); err != nil {
		return nil, fmt.Errorf("vev: requesting session list: %w", err)
	}
	reply, err := transport.Recv()
	if err != nil {
		return nil, fmt.Errorf("vev: reading session list: %w", err)
	}
	return decodeSessionListReply(reply)
}

func decodeSessionListReply(reply ports.Frame) ([]ports.SessionInfo, error) {
	if reply.Type == ports.MsgError {
		em, err := ports.UnmarshalErrorMsg(reply.Payload)
		if err != nil {
			return nil, fmt.Errorf("vev: decoding error reply: %w", err)
		}
		return nil, fmt.Errorf("vev: %s", em.Text)
	}
	if reply.Type != ports.MsgSessions {
		return nil, fmt.Errorf("vev: unexpected reply type %d to list", reply.Type)
	}
	sessions, err := ports.UnmarshalSessions(reply.Payload)
	if err != nil {
		return nil, fmt.Errorf("vev: decoding session list: %w", err)
	}
	return sessions.Sessions, nil
}

// printSessions renders a session table (or a friendly note when empty).
func printSessions(w io.Writer, sessions []ports.SessionInfo) {
	if len(sessions) == 0 {
		_, _ = fmt.Fprintln(w, "no sessions")
		return
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NAME\tSTATE\tTABS\tATTACHED")
	for _, s := range sessions {
		state := "running"
		tabs := fmt.Sprintf("%d", s.Tabs)
		attached := "no"
		switch s.State {
		case ports.SessionStopped:
			state = "stopped"
			tabs = "-"
		case ports.SessionBroken:
			state = "broken"
			tabs = "-"
		default:
			if s.Ephemeral {
				state = "temporary"
			}
		}
		if s.Attached {
			attached = "yes"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", s.Name, state, tabs, attached)
	}
	_ = tw.Flush()
}

func unreadableCatalogueError(stateDir string) error {
	return fmt.Errorf("%w: vev: durable session state at %s cannot be read and was left untouched.\n"+
		"vev does not erase it automatically. To start fresh, remove it:\n"+
		"    rm -rf %s", persist.ErrCatalogueUnreadable, stateDir, stateDir)
}

func runOfflineNamedKill(ctx context.Context, name string) (retErr error) {
	stateDir := platform.StateDir()
	if _, err := os.Stat(persist.StorePath(stateDir)); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("vev: no such session: %s", name)
	} else if err != nil {
		return fmt.Errorf("vev: reading stored sessions: %w", err)
	}
	repository := snapshotadapter.NewRepository(snapshotDir())
	opened, err := openCatalogue(stateDir)
	if err != nil {
		if errors.Is(err, persist.ErrCatalogueUnreadable) {
			return unreadableCatalogueError(stateDir)
		}
		return fmt.Errorf("vev: opening stored sessions: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, opened.Catalogue.Close()) }()

	if _, ok, err := opened.Catalogue.Record(name); err != nil {
		return fmt.Errorf("vev: reading stored session: %w", err)
	} else if !ok {
		return fmt.Errorf("vev: no such session: %s", name)
	}
	coordinator := recovery.NewCoordinator(opened.Catalogue, repository, rand.Reader)
	if err := coordinator.Delete(ctx, name); err != nil {
		return fmt.Errorf("vev: deleting stored session: %w", err)
	}
	return nil
}

// runKill asks the daemon to terminate a named session, every session, or the daemon.
func runKill(ctx context.Context, name string, all, daemon bool) (retErr error) {
	transport, owner, err := waitForDaemonOrLifecycle(ctx, ipc.SocketDir(), realDial, defaultBackoff)
	if err != nil {
		return fmt.Errorf("vev: waiting for durable session state: %w", err)
	}
	if owner != nil {
		defer joinLifecycleReleaseError(&retErr, owner)
		if name != "" && !all && !daemon {
			if err := runOfflineNamedKill(ctx, name); err != nil {
				return err
			}
			printKillSuccess(name, all, daemon)
			return nil
		}
		return fmt.Errorf("vev: no daemon running")
	}
	defer func() { _ = transport.Close() }()

	if err := transport.Send(ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{Name: name, All: all || daemon})}); err != nil {
		return fmt.Errorf("vev: requesting kill: %w", err)
	}
	reply, err := transport.Recv()
	if err != nil {
		// The daemon may close the connection after killing; treat a clean
		// EOF as success.
		if errors.Is(err, io.EOF) {
			printKillSuccess(name, all, daemon)
			return nil
		}
		return fmt.Errorf("vev: reading kill reply: %w", err)
	}
	if reply.Type == ports.MsgError {
		em, _ := ports.UnmarshalErrorMsg(reply.Payload)
		return fmt.Errorf("vev: %s", em.Text)
	}
	printKillSuccess(name, all, daemon)
	return nil
}

func printKillSuccess(name string, all, daemon bool) {
	if daemon {
		fmt.Println("killed daemon")
		return
	}
	if all {
		fmt.Println("killed all sessions and stopped daemon")
		return
	}
	fmt.Printf("killed %s\n", name)
}
