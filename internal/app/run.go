// Package app wires vev's runtime together via dependency injection. It is
// the single place where usecases (client, daemon) are connected to concrete
// adapters (ipc transport/listener, terminal); usecases themselves never
// import adapters.
package app

import (
	"bufio"
	"context"
	"crypto/rand"
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
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/adapters/noticefile"
	"github.com/bnema/vev/internal/adapters/observability"
	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/adapters/quic"
	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/adapters/shellcmd"
	snapshotadapter "github.com/bnema/vev/internal/adapters/snapshot"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/logging"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/internal/usecase/recovery"
	"github.com/bnema/vev/internal/usecase/remotes"
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
	kindQUICBootstrap
	kindQUICProxy
	kindRemotePreview
	kindUIDriver
	kindUIRemoteCleanup
	kindWebDaemon
	kindWebRenew
	kindWebServe
	kindHelp
	kindVersion
)

// command is the parsed CLI invocation: what to do, plus the attach intent
// and session name where relevant.
type command struct {
	kind                 cmdKind
	intent               uint8
	name                 string
	remoteTarget         string
	listHost             string
	listAll              bool
	hostAction           string
	hostTarget           string
	killAll              bool
	killDaemon           bool
	cmd                  cmdInvocation
	remotePreviewPayload string
	uiDriver             uiDriverOptions
	uiObserve            bool
	uiControl            bool
	uiSocket             string
	web                  webOptions
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
  vev --ui-observe    expose passive observation for this interactive client
                      (optional: --ui-socket PATH)
  vev --ui-control    expose observation and input control for this client
                      (optional: --ui-socket PATH)
  vev --web-daemon    start the private web terminal or print its current link
  vev --web-renew-token  revoke web access and print a fresh link
  Web options: --web-listen IP:PORT --web-origin http(s)://HOST[:PORT]
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
	listenDaemon  = func(dir string, observer ports.SerializedRuntimeObserver) (wire.Listener, error) {
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
	if err := platform.ActivateDevelopmentEnvironment(); err != nil {
		return err
	}
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
		return command{kind: kindAttach, intent: protocol.IntentEphemeral}, nil
	}
	if args[0] == "--ui-driver" {
		options, err := parseUIDriverArgs(args[1:])
		if err != nil {
			return command{}, err
		}
		return command{kind: kindUIDriver, uiDriver: options}, nil
	}

	var uiObserve, uiControl bool
	var uiSocket string
	for len(args) > 0 {
		switch args[0] {
		case "--ui-observe":
			uiObserve = true
		case "--ui-control":
			uiControl = true
		case "--ui-socket":
			if len(args) < 2 || args[1] == "" {
				return command{}, usagef("`--ui-socket` requires a path")
			}
			uiSocket = args[1]
			args = args[2:]
			continue
		default:
			goto parsedUIFlags
		}
		args = args[1:]
	}
parsedUIFlags:
	interactive, err := parseInteractiveUIFlags(nil, uiObserve, uiControl, uiSocket)
	if err != nil {
		return command{}, err
	}
	uiObserve, uiControl, uiSocket = interactive.observe, interactive.control, interactive.socket
	if len(args) == 0 {
		return command{kind: kindAttach, intent: protocol.IntentEphemeral, uiObserve: uiObserve, uiControl: uiControl, uiSocket: uiSocket}, nil
	}
	if (uiObserve || uiControl || uiSocket != "") && args[0] != "new" && args[0] != "attach" && args[0] != "a" {
		return command{}, usagef("UI flags are only valid for an attach command")
	}

	switch args[0] {
	case "--web-daemon", "--web-serve":
		return parseWebArgs(args)
	case "--web-renew-token":
		if len(args) != 1 {
			return command{}, usagef("`%s` does not accept arguments", args[0])
		}
		return command{kind: kindWebRenew}, nil
	case "--daemon":
		return command{kind: kindDaemon}, nil
	case "--daemon-launcher":
		return command{kind: kindDaemonLauncher}, nil
	case "_stdio":
		if len(args) != 1 {
			return command{}, usagef("`_stdio` does not accept a session name")
		}
		return command{kind: kindStdio}, nil
	case "_quic-bootstrap":
		if len(args) != 1 {
			return command{}, usagef("`_quic-bootstrap` does not accept a session name")
		}
		return command{kind: kindQUICBootstrap}, nil
	case "_quic-proxy":
		if len(args) != 1 {
			return command{}, usagef("`_quic-proxy` does not accept a session name")
		}
		return command{kind: kindQUICProxy}, nil
	case "_remote-preview":
		if len(args) != 2 || args[1] == "" {
			return command{}, usagef("`_remote-preview` requires one encoded request")
		}
		return command{kind: kindRemotePreview, remotePreviewPayload: args[1]}, nil
	case uiRemoteCleanupCommand:
		if len(args) != 1 {
			return command{}, usagef("`%s` does not accept arguments", uiRemoteCleanupCommand)
		}
		return command{kind: kindUIRemoteCleanup}, nil
	case "new":
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`new` requires a session name")
		}
		interactive, err := parseInteractiveUIFlags(args[2:], uiObserve, uiControl, uiSocket)
		if err != nil {
			return command{}, err
		}
		if err := domain.ValidateSessionName(args[1]); err != nil {
			return command{}, err
		}
		return command{kind: kindAttach, intent: protocol.IntentNew, name: args[1], uiObserve: interactive.observe, uiControl: interactive.control, uiSocket: interactive.socket}, nil
	case "attach", "a":
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`attach` requires a session name")
		}
		interactive, err := parseInteractiveUIFlags(args[2:], uiObserve, uiControl, uiSocket)
		if err != nil {
			return command{}, err
		}
		cmd := command{kind: kindAttach, intent: protocol.IntentAttach, name: args[1]}
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
				cmd.intent = protocol.IntentEphemeral
			}
		}
		cmd.uiObserve = interactive.observe
		cmd.uiControl = interactive.control
		cmd.uiSocket = interactive.socket
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
	case kindWebRenew:
		return renewWebToken(ctx)
	case kindWebDaemon:
		return launchWebDaemon(ctx, cmd.web)
	case kindWebServe:
		return runWebDaemon(ctx, cmd.web)
	case kindDaemon:
		return runDaemon()
	case kindDaemonLauncher:
		return runDaemonLauncher()
	case kindStdio:
		return runStdio(ctx)
	case kindQUICBootstrap:
		return runQUICBootstrap(ctx)
	case kindQUICProxy:
		return runQUICProxy(ctx)
	case kindRemotePreview:
		return runRemotePreview(ctx, cmd.remotePreviewPayload)
	case kindUIRemoteCleanup:
		return runUIRemoteCleanup(ctx)
	case kindUIDriver:
		return runUIDriver(ctx, cmd.uiDriver)
	case kindList:
		return runList(ctx, cmd)
	case kindHost:
		return runHostCommand(ctx, cmd, defaultRemoteHostDeps())
	case kindKill:
		return runKill(ctx, cmd.name, cmd.killAll, cmd.killDaemon)
	case kindCmd:
		return runCmd(ctx, cmd.cmd)
	case kindAttach:
		return runAttachWithOptions(ctx, cmd.intent, cmd.name, cmd.remoteTarget, interactiveUIOptions{observe: cmd.uiObserve, control: cmd.uiControl, socket: cmd.uiSocket})
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

type remoteDialerForTarget func(target, session string, mode remoteadapter.TransportMode, log *slog.Logger) (wire.Dialer, error)

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
	newRemotePreviewClient                    = remoteadapter.NewPreviewClient
	newRemoteDialerFactoryWithRuntimeObserver = func(observer ports.SerializedRuntimeObserver) remoteDialerForTarget {
		return remoteadapter.NewDialerFactoryWithRuntimeObserver(observer).DialerForRemote
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
	reporter := observability.NewSerialized(observer, runtimeTraceQueueDepth)
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

func developmentTempDirOption(ensurePrivate func(string) error) (daemon.Option, error) {
	dir, active := platform.DevelopmentEnvironmentTempDir()
	if !active {
		return nil, nil
	}
	if err := ensurePrivate(dir); err != nil {
		return nil, fmt.Errorf("vev: secure development temp directory: %w", err)
	}
	return daemon.WithTempDir(dir), nil
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
	listen func() (wire.Listener, error),
) (*daemon.Daemon, wire.Listener, error) {
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
	allowedRemoteEndpoints, allowlistConfigured, err := remoteLaunchAllowlistFromEnv()
	if err != nil {
		return err
	}
	var remoteDiscoveryOpt daemon.Option
	if allowlistConfigured {
		remoteDiscoveryOpt, err = remoteDiscoveryDaemonOption(platform.StateDir(), os.Getenv(envRemoteTransport), clk, log, allowedRemoteEndpoints)
	} else {
		remoteDiscoveryOpt, err = remoteDiscoveryDaemonOption(platform.StateDir(), os.Getenv(envRemoteTransport), clk, log)
	}
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
	developmentTempOpt, err := developmentTempDirOption(safedir.EnsurePrivate)
	if err != nil {
		return err
	}
	if developmentTempOpt != nil {
		daemonOpts = append(daemonOpts, developmentTempOpt)
	}
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
	coordinator := recovery.NewCoordinator(opened.Catalogue, snapshotRepository, rand.Reader)
	if opened.Migration.Performed {
		recoveryMode = "catalogue-migrated"
		log.Info("catalogue_migrated", "source_formats", opened.Migration.SourceFormats, "target_format", opened.Migration.TargetFormat, "records", opened.Migration.RecordCount, "backup", opened.Migration.BackupPath)
	}
	logCatalogueRecovery(log, opened.Records, recoveryMode)
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
		func() (wire.Listener, error) { return listenDaemon(ipc.SocketDir(), observer) },
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
	if err := d.Serve(ctx, sessionwire.NewServerListener(ln)); err != nil {
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
		localDialer: func() wire.Dialer {
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

const (
	envRemoteTransport     = "VEV_REMOTE_TRANSPORT"
	uiRemoteCleanupCommand = "_ui-cleanup"
)

func defaultLocalDialer() wire.Dialer { return localDaemonDialer{dir: ipc.SocketDir()} }

func defaultRemoteDialerFactory() remoteDialerForTarget {
	return remoteadapter.NewDialerFactory().DialerForRemote
}

type runClientFunc func(context.Context, client.Dependencies, client.AttachRequest) error

type runAttachDeps struct {
	configPath              func() string
	localDialer             func() wire.Dialer
	remoteDialerFactory     remoteDialerForTarget
	selectedRemoteTransport string
	runClient               runClientFunc
	createDetached          func(context.Context, string) error
	runtimeObserver         ports.SerializedRuntimeObserver
	ui                      *client.UI
	terminal                func() ports.Terminal
	// interactiveConsole decides whether the missing-session prompt may read
	// its answer from a console. Nil probes the client terminal's input.
	interactiveConsole     func(ports.Terminal) bool
	clock                  func() ports.Clock
	disableCapabilityProbe bool
	localEnvironment       []string
	remoteEnvironment      func(string) []string
	// clipboard reads a clipboard image on a remote route's Ctrl+V.
	// The client retains it across local-to-remote handoffs and only enables
	// interception while the active route is remote.
	clipboard ports.ClipboardReader
	// Optional remote-learning seams.
	stateDir  func() string
	hostStore ports.RemoteHostStore
}

func remoteTransportModeFromEnv(value string) (remoteadapter.TransportMode, error) {
	switch value {
	case "", string(remoteadapter.TransportQUIC):
		return remoteadapter.TransportQUIC, nil
	case string(remoteadapter.TransportStdio):
		return remoteadapter.TransportStdio, nil
	default:
		return "", fmt.Errorf("vev: invalid remote transport %q (want %q or %q)", value, remoteadapter.TransportQUIC, remoteadapter.TransportStdio)
	}
}

func validateRemoteAttachHandoff(target protocol.AttachTarget) error {
	if err := protocol.ValidateAttachTarget(target); err != nil {
		return err
	}
	if err := domain.ValidateRemoteHostTarget(target.Endpoint); err != nil {
		return err
	}
	if err := domain.ValidateSessionName(target.Session); err != nil {
		return err
	}
	if target.RemoteTarget != nil {
		if target.EnvironmentPolicy != protocol.EnvironmentPolicyDaemonOwned {
			return errors.New("remote picker handoff must use daemon-owned environment")
		}
		if target.RemoteTarget.Endpoint != target.Endpoint || target.RemoteTarget.SessionName != target.Session {
			return errors.New("remote picker handoff identity does not match route")
		}
	}
	return nil
}

// remoteDiscoveryDaemonOption constructs the daemon-owned discovery ports from
// the same validated transport selection used by direct remote attach, then
// composes the bounded remote runtime and its monitor. Construction performs
// no I/O; the daemon starts the monitor without waiting for readiness.
func remoteDiscoveryDaemonOption(stateDir, transport string, clk ports.Clock, log *slog.Logger, allowlists ...map[string]struct{}) (daemon.Option, error) {
	_, err := remoteTransportModeFromEnv(transport)
	if err != nil {
		return nil, err
	}
	store := ports.RemoteHostStore(newRemoteHostStore(remoteadapter.HostStorePath(stateDir)))
	catalog := ports.RemoteCatalogClient(newRemoteCatalogClient())
	cache := ports.RemoteCatalogCache(newRemoteCatalogCache(remoteadapter.CatalogCachePath(stateDir)))
	previewClient := ports.RemotePreviewClient(newRemotePreviewClient())
	if len(allowlists) > 0 {
		allowed := allowlists[0]
		store = allowlistedRemoteHostStore{delegate: store, allowed: allowed}
		catalog = allowlistedRemoteCatalogClient{delegate: catalog, allowed: allowed}
		cache = allowlistedRemoteCatalogCache{delegate: cache, allowed: allowed}
		previewClient = allowlistedRemotePreviewClient{delegate: previewClient, allowed: allowed}
	}
	preview := daemon.WithRemotePreview(previewClient)
	monitor := remotes.NewMonitor(remoteadapter.NewRuntime(store, catalog, cache, log), clk, log)
	remoteMonitor := daemon.WithRemoteMonitor(monitor, monitor.Run)
	return func(d *daemon.Daemon) {
		preview(d)
		remoteMonitor(d)
	}, nil
}

func runAttachWithDeps(ctx context.Context, intent uint8, name, remoteTarget, activeSession string, log *slog.Logger, deps runAttachDeps) error {
	if activeSession != "" {
		if remoteTarget == "" && intent == protocol.IntentNew {
			return deps.createDetached(ctx, name)
		}
		return errors.New("vev: sessions should be nested with care; unset VEV to force")
	}

	if intent == protocol.IntentAttach && remoteTarget == "" {
		resolved, err := resolveMissingSessionAttach(ctx, name, deps, log)
		if err != nil {
			return err
		}
		intent = resolved
	}

	runClient := deps.runClient
	if runClient == nil {
		runClient = runClientWithDeps
	}
	localDialer := deps.localDialer
	if localDialer == nil {
		localDialer = defaultLocalDialer
	}
	mode, modeErr := remoteTransportModeFromEnv(deps.selectedRemoteTransport)
	if deps.remoteDialerFactory == nil {
		deps.remoteDialerFactory = defaultRemoteDialerFactory()
	}
	var remoteSelection *domain.RemoteSessionTarget
	remoteEnvironmentPolicy := protocol.EnvironmentPolicyDaemonOwned
	remoteDisplayOrigin := domain.RemoteDisplayOrigin(remoteTarget)
	routeOrigin := protocol.RouteOriginLocal
	routeOriginKey := "local"
	if remoteTarget != "" {
		routeOrigin = protocol.RouteOriginRemote
		routeOriginKey = remoteTarget
		if modeErr != nil {
			return modeErr
		}
	}
	pickerHandoff := remoteTarget == ""
	pickerEnvironmentPolicy := func(target *domain.RemoteSessionTarget, intent uint8, policy protocol.EnvironmentPolicy) protocol.EnvironmentPolicy {
		if pickerHandoff && target == nil && intent != protocol.IntentNew {
			return protocol.EnvironmentPolicyDaemonOwned
		}
		return policy
	}
	configPath := deps.configPath
	if configPath == nil {
		configPath = platform.ConfigPath
	}
	cfg, warnings, configErr := config.Load(configPath())
	if configErr != nil {
		if log != nil {
			log.Warn("loading client config failed; using defaults", "err", configErr)
		}
		cfg = domain.Defaults()
	}
	if log != nil {
		logConfigWarnings(log, warnings)
	}
	// The launching client owns one host registry for the whole run: endpoint
	// bindings are resolved once and reused by every later handoff, and the
	// discovery loop it drives lives exactly as long as the runner.
	registry := newClientHostRegistry(deps, mode, modeErr, log)

	handoffAttempts := 0
	for {
		var err error
		if remoteTarget != "" {
			if log != nil {
				log.Info("attaching to remote session", "target", remoteTarget, "name", name, "transport", string(mode))
			}
			// The initial remote attach resolves through the same registry as
			// every later handoff, so one endpoint has one carriage authority.
			binding, resolveErr := registry.ResolveEndpoint(ctx, remoteTarget)
			if resolveErr != nil {
				return resolveErr
			}
			err = runClient(ctx, client.Dependencies{
				AttachmentCache:        cfg.AttachmentCache,
				Dialer:                 binding.Dialer,
				LocalControlDialer:     sessionwire.NewClientDialer(dialOnlyLocalDialer{dir: ipc.SocketDir(), observer: deps.runtimeObserver}),
				Terminal:               clientTerminal(deps),
				Clock:                  clientClock(deps),
				DisableCapabilityProbe: deps.disableCapabilityProbe,
				UI:                     deps.ui,
				Clipboard:              deps.clipboard,
				Logger:                 log,
				RuntimeObserver:        deps.runtimeObserver,
				HostRegistry:           registry,
				Remote:                 true,
				Origin:                 protocol.RouteOriginRemote,
				OriginKey:              remoteTarget,
			}, client.AttachRequest{
				Intent:            intent,
				SessionName:       name,
				Remote:            true,
				Environment:       binding.Environment,
				Origin:            routeOrigin,
				OriginKey:         routeOriginKey,
				RemoteTarget:      remoteSelection,
				HostLabel:         remoteDisplayOrigin,
				EnvironmentPolicy: remoteEnvironmentPolicy,
			})
		} else {
			if log != nil {
				log.Info("attaching to local session", "intent", intent, "name", name)
			}
			err = runClient(ctx, client.Dependencies{
				AttachmentCache:        cfg.AttachmentCache,
				Dialer:                 sessionwire.NewClientDialer(localDialer()),
				LocalControlDialer:     sessionwire.NewClientDialer(dialOnlyLocalDialer{dir: ipc.SocketDir(), observer: deps.runtimeObserver}),
				Terminal:               clientTerminal(deps),
				Clock:                  clientClock(deps),
				DisableCapabilityProbe: deps.disableCapabilityProbe,
				UI:                     deps.ui,
				Clipboard:              deps.clipboard,
				Logger:                 log,
				RuntimeObserver:        deps.runtimeObserver,
				HostRegistry:           registry,
				Remote:                 false,
				Origin:                 protocol.RouteOriginLocal,
				OriginKey:              "local",
			}, client.AttachRequest{Intent: intent, SessionName: name, Environment: append([]string(nil), deps.localEnvironment...), Origin: protocol.RouteOriginLocal, OriginKey: "local"})
		}

		var handoffErr *client.AttachTargetError
		if !errors.As(err, &handoffErr) {
			return err
		}
		if handoffErr == nil {
			return fmt.Errorf("vev: invalid remote attach handoff")
		}
		if err := validateRemoteAttachHandoff(handoffErr.Target); err != nil {
			return fmt.Errorf("vev: invalid remote attach handoff: %w", err)
		}
		if handoffAttempts >= maxAttachTargetHandoffs {
			return fmt.Errorf("vev: attach handoff exceeded maximum of %d attempts", maxAttachTargetHandoffs)
		}
		handoffAttempts++
		remoteTarget = handoffErr.Target.Endpoint
		remoteDisplayOrigin = domain.RemoteDisplayOrigin(remoteTarget)
		routeOrigin = protocol.RouteOriginDiscovery
		routeOriginKey = remoteTarget
		name = handoffErr.Target.Session
		intent = handoffErr.Target.Intent
		remoteSelection = nil
		if handoffErr.Target.RemoteTarget != nil {
			copyTarget := *handoffErr.Target.RemoteTarget
			remoteSelection = &copyTarget
			remoteDisplayOrigin = copyTarget.DisplayOrigin
		}
		remoteEnvironmentPolicy = pickerEnvironmentPolicy(remoteSelection, handoffErr.Target.Intent, handoffErr.Target.EnvironmentPolicy)
		if modeErr != nil {
			return modeErr
		}
	}
}

const daemonStopTimeout = 2 * time.Second

var errDaemonNotRunning = errors.New("vev: no daemon running")

type localDaemonDialer struct {
	dir         string
	observer    ports.SerializedRuntimeObserver
	executable  string
	environment []string
}

// dialOnlyLocalDialer is the inventory control source: a typed dial-only
// connection to the local daemon socket. Unlike localDaemonDialer it never
// ensures, spawns, or starts the daemon and never creates an attachment: a
// missing socket reports local control unavailability instead of starting
// a daemon implicitly.
type dialOnlyLocalDialer struct {
	dir      string
	observer ports.SerializedRuntimeObserver
}

func (d dialOnlyLocalDialer) Dial(ctx context.Context) (wire.Transport, error) {
	if d.observer != nil {
		return ipc.DialContext(ctx, d.dir, ipc.WithRuntimeObserver(d.observer))
	}
	return ipc.DialContext(ctx, d.dir)
}

func (d localDaemonDialer) Dial(ctx context.Context) (wire.Transport, error) {
	dial := realDial
	if d.observer != nil {
		dial = func(ctx context.Context, dir string) (wire.Transport, error) {
			return ipc.DialContext(ctx, dir, ipc.WithRuntimeObserver(d.observer))
		}
	}
	spawn := realSpawn
	if d.executable != "" {
		spawn = func() error { return spawnDaemonWithEnvironment(d.executable, d.environment) }
	}
	return ensureDaemonWithLifecycle(ctx, d.dir, dial, spawn, defaultBackoff)
}

func detachedLocalHello(name, cwd string) protocol.Hello {
	termEnv := os.Getenv("TERM")
	return protocol.Hello{
		Version:   protocol.Version,
		Intent:    protocol.IntentNew,
		Name:      name,
		Size:      domain.Size{Cols: 80, Rows: 24},
		TermEnv:   termEnv,
		Cwd:       cwd,
		TrueColor: client.DetectTrueColor(termEnv, os.Getenv("COLORTERM"), os.Environ()),
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
	connection := sessionwire.NewClientConnection(transport)
	if err := connection.SendClient(hello); err != nil {
		return fmt.Errorf("vev: creating detached session: %w", err)
	}

	type detachedReply struct {
		message protocol.ServerMessage
		err     error
	}
	replyCh := make(chan detachedReply, 1)
	go func() {
		message, err := connection.ReceiveServer()
		replyCh <- detachedReply{message: message, err: err}
	}()

	select {
	case <-ctx.Done():
		_ = transport.Close()
		return ctx.Err()
	case reply := <-replyCh:
		if reply.err != nil {
			return fmt.Errorf("vev: awaiting detached session creation: %w", reply.err)
		}
		switch message := reply.message.(type) {
		case protocol.Welcome:
			return nil
		case protocol.ErrorMsg:
			return &client.ProtocolError{Code: message.Code, Text: message.Text}
		default:
			return fmt.Errorf("vev: unexpected reply %T to detached session creation", reply.message)
		}
	}
}

func runUIRemoteCleanup(ctx context.Context) error {
	root := os.Getenv("VEV_ENV_ROOT")
	if !filepath.IsAbs(root) || filepath.Clean(root) == string(filepath.Separator) {
		return errors.New("vev: remote UI cleanup requires a private absolute launch root")
	}
	if runtimeDir := os.Getenv("XDG_RUNTIME_DIR"); runtimeDir == "" || runtimeDir != filepath.Join(root, "runtime") {
		return errors.New("vev: remote UI cleanup requires the launch runtime directory")
	}
	if err := requestDaemonStop(ctx); err != nil && !errors.Is(err, errDaemonNotRunning) {
		return err
	}
	return nil
}

func requestDaemonStop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, daemonStopTimeout)
	defer cancel()

	transport, owner, err := waitForDaemonOrLifecycle(ctx, ipc.SocketDir(), realDial, defaultBackoff)
	if err != nil {
		return fmt.Errorf("vev: stopping daemon: %w", err)
	}
	if owner != nil {
		return errors.Join(errDaemonNotRunning, owner.Release())
	}
	defer func() { _ = transport.Close() }()
	connection := sessionwire.NewClientConnection(transport)
	if err := connection.SendClient(protocol.Kill{Scope: protocol.KillDaemon}); err != nil {
		return fmt.Errorf("vev: requesting daemon stop: %w", err)
	}
	if _, err := connection.ReceiveServer(); err != nil && !errors.Is(err, io.EOF) {
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
	transport, err := ensureDaemonWithLifecycle(ctx, ipc.SocketDir(), func(ctx context.Context, dir string) (wire.Transport, error) {
		return ipc.DialContext(ctx, dir, ipc.WithRuntimeObserver(observer))
	}, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	defer func() { _ = transport.Close() }()

	// sshstdio.Close owns and closes writers that implement io.Closer so a
	// blocked Send can unwind. Keep process-global stdin/stdout outside that
	// ownership boundary: sshstdio recognizes process stdin as a shared
	// cancellable pump, while output is an owned pipe drained by a forwarding
	// goroutine.
	stdin, stdout := os.Stdin, os.Stdout
	stdoutReader, stdoutWriter := io.Pipe()
	forwardDone := make(chan struct{})
	go func() {
		defer close(forwardDone)
		if _, err := io.Copy(stdout, stdoutReader); err != nil {
			_ = stdoutReader.CloseWithError(err)
		}
	}()
	defer func() {
		_ = stdoutWriter.Close()
		<-forwardDone
		_ = stdoutReader.Close()
	}()
	stdio := sshstdio.NewTransport(stdin, stdoutWriter, nil, sshstdio.WithRuntimeObserver(observer))
	return proxyTransports(ctx, stdio, transport, log)
}

// runQUICBootstrap starts a detached QUIC proxy, forwards its single
// readiness line, and exits so the SSH channel closes without owning
// the session lifetime. The detached proxy serves exactly one
// authenticated stream, then exits deterministically.
func runQUICBootstrap(ctx context.Context) error {
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
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		_ = w.Close()
		return err
	}
	defer func() { _ = devNull.Close() }()

	cmd := exec.CommandContext(ctx, exe, "_quic-proxy")
	// The proxy writes diagnostics through configureLogging; stdio is
	// detached here so the bootstrap SSH channel can close. The proxy
	// owns the session lifetime in its own session (Setsid) and is
	// released: the bootstrap child exits after forwarding readiness.
	cmd.Stdin = devNull
	cmd.Stdout = w
	cmd.Stderr = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		_ = w.Close()
		return err
	}
	_ = w.Close()

	readyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
		return fmt.Errorf("vev: quic bootstrap readiness: %w", readyCtx.Err())
	}
}

// runQUICProxy is the detached long-lived remote-side QUIC proxy: one
// ephemeral listener/certificate, one atomic token, private IPC only
// after the first authenticated stream. It exits after session
// termination, token expiry, or setup failure.
func runQUICProxy(ctx context.Context) (retErr error) {
	log, logCloser, err := configureLogging(logging.Stdio, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	log.Debug("quic proxy starting")
	server, readiness, err := quic.NewServer()
	if err != nil {
		return err
	}
	defer func() { _ = server.Close() }()
	raw, err := quic.EncodeReadiness(readiness)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(os.Stdout, "%s\n", raw); err != nil {
		return err
	}
	// Serve exactly one authenticated session, then exit
	// deterministically. Accept fails closed on token expiry, so the
	// proxy cannot outlive its advertised credential.
	transport, err := server.Accept(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = transport.Close()
		_ = waitGracefulTeardown(transport)
	}()
	return proxyQUICBootstrap(ctx, transport)
}

// gracefulTeardownWaiter is implemented by carriages (QUIC) whose Close defers
// a bounded connection close past its return. The _quic-proxy owns its
// process, so it must wait for that close before returning: exiting first
// would discard the final synchronous envelope Close already handed to the
// stream.
type gracefulTeardownWaiter interface {
	WaitGracefulTeardown(ctx context.Context) error
}

// proxyTeardownTimeout bounds the proxy's wait for a deferred graceful close,
// leaving headroom beyond the adapter's own graceful window. A carriage that
// does not defer its close is not waited on.
const proxyTeardownTimeout = 2 * time.Second

func waitGracefulTeardown(transport wire.Transport) error {
	waiter, ok := transport.(gracefulTeardownWaiter)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), proxyTeardownTimeout)
	defer cancel()
	return waiter.WaitGracefulTeardown(ctx)
}

// proxyQUICBootstrap bridges the single authenticated QUIC stream to the
// local daemon over private IPC. The daemon side is wrapped as a typed
// server connection by the daemon's own accept path; here the bridge
// copies raw envelopes both ways until either side closes. No session
// bytes flow before auth: Accept returned only the authenticated stream.
func proxyQUICBootstrap(ctx context.Context, transport wire.Transport) error {
	daemonTr, err := ensureDaemonWithLifecycle(ctx, ipc.SocketDir(), func(ctx context.Context, dir string) (wire.Transport, error) {
		return ipc.DialContext(ctx, dir)
	}, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	defer func() { _ = daemonTr.Close() }()
	return proxyTransports(ctx, transport, daemonTr, slog.Default())
}

// proxyTransports copies raw envelopes both ways until either side
// closes. It is carriage-neutral (IPC, SSH, QUIC): the only datagram
// special case (hello output-window clamping) lived in the removed
// datagram proxy, not here.
func proxyTransports(ctx context.Context, a, b wire.Transport, _ *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Own both carriages: closing them releases the copy goroutines'
	// blocked Recv calls (with serialized end marks) instead of
	// abandoning them mid-Recv when the first direction fails.
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	errCh := make(chan error, 2)
	go func() { errCh <- copyTransport(ctx, a, b) }()
	go func() { errCh <- copyTransport(ctx, b, a) }()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func copyTransport(ctx context.Context, src, dst wire.Transport) error {
	for {
		envelope, err := src.Recv()
		if err != nil {
			return err
		}
		if err := dst.Send(envelope); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
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

func listLocalSessions(ctx context.Context) (_ []protocol.SessionInfo, retErr error) {
	return listSessionsWithDialer(ctx, func(ctx context.Context) (wire.Transport, error) {
		return realDial(ctx, ipc.SocketDir())
	})
}

// listSessionsWithDialer runs the session listing over an explicit dialer.
// The attach pre-flight passes the local dialer (spawning the daemon when
// needed, like any attach); tests inject mocks.
func listSessionsWithDialer(ctx context.Context, dial func(context.Context) (wire.Transport, error)) (_ []protocol.SessionInfo, retErr error) {
	transport, owner, err := waitForDaemonOrLifecycle(ctx, ipc.SocketDir(), func(ctx context.Context, _ string) (wire.Transport, error) {
		return dial(ctx)
	}, defaultBackoff)
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
		infos := make([]protocol.SessionInfo, 0, len(records))
		for _, r := range records {
			state := protocol.SessionDown
			if r.DegradedReason != "" {
				state = protocol.SessionBroken
			}
			infos = append(infos, protocol.SessionInfo{Name: r.Name, State: state})
		}
		return infos, nil
	}
	reply, err := boundedListExchange(ctx, transport)
	if err != nil {
		return nil, err
	}
	return decodeSessionListReply(reply)
}

// preflightListTimeout bounds the attach pre-flight session listing so a
// socket that accepts but never replies cannot delay the attach fallback
// or block termination.
var preflightListTimeout = 5 * time.Second

// boundedListExchange sends a session listing request and reads the reply,
// closing the transport if the bound lapses or the parent context ends so
// a socket that accepts but never replies neither delays the attach
// fallback nor blocks Ctrl-C exit. Transport.Close interrupts blocked Send
// and Recv.
func boundedListExchange(ctx context.Context, transport wire.Transport) (protocol.ServerMessage, error) {
	listCtx, cancel := context.WithTimeout(ctx, preflightListTimeout)
	defer cancel()
	stopClose := context.AfterFunc(listCtx, func() { _ = transport.Close() })
	defer stopClose()
	defer func() { _ = transport.Close() }()

	connection := sessionwire.NewClientConnection(transport)
	if err := connection.SendClient(protocol.List{}); err != nil {
		if listCtx.Err() != nil {
			return nil, fmt.Errorf("vev: requesting session list: %w", listCtx.Err())
		}
		return nil, fmt.Errorf("vev: requesting session list: %w", err)
	}
	reply, err := connection.ReceiveServer()
	if err != nil {
		if listCtx.Err() != nil {
			return nil, fmt.Errorf("vev: reading session list: %w", listCtx.Err())
		}
		return nil, fmt.Errorf("vev: reading session list: %w", err)
	}
	return reply, nil
}

func decodeSessionListReply(reply protocol.ServerMessage) ([]protocol.SessionInfo, error) {
	if em, ok := reply.(protocol.ErrorMsg); ok {
		return nil, fmt.Errorf("vev: %s", em.Text)
	}
	sessions, ok := reply.(protocol.Sessions)
	if !ok {
		return nil, fmt.Errorf("vev: unexpected reply %T to list", reply)
	}
	return sessions.Sessions, nil
}

// printSessions renders a session table (or a friendly note when empty).
func printSessions(w io.Writer, sessions []protocol.SessionInfo) {
	if len(sessions) == 0 {
		_, _ = fmt.Fprintln(w, "no sessions")
		return
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "NAME\tSTATE\tTABS\tATTACHED")
	for _, s := range sessions {
		state := "up"
		tabs := fmt.Sprintf("%d", s.Tabs)
		attached := "no"
		switch s.State {
		case protocol.SessionDown:
			state = "down"
			tabs = "-"
		case protocol.SessionBroken:
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
		if daemon && errors.Is(err, ErrDaemonUnreachable) {
			return forceStopDaemonFallback(ctx, fmt.Errorf("vev: waiting for durable session state: %w", err))
		}
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
		return errDaemonNotRunning
	}
	defer func() { _ = transport.Close() }()
	connection := sessionwire.NewClientConnection(transport)

	scope := protocol.KillSession
	if all {
		scope = protocol.KillAll
	} else if daemon {
		scope = protocol.KillDaemon
	}
	if err := connection.SendClient(protocol.Kill{Name: name, Scope: scope}); err != nil {
		cause := fmt.Errorf("vev: requesting kill: %w", err)
		if daemon {
			return forceStopDaemonFallback(ctx, cause)
		}
		return cause
	}
	reply, err := connection.ReceiveServer()
	if err != nil && !errors.Is(err, io.EOF) {
		cause := fmt.Errorf("vev: reading kill reply: %w", err)
		if daemon {
			return forceStopDaemonFallback(ctx, cause)
		}
		return cause
	}
	if err == nil {
		if em, ok := reply.(protocol.ErrorMsg); ok {
			cause := fmt.Errorf("vev: %s", em.Text)
			if daemon {
				return forceStopDaemonFallback(ctx, cause)
			}
			return cause
		}
	}
	if all || daemon {
		waitCtx, cancel := context.WithTimeout(ctx, daemonStopTimeout)
		defer cancel()
		owner, waitErr := waitForLifecycleAvailability(waitCtx, ipc.SocketDir(), defaultBackoff)
		if waitErr != nil {
			cause := fmt.Errorf("vev: waiting for daemon ownership transfer: %w", waitErr)
			if daemon {
				return forceStopDaemonFallback(ctx, cause)
			}
			return cause
		}
		if releaseErr := owner.Release(); releaseErr != nil {
			return fmt.Errorf("vev: releasing daemon ownership probe: %w", releaseErr)
		}
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
