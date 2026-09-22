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

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/config"
	"github.com/bnema/vev/internal/adapters/daemonidentity"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/adapters/noticefile"
	"github.com/bnema/vev/internal/adapters/observability"
	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/adapters/quic"
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
	kindUIDriver
	kindUIRemoteCleanup
	kindWebDaemon
	kindWebRenew
	kindWebServe
	kindBrokerServe
	kindBrokerClient
	kindBrokerLauncher
	kindBrokerStatus
	kindBrokerMuxStdio
	kindBrokerMuxQUICBootstrap
	kindBrokerMuxQUICProxy
	kindProductionBrokerServe
	kindProductionBrokerLauncher
	kindBrokerReady
	kindHelp
	kindVersion
)

// command is the parsed CLI invocation: what to do, plus the attach intent
// and session name where relevant.
type command struct {
	kind           cmdKind
	intent         uint8
	name           string
	remoteTarget   string
	listHost       string
	listAll        bool
	hostAction     string
	hostTarget     string
	killAll        bool
	killDaemon     bool
	cmd            cmdInvocation
	brokerServe    brokerServeOptions
	brokerClient   brokerClientOptions
	brokerLauncher brokerLauncherOptions
	brokerStatus   brokerStatusOptions
	brokerMux      brokerMuxOptions
	brokerReady    brokerReadyOptions
	uiDriver       uiDriverOptions
	uiObserve      bool
	uiControl      bool
	uiSocket       string
	web            webOptions
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
  vev kill --all      kill all sessions (the daemon keeps running)
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
	case brokerServeCommand:
		return parseBrokerServeArgs(args[1:])
	case productionBrokerServeCommand:
		return parseProductionBrokerServeArgs(args[1:])
	case brokerClientCommand:
		return parseBrokerClientArgs(args[1:])
	case brokerLauncherCommand:
		return parseBrokerLauncherArgs(args[1:])
	case productionBrokerLauncherCommand:
		return parseProductionBrokerLauncherArgs(args[1:])
	case brokerStatusCommand:
		return parseBrokerStatusArgs(args[1:])
	case brokerReadyCommand:
		options, err := parseBrokerReadyArgs(args[1:])
		return command{kind: kindBrokerReady, brokerReady: options}, err
	case brokerMuxStdioCommand:
		return parseBrokerMuxArgs(brokerMuxStdioCommand, kindBrokerMuxStdio, args[1:])
	case brokerMuxQUICBootstrapCommand:
		return parseBrokerMuxArgs(brokerMuxQUICBootstrapCommand, kindBrokerMuxQUICBootstrap, args[1:])
	case brokerMuxQUICProxyCommand:
		return parseBrokerMuxArgs(brokerMuxQUICProxyCommand, kindBrokerMuxQUICProxy, args[1:])
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
	case uiRemoteCleanupCommand:
		if len(args) != 1 {
			return command{}, usagef("`%s` does not accept arguments", uiRemoteCleanupCommand)
		}
		return command{kind: kindUIRemoteCleanup}, nil
	case "new":
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`new` requires a session name")
		}
		name := args[1]
		var remoteTarget string
		optionStart := 2
		if len(args) > 2 && !strings.HasPrefix(args[2], "--") {
			remoteTarget = args[2]
			if err := domain.ValidateRemoteHostTarget(remoteTarget); err != nil {
				return command{}, err
			}
			optionStart = 3
		}
		interactive, err := parseInteractiveUIFlags(args[optionStart:], uiObserve, uiControl, uiSocket)
		if err != nil {
			return command{}, err
		}
		if err := domain.ValidateSessionName(name); err != nil {
			return command{}, err
		}
		return command{kind: kindAttach, intent: protocol.IntentNew, name: name, remoteTarget: remoteTarget, uiObserve: interactive.observe, uiControl: interactive.control, uiSocket: interactive.socket}, nil
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
	case kindBrokerServe:
		return runBrokerServeCommand(ctx, cmd.brokerServe)
	case kindBrokerClient:
		return runBrokerClientCommand(ctx, cmd.brokerClient)
	case kindBrokerLauncher:
		return runBrokerLauncherCommand(ctx, cmd.brokerLauncher)
	case kindBrokerStatus:
		return runBrokerStatusCommand(ctx, cmd.brokerStatus)
	case kindBrokerMuxStdio:
		return runBrokerMuxStdioCommand(ctx, cmd.brokerMux)
	case kindBrokerMuxQUICBootstrap:
		return runBrokerMuxQUICBootstrapCommand(ctx, cmd.brokerMux)
	case kindBrokerMuxQUICProxy:
		return runBrokerMuxQUICProxyCommand(ctx, cmd.brokerMux)
	case kindProductionBrokerServe:
		return runBrokerServeCommand(ctx, cmd.brokerServe)
	case kindProductionBrokerLauncher:
		return runProductionBrokerLauncherCommand(ctx)
	case kindBrokerReady:
		return runBrokerReady(ctx, cmd.brokerReady, os.Stdout)
	case kindStdio:
		return runStdio(ctx)
	case kindQUICBootstrap:
		return runQUICBootstrap(ctx)
	case kindQUICProxy:
		return runQUICProxy(ctx)
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

// performanceTrace creates one serialized timestamp owner for this process.
// An empty trace environment leaves all production behavior and wire bytes
// unchanged.
// Composition-root factory seams keep observer propagation testable without
// opening real transports.
var connectBroker = connectProductionBroker

var newPerformanceTrace = performanceTrace

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

func loadConfigOrDefaults(log *slog.Logger, path string) domain.Config {
	cfg, warnings, err := config.Load(path)
	if err != nil {
		if log != nil {
			log.Warn("loading config failed; using defaults", "path", path, "err", err)
		}
		cfg = domain.Defaults()
	}
	if log != nil {
		logConfigWarnings(log, warnings)
	}
	return cfg
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

	var daemonOpts []daemon.Option
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
	cfg := loadConfigOrDefaults(log, configPath)
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
	identity, err := daemonidentity.LoadOrCreate(stateDir)
	if err != nil {
		return fmt.Errorf("vev: establish daemon identity: %w", err)
	}
	incarnation, err := daemonidentity.NewIncarnation()
	if err != nil {
		return fmt.Errorf("vev: establish daemon incarnation: %w", err)
	}
	binding, err := daemonmux.NewServerBindings(identity, incarnation, []daemonmux.ServerPolicyAdmission{
		{Policy: localDaemonPolicy(), Origin: ports.SessionOriginLocal},
		{Policy: remoteBrokerPolicy("quic"), Origin: ports.SessionOriginRemote},
		{Policy: remoteBrokerPolicy("stdio"), Origin: ports.SessionOriginRemote},
	})
	if err != nil {
		return fmt.Errorf("vev: establish daemon mux authority: %w", err)
	}
	aggregate := daemonmux.NewAggregateListener()
	muxSupervisor, err := daemonmux.NewServerSupervisor(aggregate, binding, daemonmux.DefaultMuxCeilings(), 0)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, muxSupervisor.Close()) }()
	muxListener, err := ipc.ListenMux(daemonmux.SocketPath(ipc.SocketDir()))
	if err != nil {
		return fmt.Errorf("vev: listen daemon mux: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, muxListener.Close()) }()
	go func() {
		for {
			raw, acceptErr := muxListener.Accept()
			if acceptErr != nil {
				return
			}
			go func() { _ = muxSupervisor.Adopt(ctx, raw) }()
		}
	}()

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
	if err := aggregate.Register(sessionwire.NewServerListener(ln)); err != nil {
		return fmt.Errorf("vev: register local session listener: %w", err)
	}
	if err := d.Serve(ctx, aggregate); err != nil {
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
	if os.Getenv("VEV") != "" {
		if remoteTarget == "" && intent == protocol.IntentNew {
			return createDetachedTerminalSession(ctx, name)
		}
		return errors.New("vev: sessions should be nested with care; unset VEV to force")
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	_, logCloser, err := configureLogging(logging.Client, false)
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
	if observer != nil {
		observer.ObserveRuntime(ports.NewRuntimeMark("client", ports.RuntimeTransportDiagnostic, 0, true))
	}
	terminal := terminalForAttach()
	intent, preconnected, err := resolveAttachCreationIntent(ctx, intent, name, remoteTarget, terminal)
	if err != nil {
		return err
	}
	navigation, resolver, err := terminalBrokerNavigation(intent, name, remoteTarget)
	if err != nil {
		if preconnected != nil {
			_ = preconnected.Close()
		}
		return err
	}
	callbacks := terminalBrokerCallbacks()
	return runBrokerClient(ctx, brokerClientConfig{
		Connector: withPreconnected(newProductionBrokerConnector(), preconnected), Terminal: terminal, Clock: clk,
		InitialNavigation: navigation, ResolveInitialNavigation: resolver,
		AttachmentEnvironment: terminalAttachmentEnvironment(), SessionEnvironment: terminalSessionEnvironment(),
		OnState: callbacks.OnState, OnLifecycle: callbacks.OnLifecycle, OnFailure: callbacks.OnFailure,
	})
}

const (
	envRemoteTransport     = "VEV_REMOTE_TRANSPORT"
	uiRemoteCleanupCommand = "_ui-cleanup"
)

func remoteTransportModeFromEnv(value string) (string, error) {
	switch value {
	case "", "quic":
		return "quic", nil
	case "stdio":
		return "stdio", nil
	default:
		return "", fmt.Errorf("vev: invalid remote transport %q (want %q or %q)", value, "quic", "stdio")
	}
}

const daemonStopTimeout = 2 * time.Second

var (
	errDaemonNotRunning   = errors.New("vev: no daemon running")
	errKillOutcomeUnknown = errors.New("vev: kill outcome unknown")
	// errKillNotSent reports a Kill that was never placed on the wire because
	// its caller was already canceled. The outcome is definite: the daemon
	// cannot have acted, so it must not be reported as an unknown outcome.
	errKillNotSent = errors.New("vev: kill request not sent")
)

func newControlRequestID() (uint64, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0, err
	}
	id := uint64(0)
	for _, b := range raw {
		id = id<<8 | uint64(b)
	}
	if id == 0 {
		id = 1
	}
	return id, nil
}

// sendKillRequest allocates a unique nonzero RequestID and sends one Kill
// request, returning the ID its matching KillResult must carry. A caller
// already canceled before the send returns errKillNotSent (a definite
// not-sent outcome) instead of sending a request whose reply could no longer be
// awaited; the request is never replayed.
func sendKillRequest(ctx context.Context, connection ports.ClientConnection, request protocol.Kill) (uint64, error) {
	requestID, err := newControlRequestID()
	if err != nil {
		return 0, fmt.Errorf("preparing request: %w", err)
	}
	request.RequestID = requestID
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("%w: %w", errKillNotSent, err)
	}
	if err := connection.SendClient(request); err != nil {
		return 0, err
	}
	return requestID, nil
}

// receiveKillResult consumes exactly one correlated KillResult from a control
// connection. A definite failed result becomes a plain error; a close,
// cancellation, or an uncorrelated reply becomes errKillOutcomeUnknown, so a
// caller never treats a lost result as success and never replays the kill.
func receiveKillResult(ctx context.Context, connection ports.ClientConnection, requestID uint64) error {
	// Cancellation after the request was sent closes the exact connection so a
	// blocked receive unwinds; the lost reply stays outcome-unknown rather than
	// becoming a silent success or a blind retry.
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()

	reply, err := connection.ReceiveServer()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("%w: awaiting explicit result: %v", errKillOutcomeUnknown, ctxErr)
		}
		return fmt.Errorf("%w: awaiting explicit result: %v", errKillOutcomeUnknown, err)
	}
	result, ok := reply.(protocol.KillResult)
	if !ok || result.RequestID != requestID {
		return fmt.Errorf("%w: unexpected reply %T", errKillOutcomeUnknown, reply)
	}
	switch result.Outcome {
	case protocol.KillSucceeded:
		return nil
	case protocol.KillFailed:
		return fmt.Errorf("vev: %s", result.Text)
	default:
		return fmt.Errorf("%w: %s", errKillOutcomeUnknown, result.Text)
	}
}

func createDetachedLocalSession(ctx context.Context, name string) error {
	service, err := connectBroker(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", errDaemonUnreachable, err)
	}
	defer func() { _ = service.Close() }()
	operations, err := client.NewBrokerOperations(service, clock.New())
	if err != nil {
		return err
	}
	identity, _ := parseVEVEnv(os.Getenv("VEV"))
	result, err := operations.Command(ctx, localBrokerOperationRoute(service.Snapshot()), protocol.CommandRequest{
		Version: protocol.Version, Slug: "new-session", Args: []string{name}, TargetSession: identity.session,
	})
	if err != nil {
		return err
	}
	return renderDetachedCreationResult(result)
}

func renderDetachedCreationResult(result protocol.CommandResult) error {
	return renderCommandResult(io.Discard, result)
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
	service, err := connectBroker(ctx)
	if err != nil {
		return unreachableBrokerError(err)
	}
	defer func() { _ = service.Close() }()
	operations, err := client.NewBrokerOperations(service, clock.New())
	if err != nil {
		return err
	}
	result, err := operations.StopDaemon(ctx, localBrokerOperationRoute(service.Snapshot()))
	if err != nil {
		return err
	}
	return brokerKillResultError(result)
}

func unreachableBrokerError(err error) error {
	return &exitCoded{code: 3, err: fmt.Errorf("%w: %v", errDaemonUnreachable, err)}
}

func brokerKillResultError(result protocol.KillResult) error {
	switch result.Outcome {
	case protocol.KillSucceeded:
		return nil
	case protocol.KillFailed:
		if result.Text == "" {
			result.Text = "kill failed"
		}
		return errors.New(result.Text)
	default:
		if result.Text == "" {
			result.Text = "daemon did not report a final outcome"
		}
		return &exitCoded{code: 3, err: fmt.Errorf("%w: %s", errKillOutcomeUnknown, result.Text)}
	}
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

// runList obtains local live data or a multi-host catalogue snapshot through
// the per-user broker. It never dials a daemon directly.
func runList(ctx context.Context, cmd command) error {
	service, err := connectBroker(ctx)
	if err != nil {
		return unreachableBrokerError(err)
	}
	defer func() { _ = service.Close() }()
	if cmd.listAll || cmd.listHost != "" {
		return runBrokerSnapshotList(cmd, service.Snapshot(), os.Stdout)
	}
	operations, err := client.NewBrokerOperations(service, clock.New())
	if err != nil {
		return err
	}
	sessions, err := operations.List(ctx, localBrokerOperationRoute(service.Snapshot()))
	if err != nil {
		return err
	}
	printSessions(os.Stdout, sessions)
	return nil
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

// runKill executes all kill operations through the per-user broker.
func runKill(ctx context.Context, name string, all, daemon bool) error {
	service, err := connectBroker(ctx)
	if err != nil {
		return unreachableBrokerError(err)
	}
	defer func() { _ = service.Close() }()
	operations, err := client.NewBrokerOperations(service, clock.New())
	if err != nil {
		return err
	}
	route := localBrokerOperationRoute(service.Snapshot())
	var result protocol.KillResult
	switch {
	case daemon:
		result, err = operations.StopDaemon(ctx, route)
	case all:
		result, err = operations.KillAll(ctx, route)
	default:
		result, err = operations.Kill(ctx, route, name)
	}
	if err != nil {
		return err
	}
	if err := brokerKillResultError(result); err != nil {
		return err
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
		fmt.Println("killed all sessions")
		return
	}
	fmt.Printf("killed %s\n", name)
}
