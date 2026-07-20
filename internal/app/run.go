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
	"github.com/bnema/vev/internal/usecase/confirm"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/pkg/kv"
)

// cmdKind identifies which sub-command the CLI parsed.
type cmdKind int

const (
	kindAttach cmdKind = iota // ephemeral/new/attach — distinguished by intent
	kindList
	kindKill
	kindDaemon
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
	killAll      bool
	killDaemon   bool
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
  vev ls              list sessions
  vev kill <name>     kill a session
  vev kill --all      kill all sessions and stop the daemon
  vev kill --daemon   stop the active vev daemon
  vev --help          show this help
  vev --version       show version`

// Build metadata. Defaults describe a plain `go build`; releases overwrite
// them via -ldflags "-X github.com/bnema/vev/internal/app.version=..." (and
// .commit / .date) — see .goreleaser.yaml.
var (
	version = "0.1.0-dev"
	commit  = "none"
	date    = "unknown"
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
			cmd.remoteTarget = target
			cmd.name = session
			if session == "" {
				cmd.intent = ports.IntentEphemeral
			}
		}
		return cmd, nil
	case "ls", "list":
		return command{kind: kindList}, nil
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
	case kindStdio:
		return runStdio(ctx)
	case kindUDPBootstrap:
		return runUDPBootstrap(ctx, cmd.name)
	case kindUDPProxy:
		return runUDPProxy(ctx, cmd.name, os.Stdout)
	case kindList:
		return runList(ctx)
	case kindKill:
		return runKill(ctx, cmd.name, cmd.killAll, cmd.killDaemon)
	case kindAttach:
		return runAttach(ctx, cmd.intent, cmd.name, cmd.remoteTarget)
	default:
		return usagef("unhandled command")
	}
}

// performanceTrace creates one serialized timestamp owner for this process.
// An empty trace environment leaves all production behavior and wire bytes
// unchanged.
// Composition-root factory seams keep observer propagation testable without
// opening real transports.
var (
	newPerformanceTrace                       = performanceTrace
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

// runDaemon runs the daemon in the foreground (the hidden --daemon path,
// entered by an auto-spawned child): it sets up logging, binds the socket,
// constructs the daemon, and serves until the last session exits or a
// termination signal arrives (graceful shutdown notifies attached clients).
func runDaemon() (retErr error) {
	log, logCloser, err := configureLogging(logging.Daemon, true)
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
	// Every accepted local connection shares this process's serialized sink.
	ln, err := ipc.Listen(ipc.SocketDir(), ipc.WithRuntimeObserver(observer))
	if err != nil {
		log.Error("daemon listen failed", "socket_dir", ipc.SocketDir(), "err", err)
		return fmt.Errorf("vev: daemon listen: %w", err)
	}
	defer func() { _ = ln.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

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

	log.Info("daemon starting", "socket", ln.Addr())
	daemonOpts := []daemon.Option(nil)
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
	daemonOpts = append(daemonOpts, daemon.WithSnapshotStore(snapshotadapter.NewStore(snapshotDir())))
	storePath := persist.StorePath(platform.StateDir())
	var storeErr error
	if store, err := kv.Open(storePath); err != nil {
		log.Warn("opening session store failed; persistence disabled", "path", storePath, "err", err)
		storeErr = err
	} else {
		log.Info("session persistence enabled", "path", storePath)
		daemonOpts = append(daemonOpts, daemon.WithStore(store))
	}
	d := daemon.New(pty.NewFactory(), clk, log, daemonOpts...)
	if storeErr != nil {
		d.NotifyGlobal(domain.NoticeWarn, domain.NoticePersistDisabled,
			"session persistence is disabled; sessions will not survive daemon restarts", storeErr)
	}
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
		if log != nil {
			log.Info("attaching to remote session", "target", remoteTarget, "name", name, "transport", string(mode))
		}
		dialer, err := factory.DialerForRemote(remoteTarget, name, mode, log)
		if err != nil {
			return err
		}
		return runClient(ctx, client.Dependencies{
			Dialer:          dialer,
			Terminal:        term.New(),
			Clock:           clock.New(),
			Clipboard:       deps.clipboard,
			Logger:          log,
			RuntimeObserver: deps.runtimeObserver,
		}, client.AttachRequest{Intent: intent, SessionName: name, Remote: true})
	}

	localDialer := deps.localDialer
	if localDialer == nil {
		localDialer = defaultLocalDialer
	}
	return runLocalAttachWithRecovery(ctx, intent, name, attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(os.Stdin, os.Stderr),
		attach: func(ctx context.Context, intent uint8, name string) error {
			if log != nil {
				log.Info("attaching to local session", "intent", intent, "name", name)
			}
			return runClient(ctx, client.Dependencies{
				Dialer:          localDialer(),
				Terminal:        term.New(),
				Clock:           clock.New(),
				Logger:          log,
				RuntimeObserver: deps.runtimeObserver,
			}, client.AttachRequest{Intent: intent, SessionName: name})
		},
		killDaemon:      requestDaemonStop,
		settleAfterKill: waitForDaemonStop,
	})
}

type attachRecoveryDeps struct {
	confirmer       confirm.Confirmer
	attach          func(context.Context, uint8, string) error
	killDaemon      func(context.Context) error
	settleAfterKill func(context.Context) error
}

const (
	daemonStopTimeout          = 2 * time.Second
	daemonRestartSettle        = 50 * time.Millisecond
	legacyMalformedHelloSignal = "malformed hello"
)

func runLocalAttachWithRecovery(ctx context.Context, intent uint8, name string, deps attachRecoveryDeps) error {
	err := deps.attach(ctx, intent, name)
	if !isDaemonVersionDrift(err) {
		return err
	}

	ok, promptErr := deps.confirmer.Confirm("Your vev version differs from the running daemon; kill it and restart?")
	if promptErr != nil {
		return fmt.Errorf("vev: reading confirmation: %w", promptErr)
	}
	if !ok {
		return err
	}
	if killErr := deps.killDaemon(ctx); killErr != nil {
		return killErr
	}
	if deps.settleAfterKill != nil {
		if settleErr := deps.settleAfterKill(ctx); settleErr != nil {
			return settleErr
		}
	}
	return deps.attach(ctx, intent, name)
}

func isDaemonVersionDrift(err error) bool {
	var protocolErr *client.ProtocolError
	if !errors.As(err, &protocolErr) {
		return false
	}
	return protocolErr.Code == ports.ErrVersionMismatch || protocolErr.Text == legacyMalformedHelloSignal
}

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
	return ensureDaemon(ctx, d.dir, dial, realSpawn, defaultBackoff)
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
	transport, err := ensureDaemon(ctx, ipc.SocketDir(), realDial, realSpawn, defaultBackoff)
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

func waitForDaemonStop(ctx context.Context) error {
	timer := time.NewTimer(daemonRestartSettle)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func requestDaemonStop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, daemonStopTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		transport, err := realDial(ctx, ipc.SocketDir())
		if err != nil {
			done <- fmt.Errorf("vev: no daemon running")
			return
		}
		defer func() { _ = transport.Close() }()
		closed := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				_ = transport.Close()
			case <-closed:
			}
		}()
		defer close(closed)
		if err := transport.Send(ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{All: true})}); err != nil {
			done <- fmt.Errorf("vev: requesting daemon stop: %w", err)
			return
		}
		if _, err := transport.Recv(); err != nil && !errors.Is(err, io.EOF) {
			done <- fmt.Errorf("vev: reading daemon stop reply: %w", err)
			return
		}
		done <- nil
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("vev: stopping daemon: %w", ctx.Err())
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
	transport, err := ensureDaemon(ctx, ipc.SocketDir(), func(ctx context.Context, dir string) (ports.Transport, error) {
		return ipc.DialContext(ctx, dir, ipc.WithRuntimeObserver(observer))
	}, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	defer func() { _ = transport.Close() }()
	stdio := sshstdio.NewTransport(os.Stdin, os.Stdout, nil, sshstdio.WithRuntimeObserver(observer))
	return runStdioProxy(ctx, stdio, transport, os.Environ(), log)
}

var (
	udpBootstrapTimeout = 5 * time.Second
	udpProxyCommand     = func(ctx context.Context, exe string, args ...string) *exec.Cmd {
		return exec.CommandContext(ctx, exe, args...)
	}
)

// runUDPBootstrap starts a detached _udp-proxy, forwards its single readiness
// line, and exits so SSH stdio can close without owning the UDP proxy lifetime.
func runUDPBootstrap(ctx context.Context, session string) error {
	// Always start a fresh proxy for a bootstrap request. A client attach begins
	// with MsgHello, which must reach the daemon handshake path; reusing an
	// already-running proxy would forward that Hello into the daemon connection's
	// post-handshake runConnLoop, where it is intentionally ignored. The new
	// proxy publishes itself and retires any older registry entry once ready.
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
	if session != "" {
		args = append(args, session)
	}
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
func runUDPProxy(ctx context.Context, session string, ready io.Writer) (retErr error) {
	log, logCloser, err := configureLogging(logging.Stdio, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	log.Debug("udp proxy starting", "session", session)

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
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	daemonTr, err := ensureDaemon(ctx, ipc.SocketDir(), func(ctx context.Context, dir string) (ports.Transport, error) {
		return ipc.DialContext(ctx, dir, ipc.WithRuntimeObserver(observer))
	}, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	defer func() { _ = daemonTr.Close() }()
	udpOptions := udpProxyClientTransportOptions
	udpOptions.Observe = dgram.DiagnosticLogObserver(log)
	dg, err := dgram.NewTransportWithOptions(conn, nil, key, 2, 1, udpOptions, dgram.WithRuntimeObserver(observer))
	if err != nil {
		return err
	}
	defer func() { _ = dg.Close() }()

	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return errors.New("vev: udp proxy did not get a UDP address")
	}
	keyText := base64.StdEncoding.EncodeToString(key)
	registry := dgram.NewProxyRegistry(filepath.Join(ipc.SocketDir(), "udp-proxies"))
	record := dgram.ProxyRecord{Session: session, PID: os.Getpid(), Port: addr.Port, Key: keyText}
	if err := registry.Publish(record); err != nil {
		return err
	}
	defer func() { _ = registry.RemoveOwned(record) }()
	if _, err := fmt.Fprintf(ready, "VEV-UDP %d %s\n", addr.Port, keyText); err != nil {
		return err
	}

	return runUDPProxyRuntime(ctx, session, dg, daemonTr, os.Environ(), log)
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

// firstHelloTransport applies proxy-owned values to exactly the first Hello
// crossing a remote boundary. It intentionally leaves all later frames and
// malformed Hellos untouched so proxying preserves the client stream.
type firstHelloTransport struct {
	ports.Transport
	mu      sync.Mutex
	seen    bool
	session string
	env     []string
}

func newFirstHelloTransport(transport ports.Transport, session string, env []string) *firstHelloTransport {
	return &firstHelloTransport{
		Transport: transport,
		session:   session,
		env:       append([]string(nil), env...),
	}
}

func (t *firstHelloTransport) Recv() (ports.Frame, error) {
	f, err := t.Transport.Recv()
	if err != nil || f.Type != ports.MsgHello {
		return f, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.seen {
		return f, nil
	}
	t.seen = true

	h, err := ports.UnmarshalHello(f.Payload)
	if err != nil {
		return f, nil
	}
	h.Env = append([]string(nil), t.env...)
	if h.Name == "" {
		h.Name = t.session
	}
	f.Payload = ports.MarshalHello(h)
	return f, nil
}

// runStdioProxy applies the remote host environment before the stdio stream
// reaches the daemon. Its explicit environment input keeps this host boundary
// independently testable.
func runStdioProxy(ctx context.Context, client, daemon ports.Transport, env []string, log *slog.Logger) error {
	return proxyTransports(ctx, newFirstHelloTransport(client, "", env), daemon, log)
}

// runUDPProxyRuntime applies the remote host environment and selected session
// before the datagram stream reaches the daemon.
func runUDPProxyRuntime(ctx context.Context, session string, client, daemon ports.Transport, env []string, log *slog.Logger) error {
	return dgram.ProxyRuntime{
		Client:  newFirstHelloTransport(client, session, env),
		Daemon:  daemon,
		Log:     log,
		IdleTTL: udpProxyIdleTTL,
	}.Run(ctx)
}

func proxyTransports(ctx context.Context, a, b ports.Transport, log *slog.Logger) error {
	return dgram.ProxyRuntime{Client: a, Daemon: b, Log: log}.Run(ctx)
}

// runList prints the daemon's session listing. With no daemon running, it
// falls back to the persisted stopped-session records.
func runList(ctx context.Context) error {
	transport, err := realDial(ctx, ipc.SocketDir())
	if err != nil {
		records, loadErr := persist.LoadReadOnly(platform.StateDir())
		if loadErr != nil {
			return fmt.Errorf("vev: reading stored sessions: %w", loadErr)
		}
		infos := make([]ports.SessionInfo, 0, len(records))
		for _, r := range records {
			infos = append(infos, ports.SessionInfo{Name: r.Name, Stopped: true})
		}
		printSessions(os.Stdout, infos)
		return nil
	}
	defer func() { _ = transport.Close() }()

	if err := transport.Send(ports.Frame{Type: ports.MsgList, Payload: ports.MarshalList(ports.List{})}); err != nil {
		return fmt.Errorf("vev: requesting session list: %w", err)
	}
	reply, err := transport.Recv()
	if err != nil {
		return fmt.Errorf("vev: reading session list: %w", err)
	}
	if reply.Type == ports.MsgError {
		em, _ := ports.UnmarshalErrorMsg(reply.Payload)
		return fmt.Errorf("vev: %s", em.Text)
	}
	if reply.Type != ports.MsgSessions {
		return fmt.Errorf("vev: unexpected reply type %d to list", reply.Type)
	}
	sessions, err := ports.UnmarshalSessions(reply.Payload)
	if err != nil {
		return fmt.Errorf("vev: decoding session list: %w", err)
	}
	printSessions(os.Stdout, sessions.Sessions)
	return nil
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
		if s.Stopped {
			state = "stopped"
			tabs = "-"
		} else if s.Ephemeral {
			state = "temporary"
		}
		if s.Attached {
			attached = "yes"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", s.Name, state, tabs, attached)
	}
	_ = tw.Flush()
}

// runKill asks the daemon to terminate a named session, every session, or the daemon.
func runKill(ctx context.Context, name string, all, daemon bool) error {
	transport, err := realDial(ctx, ipc.SocketDir())
	if err != nil {
		if name != "" && !all && !daemon {
			storePath := persist.StorePath(platform.StateDir())
			if _, statErr := os.Stat(storePath); errors.Is(statErr, os.ErrNotExist) {
				return fmt.Errorf("vev: no such session: %s", name)
			} else if statErr != nil {
				return fmt.Errorf("vev: reading stored sessions: %w", statErr)
			}
			records, loadErr := persist.LoadReadOnly(platform.StateDir())
			if loadErr != nil {
				return fmt.Errorf("vev: reading stored sessions: %w", loadErr)
			}
			found := false
			for _, record := range records {
				if record.Name == name {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("vev: no such session: %s", name)
			}
			p, openErr := persist.Open(platform.StateDir())
			if openErr != nil {
				return fmt.Errorf("vev: opening stored sessions: %w", openErr)
			}
			if deleteErr := p.Delete(name); deleteErr != nil {
				_ = p.Close()
				return fmt.Errorf("vev: deleting stored session: %w", deleteErr)
			}
			if closeErr := p.Close(); closeErr != nil {
				return fmt.Errorf("vev: closing stored sessions: %w", closeErr)
			}
			snapshots := snapshotadapter.NewStore(snapshotDir())
			if deleteErr := snapshots.Delete(name); deleteErr != nil {
				return fmt.Errorf("vev: deleting session snapshot: %w", deleteErr)
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
