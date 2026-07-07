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
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/bnema/vev/internal/adapters/clipboard"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/config"
	"github.com/bnema/vev/internal/adapters/dgram"
	"github.com/bnema/vev/internal/adapters/ipc"
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
  vev attach <name>   attach to an existing named session (alias: a)
  vev attach user@host[:session]
                      attach through SSH to a remote vev daemon
  vev ls              list sessions
  vev kill <name>     kill a named session
  vev kill --all      kill all sessions and stop the daemon
  vev kill --daemon   stop the active vev daemon
  vev --help          show this help
  vev --version       show version`

// version is vev's reported build version. Wired here for the MVP; a build
// stamp can replace it later.
const version = "0.1.0-dev"

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
		fmt.Println("vev", version)
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

// runDaemon runs the daemon in the foreground (the hidden --daemon path,
// entered by an auto-spawned child): it sets up logging, binds the socket,
// constructs the daemon, and serves until the last session exits or a
// termination signal arrives (graceful shutdown notifies attached clients).
func runDaemon() error {
	log, logCloser, err := configureLogging(logging.Daemon, true)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()

	ln, err := ipc.Listen(ipc.SocketDir())
	if err != nil {
		log.Error("daemon listen failed", "socket_dir", ipc.SocketDir(), "err", err)
		return fmt.Errorf("vev: daemon listen: %w", err)
	}
	defer func() { _ = ln.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if addr := os.Getenv("VEV_PPROF_ADDR"); addr != "" {
		go func() {
			if err := http.ListenAndServe(addr, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("pprof server exited", "err", err)
			}
		}()
		log.Info("pprof enabled", "addr", addr)
	}

	log.Info("daemon starting", "socket", ln.Addr())
	daemonOpts := []daemon.Option(nil)
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
	if store, err := kv.Open(storePath); err != nil {
		log.Warn("opening session store failed; persistence disabled", "path", storePath, "err", err)
	} else {
		log.Info("session persistence enabled", "path", storePath)
		daemonOpts = append(daemonOpts, daemon.WithStore(store))
	}
	clk := clock.New()
	d := daemon.New(pty.NewFactory(), clk, log, daemonOpts...)
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
func runAttach(ctx context.Context, intent uint8, name, remoteTarget string) error {
	log, logCloser, err := configureLogging(logging.Client, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()

	return runAttachWithDeps(ctx, intent, name, remoteTarget, os.Getenv("VEV"), log, runAttachDeps{
		localDialer:             defaultLocalDialer,
		remoteDialerFactory:     defaultRemoteDialerFactory(),
		selectedRemoteTransport: os.Getenv(envRemoteTransport),
		runClient:               client.Run,
		createDetached:          createDetachedLocalSession,
		clipboard:               clipboard.New(),
	})
}

const envRemoteTransport = "VEV_REMOTE_TRANSPORT"

func defaultLocalDialer() ports.Dialer { return localDaemonDialer{dir: ipc.SocketDir()} }

func defaultRemoteDialerFactory() ports.RemoteDialerFactory {
	return remoteadapter.NewDialerFactory()
}

type runAttachDeps struct {
	attachLocal             func(context.Context, uint8, string, *slog.Logger) error // compatibility hook for focused tests
	localDialer             func() ports.Dialer
	remoteDialerFactory     ports.RemoteDialerFactory
	selectedRemoteTransport string
	runClient               func(context.Context, ports.Dialer, ports.Terminal, ports.Clock, uint8, string, bool, ports.ClipboardReader, *slog.Logger) error
	createDetached          func(context.Context, string) error
	// clipboard reads a clipboard image on a remote attach's Ctrl+V (see
	// docs/superpowers/specs/2026-07-04-clipboard-image-transfer-design.md).
	// Only used for the remote-dialer branch below; local attaches never
	// intercept Ctrl+V regardless of this field.
	clipboard ports.ClipboardReader
}

func remoteTransportModeFromEnv(value string) (ports.RemoteTransportMode, error) {
	switch value {
	case "", string(ports.RemoteTransportStdio):
		return ports.RemoteTransportStdio, nil
	case string(ports.RemoteTransportUDP):
		return ports.RemoteTransportUDP, nil
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
		runClient = client.Run
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
		return runClient(ctx, dialer, term.New(), clock.New(), intent, name, true, deps.clipboard, log)
	}

	attachLocal := deps.attachLocal
	if attachLocal == nil {
		localDialer := deps.localDialer
		if localDialer == nil {
			localDialer = defaultLocalDialer
		}
		attachLocal = func(ctx context.Context, intent uint8, name string, log *slog.Logger) error {
			if log != nil {
				log.Info("attaching to local session", "intent", intent, "name", name)
			}
			return runClient(ctx, localDialer(), term.New(), clock.New(), intent, name, false, nil, log)
		}
	}
	return runLocalAttachWithRecovery(ctx, intent, name, attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(os.Stdin, os.Stderr),
		attach: func(ctx context.Context, intent uint8, name string) error {
			return attachLocal(ctx, intent, name, log)
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

type localDaemonDialer struct{ dir string }

func (d localDaemonDialer) Dial(ctx context.Context) (ports.Transport, error) {
	return ensureDaemon(ctx, d.dir, realDial, realSpawn, defaultBackoff)
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
		transport, err := realDial(ipc.SocketDir())
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
func runStdio(ctx context.Context) error {
	log, logCloser, err := configureLogging(logging.Stdio, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	log.Debug("stdio proxy starting")

	transport, err := ensureDaemon(ctx, ipc.SocketDir(), realDial, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	defer func() { _ = transport.Close() }()

	stdio := sshstdio.NewTransport(os.Stdin, os.Stdout, nil)
	return proxyTransports(ctx, stdio, transport, log)
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
func runUDPProxy(ctx context.Context, session string, ready io.Writer) error {
	log, logCloser, err := configureLogging(logging.Stdio, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	log.Debug("udp proxy starting", "session", session)

	var lc net.ListenConfig
	conn, err := lc.ListenPacket(ctx, "udp", ":0")
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	daemonTr, err := ensureDaemon(ctx, ipc.SocketDir(), realDial, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	defer func() { _ = daemonTr.Close() }()
	dg, err := dgram.NewTransport(conn, nil, key, 2, 1)
	if err != nil {
		return err
	}
	defer func() { _ = dg.Close() }()

	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return errors.New("vev: udp proxy did not get a UDP address")
	}
	if _, err := fmt.Fprintf(ready, "VEV-UDP %d %s\n", addr.Port, base64.StdEncoding.EncodeToString(key)); err != nil {
		return err
	}

	var dgramTr ports.Transport = dg
	if session != "" {
		dgramTr = preHelloNameTransport{Transport: dg, session: session}
	}
	return proxyTransports(ctx, dgramTr, daemonTr, log)
}

type preHelloNameTransport struct {
	ports.Transport
	session string
}

func (t preHelloNameTransport) Recv() (ports.Frame, error) {
	f, err := t.Transport.Recv()
	if err != nil || f.Type != ports.MsgHello || t.session == "" {
		return f, err
	}
	h, err := ports.UnmarshalHello(f.Payload)
	if err != nil {
		return f, nil
	}
	if h.Name == "" {
		h.Name = t.session
		f.Payload = ports.MarshalHello(h)
	}
	return f, nil
}

func proxyTransports(ctx context.Context, a, b ports.Transport, log *slog.Logger) error {
	errCh := make(chan error, 2)
	copyFrames := func(dst, src ports.Transport) {
		for {
			f, err := src.Recv()
			if err != nil {
				errCh <- err
				return
			}
			if err := dst.Send(f); err != nil {
				errCh <- err
				return
			}
		}
	}
	go copyFrames(b, a)
	go copyFrames(a, b)

	select {
	case <-ctx.Done():
		if log != nil {
			log.Info("stdio proxy stopped by context", "err", ctx.Err())
		}
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, io.EOF) {
			if log != nil {
				log.Info("stdio proxy reached clean EOF")
			}
			return nil
		}
		if log != nil {
			log.Error("stdio proxy copy failed", "err", err)
		}
		return err
	}
}

// runList prints the daemon's session listing. With no daemon running, it
// falls back to the persisted stopped-session records.
func runList(_ context.Context) error {
	transport, err := realDial(ipc.SocketDir())
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
		} else if s.Attached {
			attached = "yes"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", s.Name, state, tabs, attached)
	}
	_ = tw.Flush()
}

// runKill asks the daemon to terminate a named session, every session, or the daemon.
func runKill(_ context.Context, name string, all, daemon bool) error {
	transport, err := realDial(ipc.SocketDir())
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
