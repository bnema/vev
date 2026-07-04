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
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/dgram"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/domain"
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
	case "new":
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`new` requires a session name")
		}
		if len(args) > 2 {
			return command{}, usagef("`new` does not support command overrides")
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

// runDaemon runs the daemon in the foreground (the hidden --daemon path,
// entered by an auto-spawned child): it sets up logging, binds the socket,
// constructs the daemon, and serves until the last session exits or a
// termination signal arrives (graceful shutdown notifies attached clients).
func runDaemon() error {
	logFile, err := setupLogging()
	if err != nil {
		return err
	}
	defer func() { _ = logFile.Close() }()

	ln, err := ipc.Listen(ipc.SocketDir())
	if err != nil {
		return fmt.Errorf("vev: daemon listen: %w", err)
	}
	defer func() { _ = ln.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if addr := os.Getenv("VEV_PPROF_ADDR"); addr != "" {
		go func() {
			if err := http.ListenAndServe(addr, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("pprof server exited", "err", err)
			}
		}()
		slog.Info("pprof enabled", "addr", addr)
	}

	slog.Info("daemon starting", "socket", ln.Addr())
	daemonOpts := []daemon.Option(nil)
	if store, err := kv.Open(persist.StorePath(platform.StateDir())); err != nil {
		slog.Warn("opening session store failed; persistence disabled", "err", err)
	} else {
		daemonOpts = append(daemonOpts, daemon.WithStore(store))
	}
	d := daemon.New(pty.NewFactory(), clock.New(), slog.Default(), daemonOpts...)
	if err := d.Serve(ctx, ln); err != nil {
		slog.Error("daemon exited", "err", err)
		return err
	}
	slog.Info("daemon exited cleanly")
	return nil
}

// runAttach dials (auto-spawning the daemon if needed) and runs the client
// attach loop. Logging goes to the shared file: the client must never write
// to the console while the terminal is raw.
func runAttach(ctx context.Context, intent uint8, name, remoteTarget string) error {
	logFile, err := setupLogging()
	if err != nil {
		return err
	}
	defer func() { _ = logFile.Close() }()

	return runAttachWithDeps(ctx, intent, name, remoteTarget, os.Getenv("VEV"), runAttachDeps{
		localDialer:    defaultLocalDialer,
		remoteDialer:   defaultRemoteDialer,
		runClient:      client.Run,
		createDetached: createDetachedLocalSession,
	})
}

func defaultLocalDialer() ports.Dialer { return localDaemonDialer{dir: ipc.SocketDir()} }

func defaultRemoteDialer(target, session string) ports.Dialer {
	return remoteDatagramDialer{target: target, session: session}
}

type runAttachDeps struct {
	attachLocal    func(context.Context, uint8, string) error // compatibility hook for focused tests
	localDialer    func() ports.Dialer
	remoteDialer   func(target, session string) ports.Dialer
	runClient      func(context.Context, ports.Dialer, ports.Terminal, uint8, string) error
	createDetached func(context.Context, string) error
}

func runAttachWithDeps(ctx context.Context, intent uint8, name, remoteTarget, activeSession string, deps runAttachDeps) error {
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
		remoteDialer := deps.remoteDialer
		if remoteDialer == nil {
			remoteDialer = defaultRemoteDialer
		}
		return runClient(ctx, remoteDialer(remoteTarget, name), term.New(), intent, name)
	}

	attachLocal := deps.attachLocal
	if attachLocal == nil {
		localDialer := deps.localDialer
		if localDialer == nil {
			localDialer = defaultLocalDialer
		}
		attachLocal = func(ctx context.Context, intent uint8, name string) error {
			return runClient(ctx, localDialer(), term.New(), intent, name)
		}
	}
	return runLocalAttachWithRecovery(ctx, intent, name, attachRecoveryDeps{
		confirmer:       confirm.NewConfirmer(os.Stdin, os.Stderr),
		attach:          attachLocal,
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

const maxBootstrapStderr = 64 * 1024

type limitedBuffer struct {
	buf []byte
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(b.buf) < maxBootstrapStderr {
		keep := min(len(p), maxBootstrapStderr-len(b.buf))
		b.buf = append(b.buf, p[:keep]...)
	}
	return len(p), nil
}

func (b *limitedBuffer) String() string { return string(b.buf) }

type remoteDatagramDialer struct {
	target  string
	session string
}

func (d remoteDatagramDialer) Dial(ctx context.Context) (ports.Transport, error) {
	tr, udpErr := d.dialDatagram(ctx)
	if udpErr == nil {
		return tr, nil
	}
	tr, stdioErr := sshstdio.Dial(d.target, d.session)
	if stdioErr != nil {
		return nil, fmt.Errorf("datagram dial failed: %w; stdio fallback failed: %w", udpErr, stdioErr)
	}
	return tr, nil
}

func (d remoteDatagramDialer) dialDatagram(ctx context.Context) (ports.Transport, error) {
	remote := []string{shellQuote("vev"), shellQuote("_udp-bootstrap")}
	if d.session != "" {
		remote = append(remote, shellQuote(d.session))
	}
	cmd := exec.CommandContext(ctx, "ssh", "--", d.target, strings.Join(remote, " "))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr limitedBuffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	started := true
	defer func() {
		if started {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("udp bootstrap: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	fields := strings.Fields(strings.TrimSpace(line))
	if len(fields) != 3 || fields[0] != "VEV-UDP" {
		return nil, fmt.Errorf("udp bootstrap: unexpected reply %q", strings.TrimSpace(line))
	}
	key, err := base64.StdEncoding.DecodeString(fields[2])
	if err != nil {
		return nil, err
	}
	host := sshTargetHost(d.target)
	peer, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, fields[1]))
	if err != nil {
		return nil, err
	}
	var lc net.ListenConfig
	pc, err := lc.ListenPacket(ctx, "udp", ":0")
	if err != nil {
		return nil, err
	}
	tr, err := dgram.NewTransport(pc, peer, key, 1, 2)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	started = false
	return closeBothTransport{Transport: tr, close: func() error {
		_ = tr.Close()
		_ = cmd.Process.Kill()
		return cmd.Wait()
	}}, nil
}

func sshTargetHost(target string) string {
	if at := strings.LastIndexByte(target, '@'); at >= 0 {
		target = target[at+1:]
	}
	if h, _, err := net.SplitHostPort(target); err == nil {
		return h
	}
	return target
}

type closeBothTransport struct {
	ports.Transport
	close func() error
}

func (t closeBothTransport) Close() error { return t.close() }

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
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
	hello := ports.Hello{
		Version: ports.ProtocolVersion,
		Intent:  ports.IntentNew,
		Name:    name,
		Size:    domain.Size{Cols: 80, Rows: 24},
		TermEnv: os.Getenv("TERM"),
		Cwd:     cwd,
	}
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
	transport, err := ensureDaemon(ctx, ipc.SocketDir(), realDial, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	defer func() { _ = transport.Close() }()

	stdio := sshstdio.NewTransport(os.Stdin, os.Stdout, nil)
	return proxyTransports(ctx, stdio, transport)
}

// runUDPBootstrap is the hidden remote-side bootstrap used before switching to
// authenticated datagrams. It deliberately prints only an ephemeral UDP port and
// random session key to stdout so callers can fall back to _stdio if setup fails.
func runUDPBootstrap(ctx context.Context, session string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	var lc net.ListenConfig
	conn, err := lc.ListenPacket(ctx, "udp", "0.0.0.0:0")
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
		return errors.New("vev: udp bootstrap did not get a UDP address")
	}
	if _, err := fmt.Fprintf(os.Stdout, "VEV-UDP %d %s\n", addr.Port, base64.StdEncoding.EncodeToString(key)); err != nil {
		return err
	}
	_ = os.Stdout.Sync()

	var dgramTr ports.Transport = dg
	if session != "" {
		dgramTr = preHelloNameTransport{Transport: dg, session: session}
	}
	return proxyTransports(ctx, dgramTr, daemonTr)
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

func proxyTransports(ctx context.Context, a, b ports.Transport) error {
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
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, io.EOF) {
			return nil
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
