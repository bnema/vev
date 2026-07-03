// Package app wires vev's runtime together via dependency injection. It is
// the single place where usecases (client, daemon) are connected to concrete
// adapters (ipc transport/listener, terminal); usecases themselves never
// import adapters.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/bnema/vev/internal/usecase/daemon"
)

// cmdKind identifies which sub-command the CLI parsed.
type cmdKind int

const (
	kindAttach cmdKind = iota // ephemeral/new/attach — distinguished by intent
	kindList
	kindKill
	kindDaemon
	kindStdio
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
	d := daemon.New(pty.NewFactory(), clock.New(), slog.Default())
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

	var transport ports.Transport
	if remoteTarget != "" {
		transport, err = sshstdio.Dial(remoteTarget, name)
	} else {
		transport, err = ensureDaemon(ctx, ipc.SocketDir(), realDial, realSpawn, defaultBackoff)
	}
	if err != nil {
		return err
	}
	// client.Attach owns and closes transport.
	return client.Attach(ctx, transport, term.New(), intent, name)
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

// runList prints the daemon's session listing. With no daemon running there
// are, by definition, no sessions.
func runList(_ context.Context) error {
	transport, err := realDial(ipc.SocketDir())
	if err != nil {
		fmt.Println("no sessions")
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
	_, _ = fmt.Fprintln(tw, "NAME\tTABS\tATTACHED")
	for _, s := range sessions {
		attached := "no"
		if s.Attached {
			attached = "yes"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%s\n", s.Name, s.Tabs, attached)
	}
	_ = tw.Flush()
}

// runKill asks the daemon to terminate a named session, every session, or the daemon.
func runKill(_ context.Context, name string, all, daemon bool) error {
	transport, err := realDial(ipc.SocketDir())
	if err != nil {
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
