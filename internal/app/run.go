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
	"os"
	"text/tabwriter"

	"github.com/bnema/vev/internal/adapters/ipc"
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
	kindHelp
	kindVersion
)

// command is the parsed CLI invocation: what to do, plus the attach intent
// and session name where relevant.
type command struct {
	kind   cmdKind
	intent uint8
	name   string
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
  vev ls              list sessions
  vev kill <name>     kill a named session
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
	case "new":
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`new` requires a session name")
		}
		return command{kind: kindAttach, intent: ports.IntentNew, name: args[1]}, nil
	case "attach", "a":
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`attach` requires a session name")
		}
		return command{kind: kindAttach, intent: ports.IntentAttach, name: args[1]}, nil
	case "ls", "list":
		return command{kind: kindList}, nil
	case "kill":
		if len(args) < 2 || args[1] == "" {
			return command{}, usagef("`kill` requires a session name")
		}
		return command{kind: kindKill, name: args[1]}, nil
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
	case kindList:
		return runList(ctx)
	case kindKill:
		return runKill(ctx, cmd.name)
	case kindAttach:
		return runAttach(ctx, cmd.intent, cmd.name)
	default:
		return usagef("unhandled command")
	}
}

// runDaemon runs the daemon in the foreground (the hidden --daemon path,
// entered by an auto-spawned child). The daemon core lands in Task 11; here
// we set up logging, bind the socket, and hand off to daemon.Serve.
func runDaemon() error {
	logFile, err := setupLogging()
	if err != nil {
		return err
	}
	defer logFile.Close()

	ln, err := ipc.Listen(ipc.SocketDir())
	if err != nil {
		return fmt.Errorf("vev: daemon listen: %w", err)
	}
	defer ln.Close()

	slog.Info("daemon starting", "socket", ln.Addr())
	err = daemon.Serve(ln)
	slog.Error("daemon exited", "err", err)
	return err
}

// runAttach dials (auto-spawning the daemon if needed) and runs the client
// attach loop. Logging goes to the shared file: the client must never write
// to the console while the terminal is raw.
func runAttach(ctx context.Context, intent uint8, name string) error {
	logFile, err := setupLogging()
	if err != nil {
		return err
	}
	defer logFile.Close()

	transport, err := ensureDaemon(ctx, ipc.SocketDir(), realDial, realSpawn, defaultBackoff)
	if err != nil {
		return err
	}
	// client.Attach owns and closes transport.
	return client.Attach(ctx, transport, term.New(), intent, name)
}

// runList prints the daemon's session listing. With no daemon running there
// are, by definition, no sessions.
func runList(_ context.Context) error {
	transport, err := realDial(ipc.SocketDir())
	if err != nil {
		fmt.Println("no sessions")
		return nil
	}
	defer transport.Close()

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
		fmt.Fprintln(w, "no sessions")
		return
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tWINDOWS\tATTACHED")
	for _, s := range sessions {
		attached := "no"
		if s.Attached {
			attached = "yes"
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\n", s.Name, s.Windows, attached)
	}
	_ = tw.Flush()
}

// runKill asks the daemon to terminate a named session.
func runKill(_ context.Context, name string) error {
	transport, err := realDial(ipc.SocketDir())
	if err != nil {
		return fmt.Errorf("vev: no daemon running")
	}
	defer transport.Close()

	if err := transport.Send(ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{Name: name})}); err != nil {
		return fmt.Errorf("vev: requesting kill: %w", err)
	}
	reply, err := transport.Recv()
	if err != nil {
		// The daemon may close the connection after killing; treat a clean
		// EOF as success.
		if errors.Is(err, io.EOF) {
			fmt.Printf("killed %s\n", name)
			return nil
		}
		return fmt.Errorf("vev: reading kill reply: %w", err)
	}
	if reply.Type == ports.MsgError {
		em, _ := ports.UnmarshalErrorMsg(reply.Payload)
		return fmt.Errorf("vev: %s", em.Text)
	}
	fmt.Printf("killed %s\n", name)
	return nil
}
