package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
)

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

// runList reads existing state without starting either background process.
func runList(ctx context.Context, cmd command) error {
	service, err := connectListBroker(ctx)
	if errors.Is(err, errBrokerAbsent) {
		return runListWithoutBroker(ctx, cmd, os.Stdout)
	}
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
		var brokerErr ports.BrokerError
		if errors.As(err, &brokerErr) && brokerErr.Code == ports.BrokerErrorNoDaemon {
			printSessions(os.Stdout, nil)
			return nil
		}
		return err
	}
	printSessions(os.Stdout, sessions)
	return nil
}

// exchangeLocalDaemon sends one request directly to an existing local daemon
// and returns its reply. It is the broker-less path of read and stop
// commands: the broker being absent is the normal idle state, and these
// commands must stay true without starting it. It never spawns a daemon.
func exchangeLocalDaemon(ctx context.Context, request protocol.ClientMessage) (protocol.ServerMessage, error) {
	transport, err := dialListDaemon(ctx, ipc.SocketDir())
	if err != nil {
		if backendAbsent(err) {
			return nil, errNoLocalDaemon
		}
		return nil, err
	}
	connection := sessionwire.NewClientConnection(transport)
	defer func() { _ = connection.Close() }()
	if err := connection.SendClient(request); err != nil {
		return nil, err
	}
	return connection.ReceiveServer()
}

func runListWithoutBroker(ctx context.Context, cmd command, out io.Writer) error {
	if cmd.listHost != "" {
		_, err := fmt.Fprintf(out, "%s: not connected (no broker running)\n", cmd.listHost)
		return err
	}
	reply, err := exchangeLocalDaemon(ctx, protocol.List{})
	switch {
	case errors.Is(err, errNoLocalDaemon):
		printSessions(out, nil)
	case err != nil:
		return fmt.Errorf("list local sessions: %w", err)
	default:
		sessions, ok := reply.(protocol.Sessions)
		if !ok {
			return fmt.Errorf("unexpected local list reply %T", reply)
		}
		printSessions(out, sessions.Sessions)
	}
	if cmd.listAll {
		return listDisconnectedHosts(out)
	}
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

// runKill deletes one session or, with sessions set, every session through the
// broker, starting it if needed.
func runKill(ctx context.Context, name string, sessions bool) error {
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
	if sessions {
		result, err = operations.KillAll(ctx, route)
	} else {
		result, err = operations.Kill(ctx, route, name)
	}
	if err != nil {
		return err
	}
	if err := brokerKillResultError(result); err != nil {
		return err
	}
	if sessions {
		fmt.Println("killed all sessions")
	} else {
		fmt.Printf("killed %s\n", name)
	}
	return nil
}
