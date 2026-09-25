package app

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/bnema/vev/internal/usecase/confirm"
	"github.com/bnema/vev/pkg/rawterm"
)

// terminalIsInteractive reports whether the prompt can read its answer from a
// console. The client terminal's input has to be a real terminal file: pipes,
// redirected files, and detached readers are not consoles, so the prompt
// never blocks on a stream nobody reads from the keyboard.
func terminalIsInteractive(terminal ports.Terminal) bool {
	if terminal == nil {
		return false
	}
	file, ok := terminal.In().(*os.File)
	return ok && rawterm.IsTerminal(int(file.Fd()))
}

// attachInteractiveConsole is the composition-root seam for the console
// decision. Production probes the real terminal file; tests inject the
// decision so the prompt matrix runs without owning a real TTY.
var attachInteractiveConsole = terminalIsInteractive

// flushingWriter pushes every prompt write to the console before the
// confirmer blocks on the answer. Terminal output is buffered, so without
// this flush the user waits in front of a blank console until the client
// repaints.
type flushingWriter struct {
	out   io.Writer
	flush func() error
}

func (w flushingWriter) Write(data []byte) (int, error) {
	written, err := w.out.Write(data)
	if err != nil {
		return written, err
	}
	if err := w.flush(); err != nil {
		return written, err
	}
	return written, nil
}

// resolveAttachCreationIntent decides the attach intent for one direct
// `attach <name>` target (local when remoteTarget is empty, otherwise a
// configured remote host) before the client owns any console reader or raw
// mode. Existence is asked of the destination daemon itself through a list
// that may start it, never read from the broker catalogue: right after
// `kill --all` the catalogue is empty while the daemon still restores the
// session from disk.
//
// When the session exists, the host is not configured, or the daemon cannot
// answer, it keeps IntentAttach; the named attach then lets the daemon decide
// and report its own refusal. Only a daemon listing without the name offers
// creation on an interactive console, returning IntentNew on confirmation; a
// declined answer or a non-interactive console keeps IntentAttach.
//
// The check runs before Supervisor.Run enters raw mode, so the create prompt
// is always the sole console reader: no attachment worker can leave a
// blocked input-pump read behind to steal the answer.
//
// When the peek connection succeeds, it is returned so the caller can hand it
// to a preconnectedBrokerConnector instead of dialing the broker a second
// time: an ordinary attach, confirmed or declined, still opens exactly one
// broker connection.
func resolveAttachCreationIntent(ctx context.Context, intent uint8, name, remoteTarget string, terminal ports.Terminal) (uint8, ports.BrokerService, error) {
	if intent != protocol.IntentAttach || name == "" {
		return intent, nil, nil
	}
	service, err := connectProductionClientBroker(ctx)
	if err != nil {
		// Existence could not be determined: keep the plain attach path and let
		// the resolver's own refusal (or the ordinary connector's own retry
		// cadence) decide, exactly as if this preflight had never run.
		return protocol.IntentAttach, nil, nil
	}
	if !daemonReportsSessionMissing(ctx, service, remoteTarget, name) {
		return protocol.IntentAttach, service, nil
	}
	resolved, err := confirmMissingSessionCreate(ctx, name, terminal)
	if err != nil {
		_ = service.Close()
		return protocol.IntentAttach, nil, err
	}
	return resolved, service, nil
}

// attachSessionLister is the daemon listing seam; production starts the
// destination daemon if needed so restored sessions are included.
var attachSessionLister = func(ctx context.Context, service ports.BrokerService, route client.BrokerOperationRoute) ([]protocol.SessionInfo, error) {
	operations, err := client.NewBrokerOperations(service, clock.New())
	if err != nil {
		return nil, err
	}
	return operations.ListStarting(ctx, route)
}

// daemonReportsSessionMissing reports true only when the destination daemon
// answered a listing that does not carry name. An unconfigured host or a
// failed listing proves nothing, so it reports false.
func daemonReportsSessionMissing(ctx context.Context, service ports.BrokerService, remoteTarget, name string) bool {
	snapshot := service.Snapshot()
	route := client.BrokerOperationRoute{Epoch: snapshot.Epoch, Destination: ports.BrokerEndpointFence{Local: true}}
	if remoteTarget != "" {
		observation, ok := snapshot.Find(remoteTarget)
		if !ok {
			return false
		}
		route.Destination = ports.BrokerEndpointFence{Registration: observation.Registration}
	}
	sessions, err := attachSessionLister(ctx, service, route)
	if err != nil {
		return false
	}
	for _, session := range sessions {
		if session.Name == name {
			return false
		}
	}
	return true
}

// confirmMissingSessionCreate prompts to create an absent session and maps
// the answer to the attach intent: IntentNew on confirmation, IntentAttach on
// decline. A cancelled wait surfaces the cancellation error. Non-interactive
// consoles, a declined answer, or unreadable input keep IntentAttach. Only
// y/yes answers confirm; empty, no, and unknown answers decline.
func confirmMissingSessionCreate(ctx context.Context, name string, terminal ports.Terminal) (uint8, error) {
	if err := ctx.Err(); err != nil {
		return protocol.IntentAttach, err
	}
	if name == "" {
		return protocol.IntentAttach, nil
	}
	if !attachInteractiveConsole(terminal) {
		return protocol.IntentAttach, nil
	}
	in, out := terminal.In(), terminal.Out()
	if in == nil || out == nil {
		return protocol.IntentAttach, nil
	}
	prompt := confirm.NewConfirmer(in, flushingWriter{out: out, flush: terminal.Flush})
	create, err := prompt.ConfirmContext(ctx, fmt.Sprintf("vev: session %q doesn't exist, want to create and attach to it?", name))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return protocol.IntentAttach, ctxErr
		}
		// A console that cannot show the question must not consume an answer:
		// keep the plain attach path and let the resolver decide.
		return protocol.IntentAttach, nil
	}
	if !create {
		return protocol.IntentAttach, nil
	}
	if err := ctx.Err(); err != nil {
		return protocol.IntentAttach, err
	}
	return protocol.IntentNew, nil
}

// preconnectedBrokerConnector hands one already-connected broker service to
// the caller's first Connect call, then falls back to the wrapped connector
// for every later attempt (the supervisor's own reconnect cadence). It exists
// only to avoid wasting the connection the attach-creation preflight already
// established: the supervisor still owns and closes whatever it adopts.
type preconnectedBrokerConnector struct {
	first    ports.BrokerService
	fallback ports.BrokerConnector

	mu   sync.Mutex
	used bool
}

func (c *preconnectedBrokerConnector) Connect(ctx context.Context) (ports.BrokerService, error) {
	c.mu.Lock()
	if !c.used {
		c.used = true
		first := c.first
		c.mu.Unlock()
		if first != nil {
			return first, nil
		}
	} else {
		c.mu.Unlock()
	}
	return c.fallback.Connect(ctx)
}

var _ ports.BrokerConnector = (*preconnectedBrokerConnector)(nil)

// withPreconnected wraps connector to serve preconnected as its first
// connection, or returns connector unchanged when preconnected is nil.
func withPreconnected(connector ports.BrokerConnector, preconnected ports.BrokerService) ports.BrokerConnector {
	if preconnected == nil {
		return connector
	}
	return &preconnectedBrokerConnector{first: preconnected, fallback: connector}
}
