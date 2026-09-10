package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/confirm"
	"github.com/bnema/vev/pkg/rawterm"
)

const maxAttachTargetHandoffs = 32

// resolveMissingSessionAttach decides the attach intent for a direct local
// attach before any client attempt runs. When the session exists, or when
// existence cannot be determined, it keeps IntentAttach and the daemon
// rejection (if any) surfaces unchanged. When the session is absent it
// offers creation on an interactive console and returns IntentNew on
// confirmation; decline keeps IntentAttach, and a cancelled wait surfaces
// the cancellation error.
//
// The check runs before the client owns any console reader, so the create
// prompt is always the sole stdin consumer: no failed attempt can leave a
// blocked input-pump read behind to steal the answer. Provenance comes from
// the listing itself, so no error-text matching is needed and in-client
// navigation handoffs can never trigger creation of the requested session.
func resolveMissingSessionAttach(ctx context.Context, name string, deps runAttachDeps, log *slog.Logger) (uint8, error) {
	switch exists, err := sessionExists(ctx, name, deps); {
	case err != nil:
		if log != nil {
			log.Debug("missing-session preflight unavailable; proceeding with attach", "err", err)
		}
	case !exists:
		return confirmMissingSessionCreate(ctx, name, deps)
	}
	return protocol.IntentAttach, nil
}

// isInteractiveTerminal reports whether the prompt input runs on a
// terminal. Only *os.File inputs are probed; anything else (pipes, test
// stubs serving canned answers) counts as interactive so tests exercise
// the prompt path. In production the terminal always wraps os.Stdin, so
// the probe is meaningful there.
func isInteractiveTerminal(terminal ports.Terminal) bool {
	if terminal == nil {
		return false
	}
	if file, ok := terminal.In().(*os.File); ok {
		return rawterm.IsTerminal(int(file.Fd()))
	}
	return true
}

// confirmMissingSessionCreate prompts to create an absent session and maps
// the answer to the attach intent: IntentNew on confirmation, IntentAttach
// on decline. A cancelled wait surfaces the cancellation error.
// Non-interactive consoles, a declined answer, or unreadable input keep
// IntentAttach. Only y/yes answers confirm; empty, no, and unknown answers
// decline.
func confirmMissingSessionCreate(ctx context.Context, name string, deps runAttachDeps) (uint8, error) {
	if name == "" || ctx.Err() != nil {
		return protocol.IntentAttach, nil
	}
	terminal := clientTerminal(deps)
	if !isInteractiveTerminal(terminal) {
		return protocol.IntentAttach, nil
	}
	in, out := terminal.In(), terminal.Out()
	if in == nil || out == nil {
		return protocol.IntentAttach, nil
	}
	create, err := confirm.NewConfirmer(in, out).ConfirmContext(ctx, fmt.Sprintf("vev: session %q doesn't exist, want to create and attach to it?", name))
	if err != nil {
		return protocol.IntentAttach, err
	}
	if !create {
		return protocol.IntentAttach, nil
	}
	if err := ctx.Err(); err != nil {
		return protocol.IntentAttach, err
	}
	return protocol.IntentNew, nil
}

// sessionExists reports whether a directly attached local session exists.
// An error means existence could not be determined.
func sessionExists(ctx context.Context, name string, deps runAttachDeps) (bool, error) {
	if name == "" {
		return false, nil
	}
	dial := deps.localDialer
	if dial == nil {
		dial = defaultLocalDialer
	}
	sessions, err := listSessionsWithDialer(ctx, dial().Dial)
	if err != nil {
		return false, err
	}
	for _, session := range sessions {
		if session.Name == name {
			return true, nil
		}
	}
	return false, nil
}
