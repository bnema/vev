package app

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/confirm"
	"github.com/bnema/vev/pkg/rawterm"
)

const maxAttachTargetHandoffs = 32

// attachPrompt carries the console streams for the missing-session create
// prompt. The zero value uses the process console and probes stdin for
// interactivity; tests inject buffers and a stub.
type attachPrompt struct {
	in       io.Reader
	out      io.Writer
	terminal func() bool
}

func (p attachPrompt) interactive() bool {
	if p.terminal != nil {
		return p.terminal()
	}
	return rawterm.IsTerminal(int(os.Stdin.Fd()))
}

func (p attachPrompt) input() io.Reader {
	if p.in != nil {
		return p.in
	}
	return os.Stdin
}

func (p attachPrompt) output() io.Writer {
	if p.out != nil {
		return p.out
	}
	return os.Stderr
}

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
		return confirmMissingSessionCreate(ctx, name, deps.attachPrompt)
	}
	return protocol.IntentAttach, nil
}

// confirmMissingSessionCreate prompts to create an absent session and maps
// the answer to the attach intent: IntentNew on confirmation, IntentAttach
// on decline. A cancelled wait surfaces the cancellation error.
// Non-interactive consoles, a declined answer, or unreadable input keep
// IntentAttach. Only y/yes answers confirm; empty, no, and unknown answers
// decline.
func confirmMissingSessionCreate(ctx context.Context, name string, prompt attachPrompt) (uint8, error) {
	if name == "" || ctx.Err() != nil || !prompt.interactive() {
		return protocol.IntentAttach, nil
	}
	create, err := confirm.NewConfirmer(prompt.input(), prompt.output()).ConfirmContext(ctx, fmt.Sprintf("vev: session %q doesn't exist, want to create and attach to it?", name))
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
