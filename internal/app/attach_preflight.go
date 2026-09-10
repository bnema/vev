package app

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/confirm"
	"github.com/bnema/vev/pkg/rawterm"
)

// preflightListTimeout bounds the attach pre-flight session listing so a
// socket that accepts but never replies cannot delay the attach fallback
// or block termination.
var preflightListTimeout = 5 * time.Second

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
// confirmation.
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
		return confirmSessionCreate(ctx, name, deps.attachPrompt)
	}
	return protocol.IntentAttach, nil
}

// confirmSessionCreate offers creation of an absent session and maps the
// answer to the attach intent: IntentNew on confirmation, IntentAttach on
// decline. A cancelled wait surfaces the cancellation error.
func confirmSessionCreate(ctx context.Context, name string, prompt attachPrompt) (uint8, error) {
	create, err := offerMissingSessionCreate(ctx, name, prompt)
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

// offerMissingSessionCreate prompts to create a locally missing session.
// It reports whether the caller should attach with IntentNew instead.
// Non-interactive consoles, a cancelled wait, a declined answer, or
// unreadable input keep the attach intent unchanged. Only y/yes answers
// confirm; empty, no, and unknown answers decline.
func offerMissingSessionCreate(ctx context.Context, name string, prompt attachPrompt) (bool, error) {
	if name == "" || ctx.Err() != nil || !prompt.interactive() {
		return false, nil
	}
	create, err := confirmWithContext(ctx, prompt.input(), prompt.output(), fmt.Sprintf("vev: session %q doesn't exist, want to create and attach to it?", name))
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return create, nil
}

// confirmWithContext asks one confirmation question while also observing
// ctx. A cancelled context releases the caller with the cancellation error
// instead of blocking on input. The reader goroutine may stay blocked, so
// callers must only use this where the process exits (or detaches from the
// console) afterward rather than continuing to interact on the same stream.
func confirmWithContext(ctx context.Context, input io.Reader, output io.Writer, question string) (bool, error) {
	if ctx == nil {
		return confirm.NewConfirmer(input, output).Confirm(question)
	}
	type outcome struct {
		create bool
		err    error
	}
	result := make(chan outcome, 1)
	go func() {
		create, err := confirm.NewConfirmer(input, output).Confirm(question)
		result <- outcome{create: create, err: err}
	}()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case reply := <-result:
		return reply.create, reply.err
	}
}
