// SSH stdio daemonmux carriage.
//
// This file exposes the mux-specific SSH stdio carriage the daemonmux
// multiplexer (internal/adapters/daemonmux) runs over. It is deliberately
// separate from the ordinary Dial/DialContext pair: DialMuxContext starts an
// explicit, caller-supplied command specification and never builds or reuses
// the `vev _stdio` session command, so a mux carriage can never become a
// second navigation path to a session. Session selection stays owned by the
// typed Hello handshake over the ordinary carriage; the two endpoints cannot be
// confused because the mux dial takes the whole command from its caller.
//
// The carriage reuses the existing SSH stdio machinery unchanged: an ssh (or
// other) child process started with exec.Command argv - never through a shell -
// whose stdin/stdout are wrapped by the shared streamframe framing and the
// bounded RecvBounded capability the daemonmux FramedCarrierBridge consumes.
// One child process is one physical daemonmux connection; logical attachments
// are multiplexed inside its stdio stream, never mapped to further processes.
//
// Non-interactive by construction: stdin is a pipe the transport owns, no PTY
// is ever allocated locally, and BuildCommandForMux adds only ssh's `-T` (no
// remote TTY). Host-key and authentication trust stay entirely owned by the
// target's effective OpenSSH configuration: this file never injects
// StrictHostKeyChecking, UserKnownHostsFile, BatchMode, or ProxyCommand, so a
// mux carriage cannot weaken the trust policy the user configured.
//
// The caller's setup context (or its deadline) bounds subprocess start and is
// shared with the daemonmux physical handshake the caller runs afterwards over
// the returned carriage. A successful carriage is deliberately detached from
// that context: Transport.Close - not the setup context - owns the subprocess
// and its pipes, so a handshake deadline cannot kill a healthy carriage.
//
// Diagnostics are bounded and sanitized. The child's stderr is captured up to a
// hard byte ceiling; a non-clean exit logs the captured text with terminal
// escapes and control bytes stripped, and the public error carries only a typed
// sentinel plus the process outcome, never raw remote stderr.

package sshstdio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/protocol/wire"
)

const (
	// muxStderrLimit bounds how much of the mux child's stderr is captured.
	// The capture is a diagnostic aid, so an unbounded or hostile stream can
	// never grow the buffer without limit.
	muxStderrLimit = 4 << 10
	// MuxExitNoDaemon is reserved for the hidden mux helper when its local
	// daemon carriage is absent. OpenSSH uses 255 for transport/auth failures.
	MuxExitNoDaemon = 73
	// muxDiagnosticLimit bounds the sanitized diagnostic echoed to a logger.
	muxDiagnosticLimit = 512
)

// Mux carriage sentinels. They are typed so a caller can classify a
// configuration or process outcome without matching on message text.
var (
	// ErrMuxConfig reports a DialMuxContext call with an empty command
	// specification. It is a caller bug, not a peer outcome.
	ErrMuxConfig = errors.New("sshstdio: invalid mux command specification")
	// ErrMuxSSHExit reports a mux child that exited non-cleanly. Its cause
	// carries the process outcome; the child's stderr is bounded, sanitized,
	// and logged, never embedded in the public error.
	ErrMuxSSHExit = errors.New("sshstdio: mux ssh exited")
)

// BuildCommandForMux constructs the local ssh argv for one explicit remote mux
// command. It is the documented way to build a mux CommandSpec: it guarantees
// stdio-only invocation (`-T`, no remote PTY) without touching host-key,
// authentication, or proxy policy, which stays authoritative in the target's
// effective OpenSSH configuration. Every remote word is POSIX single-quoted and
// the target stays one local argv word after the option terminator.
func BuildCommandForMux(target string, command ...string) CommandSpec {
	spec := BuildCommandForRemoteCommand(target, command...)
	return CommandSpec{Path: spec.Path, Args: append([]string{"-T"}, spec.Args...)}
}

// DialMuxContext starts spec as one explicit remote command over a local ssh
// child process and returns its stdin/stdout as the raw bounded carriage the
// daemonmux multiplexer runs over. spec is executed verbatim as argv, never
// through a shell, and is never the `vev _stdio` session command: the caller
// supplies the whole command.
//
// ctx (or its deadline) bounds subprocess start. The returned carriage is
// detached from ctx and owned by Close, which terminates the child and its
// pipes and joins the process wait; cancellation and Close therefore stop the
// carriage promptly without leaking the process. A nil logger disables
// diagnostics; a non-clean exit is logged with bounded, sanitized stderr and
// surfaced as ErrMuxSSHExit with the process outcome as its cause.
func DialMuxContext(ctx context.Context, spec CommandSpec, logger *slog.Logger, opts ...Option) (wire.BoundedTransport, error) {
	return dialMuxContext(ctx, spec, logger, opts...)
}

// NewDiagnosticSink returns a bounded stderr sink that captures at most limit
// bytes and reports a sanitized, truncated diagnostic: terminal escapes and
// control bytes are stripped, so a hostile peer can neither grow the capture
// without bound nor inject control bytes into a log. It is the same bounded,
// sanitized capture the mux carriage uses for its own child diagnostics, shared
// with callers (such as the broker's authenticated QUIC bootstrap) that start
// an ssh child of their own and must never surface raw remote stderr.
func NewDiagnosticSink(limit int) *DiagnosticSink {
	if limit <= 0 {
		limit = muxStderrLimit
	}
	return &DiagnosticSink{capped: newCappedDiagnostic(limit)}
}

// DiagnosticSink is a bounded, sanitizing, concurrent-safe io.Writer for one
// child's stderr. It is safe for the os/exec stderr copy goroutine to write
// while another goroutine reads String: the read is internally synchronized and
// observes a bounded, sanitized snapshot rather than a partial capture.
type DiagnosticSink struct{ capped *cappedDiagnostic }

// Write captures at most the sink's ceiling and records that the rest was
// dropped, so the child is never blocked writing diagnostics.
func (s *DiagnosticSink) Write(p []byte) (int, error) { return s.capped.Write(p) }

// String returns the captured text sanitized and truncated to the sink's
// ceiling, so every reader observes a bounded, control-byte-free value.
func (s *DiagnosticSink) String() string { return s.capped.String() }

// NewStdioTransport returns the helper half of one SSH stdio carriage: the
// current process' own stdin and stdout framed as the raw bounded transport the
// daemonmux daemon path consumes. It is the counterpart of DialMuxContext, so a
// hidden remote mux helper serves the one physical connection that dial started
// without inventing a frame format of its own. Close releases the reader; the
// process owns its own stdout, exactly as the session helper does.
func NewStdioTransport(opts ...Option) wire.BoundedTransport {
	return NewTransport(os.Stdin, os.Stdout, nil, opts...).(wire.BoundedTransport)
}

func dialMuxContext(ctx context.Context, spec CommandSpec, logger *slog.Logger, opts ...Option) (wire.BoundedTransport, error) {
	if spec.Path == "" {
		return nil, ErrMuxConfig
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The argument vector is executed verbatim. The caller's context is not
	// bound to the command: it is shared with the daemonmux handshake that
	// follows, and Transport.Close owns the subprocess lifetime.
	cmd := exec.Command(spec.Path, spec.Args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("sshstdio: mux stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("sshstdio: mux stdout pipe: %w", err)
	}
	stderr := newCappedDiagnostic(muxStderrLimit)
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("sshstdio: mux start %s: %w", spec.Path, err)
	}
	waiter := newMuxProcessWaiter(cmd, stdin, stderr, sshCloseTimeout, logger)
	transport := newTransport(stdout, stdin, waiter.close, waiter.eofErr)
	if err := ctx.Err(); err != nil {
		_ = transport.Close()
		return nil, err
	}
	for _, opt := range opts {
		if opt != nil {
			opt(transport)
		}
	}
	return transport, nil
}

// cappedDiagnostic captures a child process' stderr up to an explicit byte
// ceiling and reports a bounded, sanitized diagnostic: terminal escapes and
// non-printable bytes are stripped, so a hostile peer can neither grow the
// capture without bound nor inject control bytes into a log.
type cappedDiagnostic struct {
	// mu makes Write and String safe to call concurrently: an os/exec stderr
	// copy goroutine may still be writing while a reader renders the diagnostic,
	// and the capture must never be observed partially written.
	mu       sync.Mutex
	buf      bytes.Buffer
	limit    int
	overflow bool
}

func newCappedDiagnostic(limit int) *cappedDiagnostic {
	return &cappedDiagnostic{limit: limit}
}

// Write captures at most the configured ceiling and records that the rest was
// dropped, so the child is never blocked writing diagnostics.
func (d *cappedDiagnostic) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	remaining := d.limit - d.buf.Len()
	if remaining <= 0 {
		d.overflow = true
		return len(p), nil
	}
	if len(p) > remaining {
		d.overflow = true
		_, _ = d.buf.Write(p[:remaining])
		return len(p), nil
	}
	return d.buf.Write(p)
}

// String returns the captured text sanitized and truncated to the diagnostic
// ceiling, so every reader observes a bounded, control-byte-free value.
func (d *cappedDiagnostic) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return sanitizeDiagnostic(d.buf.String())
}

// sanitizeDiagnostic strips terminal escapes and control bytes while preserving
// useful printable diagnostic text, and truncates the result to the diagnostic
// ceiling.
func sanitizeDiagnostic(raw string) string {
	var b strings.Builder
	b.Grow(min(len(raw), muxDiagnosticLimit))
	for i := 0; i < len(raw); {
		if raw[i] == 0x1b {
			i = skipTerminalEscape(raw, i+1)
			continue
		}
		r, size := utf8.DecodeRuneInString(raw[i:])
		if r == utf8.RuneError && size == 1 {
			i++
			continue
		}
		if unicode.IsPrint(r) || r == '\t' || r == '\n' {
			if b.Len()+size > muxDiagnosticLimit {
				break
			}
			b.WriteRune(r)
		}
		i += size
	}
	return strings.TrimSpace(b.String())
}

// skipTerminalEscape returns the index just past one terminal escape sequence
// starting at i (the byte after ESC).
func skipTerminalEscape(s string, i int) int {
	if i >= len(s) {
		return i
	}
	switch s[i] {
	case '[': // CSI: ESC [ ... final byte in 0x40-0x7E
		i++
		for i < len(s) {
			ch := s[i]
			i++
			if ch >= 0x40 && ch <= 0x7e {
				break
			}
		}
		return i
	case ']': // OSC: ESC ] ... BEL or ST (ESC \)
		i++
		for i < len(s) {
			if s[i] == 0x07 {
				return i + 1
			}
			if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '\\' {
				return i + 2
			}
			i++
		}
		return i
	default:
		// Two-byte Fe escape or unknown: drop ESC and the next byte.
		return i + 1
	}
}
