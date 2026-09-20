//go:build linux

package app

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/lifecycle"
	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/adapters/snapshot"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/bnema/vev/internal/usecase/recovery"
)

// These integration tests drive the real daemon over a real unix socket with a
// real PTY and the real VT/renderer pipeline. They live in internal/app (which
// is allowed to import adapters) because the layering guard forbids
// internal/usecase from importing internal/adapters, even in tests.

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startDaemon binds a real listener in a temp dir and serves in the background.
// The returned served channel receives Serve's result exactly once.
func startDaemon(t *testing.T, opts ...daemon.Option) (dir string, served <-chan error) {
	t.Helper()
	return startDaemonInDir(t, filepath.Join(t.TempDir(), "vev"), opts...)
}

func startDaemonInDir(t *testing.T, dir string, opts ...daemon.Option) (string, <-chan error) {
	t.Helper()
	ln, err := ipc.Listen(dir)
	require.NoError(t, err)

	d := daemon.New(pty.NewFactory(), clock.New(), discardLog(), opts...)
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- d.Serve(ctx, sessionwire.NewServerListener(ln)) }()
	t.Cleanup(cancel)
	return dir, ch
}

// pump wires a background Recv pump to a raw transport so tests can await
// envelopes with a timeout (wire.Transport has no read deadline).
type pump struct{ ch chan wire.Envelope }

func recvPump(tr wire.Transport) *pump {
	p := &pump{ch: make(chan wire.Envelope, 128)}
	go func() {
		for {
			f, err := tr.Recv()
			if err != nil {
				close(p.ch)
				return
			}
			p.ch <- f
		}
	}()
	return p
}

// typedPump wires a background ReceiveServer pump to a typed client connection
// so tests can await semantic messages with a timeout.
type typedPump struct{ ch chan protocol.ServerMessage }

func recvTypedPump(conn ports.ClientConnection) *typedPump {
	p := &typedPump{ch: make(chan protocol.ServerMessage, 128)}
	go func() {
		for {
			message, err := conn.ReceiveServer()
			if err != nil {
				close(p.ch)
				return
			}
			p.ch <- message
		}
	}()
	return p
}

// attach dials, handshakes, and returns the typed client connection plus its
// semantic message pump.
func attach(t *testing.T, dir string, intent uint8, name string, sz domain.Size) (ports.ClientConnection, *typedPump) {
	t.Helper()
	return attachWithEnvironment(t, dir, intent, name, sz, nil)
}

func attachWithEnvironment(t *testing.T, dir string, intent uint8, name string, sz domain.Size, env []string) (ports.ClientConnection, *typedPump) {
	t.Helper()
	raw, err := ipc.DialContext(context.Background(), dir)
	require.NoError(t, err)
	conn := sessionwire.NewClientConnection(raw)
	hello := protocol.Hello{Version: protocol.Version, Intent: intent, Name: name, Size: sz, TermEnv: "xterm-256color", TrueColor: true, Env: env}
	require.NoError(t, conn.SendClient(hello))
	p := recvTypedPump(conn)
	select {
	case message, ok := <-p.ch:
		require.True(t, ok, "connection closed before welcome")
		require.IsType(t, protocol.Welcome{}, message)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for welcome")
	}
	return conn, p
}

func listRemoteSessions(t *testing.T, dir string) protocol.Sessions {
	t.Helper()
	raw, err := ipc.DialContext(context.Background(), dir)
	require.NoError(t, err)
	conn := sessionwire.NewClientConnection(raw)
	defer func() { _ = conn.Close() }()
	require.NoError(t, conn.SendClient(protocol.List{}))
	message, err := conn.ReceiveServer()
	require.NoError(t, err)
	sessions, ok := message.(protocol.Sessions)
	require.True(t, ok, "expected session listing, got %T", message)
	return sessions
}

func killAll(dir string) error {
	return requestKill(dir, protocol.KillAll)
}

func killDaemon(dir string) error {
	return requestKill(dir, protocol.KillDaemon)
}

// nextKillRequestID allocates a unique nonzero correlation ID for one control
// request, matching the production caller contract.
var killRequestSeq atomic.Uint64

func nextKillRequestID() uint64 { return killRequestSeq.Add(1) }

// killNamedSession deletes one named live or stopped session over the control
// connection, using the same wire scope the client sends for `kill -s NAME`.
// It sends a unique RequestID and consumes the matching KillResult: a failed
// outcome is an error, and a close without a result is never read as success.
func killNamedSession(dir, name string) error {
	return controlKill(dir, protocol.KillScope(protocol.KillSession), name)
}

func requestKill(dir string, scope protocol.KillScope) error {
	return controlKill(dir, scope, "")
}

func controlKill(dir string, scope protocol.KillScope, name string) error {
	raw, err := ipc.DialContext(context.Background(), dir)
	if err != nil {
		return err
	}
	conn := sessionwire.NewClientConnection(raw)
	defer func() { _ = conn.Close() }()
	requestID := nextKillRequestID()
	if err := conn.SendClient(protocol.Kill{RequestID: requestID, Scope: scope, Name: name}); err != nil {
		return err
	}
	result, err := awaitKillResult(conn, requestID, daemonStopTimeout)
	if err != nil {
		return err
	}
	switch result.Outcome {
	case protocol.KillSucceeded:
		return nil
	case protocol.KillFailed:
		return fmt.Errorf("kill failed: %s", result.Text)
	default:
		return fmt.Errorf("kill outcome unknown: %s", result.Text)
	}
}

// awaitKillResult reads one correlated KillResult with a bounded timeout. A
// close, a wrong type, or a mismatched RequestID is a definite error rather
// than an inferred success, so a lost result is never reported as a kill.
func awaitKillResult(conn ports.ClientConnection, requestID uint64, timeout time.Duration) (protocol.KillResult, error) {
	type result struct {
		message protocol.ServerMessage
		err     error
	}
	received := make(chan result, 1)
	go func() {
		message, err := conn.ReceiveServer()
		received <- result{message: message, err: err}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var reply result
	select {
	case reply = <-received:
	case <-timer.C:
		_ = conn.Close()
		reply = <-received
		return protocol.KillResult{}, errors.Join(context.DeadlineExceeded, reply.err)
	}
	if reply.err != nil {
		return protocol.KillResult{}, reply.err
	}
	killResult, ok := reply.message.(protocol.KillResult)
	if !ok {
		return protocol.KillResult{}, fmt.Errorf("unexpected reply %T to kill", reply.message)
	}
	if killResult.RequestID != requestID {
		return protocol.KillResult{}, fmt.Errorf("kill result request id %d, want %d", killResult.RequestID, requestID)
	}
	return killResult, nil
}

// blockingClientConnection parks ReceiveServer until Close, so a test can prove
// the bounded await returns on timeout rather than hanging.
type blockingClientConnection struct {
	closed chan struct{}
	once   sync.Once
}

func (*blockingClientConnection) SendClient(protocol.ClientMessage) error { return nil }

func (c *blockingClientConnection) ReceiveServer() (protocol.ServerMessage, error) {
	<-c.closed
	return nil, io.ErrClosedPipe
}

func (*blockingClientConnection) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{}
}

func (*blockingClientConnection) LinkState() ports.LinkState         { return ports.LinkState(0) }
func (*blockingClientConnection) LinkEvents() <-chan ports.LinkEvent { return nil }

func (c *blockingClientConnection) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// TestAwaitKillResultTimesOut proves the bounded result await reports a timeout
// (never a silent success) and closes the exact connection when no
// KillResult arrives.
func TestAwaitKillResultTimesOut(t *testing.T) {
	conn := &blockingClientConnection{closed: make(chan struct{})}
	_, err := awaitKillResult(conn, 1, 10*time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

// TestAwaitKillResultRejectsUncorrelatedReply proves a reply of the wrong type
// or with a mismatched RequestID is a definite error, so a lost or stale result
// is never mistaken for this request's success.
func TestAwaitKillResultRejectsUncorrelatedReply(t *testing.T) {
	tests := []struct {
		name  string
		reply protocol.ServerMessage
	}{
		{name: "wrong type", reply: protocol.Pong{}},
		{name: "wrong request id", reply: protocol.KillResult{RequestID: 9, Outcome: protocol.KillSucceeded}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &scriptedKillReplyConnection{reply: tt.reply}
			_, err := awaitKillResult(conn, 1, time.Second)
			require.Error(t, err)
		})
	}
}

// scriptedKillReplyConnection returns exactly one canned server message.
type scriptedKillReplyConnection struct {
	reply protocol.ServerMessage
}

func (*scriptedKillReplyConnection) SendClient(protocol.ClientMessage) error { return nil }

func (c *scriptedKillReplyConnection) ReceiveServer() (protocol.ServerMessage, error) {
	if c.reply == nil {
		return nil, io.EOF
	}
	reply := c.reply
	c.reply = nil
	return reply, nil
}

func (*scriptedKillReplyConnection) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{}
}

func (*scriptedKillReplyConnection) LinkState() ports.LinkState         { return ports.LinkState(0) }
func (*scriptedKillReplyConnection) LinkEvents() <-chan ports.LinkEvent { return nil }
func (*scriptedKillReplyConnection) Close() error                       { return nil }

// mustHelloBytes encodes a minimal valid Hello for first-frame shape probes.
func mustHelloBytes() []byte {
	raw, err := sessionwire.EncodeClientMessage(protocol.Hello{
		Version: protocol.Version, Size: domain.Size{Cols: 80, Rows: 24},
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func TestIntegration_MalformedCommandPreservesVersionAndRequestID(t *testing.T) {
	versionMismatch, err := sessionwire.EncodeClientMessage(protocol.CommandRequest{
		Version: protocol.Version + 1, RequestID: 42, Slug: "list-sessions",
	})
	require.NoError(t, err)
	valid, err := sessionwire.EncodeClientMessage(protocol.CommandRequest{
		Version: protocol.Version, RequestID: 43, Slug: "list-sessions",
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		payload []byte
		want    protocol.ServerMessage
	}{
		{
			name:    "incompatible version",
			payload: versionMismatch,
			want:    &protocol.CommandResult{Outcome: protocol.CommandFailed, RequestID: 42, Code: protocol.ErrVersionMismatch, Text: "protocol version mismatch"},
		},
		{
			// A single zero byte carries no recoverable envelope tag:
			// the daemon answers the generic hello error.
			name:    "truncated version prefix",
			payload: []byte{0},
			want:    &protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "expected hello"},
		},
		{
			// Scan-level trailing garbage on a one-shot command
			// connection carries no recoverable request: the daemon
			// logs the rejection and closes without a typed refusal,
			// so the client observes EOF.
			name:    "trailing garbage closes without refusal",
			payload: append(append([]byte(nil), valid...), 0xff),
		},
		{
			// A decodable Hello with trailing garbage fails semantic
			// validation after scanning: it is neither a command nor a
			// well-formed attach, so the daemon answers the hello
			// refusal before closing.
			name:    "hello-shaped trailing garbage replies hello error",
			payload: append(append([]byte(nil), mustHelloBytes()...), 0xff),
			want:    &protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "malformed hello"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := startDaemon(t)
			raw, err := ipc.DialContext(context.Background(), dir)
			require.NoError(t, err)
			defer func() { _ = raw.Close() }()

			preambleRequest, err := sessionwire.EncodePreambleRequestForTest()
			require.NoError(t, err)
			require.NoError(t, raw.Send(wire.Envelope{Payload: preambleRequest}))
			response, err := raw.Recv()
			require.NoError(t, err)
			require.True(t, sessionwire.DecodePreambleAcceptanceForTest(response.Payload))

			require.NoError(t, raw.Send(wire.Envelope{Payload: tt.payload}))
			frame, err := raw.Recv()
			if tt.want == nil {
				require.ErrorIs(t, err, io.EOF)
				return
			}
			require.NoError(t, err)
			message, err := sessionwire.DecodeServerEnvelope(frame.Payload)
			require.NoError(t, err)
			switch want := tt.want.(type) {
			case *protocol.CommandResult:
				result, ok := message.(protocol.CommandResult)
				require.True(t, ok, "expected command result, got %T", message)
				require.Equal(t, *want, result)
			case *protocol.ErrorMsg:
				reply, ok := message.(protocol.ErrorMsg)
				require.True(t, ok, "expected error reply, got %T", message)
				require.Equal(t, *want, reply)
			default:
				t.Fatalf("unsupported want type %T", tt.want)
			}
		})
	}
}

// awaitText decodes MsgOutput frames into a fresh VT screen and returns once
// the reconstructed grid contains want.
func awaitText(t *testing.T, p *typedPump, sz domain.Size, want string) {
	t.Helper()
	_ = awaitScreenText(t, p, sz, want)
}

// awaitDetached consumes typed messages until the daemon signals the session's
// end with the expected Detached reason, ignoring interleaved Output frames.
func awaitDetached(t *testing.T, p *typedPump, reason uint8) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case message, ok := <-p.ch:
			if !ok {
				t.Fatal("connection closed before Detached")
			}
			if detached, ok := message.(protocol.Detached); ok {
				require.Equal(t, reason, detached.Reason)
				return
			}
		case <-timeout:
			t.Fatal("timed out waiting for Detached")
		}
	}
}

// awaitScreenText is like awaitText, but returns the reconstructed screen text
// at the point the wanted text appears so callers can make additional checks.
func awaitScreenText(t *testing.T, p *typedPump, sz domain.Size, want string) string {
	t.Helper()
	screen := vt.NewScreen(sz.Cols, sz.Rows)
	timeout := time.After(5 * time.Second)
	for {
		select {
		case message, ok := <-p.ch:
			if !ok {
				t.Fatalf("connection closed before %q appeared; screen=%q", want, screenText(screen))
			}
			if output, ok := message.(protocol.Output); ok {
				screen.Write(output.Data)
				text := screenText(screen)
				if strings.Contains(text, want) {
					return text
				}
			}
		case <-timeout:
			t.Fatalf("timed out waiting for %q; screen=%q", want, screenText(screen))
		}
	}
}

func assertNoTextAfterInput(t *testing.T, p *typedPump, sz domain.Size, absent string) {
	t.Helper()
	screen := vt.NewScreen(sz.Cols, sz.Rows)
	timeout := time.After(300 * time.Millisecond)
	for {
		select {
		case message, ok := <-p.ch:
			if !ok {
				return
			}
			if output, ok := message.(protocol.Output); ok {
				screen.Write(output.Data)
				require.NotContains(t, screenText(screen), absent)
			}
		case <-timeout:
			return
		}
	}
}

func screenText(s *vt.Screen) string {
	var b strings.Builder
	for y := range s.Rows() {
		for x := range s.Columns() {
			r := s.Cell(x, y).Rune
			if r == 0 {
				r = ' '
			}
			b.WriteRune(r)
		}
	}
	return b.String()
}

func shellFixture(t *testing.T, label string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shell")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\nprintf 'SHELL_COMMAND="+label+"\\n'\nexec /bin/sh\n"), 0o700))
	return path
}

func assertChildEnvironment(t *testing.T, tr ports.ClientConnection, p *typedPump, sz domain.Size, wantTestEnv, wantShell, wantRuntimeDir, wantWayland string) {
	t.Helper()
	command := "printf '\\033[2J\\033[H'; printf 'VEV_TEST_ENV=%s SHELL=%s XDG_RUNTIME_DIR=%s WAYLAND_DISPLAY=%s TERM=%s COLORTERM=%s TERM_PROGRAM=%s VEV_PREFIX=%.24s\\n' \"$VEV_TEST_ENV\" \"${SHELL##*/}\" \"$XDG_RUNTIME_DIR\" \"$WAYLAND_DISPLAY\" \"$TERM\" \"$COLORTERM\" \"$TERM_PROGRAM\" \"$VEV\"\n"
	require.NoError(t, tr.SendClient(protocol.Input{Data: []byte(command)}))
	text := awaitScreenText(t, p, sz, "TERM_PROGRAM=vev")
	// The concrete PTY adapter selects the profile from host terminfo. Exact
	// direct/fallback behavior is covered by the adapter's hermetic tests.
	require.Regexp(t, `TERM=xterm-(direct|256color)\s`, text)
	for _, want := range []string{
		"VEV_TEST_ENV=" + wantTestEnv,
		"SHELL=" + filepath.Base(wantShell),
		"XDG_RUNTIME_DIR=" + wantRuntimeDir,
		"WAYLAND_DISPLAY=" + wantWayland,
		"COLORTERM=truecolor",
		"TERM_PROGRAM=vev",
		"VEV_PREFIX=session=environment,tab=",
	} {
		require.Contains(t, text, want)
	}
}

func TestIntegration_AttachEnvironmentRefreshesFuturePTYChildren(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t)
	firstShell := shellFixture(t, "first")
	secondShell := shellFixture(t, "second")
	firstEnv := []string{
		"VEV_TEST_ENV=first", "SHELL=" + firstShell, "XDG_RUNTIME_DIR=/run/first", "WAYLAND_DISPLAY=wayland-first",
		"TERM=client", "COLORTERM=client", "TERM_PROGRAM=client", "VEV=client",
	}

	tr1, p1 := attachWithEnvironment(t, dir, protocol.IntentNew, "environment", sz, firstEnv)
	defer func() { _ = tr1.Close() }()
	awaitText(t, p1, sz, "SHELL_COMMAND=first")
	assertChildEnvironment(t, tr1, p1, sz, "first", firstShell, "/run/first", "wayland-first")

	secondEnv := []string{
		"VEV_TEST_ENV=second", "SHELL=" + secondShell, "XDG_RUNTIME_DIR=/run/second", "WAYLAND_DISPLAY=wayland-second",
		"TERM=client", "COLORTERM=client", "TERM_PROGRAM=client", "VEV=client",
	}
	tr2, p2 := attachWithEnvironment(t, dir, protocol.IntentAttach, "environment", sz, secondEnv)
	defer func() { _ = tr2.Close() }()

	// The first shell was already running, so it retains its original environment.
	assertChildEnvironment(t, tr2, p2, sz, "first", firstShell, "/run/first", "wayland-first")

	require.NoError(t, tr2.SendClient(protocol.Input{Data: []byte("\x1b ")}))
	awaitText(t, p2, sz, "Commands")
	require.NoError(t, tr2.SendClient(protocol.Input{Data: []byte("CNT\r")}))
	awaitText(t, p2, sz, "SHELL_COMMAND=second")
	assertChildEnvironment(t, tr2, p2, sz, "second", secondShell, "/run/second", "wayland-second")
}

func TestIntegration_TwoAttachmentsReceiveSharedMutationPTYOutput(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t)
	firstShell := shellFixture(t, "first")
	const sharedOutput = "SECOND_SHARED_PTY_OUTPUT"
	secondShell := filepath.Join(t.TempDir(), "second-shell")
	require.NoError(t, os.WriteFile(secondShell, []byte("#!/bin/sh\nprintf 'SHELL_COMMAND=second\\n'\nIFS= read -r _\nprintf '"+sharedOutput+"\\n'\nexec /bin/sh\n"), 0o700))
	firstEnv := []string{
		"VEV_TEST_ENV=first", "SHELL=" + firstShell, "XDG_RUNTIME_DIR=/run/first", "WAYLAND_DISPLAY=wayland-first",
		"TERM=client", "COLORTERM=client", "TERM_PROGRAM=client", "VEV=client",
	}
	tr1, p1 := attachWithEnvironment(t, dir, protocol.IntentNew, "environment", sz, firstEnv)
	defer func() { _ = tr1.Close() }()
	awaitText(t, p1, sz, "SHELL_COMMAND=first")
	assertChildEnvironment(t, tr1, p1, sz, "first", firstShell, "/run/first", "wayland-first")

	secondEnv := []string{
		"VEV_TEST_ENV=second", "SHELL=" + secondShell, "XDG_RUNTIME_DIR=/run/second", "WAYLAND_DISPLAY=wayland-second",
		"TERM=client", "COLORTERM=client", "TERM_PROGRAM=client", "VEV=client",
	}
	tr2, p2 := attachWithEnvironment(t, dir, protocol.IntentAttach, "environment", sz, secondEnv)
	defer func() { _ = tr2.Close() }()
	assertChildEnvironment(t, tr2, p2, sz, "first", firstShell, "/run/first", "wayland-first")

	// The second attachment mutates shared tab state. Its newly opened PTY must
	// also be rendered to that attachment, not only to the coordinator primary.
	require.NoError(t, tr2.SendClient(protocol.Input{Data: []byte("\x1b ")}))
	awaitText(t, p2, sz, "Commands")
	require.NoError(t, tr2.SendClient(protocol.Input{Data: []byte("CNT\r")}))
	awaitText(t, p2, sz, "SHELL_COMMAND=second")

	// Each attachment owns its selected tab. Move the first attachment to the
	// new PTY before releasing its gate, so the fresh PTY output must reach both
	// views rather than only being recovered by a later first paint.
	require.NoError(t, tr1.SendClient(protocol.Input{Data: []byte("\x1b2")}))
	awaitText(t, p1, sz, "SHELL_COMMAND=second")
	require.NoError(t, tr1.SendClient(protocol.Input{Data: []byte("go\n")}))
	awaitText(t, p2, sz, sharedOutput)
	awaitText(t, p1, sz, sharedOutput)
}

func TestIntegration_AttachFirstOutput(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf HELLO; sleep 30"}))

	tr, p := attach(t, dir, protocol.IntentEphemeral, "", sz)
	defer func() { _ = tr.Close() }()

	awaitText(t, p, sz, "HELLO")
}

func TestIntegration_InputRoundtrip(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/cat", nil))

	tr, p := attach(t, dir, protocol.IntentEphemeral, "", sz)
	defer func() { _ = tr.Close() }()

	require.NoError(t, tr.SendClient(protocol.Input{Data: []byte("PINGPONG\n")}))
	awaitText(t, p, sz, "PINGPONG")
}

func TestIntegration_CommandPaletteCreatesTab(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf READY; sleep 30"}))

	tr, p := attach(t, dir, protocol.IntentEphemeral, "", sz)
	defer func() { _ = tr.Close() }()
	awaitText(t, p, sz, "READY")

	require.NoError(t, tr.SendClient(protocol.Input{Data: []byte("\x1b ")}))
	awaitText(t, p, sz, "Commands")

	require.NoError(t, tr.SendClient(protocol.Input{Data: []byte("CNT\r")}))
	// Tab labels are enriched with the focused pane's title; with no process
	// inspector wired in this test, that title falls back to the shell's
	// basename ("sh").
	text := awaitScreenText(t, p, sz, " 1 (sh)  2 (sh) ")
	require.Contains(t, text, " 1 (sh)  2 (sh) ")
}

func TestIntegration_CommandPaletteRenamesEphemeralSession(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf READY; sleep 30"}))

	tr, p := attach(t, dir, protocol.IntentEphemeral, "", sz)
	defer func() { _ = tr.Close() }()
	text := awaitScreenText(t, p, sz, "READY")
	require.Contains(t, text, " 0* ")

	require.NoError(t, tr.SendClient(protocol.Input{Data: []byte("\x1b ")}))
	awaitText(t, p, sz, "Commands")

	require.NoError(t, tr.SendClient(protocol.Input{Data: []byte("RNS\r")}))
	awaitText(t, p, sz, "Rename session")

	require.NoError(t, tr.SendClient(protocol.Input{Data: []byte("\x7fwork\r")}))
	text = awaitScreenText(t, p, sz, " work ")
	require.NotContains(t, text, "work*")

	listRaw, err := ipc.DialContext(context.Background(), dir)
	require.NoError(t, err)
	listTr := sessionwire.NewClientConnection(listRaw)
	defer func() { _ = listTr.Close() }()
	require.NoError(t, listTr.SendClient(protocol.List{}))
	message, err := listTr.ReceiveServer()
	require.NoError(t, err)
	sessions, ok := message.(protocol.Sessions)
	require.True(t, ok, "expected session listing, got %T", message)
	require.NotEmpty(t, sessions.Sessions)
	require.Equal(t, "work", sessions.Sessions[0].Name)
	require.False(t, sessions.Sessions[0].Ephemeral)
}

func TestIntegration_AltCWithoutPaletteDoesNotCreateTab(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf READY; sleep 30"}))

	tr, p := attach(t, dir, protocol.IntentEphemeral, "", sz)
	defer func() { _ = tr.Close() }()
	text := awaitScreenText(t, p, sz, "READY")
	require.NotContains(t, text, " 1  2 ")

	require.NoError(t, tr.SendClient(protocol.Input{Data: []byte("\x1bc")}))
	assertNoTextAfterInput(t, p, sz, " 1  2 ")
}

func TestIntegration_EphemeralSurvivesDetachAndReattaches(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, served := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf MARKER; sleep 30"}))

	tr1, p1 := attach(t, dir, protocol.IntentEphemeral, "", sz)
	awaitText(t, p1, sz, "MARKER")
	require.NoError(t, tr1.Close())

	select {
	case err := <-served:
		t.Fatalf("daemon shut down while an ephemeral session was alive: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	tr2, p2 := attach(t, dir, protocol.IntentAttach, "0", sz)
	defer func() { _ = tr2.Close() }()
	awaitText(t, p2, sz, "MARKER")

	// An ephemeral session's daemon is ended only by the explicit daemon stop.
	require.NoError(t, killDaemon(dir))
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop after kill --daemon")
	}
}

func TestIntegration_EphemeralNotListedAfterDaemonRestart(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
	dir := ipc.SocketDir()

	_, served := startDaemonInDir(t, dir, daemon.WithShell("/bin/sh", []string{"-c", "printf TEMP; sleep 30"}))
	tr, p := attach(t, dir, protocol.IntentEphemeral, "", sz)
	awaitText(t, p, sz, "TEMP")
	require.NoError(t, tr.Close())

	require.NoError(t, runKill(context.Background(), "", false, true))
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop after kill --daemon")
	}

	_, served2 := startDaemonInDir(t, dir, daemon.WithShell("/bin/sh", []string{"-c", "sleep 30"}))
	sessions := listRemoteSessions(t, dir)
	require.Empty(t, sessions.Sessions)

	require.NoError(t, runKill(context.Background(), "", false, true))
	select {
	case err := <-served2:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("restarted daemon did not stop after kill --daemon")
	}
}

func TestIntegration_KillDaemonPreservesMultipleNamedSessions(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	stateDir := filepath.Join(t.TempDir(), "state")
	dir := filepath.Join(t.TempDir(), "runtime")
	repository := snapshot.NewRepository(filepath.Join(stateDir, "snapshots"))

	start := func() <-chan error {
		opened, err := persist.OpenOrCreate(stateDir)
		require.NoError(t, err)
		coordinator := recovery.NewCoordinator(opened.Catalogue, repository, rand.Reader)
		_, served := startDaemonInDir(t, dir,
			daemon.WithShell("/bin/sh", []string{"-c", "sleep 30"}),
			daemon.WithCatalogue(opened.Catalogue, opened.Records),
			daemon.WithSnapshotRepository(repository),
			daemon.WithRecoveryCoordinator(coordinator),
		)
		return served
	}

	served := start()
	first, _ := attach(t, dir, protocol.IntentNew, "alpha", sz)
	require.NoError(t, first.Close())
	second, _ := attach(t, dir, protocol.IntentNew, "beta", sz)
	require.NoError(t, second.Close())

	require.NoError(t, killDaemon(dir))
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop")
	}

	served = start()
	sessions := listRemoteSessions(t, dir)
	require.Equal(t, []protocol.SessionInfo{
		{Name: "alpha", State: protocol.SessionDown},
		{Name: "beta", State: protocol.SessionDown},
	}, sessions.Sessions)

	// Kill-all purges every durable record but leaves this daemon serving.
	require.NoError(t, killAll(dir))
	require.Empty(t, listRemoteSessions(t, dir).Sessions)
	select {
	case err := <-served:
		t.Fatalf("kill-all stopped the restarted daemon: %v", err)
	default:
	}
	require.NoError(t, killDaemon(dir))
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("restarted daemon did not stop after kill --daemon")
	}

	served = start()
	require.Empty(t, listRemoteSessions(t, dir).Sessions)
	require.NoError(t, killDaemon(dir))
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("empty daemon did not stop")
	}
}

func TestIntegration_NamedSurvivesReattach(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, served := startDaemon(t, daemon.WithShell("/bin/sh", []string{"-c", "printf MARKER; sleep 30"}))

	tr1, p1 := attach(t, dir, protocol.IntentNew, "work", sz)
	awaitText(t, p1, sz, "MARKER")

	// Detach: a named session must survive.
	require.NoError(t, tr1.Close())

	// Daemon must still be running (session alive).
	select {
	case err := <-served:
		t.Fatalf("daemon shut down while a named session was alive: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	// Re-attach: the first paint must reproduce the retained screen state.
	tr2, p2 := attach(t, dir, protocol.IntentAttach, "work", sz)
	defer func() { _ = tr2.Close() }()
	awaitText(t, p2, sz, "MARKER")
}

// TestIntegration_NamedSessionLastRemovalKeepsDaemonServing is the P4.1
// app-level contract (P4.1 review GO-003): a named session killed as the last
// registry entry is removed without stopping Serve, and the same daemon process
// accepts the same name again. It runs over the real socket, PTY, and
// catalogue/recovery/snapshot persistence the production composition installs.
// A named session's final live removal is a durable purge, so the re-attached
// name carries a fresh persisted incarnation rather than resuming the deleted
// one.
func TestIntegration_NamedSessionLastRemovalKeepsDaemonServing(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	stateDir := filepath.Join(t.TempDir(), "state")
	dir := filepath.Join(t.TempDir(), "runtime")
	repository := snapshot.NewRepository(filepath.Join(stateDir, "snapshots"))
	opened, err := persist.OpenOrCreate(stateDir)
	require.NoError(t, err)
	coordinator := recovery.NewCoordinator(opened.Catalogue, repository, rand.Reader)

	_, served := startDaemonInDir(t, dir,
		daemon.WithShell("/bin/cat", nil),
		daemon.WithCatalogue(opened.Catalogue, opened.Records),
		daemon.WithSnapshotRepository(repository),
		daemon.WithRecoveryCoordinator(coordinator),
	)

	first, p1 := attach(t, dir, protocol.IntentNew, "work", sz)
	require.NoError(t, first.SendClient(protocol.Input{Data: []byte("ping\n")}))
	awaitText(t, p1, sz, "ping")
	firstRecord, ok, err := opened.Catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok, "a live named session must own a durable record")

	// End it as the last session: the registry empties, but that is not daemon
	// shutdown in the P4.1 model.
	require.NoError(t, killNamedSession(dir, "work"))
	awaitDetached(t, p1, protocol.ReasonSessionKilled)
	require.Eventually(t, func() bool { return len(listRemoteSessions(t, dir).Sessions) == 0 },
		5*time.Second, 10*time.Millisecond, "session was not removed")
	select {
	case err := <-served:
		t.Fatalf("Serve stopped after the last session was removed: %v", err)
	default:
	}
	_, ok, err = opened.Catalogue.Record("work")
	require.NoError(t, err)
	require.False(t, ok, "final live removal purges the durable record")

	// The same daemon process accepts the same name again and can still serve a
	// fresh terminal operation.
	second, p2 := attach(t, dir, protocol.IntentNew, "work", sz)
	require.NoError(t, second.SendClient(protocol.Input{Data: []byte("pong\n")}))
	awaitText(t, p2, sz, "pong")
	sessions := listRemoteSessions(t, dir).Sessions
	require.Len(t, sessions, 1)
	require.Equal(t, "work", sessions[0].Name)
	require.Equal(t, protocol.SessionUp, sessions[0].State)
	secondRecord, ok, err := opened.Catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok, "re-attached named session must persist its identity")
	require.NotEqual(t, firstRecord.IncarnationID, secondRecord.IncarnationID,
		"a re-created name must not reuse the purged incarnation")

	require.NoError(t, second.Close())
	require.NoError(t, killDaemon(dir))
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop after kill --daemon")
	}
}

// TestIntegration_NamedSessionRestoresPersistedIdentity proves a stopped named
// session is restored inside the daemon process that is already serving it,
// keeping its exact persisted identity. The seed daemon stops while holding the
// session as its only registry entry, which preserves it as stopped authority;
// the daemon under test then resumes it over a real socket with the production
// catalogue/recovery/snapshot options and serves a fresh operation.
func TestIntegration_NamedSessionRestoresPersistedIdentity(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	stateDir := filepath.Join(t.TempDir(), "state")
	repository := snapshot.NewRepository(filepath.Join(stateDir, "snapshots"))
	name := publishRestorableCheckpoint(t, stateDir, repository)

	opened, err := persist.OpenOrCreate(stateDir)
	require.NoError(t, err)
	stopped, ok, err := opened.Catalogue.Record(name)
	require.NoError(t, err)
	require.True(t, ok, "stopped named session must keep its durable record")
	require.NotNil(t, stopped.Committed, "stopped named session must keep its committed checkpoint")

	dir := filepath.Join(t.TempDir(), "runtime")
	coordinator := recovery.NewCoordinator(opened.Catalogue, repository, rand.Reader)
	_, served := startDaemonInDir(t, dir,
		daemon.WithShell("/bin/cat", nil),
		daemon.WithCatalogue(opened.Catalogue, opened.Records),
		daemon.WithSnapshotRepository(repository),
		daemon.WithRecoveryCoordinator(coordinator),
	)
	require.Equal(t, []protocol.SessionInfo{{Name: name, State: protocol.SessionDown}}, listRemoteSessions(t, dir).Sessions)
	select {
	case err := <-served:
		t.Fatalf("Serve stopped before the restore: %v", err)
	default:
	}

	conn, p := attach(t, dir, protocol.IntentAttach, name, sz)
	require.NoError(t, conn.SendClient(protocol.Input{Data: []byte("revived\n")}))
	awaitText(t, p, sz, "revived")
	require.Eventually(t, func() bool {
		sessions := listRemoteSessions(t, dir).Sessions
		return len(sessions) == 1 && sessions[0].State == protocol.SessionUp
	}, 5*time.Second, 10*time.Millisecond, "named session was not restored")

	restored, ok, err := opened.Catalogue.Record(name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, stopped.IncarnationID, restored.IncarnationID,
		"restore must retain the persisted incarnation")
	require.Equal(t, stopped.CreatedAt, restored.CreatedAt,
		"restore must retain the persisted lifecycle timestamp")

	require.NoError(t, conn.Close())
	select {
	case err := <-served:
		t.Fatalf("Serve stopped while the restored session was live: %v", err)
	default:
	}
	require.NoError(t, killDaemon(dir))
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop after kill --daemon")
	}
}

func TestMultipleClientsOneLifecycleOwner(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "runtime")
	owner, err := lifecycle.TryAcquire(dir)
	require.NoError(t, err)

	ready := make(chan struct{})
	transport := &integrationTransport{}
	dial := func(context.Context, string) (wire.Transport, error) {
		select {
		case <-ready:
			return transport, nil
		default:
			return nil, os.ErrNotExist
		}
	}
	var spawns atomic.Int32
	cfg := backoffConfig{initial: time.Millisecond, max: 2 * time.Millisecond, total: time.Second}

	const clients = 4
	errs := make(chan error, clients)
	var wg sync.WaitGroup
	for range clients {
		wg.Go(func() {
			_, err := ensureDaemonWithLifecycle(context.Background(), dir, dial, func() error {
				spawns.Add(1)
				return nil
			}, cfg)
			errs <- err
		})
	}
	require.Never(t, func() bool { return spawns.Load() != 0 }, 50*time.Millisecond, time.Millisecond)
	close(ready)
	require.NoError(t, owner.Release())
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Zero(t, spawns.Load())
}

func TestListWaitsForLifecycleOwner(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
	p := newTestPersister(t, filepath.Join(stateRoot, "vev"))
	require.NoError(t, p.Close())
	owner, err := lifecycle.TryAcquire(ipc.SocketDir())
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- runList(context.Background(), command{kind: kindList}) }()
	require.Never(t, func() bool { return len(done) != 0 }, 50*time.Millisecond, time.Millisecond)
	require.NoError(t, owner.Release())
	require.NoError(t, <-done)
}

func TestOfflineKillWaitsForLifecycleOwner(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
	p := newTestPersister(t, filepath.Join(stateRoot, "vev"))
	now := time.Now().UnixNano()
	require.NoError(t, p.Save(persist.Record{Name: "named", IncarnationID: domain.IncarnationID{1}, CreatedAt: now, UpdatedAt: now}))
	require.NoError(t, p.Close())
	owner, err := lifecycle.TryAcquire(ipc.SocketDir())
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- runKill(context.Background(), "named", false, false) }()
	require.Never(t, func() bool { return len(done) != 0 }, 50*time.Millisecond, time.Millisecond)
	require.NoError(t, owner.Release())
	require.NoError(t, <-done)
}

func TestKillDaemonWaitsForOwnershipTransfer(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
	owner, err := lifecycle.TryAcquire(ipc.SocketDir())
	require.NoError(t, err)
	_, served := startDaemonInDir(t, ipc.SocketDir())

	done := make(chan error, 1)
	go func() { done <- requestDaemonStop(context.Background()) }()
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("daemon did not stop")
	}
	require.Never(t, func() bool { return len(done) != 0 }, 50*time.Millisecond, time.Millisecond)
	require.NoError(t, owner.Release())
	require.NoError(t, <-done)
}

func TestLifecycleOwnershipOutlivesMaintenanceWriter(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	stateDir := filepath.Join(t.TempDir(), "state")
	catalogue := &lifecycleObservedCatalogue{
		Catalogue: newTestPersister(t, stateDir),
		closed:    make(chan struct{}),
	}
	require.NoError(t, catalogue.Create(domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{1}}))
	repository := snapshot.NewRepository(filepath.Join(stateDir, "snapshots"))
	shutdownClock := newLifecycleShutdownClock(t)
	maintenanceRepository := &lifecycleBlockingMaintenanceRepository{
		SnapshotRepository: repository,
		entered:            make(chan struct{}),
		release:            make(chan struct{}),
		returned:           make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var releaseOnce sync.Once
	releaseMaintenance := func() { releaseOnce.Do(func() { close(maintenanceRepository.release) }) }
	t.Cleanup(releaseMaintenance)

	callbackReturned := make(chan struct{})
	wrapperReturned := make(chan error, 1)
	go func() {
		wrapperReturned <- runWithLifecycleOwner(ctx, runtimeDir, stateDir, func(ctx context.Context) error {
			defer close(callbackReturned)
			d := daemon.New(
				pty.NewFactory(),
				shutdownClock,
				discardLog(),
				daemon.WithSnapshotRepository(repository),
				daemon.WithDurableMaintenance(catalogue, maintenanceRepository),
				daemon.WithCatalogue(catalogue, nil),
			)
			if err := d.CollectStartupGarbage(ctx); err != nil {
				return err
			}
			listener, err := ipc.Listen(runtimeDir)
			if err != nil {
				return err
			}
			return d.Serve(ctx, sessionwire.NewServerListener(listener))
		})
	}()

	awaitLifecycleStage(t, maintenanceRepository.entered, "pre-publication maintenance repository call")
	cancel()
	select {
	case <-callbackReturned:
		t.Fatal("Serve callback returned after shutdown deadline while maintenance was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	assertLifecycleStagePending(t, catalogue.closed, "catalogue close while maintenance is blocked")
	assertLifecycleResultPending(t, wrapperReturned, "lifecycle wrapper return while maintenance is blocked")
	_, err := lifecycle.TryAcquire(runtimeDir)
	require.ErrorIs(t, err, lifecycle.ErrBusy)

	releaseMaintenance()
	awaitLifecycleStage(t, maintenanceRepository.returned, "maintenance repository return")
	awaitLifecycleStage(t, catalogue.closed, "catalogue close after maintenance")
	awaitLifecycleStage(t, callbackReturned, "Serve callback return after catalogue close")
	require.NoError(t, awaitLifecycleResult(t, wrapperReturned, "lifecycle wrapper return"))
	owner, err := lifecycle.TryAcquire(runtimeDir)
	require.NoError(t, err)
	require.NoError(t, owner.Release())
}

type lifecycleBlockingMaintenanceRepository struct {
	ports.SnapshotRepository
	entered    chan struct{}
	release    chan struct{}
	returned   chan struct{}
	enterOnce  sync.Once
	returnOnce sync.Once
}

func (r *lifecycleBlockingMaintenanceRepository) CollectGarbage(context.Context, map[domain.IncarnationID]domain.CheckpointRef) error {
	r.enterOnce.Do(func() { close(r.entered) })
	<-r.release // Intentionally ignore cancellation: lifecycle ownership must outlive this call.
	r.returnOnce.Do(func() { close(r.returned) })
	return nil
}

type lifecycleObservedCatalogue struct {
	ports.Catalogue
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *lifecycleObservedCatalogue) Close() error {
	err := c.Catalogue.Close()
	c.closeOnce.Do(func() { close(c.closed) })
	return err
}

func TestLifecycleOwnershipOutlivesSnapshotWriter(t *testing.T) {
	result := runBlockedSnapshotWriterShutdown(t)
	assertLifecycleStagePending(t, result.callbackReturned, "Serve callback return while snapshot writer is blocked")
	assertLifecycleResultPending(t, result.wrapperReturned, "lifecycle wrapper return while snapshot writer is blocked")
	_, err := lifecycle.TryAcquire(result.runtimeDir)
	require.ErrorIs(t, err, lifecycle.ErrBusy)

	result.releaseWriter()
	awaitLifecycleStage(t, result.publishReturned, "snapshot writer return")
	require.NoError(t, awaitLifecycleResult(t, result.wrapperReturned, "lifecycle wrapper return"))
	owner, err := lifecycle.TryAcquire(result.runtimeDir)
	require.NoError(t, err)
	require.NoError(t, owner.Release())
}

func TestLifecycleCallbackWaitsForEveryWriter(t *testing.T) {
	result := runBlockedSnapshotWriterShutdown(t)
	assertLifecycleStagePending(t, result.callbackReturned, "Serve callback return while snapshot writer is blocked")

	result.releaseWriter()
	awaitLifecycleStage(t, result.publishReturned, "snapshot writer return")
	awaitLifecycleStage(t, result.callbackReturned, "Serve callback return after snapshot writer")
	require.NoError(t, awaitLifecycleResult(t, result.wrapperReturned, "lifecycle wrapper return"))
}

func TestFinalCheckpointTimeoutKeepsOwner(t *testing.T) {
	result := runBlockedSnapshotWriterShutdown(t)
	assertLifecycleResultPending(t, result.wrapperReturned, "ownership release after checkpoint timeout with writer alive")
	_, err := lifecycle.TryAcquire(result.runtimeDir)
	require.ErrorIs(t, err, lifecycle.ErrBusy)

	result.releaseWriter()
	require.NoError(t, awaitLifecycleResult(t, result.wrapperReturned, "lifecycle wrapper return"))
}

// Startup restoration repairs HEADs, promotes fallbacks, and replaces catalogue
// records. Lifecycle ownership must therefore outlive it: releasing the flock
// while a restoration repository call is still running would let a second
// daemon mutate the same durable state.
func TestLifecycleOwnershipOutlivesRestorationWriter(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	repository := snapshot.NewRepository(filepath.Join(stateDir, "snapshots"))
	checkpointed := publishRestorableCheckpoint(t, stateDir, repository)

	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	blocking := &lifecycleBlockingRestore{
		SnapshotRepository: repository,
		entered:            make(chan struct{}),
		release:            make(chan struct{}),
		returned:           make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseRestore := func() { releaseOnce.Do(func() { close(blocking.release) }) }
	t.Cleanup(releaseRestore)

	shutdownClock := newLifecycleShutdownClock(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	listenerReady := make(chan struct{})
	callbackReturned := make(chan struct{})
	wrapperReturned := make(chan error, 1)
	go func() {
		wrapperReturned <- runWithLifecycleOwner(ctx, runtimeDir, stateDir, func(ctx context.Context) error {
			defer close(callbackReturned)
			opened, err := persist.OpenOrCreate(stateDir)
			if err != nil {
				return err
			}
			listener, err := ipc.Listen(runtimeDir)
			if err != nil {
				return errors.Join(err, opened.Catalogue.Close())
			}
			close(listenerReady)
			d := daemon.New(
				pty.NewFactory(),
				shutdownClock,
				discardLog(),
				daemon.WithShell("/bin/cat", nil),
				daemon.WithCatalogue(opened.Catalogue, opened.Records),
				daemon.WithSnapshotRepository(blocking),
			)
			return d.Serve(ctx, sessionwire.NewServerListener(listener))
		})
	}()

	awaitLifecycleStage(t, listenerReady, "daemon listener")
	awaitLifecycleStage(t, blocking.entered, "restoration repository call")
	require.Equal(t, checkpointed, blocking.repairedName(), "restoration must target the checkpointed session")

	cancel()
	shutdownClock.nextTimer(t).fire()
	select {
	case <-callbackReturned:
		t.Fatal("Serve callback returned after shutdown deadline while restoration was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	assertLifecycleStagePending(t, callbackReturned, "Serve callback return while restoration is blocked")
	assertLifecycleResultPending(t, wrapperReturned, "lifecycle wrapper return while restoration is blocked")
	_, err := lifecycle.TryAcquire(runtimeDir)
	require.ErrorIs(t, err, lifecycle.ErrBusy)

	releaseRestore()
	awaitLifecycleStage(t, blocking.returned, "restoration repository return")
	awaitLifecycleStage(t, callbackReturned, "Serve callback return after restoration")
	require.NoError(t, awaitLifecycleResult(t, wrapperReturned, "lifecycle wrapper return"))
	owner, err := lifecycle.TryAcquire(runtimeDir)
	require.NoError(t, err)
	require.NoError(t, owner.Release())
}

// publishRestorableCheckpoint runs a complete daemon over the real catalogue and
// repository until one named session owns a committed checkpoint, then shuts it
// down. The returned name is restorable by any later daemon on the same state.
func publishRestorableCheckpoint(t *testing.T, stateDir string, repository *snapshot.Repository) string {
	t.Helper()
	const name = "restorable"
	runtimeDir := filepath.Join(t.TempDir(), "seed-runtime")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opened, err := persist.OpenOrCreate(stateDir)
	require.NoError(t, err)
	coordinator := recovery.NewCoordinator(opened.Catalogue, repository, rand.Reader)
	listener, err := ipc.Listen(runtimeDir)
	require.NoError(t, err)
	d := daemon.New(
		pty.NewFactory(),
		clock.New(),
		discardLog(),
		daemon.WithShell("/bin/cat", nil),
		daemon.WithCatalogue(opened.Catalogue, opened.Records),
		daemon.WithSnapshotRepository(repository),
		daemon.WithRecoveryCoordinator(coordinator),
	)
	served := make(chan error, 1)
	go func() { served <- d.Serve(ctx, sessionwire.NewServerListener(listener)) }()

	tr, _ := attach(t, runtimeDir, protocol.IntentNew, name, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, tr.SendClient(protocol.Input{Data: []byte("checkpoint me\n")}))
	require.Eventually(t, func() bool {
		record, ok, _ := opened.Catalogue.Record(name)
		return ok && record.Committed != nil && record.DegradedReason == ""
	}, 5*time.Second, 10*time.Millisecond, "session never committed a checkpoint")
	require.NoError(t, tr.Close())

	cancel()
	require.NoError(t, awaitLifecycleResult(t, served, "seed daemon shutdown"))
	return name
}

type lifecycleBlockingRestore struct {
	ports.SnapshotRepository
	entered    chan struct{}
	release    chan struct{}
	returned   chan struct{}
	enterOnce  sync.Once
	returnOnce sync.Once
	mu         sync.Mutex
	repaired   string
}

func (r *lifecycleBlockingRestore) LoadCheckpoint(ctx context.Context, id domain.IncarnationID, name string, ref domain.CheckpointRef) (ports.SnapshotGeneration, error) {
	r.mu.Lock()
	r.repaired = name
	r.mu.Unlock()
	return r.SnapshotRepository.LoadCheckpoint(ctx, id, name, ref)
}

func (r *lifecycleBlockingRestore) ReconcileCheckpoint(ctx context.Context, id domain.IncarnationID, ref domain.CheckpointRef) error {
	r.enterOnce.Do(func() { close(r.entered) })
	<-r.release // Intentionally ignore cancellation: lifecycle ownership must outlive this call.
	r.returnOnce.Do(func() { close(r.returned) })
	return r.SnapshotRepository.ReconcileCheckpoint(ctx, id, ref)
}

func (r *lifecycleBlockingRestore) repairedName() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.repaired
}

type blockedSnapshotWriterShutdown struct {
	runtimeDir       string
	callbackReturned <-chan struct{}
	wrapperReturned  <-chan error
	publishReturned  <-chan struct{}
	releaseWriter    func()
}

func runBlockedSnapshotWriterShutdown(t *testing.T) blockedSnapshotWriterShutdown {
	t.Helper()
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	stateDir := filepath.Join(t.TempDir(), "state")
	shutdownClock := newLifecycleShutdownClock(t)
	repository := newLifecycleBlockingRepository()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	listenerReady := make(chan struct{})
	listenerClosed := make(chan struct{})
	callbackReturned := make(chan struct{})
	wrapperReturned := make(chan error, 1)
	go func() {
		wrapperReturned <- runWithLifecycleOwner(ctx, runtimeDir, stateDir, func(ctx context.Context) error {
			defer close(callbackReturned)
			listener, err := ipc.Listen(runtimeDir)
			if err != nil {
				return err
			}
			observed := &lifecycleObservedListener{Listener: listener, closed: listenerClosed}
			close(listenerReady)
			d := daemon.New(
				pty.NewFactory(),
				shutdownClock,
				discardLog(),
				daemon.WithShell("/bin/cat", nil),
				daemon.WithSnapshotRepository(repository),
			)
			return d.Serve(ctx, sessionwire.NewServerListener(observed))
		})
	}()

	awaitLifecycleStage(t, listenerReady, "daemon listener")
	tr, _ := attach(t, runtimeDir, protocol.IntentNew, "work", domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, tr.SendClient(protocol.Input{Data: []byte("dirty state\n")}))
	awaitLifecycleStage(t, repository.entered, "snapshot publication")
	require.NoError(t, tr.Close())
	require.Eventually(t, func() bool {
		sessions := listRemoteSessions(t, runtimeDir)
		return len(sessions.Sessions) == 1 && !sessions.Sessions[0].Attached
	}, time.Second, 10*time.Millisecond)
	cancel()
	awaitLifecycleStage(t, listenerClosed, "listener close")

	shutdownClock.nextTimer(t).fire()
	select {
	case <-callbackReturned:
		t.Fatal("Serve callback returned after checkpoint timeout while snapshot writer was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	assertLifecycleStagePending(t, callbackReturned, "Serve callback return after checkpoint timeout")
	assertLifecycleResultPending(t, wrapperReturned, "lifecycle wrapper return after checkpoint timeout")

	var releaseOnce sync.Once
	releaseWriter := func() { releaseOnce.Do(func() { close(repository.release) }) }
	t.Cleanup(releaseWriter)
	return blockedSnapshotWriterShutdown{
		runtimeDir:       runtimeDir,
		callbackReturned: callbackReturned,
		wrapperReturned:  wrapperReturned,
		publishReturned:  repository.returned,
		releaseWriter:    releaseWriter,
	}
}

type lifecycleBlockingRepository struct {
	ports.SnapshotRepository
	entered    chan struct{}
	release    chan struct{}
	returned   chan struct{}
	enterOnce  sync.Once
	returnOnce sync.Once
}

func newLifecycleBlockingRepository() *lifecycleBlockingRepository {
	return &lifecycleBlockingRepository{
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
}

func (r *lifecycleBlockingRepository) Publish(context.Context, ports.SnapshotPublication) error {
	r.enterOnce.Do(func() { close(r.entered) })
	<-r.release // Intentionally ignore cancellation: lifecycle ownership must outlive this call.
	r.returnOnce.Do(func() { close(r.returned) })
	return nil
}

type lifecycleShutdownClock struct {
	t      *testing.T
	base   ports.Clock
	timers chan *lifecycleManualTimer
}

func newLifecycleShutdownClock(t *testing.T) *lifecycleShutdownClock {
	t.Helper()
	return &lifecycleShutdownClock{t: t, base: clock.New(), timers: make(chan *lifecycleManualTimer, 4)}
}

func (c *lifecycleShutdownClock) Now() time.Time { return c.base.Now() }
func (c *lifecycleShutdownClock) NewTimer(delay time.Duration) ports.Timer {
	if delay != daemon.SnapshotShutdownTimeout() {
		return c.base.NewTimer(delay)
	}
	timer := &lifecycleManualTimer{t: c.t, ch: make(chan time.Time, 1)}
	select {
	case c.timers <- timer:
	default:
		c.t.Errorf("unexpected additional snapshot shutdown timer with delay %s", delay)
		timer.fire()
	}
	return timer
}

func (c *lifecycleShutdownClock) nextTimer(t *testing.T) *lifecycleManualTimer {
	t.Helper()
	select {
	case timer := <-c.timers:
		return timer
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for shutdown deadline timer")
		return nil
	}
}

type lifecycleManualTimer struct {
	t       *testing.T
	ch      chan time.Time
	stopped atomic.Bool
}

func (t *lifecycleManualTimer) C() <-chan time.Time { return t.ch }
func (t *lifecycleManualTimer) Reset(delay time.Duration) bool {
	if delay != daemon.SnapshotShutdownTimeout() {
		t.t.Errorf("snapshot shutdown timer reset with delay %s, want %s", delay, daemon.SnapshotShutdownTimeout())
	}
	t.stopped.Store(false)
	return false
}
func (t *lifecycleManualTimer) Stop() bool { return !t.stopped.Swap(true) }
func (t *lifecycleManualTimer) fire() {
	if t.stopped.Load() {
		return
	}
	select {
	case t.ch <- time.Now():
	default:
	}
}

func TestLifecycleManualTimerRepeatedFireIsNonBlocking(t *testing.T) {
	timer := &lifecycleManualTimer{t: t, ch: make(chan time.Time, 1)}
	timer.fire()

	returned := make(chan struct{})
	go func() {
		timer.fire()
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("repeated manual timer fire blocked on a pending tick")
	}
}

type lifecycleObservedListener struct {
	wire.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *lifecycleObservedListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

func TestLifecycleSocketCloseCatalogueRace(t *testing.T) {
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	stateDir := filepath.Join(t.TempDir(), "state")
	oldOwner, err := lifecycle.TryAcquire(runtimeDir)
	require.NoError(t, err)
	oldOpened, err := persist.OpenOrCreate(stateDir)
	require.NoError(t, err)
	oldListener, err := ipc.Listen(runtimeDir)
	require.NoError(t, err)

	socketCloseEntered := make(chan struct{})
	allowSocketClose := make(chan struct{})
	socketClosed := make(chan struct{})
	catalogueCloseEntered := make(chan struct{})
	allowCatalogueClose := make(chan struct{})
	catalogueClosed := make(chan struct{})
	ownerReleaseEntered := make(chan struct{})
	allowOwnerRelease := make(chan struct{})
	ownerReleased := make(chan struct{})
	teardownEvents := make(chan string, 3)

	controlledListener := &lifecycleControlledListener{
		Listener: oldListener,
		entered:  socketCloseEntered,
		proceed:  allowSocketClose,
		closed:   socketClosed,
		events:   teardownEvents,
	}
	controlledCatalogue := &lifecycleControlledCatalogue{
		Catalogue: oldOpened.Catalogue,
		entered:   catalogueCloseEntered,
		proceed:   allowCatalogueClose,
		closed:    catalogueClosed,
		events:    teardownEvents,
	}
	oldDaemon := daemon.New(pty.NewFactory(), clock.New(), discardLog(), daemon.WithCatalogue(controlledCatalogue, oldOpened.Records))
	oldCtx, stopOld := context.WithCancel(context.Background())
	oldDone := make(chan error, 1)
	go func() {
		serveErr := oldDaemon.Serve(oldCtx, sessionwire.NewServerListener(controlledListener))
		teardownEvents <- "owner-release"
		close(ownerReleaseEntered)
		<-allowOwnerRelease
		releaseErr := oldOwner.Release()
		close(ownerReleased)
		oldDone <- errors.Join(serveErr, releaseErr)
	}()

	newAcquireAttempted := make(chan struct{})
	newDurableOpen := make(chan struct{})
	newListen := make(chan struct{})
	startupEvents := make(chan string, 3)
	newDone := make(chan error, 1)
	go func() {
		newDone <- runWithLifecycleOwnerDeps(context.Background(), runtimeDir, stateDir, func(ctx context.Context) error {
			close(newDurableOpen)
			opened, err := persist.OpenOrCreate(stateDir)
			if err != nil {
				return err
			}
			startupEvents <- "durable-open"

			close(newListen)
			listener, err := ipc.Listen(runtimeDir)
			if err != nil {
				return errors.Join(err, opened.Catalogue.Close())
			}
			startupEvents <- "listen"
			return errors.Join(listener.Close(), opened.Catalogue.Close())
		}, lifecycleStartupDeps{
			ensurePrivate: func(string) error { return nil },
			acquire: func(ctx context.Context, dir string, retry time.Duration) (lifecycleOwnership, error) {
				close(newAcquireAttempted)
				owner, err := lifecycle.Acquire(ctx, dir, retry)
				if err != nil {
					return nil, err
				}
				<-ownerReleased
				startupEvents <- "owner-acquired"
				return owner, nil
			},
		})
	}()

	awaitLifecycleStage(t, newAcquireAttempted, "new lifecycle acquisition attempt")
	assertLifecycleStagePending(t, newDurableOpen, "new durable open before old teardown")
	assertLifecycleStagePending(t, newListen, "new listen before old teardown")

	stopOld()
	awaitLifecycleStage(t, socketCloseEntered, "old socket close")
	assertNewDaemonStartupPending(t, newDurableOpen, newListen, "socket close")
	close(allowSocketClose)
	awaitLifecycleStage(t, socketClosed, "old socket closed")

	awaitLifecycleStage(t, catalogueCloseEntered, "old catalogue close")
	assertNewDaemonStartupPending(t, newDurableOpen, newListen, "catalogue close")
	close(allowCatalogueClose)
	awaitLifecycleStage(t, catalogueClosed, "old catalogue closed")

	awaitLifecycleStage(t, ownerReleaseEntered, "old lifecycle owner release")
	assertNewDaemonStartupPending(t, newDurableOpen, newListen, "owner release")
	close(allowOwnerRelease)
	awaitLifecycleStage(t, ownerReleased, "old lifecycle owner released")

	require.NoError(t, awaitLifecycleResult(t, oldDone, "old daemon teardown"))
	require.NoError(t, awaitLifecycleResult(t, newDone, "new daemon startup"))
	require.Equal(t, []string{"socket-close", "catalogue-close", "owner-release"}, []string{
		<-teardownEvents,
		<-teardownEvents,
		<-teardownEvents,
	})
	require.Equal(t, []string{"owner-acquired", "durable-open", "listen"}, []string{
		<-startupEvents,
		<-startupEvents,
		<-startupEvents,
	})
}

func awaitLifecycleStage(t *testing.T, stage <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-stage:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func awaitLifecycleResult(t *testing.T, result <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return nil
	}
}

func assertLifecycleStagePending(t *testing.T, stage <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-stage:
		t.Fatalf("unexpected %s", name)
	default:
	}
}

func assertLifecycleResultPending(t *testing.T, result <-chan error, name string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("unexpected %s: %v", name, err)
	default:
	}
}

func assertNewDaemonStartupPending(t *testing.T, durableOpen, listen <-chan struct{}, heldAt string) {
	t.Helper()
	assertLifecycleStagePending(t, durableOpen, "new durable open while old daemon held at "+heldAt)
	assertLifecycleStagePending(t, listen, "new listen while old daemon held at "+heldAt)
}

type lifecycleControlledListener struct {
	wire.Listener
	entered chan struct{}
	proceed chan struct{}
	closed  chan struct{}
	events  chan<- string
	once    sync.Once
	err     error
}

func (l *lifecycleControlledListener) Close() error {
	l.once.Do(func() {
		l.events <- "socket-close"
		close(l.entered)
		<-l.proceed
		l.err = l.Listener.Close()
		close(l.closed)
	})
	return l.err
}

type lifecycleControlledCatalogue struct {
	ports.Catalogue
	entered chan struct{}
	proceed chan struct{}
	closed  chan struct{}
	events  chan<- string
	once    sync.Once
	err     error
}

func (c *lifecycleControlledCatalogue) Close() error {
	c.once.Do(func() {
		c.events <- "catalogue-close"
		close(c.entered)
		<-c.proceed
		c.err = c.Catalogue.Close()
		close(c.closed)
	})
	return c.err
}

type integrationTransport struct{}

func (*integrationTransport) Send(wire.Envelope) error     { return nil }
func (*integrationTransport) Recv() (wire.Envelope, error) { return wire.Envelope{}, io.EOF }
func (*integrationTransport) Close() error                 { return nil }
func (*integrationTransport) LocalAddr() net.Addr          { return nil }
func (*integrationTransport) RemoteAddr() net.Addr         { return nil }

// TestIntegration_KillAllPurgesSessionsAndKeepsDaemon is the P4.2 app-level
// contract: `kill --all` removes every live and stopped session but leaves the
// daemon serving, and the same process accepts fresh named and ephemeral
// sessions that start PTYs, forward input, and open a tab through the ordinary
// command palette. Only the distinct explicit daemon stop ends it.
func TestIntegration_KillAllPurgesSessionsAndKeepsDaemon(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	dir, served := startDaemon(t, daemon.WithShell("/bin/cat", nil))

	first, firstPump := attach(t, dir, protocol.IntentNew, "one", sz)
	require.NoError(t, first.SendClient(protocol.Input{Data: []byte("alpha\n")}))
	awaitText(t, firstPump, sz, "alpha")
	// A detached named session becomes stopped authority, so kill-all must purge
	// both the live and the stopped record in one operation.
	second, _ := attach(t, dir, protocol.IntentNew, "two", sz)
	require.NoError(t, second.Close())
	require.Len(t, listRemoteSessions(t, dir).Sessions, 2)

	require.NoError(t, killAll(dir))
	// The daemon survives the purge: Serve has not returned and the registry is
	// empty.
	select {
	case err := <-served:
		t.Fatalf("KillAll stopped Serve: %v", err)
	default:
	}
	require.Eventually(t, func() bool { return len(listRemoteSessions(t, dir).Sessions) == 0 },
		5*time.Second, 10*time.Millisecond, "kill-all did not purge every session")

	// Reuse: a fresh named session starts its PTY and forwards input.
	third, thirdPump := attach(t, dir, protocol.IntentNew, "again", sz)
	require.NoError(t, third.SendClient(protocol.Input{Data: []byte("beta\n")}))
	awaitText(t, thirdPump, sz, "beta")
	// A fresh ephemeral session is admitted too.
	fourth, fourthPump := attach(t, dir, protocol.IntentEphemeral, "", sz)
	require.NoError(t, fourth.SendClient(protocol.Input{Data: []byte("gamma\n")}))
	awaitText(t, fourthPump, sz, "gamma")

	// A tab opened after the purge is real, live topology.
	require.NoError(t, third.SendClient(protocol.Input{Data: []byte("\x1b ")}))
	awaitText(t, thirdPump, sz, "Commands")
	require.NoError(t, third.SendClient(protocol.Input{Data: []byte("CNT\r")}))
	require.Eventually(t, func() bool {
		for _, info := range listRemoteSessions(t, dir).Sessions {
			if info.Name == "again" {
				return info.Tabs == 2
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "a tab created after the purge was not published")

	_ = first.Close()
	require.NoError(t, third.Close())
	require.NoError(t, fourth.Close())

	// Only the distinct explicit daemon stop ends the daemon.
	require.NoError(t, killDaemon(dir))
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not stop after kill --daemon")
	}
}
