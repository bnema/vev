package sshstdio

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// TestBuildCommandForMuxIsStdioOnlyAndTrustPreserving proves the documented mux
// argv requests no PTY while leaving host-key, authentication, and proxy policy
// entirely to the target's effective OpenSSH configuration.
func TestBuildCommandForMuxIsStdioOnlyAndTrustPreserving(t *testing.T) {
	spec := BuildCommandForMux("user@host; touch /tmp/pwn", "vev", "_mux", "--flag")
	require.Equal(t, "ssh", spec.Path)
	require.Equal(t, []string{"-T", "--", "user@host; touch /tmp/pwn", "'vev' '_mux' '--flag'"}, spec.Args)

	flat := strings.Join(spec.Args, " ")
	require.Contains(t, flat, "-T")
	for _, forbidden := range []string{"StrictHostKeyChecking", "UserKnownHostsFile", "BatchMode", "ProxyCommand"} {
		require.NotContains(t, flat, forbidden)
	}
}

func TestDialMuxContextRejectsEmptyCommandSpec(t *testing.T) {
	transport, err := DialMuxContext(context.Background(), CommandSpec{}, nil)
	require.ErrorIs(t, err, ErrMuxConfig)
	require.Nil(t, transport)
}

// TestDialMuxContextCanceledBeforeStart proves one canceled setup context is
// checked before the subprocess starts: nothing is executed and the cancellation
// remains recognizable.
func TestDialMuxContextCanceledBeforeStart(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "started")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	transport, err := DialMuxContext(ctx, CommandSpec{Path: "sh", Args: []string{"-c", "touch " + marker + "; sleep 30"}}, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, transport)
	_, statErr := os.Stat(marker)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

// TestDialMuxContextBoundedSanitizedStderr proves a non-clean mux child never
// leaks raw remote stderr into the public error: the captured text is bounded
// and sanitized for the logger, and the returned error carries only the typed
// outcome.
func TestDialMuxContextBoundedSanitizedStderr(t *testing.T) {
	const marker = "remote-secret-value"
	// A terminal-colored diagnostic, and a long tail the ceiling must drop.
	script := "printf '\\033[31m" + marker + "\\033[0m' >&2; printf 'A%.0s' $(seq 1 20000) >&2; exit 3"

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
	transport, err := DialMuxContext(context.Background(), CommandSpec{Path: "sh", Args: []string{"-c", script}}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = transport.Close() })

	_, recvErr := transport.RecvBounded(wire.PreambleLimit)
	require.ErrorIs(t, recvErr, ErrMuxSSHExit)
	require.NotContains(t, recvErr.Error(), marker)
	require.NotContains(t, recvErr.Error(), "\x1b")
	require.Less(t, len(recvErr.Error()), muxDiagnosticLimit)

	logged := logBuf.String()
	require.Contains(t, logged, "ssh exited non-cleanly")
	require.NotContains(t, logged, `\u001b`)
	require.Less(t, len(logged), muxStderrLimit+2048, "log must stay bounded")
}

// TestCappedDiagnosticBoundsAndSanitizes proves the diagnostic capture both
// caps its retained bytes and strips terminal escapes and control bytes.
func TestCappedDiagnosticBoundsAndSanitizes(t *testing.T) {
	sink := newCappedDiagnostic(16)
	input := []byte("\x1b[31mok\x1b[0m\x00" + strings.Repeat("z", 100))
	written, err := sink.Write(input)
	require.NoError(t, err)
	require.Equal(t, len(input), written)
	require.True(t, sink.overflow)
	require.LessOrEqual(t, sink.buf.Len(), 16)
	require.Equal(t, "okz", sink.String()[:3])
	require.NotContains(t, sink.String(), "\x1b")
	require.NotContains(t, sink.String(), "\x00")
	require.LessOrEqual(t, len(sink.String()), muxDiagnosticLimit)
}

// TestDialMuxContextCloseTerminatesProcess proves Close owns the subprocess
// lifetime: it stops a child that ignores its stdin and returns without
// abandoning the wait.
func TestDialMuxContextCloseTerminatesProcess(t *testing.T) {
	transport, err := DialMuxContext(context.Background(), CommandSpec{Path: "sh", Args: []string{"-c", "sleep 30"}}, nil)
	require.NoError(t, err)

	started := time.Now()
	closeErr := transport.Close()
	require.Error(t, closeErr, "a killed child is a non-clean exit")
	require.Less(t, time.Since(started), sshCloseTimeout+2*time.Second)

	// A second Close is idempotent and joins the same reaped process.
	require.Error(t, transport.Close())
	<-time.After(50 * time.Millisecond)
}
