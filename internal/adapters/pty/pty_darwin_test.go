//go:build darwin

package pty_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func readAllDarwin(t *testing.T, p ports.PTY, deadline time.Duration) []byte {
	t.Helper()
	type result struct {
		output []byte
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		var output bytes.Buffer
		_, err := io.Copy(&output, p)
		resultCh <- result{output: output.Bytes(), err: err}
	}()

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		return result.output
	case <-time.After(deadline):
		t.Fatal("timed out reading from pty")
		return nil
	}
}

func TestOpen_DarwinEchoAndOutputEOF(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pty integration test in -short mode")
	}

	factory := pty.NewFactory()
	outputPTY, err := factory.Open(context.Background(), "sh", []string{"-c", "printf hello"}, os.Environ(), "", domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = outputPTY.Close() })
	require.Equal(t, "hello", string(readAllDarwin(t, outputPTY, 5*time.Second)))

	echoPTY, err := factory.Open(context.Background(), "cat", nil, os.Environ(), "", domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = echoPTY.Close() })
	_, err = io.WriteString(echoPTY, "roundtrip\n")
	require.NoError(t, err)

	output := readUntilDarwin(t, echoPTY, "roundtrip", 5*time.Second)
	require.Contains(t, output, "roundtrip")
}

func readUntilDarwin(t *testing.T, p ports.PTY, marker string, deadline time.Duration) string {
	t.Helper()
	type result struct {
		output string
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		var output strings.Builder
		buf := make([]byte, 256)
		for !strings.Contains(output.String(), marker) {
			n, err := p.Read(buf)
			output.Write(buf[:n])
			if err != nil {
				resultCh <- result{output: output.String(), err: err}
				return
			}
		}
		resultCh <- result{output: output.String()}
	}()

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		return result.output
	case <-time.After(deadline):
		t.Fatal("timed out waiting for pty readiness")
		return ""
	}
}

func TestOpen_DarwinResizeAndForegroundPgid(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pty integration test in -short mode")
	}

	const ready = "__vev_ready__"
	p, err := pty.NewFactory().Open(context.Background(), "sh", []string{"-c", "stty -echo; printf '__vev_ready__\\n'; IFS= read -r release; stty size"}, os.Environ(), "", domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	require.Contains(t, readUntilDarwin(t, p, ready, 5*time.Second), ready)
	pgid, err := p.ForegroundPgid()
	require.NoError(t, err)
	require.Greater(t, pgid, 0)
	require.NoError(t, p.Resize(domain.Geometry{Size: domain.Size{Cols: 120, Rows: 40}}))
	_, err = io.WriteString(p, "release\n")
	require.NoError(t, err)
	require.Equal(t, "40 120", strings.TrimSpace(string(readAllDarwin(t, p, 5*time.Second))))
}

func TestOpen_DarwinContextCancellationEndsStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pty integration test in -short mode")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := pty.NewFactory().Open(ctx, "sh", []string{"-c", "exec sleep 60"}, os.Environ(), "", domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	streamEnded := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, p)
		streamEnded <- err
	}()

	cancel()
	select {
	case err := <-streamEnded:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("pty stream did not end after context cancellation")
	}
}
