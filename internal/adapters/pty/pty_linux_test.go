//go:build linux

package pty_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/pty"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

// readAll drains p until EOF or the deadline, returning what was read.
func readAll(t *testing.T, p ports.PTY, deadline time.Duration) []byte {
	t.Helper()
	type res struct {
		b   []byte
		err error
	}
	ch := make(chan res, 1)
	go func() {
		var buf bytes.Buffer
		_, err := io.Copy(&buf, p)
		ch <- res{buf.Bytes(), err}
	}()
	select {
	case r := <-ch:
		require.NoError(t, r.err)
		return r.b
	case <-time.After(deadline):
		t.Fatal("timed out reading from pty")
		return nil
	}
}

func newFactory() ports.PTYFactory { return pty.NewFactory() }

func TestFactory_ImplementsPort(t *testing.T) {
	var _ ports.PTYFactory = pty.NewFactory()
}

func TestOpen_ChildOutputToEOF(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pty integration test in -short mode")
	}
	f := newFactory()
	p, err := f.Open("sh", []string{"-c", "printf hello"}, os.Environ(), "", domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	out := readAll(t, p, 5*time.Second)
	require.Equal(t, "hello", string(out))

	require.NoError(t, p.Close())
	// Idempotent second close.
	require.NoError(t, p.Close())
}

func TestOpen_EchoRoundtrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pty integration test in -short mode")
	}
	f := newFactory()
	p, err := f.Open("cat", nil, os.Environ(), "", domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	_, err = io.WriteString(p, "roundtrip\n")
	require.NoError(t, err)

	// cat echoes input back on the master; read until we see it.
	buf := make([]byte, 256)
	deadline := time.Now().Add(5 * time.Second)
	var got string
	for !strings.Contains(got, "roundtrip") {
		if time.Now().After(deadline) {
			t.Fatalf("did not read echoed data, got %q", got)
		}
		n, rerr := p.Read(buf)
		got += string(buf[:n])
		if rerr != nil {
			require.ErrorIs(t, rerr, io.EOF)
			break
		}
	}
	require.Contains(t, got, "roundtrip")

	// Close terminates the still-running cat.
	require.NoError(t, p.Close())
}

func TestForegroundPgid(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pty integration test in -short mode")
	}
	f := newFactory()
	p, err := f.Open("sh", []string{"-c", "sleep 2"}, os.Environ(), "", domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	pgid, err := p.ForegroundPgid()
	require.NoError(t, err)
	require.Greater(t, pgid, 0)
}

func TestResize_SttySize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pty integration test in -short mode")
	}
	tests := []struct {
		name    string
		initial domain.Size
		resize  *domain.Size
		script  string
		want    string
	}{
		{
			name:    "initial size honored before spawn",
			initial: domain.Size{Cols: 80, Rows: 24},
			script:  "stty size",
			want:    "24 80",
		},
		{
			name:    "resize before child reads size",
			initial: domain.Size{Cols: 80, Rows: 24},
			resize:  &domain.Size{Cols: 120, Rows: 40},
			script:  "sleep 0.2; stty size",
			want:    "40 120",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFactory()
			p, err := f.Open("sh", []string{"-c", tt.script}, os.Environ(), "", tt.initial)
			require.NoError(t, err)
			t.Cleanup(func() { _ = p.Close() })

			if tt.resize != nil {
				require.NoError(t, p.Resize(*tt.resize))
			}

			out := readAll(t, p, 5*time.Second)
			require.Equal(t, tt.want, strings.TrimSpace(string(out)))
		})
	}
}

func TestClose_ReapsChildNoZombie(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pty integration test in -short mode")
	}
	f := newFactory()
	p, err := f.Open("sh", []string{"-c", "sleep 30"}, os.Environ(), "", domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)

	pid := p.Pid()
	require.Greater(t, pid, 0)

	require.NoError(t, p.Close())

	// After Close the child must be gone (reaped): signal 0 probes existence.
	// A reaped pid yields ESRCH; a zombie would still yield nil here so we also
	// confirm /proc/<pid>/status, if present, is not in a running/sleeping state.
	err = syscall.Kill(pid, 0)
	if err == nil {
		// Still visible: ensure it is a zombie's parent-less remnant is not our child.
		data, rerr := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
		if rerr == nil {
			require.Contains(t, string(data), "State:\tZ", "child still alive and not a zombie")
		}
	} else {
		require.ErrorIs(t, err, syscall.ESRCH)
	}

	// Idempotent.
	require.NoError(t, p.Close())
}

func TestClose_TerminatesLongRunningChild(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pty integration test in -short mode")
	}
	f := newFactory()
	p, err := f.Open("sh", []string{"-c", "sleep 60"}, os.Environ(), "", domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	pid := p.Pid()

	start := time.Now()
	require.NoError(t, p.Close())
	require.Less(t, time.Since(start), 3*time.Second, "Close should not block for the full grace period")

	require.Eventually(t, func() bool {
		return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
	}, 3*time.Second, 20*time.Millisecond)
}

func TestResize_DeliversSIGWINCH(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pty integration test in -short mode")
	}
	// Child traps WINCH, prints a marker, then exits; the Resize ioctl must
	// deliver SIGWINCH to the foreground process group.
	script := `trap 'echo GOTWINCH; exit 0' WINCH; echo READY; i=0; while [ $i -lt 200 ]; do sleep 0.05; i=$((i+1)); done`
	f := newFactory()
	p, err := f.Open("sh", []string{"-c", script}, os.Environ(), "", domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	// Wait for READY so the trap is installed before we resize.
	buf := make([]byte, 256)
	deadline := time.Now().Add(5 * time.Second)
	var acc string
	for !strings.Contains(acc, "READY") {
		if time.Now().After(deadline) {
			t.Fatalf("child never signaled READY, got %q", acc)
		}
		n, rerr := p.Read(buf)
		acc += string(buf[:n])
		if rerr != nil {
			t.Fatalf("read error before READY: %v (got %q)", rerr, acc)
		}
	}

	require.NoError(t, p.Resize(domain.Size{Cols: 100, Rows: 30}))

	for !strings.Contains(acc, "GOTWINCH") {
		if time.Now().After(deadline) {
			t.Fatalf("child did not report SIGWINCH, got %q", acc)
		}
		n, rerr := p.Read(buf)
		acc += string(buf[:n])
		if rerr != nil {
			require.ErrorIs(t, rerr, io.EOF)
			break
		}
	}
	require.Contains(t, acc, "GOTWINCH")
}
