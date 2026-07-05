//go:build linux

package term

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// openPTYPair opens a real Linux PTY master/slave pair directly via
// /dev/ptmx, self-contained (no import of the vev pty adapter package,
// which is being built in parallel).
func openPTYPair(t *testing.T) (master, slave *os.File) {
	t.Helper()

	m, err := os.OpenFile("/dev/ptmx", os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatalf("open /dev/ptmx: %v", err)
	}

	if err := unix.IoctlSetPointerInt(int(m.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		_ = m.Close()
		t.Fatalf("unlock pty (TIOCSPTLCK): %v", err)
	}

	n, err := unix.IoctlGetInt(int(m.Fd()), unix.TIOCGPTN)
	if err != nil {
		_ = m.Close()
		t.Fatalf("get pty number (TIOCGPTN): %v", err)
	}

	slavePath := fmt.Sprintf("/dev/pts/%d", n)
	s, err := os.OpenFile(slavePath, os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		_ = m.Close()
		t.Fatalf("open slave %s: %v", slavePath, err)
	}

	return m, s
}

// TestTerminal_RealPTY_RawModeSizeAndEscapes is the ONE integration test
// exercising the real-tty paths (MakeRaw/Restore, GetSize) against an
// actual PTY, since those are pure-syscall lines that unit tests with
// pipes can't cover.
func TestTerminal_RealPTY_RawModeSizeAndEscapes(t *testing.T) {
	master, slave := openPTYPair(t)
	defer func() { _ = master.Close() }()
	defer func() { _ = slave.Close() }()

	ws := &unix.Winsize{Row: 24, Col: 80}
	if err := unix.IoctlSetWinsize(int(slave.Fd()), unix.TIOCSWINSZ, ws); err != nil {
		t.Fatalf("set winsize: %v", err)
	}

	// Drain everything written to the slave (escape sequences) so the
	// batched writer's Flush never blocks on the PTY's internal buffer.
	captured := make(chan []byte, 1)
	go func() {
		buf, _ := io.ReadAll(master)
		captured <- buf
	}()

	tm := NewWithFiles(slave, slave)

	sz, err := tm.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if sz.Cols != 80 || sz.Rows != 24 {
		t.Fatalf("Size = %+v, want {Cols:80 Rows:24}", sz)
	}

	restore, err := tm.EnterRaw()
	if err != nil {
		t.Fatalf("EnterRaw: %v", err)
	}
	if tm.rawSkipped {
		t.Fatalf("expected rawSkipped=false for a real PTY")
	}

	if err := restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	// Idempotent.
	if err := restore(); err != nil {
		t.Fatalf("second restore: %v", err)
	}

	// Closing the slave lets the reader goroutine observe EOF on the
	// master naturally. Don't also close master here: that would race
	// the reader's in-flight Read against this goroutine's Close on the
	// same fd and could discard already-buffered-but-unread data.
	if err := slave.Close(); err != nil {
		t.Fatalf("close slave: %v", err)
	}

	select {
	case got := <-captured:
		want := altScreenEnter + cursorHide + mouseEnable + bracketedPasteEnable + colorSchemeEnable + oscColorQuery + cursorShow + cursorStyleDefault + mouseDisable + bracketedPasteDisable + colorSchemeDisable + altScreenExit
		if string(got) != want {
			t.Fatalf("captured escapes = %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for captured PTY output")
	}
}
