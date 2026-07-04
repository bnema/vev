// Package term adapts vev's ports to terminal I/O implementations.
package term

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"golang.org/x/term"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Compile-time check that Terminal satisfies ports.Terminal.
var _ ports.Terminal = (*Terminal)(nil)

// Escape sequences for alt-screen and cursor visibility control. All
// emissions go through the batched writer and are explicitly flushed.
const (
	altScreenEnter        = "\x1b[?1049h"
	altScreenExit         = "\x1b[?1049l"
	cursorHide            = "\x1b[?25l"
	cursorShow            = "\x1b[?25h"
	cursorStyleDefault    = "\x1b[0 q"
	mouseEnable           = "\x1b[?1002h\x1b[?1006h"
	mouseDisable          = "\x1b[?1002l\x1b[?1006l"
	bracketedPasteEnable  = "\x1b[?2004h"
	bracketedPasteDisable = "\x1b[?2004l"
	oscColorQuery         = "\x1b]10;?\x07\x1b]11;?\x07"
)

// bufSize is the batched writer's buffer capacity.
const bufSize = 64 * 1024

// Terminal implements ports.Terminal for the client-side controlling
// terminal: raw mode, alt-screen, SIGWINCH-driven resize events, and a
// batched stdout writer.
//
// Zero value is not usable; use New or NewWithFiles.
type Terminal struct {
	in  *os.File
	out *os.File
	fd  int
	bw  *batchWriter

	mu         sync.Mutex
	orig       *term.State  // saved terminal state; nil when raw mode wasn't entered (or was restored)
	entered    bool         // true between a successful EnterRaw and its restore
	rawSkipped bool         // true if fd wasn't a tty, so MakeRaw/Restore were skipped
	restoreFn  func() error // the single idempotent restore closure for the current session

	// Resize watcher state, also guarded by mu so watcher start
	// (ResizeEvents) and watcher stop (restore) are strictly ordered:
	// a restore that runs before the first ResizeEvents call must
	// prevent any watcher (and its signal.Notify) from ever starting.
	resizeCh   chan domain.Size
	resizeQuit chan struct{}
	sigCh      chan os.Signal
	resizeDone bool // set by stopResizeLocked; no watcher may start afterwards
	resizeWG   sync.WaitGroup
}

// New creates a Terminal backed by os.Stdin and os.Stdout.
func New() *Terminal {
	return NewWithFiles(os.Stdin, os.Stdout)
}

// NewWithFiles creates a Terminal backed by the given tty files. It
// exists so tests can inject a PTY pair (or a plain pipe, for the
// non-tty paths) in place of the process's real stdin/stdout.
func NewWithFiles(in, out *os.File) *Terminal {
	return &Terminal{
		in:  in,
		out: out,
		fd:  int(in.Fd()),
		bw:  newBatchWriter(out, bufSize),
	}
}

// EnterRaw puts the terminal into raw mode (when the input fd is
// actually a tty; otherwise the raw-mode syscalls are skipped and
// recorded, not treated as failure), enters the alt screen, and hides
// the cursor. The returned restore func idempotently reverses all of
// this; it is safe to call multiple times.
func (t *Terminal) EnterRaw() (func() error, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.entered {
		return t.restoreFn, nil
	}

	var old *term.State
	if term.IsTerminal(t.fd) {
		s, err := term.MakeRaw(t.fd)
		if err != nil {
			return nil, fmt.Errorf("term: make raw: %w", err)
		}
		old = s
		t.rawSkipped = false
	} else {
		t.rawSkipped = true
	}
	t.orig = old

	if _, err := t.bw.WriteString(altScreenEnter + cursorHide + mouseEnable + bracketedPasteEnable + oscColorQuery); err != nil {
		_ = t.restoreRawLocked()
		return nil, fmt.Errorf("term: enter alt screen: %w", err)
	}
	if err := t.bw.Flush(); err != nil {
		_ = t.restoreRawLocked()
		return nil, fmt.Errorf("term: enter alt screen: %w", err)
	}

	t.entered = true
	t.restoreFn = t.makeRestoreLocked()
	return t.restoreFn, nil
}

// makeRestoreLocked returns an idempotent restore closure for the
// current EnterRaw "session". Must be called with t.mu held.
func (t *Terminal) makeRestoreLocked() func() error {
	var once sync.Once
	var err error
	return func() error {
		once.Do(func() {
			err = t.restore()
		})
		return err
	}
}

// restore reverses EnterRaw: it shows the cursor, exits the alt screen,
// restores the original terminal mode (if raw mode was actually
// entered), and stops the resize watcher, closing its channel.
func (t *Terminal) restore() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, werr := t.bw.WriteString(cursorShow + cursorStyleDefault + mouseDisable + bracketedPasteDisable + altScreenExit)
	ferr := t.bw.Flush()

	rerr := t.restoreRawLocked()

	t.entered = false
	t.restoreFn = nil

	t.stopResizeLocked()

	switch {
	case werr != nil:
		return fmt.Errorf("term: restore: write: %w", werr)
	case ferr != nil:
		return fmt.Errorf("term: restore: flush: %w", ferr)
	case rerr != nil:
		return fmt.Errorf("term: restore: %w", rerr)
	default:
		return nil
	}
}

// restoreRawLocked restores the original terminal mode if it was saved.
// Must be called with t.mu held.
func (t *Terminal) restoreRawLocked() error {
	if t.orig == nil {
		return nil
	}
	err := term.Restore(t.fd, t.orig)
	t.orig = nil
	return err
}

// Size returns the current terminal dimensions.
func (t *Terminal) Size() (domain.Size, error) {
	cols, rows, err := term.GetSize(t.fd)
	if err != nil {
		return domain.Size{}, fmt.Errorf("term: get size: %w", err)
	}
	return domain.Size{Cols: cols, Rows: rows}, nil
}

// ResizeEvents returns a channel of coalesced terminal sizes, one per
// SIGWINCH burst. The SIGWINCH watcher starts lazily on the first call;
// subsequent calls return the same channel. The channel is closed once
// restore (returned by EnterRaw) runs. If restore has already run when
// ResizeEvents is first called, no watcher is started and the returned
// channel is already closed.
func (t *Terminal) ResizeEvents() <-chan domain.Size {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.resizeCh != nil {
		return t.resizeCh
	}
	t.resizeCh = make(chan domain.Size)

	if t.resizeDone {
		// restore ran before the first ResizeEvents call: never start a
		// watcher (or a signal handler) after stop; hand back a closed
		// channel so a consumer selecting on it doesn't hang.
		close(t.resizeCh)
		return t.resizeCh
	}

	t.resizeQuit = make(chan struct{})
	t.sigCh = make(chan os.Signal, 1)
	signal.Notify(t.sigCh, syscall.SIGWINCH)

	t.resizeWG.Go(func() {
		defer signal.Stop(t.sigCh)
		resizeLoop(t.sigCh, t.resizeCh, t.resizeQuit, t.Size)
	})

	return t.resizeCh
}

// stopResizeLocked stops the SIGWINCH watcher goroutine (if one was
// started) and waits for it to exit, so its channel is guaranteed
// closed before stopResizeLocked returns. It marks the watcher as done
// so no watcher can start afterwards. Must be called with t.mu held;
// the watcher goroutine never takes t.mu, so waiting under the lock
// cannot deadlock.
func (t *Terminal) stopResizeLocked() {
	if !t.resizeDone {
		t.resizeDone = true
		if t.resizeQuit != nil {
			close(t.resizeQuit)
		}
	}
	t.resizeWG.Wait()
}

// In returns the reader for the controlling terminal's input.
func (t *Terminal) In() io.Reader {
	return t.in
}

// Out returns the batched writer for the controlling terminal's output.
// Call Flush to push buffered bytes to the underlying tty.
func (t *Terminal) Out() io.Writer {
	return t.bw
}

// Flush writes any buffered output to the terminal device.
func (t *Terminal) Flush() error {
	return t.bw.Flush()
}
