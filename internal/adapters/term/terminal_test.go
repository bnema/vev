package term

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
)

// pipePair returns an os.Pipe and a goroutine that continuously copies
// everything written to the write end into captured, closing done once
// the write end is closed (EOF).
func pipeCapture(t *testing.T) (r, w *os.File, captured *bytes.Buffer, done chan struct{}) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	captured = &bytes.Buffer{}
	done = make(chan struct{})
	go func() {
		_, _ = io.Copy(captured, r)
		close(done)
	}()
	return r, w, captured, done
}

func TestTerminal_EnterRaw_NonTTY_EmitsAltScreenAndCursorEscapes(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(in): %v", err)
	}
	defer func() { _ = inW.Close() }()
	defer func() { _ = inR.Close() }()

	outR, outW, captured, done := pipeCapture(t)
	defer func() { _ = outR.Close() }()

	tm := NewWithFiles(inR, outW)

	restore, err := tm.EnterRaw()
	if err != nil {
		t.Fatalf("EnterRaw: %v", err)
	}
	if !tm.rawSkipped {
		t.Fatalf("expected rawSkipped=true for a non-tty fd")
	}

	if err := restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	// Idempotent: calling again must not error or duplicate output.
	if err := restore(); err != nil {
		t.Fatalf("second restore call: %v", err)
	}

	_ = outW.Close()
	<-done

	want := altScreenEnter + autowrapDisable + cursorHide + mouseEnable + bracketedPasteEnable + colorSchemeEnable + cursorShow + cursorStyleDefault + mouseDisable + bracketedPasteDisable + colorSchemeDisable + autowrapEnable + altScreenExit
	if got := captured.String(); got != want {
		t.Fatalf("captured escapes = %q, want %q", got, want)
	}
	if got := captured.String(); bytes.Contains([]byte(got), []byte("\x1b]10;?")) || bytes.Contains([]byte(got), []byte("\x1b]4;")) {
		t.Fatalf("EnterRaw emitted a color query: %q", got)
	}
}

// A frame the daemon composed for a wider terminal must never bleed onto the
// next row: with DECAWM left on, an over-long styled run (the full-width tab
// bar) wraps and paints a second row in the accent colour. vev positions every
// row explicitly and never relies on the terminal wrapping for it, so autowrap
// stays off for as long as vev owns the alt screen.
func TestTerminal_EnterRaw_DisablesAutowrapInsideAltScreen(t *testing.T) {
	const (
		autowrapOff = "\x1b[?7l"
		autowrapOn  = "\x1b[?7h"
		altEnter    = "\x1b[?1049h"
		altExit     = "\x1b[?1049l"
	)

	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(in): %v", err)
	}
	defer func() { _ = inW.Close() }()
	defer func() { _ = inR.Close() }()

	outR, outW, captured, done := pipeCapture(t)
	defer func() { _ = outR.Close() }()

	tm := NewWithFiles(inR, outW)

	restore, err := tm.EnterRaw()
	if err != nil {
		t.Fatalf("EnterRaw: %v", err)
	}
	if err := restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}

	_ = outW.Close()
	<-done
	got := captured.String()

	for _, tc := range []struct {
		name string
		seq  string
	}{
		{"autowrap disabled exactly once", autowrapOff},
		{"autowrap restored exactly once", autowrapOn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if n := strings.Count(got, tc.seq); n != 1 {
				t.Fatalf("emitted %q %d times, want exactly 1; full output %q", tc.seq, n, got)
			}
		})
	}

	// Ordering: disabling autowrap must land inside the alt screen, and it
	// must be restored before vev hands the screen back to the shell.
	for _, tc := range []struct {
		name   string
		before string
		after  string
	}{
		{"alt screen entered before autowrap is disabled", altEnter, autowrapOff},
		{"autowrap disabled before it is restored", autowrapOff, autowrapOn},
		{"autowrap restored before leaving the alt screen", autowrapOn, altExit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i, j := strings.Index(got, tc.before), strings.Index(got, tc.after)
			if i < 0 || j < 0 {
				t.Fatalf("missing sequence: %q at %d, %q at %d; full output %q", tc.before, i, tc.after, j, got)
			}
			if i >= j {
				t.Fatalf("%q (index %d) must precede %q (index %d); full output %q", tc.before, i, tc.after, j, got)
			}
		})
	}
}

// prefixThenFailWriter forwards at most prefix bytes to sink and then reports
// a failure, standing in for a tty that accepts part of a mode sequence and
// then stops. A prefix of 0 fails without emitting anything.
type prefixThenFailWriter struct {
	sink   io.Writer
	prefix int
}

func (w *prefixThenFailWriter) Write(p []byte) (int, error) {
	if len(p) > w.prefix {
		p = p[:w.prefix]
	}
	n := 0
	if len(p) > 0 {
		var err error
		n, err = w.sink.Write(p)
		if err != nil {
			return n, err
		}
	}
	w.prefix -= n
	return n, errors.New("sink failed after prefix")
}

// When the batched writer fails, part of the mode sequence may already have
// reached the terminal, so the visual modes must be reset straight to the fd
// rather than through the writer that just failed. Poisoning tm.bw while
// leaving tm.out intact isolates that fallback: the buffered path fails, the
// real descriptor still records what the fallback attempted.
func TestTerminal_WriteFailure_AttemptsVisualResetOnFd(t *testing.T) {
	// Every mode EnterRaw can turn on has to be turned back off, not just the
	// alt screen: a partial emission that got as far as cursorHide/mouseEnable
	// would otherwise leave the cursor hidden and mouse reporting on.
	const wantCleanup = cursorShow + cursorStyleDefault + mouseDisable +
		bracketedPasteDisable + colorSchemeDisable + autowrapEnable + altScreenExit
	const enterSequence = altScreenEnter + autowrapDisable + cursorHide +
		mouseEnable + bracketedPasteEnable + colorSchemeEnable

	for _, tc := range []struct {
		name        string
		prefix      int  // bytes the sink accepts before failing
		poisonAfter bool // poison the writer after EnterRaw, i.e. fail during restore
		want        string
	}{
		{
			name: "enter fails before emitting anything",
			want: wantCleanup,
		},
		{
			name:   "enter fails after emitting part of the sequence",
			prefix: len(altScreenEnter + autowrapDisable + cursorHide),
			want:   altScreenEnter + autowrapDisable + cursorHide + wantCleanup,
		},
		{
			name:        "restore fails after a successful enter",
			poisonAfter: true,
			want:        enterSequence + wantCleanup,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inR, inW, err := os.Pipe()
			if err != nil {
				t.Fatalf("os.Pipe(in): %v", err)
			}
			defer func() { _ = inW.Close() }()
			defer func() { _ = inR.Close() }()

			outR, outW, captured, done := pipeCapture(t)
			defer func() { _ = outR.Close() }()

			tm := NewWithFiles(inR, outW)
			poison := func() {
				tm.bw = newBatchWriter(&prefixThenFailWriter{sink: outW, prefix: tc.prefix}, bufSize)
			}

			if !tc.poisonAfter {
				poison()
				if _, err := tm.EnterRaw(); err == nil {
					t.Fatalf("EnterRaw returned nil error on a failing sink")
				}
			} else {
				restore, err := tm.EnterRaw()
				if err != nil {
					t.Fatalf("EnterRaw: %v", err)
				}
				poison()
				if err := restore(); err == nil {
					t.Fatalf("restore returned nil error on a failing sink")
				}
			}

			_ = outW.Close()
			<-done

			if got := captured.String(); got != tc.want {
				t.Fatalf("bytes written to the fd = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTerminal_EnterRaw_IsIdempotentAcrossCalls(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(in): %v", err)
	}
	defer func() { _ = inW.Close() }()
	defer func() { _ = inR.Close() }()

	outR, outW, captured, done := pipeCapture(t)
	defer func() { _ = outR.Close() }()

	tm := NewWithFiles(inR, outW)

	restore1, err := tm.EnterRaw()
	if err != nil {
		t.Fatalf("EnterRaw #1: %v", err)
	}
	restore2, err := tm.EnterRaw()
	if err != nil {
		t.Fatalf("EnterRaw #2: %v", err)
	}

	if err := restore1(); err != nil {
		t.Fatalf("restore1: %v", err)
	}
	if err := restore2(); err != nil {
		t.Fatalf("restore2: %v", err)
	}

	_ = outW.Close()
	<-done

	// Alt-screen/cursor escapes must appear exactly once for enter and exits
	// exactly once, regardless of how many times EnterRaw/restore were called.
	want := altScreenEnter + autowrapDisable + cursorHide + mouseEnable + bracketedPasteEnable + colorSchemeEnable + cursorShow + cursorStyleDefault + mouseDisable + bracketedPasteDisable + colorSchemeDisable + autowrapEnable + altScreenExit
	if got := captured.String(); got != want {
		t.Fatalf("captured escapes = %q, want %q", got, want)
	}
}

func TestTerminal_InOut(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(in): %v", err)
	}
	defer func() { _ = inW.Close() }()
	defer func() { _ = inR.Close() }()

	outR, outW, captured, done := pipeCapture(t)
	defer func() { _ = outR.Close() }()

	tm := NewWithFiles(inR, outW)

	if tm.In() == nil {
		t.Fatalf("In() = nil")
	}

	if _, err := tm.Out().Write([]byte("payload")); err != nil {
		t.Fatalf("Out().Write: %v", err)
	}
	if err := tm.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	_ = outW.Close()
	<-done

	if got := captured.String(); got != "payload" {
		t.Fatalf("captured = %q, want %q", got, "payload")
	}
}

func TestTerminal_ResizeEvents_ReturnsSameChannelAndClosesOnRestore(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(in): %v", err)
	}
	defer func() { _ = inW.Close() }()
	defer func() { _ = inR.Close() }()

	outR, outW, _, done := pipeCapture(t)
	defer func() { _ = outR.Close() }()

	tm := NewWithFiles(inR, outW)

	restore, err := tm.EnterRaw()
	if err != nil {
		t.Fatalf("EnterRaw: %v", err)
	}

	ch1 := tm.ResizeEvents()
	ch2 := tm.ResizeEvents()
	if ch1 != ch2 {
		t.Fatalf("ResizeEvents() returned different channels across calls")
	}

	if err := restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}

	select {
	case _, ok := <-ch1:
		if ok {
			t.Fatalf("expected resize channel to be closed after restore")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("resize channel was not closed after restore")
	}

	_ = outW.Close()
	<-done
}

func TestTerminal_ResizeEvents_AfterRestore_NoWatcherAndClosedChannel(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(in): %v", err)
	}
	defer func() { _ = inW.Close() }()
	defer func() { _ = inR.Close() }()

	outR, outW, _, done := pipeCapture(t)
	defer func() { _ = outR.Close() }()

	tm := NewWithFiles(inR, outW)

	restore, err := tm.EnterRaw()
	if err != nil {
		t.Fatalf("EnterRaw: %v", err)
	}
	// restore BEFORE the first ResizeEvents call (signal-driven shutdown
	// racing ahead of the resize pump's startup).
	if err := restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}

	ch := tm.ResizeEvents()

	// The channel must already be closed so a consumer selecting on it
	// doesn't hang.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("expected closed resize channel, got a value")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("resize channel not closed after restore-before-ResizeEvents")
	}

	// No watcher may have started: no signal handler installed, no quit
	// channel allocated, no goroutine registered.
	tm.mu.Lock()
	sigCh, quit := tm.sigCh, tm.resizeQuit
	tm.mu.Unlock()
	if sigCh != nil {
		t.Fatalf("expected no signal channel (no signal.Notify) after restore-first ordering")
	}
	if quit != nil {
		t.Fatalf("expected no quit channel (no watcher goroutine) after restore-first ordering")
	}
	// Returns immediately iff no watcher goroutine was leaked.
	tm.resizeWG.Wait()

	_ = outW.Close()
	<-done
}

func TestTerminal_ResizeEvents_ConcurrentWithRestore(t *testing.T) {
	// Race ResizeEvents against a restore triggered from another
	// goroutine (the signal-driven shutdown shape). Under -race this
	// locks in that watcher start/stop are ordered by the mutex; in all
	// interleavings the returned channel must eventually be closed and
	// no watcher goroutine may leak.
	for i := range 50 {
		inR, inW, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe(in): %v", err)
		}
		outR, outW, _, done := pipeCapture(t)

		tm := NewWithFiles(inR, outW)
		restore, err := tm.EnterRaw()
		if err != nil {
			t.Fatalf("EnterRaw: %v", err)
		}

		start := make(chan struct{})
		chch := make(chan (<-chan domain.Geometry), 1)
		go func() {
			<-start
			chch <- tm.ResizeEvents()
		}()
		go func() {
			<-start
			_ = restore()
		}()
		close(start)

		ch := <-chch
		// restore may still be in flight; it is idempotent and returns
		// only after the watcher (if any) has fully stopped.
		if err := restore(); err != nil {
			t.Fatalf("iteration %d: restore: %v", i, err)
		}

		select {
		case _, ok := <-ch:
			if ok {
				// A real size emission is impossible here (pipe, not a
				// tty, and no SIGWINCH sent) — anything but a close is a
				// bug.
				t.Fatalf("iteration %d: expected closed channel, got a value", i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: resize channel never closed", i)
		}
		tm.resizeWG.Wait()

		_ = outW.Close()
		<-done
		_ = inR.Close()
		_ = inW.Close()
		_ = outR.Close()
	}
}

func TestTerminal_Size_NonTTY_ReturnsError(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(in): %v", err)
	}
	defer func() { _ = inW.Close() }()
	defer func() { _ = inR.Close() }()

	outR, outW, _, _ := pipeCapture(t)
	defer func() { _ = outR.Close() }()
	defer func() { _ = outW.Close() }()

	tm := NewWithFiles(inR, outW)

	if _, err := tm.Geometry(); err == nil {
		t.Fatalf("expected Geometry() to error on a non-tty fd")
	}
}
