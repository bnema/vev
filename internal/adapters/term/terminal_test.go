package term

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"
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
	defer inW.Close()
	defer inR.Close()

	outR, outW, captured, done := pipeCapture(t)
	defer outR.Close()

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

	outW.Close()
	<-done

	want := altScreenEnter + cursorHide + cursorShow + altScreenExit
	if got := captured.String(); got != want {
		t.Fatalf("captured escapes = %q, want %q", got, want)
	}
}

func TestTerminal_EnterRaw_IsIdempotentAcrossCalls(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(in): %v", err)
	}
	defer inW.Close()
	defer inR.Close()

	outR, outW, captured, done := pipeCapture(t)
	defer outR.Close()

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

	outW.Close()
	<-done

	// Alt-screen/cursor escapes must appear exactly once for enter and
	// once for exit, regardless of how many times EnterRaw/restore were
	// called.
	want := altScreenEnter + cursorHide + cursorShow + altScreenExit
	if got := captured.String(); got != want {
		t.Fatalf("captured escapes = %q, want %q", got, want)
	}
}

func TestTerminal_InOut(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(in): %v", err)
	}
	defer inW.Close()
	defer inR.Close()

	outR, outW, captured, done := pipeCapture(t)
	defer outR.Close()

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

	outW.Close()
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
	defer inW.Close()
	defer inR.Close()

	outR, outW, _, done := pipeCapture(t)
	defer outR.Close()

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

	outW.Close()
	<-done
}

func TestTerminal_Size_NonTTY_ReturnsError(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe(in): %v", err)
	}
	defer inW.Close()
	defer inR.Close()

	outR, outW, _, _ := pipeCapture(t)
	defer outR.Close()
	defer outW.Close()

	tm := NewWithFiles(inR, outW)

	if _, err := tm.Size(); err == nil {
		t.Fatalf("expected Size() to error on a non-tty fd")
	}
}
