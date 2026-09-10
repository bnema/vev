package confirm

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestConfirmerAcceptsYes(t *testing.T) {
	var out bytes.Buffer
	ok, err := NewConfirmer(strings.NewReader("y\n"), &out).Confirm("Kill daemon?")
	if err != nil {
		t.Fatalf("Confirm returned error: %v", err)
	}
	if !ok {
		t.Fatal("Confirm returned false, want true")
	}
	if got := out.String(); !strings.Contains(got, "Kill daemon?") || !strings.Contains(got, "[y/N]") {
		t.Fatalf("prompt output = %q, want question and default", got)
	}
}

func TestConfirmerDefaultsNo(t *testing.T) {
	ok, err := NewConfirmer(strings.NewReader("\n"), &bytes.Buffer{}).Confirm("Kill daemon?")
	if err != nil {
		t.Fatalf("Confirm returned error: %v", err)
	}
	if ok {
		t.Fatal("Confirm returned true for empty answer, want false")
	}
}

func TestConfirmerDefaultsNoOnEOF(t *testing.T) {
	ok, err := NewConfirmer(strings.NewReader(""), &bytes.Buffer{}).Confirm("Kill daemon?")
	if err != nil {
		t.Fatalf("Confirm returned error: %v", err)
	}
	if ok {
		t.Fatal("Confirm returned true for EOF, want false")
	}
}

func TestConfirmerRejectsUnknownAnswer(t *testing.T) {
	ok, err := NewConfirmer(strings.NewReader("later\n"), &bytes.Buffer{}).Confirm("Kill daemon?")
	if err != nil {
		t.Fatalf("Confirm returned error: %v", err)
	}
	if ok {
		t.Fatal("Confirm returned true for unknown answer, want false")
	}
}

func TestConfirmerReleasesOnCancel(t *testing.T) {
	reader, writer := io.Pipe()
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wrote := make(chan struct{}, 1)
	out := writeFunc(func(p []byte) (int, error) {
		select {
		case wrote <- struct{}{}:
		default:
		}
		return io.Discard.Write(p)
	})
	done := make(chan error, 1)
	go func() {
		_, err := NewConfirmer(reader, out).ConfirmContext(ctx, "create?")
		done <- err
	}()
	// Wait until the prompt write lands before cancelling, so the test
	// exercises release from a blocked read rather than startup.
	select {
	case <-wrote:
	case <-time.After(5 * time.Second):
		t.Fatal("confirm did not write its prompt")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ConfirmContext error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("confirm did not release on context cancellation")
	}
}

type writeFunc func([]byte) (int, error)

func (f writeFunc) Write(p []byte) (int, error) { return f(p) }
