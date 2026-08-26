package term

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
)

func TestResizeLoop_CoalescesBurstToOneEmission(t *testing.T) {
	// Fill the signal channel with a burst before the loop starts
	// consuming: draining is synchronous and deterministic here since
	// nothing else sends concurrently.
	sig := make(chan os.Signal, 8)
	for range 5 {
		sig <- syscall.SIGWINCH
	}

	out := make(chan domain.Geometry)
	quit := make(chan struct{})

	var calls int
	getGeometry := func() (domain.Geometry, error) {
		calls++
		return domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480}, nil
	}

	go resizeLoop(sig, out, quit, getGeometry)

	got := <-out
	want := domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480}
	if got != want {
		t.Fatalf("emitted size = %+v, want %+v", got, want)
	}

	select {
	case sz2, ok := <-out:
		t.Fatalf("unexpected second emission (ok=%v): %+v", ok, sz2)
	case <-time.After(50 * time.Millisecond):
	}

	if calls != 1 {
		t.Fatalf("getSize called %d times, want 1 (burst should coalesce)", calls)
	}

	close(quit)
	if _, ok := <-out; ok {
		t.Fatalf("expected out to be closed after quit")
	}
}

func TestResizeLoop_GetSizeErrorSkipsEmission(t *testing.T) {
	sig := make(chan os.Signal) // unbuffered: sends rendezvous with the loop's receive
	out := make(chan domain.Geometry)
	quit := make(chan struct{})

	var calls int
	firstGetSize := make(chan struct{})
	getGeometry := func() (domain.Geometry, error) {
		calls++
		if calls == 1 {
			close(firstGetSize)
			return domain.Geometry{}, errors.New("boom")
		}
		return domain.Geometry{Size: domain.Size{Cols: 10, Rows: 5}}, nil
	}

	go resizeLoop(sig, out, quit, getGeometry)

	sig <- syscall.SIGWINCH // errors; no emission
	<-firstGetSize          // wait until the failed signal has been handled
	sig <- syscall.SIGWINCH // succeeds

	got := <-out
	want := domain.Geometry{Size: domain.Size{Cols: 10, Rows: 5}}
	if got != want {
		t.Fatalf("emitted size = %+v, want %+v", got, want)
	}
	if calls != 2 {
		t.Fatalf("getSize called %d times, want 2", calls)
	}

	close(quit)
	if _, ok := <-out; ok {
		t.Fatalf("expected out to be closed after quit")
	}
}

func TestResizeLoop_QuitClosesOutImmediately(t *testing.T) {
	sig := make(chan os.Signal)
	out := make(chan domain.Geometry)
	quit := make(chan struct{})

	getGeometry := func() (domain.Geometry, error) {
		t.Fatalf("getGeometry should not be called")
		return domain.Geometry{}, nil
	}

	go resizeLoop(sig, out, quit, getGeometry)
	close(quit)

	select {
	case _, ok := <-out:
		if ok {
			t.Fatalf("expected out to be closed, got a value")
		}
	case <-time.After(time.Second):
		t.Fatalf("out was not closed after quit")
	}
}

func TestDrainSignals_RemovesQueuedSignalsOnly(t *testing.T) {
	sig := make(chan os.Signal, 4)
	sig <- syscall.SIGWINCH
	sig <- syscall.SIGWINCH
	sig <- syscall.SIGWINCH

	drainSignals(sig)

	select {
	case s := <-sig:
		t.Fatalf("expected sig to be empty after drain, got %v", s)
	default:
	}
}
