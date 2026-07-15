package dgram

import (
	"errors"
	"testing"
	"time"
)

func TestBytePacerUsesBurstThenFakeClockRate(t *testing.T) {
	const mtu = 1200
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	c := newCongestionController(mtu)
	p := bytePacer{clk: clk}
	done := make(chan struct{})
	limits := func() (int, int) { return c.bytesPerSecond(), c.burstBytes() }

	for range dataBurstPackets {
		if err := p.wait(done, mtu, limits); err != nil {
			t.Fatal(err)
		}
	}
	wait := make(chan error, 1)
	go func() { wait <- p.wait(done, mtu, limits) }()
	awaitSignal(t, clk.timerCreated, "byte pacer timer")
	clk.advance(24 * time.Millisecond)
	select {
	case err := <-wait:
		t.Fatalf("third MTU released early: %v", err)
	default:
	}
	clk.advance(time.Millisecond)
	if err := awaitResult(t, wait, "third paced MTU"); err != nil {
		t.Fatal(err)
	}

	c.onLoss()
	next := make(chan error, 1)
	go func() { next <- p.wait(done, mtu, limits) }()
	awaitSignal(t, clk.timerCreated, "reduced-cwnd byte pacer timer")
	clk.advance(49 * time.Millisecond)
	select {
	case err := <-next:
		t.Fatalf("reduced cwnd did not lengthen wait: %v", err)
	default:
	}
	clk.advance(time.Millisecond)
	if err := awaitResult(t, next, "reduced-cwnd paced MTU"); err != nil {
		t.Fatal(err)
	}
}

func TestBytePacerMaximumRateRefillsAfterMultiSecondFakeClockJump(t *testing.T) {
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	p := bytePacer{clk: clk}
	done := make(chan struct{})
	burst := int(^uint(0) >> 1)
	limits := func() (int, int) { return burst, burst }

	if err := p.wait(done, burst, limits); err != nil {
		t.Fatal(err)
	}
	clk.advance(3 * time.Second)
	if err := p.wait(done, burst, limits); err != nil {
		t.Fatal(err)
	}

	wait := make(chan error, 1)
	go func() { wait <- p.wait(done, burst, limits) }()
	awaitSignal(t, clk.timerCreated, "maximum-rate byte pacer timer")
	clk.advance(time.Second - time.Nanosecond)
	select {
	case err := <-wait:
		t.Fatalf("maximum-rate burst released early: %v", err)
	default:
	}
	clk.advance(time.Nanosecond)
	if err := awaitResult(t, wait, "maximum-rate burst"); err != nil {
		t.Fatal(err)
	}
}

func TestBytePacerCancellation(t *testing.T) {
	const mtu = 1200
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	p := bytePacer{clk: clk}
	done := make(chan struct{})
	limits := func() (int, int) { return mtu, mtu }
	if err := p.wait(done, mtu, limits); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- p.wait(done, mtu, limits) }()
	awaitSignal(t, clk.timerCreated, "cancelled byte pacer timer")
	close(done)
	if err := awaitResult(t, wait, "byte pacer cancellation"); !errors.Is(err, errPacerClosed) {
		t.Fatalf("cancellation error=%v, want %v", err, errPacerClosed)
	}
}
