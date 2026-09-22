package client

import (
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

type attachPaletteClock struct {
	timers          chan *attachPaletteTimer
	handshakeTimers chan *attachPaletteTimer
}

type attachPaletteTimer struct {
	ch       chan time.Time
	stopped  chan struct{}
	stopOnce sync.Once
	duration time.Duration
}

func newAttachPaletteClock() *attachPaletteClock {
	return &attachPaletteClock{
		timers:          make(chan *attachPaletteTimer, 16),
		handshakeTimers: make(chan *attachPaletteTimer, 1),
	}
}

func (*attachPaletteClock) Now() time.Time { return time.Time{} }
func (c *attachPaletteClock) NewTimer(delay time.Duration) ports.Timer {
	timer := &attachPaletteTimer{ch: make(chan time.Time, 1), stopped: make(chan struct{}), duration: delay}
	if delay == protocol.HandshakeTimeout {
		select {
		case c.handshakeTimers <- timer:
		default:
			panic("unexpected extra handshake timer")
		}
	} else {
		c.timers <- timer
	}
	return timer
}
func (t *attachPaletteTimer) C() <-chan time.Time    { return t.ch }
func (*attachPaletteTimer) Reset(time.Duration) bool { return false }
func (t *attachPaletteTimer) Stop() bool {
	t.stopOnce.Do(func() { close(t.stopped) })
	return true
}
func (t *attachPaletteTimer) fire() { t.ch <- time.Time{} }

func requireTimer(t *testing.T, timers <-chan *attachPaletteTimer, message string) *attachPaletteTimer {
	t.Helper()
	select {
	case timer := <-timers:
		return timer
	case <-time.After(time.Second):
		t.Fatal(message)
		return nil
	}
}

func requireSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}
