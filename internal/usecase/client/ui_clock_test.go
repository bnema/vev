package client

import (
	"sync"
	"testing"
	"time"

	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

type attachPaletteClock struct {
	port            *portsmocks.MockClock
	owner           *testing.T
	timers          chan *attachPaletteTimer
	handshakeTimers chan *attachPaletteTimer
}

type attachPaletteTimer struct {
	ch       chan time.Time
	stopped  chan struct{}
	stopOnce sync.Once
	duration time.Duration
}

func newAttachPaletteClock(t *testing.T) *attachPaletteClock {
	c := &attachPaletteClock{
		port: portsmocks.NewMockClock(t), owner: t,
		timers:          make(chan *attachPaletteTimer, 16),
		handshakeTimers: make(chan *attachPaletteTimer, 1),
	}
	c.port.EXPECT().Now().Return(time.Time{}).Maybe()
	c.port.EXPECT().NewTimer(mock.Anything).RunAndReturn(c.newTimer).Maybe()
	return c
}

func (c *attachPaletteClock) newTimer(delay time.Duration) ports.Timer {
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
	m := &portsmocks.MockTimer{}
	m.Mock.Test(c.owner)
	m.EXPECT().C().Return((<-chan time.Time)(timer.ch)).Maybe()
	m.EXPECT().Reset(mock.Anything).Return(false).Maybe()
	m.EXPECT().Stop().RunAndReturn(timer.stop).Maybe()
	return m
}
func (t *attachPaletteTimer) stop() bool {
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
