package client

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

type kittyProbeTerminal struct {
	in  io.Reader
	out bytes.Buffer
}

func (t *kittyProbeTerminal) EnterRaw() (func() error, error) {
	return func() error { return nil }, nil
}
func (t *kittyProbeTerminal) Geometry() (domain.Geometry, error) {
	return domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil
}
func (*kittyProbeTerminal) ResizeEvents() <-chan domain.Geometry { return nil }
func (t *kittyProbeTerminal) In() io.Reader                      { return t.in }
func (t *kittyProbeTerminal) Out() io.Writer                     { return &t.out }
func (*kittyProbeTerminal) Flush() error                         { return nil }

type kittyProbeTimer struct{ *time.Timer }

func (t kittyProbeTimer) C() <-chan time.Time        { return t.Timer.C }
func (t kittyProbeTimer) Reset(d time.Duration) bool { return t.Timer.Reset(d) }
func (t kittyProbeTimer) Stop() bool                 { return t.Timer.Stop() }

type kittyProbeClock struct{}

func (kittyProbeClock) Now() time.Time { return time.Now() }
func (kittyProbeClock) NewTimer(d time.Duration) ports.Timer {
	return kittyProbeTimer{Timer: time.NewTimer(d)}
}

func TestKittyProbeUsesOneInputPumpAndReplaysUnrelatedInput(t *testing.T) {
	t.Setenv("TERM", "xterm-kitty")
	t.Setenv("KITTY_WINDOW_ID", "1")
	input := newTerminalInputPump(strings.NewReader("typed\x1b[?1;2c\x1b_Gi=31;OK\x1b\\"))
	input.start()
	defer input.stop()
	term := &kittyProbeTerminal{in: input.in}
	runner := &Runner{term: term, clock: kittyProbeClock{}, probeCapabilities: true}

	require.True(t, runner.probeKittyDirectGraphics(context.Background(), input))
	require.Equal(t, "\x1b_Gi=31,s=1,v=1,a=q;\x1b\\\x1b[c", term.out.String())

	consumer := input.claim()
	defer input.revoke(consumer)
	result, ok := input.take(context.Background(), consumer)
	require.True(t, ok)
	require.Equal(t, []byte("typed"), result.data)
	input.ack(consumer)
}
