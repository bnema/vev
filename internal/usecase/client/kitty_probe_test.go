package client

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev-vt/protocol/terminalquery"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/require"
)

type kittyProbeTimer struct{ *time.Timer }

func (t kittyProbeTimer) C() <-chan time.Time        { return t.Timer.C }
func (t kittyProbeTimer) Reset(d time.Duration) bool { return t.Timer.Reset(d) }
func (t kittyProbeTimer) Stop() bool                 { return t.Timer.Stop() }

type kittyProbeClock struct{}

func (kittyProbeClock) Now() time.Time { return time.Now() }
func (kittyProbeClock) NewTimer(d time.Duration) ports.Timer {
	return kittyProbeTimer{Timer: time.NewTimer(d)}
}

func TestKittyProbeEnabledIsTerminalAgnostic(t *testing.T) {
	for _, term := range []string{"", "arbitrary", "unknown-terminal"} {
		t.Run(term, func(t *testing.T) {
			t.Setenv("TERM", term)
			runner := &Runner{probeCapabilities: true}
			require.True(t, runner.kittyProbeEnabled())
		})
	}
}

func TestKittyProbeUsesOneInputPumpAndReplaysUnrelatedInput(t *testing.T) {
	term := portsmocks.NewMockTerminal(t)
	term.EXPECT().In().Return(strings.NewReader("typed\x1b_Gi=31;OK\x1b\\\x1b[?1;2c")).Once()
	input := newTerminalInputPump(term.In())
	input.start()
	defer input.stop()
	var out bytes.Buffer
	term.EXPECT().Out().Return(&out).Once()
	term.EXPECT().Flush().Return(nil).Once()
	runner := &Runner{term: term, clock: kittyProbeClock{}, probeCapabilities: true}

	require.True(t, runner.probeKittyDirectGraphics(context.Background(), input))
	require.Equal(t, terminalquery.KittyGraphicsQuery+terminalquery.DeviceAttributesQuery, out.String())

	consumer := input.claim()
	defer input.revoke(consumer)
	result, ok := input.take(context.Background(), consumer)
	require.True(t, ok)
	require.Equal(t, []byte("typed"), result.data)
	input.ack(consumer)
}
