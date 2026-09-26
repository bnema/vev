package client

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/bnema/vev-vt/protocol/terminalquery"
	"github.com/stretchr/testify/require"

	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestProbeTerminalCapabilities(t *testing.T) {
	const query = terminalquery.KittyGraphicsQuery + terminalquery.KittyKeyboardQuery + terminalquery.DeviceAttributesQuery
	tests := []struct {
		name     string
		reply    string
		timeout  bool
		want     terminalCapabilities
		wantRest string
	}{
		{name: "graphics and keyboard", reply: "\x1b_Gi=31;OK\x1b\\\x1b[?0u\x1b[?62;c", want: terminalCapabilities{KittyGraphics: true, KittyKeyboard: true}},
		{name: "keyboard only", reply: "\x1b[?0u\x1b[?62;22c", want: terminalCapabilities{KittyKeyboard: true}},
		{name: "neither", reply: "\x1b[?62;22c"},
		{name: "typed input is kept in order", reply: "ab\x1b[?1u\x1b[?62;22ccd", want: terminalCapabilities{KittyKeyboard: true}, wantRest: "abcd"},
		{name: "no DA1 means nothing", reply: "\x1b[?0u", timeout: true, wantRest: ""},
		{name: "silent terminal times out", timeout: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inR, inW := io.Pipe()
			defer func() { _ = inW.Close() }()
			pump := newTerminalInputPump(inR)
			pump.start()
			defer pump.stop()

			var out bytes.Buffer
			term := portsmocks.NewMockTerminal(t)
			term.EXPECT().Out().Return(&out).Once()
			written := make(chan struct{})
			term.EXPECT().Flush().RunAndReturn(func() error {
				go func() {
					defer close(written)
					if tt.reply != "" {
						_, _ = inW.Write([]byte(tt.reply))
					}
				}()
				return nil
			}).Once()
			clk := &pcFakeClock{}

			got := make(chan terminalCapabilities, 1)
			go func() { got <- probeTerminalCapabilities(context.Background(), term, clk, pump) }()
			if tt.timeout {
				require.Eventually(t, func() bool {
					clk.mu.Lock()
					defer clk.mu.Unlock()
					return len(clk.timers) == 1
				}, timeoutForTest, pollForTest)
				// A pipe write returns once the pump read it, so a partial
				// reply is in before the deadline fires.
				<-written
				require.Eventually(t, func() bool { return pumpDrained(pump) }, timeoutForTest, pollForTest)
				clk.fireLast()
			}
			caps := <-got
			require.Equal(t, query, out.String())
			require.Equal(t, tt.want, caps)

			consumer, ok := pump.tryClaim()
			require.True(t, ok)
			defer pump.revoke(consumer)
			if tt.wantRest == "" {
				_, ok := pump.take(context.Background(), consumer)
				require.False(t, ok, "no byte may be left for the next consumer")
				return
			}
			result, ok := pump.take(context.Background(), consumer)
			require.True(t, ok)
			require.Equal(t, tt.wantRest, string(result.data))
		})
	}
}

const (
	timeoutForTest = 2 * time.Second
	pollForTest    = time.Millisecond
)

// pumpDrained reports that the probe consumed every read the pump delivered.
func pumpDrained(p *terminalInputPump) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pending == nil && p.delivering == 0
}
