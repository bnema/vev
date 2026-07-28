package daemon

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

func TestSnatchedPanelFrame(t *testing.T) {
	styles := themeui.Styles{
		PickerBase:      renderer.Style{Foreground: 1},
		PickerSelection: renderer.Style{Foreground: 2},
		BorderMuted:     renderer.Style{Foreground: 3},
	}
	tests := []struct {
		name       string
		size       domain.Size
		feedback   string
		wantSize   domain.Size
		want       []string
		wantBounds domain.Rect
		compact    bool
	}{
		{
			name:       "centered bordered panel",
			size:       domain.Size{Cols: 80, Rows: 24},
			wantSize:   domain.Size{Cols: 80, Rows: 24},
			want:       []string{"Session snatched", "This session is now active elsewhere.", "r  Resume here", "q / Esc  Quit"},
			wantBounds: domain.Rect{X: 19, Y: 8, Width: 42, Height: 7},
		},
		{
			name:     "compact fallback",
			size:     domain.Size{Cols: 24, Rows: 4},
			wantSize: domain.Size{Cols: 24, Rows: 4},
			want:     []string{"Session snatched", "r Resume", "q Quit"},
			compact:  true,
		},
		{
			name:     "compact unavailable feedback",
			size:     domain.Size{Cols: 24, Rows: 4},
			feedback: "Session is no longer available.",
			wantSize: domain.Size{Cols: 24, Rows: 4},
			want:     []string{"Session unavailable", "r Resume", "q Quit"},
			compact:  true,
		},
		{
			name:       "bordered unavailable feedback",
			size:       domain.Size{Cols: 80, Rows: 24},
			feedback:   "Session is no longer available.",
			wantSize:   domain.Size{Cols: 80, Rows: 24},
			want:       []string{"Session snatched", "Session is no longer available.", "r  Resume here", "q / Esc  Quit"},
			wantBounds: domain.Rect{X: 19, Y: 8, Width: 42, Height: 7},
		},
		{
			name:     "zero dimensions clamp",
			size:     domain.Size{},
			wantSize: domain.Size{Cols: 1, Rows: 1},
			compact:  true,
		},
		{
			name:     "negative dimensions clamp",
			size:     domain.Size{Cols: -10, Rows: -20},
			wantSize: domain.Size{Cols: 1, Rows: 1},
			compact:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := snatchedPanelFrame(tt.size, styles, tt.feedback)
			require.Equal(t, tt.wantSize.Cols, frame.Width)
			require.Equal(t, tt.wantSize.Rows, frame.Height)
			require.NoError(t, frame.Validate())

			text := snatchedFrameText(frame)
			for _, want := range tt.want {
				require.Contains(t, text, want)
			}

			if tt.compact {
				require.NotContains(t, text, "┌")
				require.NotContains(t, text, "┘")
				return
			}

			require.Equal(t, tt.wantBounds, snatchedModal.Bounds(tt.wantSize))
			topLeft := frame.At(tt.wantBounds.X, tt.wantBounds.Y)
			bottomRight := frame.At(tt.wantBounds.X+tt.wantBounds.Width-1, tt.wantBounds.Y+tt.wantBounds.Height-1)
			require.Equal(t, '┌', topLeft.Rune)
			require.Equal(t, '┘', bottomRight.Rune)
			require.True(t, topLeft.Style.Equal(styles.BorderMuted), "panel border must use captured chrome style")
			require.True(t, bottomRight.Style.Equal(styles.BorderMuted), "panel border must use captured chrome style")
			requireFreshSnatchedBackground(t, frame, tt.wantBounds, styles.PickerBase)
		})
	}
}

func TestSendSnatchedPanelRebasesOutput(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	tr := &closeTrackingTransport{}
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.output.next = 7
	ac.setAppliedTheme(appliedTheme{Resolved: themeui.Resolve(themeui.Theme{}, domain.ThemeAccent{Mode: domain.ThemeAccentAuto})})
	ac.roleGeneration.Store(1)

	expected := ac.transportSnapshot()
	require.NoError(t, d.sendSnatchedPanel(ac, expected, 1, ""))

	frames := tr.Sends()
	require.Len(t, frames, 1)
	require.Equal(t, ports.MsgOutput, frames[0].Type)
	out, err := ports.UnmarshalOutput(frames[0].Payload)
	require.NoError(t, err)
	require.Zero(t, out.BaseStateNum)
	require.Equal(t, uint64(8), out.NewStateNum)
	require.Contains(t, string(out.Data), "Session snatched")
	require.True(t, strings.HasSuffix(string(out.Data), "\x1b[?25l"), "panel must force the cursor hidden")
}

func TestSendSnatchedPanelTimeoutClosesCapturedTransportOnly(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clock)
	old := newBlockingSnatchedTransport()
	fresh := &closeTrackingTransport{}
	ac := &attachedClient{tr: old, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.roleGeneration.Store(1)
	expected := ac.transportSnapshot()

	result := make(chan error, 1)
	go func() {
		result <- d.sendSnatchedPanel(ac, expected, 1, "")
	}()

	timer := awaitTestValue(t, clock.timers, "snatched send did not install its timeout")
	awaitTestCompletion(t, old.sendEntered, "snatched send did not block in the captured transport")
	ac.replaceTransport(fresh)
	timer.ch <- time.Time{}

	require.ErrorIs(t, awaitTestValue(t, result, "snatched send did not time out"), errSendTimedOut)
	defer func() { _ = old.Close() }() // Release the blocked worker if the assertion below fails.
	require.True(t, old.Closed(), "timed-out captured transport was not closed")
	require.False(t, fresh.Closed(), "replacement transport must remain open")

	sendMuReleased := make(chan uint64, 1)
	go func() {
		ac.sendMu.Lock()
		generation := ac.roleGeneration.Load()
		ac.sendMu.Unlock()
		sendMuReleased <- generation
	}()
	require.Equal(t, uint64(1), awaitTestValue(t, sendMuReleased, "closing the timed-out transport did not release sendMu"))
}

type blockingSnatchedTransport struct {
	sendEntered chan struct{}
	closed      chan struct{}
	sendOnce    sync.Once
	closeOnce   sync.Once
}

func newBlockingSnatchedTransport() *blockingSnatchedTransport {
	return &blockingSnatchedTransport{
		sendEntered: make(chan struct{}),
		closed:      make(chan struct{}),
	}
}

func (t *blockingSnatchedTransport) Send(ports.Frame) error {
	t.sendOnce.Do(func() { close(t.sendEntered) })
	<-t.closed
	return errors.New("transport closed")
}

func (*blockingSnatchedTransport) Recv() (ports.Frame, error) {
	return ports.Frame{}, errors.New("not implemented")
}

func (t *blockingSnatchedTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func (t *blockingSnatchedTransport) Closed() bool {
	select {
	case <-t.closed:
		return true
	default:
		return false
	}
}

func TestSendSnatchedPanelRejectsStaleTransport(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	old := &closeTrackingTransport{}
	fresh := &closeTrackingTransport{}
	ac := &attachedClient{tr: old, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.roleGeneration.Store(1)
	stale := ac.transportSnapshot()
	ac.replaceTransport(fresh)

	require.ErrorIs(t, d.sendSnatchedPanel(ac, stale, 1, ""), errTransportReplaced)
	require.Empty(t, old.Sends())
	require.Empty(t, fresh.Sends())
}

func TestSendSnatchedPanelRejectsStaleRole(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	tr := &closeTrackingTransport{}
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.roleGeneration.Store(2)
	expected := ac.transportSnapshot()

	require.ErrorIs(t, d.sendSnatchedPanel(ac, expected, 1, ""), errSnatchedOutputStale)
	require.Empty(t, tr.Sends())
}

func requireFreshSnatchedBackground(t *testing.T, frame renderer.Frame, panel domain.Rect, style renderer.Style) {
	t.Helper()
	for y := range frame.Height {
		for x := range frame.Width {
			if x >= panel.X && x < panel.X+panel.Width && y >= panel.Y && y < panel.Y+panel.Height {
				continue
			}
			cell := frame.At(x, y)
			require.Equal(t, ' ', cell.Rune, "cell (%d,%d) outside panel retained content", x, y)
			require.True(t, cell.Style.Equal(style), "cell (%d,%d) outside panel did not use captured base style", x, y)
		}
	}
}

func snatchedFrameText(frame renderer.Frame) string {
	var out strings.Builder
	for y := 0; y < frame.Height; y++ {
		for _, cell := range frame.Row(y) {
			if cell.Continuation {
				continue
			}
			r := cell.Rune
			if r == 0 {
				r = ' '
			}
			out.WriteRune(r)
		}
		out.WriteByte('\n')
	}
	return out.String()
}
