package daemon

import (
	"fmt"
	"os/exec"
	"strconv"
	"testing"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/stretchr/testify/require"
)

// ncurses xterm-direct reserves 0..7 for ANSI colors and encodes larger
// indices as packed RGB, with an empty colorspace field in colon-form SGR.
func TestXtermDirectColorContract(t *testing.T) {
	for _, value := range []int{0, 1, 7, 8, 0x123456, 0xffffff} {
		t.Run(strconv.Itoa(value), func(t *testing.T) {
			fg, bg := xtermDirectSetters(value)
			assertXtermDirectColors(t, fg+bg, value)
		})
	}
}

func TestInstalledXtermDirectColorContract(t *testing.T) {
	if _, err := exec.LookPath("tput"); err != nil {
		t.Skip("tput is not installed")
	}
	colors, err := exec.CommandContext(t.Context(), "tput", "-T", "xterm-direct", "colors").Output()
	if err != nil {
		t.Skip("xterm-direct is not installed")
	}
	require.Equal(t, "16777216\n", string(colors))
	for _, value := range []int{0, 7, 8, 0x123456, 0xffffff} {
		t.Run(strconv.Itoa(value), func(t *testing.T) {
			fg, err := exec.CommandContext(t.Context(), "tput", "-T", "xterm-direct", "setaf", strconv.Itoa(value)).Output()
			require.NoError(t, err)
			bg, err := exec.CommandContext(t.Context(), "tput", "-T", "xterm-direct", "setab", strconv.Itoa(value)).Output()
			require.NoError(t, err)
			assertXtermDirectColors(t, string(fg)+string(bg), value)
		})
	}
}

func xtermDirectSetters(value int) (string, string) {
	if value < 8 {
		return fmt.Sprintf("\x1b[%dm", 30+value), fmt.Sprintf("\x1b[%dm", 40+value)
	}
	components := fmt.Sprintf("2::%d:%d:%d", value>>16, value>>8&255, value&255)
	return "\x1b[38:" + components + "m", "\x1b[48:" + components + "m"
}

func assertXtermDirectColors(t *testing.T, setters string, value int) {
	t.Helper()
	screen := vt.NewScreen(3, 1)
	screen.Write([]byte(setters + "X\x1b[39;49mY"))
	cell := screen.Cell(0, 0)
	if value < 8 {
		require.False(t, cell.Style.HasForegroundRGB)
		require.False(t, cell.Style.HasBackgroundRGB)
		require.EqualValues(t, value, cell.Style.Foreground)
		require.EqualValues(t, value, cell.Style.Background)
	} else {
		want := renderer.RGB{R: uint8(value >> 16), G: uint8(value >> 8), B: uint8(value)}
		require.True(t, cell.Style.HasForegroundRGB)
		require.True(t, cell.Style.HasBackgroundRGB)
		require.Equal(t, want, cell.Style.ForegroundRGB)
		require.Equal(t, want, cell.Style.BackgroundRGB)
	}
	reset := screen.Cell(1, 0).Style
	require.False(t, reset.HasForegroundRGB)
	require.False(t, reset.HasBackgroundRGB)
	require.EqualValues(t, -1, reset.Foreground)
	require.EqualValues(t, -1, reset.Background)

	// Exercise the actual attachment output path, not just the VT parser.
	frame := renderer.NewFrame(3, 1)
	frame.Set(0, 0, cell)
	frame.Set(1, 0, screen.Cell(1, 0))
	prepared, err := newOutputStateStream().prepare(frame, nil, true)
	require.NoError(t, err)
	display := vt.NewScreen(3, 1)
	display.Write(prepared.data)
	require.Equal(t, cell, display.Cell(0, 0))
}
