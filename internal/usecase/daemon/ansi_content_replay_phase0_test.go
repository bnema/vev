package daemon

import (
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

// phase0ReplayCapture is deliberately test-only. It models the proposed
// remote-content boundary: ANSI is consumed by a private VT screen, then the
// owned visible grid and cursor are captured for local composition.
type phase0ReplayCapture struct {
	frame  renderer.Frame
	cursor vt.CursorSnapshot
}

func capturePhase0Replay(screen *vt.Screen) phase0ReplayCapture {
	snapshot := screen.Snapshot()
	frame := renderer.NewFrame(snapshot.Columns(), snapshot.Rows())
	for y := range snapshot.Rows() {
		copy(frame.Row(y), snapshot.Row(y))
	}
	return phase0ReplayCapture{frame: frame, cursor: snapshot.Cursor()}
}

func composePhase0Replay(capture phase0ReplayCapture) renderer.Frame {
	// Composition receives cells, not the remote ANSI stream. The private remote
	// surface occupies only the content rows, leaving locally owned chrome above
	// and below it.
	content := capture.frame
	composed := renderer.NewFrame(content.Width, content.Height+2)
	writePhase0Chrome(&composed, 0, "LOCAL TOP")
	for y := range content.Height {
		copy(composed.Row(y+1), content.Row(y))
	}
	writePhase0Chrome(&composed, composed.Height-1, "LOCAL BOTTOM")
	return composed
}

func writePhase0Chrome(frame *renderer.Frame, row int, label string) {
	for column, r := range label {
		if column >= frame.Width {
			return
		}
		frame.Set(column, row, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
	}
}

// replayPhase0ANSI intentionally splits every byte of CSI/OSC controls across
// separate writes while keeping UTF-8 runes together. vt.Screen buffers split
// escape sequences, but deliberately does not buffer incomplete UTF-8 runes.
func replayPhase0ANSI(screen *vt.Screen, data []byte) {
	for len(data) > 0 {
		if data[0] != 0x1b {
			_, size := utf8.DecodeRune(data)
			if size == 0 {
				size = 1
			}
			screen.Write(data[:size])
			data = data[size:]
			continue
		}

		screen.Write(data[:1])
		data = data[1:]
		if len(data) == 0 {
			return
		}
		kind := data[0]
		screen.Write(data[:1])
		data = data[1:]

		switch kind {
		case '[':
			for len(data) > 0 {
				b := data[0]
				screen.Write(data[:1])
				data = data[1:]
				if b >= '@' && b <= '~' {
					break
				}
			}
		case ']':
			for len(data) > 0 {
				b := data[0]
				screen.Write(data[:1])
				data = data[1:]
				if b == 0x07 {
					break
				}
				if b == 0x1b && len(data) > 0 && data[0] == '\\' {
					screen.Write(data[:1])
					data = data[1:]
					break
				}
			}
		case 'P', '_', '^', 'X':
			for len(data) > 0 {
				b := data[0]
				screen.Write(data[:1])
				data = data[1:]
				if b == 0x1b && len(data) > 0 && data[0] == '\\' {
					screen.Write(data[:1])
					data = data[1:]
					break
				}
			}
		}
	}
}

func phase0CursorTail(row, col, style int, visible bool) []byte {
	tail := []byte(fmt.Sprintf("\x1b[%d;%dH\x1b[%d q", row+1, col+1, style))
	if visible {
		return append(tail, "\x1b[?25h"...)
	}
	return append(tail, "\x1b[?25l"...)
}

func phase0StyledFrame() renderer.Frame {
	styled := renderer.Style{
		Bold:             true,
		Italic:           true,
		Attrs:            renderer.AttrUnderline,
		UnderlineStyle:   renderer.UnderlineCurly,
		Foreground:       -1,
		Background:       -1,
		HasForegroundRGB: true,
		ForegroundRGB:    renderer.RGB{R: 12, G: 34, B: 56},
		HasBackgroundRGB: true,
		BackgroundRGB:    renderer.RGB{R: 200, G: 100, B: 50},
	}
	indexed := renderer.Style{Foreground: 82, Background: 17}

	frame := renderer.NewFrame(10, 2)
	frame.Set(0, 0, renderer.Cell{Rune: 'X', Style: styled})
	frame.Set(1, 0, renderer.Cell{Rune: '界', Style: styled})
	frame.Set(2, 0, renderer.Cell{Continuation: true, Style: styled})
	frame.Set(3, 0, renderer.Cell{Rune: 'A', Style: indexed})
	frame.Set(0, 1, renderer.Cell{Rune: 'q', Style: renderer.DefaultStyle()})
	return frame
}

func requirePhase0Frame(t *testing.T, got, want renderer.Frame) {
	t.Helper()
	require.Equal(t, want.Width, got.Width)
	require.Equal(t, want.Height, got.Height)
	for y := range want.Height {
		for x := range want.Width {
			require.Equal(t, want.At(x, y), got.At(x, y), "cell(%d,%d)", x, y)
		}
	}
}

func requirePhase0ComposedContent(t *testing.T, composed, content renderer.Frame) {
	t.Helper()
	require.Equal(t, content.Width, composed.Width)
	require.Equal(t, content.Height+2, composed.Height)
	for y := range content.Height {
		for x := range content.Width {
			require.Equal(t, content.At(x, y), composed.At(x, y+1), "content cell(%d,%d)", x, y)
		}
	}
	for column, r := range "LOCAL TOP" {
		if column >= composed.Width {
			break
		}
		require.Equal(t, r, composed.At(column, 0).Rune, "top chrome column %d", column)
	}
	for column, r := range "LOCAL BOTTOM" {
		if column >= composed.Width {
			break
		}
		require.Equal(t, r, composed.At(column, composed.Height-1).Rune, "bottom chrome column %d", column)
	}
}

func TestPhase0RemoteANSIReplayFullAndIncremental(t *testing.T) {
	initial := phase0StyledFrame()
	updated := initial.Clone()
	updatedStyle := renderer.Style{
		Bold:             true,
		Attrs:            renderer.AttrUnderline,
		UnderlineStyle:   renderer.UnderlineDouble,
		Foreground:       -1,
		Background:       -1,
		HasForegroundRGB: true,
		ForegroundRGB:    renderer.RGB{R: 90, G: 80, B: 70},
	}
	updated.Set(0, 0, renderer.Cell{Rune: 'Z', Style: updatedStyle})
	updated.Set(1, 0, renderer.Cell{Rune: '好', Style: updatedStyle})
	updated.Set(2, 0, renderer.Cell{Continuation: true, Style: updatedStyle})

	remoteRenderer := renderer.New(renderer.Capabilities{SynchronizedOutput: true})
	privateScreen := vt.NewScreen(initial.Width, initial.Height)
	// These callbacks are the security boundary under test: remote content is
	// replayed into a private screen that cannot answer, notify, ring, or write
	// the host clipboard.
	privateScreen.OnResponse = nil
	privateScreen.OnBell = nil
	privateScreen.OnNotify = nil
	privateScreen.OnProgress = nil
	privateScreen.OnClipboard = nil

	full, err := remoteRenderer.Draw(initial, []renderer.Damage{renderer.FullRedraw()})
	require.NoError(t, err)
	require.Contains(t, string(full), renderer.SyncStartCSI)
	require.Contains(t, string(full), renderer.SyncEndCSI)
	full = append(full, phase0CursorTail(0, 3, 2, true)...)
	replayPhase0ANSI(privateScreen, full)

	fullCapture := capturePhase0Replay(privateScreen)
	requirePhase0Frame(t, fullCapture.frame, initial)
	fullComposition := composePhase0Replay(fullCapture)
	requirePhase0ComposedContent(t, fullComposition, initial)
	require.Equal(t, vt.CursorSnapshot{Row: 0, Col: 3, Visible: true, Style: 2, StyleSet: true}, fullCapture.cursor)
	require.False(t, privateScreen.Snapshot().Modes().SynchronizedUpdate)

	// A composed frame is rendered by the local renderer. The remote byte
	// stream is not passed to this renderer or to a transport.
	localRenderer := renderer.New(renderer.Capabilities{})
	localBytes, err := localRenderer.Draw(fullComposition, []renderer.Damage{renderer.FullRedraw()})
	require.NoError(t, err)
	localScreen := vt.NewScreen(fullComposition.Width, fullComposition.Height)
	replayPhase0ANSI(localScreen, localBytes)
	requirePhase0Frame(t, localScreen.Frame, fullComposition)

	incremental, err := remoteRenderer.Draw(updated, []renderer.Damage{{Kind: renderer.DamageText, X: 0, Y: 0, Width: 3, Height: 1}})
	require.NoError(t, err)
	require.Contains(t, string(incremental), renderer.SyncStartCSI)
	require.Contains(t, string(incremental), renderer.SyncEndCSI)
	incremental = append(incremental, phase0CursorTail(1, 5, 3, false)...)
	replayPhase0ANSI(privateScreen, incremental)

	incrementalCapture := capturePhase0Replay(privateScreen)
	requirePhase0Frame(t, incrementalCapture.frame, updated)
	requirePhase0ComposedContent(t, composePhase0Replay(incrementalCapture), updated)
	require.Equal(t, vt.CursorSnapshot{Row: 1, Col: 5, Visible: false, Style: 3, StyleSet: true}, incrementalCapture.cursor)
	require.False(t, privateScreen.Snapshot().Modes().SynchronizedUpdate)

	noOp, err := remoteRenderer.Draw(updated, nil)
	require.NoError(t, err)
	require.Empty(t, noOp, "the remote renderer shadow must advance only once per emitted full/incremental frame")
}

func TestPhase0PrivateReplaySuppressesHostSideEffects(t *testing.T) {
	screen := vt.NewScreen(16, 2)
	screen.OnResponse = nil
	screen.OnBell = nil
	screen.OnNotify = nil
	screen.OnProgress = nil
	screen.OnClipboard = nil
	screen.SetDefaultColors(renderer.RGB{R: 1, G: 2, B: 3}, renderer.RGB{R: 4, G: 5, B: 6}, true)

	data := []byte("safe")
	data = append(data, "\x1b]52;c;Y2xpcA==\x07"...)
	data = append(data, "\x1b[5n\x1b[c\x1b[6n\x1b]10;?\x07"...)
	data = append(data, "\x1b]9;remote notification\x07"...)
	data = append(data, "\x1b]777;notify;title;body\x07"...)
	data = append(data, "\x1b]0;remote-title\x07"...)
	data = append(data, "\x1b]2;remote-title-2\x1b\\"...)
	data = append(data, "\x1b]9;4;2;0\x07\x1b]9;4;0\x07\a"...)
	data = append(data, "\x1bP1;2|dcs-payload\x1b\\"...)
	data = append(data, "\x1b_Ga=T,f=100;image-payload\x1b\\"...)
	replayPhase0ANSI(screen, data)

	expected := renderer.NewFrame(16, 2)
	expected.Set(0, 0, renderer.Cell{Rune: 's', Style: renderer.DefaultStyle()})
	expected.Set(1, 0, renderer.Cell{Rune: 'a', Style: renderer.DefaultStyle()})
	expected.Set(2, 0, renderer.Cell{Rune: 'f', Style: renderer.DefaultStyle()})
	expected.Set(3, 0, renderer.Cell{Rune: 'e', Style: renderer.DefaultStyle()})
	capture := capturePhase0Replay(screen)
	requirePhase0Frame(t, capture.frame, expected)
	composition := composePhase0Replay(capture)
	requirePhase0ComposedContent(t, composition, expected)
	require.Equal(t, 0, capture.cursor.Row)
	require.Equal(t, 4, capture.cursor.Col)
	// The private remote VT retains its title as terminal state, but the
	// composed local output below contains no title-setting control sequence.
	require.Equal(t, "remote-title-2", screen.TerminalTitle())

	// A control sequence is consumed before composition and cannot appear in
	// the locally rendered result.
	localBytes, err := renderer.New(renderer.Capabilities{}).Draw(composition, []renderer.Damage{renderer.FullRedraw()})
	require.NoError(t, err)
	require.NotContains(t, string(localBytes), "\x1b]")
	require.NotContains(t, string(localBytes), "52;")
}

type phase0OrderedFrame struct {
	metadata     bool
	stateBearing bool
	epoch        uint64
	base         uint64
	next         uint64
	full         bool
}

type phase0OrderedFrameContract struct {
	metadataSeen bool
	contentSeen  bool
	epoch        uint64
	next         uint64
}

func (c *phase0OrderedFrameContract) accept(frame phase0OrderedFrame) error {
	if frame.metadata {
		if frame.stateBearing || frame.full {
			return fmt.Errorf("metadata frame carries output state")
		}
		if c.metadataSeen || c.contentSeen {
			return fmt.Errorf("metadata must be unique and precede content")
		}
		c.metadataSeen = true
		return nil
	}
	if !frame.stateBearing {
		// Side-effect-like output does not participate in the content chain.
		return nil
	}
	if !c.metadataSeen {
		return fmt.Errorf("metadata must precede first full content")
	}
	if frame.epoch == 0 || frame.next == 0 {
		return fmt.Errorf("state-bearing output has invalid state numbers")
	}
	if !c.contentSeen {
		if !frame.full || frame.base != 0 {
			return fmt.Errorf("first content must be a full reset from base zero")
		}
		c.contentSeen = true
		c.epoch, c.next = frame.epoch, frame.next
		return nil
	}
	if frame.full {
		if frame.base != 0 {
			return fmt.Errorf("full content must reset from base zero")
		}
		if frame.epoch <= c.epoch {
			return fmt.Errorf("full content reset epoch %d is not newer than %d", frame.epoch, c.epoch)
		}
		c.epoch, c.next = frame.epoch, frame.next
		return nil
	}
	if frame.epoch != c.epoch {
		return fmt.Errorf("epoch changed without a full reset")
	}
	if frame.base != c.next {
		return fmt.Errorf("state-bearing output gap: base=%d want=%d", frame.base, c.next)
	}
	if frame.next != frame.base+1 {
		return fmt.Errorf("state-bearing output must advance by one")
	}
	c.next = frame.next
	return nil
}

func acceptPhase0Frames(frames []phase0OrderedFrame) error {
	var contract phase0OrderedFrameContract
	for i, frame := range frames {
		if err := contract.accept(frame); err != nil {
			return fmt.Errorf("frame %d: %w", i, err)
		}
	}
	return nil
}

func TestPhase0OrderedFrameContract(t *testing.T) {
	metadata := phase0OrderedFrame{metadata: true}
	full := phase0OrderedFrame{stateBearing: true, epoch: 1, base: 0, next: 1, full: true}

	t.Run("metadata full contiguous deltas and full reset are accepted", func(t *testing.T) {
		require.NoError(t, acceptPhase0Frames([]phase0OrderedFrame{
			{stateBearing: false},
			metadata,
			full,
			{stateBearing: false},
			{stateBearing: true, epoch: 1, base: 1, next: 2},
			{stateBearing: true, epoch: 2, base: 0, next: 1, full: true},
			{stateBearing: true, epoch: 2, base: 1, next: 2},
		}))
	})

	tests := []struct {
		name   string
		frames []phase0OrderedFrame
	}{
		{
			name:   "first full content before metadata",
			frames: []phase0OrderedFrame{full},
		},
		{
			name:   "incremental content before first full",
			frames: []phase0OrderedFrame{metadata, {stateBearing: true, epoch: 1, base: 0, next: 1}},
		},
		{
			name: "state-bearing gap",
			frames: []phase0OrderedFrame{
				metadata, full,
				{stateBearing: true, epoch: 1, base: 3, next: 4},
			},
		},
		{
			name: "epoch change without full reset",
			frames: []phase0OrderedFrame{
				metadata, full,
				{stateBearing: true, epoch: 2, base: 1, next: 2},
			},
		},
		{
			name: "full content with nonzero base",
			frames: []phase0OrderedFrame{
				metadata, full,
				{stateBearing: true, epoch: 2, base: 1, next: 2, full: true},
			},
		},
		{
			name: "stale full reset",
			frames: []phase0OrderedFrame{
				metadata, full,
				{stateBearing: true, epoch: 1, base: 0, next: 1, full: true},
			},
		},
		{
			name:   "duplicate metadata after content",
			frames: []phase0OrderedFrame{metadata, full, metadata},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, acceptPhase0Frames(tt.frames))
		})
	}
}
