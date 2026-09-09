package webterm

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bnema/vev-vt/html/browser"
	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestTerminalFlushResizeAndClose(t *testing.T) {
	terminal, err := New(t.Context(), domain.Geometry{Size: domain.Size{Cols: 10, Rows: 4}})
	require.NoError(t, err)
	defer terminal.Close()
	_, err = terminal.Write([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, rune(' '), terminal.Snapshot().Cell(0, 0).Rune)
	require.NoError(t, terminal.Flush())
	require.Equal(t, rune('h'), terminal.Snapshot().Cell(0, 0).Rune)
	old := terminal.Snapshot()
	geometry := domain.Geometry{Size: domain.Size{Cols: 20, Rows: 8}, PixelWidth: 200, PixelHeight: 160}
	require.NoError(t, terminal.Resize(geometry))
	require.Equal(t, geometry, <-terminal.ResizeEvents())
	require.NoError(t, terminal.Flush())
	require.Equal(t, 20, terminal.Snapshot().Columns())
	require.Equal(t, 10, old.Columns())
	require.Error(t, terminal.Resize(domain.Geometry{Size: domain.Size{Cols: MaxColumns + 1, Rows: 2}}))
	terminal.Close()
	_, err = terminal.Write([]byte("x"))
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestTerminalInput(t *testing.T) {
	tests := []struct {
		name  string
		event browser.Event
		want  string
	}{
		{"text", browser.Event{Kind: browser.EventText, Text: &browser.TextEvent{Text: "helloλ"}}, "helloλ"},
		{"paste sanitizes framing", browser.Event{Kind: browser.EventPaste, Paste: &browser.PasteEvent{Text: "a\x1b[201~\x03\nb"}}, "\x1b[200~a[201~\nb\x1b[201~"},
		{"palette", browser.Event{Kind: browser.EventKey, Key: &browser.KeyEvent{Key: " ", Modifiers: browser.Modifiers{Alt: true}}}, "\x1b "},
		{"mouse", browser.Event{Kind: browser.EventPointer, Pointer: &browser.PointerEvent{Action: "down", Button: 0, Row: 1, Column: 2}}, "\x1b[<0;3;2M"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			terminal, err := New(ctx, domain.Geometry{Size: domain.Size{Cols: 10, Rows: 4}})
			require.NoError(t, err)
			defer terminal.Close()
			_, err = terminal.EnterRaw()
			require.NoError(t, err)
			require.NoError(t, terminal.Handle(ctx, tt.event))
			data := make([]byte, len(tt.want))
			_, err = io.ReadFull(terminal.In(), data)
			require.NoError(t, err)
			require.Equal(t, tt.want, string(data))
		})
	}
}

func TestEncodeKey(t *testing.T) {
	tests := []struct {
		name, key string
		modifiers browser.Modifiers
		app       bool
		want      string
	}{
		{"cursor", "ArrowUp", browser.Modifiers{}, false, "\x1b[A"},
		{"application cursor", "ArrowUp", browser.Modifiers{}, true, "\x1bOA"},
		{"modified cursor", "ArrowLeft", browser.Modifiers{Ctrl: true}, false, "\x1b[1;5D"},
		{"interrupt", "c", browser.Modifiers{Ctrl: true}, false, "\x03"},
		{"alt letter", "h", browser.Modifiers{Alt: true}, false, "\x1bh"},
		{"shift tab", "Tab", browser.Modifiers{Shift: true}, false, "\x1b[Z"},
		{"delete", "Delete", browser.Modifiers{}, false, "\x1b[3~"},
		{"function", "F12", browser.Modifiers{}, false, "\x1b[24~"},
		{"meta reserved", "c", browser.Modifiers{Meta: true}, false, ""},
		{"unknown", "Unidentified", browser.Modifiers{}, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, encodeKey(browser.KeyEvent{Key: tt.key, Modifiers: tt.modifiers}, tt.app))
		})
	}
}
