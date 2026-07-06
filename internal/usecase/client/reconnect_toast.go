package client

import (
	"fmt"
	"io"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

const reconnectToastMessage = "Reconnection attempts…"

func drawReconnectToast(out io.Writer, size domain.Size) error {
	bounds := reconnectToastBounds(size)
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return nil
	}
	return writeReconnectToast(out, bounds, reconnectToastLines(bounds))
}

func clearReconnectToast(out io.Writer, size domain.Size) error {
	bounds := reconnectToastBounds(size)
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return nil
	}
	blank := strings.Repeat(" ", bounds.Width)
	lines := make([]string, bounds.Height)
	for i := range lines {
		lines[i] = blank
	}
	return writeReconnectToast(out, bounds, lines)
}

func reconnectToastBounds(size domain.Size) domain.Rect {
	return ui.ToastBounds(size, ui.Toast{Message: reconnectToastMessage, Anchor: ui.ToastCenter})
}

func reconnectToastLines(bounds domain.Rect) []string {
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return nil
	}
	frame := renderer.NewFrame(bounds.Width, bounds.Height)
	ui.CompositeToasts(frame, []ui.ActiveToast{{Toast: ui.Toast{Message: reconnectToastMessage, Anchor: ui.ToastCenter}}}, ui.ToastStyles{
		Text: renderer.DefaultStyle(),
		Box:  renderer.DefaultStyle(),
	}, nil)
	lines := make([]string, bounds.Height)
	for y := range bounds.Height {
		var b strings.Builder
		b.Grow(bounds.Width)
		for x := range bounds.Width {
			cell := frame.At(x, y)
			if cell.Continuation || cell.Rune == 0 {
				b.WriteRune(' ')
				continue
			}
			b.WriteRune(cell.Rune)
		}
		lines[y] = b.String()
	}
	return lines
}

func writeReconnectToast(out io.Writer, bounds domain.Rect, lines []string) error {
	if _, err := io.WriteString(out, "\x1b[s"); err != nil {
		return err
	}
	for i, line := range lines {
		if _, err := fmt.Fprintf(out, "\x1b[%d;%dH\x1b[0m%s", bounds.Y+i+1, bounds.X+1, line); err != nil {
			return err
		}
	}
	_, err := io.WriteString(out, "\x1b[0m\x1b[u")
	return err
}
