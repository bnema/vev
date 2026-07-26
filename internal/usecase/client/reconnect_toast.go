package client

import (
	"fmt"
	"io"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

const reconnectToastMessage = "offline; retrying…"

func reconnectStageMessage(stage reconnectStage) string {
	switch stage {
	case reconnectStageDegraded:
		return "connection degraded"
	case reconnectStageProbingUDP:
		return "probing UDP path"
	case reconnectStageSSH:
		return "offline; retrying… reconnecting through SSH"
	case reconnectStageOfflineRetrying:
		return reconnectToastMessage
	default:
		return reconnectToastMessage
	}
}

func drawReconnectToast(out io.Writer, size domain.Size) error {
	_, err := drawReconnectToastStage(out, size, reconnectStageOfflineRetrying)
	return err
}

func drawReconnectToastStage(out io.Writer, size domain.Size, stage reconnectStage) (domain.Rect, error) {
	return drawClientToast(out, size, reconnectStageMessage(stage))
}

// drawClientToast draws a client-local toast without changing terminal state.
// The attach main loop owns both this write and the later daemon-frame
// reconciliation; input pumps must only publish a request for it.
func drawClientToast(out io.Writer, size domain.Size, message string) (domain.Rect, error) {
	bounds := reconnectToastBoundsFor(size, message)
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return domain.Rect{}, nil
	}
	return bounds, writeReconnectToast(out, bounds, reconnectToastLinesFor(bounds, message))
}

func reconnectToastBounds(size domain.Size) domain.Rect {
	return reconnectToastBoundsFor(size, reconnectToastMessage)
}

func reconnectToastBoundsFor(size domain.Size, message string) domain.Rect {
	return ui.ToastBounds(size, ui.Toast{Message: message, Anchor: domain.AnchorCenter})
}

func reconnectToastLines(bounds domain.Rect) []string {
	return reconnectToastLinesFor(bounds, reconnectToastMessage)
}

func reconnectToastLinesFor(bounds domain.Rect, message string) []string {
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return nil
	}
	frame := renderer.NewFrame(bounds.Width, bounds.Height)
	ui.CompositeToasts(frame, []ui.ActiveToast{{Toast: ui.Toast{Message: message, Anchor: domain.AnchorCenter}}}, ui.ToastStyles{
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
