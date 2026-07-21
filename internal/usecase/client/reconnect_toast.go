package client

import (
	"fmt"
	"io"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
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

// clientToastUI owns a transient client-local overlay. It is intentionally
// reconciled by a daemon reset frame rather than erased locally: an
// incremental daemon diff cannot restore cells hidden by the overlay.
type clientToastUI struct {
	showing bool
	rect    domain.Rect
}

func (u *clientToastUI) draw(term ports.Terminal, size domain.Size, message string) bool {
	changed := u.showing
	if u.showing {
		_ = clearReconnectToast(term.Out(), u.rect)
		u.showing = false
		u.rect = domain.Rect{}
	}
	rect, _ := drawClientToast(term.Out(), size, message)
	if rect.Width <= 0 || rect.Height <= 0 {
		_ = term.Flush()
		return changed
	}
	u.rect = rect
	u.showing = true
	_ = term.Flush()
	return true
}

// reconcile marks the overlay gone after the daemon has painted a full reset
// frame over it. It deliberately emits no terminal bytes.
func (u *clientToastUI) reconcile() {
	u.showing = false
	u.rect = domain.Rect{}
}

func clearReconnectToast(out io.Writer, bounds domain.Rect) error {
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
