package client

import (
	"fmt"
	"io"
	"strings"

	renderer "github.com/bnema/vev-vt"
	ansirenderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/internal/usecase/ui"
)

// drawClientToast draws a client-local toast without changing terminal state.
// The attach main loop owns both this write and the later daemon-frame
// reconciliation; input pumps must only publish a request for it.
// The anchor places it: transitions stay centered, notices sit top-right so
// they never cover the picker list. The border is colored by severity.
func drawClientToast(out io.Writer, size domain.Size, message string, anchor domain.Anchor, severity domain.NoticeSeverity) (domain.Rect, error) {
	bounds := ui.ToastBounds(size, ui.Toast{Message: message, Anchor: anchor})
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return domain.Rect{}, nil
	}
	return bounds, writeClientToast(out, bounds, clientToastLines(bounds, message, toastBorderSGR(severity)))
}

// toastBorderSGR is the border color per severity, in the same fixed
// xterm-256 colors as the picker's problem dots. Info keeps the terminal's
// default color.
func toastBorderSGR(severity domain.NoticeSeverity) string {
	switch severity {
	case domain.NoticeError:
		return fmt.Sprintf("\x1b[38;5;%dm", picker.ColorProblemError)
	case domain.NoticeWarn:
		return fmt.Sprintf("\x1b[38;5;%dm", picker.ColorProblemWarn)
	default:
		return ""
	}
}

// toastBorderSGRFor is toastBorderSGR for one renderer color profile. The
// toast bypasses the renderer, so it must follow the profile itself: 16-color
// terminals get basic red/yellow, monochrome terminals get bold for problems.
// TrueColor and 256-color output is unchanged.
func toastBorderSGRFor(severity domain.NoticeSeverity, profile ansirenderer.ColorProfile) string {
	switch profile {
	case ansirenderer.ColorProfileANSI16:
		switch severity {
		case domain.NoticeError:
			return "\x1b[31m"
		case domain.NoticeWarn:
			return "\x1b[33m"
		}
		return ""
	case ansirenderer.ColorProfileMonochrome:
		if severity == domain.NoticeError || severity == domain.NoticeWarn {
			return "\x1b[1m"
		}
		return ""
	default:
		return toastBorderSGR(severity)
	}
}

// toastBorderReset undoes toastBorderSGR's attribute: default foreground for
// colors, normal intensity for the monochrome bold border.
func toastBorderReset(borderSGR string) string {
	if borderSGR == "\x1b[1m" {
		return "\x1b[22m"
	}
	return "\x1b[39m"
}

// clientToastLines renders the toast box as one string per row, exactly
// bounds.Width columns wide.
func clientToastLines(bounds domain.Rect, message, borderSGR string) []string {
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return nil
	}
	frame := renderer.NewFrame(bounds.Width, bounds.Height)
	ui.DrawToast(frame, ui.Toast{Message: message, Anchor: domain.AnchorCenter}, ui.ToastStyles{
		Text: renderer.DefaultStyle(),
		Box:  renderer.DefaultStyle(),
	})
	lines := make([]string, bounds.Height)
	for y := range bounds.Height {
		var b strings.Builder
		b.Grow(bounds.Width)
		colored := false
		for x := range bounds.Width {
			border := y == 0 || y == bounds.Height-1 || x == 0 || x == bounds.Width-1
			if borderSGR != "" && border != colored {
				if border {
					b.WriteString(borderSGR)
				} else {
					b.WriteString(toastBorderReset(borderSGR))
				}
				colored = border
			}
			cell := frame.At(x, y)
			if cell.Continuation {
				// The wide rune before it already covers this column.
				continue
			}
			if cell.Rune == 0 {
				b.WriteRune(' ')
				continue
			}
			b.WriteRune(cell.Rune)
		}
		if colored {
			b.WriteString(toastBorderReset(borderSGR))
		}
		lines[y] = b.String()
	}
	return lines
}

// blankToastLines erases a toast box. The session under it belongs to the
// daemon, so blank cells are the best the client can restore.
func blankToastLines(bounds domain.Rect) []string {
	lines := make([]string, max(0, bounds.Height))
	for i := range lines {
		lines[i] = strings.Repeat(" ", max(0, bounds.Width))
	}
	return lines
}

func writeClientToast(out io.Writer, bounds domain.Rect, lines []string) error {
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
