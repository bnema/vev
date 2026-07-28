package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

var snatchedModal = ui.Modal{
	FixedWidth: 42, FixedHeight: 7,
	Title: " Session snatched ", Anchor: domain.AnchorCenter,
}

func snatchedPanelFrame(size domain.Size, styles themeui.Styles, feedback string) renderer.Frame {
	width, height := max(size.Cols, 1), max(size.Rows, 1)
	frame := renderer.NewFrame(width, height)
	ui.FillRect(frame, domain.Rect{Width: width, Height: height}, renderer.Cell{Rune: ' ', Style: styles.PickerBase})
	if width < snatchedModal.FixedWidth || height < snatchedModal.FixedHeight {
		drawSnatchedCompact(frame, styles, feedback)
		return frame
	}

	presentation := snatchedModal.Resolve(domain.Size{Cols: width, Rows: height})
	inner := snatchedModal.CompositePresentation(frame, presentation, styles.BorderMuted, styles.PickerBase)
	message := "This session is now active elsewhere."
	if feedback != "" {
		message = feedback
	}
	drawCenteredLine(frame, inner, inner.Y+1, message, styles.PickerBase)
	drawCenteredLine(frame, inner, inner.Y+3, "r  Resume here        q / Esc  Quit", styles.PickerSelection)
	return frame
}

func drawSnatchedCompact(frame renderer.Frame, styles themeui.Styles, feedback string) {
	status := "Session snatched"
	if feedback != "" {
		status = "Session unavailable"
	}
	lines := []string{status, "r Resume", "q Quit"}
	startY := max((frame.Height-len(lines))/2, 0)
	for i, line := range lines {
		drawCenteredLine(frame, domain.Rect{Width: frame.Width, Height: frame.Height}, startY+i, line, styles.PickerBase)
	}
}

func drawCenteredLine(frame renderer.Frame, bounds domain.Rect, y int, text string, style renderer.Style) {
	width := 0
	for _, r := range text {
		width += renderer.RuneWidth(r)
	}
	x := bounds.X + max((bounds.Width-width)/2, 0)
	ui.DrawText(frame, x, y, bounds.X+bounds.Width, text, style)
}

var errSnatchedOutputStale = errors.New("snatched output role changed")

// sendSnatchedPanel serializes a dependency-free panel reset onto one exact
// attachment role and transport lifetime. It does not inspect session state or
// capture pane content.
func (d *Daemon) sendSnatchedPanel(ac *attachedClient, expected transportSnapshot, generation uint64, feedback string, effects ...*roleEffectTicket) error {
	if expected.transport == nil {
		return errors.New("snatched client transport is nil")
	}
	var effect *roleEffectTicket
	if len(effects) != 0 {
		effect = effects[0]
	}
	tr, err := d.boundedSendWith(expected.transport, func() error {
		ac.sendMu.Lock()
		defer ac.sendMu.Unlock()
		if !ac.transportSnapshotCurrent(expected) {
			return errTransportReplaced
		}
		if ac.roleGeneration.Load() != generation {
			return errSnatchedOutputStale
		}

		applied := ac.getAppliedTheme()
		frame := snatchedPanelFrame(ac.size, applied.Resolved.Styles, feedback)
		ac.output.rebase()
		prepared, err := ac.output.prepare(frame, []renderer.Damage{renderer.FullRedraw()}, true)
		if err != nil {
			return err
		}
		data := append(append([]byte(nil), prepared.data...), ac.encodeCursorTail(cursorOut{hidden: true}, true)...)
		if effect != nil && !effect.beginTransportSend(expected) {
			return errAttachmentTransition
		}
		err = prepared.send(data, ac.echoAck.Load(), expected.transport.Send)
		if effect != nil {
			if err != nil {
				effect.reportTransportFailure(expected)
			}
			effect.endTransportSend()
		}
		return err
	})
	if errors.Is(err, errSendTimedOut) {
		_ = ac.closeCapturedTransport(tr)
	}
	return err
}
