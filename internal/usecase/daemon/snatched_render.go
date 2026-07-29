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
	drawCenteredSegments(frame, inner, inner.Y+3, []textSegment{
		{" r ", styles.HintKey},
		{"  Resume here", styles.PickerBase},
		{"  ", styles.PickerBase},
		{" q ", styles.HintKey},
		{" / ", styles.PickerDescription},
		{" Esc ", styles.HintKey},
		{"  Quit", styles.PickerBase},
	})
	return frame
}

func drawSnatchedCompact(frame renderer.Frame, styles themeui.Styles, feedback string) {
	status := "Session snatched"
	if feedback != "" {
		status = "Session unavailable"
	}
	bounds := domain.Rect{Width: frame.Width, Height: frame.Height}
	startY := max((frame.Height-3)/2, 0)
	drawCenteredLine(frame, bounds, startY, status, styles.PickerBase)
	drawCenteredSegments(frame, bounds, startY+1, []textSegment{
		{" r ", styles.HintKey},
		{" Resume", styles.PickerBase},
	})
	drawCenteredSegments(frame, bounds, startY+2, []textSegment{
		{" q ", styles.HintKey},
		{" Quit", styles.PickerBase},
	})
}

// textSegment is one differently-styled run within a centered hint line.
type textSegment struct {
	text  string
	style renderer.Style
}

func drawCenteredLine(frame renderer.Frame, bounds domain.Rect, y int, text string, style renderer.Style) {
	drawCenteredSegments(frame, bounds, y, []textSegment{{text, style}})
}

// drawCenteredSegments draws consecutive differently-styled segments as one
// logical line, centered as a whole within bounds.
func drawCenteredSegments(frame renderer.Frame, bounds domain.Rect, y int, segments []textSegment) {
	total := 0
	for _, seg := range segments {
		for _, r := range seg.text {
			total += renderer.RuneWidth(r)
		}
	}
	x := bounds.X + max((bounds.Width-total)/2, 0)
	limit := bounds.X + bounds.Width
	for _, seg := range segments {
		ui.DrawText(frame, x, y, limit, seg.text, seg.style)
		for _, r := range seg.text {
			x += renderer.RuneWidth(r)
		}
	}
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
