package client

import (
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Picker is the composition seam over the client-owned picker.
//
// A composition must construct one picker with NewPicker, assign it once to
// SupervisorConfig.Picker, and paint the bytes returned by Render and
// RenderNotice. Its zero value is inert so optional composition remains safe,
// but it does not provide a working picker.
//
// The remaining exported methods are the supervisor contract: ApplySnapshot,
// TakeOp, ResolveKey, SetOwnsInput, OpsReady, and ConsumeTerminalRead. A
// composition must never drive those methods itself.
// Doing so can violate the supervisor's single-reader and single-input-owner
// invariants or consume a pending user decision before the supervisor handles
// it.
//
// Painting is allowed only while PickerPresentation reports true: the plain
// picker, or the picker overlay over a live attachment whose output the
// attachment foreground suppresses while the overlay is open. In
// particular, PresentConnecting is not safe: once the attachment foreground is
// admitted it owns the same terminal writer before MarkAttached or the initial
// publication. Nothing serializes picker painting with that foreground.
// The picker deliberately neither owns a terminal writer nor consumes the
// terminal resize channel; the composition supplies its current geometry when
// rendering.
type Picker struct {
	controller *pickerController
}

// Picker is the supervisor's picker seam: the value assigned to
// SupervisorConfig.Picker.
var _ pickerHost = (*Picker)(nil)

// PickerPresentation reports whether composition-owned picker bytes may be
// written for state. This is intentionally narrower than "not attached": an
// admitted foreground can write while the public presentation is still
// PresentConnecting.
func PickerPresentation(state State) bool {
	return state.Presentation == PresentPicker || state.Presentation == PresentAttachedPicker
}

// NewPicker composes one client-owned picker. A nil clock uses the system
// clock, and a zero freshness uses the catalogue default. trueColor is the
// terminal's detected color capability (AttachmentEnvironment.TrueColor); the
// picker renders indexed colors without it.
func NewPicker(clock ports.Clock, freshness time.Duration, trueColor bool) *Picker {
	return &Picker{controller: newPickerController(clock, freshness, trueColor)}
}

// ApplySnapshot forwards a broker publication to the supervisor-owned picker.
func (p *Picker) ApplySnapshot(snapshot ports.BrokerSnapshot) {
	if p == nil || p.controller == nil {
		return
	}
	p.controller.ApplySnapshot(snapshot)
}

// TakeOp returns and clears the pending picker operation for the supervisor.
func (p *Picker) TakeOp() (pickerOp, string) {
	if p == nil || p.controller == nil {
		return pickerOp{}, ""
	}
	return p.controller.TakeOp()
}

// ResolveKey resolves a committed catalogue key for the supervisor.
func (p *Picker) ResolveKey(key string, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	if p == nil || p.controller == nil {
		return (*pickerController)(nil).ResolveKey(key, base)
	}
	return p.controller.ResolveKey(key, base)
}

// ResolveKeyTarget resolves a committed key plus the exact tab it names.
func (p *Picker) ResolveKeyTarget(key string, base pickerResolveBase) (ports.BrokerOpenStreamRequest, attachmentTab, error) {
	if p == nil || p.controller == nil {
		return (*pickerController)(nil).ResolveKeyTarget(key, base)
	}
	return p.controller.ResolveKeyTarget(key, base)
}

// ResolveKill revalidates one kill key for the supervisor.
func (p *Picker) ResolveKill(key string) (pickerKillTarget, error) {
	if p == nil || p.controller == nil {
		return (*pickerController)(nil).ResolveKill(key)
	}
	return p.controller.ResolveKill(key)
}

// SetCurrent names the attachment the picker is presented over.
func (p *Picker) SetCurrent(current pickerCurrent) {
	if p != nil && p.controller != nil {
		p.controller.SetCurrent(current)
	}
}

// SetOwnsInput updates picker input ownership at an attachment boundary.
func (p *Picker) PreviewRequest(connection ports.BrokerConnectionID, stream ports.BrokerStreamID, size domain.Size) (ports.BrokerOpenStreamRequest, protocol.RemotePreviewRequest, bool) {
	if p == nil || p.controller == nil {
		return ports.BrokerOpenStreamRequest{}, protocol.RemotePreviewRequest{}, false
	}
	return p.controller.PreviewRequest(connection, stream, size)
}

func (p *Picker) SetPreview(preview protocol.RemotePreview) {
	if p != nil && p.controller != nil {
		p.controller.SetPreview(preview)
	}
}

func (p *Picker) ClearPreview() {
	if p != nil && p.controller != nil {
		p.controller.ClearPreview()
	}
}

func (p *Picker) SetOwnsInput(owns bool) {
	if p == nil || p.controller == nil {
		return
	}
	p.controller.SetOwnsInput(owns)
}

// offerNotice shows one bounded client-local notice on the picker.
func (p *Picker) offerNotice(id, message string) {
	if p != nil && p.controller != nil {
		p.controller.offerNotice(id, message)
	}
}

// invalidatePresentation forces the next frame to redraw the whole box.
func (p *Picker) invalidatePresentation() {
	if p != nil && p.controller != nil {
		p.controller.invalidatePresentation()
	}
}

// OpsReady returns the supervisor's coalescing operation wake channel.
func (p *Picker) OpsReady() <-chan struct{} {
	if p == nil || p.controller == nil {
		return nil
	}
	return p.controller.OpsReady()
}

// ConsumeTerminalRead lets the supervisor offer one read to the picker.
func (p *Picker) ConsumeTerminalRead(data []byte) bool {
	if p == nil || p.controller == nil {
		return false
	}
	return p.controller.ConsumeTerminalRead(data)
}

// Render returns the current picker frame for size, or nil when no catalogue
// has been applied yet. A composition paints this payload when Render is
// invoked and the reported presentation is PresentPicker.
func (p *Picker) Render(size domain.Size) []byte {
	if p == nil || p.controller == nil {
		return nil
	}
	return p.controller.Render(size)
}

// RenderNotice returns the newest bounded notice frame for size, or nil when no
// notice is live. A composition renders it over or beside the picker frame only
// while the supervisor reports PresentPicker.
func (p *Picker) RenderNotice(size domain.Size) []byte {
	if p == nil || p.controller == nil {
		return nil
	}
	return p.controller.RenderNotice(size)
}
