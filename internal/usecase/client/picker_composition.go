package client

import (
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Picker is the composition seam over the client-owned picker.
//
// A composition must construct one picker with NewPicker, assign it once to
// SupervisorConfig.Picker, and paint the bytes returned by Render and
// RenderNotice. Its zero value is inert so optional composition remains safe,
// but it does not provide a working picker.
//
// The remaining exported methods are the supervisor contract: ApplySnapshot,
// TakeOp, ResolveKey, ResolveInitial, SetOwnsInput, OpsReady, and
// ConsumeTerminalRead. A composition must never drive those methods itself.
// Doing so can violate the supervisor's single-reader and single-input-owner
// invariants or consume a pending user decision before the supervisor handles
// it.
//
// Painting is allowed only while the supervisor reports PresentPicker. A
// composition must not paint picker bytes during an attachment: the attachment
// writes through the same terminal, and nothing serializes those two writers.
// The picker deliberately neither owns a terminal writer nor consumes the
// terminal resize channel; the composition supplies its current geometry when
// rendering.
type Picker struct {
	controller *pickerController
}

// Picker is the supervisor's picker seam: the value assigned to
// SupervisorConfig.Picker.
var _ pickerHost = (*Picker)(nil)

// NewPicker composes one client-owned picker. A nil clock uses the system
// clock, and a zero freshness uses the catalogue default.
func NewPicker(clock ports.Clock, freshness time.Duration) *Picker {
	return &Picker{controller: newPickerController(clock, freshness)}
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

// ResolveInitial resolves the supervisor's one-shot initial navigation.
func (p *Picker) ResolveInitial(navigation InitialNavigation, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	if p == nil || p.controller == nil {
		return (*pickerController)(nil).ResolveInitial(navigation, base)
	}
	return p.controller.ResolveInitial(navigation, base)
}

// SetOwnsInput updates picker input ownership at an attachment boundary.
func (p *Picker) SetOwnsInput(owns bool) {
	if p == nil || p.controller == nil {
		return
	}
	p.controller.SetOwnsInput(owns)
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
