package client

import (
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Picker is the composition seam over the client-owned picker.
//
// A composition constructs one picker and assigns it to
// SupervisorConfig.Picker. The supervisor then folds every broker publication
// into it and hands it every terminal read from the single input lifetime it
// owns, so the picker never starts a reader of its own. The supervisor also
// owns raw mode, the terminal writer, and the attachment geometry collector, so
// rendering stays with the composition: it calls Render and RenderNotice with
// the geometry it reads at render time.
//
// The picker deliberately does not consume the terminal's resize channel. That
// channel has one consumer for the whole run, the supervisor's attachment
// geometry collector, so a renderer that also selected on it would compete for
// resizes with the attachment that owns the geometry claimant.
//
// Assigning this value to SupervisorConfig.Picker is what hands the supervisor
// the picker contract, so the value necessarily also carries the supervisor's
// own entry points (publications, ownership changes, committed keys). A
// composition constructs the picker, assigns it once, and renders it; it never
// drives those entry points itself, because doing so would bypass the single
// input owner the supervisor maintains.
type Picker struct {
	*pickerController
}

// Picker is the supervisor's picker seam: the value assigned to
// SupervisorConfig.Picker.
var _ pickerHost = (*Picker)(nil)

// NewPicker composes one client-owned picker. A nil clock uses the system
// clock, and a zero freshness uses the catalogue default.
func NewPicker(clock ports.Clock, freshness time.Duration) *Picker {
	return &Picker{pickerController: newPickerController(clock, freshness)}
}

// Render returns the current picker frame for size, or nil when no catalogue
// has been applied yet.
func (p *Picker) Render(size domain.Size) []byte {
	if p == nil || p.pickerController == nil {
		return nil
	}
	return p.pickerController.Render(size)
}

// RenderNotice returns the newest bounded notice frame for size, or nil when no
// notice is live. A composition renders it over or beside the picker frame.
func (p *Picker) RenderNotice(size domain.Size) []byte {
	if p == nil || p.pickerController == nil {
		return nil
	}
	return p.pickerController.RenderNotice(size)
}
