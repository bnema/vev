package sessionwire

import (
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func suspendAttachmentToWire(m protocol.SuspendAttachment) (*wire.SuspendAttachment, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &wire.SuspendAttachment{RequestId: m.RequestID}, nil
}
func suspendAttachmentFromWire(m *wire.SuspendAttachment) (protocol.SuspendAttachment, error) {
	out := protocol.SuspendAttachment{RequestID: m.GetRequestId()}
	return out, out.Validate()
}
func attachmentSuspendedToWire(m protocol.AttachmentSuspended) (*wire.AttachmentSuspended, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &wire.AttachmentSuspended{RequestId: m.RequestID, Target: exactTargetToWire(&m.Target)}, nil
}
func attachmentSuspendedFromWire(m *wire.AttachmentSuspended) (protocol.AttachmentSuspended, error) {
	target, err := exactTargetFromWire(m.GetTarget())
	if err != nil || target == nil {
		return protocol.AttachmentSuspended{}, protocol.ErrInvalidAttachment
	}
	out := protocol.AttachmentSuspended{RequestID: m.GetRequestId(), Target: *target}
	return out, out.Validate()
}
func activateAttachmentToWire(m protocol.ActivateAttachment) (*wire.ActivateAttachment, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	cols, rows, pw, ph, err := geometryToWire(m.Size, m.PixelWidth, m.PixelHeight)
	if err != nil {
		return nil, err
	}
	return &wire.ActivateAttachment{RequestId: m.RequestID, Target: exactTargetToWire(&m.Target), Cols: cols, Rows: rows, PixelWidth: pw, PixelHeight: ph}, nil
}
func activateAttachmentFromWire(m *wire.ActivateAttachment) (protocol.ActivateAttachment, error) {
	target, err := exactTargetFromWire(m.GetTarget())
	if err != nil || target == nil {
		return protocol.ActivateAttachment{}, protocol.ErrInvalidAttachment
	}
	geometry, err := resizeFromWire(&wire.Resize{Cols: m.GetCols(), Rows: m.GetRows(), PixelWidth: m.GetPixelWidth(), PixelHeight: m.GetPixelHeight()})
	if err != nil {
		return protocol.ActivateAttachment{}, err
	}
	out := protocol.ActivateAttachment{RequestID: m.GetRequestId(), Target: *target, Size: geometry.Size, PixelWidth: geometry.PixelWidth, PixelHeight: geometry.PixelHeight}
	return out, out.Validate()
}
func attachmentActivatedToWire(m protocol.AttachmentActivated) (*wire.AttachmentActivated, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	identity, err := committedIdentityToWire(m.Identity)
	if err != nil {
		return nil, err
	}
	return &wire.AttachmentActivated{RequestId: m.RequestID, Identity: identity, Epoch: m.Epoch, State: m.State, ViewPublication: m.ViewPublication}, nil
}
func attachmentActivatedFromWire(m *wire.AttachmentActivated) (protocol.AttachmentActivated, error) {
	identity, err := committedIdentityFromWire(m.GetIdentity())
	if err != nil {
		return protocol.AttachmentActivated{}, protocol.ErrInvalidAttachment
	}
	out := protocol.AttachmentActivated{RequestID: m.GetRequestId(), Identity: identity, Epoch: m.GetEpoch(), State: m.GetState(), ViewPublication: m.GetViewPublication()}
	return out, out.Validate()
}
