package protocol

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
)

var ErrInvalidAttachment = errors.New("invalid attachment transition")

// SuspendAttachment requests a publication barrier on the current attachment.
// RequestID is nonzero and unique among this connection's transitions. Until
// its matching ACK arrives the client must not transfer foreground ownership.
type SuspendAttachment struct{ RequestID uint64 }

// AttachmentSuspended acknowledges that input, UI, rendering and geometry
// authority have been revoked and pending publications drained. After this ACK
// no pre-suspension output may become visible. Target is the retained lifecycle,
// not authority to select any other session. The authenticated transport stays open.
type AttachmentSuspended struct {
	RequestID uint64
	Target    ExactSessionTarget
}

// ActivateAttachment reclaims only the exact target retained by this suspended
// attachment, with the client's current geometry. It does not grant same-peer
// navigation authority. Input remains gated until the matching full publication
// and AttachmentActivated have both been received.
type ActivateAttachment struct {
	RequestID   uint64
	Target      ExactSessionTarget
	Size        domain.Size
	PixelWidth  int
	PixelHeight int
}

func (m ActivateAttachment) Geometry() domain.Geometry {
	return domain.Geometry{Size: m.Size, PixelWidth: m.PixelWidth, PixelHeight: m.PixelHeight}.NormalizePixels()
}

// AttachmentActivated commits a successful activation after a full Output and
// changed RoutePosition on a fresh epoch. Identity and the
// epoch/state/view-publication fence must match that Output's context. The
// receiver checks freshness against its previous
// chain and correlation against the pending request; Validate checks only the
// self-contained message. Failure must not emit this success ACK (the existing
// error/close path permits eviction and reconnect).
type AttachmentActivated struct {
	RequestID       uint64
	Identity        CommittedRouteIdentity
	Epoch           uint64
	State           uint64
	ViewPublication uint64
}

func (m SuspendAttachment) Validate() error {
	if m.RequestID == 0 {
		return ErrInvalidAttachment
	}
	return nil
}
func (m AttachmentSuspended) Validate() error {
	if m.RequestID == 0 || m.Target.Validate() != nil {
		return ErrInvalidAttachment
	}
	return nil
}
func (m ActivateAttachment) Validate() error {
	if m.RequestID == 0 || m.Target.Validate() != nil {
		return ErrInvalidAttachment
	}
	if ValidateGeometry(domain.Geometry{Size: m.Size, PixelWidth: m.PixelWidth, PixelHeight: m.PixelHeight}) != nil {
		return ErrInvalidAttachment
	}
	return nil
}
func (m AttachmentActivated) Validate() error {
	if m.RequestID == 0 || m.Identity.Validate() != nil || m.Epoch == 0 || m.State == 0 || m.ViewPublication == 0 {
		return ErrInvalidAttachment
	}
	return nil
}

func (SuspendAttachment) clientMessage()   {}
func (ActivateAttachment) clientMessage()  {}
func (AttachmentSuspended) serverMessage() {}
func (AttachmentActivated) serverMessage() {}
