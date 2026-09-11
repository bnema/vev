package protocol

import (
	"errors"
	"math"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
)

// RemotePreviewSchemaVersion is independent from the attachment protocol version.
const RemotePreviewSchemaVersion uint16 = 1

type RemotePreviewStatus uint8

const (
	RemotePreviewOK RemotePreviewStatus = iota
	RemotePreviewUnavailable
	RemotePreviewNoSuchTarget
	RemotePreviewStale
	RemotePreviewMalformed
	RemotePreviewTooLarge
)

type RemotePreviewRequest struct {
	Version uint16
	Target  domain.RemoteSessionTarget
	Width   uint16
	Height  uint16
}

// RemotePreview is a bounded row-major styled-cell viewport. It is never
// persisted, logged, or traced.
type RemotePreview struct {
	Version     uint16
	Status      RemotePreviewStatus
	LifecycleID domain.SessionLifecycleID
	TabID       domain.TabStableID
	Revision    uint64
	Width       uint16
	Height      uint16
	Cells       []renderer.Cell
}

const (
	RemotePreviewMaxWidth  = 256
	RemotePreviewMaxHeight = 128
	RemotePreviewMaxCells  = RemotePreviewMaxWidth * RemotePreviewMaxHeight
	RemotePreviewMaxBytes  = 1 << 20
)

var (
	ErrInvalidRemotePreviewRequest   = errors.New("ports: invalid remote preview request")
	ErrInvalidRemotePreview          = errors.New("ports: invalid remote preview")
	ErrRemotePreviewTooLarge         = errors.New("ports: remote preview exceeds size limit")
	ErrRemotePreviewTimeout          = errors.New("ports: remote preview command timed out")
	ErrRemotePreviewUnsupportedStyle = errors.New("ports: remote preview has unsupported style")
)

func ValidateRemotePreviewRequest(request RemotePreviewRequest) error {
	if request.Version != RemotePreviewSchemaVersion || request.Target.Stopped || request.Target.Validate() != nil {
		return ErrInvalidRemotePreviewRequest
	}
	// These fields are encoded with uint16 byte lengths. Reject an oversized
	// nested route before putString truncates it on the wire.
	if len(request.Target.Endpoint) > math.MaxUint16 ||
		len(request.Target.DisplayOrigin) > math.MaxUint16 ||
		len(request.Target.SessionName) > math.MaxUint16 ||
		len(request.Target.LiveTabID) > math.MaxUint16 {
		return ErrInvalidRemotePreviewRequest
	}
	if request.Width == 0 || request.Height == 0 || int(request.Width) > RemotePreviewMaxWidth || int(request.Height) > RemotePreviewMaxHeight {
		return ErrInvalidRemotePreviewRequest
	}
	return nil
}

func ValidateRemotePreview(preview RemotePreview) error {
	if preview.Version != RemotePreviewSchemaVersion || preview.Status > RemotePreviewTooLarge {
		return ErrInvalidRemotePreview
	}
	if preview.Status != RemotePreviewOK {
		if preview.Width != 0 || preview.Height != 0 || len(preview.Cells) != 0 {
			return ErrInvalidRemotePreview
		}
		return nil
	}
	if preview.LifecycleID == (domain.SessionLifecycleID{}) || domain.ValidateTabStableID(preview.TabID) != nil || preview.Revision == 0 {
		return ErrInvalidRemotePreview
	}
	if preview.Width == 0 || preview.Height == 0 || int(preview.Width) > RemotePreviewMaxWidth || int(preview.Height) > RemotePreviewMaxHeight {
		return ErrInvalidRemotePreview
	}
	want := int(preview.Width) * int(preview.Height)
	if want > RemotePreviewMaxCells || len(preview.Cells) != want {
		return ErrRemotePreviewTooLarge
	}
	if !previewCellRunValid(int(preview.Width), preview.Cells) {
		return ErrInvalidRemotePreview
	}
	return nil
}

func validPreviewCellStyle(style renderer.Style) bool {
	return style.Attrs&^(renderer.AttrDim|renderer.AttrUnderline|renderer.AttrBlink|renderer.AttrStrikethrough) == 0 &&
		style.UnderlineStyle <= renderer.UnderlineDashed &&
		style.Foreground >= math.MinInt16 && style.Foreground <= math.MaxInt16 &&
		style.Background >= math.MinInt16 && style.Background <= math.MaxInt16 &&
		style.UnderlineColor >= math.MinInt16 && style.UnderlineColor <= math.MaxInt16
}

func (p RemotePreview) FrameRows() [][]renderer.Cell {
	if p.Width == 0 || p.Height == 0 || len(p.Cells) != int(p.Width)*int(p.Height) {
		return nil
	}
	rows := make([][]renderer.Cell, p.Height)
	width := int(p.Width)
	for y := range rows {
		rows[y] = append([]renderer.Cell(nil), p.Cells[y*width:(y+1)*width]...)
	}
	return rows
}
