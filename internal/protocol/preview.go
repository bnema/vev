package protocol

import (
	"errors"
	"math"
	"time"
	"unicode"
	"unicode/utf8"

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

// RemotePreviewRequest is the bounded exact-target viewport a preview watch
// observes. It is never a session message on its own.
type RemotePreviewRequest struct {
	Version uint16
	Target  domain.RemoteSessionTarget
	Width   uint16
	Height  uint16
}

// RemotePreviewWatch is a client first message: the daemon answers with a
// stream of RemotePreview frames for Request, at most one per MinInterval,
// until the client closes the connection. A dead target ends the stream with
// one RemotePreviewNoSuchTarget frame.
type RemotePreviewWatch struct {
	Request     RemotePreviewRequest
	MinInterval time.Duration
}

// Watch interval bounds. The wire carries whole milliseconds.
const (
	RemotePreviewWatchMinInterval = 16 * time.Millisecond
	RemotePreviewWatchMaxInterval = time.Second
)

var ErrInvalidRemotePreviewWatch = errors.New("vev: invalid remote preview watch")

// ValidateRemotePreviewWatch accepts only a valid request and a whole
// millisecond interval inside the watch bounds.
func ValidateRemotePreviewWatch(watch RemotePreviewWatch) error {
	if ValidateRemotePreviewRequest(watch.Request) != nil ||
		watch.MinInterval < RemotePreviewWatchMinInterval || watch.MinInterval > RemotePreviewWatchMaxInterval ||
		watch.MinInterval%time.Millisecond != 0 {
		return ErrInvalidRemotePreviewWatch
	}
	return nil
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

// previewCellRunValid reports whether cells form the row-major run of a
// width-wide viewport: every rune and style is displayable, and each
// continuation column follows a double-width cell on the same row.
func previewCellRunValid(width int, cells []renderer.Cell) bool {
	if width <= 0 {
		return false
	}
	for i, cell := range cells {
		if !utf8.ValidRune(cell.Rune) || (cell.Rune != 0 && unicode.IsControl(cell.Rune)) || !validPreviewCellStyle(cell.Style) {
			return false
		}
		rowStart := (i / width) * width
		rowEnd := rowStart + width
		if cell.Continuation {
			if cell.Rune != 0 || i == rowStart || cells[i-1].Continuation || renderer.RuneWidth(cells[i-1].Rune) != 2 {
				return false
			}
		} else if renderer.RuneWidth(cell.Rune) == 2 && (i+1 >= rowEnd || !cells[i+1].Continuation) {
			return false
		}
	}
	return true
}
