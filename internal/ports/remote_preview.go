package ports

import (
	"errors"
	"math"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

// RemotePreviewSchemaVersion is independent from the attachment IPC version.
const RemotePreviewSchemaVersion uint16 = 1

const (
	RemotePreviewMaxWidth  = 256
	RemotePreviewMaxHeight = 128
	RemotePreviewMaxCells  = RemotePreviewMaxWidth * RemotePreviewMaxHeight
	RemotePreviewMaxBytes  = 1 << 20
)

// RemotePreviewStatus is a closed response taxonomy. Terminal contents are
// never carried in an error response.
type RemotePreviewStatus uint8

const (
	RemotePreviewOK RemotePreviewStatus = iota
	RemotePreviewUnavailable
	RemotePreviewNoSuchTarget
	RemotePreviewStale
	RemotePreviewMalformed
	RemotePreviewTooLarge
)

// RemotePreviewRequest asks the owning daemon for an in-memory live viewport.
type RemotePreviewRequest struct {
	Version uint16
	Target  domain.RemoteSessionTarget
	Width   uint16
	Height  uint16
}

// RemotePreview is a bounded row-major styled-cell viewport. It is a
// process-memory DTO and is never persisted, logged, or traced.
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

var (
	ErrInvalidRemotePreviewRequest   = errors.New("ports: invalid remote preview request")
	ErrInvalidRemotePreview          = errors.New("ports: invalid remote preview")
	ErrRemotePreviewTooLarge         = errors.New("ports: remote preview exceeds size limit")
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
	for i, cell := range preview.Cells {
		if !utf8.ValidRune(cell.Rune) || !validRemotePreviewStyle(cell.Style) {
			return ErrInvalidRemotePreview
		}
		if cell.Continuation {
			if cell.Rune != 0 || i == 0 || preview.Cells[i-1].Continuation || renderer.RuneWidth(preview.Cells[i-1].Rune) != 2 {
				return ErrInvalidRemotePreview
			}
		} else if renderer.RuneWidth(cell.Rune) == 2 && (i+1 >= len(preview.Cells) || !preview.Cells[i+1].Continuation) {
			return ErrInvalidRemotePreview
		}
	}
	return nil
}

func validRemotePreviewStyle(style renderer.Style) bool {
	return style.Attrs&^(renderer.AttrDim|renderer.AttrUnderline|renderer.AttrBlink|renderer.AttrStrikethrough) == 0 &&
		style.UnderlineStyle <= renderer.UnderlineDashed &&
		style.Foreground >= math.MinInt16 && style.Foreground <= math.MaxInt16 &&
		style.Background >= math.MinInt16 && style.Background <= math.MaxInt16 &&
		style.UnderlineColor >= math.MinInt16 && style.UnderlineColor <= math.MaxInt16
}

func putInt16(w *payloadWriter, n int) { w.putUint16(uint16(int16(n))) }
func getInt16(r *payloadReader) (int, error) {
	n, err := r.getUint16()
	return int(int16(n)), err
}
func putRGB(w *payloadWriter, c renderer.RGB) { w.putUint8(c.R); w.putUint8(c.G); w.putUint8(c.B) }
func getRGB(r *payloadReader) (renderer.RGB, error) {
	red, err := r.getUint8()
	if err != nil {
		return renderer.RGB{}, err
	}
	green, err := r.getUint8()
	if err != nil {
		return renderer.RGB{}, err
	}
	blue, err := r.getUint8()
	if err != nil {
		return renderer.RGB{}, err
	}
	return renderer.RGB{R: red, G: green, B: blue}, nil
}

func putPreviewStyle(w *payloadWriter, s renderer.Style) {
	var flags uint8
	if s.Bold {
		flags |= 1 << 0
	}
	if s.Italic {
		flags |= 1 << 1
	}
	if s.Inverse {
		flags |= 1 << 2
	}
	if s.HasForegroundRGB {
		flags |= 1 << 3
	}
	if s.HasBackgroundRGB {
		flags |= 1 << 4
	}
	if s.HasUnderlineColor {
		flags |= 1 << 5
	}
	if s.HasUnderlineColorRGB {
		flags |= 1 << 6
	}
	w.putUint8(flags)
	w.putUint16(uint16(s.Attrs))
	putInt16(w, s.Foreground)
	putInt16(w, s.Background)
	w.putUint8(uint8(s.UnderlineStyle))
	putInt16(w, s.UnderlineColor)
	putRGB(w, s.ForegroundRGB)
	putRGB(w, s.BackgroundRGB)
	putRGB(w, s.UnderlineColorRGB)
}

func getPreviewStyle(r *payloadReader) (renderer.Style, error) {
	flags, err := r.getUint8()
	if err != nil {
		return renderer.Style{}, err
	}
	if flags&0x80 != 0 {
		return renderer.Style{}, ErrRemotePreviewUnsupportedStyle
	}
	attrs, err := r.getUint16()
	if err != nil {
		return renderer.Style{}, err
	}
	fg, err := getInt16(r)
	if err != nil {
		return renderer.Style{}, err
	}
	bg, err := getInt16(r)
	if err != nil {
		return renderer.Style{}, err
	}
	ul, err := r.getUint8()
	if err != nil {
		return renderer.Style{}, err
	}
	ulc, err := getInt16(r)
	if err != nil {
		return renderer.Style{}, err
	}
	fgrgb, err := getRGB(r)
	if err != nil {
		return renderer.Style{}, err
	}
	bgrgb, err := getRGB(r)
	if err != nil {
		return renderer.Style{}, err
	}
	ulrgb, err := getRGB(r)
	if err != nil {
		return renderer.Style{}, err
	}
	return renderer.Style{Bold: flags&1 != 0, Italic: flags&(1<<1) != 0, Inverse: flags&(1<<2) != 0,
		HasForegroundRGB: flags&(1<<3) != 0, HasBackgroundRGB: flags&(1<<4) != 0,
		HasUnderlineColor: flags&(1<<5) != 0, HasUnderlineColorRGB: flags&(1<<6) != 0,
		Attrs: renderer.StyleAttrs(attrs), Foreground: fg, Background: bg, UnderlineStyle: renderer.UnderlineStyle(ul), UnderlineColor: ulc,
		ForegroundRGB: fgrgb, BackgroundRGB: bgrgb, UnderlineColorRGB: ulrgb}, nil
}

func MarshalRemotePreviewRequest(request RemotePreviewRequest) []byte {
	if ValidateRemotePreviewRequest(request) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint16(request.Version)
	w.putBytes(request.Target.LifecycleID[:])
	w.putString(request.Target.Endpoint)
	w.putString(request.Target.DisplayOrigin)
	w.putString(request.Target.SessionName)
	w.putString(string(request.Target.LiveTabID))
	w.putUint16(request.Width)
	w.putUint16(request.Height)
	return w.b
}

func UnmarshalRemotePreviewRequest(data []byte) (RemotePreviewRequest, error) {
	if len(data) > RemotePreviewMaxBytes {
		return RemotePreviewRequest{}, ErrInvalidRemotePreviewRequest
	}
	r := payloadReader{b: data}
	var q RemotePreviewRequest
	var err error
	if q.Version, err = r.getUint16(); err != nil {
		return q, err
	}
	id, err := r.getBytes(16)
	if err != nil {
		return q, err
	}
	copy(q.Target.LifecycleID[:], id)
	if q.Target.Endpoint, err = r.getString(); err != nil {
		return q, err
	}
	if q.Target.DisplayOrigin, err = r.getString(); err != nil {
		return q, err
	}
	if q.Target.SessionName, err = r.getString(); err != nil {
		return q, err
	}
	tab, err := r.getString()
	if err != nil {
		return q, err
	}
	q.Target.LiveTabID = domain.TabStableID(tab)
	if q.Width, err = r.getUint16(); err != nil {
		return q, err
	}
	if q.Height, err = r.getUint16(); err != nil {
		return q, err
	}
	if err := r.done(); err != nil {
		return q, err
	}
	if err := ValidateRemotePreviewRequest(q); err != nil {
		return q, err
	}
	return q, nil
}

const previewCellWireSize = 4 + 1 + 1 + 2 + 2 + 2 + 1 + 2 + 3 + 3 + 3

func MarshalRemotePreview(preview RemotePreview) []byte {
	if ValidateRemotePreview(preview) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint16(preview.Version)
	w.putUint8(uint8(preview.Status))
	w.putBytes(preview.LifecycleID[:])
	w.putString(string(preview.TabID))
	w.putUint64(preview.Revision)
	w.putUint16(preview.Width)
	w.putUint16(preview.Height)
	w.putUint32(uint32(len(preview.Cells)))
	for _, cell := range preview.Cells {
		var flags uint8
		if cell.Continuation {
			flags = 1
		}
		w.putUint32(uint32(cell.Rune))
		w.putUint8(flags)
		putPreviewStyle(&w, cell.Style)
	}
	return w.b
}

func UnmarshalRemotePreview(data []byte) (RemotePreview, error) {
	if len(data) > RemotePreviewMaxBytes {
		return RemotePreview{}, ErrRemotePreviewTooLarge
	}
	r := payloadReader{b: data}
	var p RemotePreview
	var err error
	if p.Version, err = r.getUint16(); err != nil {
		return p, err
	}
	status, err := r.getUint8()
	if err != nil {
		return p, err
	}
	p.Status = RemotePreviewStatus(status)
	id, err := r.getBytes(16)
	if err != nil {
		return p, err
	}
	copy(p.LifecycleID[:], id)
	tab, err := r.getString()
	if err != nil {
		return p, err
	}
	p.TabID = domain.TabStableID(tab)
	if p.Revision, err = r.getUint64(); err != nil {
		return p, err
	}
	if p.Width, err = r.getUint16(); err != nil {
		return p, err
	}
	if p.Height, err = r.getUint16(); err != nil {
		return p, err
	}
	count, err := r.getUint32()
	if err != nil {
		return p, err
	}
	if count > RemotePreviewMaxCells || uint64(count) > uint64(len(r.b)/previewCellWireSize) {
		return p, ErrRemotePreviewTooLarge
	}
	if count != 0 {
		p.Cells = make([]renderer.Cell, 0, int(count))
		for range int(count) {
			runeValue, e := r.getUint32()
			if e != nil {
				return p, e
			}
			flags, e := r.getUint8()
			if e != nil {
				return p, e
			}
			if flags&^uint8(1) != 0 {
				return p, ErrInvalidRemotePreview
			}
			style, e := getPreviewStyle(&r)
			if e != nil {
				return p, e
			}
			p.Cells = append(p.Cells, renderer.Cell{Rune: rune(runeValue), Continuation: flags&1 != 0, Style: style})
		}
	}
	if err := r.done(); err != nil {
		return p, err
	}
	if err := ValidateRemotePreview(p); err != nil {
		return p, err
	}
	return p, nil
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
