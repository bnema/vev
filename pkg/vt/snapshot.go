package vt

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/bnema/vev/pkg/renderer"
)

const visibleMagic = "VTV1"

// DecodeStats describes resources declared by one canonical VT blob.
type DecodeStats struct {
	Chunks uint64
	Rows   uint64
	Cells  uint64
	Styles uint64
	Bytes  uint64
}

// Add adds another preflight result with overflow checking.
func (s *DecodeStats) Add(other DecodeStats) bool {
	for _, totals := range [][2]*uint64{{&s.Chunks, &other.Chunks}, {&s.Rows, &other.Rows}, {&s.Cells, &other.Cells}, {&s.Styles, &other.Styles}, {&s.Bytes, &other.Bytes}} {
		if math.MaxUint64-*totals[0] < *totals[1] {
			return false
		}
		*totals[0] += *totals[1]
	}
	return true
}

// PreflightHistoryBlob validates one self-contained history blob without
// allocating decoded rows.
func PreflightHistoryBlob(data []byte) (DecodeStats, error) {
	if len(data) < 9 || string(data[:4]) != historyMagic || data[4] != historyVersion {
		return DecodeStats{}, fmt.Errorf("preflight history: %w", errInvalidHistory)
	}
	stats, ok := preflightHistory(data[5:])
	if !ok {
		return DecodeStats{}, fmt.Errorf("preflight history: %w", errInvalidHistory)
	}
	return DecodeStats{Chunks: stats.chunks, Rows: stats.rows, Cells: stats.cells, Styles: stats.styles, Bytes: stats.bytes}, nil
}

// MarshalHistoryChunk encodes one immutable chunk as a self-contained blob.
func MarshalHistoryChunk(chunk *HistoryChunk) ([]byte, error) {
	if chunk == nil {
		return nil, fmt.Errorf("marshal history chunk: %w", errInvalidHistory)
	}
	return MarshalHistory(HistoryView{chunks: []*HistoryChunk{chunk}, rows: len(chunk.rows)})
}

// MarshalEmptyHistoryTail returns the mandatory canonical empty tail blob.
func MarshalEmptyHistoryTail() ([]byte, error) { return MarshalHistory(HistoryView{}) }

// MarshalSealedHistory serializes a SealAndView result as oldest-first,
// self-contained sealed blobs plus a mandatory empty canonical tail blob.
func MarshalSealedHistory(view HistoryView) ([][]byte, []byte, error) {
	if view.rows != historyViewRowCount(view) {
		return nil, nil, fmt.Errorf("marshal sealed history: %w", errInvalidHistory)
	}
	sealed := make([][]byte, len(view.chunks))
	for i, chunk := range view.chunks {
		blob, err := MarshalHistoryChunk(chunk)
		if err != nil {
			return nil, nil, err
		}
		sealed[i] = blob
	}
	tail, err := MarshalEmptyHistoryTail()
	if err != nil {
		return nil, nil, err
	}
	return sealed, tail, nil
}

// HistoryFromBlobs restores history directly from sealed, oldest-first blobs
// and the mandatory tail blob. It never feeds decoded rows through Append.
func HistoryFromBlobs(config HistoryConfig, sealed [][]byte, tail []byte) (*History, error) {
	h := NewHistory(config)
	for _, blob := range sealed {
		view, err := UnmarshalHistory(blob)
		if err != nil || len(view.chunks) != 1 || len(view.chunks[0].rows) == 0 {
			return nil, fmt.Errorf("restore sealed history: %w", errInvalidHistory)
		}
		h.chunks = append(h.chunks, view.chunks[0])
		h.rows += len(view.chunks[0].rows)
	}
	view, err := UnmarshalHistory(tail)
	if err != nil || len(view.chunks) > 1 {
		return nil, fmt.Errorf("restore history tail: %w", errInvalidHistory)
	}
	if len(view.chunks) == 1 {
		h.tail = view.chunks[0].rows
		h.rows += len(h.tail)
	}
	h.evict()
	return h, nil
}

// NewScreenWithRestoredHistory constructs a screen that owns history restored
// directly from canonical blobs. Call RestorePrimaryVisible to install a
// persisted primary frame before making the screen live.
func NewScreenWithRestoredHistory(width, height int, config HistoryConfig, sealed [][]byte, tail []byte) (*Screen, error) {
	history, err := HistoryFromBlobs(config, sealed, tail)
	if err != nil {
		return nil, err
	}
	screen := NewScreenWithHistory(width, height, config)
	screen.history = history
	return screen, nil
}

// MarshalVisible encodes the exact visible primary frame, including every
// blank, row width, and terminal cell attribute.
func MarshalVisible(frame renderer.Frame) ([]byte, error) {
	if frame.Width < 0 || frame.Height < 0 || uint64(frame.Width) > math.MaxUint32 || uint64(frame.Height) > math.MaxUint32 {
		return nil, fmt.Errorf("marshal visible: %w", errInvalidHistory)
	}
	cells := uint64(frame.Width) * uint64(frame.Height)
	bytes, ok := historyCellByteCount(cells)
	if !ok || cells > maxHistoryCells || bytes > maxHistoryDecodedBytes {
		return nil, fmt.Errorf("marshal visible: %w", errInvalidHistory)
	}
	out := make([]byte, 0, 13+bytes)
	out = append(out, visibleMagic...)
	out = append(out, historyVersion)
	out = binary.BigEndian.AppendUint32(out, uint32(frame.Width))
	out = binary.BigEndian.AppendUint32(out, uint32(frame.Height))
	for y := range frame.Height {
		for _, cell := range frame.Row(y) {
			if !validHistoryCell(cell) {
				return nil, fmt.Errorf("marshal visible: %w", errInvalidHistory)
			}
			out = appendHistoryCell(out, cell)
		}
	}
	return out, nil
}

// PreflightVisibleBlob validates dimensions and cell declarations without
// allocating a frame.
func PreflightVisibleBlob(data []byte) (DecodeStats, error) {
	if len(data) < 13 || string(data[:4]) != visibleMagic || data[4] != historyVersion {
		return DecodeStats{}, fmt.Errorf("preflight visible: %w", errInvalidHistory)
	}
	width, height := uint64(binary.BigEndian.Uint32(data[5:9])), uint64(binary.BigEndian.Uint32(data[9:13]))
	if width != 0 && height > math.MaxUint64/width {
		return DecodeStats{}, fmt.Errorf("preflight visible: %w", errInvalidHistory)
	}
	cells := width * height
	bytes, ok := historyCellByteCount(cells)
	if !ok || cells > maxHistoryCells || bytes > maxHistoryDecodedBytes || uint64(len(data)-13) != bytes {
		return DecodeStats{}, fmt.Errorf("preflight visible: %w", errInvalidHistory)
	}
	p := historyParser{data: data[13:]}
	for range cells {
		cell, ok := p.cell()
		if !ok || !validHistoryCell(cell) {
			return DecodeStats{}, fmt.Errorf("preflight visible: %w", errInvalidHistory)
		}
	}
	return DecodeStats{Rows: height, Cells: cells, Styles: cells, Bytes: bytes}, nil
}

// UnmarshalVisible decodes a preflighted exact visible frame.
func UnmarshalVisible(data []byte) (renderer.Frame, error) {
	if _, err := PreflightVisibleBlob(data); err != nil {
		return renderer.Frame{}, err
	}
	width, height := int(binary.BigEndian.Uint32(data[5:9])), int(binary.BigEndian.Uint32(data[9:13]))
	frame := renderer.NewFrame(width, height)
	p := historyParser{data: data[13:]}
	for y := range height {
		for x := range width {
			cell, _ := p.cell()
			frame.Set(x, y, cell)
		}
	}
	return frame, nil
}

// PrimaryVisibleFrame returns a deep copy of the exact visible primary frame.
func (s *Screen) PrimaryVisibleFrame() renderer.Frame {
	if s.alternate != nil {
		return s.alternate.frame.Clone()
	}
	return s.Frame.Clone()
}

// PrimaryVisibleRows returns a deep copy of the visible primary-screen rows.
// When the alternate screen is active, it snapshots the saved primary screen
// rather than the currently displayed alternate frame.
func (s *Screen) PrimaryVisibleRows() [][]renderer.Cell {
	frame := s.Frame
	if s.alternate != nil {
		frame = s.alternate.frame
	}
	rows := make([][]renderer.Cell, frame.Height)
	for y := range frame.Height {
		rows[y] = append([]renderer.Cell(nil), frame.Row(y)...)
	}
	return rows
}

// RestorePrimaryVisible replaces the primary frame from an exact visible blob.
func (s *Screen) RestorePrimaryVisible(blob []byte) error {
	frame, err := UnmarshalVisible(blob)
	if err != nil {
		return err
	}
	// A persisted primary frame is authoritative: restoring it directly keeps
	// exact row widths and height even when a collapsed layout chose a smaller
	// initial PTY size. A subsequent resize reconciles it with live geometry.
	if s.alternate != nil {
		s.alternate.frame = frame
	} else {
		s.Frame = frame
	}
	s.fullRedraw()
	return nil
}
