package vt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/bnema/vev/pkg/renderer"
)

const (
	historyMagic        = "VTH1"
	historyVersion      = 2
	historyCellBytes    = 41
	maxHistoryChunkRows = 256

	// The daemon retains 10,000 history rows. Support a 20% row margin and
	// 160-cell rows, which leaves headroom above the representative 10k×120
	// terminal while bounding snapshot allocation.
	maxHistoryChunks   = 12_000
	maxHistoryRows     = 12_000
	maxHistoryRowCells = 160
	maxHistoryCells    = maxHistoryRows * maxHistoryRowCells

	maxHistoryDecodeStyles  = maxHistoryCells
	maxHistoryDecodedBytes  = maxHistoryCells * historyCellBytes
	maxVisibleBoundaryBytes = maxHistoryRows * visibleBoundaryBytes
)

var errInvalidHistory = errors.New("invalid history payload")

// MarshalHistory encodes a HistoryView in a deterministic, self-contained
// format. It preserves chunk boundaries as well as every Cell and Style field.
func MarshalHistory(view HistoryView) ([]byte, error) {
	if view.rows < 0 || view.rows != historyViewRowCount(view) || uint64(len(view.chunks)) > math.MaxUint32 || uint64(len(view.chunks)) > maxHistoryChunks {
		return nil, fmt.Errorf("marshal history: %w", errInvalidHistory)
	}
	stats := historyDecodeStats{}
	for _, chunk := range view.chunks {
		if chunk == nil || len(chunk.rows) == 0 || len(chunk.rows) > maxHistoryChunkRows ||
			!addHistoryDecodeBudget(&stats.rows, uint64(len(chunk.rows)), maxHistoryRows) {
			return nil, fmt.Errorf("marshal history: %w", errInvalidHistory)
		}
		for _, row := range chunk.rows {
			cellCount := uint64(len(row))
			cellBytes, ok := historyCellByteCount(cellCount)
			if !ok ||
				!addHistoryDecodeBudget(&stats.cells, cellCount, maxHistoryCells) ||
				!addHistoryDecodeBudget(&stats.styles, cellCount, maxHistoryDecodeStyles) ||
				!addHistoryDecodeBudget(&stats.bytes, cellBytes, maxHistoryDecodedBytes) {
				return nil, fmt.Errorf("marshal history: %w", errInvalidHistory)
			}
		}
	}

	out := make([]byte, 0, 9)
	out = append(out, historyMagic...)
	out = append(out, historyVersion)
	out = appendUint32(out, uint32(len(view.chunks)))
	for _, chunk := range view.chunks {
		if chunk == nil || len(chunk.rows) == 0 || len(chunk.rows) > maxHistoryChunkRows {
			return nil, fmt.Errorf("marshal history: %w", errInvalidHistory)
		}
		out = appendUint32(out, uint32(len(chunk.rows)))
		for i, row := range chunk.rows {
			if uint64(len(row)) > math.MaxUint32 {
				return nil, fmt.Errorf("marshal history: %w", errInvalidHistory)
			}
			bound := chunkBound(chunk, i)
			if !validHistoryBound(bound, len(row)) {
				return nil, fmt.Errorf("marshal history: %w", errInvalidHistory)
			}
			out = appendUint32(out, uint32(len(row)))
			for _, cell := range row {
				if !validHistoryCell(cell) {
					return nil, fmt.Errorf("marshal history: %w", errInvalidHistory)
				}
				out = appendHistoryCell(out, cell)
			}
			out = appendHistoryBound(out, bound)
		}
	}
	return out, nil
}

func historyViewRowCount(view HistoryView) int {
	n := 0
	for _, chunk := range view.chunks {
		if chunk != nil && len(chunk.rows) <= math.MaxInt-n {
			n += len(chunk.rows)
			continue
		}
		return -1
	}
	return n
}

func appendUint32(dst []byte, v uint32) []byte {
	return binary.BigEndian.AppendUint32(dst, v)
}

func appendHistoryCell(dst []byte, cell renderer.Cell) []byte {
	style := cell.Style
	dst = binary.BigEndian.AppendUint32(dst, uint32(cell.Rune))
	flags := byte(0)
	if cell.Continuation {
		flags |= 1 << 0
	}
	if style.Bold {
		flags |= 1 << 1
	}
	if style.Italic {
		flags |= 1 << 2
	}
	if style.Inverse {
		flags |= 1 << 3
	}
	if style.HasForegroundRGB {
		flags |= 1 << 4
	}
	if style.HasBackgroundRGB {
		flags |= 1 << 5
	}
	if style.HasUnderlineColor {
		flags |= 1 << 6
	}
	if style.HasUnderlineColorRGB {
		flags |= 1 << 7
	}
	dst = append(dst, flags)
	dst = binary.BigEndian.AppendUint16(dst, uint16(style.Attrs))
	dst = binary.BigEndian.AppendUint64(dst, uint64(int64(style.Foreground)))
	dst = binary.BigEndian.AppendUint64(dst, uint64(int64(style.Background)))
	dst = append(dst, style.ForegroundRGB.R, style.ForegroundRGB.G, style.ForegroundRGB.B)
	dst = append(dst, style.BackgroundRGB.R, style.BackgroundRGB.G, style.BackgroundRGB.B)
	dst = append(dst, byte(style.UnderlineStyle))
	dst = binary.BigEndian.AppendUint64(dst, uint64(int64(style.UnderlineColor)))
	return append(dst, style.UnderlineColorRGB.R, style.UnderlineColorRGB.G, style.UnderlineColorRGB.B)
}

func chunkBound(chunk *HistoryChunk, i int) LineBound {
	if i < 0 || i >= len(chunk.bounds) {
		return LineBound{}
	}
	return chunk.bounds[i]
}

func appendHistoryBound(dst []byte, b LineBound) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(b.End))
	if b.Soft {
		return append(dst, 1)
	}
	return append(dst, 0)
}

// validHistoryBound rejects extents that cannot describe their own row.
func validHistoryBound(b LineBound, cells int) bool {
	return b.End >= 0 && b.End <= cells
}

// UnmarshalHistory strictly decodes a MarshalHistory payload. It rejects
// malformed declarations, cells, truncated data, and trailing bytes.
func UnmarshalHistory(data []byte) (HistoryView, error) {
	view, _, ok := parseHistory(data, true)
	if !ok {
		return HistoryView{}, fmt.Errorf("unmarshal history: %w", errInvalidHistory)
	}
	return view, nil
}

type historyDecodeStats struct {
	chunks uint64
	rows   uint64
	cells  uint64
	styles uint64
	bytes  uint64
}

// preflightHistory validates a history blob without allocating decoded rows.
// It shares parseHistory with UnmarshalHistory so both paths enforce the same
// structural, cell, and aggregate-budget constraints.
func preflightHistory(data []byte) (historyDecodeStats, bool) {
	_, stats, ok := parseHistory(data, false)
	return stats, ok
}

// parseHistory is the authoritative history payload validation path. populate
// controls whether validated cells are retained in a HistoryView.
func parseHistory(data []byte, populate bool) (HistoryView, historyDecodeStats, bool) {
	if len(data) < 9 || string(data[:4]) != historyMagic || data[4] != historyVersion {
		return HistoryView{}, historyDecodeStats{}, false
	}

	p := historyParser{data: data[5:]}
	chunkCount, ok := p.uint32()
	if !ok || uint64(chunkCount) > maxHistoryChunks || uint64(chunkCount) > uint64(len(p.data))/4 {
		return HistoryView{}, historyDecodeStats{}, false
	}
	stats := historyDecodeStats{chunks: uint64(chunkCount)}
	var chunks []*HistoryChunk
	if populate {
		chunks = make([]*HistoryChunk, 0, chunkCount)
	}
	for range chunkCount {
		rowCount, ok := p.uint32()
		if !ok || rowCount == 0 || rowCount > maxHistoryChunkRows ||
			uint64(rowCount) > uint64(len(p.data))/(4+visibleBoundaryBytes) ||
			!addHistoryDecodeBudget(&stats.rows, uint64(rowCount), maxHistoryRows) {
			return HistoryView{}, historyDecodeStats{}, false
		}

		var rows [][]renderer.Cell
		var bounds []LineBound
		if populate {
			rows = make([][]renderer.Cell, 0, rowCount)
			bounds = make([]LineBound, 0, rowCount)
		}
		for range rowCount {
			cellCount, ok := p.uint32()
			cellBytes, validCellBytes := historyCellByteCount(uint64(cellCount))
			if !ok || !validCellBytes ||
				!addHistoryDecodeBudget(&stats.cells, uint64(cellCount), maxHistoryCells) ||
				!addHistoryDecodeBudget(&stats.styles, uint64(cellCount), maxHistoryDecodeStyles) ||
				!addHistoryDecodeBudget(&stats.bytes, cellBytes, maxHistoryDecodedBytes) ||
				uint64(cellCount) > uint64(len(p.data))/historyCellBytes {
				return HistoryView{}, historyDecodeStats{}, false
			}

			var row []renderer.Cell
			if populate {
				row = make([]renderer.Cell, cellCount)
			}
			for i := range cellCount {
				cell, ok := p.cell()
				if !ok || !validHistoryCell(cell) {
					return HistoryView{}, historyDecodeStats{}, false
				}
				if populate {
					row[i] = cell
				}
			}
			bound, ok := p.bound()
			if !ok || !validHistoryBound(bound, int(cellCount)) {
				return HistoryView{}, historyDecodeStats{}, false
			}
			if populate {
				rows = append(rows, row)
				bounds = append(bounds, bound)
			}
		}
		if populate {
			chunks = append(chunks, &HistoryChunk{rows: rows, bounds: bounds})
		}
	}
	if len(p.data) != 0 {
		return HistoryView{}, historyDecodeStats{}, false
	}
	return HistoryView{chunks: chunks, rows: int(stats.rows), cells: int(stats.cells)}, stats, true
}

func historyCellByteCount(cellCount uint64) (uint64, bool) {
	if cellCount > math.MaxUint64/historyCellBytes {
		return 0, false
	}
	return cellCount * historyCellBytes, true
}

func addHistoryDecodeBudget(total *uint64, amount, limit uint64) bool {
	if *total > limit || amount > limit-*total {
		return false
	}
	*total += amount
	return true
}

type historyParser struct{ data []byte }

func (p *historyParser) uint32() (uint32, bool) {
	if len(p.data) < 4 {
		return 0, false
	}
	v := binary.BigEndian.Uint32(p.data)
	p.data = p.data[4:]
	return v, true
}

func (p *historyParser) cell() (renderer.Cell, bool) {
	if len(p.data) < historyCellBytes {
		return renderer.Cell{}, false
	}
	b := p.data[:historyCellBytes]
	p.data = p.data[historyCellBytes:]
	foreground, ok := historyInt(binary.BigEndian.Uint64(b[7:15]))
	if !ok {
		return renderer.Cell{}, false
	}
	background, ok := historyInt(binary.BigEndian.Uint64(b[15:23]))
	if !ok {
		return renderer.Cell{}, false
	}
	underlineColor, ok := historyInt(binary.BigEndian.Uint64(b[30:38]))
	if !ok {
		return renderer.Cell{}, false
	}
	flags := b[4]
	return renderer.Cell{
		Rune:         rune(binary.BigEndian.Uint32(b[0:4])),
		Continuation: flags&1 != 0,
		Style: renderer.Style{
			Bold:                 flags&(1<<1) != 0,
			Italic:               flags&(1<<2) != 0,
			Inverse:              flags&(1<<3) != 0,
			Attrs:                renderer.StyleAttrs(binary.BigEndian.Uint16(b[5:7])),
			Foreground:           foreground,
			Background:           background,
			HasForegroundRGB:     flags&(1<<4) != 0,
			ForegroundRGB:        renderer.RGB{R: b[23], G: b[24], B: b[25]},
			HasBackgroundRGB:     flags&(1<<5) != 0,
			BackgroundRGB:        renderer.RGB{R: b[26], G: b[27], B: b[28]},
			UnderlineStyle:       renderer.UnderlineStyle(b[29]),
			HasUnderlineColor:    flags&(1<<6) != 0,
			UnderlineColor:       underlineColor,
			HasUnderlineColorRGB: flags&(1<<7) != 0,
			UnderlineColorRGB:    renderer.RGB{R: b[38], G: b[39], B: b[40]},
		},
	}, true
}

func (p *historyParser) bound() (LineBound, bool) {
	if len(p.data) < visibleBoundaryBytes {
		return LineBound{}, false
	}
	end := binary.BigEndian.Uint32(p.data[:4])
	flag := p.data[4]
	p.data = p.data[visibleBoundaryBytes:]
	if flag > 1 || uint64(end) > math.MaxInt32 {
		return LineBound{}, false
	}
	return LineBound{End: int(end), Soft: flag == 1}, true
}

func historyInt(raw uint64) (int, bool) {
	v := int64(raw)
	if int64(int(v)) != v {
		return 0, false
	}
	return int(v), true
}

func validHistoryCell(cell renderer.Cell) bool {
	return utf8.ValidRune(cell.Rune) && cell.Style.UnderlineStyle <= renderer.UnderlineDashed
}
