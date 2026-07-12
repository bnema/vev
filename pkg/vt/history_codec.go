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
	historyVersion      = 1
	historyCellBytes    = 41
	maxHistoryChunkRows = 256

	// Preserve ten-thousand-row histories while bounding untrusted snapshot
	// payloads. The cell limit permits an average width of 100 cells per row.
	maxHistoryDecodeChunks = 10_000
	maxHistoryDecodeRows   = 10_000
	maxHistoryDecodeCells  = 1_000_000
	maxHistoryDecodeStyles = maxHistoryDecodeCells
	maxHistoryDecodedBytes = 64 << 20
)

var errInvalidHistory = errors.New("invalid history payload")

// MarshalHistory encodes a HistoryView in a deterministic, self-contained
// format. It preserves chunk boundaries as well as every Cell and Style field.
func MarshalHistory(view HistoryView) ([]byte, error) {
	if view.rows < 0 || view.rows != historyViewRowCount(view) || uint64(len(view.chunks)) > math.MaxUint32 {
		return nil, fmt.Errorf("marshal history: %w", errInvalidHistory)
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
		for _, row := range chunk.rows {
			if uint64(len(row)) > math.MaxUint32 {
				return nil, fmt.Errorf("marshal history: %w", errInvalidHistory)
			}
			out = appendUint32(out, uint32(len(row)))
			for _, cell := range row {
				if !validHistoryCell(cell) {
					return nil, fmt.Errorf("marshal history: %w", errInvalidHistory)
				}
				out = appendHistoryCell(out, cell)
			}
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

// UnmarshalHistory strictly decodes a MarshalHistory payload. It rejects
// malformed declarations, truncated data, and trailing bytes.
func UnmarshalHistory(data []byte) (HistoryView, error) {
	if len(data) < 9 || string(data[:4]) != historyMagic || data[4] != historyVersion {
		return HistoryView{}, fmt.Errorf("unmarshal history: %w", errInvalidHistory)
	}
	if _, ok := preflightHistory(data[5:]); !ok {
		return HistoryView{}, fmt.Errorf("unmarshal history: %w", errInvalidHistory)
	}

	p := historyParser{data: data[5:]}
	chunkCount, ok := p.uint32()
	if !ok || uint64(chunkCount) > uint64(len(p.data))/4 {
		return HistoryView{}, fmt.Errorf("unmarshal history: %w", errInvalidHistory)
	}
	chunks := make([]*HistoryChunk, 0, chunkCount)
	rowsTotal := 0
	for range chunkCount {
		rowCount, ok := p.uint32()
		if !ok || rowCount == 0 || rowCount > maxHistoryChunkRows || uint64(rowCount) > uint64(len(p.data))/4 {
			return HistoryView{}, fmt.Errorf("unmarshal history: %w", errInvalidHistory)
		}
		if rowsTotal > math.MaxInt-int(rowCount) {
			return HistoryView{}, fmt.Errorf("unmarshal history: %w", errInvalidHistory)
		}
		rows := make([][]renderer.Cell, 0, rowCount)
		for range rowCount {
			cellCount, ok := p.uint32()
			if !ok || uint64(cellCount) > uint64(len(p.data))/historyCellBytes || uint64(cellCount) > uint64(math.MaxInt) {
				return HistoryView{}, fmt.Errorf("unmarshal history: %w", errInvalidHistory)
			}
			row := make([]renderer.Cell, cellCount)
			for i := range row {
				cell, ok := p.cell()
				if !ok || !validHistoryCell(cell) {
					return HistoryView{}, fmt.Errorf("unmarshal history: %w", errInvalidHistory)
				}
				row[i] = cell
			}
			rows = append(rows, row)
		}
		rowsTotal += int(rowCount)
		chunks = append(chunks, &HistoryChunk{rows: rows})
	}
	if len(p.data) != 0 {
		return HistoryView{}, fmt.Errorf("unmarshal history: %w", errInvalidHistory)
	}
	return HistoryView{chunks: chunks, rows: rowsTotal}, nil
}

type historyDecodeStats struct {
	chunks uint64
	rows   uint64
	cells  uint64
	styles uint64
	bytes  uint64
}

// preflightHistory validates declarations and aggregate budgets without
// allocating decoded history. Its totals can be composed by snapshot decoders.
func preflightHistory(data []byte) (historyDecodeStats, bool) {
	p := historyParser{data: data}
	chunkCount, ok := p.uint32()
	if !ok || uint64(chunkCount) > maxHistoryDecodeChunks || uint64(chunkCount) > uint64(len(p.data))/4 {
		return historyDecodeStats{}, false
	}
	stats := historyDecodeStats{chunks: uint64(chunkCount)}
	for range chunkCount {
		rowCount, ok := p.uint32()
		if !ok || rowCount == 0 || rowCount > maxHistoryChunkRows || uint64(rowCount) > uint64(len(p.data))/4 || !addHistoryDecodeBudget(&stats.rows, uint64(rowCount), maxHistoryDecodeRows) {
			return historyDecodeStats{}, false
		}
		for range rowCount {
			cellCount, ok := p.uint32()
			if !ok || uint64(cellCount) > uint64(len(p.data))/historyCellBytes {
				return historyDecodeStats{}, false
			}
			cellBytes := uint64(cellCount) * historyCellBytes
			if !addHistoryDecodeBudget(&stats.cells, uint64(cellCount), maxHistoryDecodeCells) ||
				!addHistoryDecodeBudget(&stats.styles, uint64(cellCount), maxHistoryDecodeStyles) ||
				!addHistoryDecodeBudget(&stats.bytes, cellBytes, maxHistoryDecodedBytes) {
				return historyDecodeStats{}, false
			}
			p.data = p.data[int(cellBytes):]
		}
	}
	return stats, len(p.data) == 0
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
