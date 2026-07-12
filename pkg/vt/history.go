package vt

import "github.com/bnema/vev/pkg/renderer"

// HistoryConfig controls the bounded terminal history retained by a Screen.
type HistoryConfig struct {
	MaxRows   int
	ChunkRows int
}

// History stores terminal rows in immutable chunks. It is intended to be
// mutated by the owner of a Screen; views are safe to retain after later
// appends.
type History struct {
	maxRows   int
	chunkRows int
	chunks    []*HistoryChunk
	tail      [][]renderer.Cell
	rows      int
}

// HistoryChunk is an immutable group of history rows. Its identity is stable
// and can be used by consumers to avoid copying unchanged sealed chunks.
type HistoryChunk struct {
	rows [][]renderer.Cell
}

// HistoryView is an immutable snapshot of history. Row returns a copy so the
// storage behind a sealed chunk remains owned by VT.
type HistoryView struct {
	chunks []*HistoryChunk
	rows   int
}

func NewHistory(config HistoryConfig) *History {
	if config.MaxRows <= 0 {
		return &History{}
	}
	chunkRows := config.ChunkRows
	if chunkRows <= 0 {
		chunkRows = 256
	}
	chunkRows = min(chunkRows, 256)
	chunkRows = min(chunkRows, config.MaxRows)
	return &History{maxRows: config.MaxRows, chunkRows: chunkRows}
}

// Append records a copy of row. Once a chunk is full it is sealed forever.
func (h *History) Append(row []renderer.Cell) {
	if h == nil || h.maxRows == 0 {
		return
	}
	h.tail = append(h.tail, append([]renderer.Cell(nil), row...))
	h.rows++
	if len(h.tail) == h.chunkRows {
		h.sealTail()
	}
	h.evict()
}

func (h *History) sealTail() {
	if len(h.tail) == 0 {
		return
	}
	h.chunks = append(h.chunks, &HistoryChunk{rows: h.tail})
	h.tail = nil
}

func (h *History) evict() {
	for h.rows > h.maxRows && len(h.chunks) > 0 {
		evicted := h.chunks[0]
		h.rows -= len(evicted.rows)
		copy(h.chunks, h.chunks[1:])
		h.chunks[len(h.chunks)-1] = nil
		h.chunks = h.chunks[:len(h.chunks)-1]
	}
}

// SealAndView rotates the mutable tail into an immutable chunk, then captures
// the sealed chunks by identity. Callers must synchronize access to History.
func (h *History) SealAndView() HistoryView {
	if h != nil {
		h.sealTail()
	}
	return h.View()
}

// View captures the current history. Sealed chunks are shared by identity; a
// partially-filled tail is copied into a new immutable chunk for this view.
func (h *History) View() HistoryView {
	if h == nil || h.rows == 0 {
		return HistoryView{}
	}
	chunks := append([]*HistoryChunk(nil), h.chunks...)
	if len(h.tail) > 0 {
		rows := make([][]renderer.Cell, len(h.tail))
		for i, row := range h.tail {
			rows[i] = append([]renderer.Cell(nil), row...)
		}
		chunks = append(chunks, &HistoryChunk{rows: rows})
	}
	return HistoryView{chunks: chunks, rows: h.rows}
}

// Len returns the currently retained history row count.
func (h *History) Len() int {
	if h == nil {
		return 0
	}
	return h.rows
}

// Cap returns the configured bounded row capacity.
func (h *History) Cap() int {
	if h == nil {
		return 0
	}
	return h.maxRows
}

func (v HistoryView) Len() int        { return v.rows }
func (v HistoryView) ChunkCount() int { return len(v.chunks) }

// Chunk returns the immutable chunk at i, or nil when i is out of range.
func (v HistoryView) Chunk(i int) *HistoryChunk {
	if i < 0 || i >= len(v.chunks) {
		return nil
	}
	return v.chunks[i]
}

// Row returns a copy of the row at i, or nil when i is out of range.
func (v HistoryView) Row(i int) []renderer.Cell {
	if i < 0 {
		return nil
	}
	for _, chunk := range v.chunks {
		if i < len(chunk.rows) {
			return append([]renderer.Cell(nil), chunk.rows[i]...)
		}
		i -= len(chunk.rows)
	}
	return nil
}
