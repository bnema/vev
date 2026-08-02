package vt

import (
	"errors"
	"math"
	"slices"

	"github.com/bnema/vev/pkg/renderer"
)

// ErrHistoryRowTooWide is returned when a row cannot fit within the configured
// cell budget. The history is not modified when this error is returned.
var ErrHistoryRowTooWide = errors.New("history row exceeds cell capacity")

var errInvalidHistoryRowID = errors.New("invalid history row ID")

// HistoryConfig controls the bounded terminal history retained by a Screen.
type HistoryConfig struct {
	MaxRows   int
	MaxCells  int
	ChunkRows int
}

// History stores terminal rows in immutable chunks. It is intended to be
// mutated by the owner of a Screen; views are safe to retain after later
// appends.
type History struct {
	maxRows    int
	maxCells   int
	chunkRows  int
	chunks     []*HistoryChunk
	tail       [][]renderer.Cell
	tailBounds []LineBound
	tailIDs    []RowID
	rows       int
	cells      int
	nextRowID  RowID
}

// HistoryChunk is an immutable group of history rows. Its identity is stable
// and can be used by consumers to avoid copying unchanged sealed chunks.
// bounds is parallel to rows and always has the same length.
type HistoryChunk struct {
	rows   [][]renderer.Cell
	bounds []LineBound
	rowIDs []RowID
}

// HistoryView is an immutable snapshot of history. Row returns a copy so the
// storage behind a sealed chunk remains owned by VT.
type HistoryView struct {
	chunks    []*HistoryChunk
	rows      int
	cells     int
	nextRowID RowID
}

// HistorySnapshotView captures sealed history chunks and the mutable tail
// independently. Sealed chunks are shared by identity; Tail is owned by the
// view and can be serialized without rotating the live tail into a chunk.
type HistorySnapshotView struct {
	chunks     []*HistoryChunk
	tail       [][]renderer.Cell
	tailBounds []LineBound
	tailIDs    []RowID
	rows       int
	cells      int
	nextRowID  RowID
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
	maxCells := config.MaxCells
	if maxCells <= 0 {
		maxCells = defaultHistoryMaxCells(config.MaxRows)
	}
	return &History{maxRows: config.MaxRows, maxCells: maxCells, chunkRows: chunkRows, nextRowID: 1}
}

func defaultHistoryMaxCells(maxRows int) int {
	if maxRows > math.MaxInt/160 {
		return math.MaxInt
	}
	return maxRows * 160
}

// Append records a copy of row along with its logical extent and an
// automatically allocated nonzero ID. Once a chunk is full it is sealed
// forever. Rows wider than the total cell capacity are rejected without
// mutation.
func (h *History) Append(row []renderer.Cell, bound LineBound) error {
	if h == nil || h.maxRows == 0 {
		return nil
	}
	if len(row) > h.maxCells {
		return ErrHistoryRowTooWide
	}
	id, err := h.allocateRowID()
	if err != nil {
		return err
	}
	return h.appendRow(row, bound, id)
}

// AppendWithID records a copy of row with an explicit persisted identity.
// Unlike Append, malformed identities are rejected rather than synthesized.
func (h *History) AppendWithID(row []renderer.Cell, bound LineBound, id RowID) error {
	if id == 0 || id >= ^RowID(0)-1 {
		return errInvalidHistoryRowID
	}
	if h == nil || h.maxRows == 0 {
		return nil
	}
	if len(row) > h.maxCells {
		return ErrHistoryRowTooWide
	}
	if h.hasRowID(id) {
		return errInvalidHistoryRowID
	}
	return h.appendRow(row, bound, id)
}

func (h *History) appendRow(row []renderer.Cell, bound LineBound, id RowID) error {
	if h.nextRowID == ^RowID(0) {
		return errInvalidHistoryRowID
	}
	if len(row) > h.maxCells {
		return ErrHistoryRowTooWide
	}
	if id >= ^RowID(0)-1 {
		return errInvalidHistoryRowID
	}
	if id >= h.nextRowID {
		h.nextRowID = id + 1
	}
	// Make room before adding so Cells cannot overflow even when the default
	// capacity is MaxInt.
	h.evictFor(len(row))
	h.tail = append(h.tail, append([]renderer.Cell(nil), row...))
	h.tailBounds = append(h.tailBounds, clampBound(bound, len(row)))
	h.tailIDs = append(h.tailIDs, id)
	h.rows++
	h.cells += len(row)
	if len(h.tail) == h.chunkRows {
		h.sealTail()
	}
	return nil
}

func (h *History) allocateRowID() (RowID, error) {
	if h.nextRowID == 0 {
		h.nextRowID = 1
	}
	for h.hasRowID(h.nextRowID) {
		if h.nextRowID >= ^RowID(0)-1 {
			return 0, errInvalidHistoryRowID
		}
		h.nextRowID++
	}
	id := h.nextRowID
	if h.nextRowID >= ^RowID(0)-1 {
		return 0, errInvalidHistoryRowID
	}
	h.nextRowID++
	return id, nil
}

func (h *History) hasRowID(id RowID) bool {
	for _, chunk := range h.chunks {
		if slices.Contains(chunk.rowIDs, id) {
			return true
		}
	}
	return slices.Contains(h.tailIDs, id)
}

// clampBound keeps End inside the row it describes so a persisted bound can be
// validated without consulting its row again.
func clampBound(b LineBound, cells int) LineBound {
	b.End = min(max(b.End, 0), cells)
	return b
}

// growBounds returns bounds sized to rows, padding with zero values. It keeps
// the parallel-slice invariant when a chunk arrives with fewer bounds than rows.
func growBounds(bounds []LineBound, rows int) []LineBound {
	if len(bounds) == rows {
		return bounds
	}
	out := make([]LineBound, rows)
	copy(out, bounds)
	return out
}

func growRowIDs(ids []RowID, rows int) []RowID {
	if len(ids) == rows {
		return ids
	}
	out := make([]RowID, rows)
	copy(out, ids)
	return out
}

func cloneRowIDs(ids []RowID, rows int) []RowID {
	out := make([]RowID, rows)
	copy(out, ids)
	return out
}

func (h *History) sealTail() {
	if len(h.tail) == 0 {
		return
	}
	h.chunks = append(h.chunks, &HistoryChunk{
		rows:   h.tail,
		bounds: growBounds(h.tailBounds, len(h.tail)),
		rowIDs: h.tailIDs,
	})
	h.tail = nil
	h.tailBounds = nil
	h.tailIDs = nil
}

// normalizeTail seals complete chunks so the mutable tail remains shorter than
// chunkRows. Restored snapshots may have been written with a different chunk
// size than the current history configuration.
func (h *History) normalizeTail() {
	h.tailIDs = growRowIDs(h.tailIDs, len(h.tail))
	for h.chunkRows > 0 && len(h.tail) >= h.chunkRows {
		bounds := growBounds(h.tailBounds, len(h.tail))
		h.chunks = append(h.chunks, &HistoryChunk{
			rows:   h.tail[:h.chunkRows],
			bounds: bounds[:h.chunkRows],
			rowIDs: h.tailIDs[:h.chunkRows],
		})
		h.tail = h.tail[h.chunkRows:]
		h.tailBounds = bounds[h.chunkRows:]
		h.tailIDs = h.tailIDs[h.chunkRows:]
	}
}

func (h *History) evict() {
	h.evictUntil(0, 0)
}

// evictFor discards oldest rows until another row using cellCount cells can fit.
func (h *History) evictFor(cellCount int) {
	h.evictUntil(1, cellCount)
}

// evictUntil makes room for rowCount rows and cellCount cells without
// overflowing either accounting total.
func (h *History) evictUntil(rowCount, cellCount int) {
	for h.rows > h.maxRows-rowCount || h.cells > h.maxCells-cellCount {
		if len(h.chunks) > 0 {
			chunk := h.chunks[0]
			row := chunk.rows[0]
			h.rows--
			h.cells -= len(row)
			if len(chunk.rows) == 1 {
				copy(h.chunks, h.chunks[1:])
				h.chunks[len(h.chunks)-1] = nil
				h.chunks = h.chunks[:len(h.chunks)-1]
			} else {
				// Preserve cell storage while replacing only the chunk wrapper: a
				// retained view may still refer to the original immutable chunk.
				bounds := growBounds(chunk.bounds, len(chunk.rows))
				rowIDs := growRowIDs(chunk.rowIDs, len(chunk.rows))
				h.chunks[0] = &HistoryChunk{
					rows:   chunk.rows[1:],
					bounds: bounds[1:],
					rowIDs: rowIDs[1:],
				}
			}
			continue
		}
		if len(h.tail) == 0 {
			return
		}
		row := h.tail[0]
		h.rows--
		h.cells -= len(row)
		h.tail[0] = nil
		h.tail = h.tail[1:]
		h.tailBounds = growBounds(h.tailBounds, len(h.tail)+1)[1:]
		h.tailIDs = growRowIDs(h.tailIDs, len(h.tail)+1)[1:]
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
	if h == nil {
		return HistoryView{}
	}
	if h.rows == 0 {
		return HistoryView{nextRowID: h.nextRowID}
	}
	chunks := make([]*HistoryChunk, len(h.chunks), len(h.chunks)+1)
	copy(chunks, h.chunks)
	if len(h.tail) > 0 {
		chunks = append(chunks, &HistoryChunk{
			rows:   cloneHistoryRows(h.tail),
			bounds: append([]LineBound(nil), growBounds(h.tailBounds, len(h.tail))...),
			rowIDs: cloneRowIDs(h.tailIDs, len(h.tail)),
		})
	}
	return HistoryView{chunks: chunks, rows: h.rows, cells: h.cells, nextRowID: h.nextRowID}
}

// SnapshotView captures history for persistence without sealing the mutable
// tail. Sealed chunks are shared by identity and the tail is deeply copied.
func (h *History) SnapshotView() HistorySnapshotView {
	if h == nil {
		return HistorySnapshotView{}
	}
	if h.rows == 0 {
		return HistorySnapshotView{nextRowID: h.nextRowID}
	}
	return HistorySnapshotView{
		chunks:     append([]*HistoryChunk(nil), h.chunks...),
		tail:       cloneHistoryRows(h.tail),
		tailBounds: append([]LineBound(nil), growBounds(h.tailBounds, len(h.tail))...),
		tailIDs:    cloneRowIDs(h.tailIDs, len(h.tail)),
		rows:       h.rows,
		cells:      h.cells,
		nextRowID:  h.nextRowID,
	}
}

func cloneHistoryRows(rows [][]renderer.Cell) [][]renderer.Cell {
	if len(rows) == 0 {
		return nil
	}
	copyRows := make([][]renderer.Cell, len(rows))
	for i, row := range rows {
		copyRows[i] = append([]renderer.Cell(nil), row...)
	}
	return copyRows
}

// Len returns the currently retained history row count.
func (h *History) Len() int {
	if h == nil {
		return 0
	}
	return h.rows
}

// Cells returns the currently retained history cell count.
func (h *History) Cells() int {
	if h == nil {
		return 0
	}
	return h.cells
}

// NextRowID returns the next identity allocated by this history.
func (h *History) NextRowID() RowID {
	if h == nil || h.nextRowID == 0 {
		return 1
	}
	return h.nextRowID
}

// Cap returns the configured bounded row capacity.
func (h *History) Cap() int {
	if h == nil {
		return 0
	}
	return h.maxRows
}

// CellCap returns the configured bounded cell capacity.
func (h *History) CellCap() int {
	if h == nil {
		return 0
	}
	return h.maxCells
}

func (v HistoryView) Len() int        { return v.rows }
func (v HistoryView) Cells() int      { return v.cells }
func (v HistoryView) ChunkCount() int { return len(v.chunks) }

// NextRowID returns the next identity recorded by this view.
func (v HistoryView) NextRowID() RowID {
	if v.nextRowID == 0 {
		next, ok := historyViewNextRowID(v)
		if !ok {
			return 1
		}
		return next
	}
	return v.nextRowID
}

// Chunk returns the immutable chunk at i, or nil when i is out of range.
func (v HistoryView) Chunk(i int) *HistoryChunk {
	if i < 0 || i >= len(v.chunks) {
		return nil
	}
	return v.chunks[i]
}

// Range calls yield for each row in oldest-first order. The row is borrowed
// immutable storage: yield must not mutate or retain it after returning.
func (v HistoryView) Range(yield func([]renderer.Cell) bool) {
	for _, chunk := range v.chunks {
		for _, row := range chunk.rows {
			if !yield(row) {
				return
			}
		}
	}
}

// Row returns a copy of the row at i, or nil when i is out of range.
func (v HistoryView) Row(i int) []renderer.Cell {
	return append([]renderer.Cell(nil), v.BorrowedRow(i)...)
}

// BorrowedRow returns immutable storage for the row at i, or nil when i is out
// of range. The caller must not mutate or retain the result after it no longer
// retains v. Consumers that need ownership should use Row instead.
func (v HistoryView) BorrowedRow(i int) []renderer.Cell {
	if i < 0 {
		return nil
	}
	for _, chunk := range v.chunks {
		if i < len(chunk.rows) {
			return chunk.rows[i]
		}
		i -= len(chunk.rows)
	}
	return nil
}

// RowID returns the identity of the row at i, or zero when i is out of range.
func (v HistoryView) RowID(i int) RowID {
	if i < 0 {
		return 0
	}
	for _, chunk := range v.chunks {
		if i < len(chunk.rows) {
			if i < len(chunk.rowIDs) {
				return chunk.rowIDs[i]
			}
			return 0
		}
		i -= len(chunk.rows)
	}
	return 0
}

// FindRowID returns the oldest-first index of id, or -1 when id is absent.
func (v HistoryView) FindRowID(id RowID) int {
	if id == 0 {
		return -1
	}
	index := 0
	for _, chunk := range v.chunks {
		for row := range chunk.rows {
			if row < len(chunk.rowIDs) && chunk.rowIDs[row] == id {
				return index
			}
			index++
		}
	}
	return -1
}

// RowID returns the identity of the row within an immutable chunk.
func (c *HistoryChunk) RowID(i int) RowID {
	if c == nil || i < 0 || i >= len(c.rowIDs) {
		return 0
	}
	return c.rowIDs[i]
}

// Bound returns the logical extent of the row at i, or the zero value when i is
// out of range. A zero value describes a hard row whose End is not meaningful.
func (v HistoryView) Bound(i int) LineBound {
	if i < 0 {
		return LineBound{}
	}
	for _, chunk := range v.chunks {
		if i < len(chunk.rows) {
			if i < len(chunk.bounds) {
				return chunk.bounds[i]
			}
			return LineBound{}
		}
		i -= len(chunk.rows)
	}
	return LineBound{}
}

func (v HistorySnapshotView) Len() int        { return v.rows }
func (v HistorySnapshotView) Cells() int      { return v.cells }
func (v HistorySnapshotView) ChunkCount() int { return len(v.chunks) }

// NextRowID returns the next identity recorded by this snapshot.
func (v HistorySnapshotView) NextRowID() RowID {
	if v.nextRowID == 0 {
		return v.Tail().NextRowID()
	}
	return v.nextRowID
}

// Chunk returns a sealed immutable chunk at i, or nil when i is out of range.
func (v HistorySnapshotView) Chunk(i int) *HistoryChunk {
	if i < 0 || i >= len(v.chunks) {
		return nil
	}
	return v.chunks[i]
}

// Tail returns an immutable view of the copied mutable tail.
func (v HistorySnapshotView) Tail() HistoryView {
	if len(v.tail) == 0 {
		return HistoryView{nextRowID: v.nextRowID}
	}
	return HistoryView{
		chunks:    []*HistoryChunk{{rows: v.tail, bounds: growBounds(v.tailBounds, len(v.tail)), rowIDs: growRowIDs(v.tailIDs, len(v.tail))}},
		rows:      len(v.tail),
		cells:     historyRowsCells(v.tail),
		nextRowID: v.nextRowID,
	}
}

func historyRowsCells(rows [][]renderer.Cell) int {
	cells := 0
	for _, row := range rows {
		cells += len(row)
	}
	return cells
}
