package vt

import "github.com/bnema/vev/pkg/renderer"

// RecoveryTranscriptSnapshot is an owned, immutable capture of the viewport
// rows that should be replayed after retained terminal history during recovery.
type RecoveryTranscriptSnapshot struct {
	segments []recoveryTranscriptSegment
}

type recoveryTranscriptSegment struct {
	rows   [][]renderer.Cell
	bounds []LineBound
}

// RecoveryTranscriptSnapshot captures the active primary viewport, or the
// saved primary viewport followed by the active alternate viewport.
func (s *Screen) RecoveryTranscriptSnapshot() RecoveryTranscriptSnapshot {
	if s == nil {
		return RecoveryTranscriptSnapshot{}
	}

	buffers := []*buffer{s.buffer}
	if s.alternate != nil {
		buffers = []*buffer{s.alternate.buffer, s.buffer}
	}

	snapshot := RecoveryTranscriptSnapshot{segments: make([]recoveryTranscriptSegment, 0, len(buffers))}
	for _, b := range buffers {
		segment := captureRecoveryTranscriptSegment(b)
		if len(segment.rows) > 0 {
			snapshot.segments = append(snapshot.segments, segment)
		}
	}
	return snapshot
}

func captureRecoveryTranscriptSegment(b *buffer) recoveryTranscriptSegment {
	if b == nil {
		return recoveryTranscriptSegment{}
	}

	rowCount := b.frame.Height
	for rowCount > 0 && recoveryTranscriptRowUntouched(b.frame.Row(rowCount-1), b.bound(rowCount-1)) {
		rowCount--
	}
	if rowCount == 0 {
		return recoveryTranscriptSegment{}
	}

	segment := recoveryTranscriptSegment{
		rows:   make([][]renderer.Cell, rowCount),
		bounds: append([]LineBound(nil), b.boundaries[:rowCount]...),
	}
	for y := range rowCount {
		segment.rows[y] = append([]renderer.Cell(nil), b.frame.Row(y)...)
	}
	segment.bounds[rowCount-1].Soft = false
	return segment
}

func recoveryTranscriptRowUntouched(row []renderer.Cell, bound LineBound) bool {
	if bound.End != 0 || bound.Soft {
		return false
	}
	blank := renderer.BlankCell()
	for _, cell := range row {
		if !cell.Equal(blank) {
			return false
		}
	}
	return true
}

// Marshal encodes the captured rows as canonical terminal history without
// consulting or mutating live Screen history.
func (snapshot RecoveryTranscriptSnapshot) Marshal() ([]byte, error) {
	rows := 0
	cells := 0
	for _, segment := range snapshot.segments {
		rows += len(segment.rows)
		for _, row := range segment.rows {
			cells += len(row)
		}
	}

	view := HistoryView{rows: rows, cells: cells}
	for _, segment := range snapshot.segments {
		for start := 0; start < len(segment.rows); {
			space := maxHistoryChunkRows
			if len(view.chunks) > 0 {
				last := view.chunks[len(view.chunks)-1]
				if len(last.rows) < maxHistoryChunkRows {
					space = maxHistoryChunkRows - len(last.rows)
					end := min(start+space, len(segment.rows))
					last.rows = append(last.rows, segment.rows[start:end]...)
					last.bounds = append(last.bounds, segment.bounds[start:end]...)
					start = end
					continue
				}
			}

			end := min(start+space, len(segment.rows))
			view.chunks = append(view.chunks, &HistoryChunk{
				rows:   append([][]renderer.Cell(nil), segment.rows[start:end]...),
				bounds: append([]LineBound(nil), segment.bounds[start:end]...),
			})
			start = end
		}
	}
	return MarshalHistory(view)
}
