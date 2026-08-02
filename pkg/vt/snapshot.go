package vt

import (
	"fmt"
	"math"
)

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
	stats, ok := preflightHistory(data)
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

// MarshalHistoryTail encodes the copied mutable tail of a snapshot view as one
// canonical history blob. It does not seal or otherwise mutate live history.
func MarshalHistoryTail(view HistorySnapshotView) ([]byte, error) {
	return MarshalHistory(view.Tail())
}

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
	tail, err := MarshalHistory(HistoryView{nextRowID: view.NextRowID()})
	if err != nil {
		return nil, nil, err
	}
	return sealed, tail, nil
}

// HistoryFromBlobs restores history directly from sealed, oldest-first blobs
// and the mandatory tail blob. It never feeds decoded rows through Append.
func HistoryFromBlobs(config HistoryConfig, sealed [][]byte, tail []byte) (*History, error) {
	h := NewHistory(config)
	seen := make(map[RowID]struct{})
	for _, blob := range sealed {
		view, err := UnmarshalHistory(blob)
		if err != nil || len(view.chunks) != 1 || len(view.chunks[0].rows) == 0 || !validateRestoredHistoryView(view, seen) {
			return nil, fmt.Errorf("restore sealed history: %w", errInvalidHistory)
		}
		chunk := view.chunks[0]
		h.nextRowID = max(h.nextRowID, view.nextRowID)
		h.evictUntil(len(chunk.rows), view.Cells())
		h.chunks = append(h.chunks, chunk)
		h.rows += len(chunk.rows)
		h.cells += view.Cells()
	}
	view, err := UnmarshalHistory(tail)
	if err != nil || len(view.chunks) > 1 || !validateRestoredHistoryView(view, seen) {
		return nil, fmt.Errorf("restore history tail: %w", errInvalidHistory)
	}
	h.nextRowID = max(h.nextRowID, view.nextRowID)
	if len(view.chunks) == 1 {
		chunk := view.chunks[0]
		h.evictUntil(len(chunk.rows), view.Cells())
		h.tail = chunk.rows
		h.tailBounds = append([]LineBound(nil), chunk.bounds...)
		h.tailIDs = append([]RowID(nil), chunk.rowIDs...)
		h.rows += len(h.tail)
		h.cells += view.Cells()
	}
	h.evict()
	h.normalizeTail()
	return h, nil
}

func validateRestoredHistoryView(view HistoryView, seen map[RowID]struct{}) bool {
	for _, chunk := range view.chunks {
		if chunk == nil || len(chunk.rows) != len(chunk.rowIDs) || len(chunk.rows) != len(chunk.bounds) {
			return false
		}
		for _, id := range chunk.rowIDs {
			if id == 0 || id == ^RowID(0) {
				return false
			}
			if _, duplicate := seen[id]; duplicate {
				return false
			}
			seen[id] = struct{}{}
		}
	}
	return true
}

// NewScreenWithRecoveryTranscript constructs a fresh blank screen whose
// history contains the restored bounded history followed by the recovery
// transcript. The transcript is decoded in full before history is restored.
func NewScreenWithRecoveryTranscript(width, height int, config HistoryConfig, sealed [][]byte, tail, transcript []byte) (*Screen, error) {
	transcriptView, err := UnmarshalHistory(transcript)
	if err != nil {
		return nil, fmt.Errorf("restore recovery transcript: %w", err)
	}

	history, err := HistoryFromBlobs(config, sealed, tail)
	if err != nil {
		return nil, err
	}
	remainingRows := transcriptView.rows
	for _, chunk := range transcriptView.chunks {
		for i, row := range chunk.rows {
			remainingRows--
			bound := chunk.bounds[i]
			if remainingRows == 0 {
				bound.Soft = false
			}
			if err := history.appendRestored(row, bound, chunk.rowIDs[i]); err != nil {
				return nil, fmt.Errorf("restore recovery transcript: %w", err)
			}
		}
	}

	screen := NewScreenWithHistory(width, height, config)
	screen.history = history
	counter := max(history.nextRowID, transcriptView.nextRowID, RowID(1))
	if counter == ^RowID(0) {
		return nil, fmt.Errorf("restore recovery transcript: %w", errInvalidHistory)
	}
	screen.nextRowID = counter - 1
	for i := range screen.buffer.rowIDs {
		screen.buffer.rowIDs[i] = screen.nextRowIDValue()
	}
	screen.Frame = screen.buffer.frame
	return screen, nil
}
