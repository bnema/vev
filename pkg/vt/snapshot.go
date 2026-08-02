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
	sealedViews, tailView, err := decodeRestoredHistoryBlobs(sealed, tail)
	if err != nil {
		return nil, err
	}
	return restoreHistoryViews(config, sealedViews, tailView), nil
}

func decodeRestoredHistoryBlobs(sealed [][]byte, tail []byte, extra ...HistoryView) ([]HistoryView, HistoryView, error) {
	seen := make(map[RowID]struct{})
	sealedViews := make([]HistoryView, len(sealed))
	for i, blob := range sealed {
		view, err := UnmarshalHistory(blob)
		if err != nil || len(view.chunks) != 1 || len(view.chunks[0].rows) == 0 || !validateRestoredHistoryView(view, seen) {
			return nil, HistoryView{}, fmt.Errorf("restore sealed history: %w", errInvalidHistory)
		}
		sealedViews[i] = view
	}

	tailView, err := UnmarshalHistory(tail)
	if err != nil || len(tailView.chunks) > 1 || !validateRestoredHistoryView(tailView, seen) {
		return nil, HistoryView{}, fmt.Errorf("restore history tail: %w", errInvalidHistory)
	}
	for _, view := range extra {
		if !validateRestoredHistoryView(view, seen) {
			return nil, HistoryView{}, fmt.Errorf("restore recovery transcript: %w", errInvalidHistory)
		}
	}
	return sealedViews, tailView, nil
}

func restoreHistoryViews(config HistoryConfig, sealed []HistoryView, tail HistoryView) *History {
	h := NewHistory(config)
	for _, view := range sealed {
		h.nextRowID = max(h.nextRowID, view.nextRowID)
		if h.maxRows == 0 {
			continue
		}
		chunk := view.chunks[0]
		h.chunks = append(h.chunks, chunk)
		h.rows += len(chunk.rows)
		h.cells += view.Cells()
		h.evict()
	}
	h.nextRowID = max(h.nextRowID, tail.nextRowID)
	if h.maxRows == 0 || len(tail.chunks) == 0 {
		return h
	}
	chunk := tail.chunks[0]
	h.tail = chunk.rows
	h.tailBounds = append([]LineBound(nil), chunk.bounds...)
	h.tailIDs = append([]RowID(nil), chunk.rowIDs...)
	h.rows += len(h.tail)
	h.cells += tail.Cells()
	h.evict()
	h.normalizeTail()
	return h
}

func validateRestoredHistoryView(view HistoryView, seen map[RowID]struct{}) bool {
	if view.nextRowID == 0 || view.nextRowID == ^RowID(0) {
		return false
	}
	var maxID RowID
	for _, chunk := range view.chunks {
		if chunk == nil || len(chunk.rows) != len(chunk.rowIDs) || len(chunk.rows) != len(chunk.bounds) {
			return false
		}
		for _, id := range chunk.rowIDs {
			if id == 0 || id >= ^RowID(0)-1 {
				return false
			}
			if _, duplicate := seen[id]; duplicate {
				return false
			}
			seen[id] = struct{}{}
			maxID = max(maxID, id)
		}
	}
	return view.nextRowID > maxID
}

// NewScreenWithRecoveryTranscript constructs a fresh blank screen whose
// history contains the restored bounded history followed by the recovery
// transcript. The transcript is decoded in full before history is restored.
func NewScreenWithRecoveryTranscript(width, height int, config HistoryConfig, sealed [][]byte, tail, transcript []byte) (*Screen, error) {
	transcriptView, err := UnmarshalHistory(transcript)
	if err != nil {
		return nil, fmt.Errorf("restore recovery transcript: %w", err)
	}

	sealedViews, tailView, err := decodeRestoredHistoryBlobs(sealed, tail, transcriptView)
	if err != nil {
		return nil, err
	}
	history := restoreHistoryViews(config, sealedViews, tailView)
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
	if counter == 0 || counter >= ^RowID(0) || uint64(height) > uint64(^RowID(0)-1-counter) {
		return nil, fmt.Errorf("restore recovery transcript: %w", errInvalidHistory)
	}
	screen.nextRowID = counter - 1
	for i := range screen.buffer.rowIDs {
		screen.buffer.rowIDs[i] = screen.nextRowIDValue()
	}
	screen.Frame = screen.buffer.frame
	return screen, nil
}
