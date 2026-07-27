package snapshot

import (
	"fmt"
	"math"

	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/vt"
)

func preflightSession(body []byte) error {
	if err := preflightSessionStructure(body); err != nil {
		return err
	}
	return preflightSessionReferences(body)
}

func preflightSessionStructure(body []byte) error {
	r := payloadReader{b: body}
	if err := skipString(&r); err != nil {
		return err
	}
	if _, err := r.getUint64(); err != nil {
		return err
	}
	active, err := r.getUint16()
	if err != nil {
		return err
	}
	tabs, err := r.getUint16()
	if err != nil {
		return err
	}
	if err := validateActive(active, int(tabs)); err != nil {
		return err
	}
	if uint64(tabs) > uint64(len(r.b))/2 {
		return ErrShortPayload
	}

	var totals vt.DecodeStats
	blobs := uint64(0)
	budget := preflightBudget{}
	for range tabs {
		if err := preflightTabStructure(&r, &totals, &blobs, &budget); err != nil {
			return fmt.Errorf("preflight tab: %w", err)
		}
	}
	if err := r.done(); err != nil {
		return err
	}
	if totals.Rows > maxSnapshotRows || totals.Cells > maxSnapshotCells || totals.Bytes > maxSnapshotBytes {
		return fmt.Errorf("%w: global VT budget", ErrInvalidData)
	}
	return nil
}

func skipString(r *payloadReader) error {
	n, err := r.getUint16()
	if err != nil {
		return err
	}
	if uint64(n) > uint64(len(r.b)) {
		return ErrShortPayload
	}
	r.b = r.b[n:]
	return nil
}

type preflightBudget struct {
	nodes uint64
	alloc uint64
}

func (b *preflightBudget) addAllocation(amount uint64) bool {
	if amount > maxSnapshotDecodedAllocation-b.alloc {
		return false
	}
	b.alloc += amount
	return true
}

func (b *preflightBudget) addProduct(count, size uint64) bool {
	if count != 0 && size > math.MaxUint64/count {
		return false
	}
	return b.addAllocation(count * size)
}

func (b *preflightBudget) addNode() bool {
	if b.nodes >= maxSnapshotTreeNodes || !b.addAllocation(128) {
		return false
	}
	b.nodes++
	return true
}

func preflightTabStructure(r *payloadReader, totals *vt.DecodeStats, blobs *uint64, budget *preflightBudget) error {
	if !budget.addAllocation(256) {
		return fmt.Errorf("%w: decoded allocation budget", ErrInvalidData)
	}
	if err := skipString(r); err != nil {
		return err
	}
	if _, err := r.getUint16(); err != nil {
		return err
	}
	if _, err := r.getUint16(); err != nil {
		return err
	}
	if _, err := r.getUint64(); err != nil {
		return err
	}
	if err := skipString(r); err != nil {
		return err
	}
	if err := preflightNodeStructure(r, 0, true, budget); err != nil {
		return err
	}
	panes, err := r.getUint16()
	if err != nil {
		return err
	}
	if uint64(panes) > uint64(len(r.b))/2 {
		return ErrShortPayload
	}
	for range panes {
		if err := preflightPaneStructure(r, totals, blobs, budget); err != nil {
			return err
		}
	}
	return nil
}

func preflightNodeStructure(r *payloadReader, depth int, root bool, budget *preflightBudget) error {
	if depth > 64 {
		return fmt.Errorf("%w: tree depth", ErrInvalidData)
	}
	if !budget.addNode() {
		return fmt.Errorf("%w: tree node or decoded allocation budget", ErrInvalidData)
	}
	kind, err := r.getUint8()
	if err != nil {
		return err
	}
	switch kind {
	case 0:
		return skipString(r)
	case 1:
		dir, err := r.getUint8()
		if err != nil {
			return err
		}
		if layout.SplitDir(dir) != layout.Horizontal && layout.SplitDir(dir) != layout.Vertical {
			return fmt.Errorf("%w: split dir", ErrInvalidData)
		}
	case 2:
		if err := skipString(r); err != nil {
			return err
		}
	case 3:
		if !root {
			return fmt.Errorf("%w: nil child node", ErrInvalidData)
		}
		return nil
	default:
		return fmt.Errorf("%w: node kind", ErrInvalidData)
	}
	children, err := r.getUint16()
	if err != nil {
		return err
	}
	if uint64(children) > uint64(len(r.b)) {
		return ErrShortPayload
	}
	for range children {
		if err := preflightNodeStructure(r, depth+1, false, budget); err != nil {
			return err
		}
	}
	return nil
}

func preflightPaneStructure(r *payloadReader, totals *vt.DecodeStats, blobs *uint64, budget *preflightBudget) error {
	if !budget.addAllocation(256) {
		return fmt.Errorf("%w: decoded allocation budget", ErrInvalidData)
	}
	if err := skipString(r); err != nil {
		return err
	}
	if err := skipString(r); err != nil {
		return err
	}
	if err := skipString(r); err != nil {
		return err
	}
	n, err := r.getUint32()
	if err != nil {
		return err
	}
	if n > maxSnapshotObjects || uint64(n) > uint64(len(r.b))/4 {
		return fmt.Errorf("%w: sealed chunks", ErrInvalidData)
	}
	for range n {
		stats, err := preflightBlob(r, totals, blobs, budget)
		if err != nil {
			return err
		}
		if stats.Chunks != 1 || stats.Rows == 0 {
			return fmt.Errorf("%w: sealed blob role", ErrInvalidData)
		}
	}
	tail, err := preflightBlob(r, totals, blobs, budget)
	if err != nil {
		return err
	}
	// A capture keeps the live mutable tail separate from immutable sealed
	// chunks. It may therefore contain one partial chunk (or the canonical
	// empty tail), but never a sequence of sealed chunks.
	if tail.Chunks > 1 {
		return fmt.Errorf("%w: tail blob role", ErrInvalidData)
	}
	transcript, err := preflightBlob(r, totals, blobs, budget)
	if err != nil {
		return err
	}
	// Transcripts are canonical history; they may contain multiple chunks or be
	// empty, so no blob-role assertion applies.
	_ = transcript
	return preflightProcess(r)
}

func preflightBlob(r *payloadReader, totals *vt.DecodeStats, blobs *uint64, budget *preflightBudget) (vt.DecodeStats, error) {
	n, err := r.getUint32()
	if err != nil {
		return vt.DecodeStats{}, err
	}
	if n == 0 || uint64(n) > uint64(len(r.b)) || uint64(n) > maxSnapshotBytes {
		return vt.DecodeStats{}, fmt.Errorf("%w: VT blob length", ErrInvalidData)
	}
	if *blobs >= maxSnapshotObjects {
		return vt.DecodeStats{}, fmt.Errorf("%w: too many VT blobs", ErrInvalidData)
	}
	blob := r.b[:n]
	r.b = r.b[n:]
	stats, err := vt.PreflightHistoryBlob(blob)
	if err != nil {
		return vt.DecodeStats{}, fmt.Errorf("%w: VT blob", ErrInvalidData)
	}
	if !totals.Add(stats) {
		return vt.DecodeStats{}, fmt.Errorf("%w: VT budget overflow", ErrInvalidData)
	}
	if !budget.addAllocation(uint64(n)) || !budget.addProduct(stats.Chunks, 64) || !budget.addProduct(stats.Rows, 32) || !budget.addProduct(stats.Cells, 64) {
		return vt.DecodeStats{}, fmt.Errorf("%w: decoded allocation budget", ErrInvalidData)
	}
	*blobs++
	return stats, nil
}

func preflightProcess(r *payloadReader) error {
	present, err := r.getUint8()
	if err != nil {
		return err
	}
	if present == 0 {
		return nil
	}
	if present != 1 {
		return fmt.Errorf("%w: process presence", ErrInvalidData)
	}
	n, err := r.getUint16()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: process argv empty", ErrInvalidData)
	}
	for range n {
		if err := skipString(r); err != nil {
			return err
		}
	}
	if err := skipString(r); err != nil {
		return err
	}
	return skipString(r)
}

// preflightSessionReferences validates duplicate, empty, and dangling pane
// references only after the structural pass has admitted the complete payload.
func preflightSessionReferences(body []byte) error {
	r := payloadReader{b: body}
	if err := skipString(&r); err != nil {
		return err
	}
	if _, err := r.getUint64(); err != nil {
		return err
	}
	if _, err := r.getUint16(); err != nil {
		return err
	}
	tabs, err := r.getUint16()
	if err != nil {
		return err
	}
	for range tabs {
		if err := preflightTabReferences(&r); err != nil {
			return fmt.Errorf("preflight tab references: %w", err)
		}
	}
	return r.done()
}

func preflightTabReferences(r *payloadReader) error {
	if err := skipString(r); err != nil {
		return err
	}
	if _, err := r.getUint16(); err != nil {
		return err
	}
	if _, err := r.getUint16(); err != nil {
		return err
	}
	if _, err := r.getUint64(); err != nil {
		return err
	}
	focus, err := r.getString()
	if err != nil {
		return err
	}
	references := preflightTreeReferences{}
	if err := preflightNodeReferences(r, &references); err != nil {
		return err
	}
	panes, err := r.getUint16()
	if err != nil {
		return err
	}
	ids := make(map[layout.PaneID]struct{}, panes)
	for range panes {
		id, err := r.getString()
		if err != nil {
			return err
		}
		if id == "" {
			return fmt.Errorf("%w: empty pane ID", ErrInvalidData)
		}
		paneID := layout.PaneID(id)
		if _, exists := ids[paneID]; exists {
			return fmt.Errorf("%w: duplicate pane ID", ErrInvalidData)
		}
		ids[paneID] = struct{}{}
		if err := skipString(r); err != nil {
			return err
		}
		if err := skipString(r); err != nil {
			return err
		}
		n, err := r.getUint32()
		if err != nil {
			return err
		}
		for range n {
			if err := skipBlob(r); err != nil {
				return err
			}
		}
		if err := skipBlob(r); err != nil {
			return err
		}
		if err := skipBlob(r); err != nil {
			return err
		}
		if err := preflightProcess(r); err != nil {
			return err
		}
	}
	if focus != "" {
		references.ids = append(references.ids, layout.PaneID(focus))
	}
	for _, id := range references.ids {
		if _, ok := ids[id]; !ok {
			return ErrUnknownPane
		}
	}
	return nil
}

func skipBlob(r *payloadReader) error {
	n, err := r.getUint32()
	if err != nil {
		return err
	}
	if uint64(n) > uint64(len(r.b)) {
		return ErrShortPayload
	}
	r.b = r.b[n:]
	return nil
}

type preflightTreeReferences struct{ ids []layout.PaneID }

func (r *preflightTreeReferences) add(id layout.PaneID) {
	r.ids = append(r.ids, id)
}

func preflightNodeReferences(r *payloadReader, references *preflightTreeReferences) error {
	kind, err := r.getUint8()
	if err != nil {
		return err
	}
	switch kind {
	case 0:
		leaf, err := r.getString()
		if err != nil {
			return err
		}
		references.add(layout.PaneID(leaf))
		return nil
	case 1:
		if _, err := r.getUint8(); err != nil {
			return err
		}
	case 2:
		expanded, err := r.getString()
		if err != nil {
			return err
		}
		if expanded != "" {
			references.add(layout.PaneID(expanded))
		}
	case 3:
		return nil
	default:
		return fmt.Errorf("%w: node kind", ErrInvalidData)
	}
	children, err := r.getUint16()
	if err != nil {
		return err
	}
	for range children {
		if err := preflightNodeReferences(r, references); err != nil {
			return err
		}
	}
	return nil
}
