package snapshot

import (
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

// preflightManifest rejects hostile declarations before UnmarshalManifest
// allocates tab, pane, string, or reference slices.
func preflightManifest(body []byte) error {
	r := payloadReader{b: body}
	budget := manifestPreflightBudget{}
	generation, err := r.getUint64()
	if err != nil {
		return err
	}
	if generation == 0 {
		return fmt.Errorf("%w: manifest generation", ErrInvalidData)
	}
	if len(r.b) < len(domain.IncarnationID{}) {
		return ErrShortPayload
	}
	var incarnationID domain.IncarnationID
	copy(incarnationID[:], r.b[:len(incarnationID)])
	r.b = r.b[len(incarnationID):]
	if incarnationID == (domain.IncarnationID{}) {
		return fmt.Errorf("%w: manifest incarnation", ErrInvalidData)
	}
	parentPresent, err := r.getUint8()
	if err != nil {
		return err
	}
	switch parentPresent {
	case 0:
	case 1:
		parentGeneration, err := r.getUint64()
		if err != nil {
			return err
		}
		if len(r.b) < 32 {
			return ErrShortPayload
		}
		digest := r.b[:32]
		r.b = r.b[32:]
		if parentGeneration == 0 || parentGeneration >= generation || isZeroDigestBytes(digest) {
			return fmt.Errorf("%w: parent checkpoint", ErrInvalidData)
		}
	default:
		return fmt.Errorf("%w: parent checkpoint presence", ErrInvalidData)
	}
	name, err := preflightManifestStringBytes(&r, &budget)
	if err != nil {
		return err
	}
	if len(name) == 0 {
		return fmt.Errorf("%w: manifest name", ErrInvalidData)
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
	if uint64(tabs) > uint64(len(r.b))/2 || !budget.add(uint64(tabs)*128) {
		return fmt.Errorf("%w: tab allocation", ErrInvalidData)
	}
	for range tabs {
		if err := preflightManifestTab(&r, &budget); err != nil {
			return err
		}
	}
	return preflightManifestWeights(&r, budget.nodes)
}

type manifestPreflightBudget struct {
	allocation uint64
	nodes      uint32
}

func (b *manifestPreflightBudget) add(n uint64) bool {
	if n > maxSnapshotDecodedAllocation-b.allocation {
		return false
	}
	b.allocation += n
	return true
}

func preflightManifestStringBytes(r *payloadReader, budget *manifestPreflightBudget) ([]byte, error) {
	n, err := r.getUint16()
	if err != nil {
		return nil, err
	}
	if uint64(n) > uint64(len(r.b)) {
		return nil, ErrShortPayload
	}
	if !budget.add(uint64(n) + 16) {
		return nil, fmt.Errorf("%w: string allocation", ErrInvalidData)
	}
	b := r.b[:n]
	r.b = r.b[n:]
	return b, nil
}

// skipManifestString accounts for the decoded string without materializing it.
func skipManifestString(r *payloadReader, budget *manifestPreflightBudget) error {
	_, err := preflightManifestStringBytes(r, budget)
	return err
}

func preflightManifestString(r *payloadReader, budget *manifestPreflightBudget) (string, error) {
	b, err := preflightManifestStringBytes(r, budget)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func preflightManifestTab(r *payloadReader, budget *manifestPreflightBudget) error {
	if err := skipManifestString(r, budget); err != nil {
		return err
	}
	cols, err := r.getUint16()
	if err != nil {
		return err
	}
	rows, err := r.getUint16()
	if err != nil {
		return err
	}
	if !safeDimensions(cols, rows) {
		return fmt.Errorf("%w: dimensions", ErrInvalidData)
	}
	if _, err := r.getUint64(); err != nil {
		return err
	}
	focus, err := preflightManifestString(r, budget)
	if err != nil {
		return err
	}
	references := make([]layout.PaneID, 0, 8)
	if err := preflightManifestNode(r, 0, true, budget, &references); err != nil {
		return err
	}
	panes, err := r.getUint16()
	if err != nil {
		return err
	}
	if uint64(panes) > uint64(len(r.b))/2 || !budget.add(uint64(panes)*256) {
		return fmt.Errorf("%w: pane allocation", ErrInvalidData)
	}
	ids := make(map[layout.PaneID]struct{}, panes)
	for range panes {
		id, err := preflightManifestPane(r, budget)
		if err != nil {
			return err
		}
		if id == "" {
			return fmt.Errorf("%w: empty pane ID", ErrInvalidData)
		}
		if _, duplicate := ids[id]; duplicate {
			return fmt.Errorf("%w: duplicate pane ID", ErrInvalidData)
		}
		ids[id] = struct{}{}
	}
	if focus != "" {
		references = append(references, layout.PaneID(focus))
	}
	for _, id := range references {
		if _, ok := ids[id]; !ok {
			return ErrUnknownPane
		}
	}
	return nil
}

func preflightManifestNode(r *payloadReader, depth int, root bool, budget *manifestPreflightBudget, refs *[]layout.PaneID) error {
	if depth > 64 || !budget.add(128) {
		return fmt.Errorf("%w: tree resource declaration", ErrInvalidData)
	}
	kind, err := r.getUint8()
	if err != nil {
		return err
	}
	switch kind {
	case manifestNodeLeaf:
		leaf, err := preflightManifestString(r, budget)
		if err != nil {
			return err
		}
		budget.nodes++
		*refs = append(*refs, layout.PaneID(leaf))
		return nil
	case manifestNodeSplit:
		dir, err := r.getUint8()
		if err != nil {
			return err
		}
		if layout.SplitDir(dir) != layout.Horizontal && layout.SplitDir(dir) != layout.Vertical {
			return fmt.Errorf("%w: split dir", ErrInvalidData)
		}
	case manifestNodeStack:
		expanded, err := preflightManifestString(r, budget)
		if err != nil {
			return err
		}
		if expanded != "" {
			*refs = append(*refs, layout.PaneID(expanded))
		}
	case manifestNodeNil:
		if !root {
			return fmt.Errorf("%w: nil child node", ErrInvalidData)
		}
		return nil
	default:
		return fmt.Errorf("%w: node kind", ErrInvalidData)
	}
	budget.nodes++
	n, err := r.getUint16()
	if err != nil {
		return err
	}
	if uint64(n) > uint64(len(r.b)) {
		return ErrShortPayload
	}
	for range n {
		if err := preflightManifestNode(r, depth+1, false, budget, refs); err != nil {
			return err
		}
	}
	return nil
}

func preflightManifestWeights(r *payloadReader, wantNodes uint32) error {
	return readManifestWeightExtension(r, uint64(wantNodes), nil)
}

func preflightManifestPane(r *payloadReader, budget *manifestPreflightBudget) (layout.PaneID, error) {
	id, err := preflightManifestString(r, budget)
	if err != nil {
		return "", err
	}
	if err := skipManifestString(r, budget); err != nil {
		return "", err
	}
	if err := skipManifestString(r, budget); err != nil {
		return "", err
	}
	sealed, err := r.getUint16()
	if err != nil {
		return "", err
	}
	if uint64(sealed) > uint64(len(r.b))/(objectRefDigestSize+objectRefSizeFieldSize+1) || !budget.add(uint64(sealed)*48) {
		return "", fmt.Errorf("%w: sealed allocation", ErrInvalidData)
	}
	for range sealed {
		if err := preflightObjectRef(r, HistoryChunk); err != nil {
			return "", err
		}
	}
	if err := preflightObjectRef(r, HistoryTail); err != nil {
		return "", err
	}
	if err := preflightObjectRef(r, Visible); err != nil {
		return "", err
	}
	if err := preflightManifestProcess(r, budget); err != nil {
		return "", err
	}
	return layout.PaneID(id), nil
}

func preflightObjectRef(r *payloadReader, want ObjectKind) error {
	kind, digest, size, err := parseObjectRef(r)
	if err != nil {
		return err
	}
	return validateObjectRefFields(kind, digest, size, want)
}
func preflightManifestProcess(r *payloadReader, budget *manifestPreflightBudget) error {
	present, err := r.getUint8()
	if err != nil {
		return err
	}
	if present == processAbsent {
		return nil
	}
	if present != processPresent {
		return fmt.Errorf("%w: process presence", ErrInvalidData)
	}
	n, err := r.getUint16()
	if err != nil {
		return err
	}
	if n == 0 || !budget.add(uint64(n)*16) {
		return fmt.Errorf("%w: process argv", ErrInvalidData)
	}
	for range n {
		if err := skipManifestString(r, budget); err != nil {
			return err
		}
	}
	if err := skipManifestString(r, budget); err != nil {
		return err
	}
	return skipManifestString(r, budget)
}
