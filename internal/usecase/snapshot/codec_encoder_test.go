package snapshot

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/bnema/vev/internal/usecase/layout"
)

// marshalTest is a test-only v3 encoder. Production retains only the decoder
// required by the one-time legacy import.
func marshalTest(s Session) ([]byte, error) {
	var w payloadWriter
	if err := writeSession(&w, s); err != nil {
		return nil, err
	}
	if len(w.b) > maxDecodedBodySize || len(w.b) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: body too large", ErrInvalidData)
	}
	out := make([]byte, 16+len(w.b))
	copy(out[:4], magic)
	binary.BigEndian.PutUint16(out[4:6], version)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(w.b)))
	binary.BigEndian.PutUint32(out[12:16], crc32.ChecksumIEEE(w.b))
	copy(out[16:], w.b)
	return out, nil
}

func writeSession(w *payloadWriter, s Session) error {
	if len(s.Tabs) > math.MaxUint16 {
		return fmt.Errorf("%w: too many tabs", ErrInvalidData)
	}
	if err := validateActive(s.Active, len(s.Tabs)); err != nil {
		return err
	}
	if err := w.putString(s.Name); err != nil {
		return err
	}
	w.putUint64(s.CreatedAt)
	w.putUint16(s.Active)
	w.putUint16(uint16(len(s.Tabs)))
	for _, t := range s.Tabs {
		if err := writeTab(w, t); err != nil {
			return err
		}
	}
	return nil
}

func writeTab(w *payloadWriter, t Tab) error {
	if err := w.putString(t.StableID); err != nil {
		return err
	}
	w.putUint16(t.Cols)
	w.putUint16(t.Rows)
	w.putUint64(t.NextPaneID)
	if err := w.putString(string(t.Focus)); err != nil {
		return err
	}
	var root *layout.Node
	if t.Tree != nil {
		root = t.Tree.Root
	}
	if err := writeNode(w, root); err != nil {
		return err
	}
	if len(t.Panes) > math.MaxUint16 {
		return fmt.Errorf("%w: too many panes", ErrInvalidData)
	}
	w.putUint16(uint16(len(t.Panes)))
	for _, p := range t.Panes {
		if err := writePane(w, p); err != nil {
			return err
		}
	}
	return nil
}

func writePane(w *payloadWriter, p Pane) error {
	if err := w.putString(string(p.ID)); err != nil {
		return err
	}
	if err := w.putString(p.StableID); err != nil {
		return err
	}
	if err := w.putString(p.Cwd); err != nil {
		return err
	}
	if len(p.SealedChunks) > maxSnapshotObjects {
		return fmt.Errorf("%w: too many sealed chunks", ErrInvalidData)
	}
	w.putUint32(uint32(len(p.SealedChunks)))
	for _, blob := range p.SealedChunks {
		if len(blob) == 0 || len(blob) > math.MaxUint32 {
			return fmt.Errorf("%w: sealed chunk length", ErrInvalidData)
		}
		w.putUint32(uint32(len(blob)))
		w.b = append(w.b, blob...)
	}
	if len(p.Tail) == 0 || len(p.Tail) > math.MaxUint32 {
		return fmt.Errorf("%w: tail length", ErrInvalidData)
	}
	w.putUint32(uint32(len(p.Tail)))
	w.b = append(w.b, p.Tail...)
	if len(p.Visible) == 0 || len(p.Visible) > math.MaxUint32 {
		return fmt.Errorf("%w: visible length", ErrInvalidData)
	}
	w.putUint32(uint32(len(p.Visible)))
	w.b = append(w.b, p.Visible...)
	return writeProcess(w, p.Process)
}
