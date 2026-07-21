package snapshot

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
)

const (
	manifestMagic                = "VEVM"
	objectMagic                  = "VEVO"
	ManifestVersion              = uint16(1)
	manifestHeaderSize           = 16
	objectEnvelopeBodyPrefixSize = 1 + 4 // object kind and payload length
	minObjectEnvelopeSize        = manifestHeaderSize + objectEnvelopeBodyPrefixSize + 1
	// maxDecodedBodySize includes the object kind and payload-length prefix.
	maxObjectEnvelopeSize = manifestHeaderSize + maxDecodedBodySize
	maxObjectPayloadSize  = maxDecodedBodySize - objectEnvelopeBodyPrefixSize
)

// SnapshotDigest is the shared content-address type used by the repository.
type SnapshotDigest = ports.SnapshotDigest

func ManifestDigest(encoded []byte) SnapshotDigest { return sha256.Sum256(encoded) }

type ObjectKind uint8

const (
	HistoryChunk ObjectKind = iota + 1
	HistoryTail
	Visible
)

type ObjectRef struct {
	Kind   ObjectKind
	Digest SnapshotDigest
	Size   uint32
}

// Head is the repository-owned generation pointer. It is deliberately not a
// VEVM payload; adapters produce and atomically manage HEAD later.
type Head struct {
	Generation     uint64
	ManifestDigest SnapshotDigest
}

// Manifest is the complete VEVM payload carried directly in a publication.
type Manifest struct {
	Generation uint64
	Name       string
	CreatedAt  uint64
	Active     uint16
	Tabs       []ManifestTab
}

type ManifestTab struct {
	StableID   string
	Cols       uint16
	Rows       uint16
	NextPaneID uint64
	Focus      layout.PaneID
	Tree       *layout.Tree
	Panes      []ManifestPane
}

type ManifestPane struct {
	ID       layout.PaneID
	StableID string
	Cwd      string
	Sealed   []ObjectRef
	Tail     ObjectRef
	Visible  ObjectRef
	Process  *Process
}

func MarshalManifest(m Manifest) ([]byte, error) {
	if err := validateManifest(m); err != nil {
		return nil, err
	}
	var w payloadWriter
	w.putUint64(m.Generation)
	if err := w.putString(m.Name); err != nil {
		return nil, err
	}
	w.putUint64(m.CreatedAt)
	w.putUint16(m.Active)
	if len(m.Tabs) > math.MaxUint16 {
		return nil, fmt.Errorf("%w: too many tabs", ErrInvalidData)
	}
	w.putUint16(uint16(len(m.Tabs)))
	for _, tab := range m.Tabs {
		if err := writeManifestTab(&w, tab); err != nil {
			return nil, err
		}
	}
	return marshalManifestEnvelope(manifestMagic, w.b)
}

func UnmarshalManifest(encoded []byte) (Manifest, error) {
	body, err := unmarshalManifestEnvelope(encoded, manifestMagic)
	if err != nil {
		return Manifest{}, err
	}
	if err := preflightManifest(body); err != nil {
		return Manifest{}, err
	}
	r := payloadReader{b: body}
	generation, err := r.getUint64()
	if err != nil {
		return Manifest{}, err
	}
	name, err := r.getString()
	if err != nil {
		return Manifest{}, err
	}
	createdAt, err := r.getUint64()
	if err != nil {
		return Manifest{}, err
	}
	active, err := r.getUint16()
	if err != nil {
		return Manifest{}, err
	}
	n, err := r.getUint16()
	if err != nil {
		return Manifest{}, err
	}
	m := Manifest{Generation: generation, Name: name, CreatedAt: createdAt, Active: active, Tabs: make([]ManifestTab, n)}
	for i := range m.Tabs {
		if m.Tabs[i], err = readManifestTab(&r); err != nil {
			return Manifest{}, err
		}
	}
	if err := r.done(); err != nil {
		return Manifest{}, err
	}
	if err := validateManifest(m); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// MarshalObject frames payload as VEVO and returns the bytes together with the
// SHA-256 address of those complete bytes.
func MarshalObject(kind ObjectKind, payload []byte) (ports.SnapshotObject, error) {
	if !validObjectKind(kind) || len(payload) == 0 || len(payload) > maxObjectPayloadSize {
		return ports.SnapshotObject{}, fmt.Errorf("%w: object", ErrInvalidData)
	}
	var w payloadWriter
	w.putUint8(uint8(kind))
	w.putUint32(uint32(len(payload)))
	w.b = append(w.b, payload...)
	data, err := marshalManifestEnvelope(objectMagic, w.b)
	if err != nil {
		return ports.SnapshotObject{}, err
	}
	return ports.SnapshotObject{Digest: sha256.Sum256(data), Data: data}, nil
}

func UnmarshalObject(encoded []byte) (ObjectKind, []byte, error) {
	kind, payload, err := PreflightObject(encoded)
	if err != nil {
		return 0, nil, err
	}
	return kind, append([]byte(nil), payload...), nil
}

// PreflightObject verifies VEVO framing and declared payload size without
// copying; the returned payload aliases encoded and is only valid while it is.
func PreflightObject(encoded []byte) (ObjectKind, []byte, error) {
	body, err := unmarshalManifestEnvelope(encoded, objectMagic)
	if err != nil {
		return 0, nil, err
	}
	if len(body) < 5 {
		return 0, nil, ErrShortPayload
	}
	kind := ObjectKind(body[0])
	if !validObjectKind(kind) {
		return 0, nil, fmt.Errorf("%w: object kind", ErrInvalidData)
	}
	n := binary.BigEndian.Uint32(body[1:5])
	if n == 0 || n > maxObjectPayloadSize {
		return 0, nil, fmt.Errorf("%w: object length", ErrInvalidData)
	}
	if uint64(n) > uint64(len(body)-5) {
		return 0, nil, ErrShortPayload
	}
	if uint64(n) != uint64(len(body)-5) {
		return 0, nil, ErrTrailingBytes
	}
	return kind, body[5:], nil
}

func marshalManifestEnvelope(magic string, body []byte) ([]byte, error) {
	if len(body) > maxDecodedBodySize || len(body) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: body too large", ErrInvalidData)
	}
	out := make([]byte, manifestHeaderSize+len(body))
	copy(out[:4], magic)
	binary.BigEndian.PutUint16(out[4:6], ManifestVersion)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(body)))
	binary.BigEndian.PutUint32(out[12:16], crc32.ChecksumIEEE(body))
	copy(out[16:], body)
	return out, nil
}

func unmarshalManifestEnvelope(encoded []byte, expectedMagic string) ([]byte, error) {
	if len(encoded) < manifestHeaderSize {
		return nil, ErrShortPayload
	}
	if string(encoded[:4]) != expectedMagic {
		return nil, ErrBadMagic
	}
	if binary.BigEndian.Uint16(encoded[4:6]) != ManifestVersion {
		return nil, ErrBadVersion
	}
	if binary.BigEndian.Uint16(encoded[6:8]) != 0 {
		return nil, fmt.Errorf("%w: flags", ErrInvalidData)
	}
	n := binary.BigEndian.Uint32(encoded[8:12])
	if n > maxDecodedBodySize {
		return nil, fmt.Errorf("%w: body too large", ErrInvalidData)
	}
	if uint64(len(encoded)) < uint64(manifestHeaderSize)+uint64(n) {
		return nil, ErrShortPayload
	}
	if uint64(len(encoded)) != uint64(manifestHeaderSize)+uint64(n) {
		return nil, ErrTrailingBytes
	}
	body := encoded[manifestHeaderSize:]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(encoded[12:16]) {
		return nil, ErrBadCRC
	}
	return body, nil
}

func writeManifestTab(w *payloadWriter, tab ManifestTab) error {
	if err := w.putString(tab.StableID); err != nil {
		return err
	}
	w.putUint16(tab.Cols)
	w.putUint16(tab.Rows)
	w.putUint64(tab.NextPaneID)
	if err := w.putString(string(tab.Focus)); err != nil {
		return err
	}
	var root *layout.Node
	if tab.Tree != nil {
		root = tab.Tree.Root
	}
	if err := writeNode(w, root); err != nil {
		return err
	}
	if len(tab.Panes) > math.MaxUint16 {
		return fmt.Errorf("%w: too many panes", ErrInvalidData)
	}
	w.putUint16(uint16(len(tab.Panes)))
	for _, pane := range tab.Panes {
		if err := writeManifestPane(w, pane); err != nil {
			return err
		}
	}
	return nil
}
func writeManifestPane(w *payloadWriter, pane ManifestPane) error {
	if err := w.putString(string(pane.ID)); err != nil {
		return err
	}
	if err := w.putString(pane.StableID); err != nil {
		return err
	}
	if err := w.putString(pane.Cwd); err != nil {
		return err
	}
	if len(pane.Sealed) > math.MaxUint16 {
		return fmt.Errorf("%w: too many sealed references", ErrInvalidData)
	}
	w.putUint16(uint16(len(pane.Sealed)))
	for _, ref := range pane.Sealed {
		writeObjectRef(w, ref)
	}
	writeObjectRef(w, pane.Tail)
	writeObjectRef(w, pane.Visible)
	return writeProcess(w, pane.Process)
}
func writeObjectRef(w *payloadWriter, ref ObjectRef) {
	w.putUint8(uint8(ref.Kind))
	w.b = append(w.b, ref.Digest[:]...)
	w.putUint32(ref.Size)
}

func readManifestTab(r *payloadReader) (ManifestTab, error) {
	stableID, err := r.getString()
	if err != nil {
		return ManifestTab{}, err
	}
	cols, err := r.getUint16()
	if err != nil {
		return ManifestTab{}, err
	}
	rows, err := r.getUint16()
	if err != nil {
		return ManifestTab{}, err
	}
	next, err := r.getUint64()
	if err != nil {
		return ManifestTab{}, err
	}
	focus, err := r.getString()
	if err != nil {
		return ManifestTab{}, err
	}
	root, err := readNode(r)
	if err != nil {
		return ManifestTab{}, err
	}
	n, err := r.getUint16()
	if err != nil {
		return ManifestTab{}, err
	}
	tab := ManifestTab{StableID: stableID, Cols: cols, Rows: rows, NextPaneID: next, Focus: layout.PaneID(focus), Tree: &layout.Tree{Root: root, Focus: layout.PaneID(focus)}, Panes: make([]ManifestPane, n)}
	for i := range tab.Panes {
		if tab.Panes[i], err = readManifestPane(r); err != nil {
			return ManifestTab{}, err
		}
	}
	return tab, nil
}
func readManifestPane(r *payloadReader) (ManifestPane, error) {
	id, err := r.getString()
	if err != nil {
		return ManifestPane{}, err
	}
	stableID, err := r.getString()
	if err != nil {
		return ManifestPane{}, err
	}
	cwd, err := r.getString()
	if err != nil {
		return ManifestPane{}, err
	}
	n, err := r.getUint16()
	if err != nil {
		return ManifestPane{}, err
	}
	pane := ManifestPane{ID: layout.PaneID(id), StableID: stableID, Cwd: cwd, Sealed: make([]ObjectRef, n)}
	for i := range pane.Sealed {
		if pane.Sealed[i], err = readObjectRef(r); err != nil {
			return ManifestPane{}, err
		}
	}
	if pane.Tail, err = readObjectRef(r); err != nil {
		return ManifestPane{}, err
	}
	if pane.Visible, err = readObjectRef(r); err != nil {
		return ManifestPane{}, err
	}
	pane.Process, err = readProcess(r)
	return pane, err
}
func readObjectRef(r *payloadReader) (ObjectRef, error) {
	kind, err := r.getUint8()
	if err != nil {
		return ObjectRef{}, err
	}
	if len(r.b) < sha256.Size+4 {
		return ObjectRef{}, ErrShortPayload
	}
	var digest SnapshotDigest
	copy(digest[:], r.b[:sha256.Size])
	r.b = r.b[sha256.Size:]
	size := binary.BigEndian.Uint32(r.b)
	r.b = r.b[4:]
	return ObjectRef{Kind: ObjectKind(kind), Digest: digest, Size: size}, nil
}

func validateManifest(m Manifest) error {
	if m.Name == "" {
		return fmt.Errorf("%w: manifest name", ErrInvalidData)
	}
	if err := validateActive(m.Active, len(m.Tabs)); err != nil {
		return err
	}
	for _, tab := range m.Tabs {
		if !safeDimensions(tab.Cols, tab.Rows) {
			return fmt.Errorf("%w: dimensions", ErrInvalidData)
		}
		ids := make(map[layout.PaneID]struct{}, len(tab.Panes))
		for _, pane := range tab.Panes {
			if pane.ID == "" {
				return fmt.Errorf("%w: empty pane ID", ErrInvalidData)
			}
			if _, exists := ids[pane.ID]; exists {
				return fmt.Errorf("%w: duplicate pane ID", ErrInvalidData)
			}
			if err := validatePaneRefs(pane); err != nil {
				return err
			}
			ids[pane.ID] = struct{}{}
		}
		if _, ok := ids[tab.Focus]; tab.Focus != "" && !ok {
			return ErrUnknownPane
		}
		if err := validateTree(tab.Tree, ids); err != nil {
			return err
		}
	}
	return nil
}
func safeDimensions(cols, rows uint16) bool {
	return cols != 0 && rows != 0 && uint64(cols)*uint64(rows) <= maxSnapshotCells
}
func validatePaneRefs(p ManifestPane) error {
	for _, ref := range p.Sealed {
		if err := validateObjectRef(ref, HistoryChunk); err != nil {
			return err
		}
	}
	if err := validateObjectRef(p.Tail, HistoryTail); err != nil {
		return err
	}
	return validateObjectRef(p.Visible, Visible)
}
func validateObjectRef(ref ObjectRef, want ObjectKind) error {
	if ref.Kind != want || !validObjectEnvelopeSize(ref.Size) || isZeroDigest(ref.Digest) {
		return fmt.Errorf("%w: object reference", ErrInvalidData)
	}
	return nil
}
func validObjectEnvelopeSize(size uint32) bool {
	return size >= minObjectEnvelopeSize && size <= maxObjectEnvelopeSize
}

func validObjectKind(kind ObjectKind) bool {
	return kind == HistoryChunk || kind == HistoryTail || kind == Visible
}
func isZeroDigest(d SnapshotDigest) bool {
	for _, b := range d {
		if b != 0 {
			return false
		}
	}
	return true
}
