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
	manifestMagic = "VEVM"
	headMagic     = "VEVH"
	objectMagic   = "VEVO"
	codecVersion  = uint16(1)
)

// SnapshotDigest is the shared content-address type used by the repository.
type SnapshotDigest = ports.SnapshotDigest

// ManifestDigest returns the content address of a complete VEVM manifest.
func ManifestDigest(encoded []byte) SnapshotDigest { return sha256.Sum256(encoded) }

// ObjectRole binds a referenced object to its position in a pane snapshot.
type ObjectRole uint8

const (
	ObjectRoleHistory ObjectRole = iota + 1
	ObjectRoleTail
	ObjectRoleVisible
)

// ObjectRef is a content-addressed object used by a reference-only manifest.
type ObjectRef struct {
	Role   ObjectRole
	Digest SnapshotDigest
}

// Manifest is the incremental durable representation of a named session. Its
// panes contain references only; terminal bytes belong exclusively in VEVO
// object envelopes.
type Manifest struct {
	Name      string
	CreatedAt uint64
	Active    uint16
	Tabs      []Tab
}

// Head is the atomically published name-to-manifest pointer.
type Head struct {
	Name       string
	Generation uint64
	Manifest   SnapshotDigest
}

// Object is a complete typed object payload before VEVO framing.
type Object struct {
	Role ObjectRole
	Data []byte
}

// MarshalManifest encodes a deterministic, strict VEVM v1 envelope.
func MarshalManifest(m Manifest) ([]byte, error) {
	if err := validateManifest(m); err != nil {
		return nil, err
	}
	var w payloadWriter
	if err := writeManifest(&w, m); err != nil {
		return nil, err
	}
	return marshalEnvelope(manifestMagic, w.b)
}

// UnmarshalManifest validates a whole VEVM envelope before allocating its
// decoded graph. It rejects non-reference pane data, malformed layouts and
// noncanonical object roles.
func UnmarshalManifest(encoded []byte) (Manifest, error) {
	body, err := unmarshalEnvelope(encoded, manifestMagic)
	if err != nil {
		return Manifest{}, err
	}
	if err := preflightManifest(body); err != nil {
		return Manifest{}, err
	}
	r := payloadReader{b: body}
	m, err := readManifest(&r)
	if err != nil {
		return Manifest{}, err
	}
	if err := r.done(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// MarshalHead encodes a deterministic VEVH v1 publication pointer.
func MarshalHead(h Head) ([]byte, error) {
	if h.Name == "" || isZeroDigest(h.Manifest) {
		return nil, fmt.Errorf("%w: head name or manifest digest", ErrInvalidData)
	}
	var w payloadWriter
	if err := w.putString(h.Name); err != nil {
		return nil, err
	}
	w.putUint64(h.Generation)
	w.b = append(w.b, h.Manifest[:]...)
	return marshalEnvelope(headMagic, w.b)
}

// UnmarshalHead decodes a complete VEVH envelope.
func UnmarshalHead(encoded []byte) (Head, error) {
	body, err := unmarshalEnvelope(encoded, headMagic)
	if err != nil {
		return Head{}, err
	}
	r := payloadReader{b: body}
	name, err := r.getString()
	if err != nil {
		return Head{}, err
	}
	generation, err := r.getUint64()
	if err != nil {
		return Head{}, err
	}
	if len(r.b) < sha256.Size {
		return Head{}, ErrShortPayload
	}
	var digest SnapshotDigest
	copy(digest[:], r.b[:sha256.Size])
	r.b = r.b[sha256.Size:]
	if err := r.done(); err != nil {
		return Head{}, err
	}
	if name == "" || isZeroDigest(digest) {
		return Head{}, fmt.Errorf("%w: head name or manifest digest", ErrInvalidData)
	}
	return Head{Name: name, Generation: generation, Manifest: digest}, nil
}

// MarshalObject is the sole API that creates VEVO object envelopes.
func MarshalObject(object Object) ([]byte, error) {
	if !validObjectRole(object.Role) || len(object.Data) == 0 || len(object.Data) > maxSnapshotBytes {
		return nil, fmt.Errorf("%w: object", ErrInvalidData)
	}
	var w payloadWriter
	w.putUint8(uint8(object.Role))
	w.putUint32(uint32(len(object.Data)))
	w.b = append(w.b, object.Data...)
	return marshalEnvelope(objectMagic, w.b)
}

// PreflightObject validates VEVO framing and all sizes without copying object
// bytes. Callers loading untrusted storage can use it before retaining data.
func PreflightObject(encoded []byte) error {
	body, err := unmarshalEnvelope(encoded, objectMagic)
	if err != nil {
		return err
	}
	if len(body) < 5 {
		return ErrShortPayload
	}
	if !validObjectRole(ObjectRole(body[0])) {
		return fmt.Errorf("%w: object role", ErrInvalidData)
	}
	n := binary.BigEndian.Uint32(body[1:5])
	if n == 0 || n > maxSnapshotBytes {
		return fmt.Errorf("%w: object length", ErrInvalidData)
	}
	if uint64(n) > uint64(len(body)-5) {
		return ErrShortPayload
	}
	if uint64(n) != uint64(len(body)-5) {
		return ErrTrailingBytes
	}
	return nil
}

// UnmarshalObject validates and returns a caller-owned object payload.
func UnmarshalObject(encoded []byte) (Object, error) {
	if err := PreflightObject(encoded); err != nil {
		return Object{}, err
	}
	body := encoded[16:]
	return Object{Role: ObjectRole(body[0]), Data: append([]byte(nil), body[5:]...)}, nil
}

func marshalEnvelope(magic string, body []byte) ([]byte, error) {
	if len(body) > maxDecodedBodySize || len(body) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: body too large", ErrInvalidData)
	}
	out := make([]byte, 16+len(body))
	copy(out[:4], magic)
	binary.BigEndian.PutUint16(out[4:6], codecVersion)
	// Flags [6:8] remain zero.
	binary.BigEndian.PutUint32(out[8:12], uint32(len(body)))
	binary.BigEndian.PutUint32(out[12:16], crc32.ChecksumIEEE(body))
	copy(out[16:], body)
	return out, nil
}

func unmarshalEnvelope(encoded []byte, expectedMagic string) ([]byte, error) {
	if len(encoded) < 16 {
		return nil, ErrShortPayload
	}
	if string(encoded[:4]) != expectedMagic {
		return nil, ErrBadMagic
	}
	if binary.BigEndian.Uint16(encoded[4:6]) != codecVersion {
		return nil, ErrBadVersion
	}
	if binary.BigEndian.Uint16(encoded[6:8]) != 0 {
		return nil, fmt.Errorf("%w: flags", ErrInvalidData)
	}
	bodyLen := binary.BigEndian.Uint32(encoded[8:12])
	if bodyLen > maxDecodedBodySize {
		return nil, fmt.Errorf("%w: body too large", ErrInvalidData)
	}
	if uint64(len(encoded)) < 16+uint64(bodyLen) {
		return nil, ErrShortPayload
	}
	if uint64(len(encoded)) != 16+uint64(bodyLen) {
		return nil, ErrTrailingBytes
	}
	body := encoded[16:]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(encoded[12:16]) {
		return nil, ErrBadCRC
	}
	return body, nil
}

func validateManifest(m Manifest) error {
	if m.Name == "" {
		return fmt.Errorf("%w: manifest name", ErrInvalidData)
	}
	if err := validateActive(m.Active, len(m.Tabs)); err != nil {
		return err
	}
	for _, tab := range m.Tabs {
		if tab.Cols == 0 || tab.Rows == 0 {
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
			if len(pane.SealedChunks) != 0 || len(pane.Tail) != 0 || len(pane.Visible) != 0 {
				return fmt.Errorf("%w: manifest pane contains inline object data", ErrInvalidData)
			}
			if err := validateObjectRefs(pane.Objects); err != nil {
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

func validateObjectRefs(refs []ObjectRef) error {
	if len(refs) < 2 {
		return fmt.Errorf("%w: missing tail or visible object", ErrInvalidData)
	}
	seenTail := false
	for i, ref := range refs {
		if isZeroDigest(ref.Digest) {
			return fmt.Errorf("%w: object digest", ErrInvalidData)
		}
		switch ref.Role {
		case ObjectRoleHistory:
			if seenTail {
				return fmt.Errorf("%w: object role order", ErrInvalidData)
			}
		case ObjectRoleTail:
			if seenTail || i+1 == len(refs) {
				return fmt.Errorf("%w: object role order", ErrInvalidData)
			}
			seenTail = true
		case ObjectRoleVisible:
			if !seenTail || i+1 != len(refs) {
				return fmt.Errorf("%w: object role order", ErrInvalidData)
			}
			return nil
		default:
			return fmt.Errorf("%w: object role", ErrInvalidData)
		}
	}
	return fmt.Errorf("%w: missing visible object", ErrInvalidData)
}

func writeManifest(w *payloadWriter, m Manifest) error {
	if len(m.Tabs) > math.MaxUint16 {
		return fmt.Errorf("%w: too many tabs", ErrInvalidData)
	}
	if err := validateActive(m.Active, len(m.Tabs)); err != nil {
		return err
	}
	if err := w.putString(m.Name); err != nil {
		return err
	}
	w.putUint64(m.CreatedAt)
	w.putUint16(m.Active)
	w.putUint16(uint16(len(m.Tabs)))
	for _, tab := range m.Tabs {
		if err := writeManifestTab(w, tab); err != nil {
			return err
		}
	}
	return nil
}

func writeManifestTab(w *payloadWriter, tab Tab) error {
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

func writeManifestPane(w *payloadWriter, pane Pane) error {
	if err := w.putString(string(pane.ID)); err != nil {
		return err
	}
	if err := w.putString(pane.StableID); err != nil {
		return err
	}
	if err := w.putString(pane.Cwd); err != nil {
		return err
	}
	if len(pane.Objects) > math.MaxUint16 {
		return fmt.Errorf("%w: too many object references", ErrInvalidData)
	}
	w.putUint16(uint16(len(pane.Objects)))
	for _, ref := range pane.Objects {
		w.putUint8(uint8(ref.Role))
		w.b = append(w.b, ref.Digest[:]...)
	}
	return writeProcess(w, pane.Process)
}

func readManifest(r *payloadReader) (Manifest, error) {
	name, err := r.getString()
	if err != nil {
		return Manifest{}, err
	}
	created, err := r.getUint64()
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
	m := Manifest{Name: name, CreatedAt: created, Active: active, Tabs: make([]Tab, n)}
	for i := range m.Tabs {
		if m.Tabs[i], err = readManifestTab(r); err != nil {
			return Manifest{}, err
		}
	}
	return m, nil
}

func readManifestTab(r *payloadReader) (Tab, error) {
	stableID, err := r.getString()
	if err != nil {
		return Tab{}, err
	}
	cols, err := r.getUint16()
	if err != nil {
		return Tab{}, err
	}
	rows, err := r.getUint16()
	if err != nil {
		return Tab{}, err
	}
	next, err := r.getUint64()
	if err != nil {
		return Tab{}, err
	}
	focus, err := r.getString()
	if err != nil {
		return Tab{}, err
	}
	root, err := readNode(r)
	if err != nil {
		return Tab{}, err
	}
	n, err := r.getUint16()
	if err != nil {
		return Tab{}, err
	}
	tab := Tab{StableID: stableID, Cols: cols, Rows: rows, NextPaneID: next, Focus: layout.PaneID(focus), Tree: &layout.Tree{Root: root, Focus: layout.PaneID(focus)}, Panes: make([]Pane, n)}
	for i := range tab.Panes {
		if tab.Panes[i], err = readManifestPane(r); err != nil {
			return Tab{}, err
		}
	}
	return tab, nil
}

func readManifestPane(r *payloadReader) (Pane, error) {
	id, err := r.getString()
	if err != nil {
		return Pane{}, err
	}
	stableID, err := r.getString()
	if err != nil {
		return Pane{}, err
	}
	cwd, err := r.getString()
	if err != nil {
		return Pane{}, err
	}
	n, err := r.getUint16()
	if err != nil {
		return Pane{}, err
	}
	objects := make([]ObjectRef, n)
	for i := range objects {
		role, err := r.getUint8()
		if err != nil {
			return Pane{}, err
		}
		if len(r.b) < sha256.Size {
			return Pane{}, ErrShortPayload
		}
		objects[i].Role = ObjectRole(role)
		copy(objects[i].Digest[:], r.b[:sha256.Size])
		r.b = r.b[sha256.Size:]
	}
	process, err := readProcess(r)
	if err != nil {
		return Pane{}, err
	}
	return Pane{ID: layout.PaneID(id), StableID: stableID, Cwd: cwd, Objects: objects, Process: process}, nil
}

func preflightManifest(body []byte) error {
	if len(body) < 2 {
		return ErrShortPayload
	}
	if binary.BigEndian.Uint16(body[:2]) == 0 {
		return fmt.Errorf("%w: manifest name", ErrInvalidData)
	}
	r := payloadReader{b: body}
	budget := preflightBudget{}
	if err := skipManifestString(&r, &budget); err != nil {
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
	for range tabs {
		if err := preflightManifestTab(&r, &budget); err != nil {
			return err
		}
	}
	if err := r.done(); err != nil {
		return err
	}
	return preflightManifestReferences(body)
}

func preflightManifestTab(r *payloadReader, budget *preflightBudget) error {
	if !budget.addAllocation(256) {
		return fmt.Errorf("%w: decoded allocation budget", ErrInvalidData)
	}
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
	if cols == 0 || rows == 0 {
		return fmt.Errorf("%w: dimensions", ErrInvalidData)
	}
	if _, err := r.getUint64(); err != nil {
		return err
	}
	if err := skipManifestString(r, budget); err != nil {
		return err
	}
	if err := preflightManifestNode(r, 0, true, budget); err != nil {
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
		if err := preflightManifestPane(r, budget); err != nil {
			return err
		}
	}
	return nil
}

func preflightManifestPane(r *payloadReader, budget *preflightBudget) error {
	if !budget.addAllocation(256) {
		return fmt.Errorf("%w: decoded allocation budget", ErrInvalidData)
	}
	for range 3 {
		if err := skipManifestString(r, budget); err != nil {
			return err
		}
	}
	n, err := r.getUint16()
	if err != nil {
		return err
	}
	if uint64(n) > uint64(len(r.b))/33 || !budget.addProduct(uint64(n), 48) {
		return fmt.Errorf("%w: object references or decoded allocation budget", ErrInvalidData)
	}
	for range n {
		role, err := r.getUint8()
		if err != nil {
			return err
		}
		if !validObjectRole(ObjectRole(role)) {
			return fmt.Errorf("%w: object role", ErrInvalidData)
		}
		if len(r.b) < sha256.Size {
			return ErrShortPayload
		}
		if isZeroDigestBytes(r.b[:sha256.Size]) {
			return fmt.Errorf("%w: object digest", ErrInvalidData)
		}
		r.b = r.b[sha256.Size:]
	}
	return preflightManifestProcess(r, budget)
}

func skipManifestString(r *payloadReader, budget *preflightBudget) error {
	n, err := r.getUint16()
	if err != nil {
		return err
	}
	if uint64(n) > uint64(len(r.b)) {
		return ErrShortPayload
	}
	if !budget.addAllocation(uint64(n) + 16) {
		return fmt.Errorf("%w: decoded allocation budget", ErrInvalidData)
	}
	r.b = r.b[n:]
	return nil
}

func preflightManifestNode(r *payloadReader, depth int, root bool, budget *preflightBudget) error {
	if depth > 64 || !budget.addNode() {
		return fmt.Errorf("%w: tree depth, node, or decoded allocation budget", ErrInvalidData)
	}
	kind, err := r.getUint8()
	if err != nil {
		return err
	}
	switch kind {
	case 0:
		return skipManifestString(r, budget)
	case 1:
		dir, err := r.getUint8()
		if err != nil {
			return err
		}
		if layout.SplitDir(dir) != layout.Horizontal && layout.SplitDir(dir) != layout.Vertical {
			return fmt.Errorf("%w: split dir", ErrInvalidData)
		}
	case 2:
		if err := skipManifestString(r, budget); err != nil {
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
		if err := preflightManifestNode(r, depth+1, false, budget); err != nil {
			return err
		}
	}
	return nil
}

func preflightManifestProcess(r *payloadReader, budget *preflightBudget) error {
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
	if n == 0 || !budget.addProduct(uint64(n), 16) {
		return fmt.Errorf("%w: process argv or decoded allocation budget", ErrInvalidData)
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

func preflightManifestReferences(body []byte) error {
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
		if err := preflightManifestTabReferences(&r); err != nil {
			return err
		}
	}
	return r.done()
}

func preflightManifestTabReferences(r *payloadReader) error {
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
	n, err := r.getUint16()
	if err != nil {
		return err
	}
	ids := make(map[layout.PaneID]struct{}, n)
	for range n {
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
		objects, err := r.getUint16()
		if err != nil {
			return err
		}
		if err := preflightObjectOrder(r, objects); err != nil {
			return err
		}
		if err := preflightProcess(r); err != nil {
			return err
		}
	}
	if focus != "" {
		references.add(layout.PaneID(focus))
	}
	for _, id := range references.ids {
		if _, ok := ids[id]; !ok {
			return ErrUnknownPane
		}
	}
	return nil
}

func preflightObjectOrder(r *payloadReader, n uint16) error {
	if n < 2 {
		return fmt.Errorf("%w: missing tail or visible object", ErrInvalidData)
	}
	seenTail := false
	for i := uint16(0); i < n; i++ {
		role, err := r.getUint8()
		if err != nil {
			return err
		}
		if len(r.b) < sha256.Size {
			return ErrShortPayload
		}
		r.b = r.b[sha256.Size:]
		switch ObjectRole(role) {
		case ObjectRoleHistory:
			if seenTail {
				return fmt.Errorf("%w: object role order", ErrInvalidData)
			}
		case ObjectRoleTail:
			if seenTail || i+1 >= n {
				return fmt.Errorf("%w: object role order", ErrInvalidData)
			}
			seenTail = true
		case ObjectRoleVisible:
			if !seenTail || i+1 != n {
				return fmt.Errorf("%w: object role order", ErrInvalidData)
			}
			return nil
		default:
			return fmt.Errorf("%w: object role", ErrInvalidData)
		}
	}
	return fmt.Errorf("%w: missing visible object", ErrInvalidData)
}

func validObjectRole(role ObjectRole) bool {
	return role == ObjectRoleHistory || role == ObjectRoleTail || role == ObjectRoleVisible
}

func isZeroDigest(digest SnapshotDigest) bool { return isZeroDigestBytes(digest[:]) }
func isZeroDigestBytes(digest []byte) bool {
	for _, b := range digest {
		if b != 0 {
			return false
		}
	}
	return true
}
