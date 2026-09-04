package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/bnema/vev/internal/usecase/layout"
)

var (
	ErrBadMagic      = errors.New("snapshot: bad magic")
	ErrBadVersion    = errors.New("snapshot: bad version")
	ErrBadCRC        = errors.New("snapshot: bad crc")
	ErrShortPayload  = errors.New("snapshot: payload too short")
	ErrTrailingBytes = errors.New("snapshot: trailing bytes")
	ErrUnknownPane   = errors.New("snapshot: tree references unknown pane")
	ErrInvalidData   = errors.New("snapshot: invalid data")
)

const (
	magic                = "VEVS"
	version              = uint16(4)
	maxDecodedBodySize   = 256 << 20
	maxSnapshotObjects   = 1 << 16
	maxSnapshotBytes     = 256 << 20
	maxSnapshotRows      = 1 << 20
	maxSnapshotCells     = 16 << 20
	maxSnapshotTreeNodes = 1 << 15
	// 256 MiB accommodates a full 10k-row history at practical terminal widths
	// while bounding hostile manifests before decoded slices are allocated.
	maxSnapshotDecodedAllocation = 256 << 20
)

// Unmarshal first measures the complete manifest and its VT blobs without
// allocating. After accepting aggregate budgets, it validates references and
// blob semantics before decoding the session.
func Unmarshal(b []byte) (Session, error) {
	if len(b) < 16 {
		return Session{}, ErrShortPayload
	}
	if string(b[:4]) != magic {
		return Session{}, ErrBadMagic
	}
	if binary.BigEndian.Uint16(b[4:6]) != version {
		return Session{}, ErrBadVersion
	}
	if binary.BigEndian.Uint16(b[6:8]) != 0 {
		return Session{}, fmt.Errorf("%w: flags", ErrInvalidData)
	}
	bodyLen := binary.BigEndian.Uint32(b[8:12])
	if bodyLen > maxDecodedBodySize {
		return Session{}, fmt.Errorf("%w: body too large", ErrInvalidData)
	}
	if uint64(len(b)) < 16+uint64(bodyLen) {
		return Session{}, ErrShortPayload
	}
	if uint64(len(b)) != 16+uint64(bodyLen) {
		return Session{}, ErrTrailingBytes
	}
	body := b[16:]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(b[12:16]) {
		return Session{}, ErrBadCRC
	}
	if err := preflightSession(body); err != nil {
		return Session{}, err
	}
	r := payloadReader{b: body}
	s, err := readSession(&r)
	if err != nil {
		return Session{}, err
	}
	if err := r.done(); err != nil {
		return Session{}, err
	}
	for _, tab := range s.Tabs {
		ids := make(map[layout.PaneID]struct{}, len(tab.Panes))
		for _, p := range tab.Panes {
			ids[p.ID] = struct{}{}
		}
		if _, ok := ids[tab.Focus]; tab.Focus != "" && !ok {
			return Session{}, ErrUnknownPane
		}
		if err := validateTree(tab.Tree, ids); err != nil {
			return Session{}, err
		}
	}
	return s, nil
}

type payloadWriter struct{ b []byte }

func (w *payloadWriter) putUint8(v uint8) { w.b = append(w.b, v) }
func (w *payloadWriter) putUint16(v uint16) {
	var t [2]byte
	binary.BigEndian.PutUint16(t[:], v)
	w.b = append(w.b, t[:]...)
}
func (w *payloadWriter) putUint32(v uint32) {
	var t [4]byte
	binary.BigEndian.PutUint32(t[:], v)
	w.b = append(w.b, t[:]...)
}
func (w *payloadWriter) putUint64(v uint64) {
	var t [8]byte
	binary.BigEndian.PutUint64(t[:], v)
	w.b = append(w.b, t[:]...)
}
func (w *payloadWriter) putString(s string) error {
	if len(s) > math.MaxUint16 {
		return fmt.Errorf("%w: string too long", ErrInvalidData)
	}
	w.putUint16(uint16(len(s)))
	w.b = append(w.b, s...)
	return nil
}
func (w *payloadWriter) putStrings(ss []string) error {
	if len(ss) > math.MaxUint16 {
		return fmt.Errorf("%w: too many strings", ErrInvalidData)
	}
	w.putUint16(uint16(len(ss)))
	for _, s := range ss {
		if err := w.putString(s); err != nil {
			return err
		}
	}
	return nil
}

func writeNode(w *payloadWriter, n *layout.Node) error {
	if n == nil {
		w.putUint8(manifestNodeNil)
		return nil
	}
	switch n.Kind {
	case layout.Leaf:
		w.putUint8(manifestNodeLeaf)
		return w.putString(string(n.Leaf))
	case layout.Split:
		w.putUint8(manifestNodeSplit)
		w.putUint8(uint8(n.Dir))
		return writeChildren(w, n.Children)
	case layout.Stack:
		w.putUint8(manifestNodeStack)
		if err := w.putString(string(n.Expanded)); err != nil {
			return err
		}
		return writeChildren(w, n.Children)
	default:
		return fmt.Errorf("%w: unknown node kind", ErrInvalidData)
	}
}

func writeChildren(w *payloadWriter, children []*layout.Node) error {
	if len(children) > math.MaxUint16 {
		return fmt.Errorf("%w: too many children", ErrInvalidData)
	}
	w.putUint16(uint16(len(children)))
	for _, c := range children {
		if err := writeNode(w, c); err != nil {
			return err
		}
	}
	return nil
}

func writeProcess(w *payloadWriter, p *Process) error {
	if p == nil {
		w.putUint8(processAbsent)
		return nil
	}
	if len(p.Argv) == 0 {
		return fmt.Errorf("%w: process argv empty", ErrInvalidData)
	}
	w.putUint8(processPresent)
	if err := w.putStrings(p.Argv); err != nil {
		return err
	}
	if err := w.putString(p.Strategy); err != nil {
		return err
	}
	return w.putString(p.Opts.AgentSessionID)
}

type payloadReader struct{ b []byte }

func (r *payloadReader) getBytes(n int) ([]byte, error) {
	if n < 0 || len(r.b) < n {
		return nil, ErrShortPayload
	}
	b := r.b[:n]
	r.b = r.b[n:]
	return b, nil
}

func (r *payloadReader) getUint8() (uint8, error) {
	if len(r.b) < 1 {
		return 0, ErrShortPayload
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v, nil
}
func (r *payloadReader) getUint16() (uint16, error) {
	if len(r.b) < 2 {
		return 0, ErrShortPayload
	}
	v := binary.BigEndian.Uint16(r.b)
	r.b = r.b[2:]
	return v, nil
}
func (r *payloadReader) getUint32() (uint32, error) {
	if len(r.b) < 4 {
		return 0, ErrShortPayload
	}
	v := binary.BigEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v, nil
}
func (r *payloadReader) getUint64() (uint64, error) {
	if len(r.b) < 8 {
		return 0, ErrShortPayload
	}
	v := binary.BigEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v, nil
}
func (r *payloadReader) getString() (string, error) {
	n, err := r.getUint16()
	if err != nil {
		return "", err
	}
	if len(r.b) < int(n) {
		return "", ErrShortPayload
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s, nil
}
func (r *payloadReader) getStrings() ([]string, error) {
	n, err := r.getUint16()
	if err != nil {
		return nil, err
	}
	out := make([]string, n)
	for i := range out {
		s, err := r.getString()
		if err != nil {
			return nil, err
		}
		out[i] = s
	}
	return out, nil
}
func (r *payloadReader) getBlob() ([]byte, error) {
	n, err := r.getUint32()
	if err != nil {
		return nil, err
	}
	if uint64(n) > uint64(len(r.b)) {
		return nil, ErrShortPayload
	}
	blob := append([]byte(nil), r.b[:n]...)
	r.b = r.b[n:]
	return blob, nil
}
func (r *payloadReader) done() error {
	if len(r.b) != 0 {
		return ErrTrailingBytes
	}
	return nil
}

func readSession(r *payloadReader) (Session, error) {
	name, err := r.getString()
	if err != nil {
		return Session{}, err
	}
	created, err := r.getUint64()
	if err != nil {
		return Session{}, err
	}
	active, err := r.getUint16()
	if err != nil {
		return Session{}, err
	}
	nt, err := r.getUint16()
	if err != nil {
		return Session{}, err
	}
	if err := validateActive(active, int(nt)); err != nil {
		return Session{}, err
	}
	s := Session{Name: name, CreatedAt: created, Active: active, Tabs: make([]Tab, nt)}
	for i := range s.Tabs {
		t, err := readTab(r)
		if err != nil {
			return Session{}, err
		}
		s.Tabs[i] = t
	}
	return s, nil
}

func readTab(r *payloadReader) (Tab, error) {
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
	np, err := r.getUint16()
	if err != nil {
		return Tab{}, err
	}
	t := Tab{StableID: stableID, Cols: cols, Rows: rows, NextPaneID: next, Focus: layout.PaneID(focus), Tree: &layout.Tree{Root: root, Focus: layout.PaneID(focus)}, Panes: make([]Pane, np)}
	for i := range t.Panes {
		p, err := readPane(r)
		if err != nil {
			return Tab{}, err
		}
		t.Panes[i] = p
	}
	return t, nil
}

func readNode(r *payloadReader) (*layout.Node, error) {
	kind, err := r.getUint8()
	if err != nil {
		return nil, err
	}
	switch kind {
	case manifestNodeLeaf:
		leaf, err := r.getString()
		if err != nil {
			return nil, err
		}
		return layout.NewLeaf(layout.PaneID(leaf)), nil
	case manifestNodeSplit:
		dir, err := r.getUint8()
		if err != nil {
			return nil, err
		}
		children, err := readChildren(r)
		if err != nil {
			return nil, err
		}
		return &layout.Node{Kind: layout.Split, Dir: layout.SplitDir(dir), Children: children}, nil
	case manifestNodeStack:
		exp, err := r.getString()
		if err != nil {
			return nil, err
		}
		children, err := readChildren(r)
		if err != nil {
			return nil, err
		}
		return &layout.Node{Kind: layout.Stack, Expanded: layout.PaneID(exp), Children: children}, nil
	case manifestNodeNil:
		return nil, nil
	default:
		return nil, fmt.Errorf("%w: unknown node kind", ErrInvalidData)
	}
}

func readChildren(r *payloadReader) ([]*layout.Node, error) {
	n, err := r.getUint16()
	if err != nil {
		return nil, err
	}
	out := make([]*layout.Node, n)
	for i := range out {
		c, err := readNode(r)
		if err != nil {
			return nil, err
		}
		out[i] = c
	}
	return out, nil
}

func readPane(r *payloadReader) (Pane, error) {
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
	n, err := r.getUint32()
	if err != nil {
		return Pane{}, err
	}
	if n > maxSnapshotObjects || uint64(n) > uint64(len(r.b))/4 {
		return Pane{}, fmt.Errorf("%w: sealed chunks", ErrInvalidData)
	}
	sealed := make([][]byte, n)
	for i := range sealed {
		if sealed[i], err = r.getBlob(); err != nil {
			return Pane{}, err
		}
	}
	tail, err := r.getBlob()
	if err != nil {
		return Pane{}, err
	}
	transcript, err := r.getBlob()
	if err != nil {
		return Pane{}, err
	}
	proc, err := readProcess(r)
	if err != nil {
		return Pane{}, err
	}
	return Pane{ID: layout.PaneID(id), StableID: stableID, Cwd: cwd, SealedChunks: sealed, Tail: tail, Transcript: transcript, Process: proc}, nil
}

func readProcess(r *payloadReader) (*Process, error) {
	present, err := r.getUint8()
	if err != nil {
		return nil, err
	}
	if present == processAbsent {
		return nil, nil
	}
	if present != processPresent {
		return nil, fmt.Errorf("%w: process presence", ErrInvalidData)
	}
	argv, err := r.getStrings()
	if err != nil {
		return nil, err
	}
	if len(argv) == 0 {
		return nil, fmt.Errorf("%w: process argv empty", ErrInvalidData)
	}
	strategy, err := r.getString()
	if err != nil {
		return nil, err
	}
	agentSessionID, err := r.getString()
	if err != nil {
		return nil, err
	}
	return &Process{Argv: argv, Strategy: strategy, Opts: ProcessOpts{AgentSessionID: agentSessionID}}, nil
}

func validateActive(active uint16, tabCount int) error {
	if tabCount == 0 {
		if active == 0 {
			return nil
		}
		return fmt.Errorf("%w: active tab", ErrInvalidData)
	}
	if int(active) >= tabCount {
		return fmt.Errorf("%w: active tab", ErrInvalidData)
	}
	return nil
}

// validateTree checks that every pane reference in the layout tree resolves
// to a pane declared by the enclosing tab.
func validateTree(t *layout.Tree, ids map[layout.PaneID]struct{}) error {
	if t == nil || t.Root == nil {
		return nil
	}
	return validateNode(t.Root, ids)
}

func validateNode(n *layout.Node, ids map[layout.PaneID]struct{}) error {
	if n == nil {
		return fmt.Errorf("%w: nil child node", ErrInvalidData)
	}
	switch n.Kind {
	case layout.Leaf:
		if _, ok := ids[n.Leaf]; !ok {
			return ErrUnknownPane
		}
	case layout.Split:
		if n.Dir != layout.Horizontal && n.Dir != layout.Vertical {
			return fmt.Errorf("%w: split dir", ErrInvalidData)
		}
		for _, c := range n.Children {
			if err := validateNode(c, ids); err != nil {
				return err
			}
		}
	case layout.Stack:
		if _, ok := ids[n.Expanded]; n.Expanded != "" && !ok {
			return ErrUnknownPane
		}
		for _, c := range n.Children {
			if err := validateNode(c, ids); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%w: node kind", ErrInvalidData)
	}
	return nil
}
