package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/vt"
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
	version              = uint16(3)
	maxDecodedBodySize   = 256 << 20
	maxSnapshotBlobs     = 1 << 16
	maxSnapshotBytes     = 256 << 20
	maxSnapshotRows      = 1 << 20
	maxSnapshotCells     = 16 << 20
	maxSnapshotTreeNodes = 1 << 15
	// 256 MiB accommodates a full 10k-row history at practical terminal widths
	// while bounding hostile manifests before decoded slices are allocated.
	maxSnapshotDecodedAllocation = 256 << 20
)

// Marshal encodes the strict v3, flags-zero durable envelope.
func Marshal(s Session) ([]byte, error) {
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

// Unmarshal validates the complete manifest and all VT blobs without
// allocations first, then decodes the already preflighted data.
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

type payloadReader struct{ b []byte }

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

func writeSession(w *payloadWriter, s Session) error {
	if err := w.putString(s.Name); err != nil {
		return err
	}
	w.putUint64(s.CreatedAt)
	w.putUint16(s.Active)
	if len(s.Tabs) > math.MaxUint16 {
		return fmt.Errorf("%w: too many tabs", ErrInvalidData)
	}
	w.putUint16(uint16(len(s.Tabs)))
	for _, t := range s.Tabs {
		if err := writeTab(w, t); err != nil {
			return err
		}
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

func writeNode(w *payloadWriter, n *layout.Node) error {
	if n == nil {
		w.putUint8(3)
		return nil
	}
	switch n.Kind {
	case layout.Leaf:
		w.putUint8(0)
		return w.putString(string(n.Leaf))
	case layout.Split:
		w.putUint8(1)
		w.putUint8(uint8(n.Dir))
		return writeChildren(w, n.Children)
	case layout.Stack:
		w.putUint8(2)
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

func readNode(r *payloadReader) (*layout.Node, error) {
	kind, err := r.getUint8()
	if err != nil {
		return nil, err
	}
	switch kind {
	case 0:
		leaf, err := r.getString()
		if err != nil {
			return nil, err
		}
		return layout.NewLeaf(layout.PaneID(leaf)), nil
	case 1:
		dir, err := r.getUint8()
		if err != nil {
			return nil, err
		}
		children, err := readChildren(r)
		if err != nil {
			return nil, err
		}
		return &layout.Node{Kind: layout.Split, Dir: layout.SplitDir(dir), Children: children}, nil
	case 2:
		exp, err := r.getString()
		if err != nil {
			return nil, err
		}
		children, err := readChildren(r)
		if err != nil {
			return nil, err
		}
		return &layout.Node{Kind: layout.Stack, Expanded: layout.PaneID(exp), Children: children}, nil
	case 3:
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
	if len(p.SealedChunks) > maxSnapshotBlobs {
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
	if n > maxSnapshotBlobs || uint64(n) > uint64(len(r.b))/4 {
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
	visible, err := r.getBlob()
	if err != nil {
		return Pane{}, err
	}
	proc, err := readProcess(r)
	if err != nil {
		return Pane{}, err
	}
	return Pane{ID: layout.PaneID(id), StableID: stableID, Cwd: cwd, SealedChunks: sealed, Tail: tail, Visible: visible, Process: proc}, nil
}

func writeProcess(w *payloadWriter, p *Process) error {
	if p == nil {
		w.putUint8(0)
		return nil
	}
	if len(p.Argv) == 0 {
		return fmt.Errorf("%w: process argv empty", ErrInvalidData)
	}
	w.putUint8(1)
	if err := w.putStrings(p.Argv); err != nil {
		return err
	}
	if err := w.putString(p.Strategy); err != nil {
		return err
	}
	return w.putString(p.Opts.AgentSessionID)
}

func readProcess(r *payloadReader) (*Process, error) {
	present, err := r.getUint8()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	if present != 1 {
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

// preflightSession walks framing and opaque VT blobs without constructing a
// Session. All count-derived allocation happens only in the second pass.
func preflightSession(body []byte) error {
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
	if uint64(tabs) > uint64(len(r.b))/2 {
		return ErrShortPayload
	}
	var totals vt.DecodeStats
	blobs := uint64(0)
	budget := preflightBudget{}
	for range tabs {
		if err := preflightTab(&r, &totals, &blobs, &budget); err != nil {
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

func preflightTab(r *payloadReader, totals *vt.DecodeStats, blobs *uint64, budget *preflightBudget) error {
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
	focus, err := r.getString()
	if err != nil {
		return err
	}
	references := preflightTreeReferences{}
	if err := preflightNode(r, 0, budget, &references); err != nil {
		return err
	}
	panes, err := r.getUint16()
	if err != nil {
		return err
	}
	if uint64(panes) > uint64(len(r.b))/2 {
		return ErrShortPayload
	}
	ids := make(map[layout.PaneID]struct{})
	for range panes {
		if err := preflightPane(r, totals, blobs, ids, budget); err != nil {
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

type preflightTreeReferences struct{ ids []layout.PaneID }

func (r *preflightTreeReferences) add(id layout.PaneID, budget *preflightBudget) error {
	if !budget.addAllocation(32) {
		return fmt.Errorf("%w: decoded allocation budget", ErrInvalidData)
	}
	r.ids = append(r.ids, id)
	return nil
}

func preflightNode(r *payloadReader, depth int, budget *preflightBudget, references *preflightTreeReferences) error {
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
		leaf, err := r.getString()
		if err != nil {
			return err
		}
		return references.add(layout.PaneID(leaf), budget)
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
			if err := references.add(layout.PaneID(expanded), budget); err != nil {
				return err
			}
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
	if uint64(children) > uint64(len(r.b)) {
		return ErrShortPayload
	}
	for range children {
		if err := preflightNode(r, depth+1, budget, references); err != nil {
			return err
		}
	}
	return nil
}

func preflightPane(r *payloadReader, totals *vt.DecodeStats, blobs *uint64, ids map[layout.PaneID]struct{}, budget *preflightBudget) error {
	if !budget.addAllocation(256) {
		return fmt.Errorf("%w: decoded allocation budget", ErrInvalidData)
	}
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
	if n > maxSnapshotBlobs || uint64(n) > uint64(len(r.b))/4 {
		return fmt.Errorf("%w: sealed chunks", ErrInvalidData)
	}
	for range n {
		stats, err := preflightBlob(r, totals, blobs, true, budget)
		if err != nil {
			return err
		}
		if stats.Chunks != 1 || stats.Rows == 0 {
			return fmt.Errorf("%w: sealed blob role", ErrInvalidData)
		}
	}
	tail, err := preflightBlob(r, totals, blobs, true, budget)
	if err != nil {
		return err
	}
	if tail.Chunks != 0 || tail.Rows != 0 || tail.Cells != 0 {
		return fmt.Errorf("%w: tail blob role", ErrInvalidData)
	}
	visible, err := preflightBlob(r, totals, blobs, false, budget)
	if err != nil {
		return err
	}
	// PreflightVisibleBlob above validates width×height and exact payload
	// cardinality. A visible terminal frame itself must also be nonempty.
	if visible.Rows == 0 || visible.Cells == 0 {
		return fmt.Errorf("%w: visible geometry", ErrInvalidData)
	}
	return preflightProcess(r)
}

func preflightBlob(r *payloadReader, totals *vt.DecodeStats, blobs *uint64, history bool, budget *preflightBudget) (vt.DecodeStats, error) {
	n, err := r.getUint32()
	if err != nil {
		return vt.DecodeStats{}, err
	}
	if n == 0 || uint64(n) > uint64(len(r.b)) || uint64(n) > maxSnapshotBytes {
		return vt.DecodeStats{}, fmt.Errorf("%w: VT blob length", ErrInvalidData)
	}
	if *blobs >= maxSnapshotBlobs {
		return vt.DecodeStats{}, fmt.Errorf("%w: too many VT blobs", ErrInvalidData)
	}
	blob := r.b[:n]
	r.b = r.b[n:]
	var stats vt.DecodeStats
	if history {
		stats, err = vt.PreflightHistoryBlob(blob)
	} else {
		stats, err = vt.PreflightVisibleBlob(blob)
	}
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
