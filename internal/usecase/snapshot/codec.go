package snapshot

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
)

var (
	ErrBadMagic      = errors.New("snapshot: bad magic")
	ErrBadVersion    = errors.New("snapshot: bad version")
	ErrBadCRC        = errors.New("snapshot: bad crc")
	ErrShortPayload  = errors.New("snapshot: payload too short")
	ErrTrailingBytes = errors.New("snapshot: trailing bytes")
	ErrStyleOOB      = errors.New("snapshot: style index out of range")
	ErrUnknownPane   = errors.New("snapshot: tree references unknown pane")
	ErrInvalidData   = errors.New("snapshot: invalid data")
)

const (
	magic              = "VEVS"
	version            = uint16(1)
	flagFlate          = uint16(1 << 0)
	flateCutoff        = 4 << 10
	maxDecodedBodySize = 256 << 20
	maxDecodedRows     = 1 << 20
	maxDecodedRuns     = 16 << 20
	maxDecodedCells    = 16 << 20
)

// Marshal encodes a v1 durable snapshot.
func Marshal(s Session) ([]byte, error) {
	var w payloadWriter
	if err := writeSession(&w, s); err != nil {
		return nil, err
	}
	body := w.b
	flags := uint16(0)
	if len(body) > flateCutoff {
		var compressed bytes.Buffer
		fw, err := flate.NewWriter(&compressed, flate.DefaultCompression)
		if err != nil {
			return nil, err
		}
		if _, err := fw.Write(body); err != nil {
			_ = fw.Close()
			return nil, err
		}
		if err := fw.Close(); err != nil {
			return nil, err
		}
		body = compressed.Bytes()
		flags |= flagFlate
	}
	if len(body) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: body too large", ErrInvalidData)
	}
	out := make([]byte, 16+len(body))
	copy(out[:4], magic)
	binary.BigEndian.PutUint16(out[4:6], version)
	binary.BigEndian.PutUint16(out[6:8], flags)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(body)))
	binary.BigEndian.PutUint32(out[12:16], crc32.ChecksumIEEE(body))
	copy(out[16:], body)
	return out, nil
}

// Unmarshal decodes a strict v1 durable snapshot.
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
	flags := binary.BigEndian.Uint16(b[6:8])
	if flags&^flagFlate != 0 {
		return Session{}, fmt.Errorf("%w: unknown flags", ErrInvalidData)
	}
	bodyLen := int(binary.BigEndian.Uint32(b[8:12]))
	if bodyLen > maxDecodedBodySize {
		return Session{}, fmt.Errorf("%w: body too large", ErrInvalidData)
	}
	if len(b) < 16+bodyLen {
		return Session{}, ErrShortPayload
	}
	if len(b) != 16+bodyLen {
		return Session{}, ErrTrailingBytes
	}
	body := b[16:]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(b[12:16]) {
		return Session{}, ErrBadCRC
	}
	if flags&flagFlate != 0 {
		fr := flate.NewReader(bytes.NewReader(body))
		decompressed, err := io.ReadAll(io.LimitReader(fr, maxDecodedBodySize+1))
		cerr := fr.Close()
		if err != nil {
			return Session{}, err
		}
		if cerr != nil {
			return Session{}, cerr
		}
		if len(decompressed) > maxDecodedBodySize {
			return Session{}, fmt.Errorf("%w: decompressed body too large", ErrInvalidData)
		}
		body = decompressed
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
	w.putUint16(t.Cols)
	w.putUint16(t.Rows)
	w.putUint64(t.NextPaneID)
	if err := w.putString(string(t.Focus)); err != nil {
		return err
	}
	if t.Tree == nil {
		t.Tree = &layout.Tree{Focus: t.Focus}
	}
	if err := writeNode(w, t.Tree.Root); err != nil {
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
	t := Tab{Cols: cols, Rows: rows, NextPaneID: next, Focus: layout.PaneID(focus), Tree: &layout.Tree{Root: root, Focus: layout.PaneID(focus)}, Panes: make([]Pane, np)}
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
		w.putUint8(0)
		return w.putString("")
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
	if err := w.putString(p.Cwd); err != nil {
		return err
	}
	styles := styleTable(p)
	if len(styles) > math.MaxUint16 {
		return fmt.Errorf("%w: too many styles", ErrInvalidData)
	}
	w.putUint16(uint16(len(styles)))
	for _, s := range styles {
		writeStyle(w, s)
	}
	idx := make(map[renderer.Style]uint16, len(styles))
	for i, s := range styles {
		idx[s] = uint16(i)
	}
	if err := writeRows(w, p.Scrollback, idx, false); err != nil {
		return err
	}
	return writeRows(w, p.Visible, idx, true)
}

func readPane(r *payloadReader) (Pane, error) {
	id, err := r.getString()
	if err != nil {
		return Pane{}, err
	}
	cwd, err := r.getString()
	if err != nil {
		return Pane{}, err
	}
	ns, err := r.getUint16()
	if err != nil {
		return Pane{}, err
	}
	styles := make([]renderer.Style, ns)
	for i := range styles {
		s, err := readStyle(r)
		if err != nil {
			return Pane{}, err
		}
		styles[i] = s
	}
	budget := rowBudget{cells: maxDecodedCells, runs: maxDecodedRuns}
	sb, err := readRows(r, styles, &budget)
	if err != nil {
		return Pane{}, err
	}
	vis, err := readRows(r, styles, &budget)
	if err != nil {
		return Pane{}, err
	}
	return Pane{ID: layout.PaneID(id), Cwd: cwd, Scrollback: sb, Visible: vis}, nil
}

func writeRows(w *payloadWriter, rows [][]renderer.Cell, idx map[renderer.Style]uint16, trimBlankRows bool) error {
	clean := trimRows(rows, trimBlankRows)
	if len(clean) > math.MaxUint32 {
		return fmt.Errorf("%w: too many rows", ErrInvalidData)
	}
	w.putUint32(uint32(len(clean)))
	for _, row := range clean {
		runs := makeRuns(row)
		if len(runs) > math.MaxUint16 {
			return fmt.Errorf("%w: too many runs", ErrInvalidData)
		}
		w.putUint16(uint16(len(runs)))
		for _, run := range runs {
			w.putUint16(uint16(run.len))
			w.putUint32(uint32(run.cell.Rune))
			w.putUint16(idx[run.cell.Style])
			var flags uint8
			if run.cell.Continuation {
				flags |= 1
			}
			w.putUint8(flags)
		}
	}
	return nil
}

type rowBudget struct {
	cells int
	runs  int
}

func readRows(r *payloadReader, styles []renderer.Style, budget *rowBudget) ([][]renderer.Cell, error) {
	n, err := r.getUint32()
	if err != nil {
		return nil, err
	}
	if n > maxDecodedRows {
		return nil, fmt.Errorf("%w: too many rows", ErrInvalidData)
	}
	if uint64(n) > uint64(len(r.b))/2 {
		return nil, ErrShortPayload
	}
	rows := make([][]renderer.Cell, int(n))
	for i := range rows {
		nr, err := r.getUint16()
		if err != nil {
			return nil, err
		}
		if budget != nil {
			if int(nr) > budget.runs {
				return nil, fmt.Errorf("%w: too many runs", ErrInvalidData)
			}
			budget.runs -= int(nr)
		}
		if uint64(nr) > uint64(len(r.b))/9 {
			return nil, ErrShortPayload
		}
		rowCells := 0
		for j := 0; j < int(nr); j++ {
			ln, err := r.getUint16()
			if err != nil {
				return nil, err
			}
			rn, err := r.getUint32()
			if err != nil {
				return nil, err
			}
			si, err := r.getUint16()
			if err != nil {
				return nil, err
			}
			flags, err := r.getUint8()
			if err != nil {
				return nil, err
			}
			if int(si) >= len(styles) {
				return nil, ErrStyleOOB
			}
			if flags&^1 != 0 {
				return nil, fmt.Errorf("%w: unknown cell flags", ErrInvalidData)
			}
			if budget != nil {
				if int(ln) > budget.cells {
					return nil, fmt.Errorf("%w: too many cells", ErrInvalidData)
				}
				budget.cells -= int(ln)
			}
			cell := renderer.Cell{Rune: rune(rn), Style: styles[si], Continuation: flags&1 != 0}
			rowCells += int(ln)
			rows[i] = append(rows[i], make([]renderer.Cell, int(ln))...)
			for k := len(rows[i]) - int(ln); k < len(rows[i]); k++ {
				rows[i][k] = cell
			}
		}
		if rowCells == 0 {
			rows[i] = nil
		}
	}
	return rows, nil
}

type cellRun struct {
	len  int
	cell renderer.Cell
}

func makeRuns(row []renderer.Cell) []cellRun {
	if len(row) == 0 {
		return nil
	}
	runs := []cellRun{{len: 1, cell: row[0]}}
	for _, c := range row[1:] {
		last := &runs[len(runs)-1]
		if last.cell.Equal(c) && last.len < math.MaxUint16 {
			last.len++
		} else {
			runs = append(runs, cellRun{len: 1, cell: c})
		}
	}
	return runs
}

func trimRows(rows [][]renderer.Cell, trimBlankRows bool) [][]renderer.Cell {
	out := make([][]renderer.Cell, len(rows))
	for i, row := range rows {
		out[i] = trimTrailingBlankCells(row)
	}
	if trimBlankRows {
		for len(out) > 0 && len(out[len(out)-1]) == 0 {
			out = out[:len(out)-1]
		}
	}
	return out
}

func trimTrailingBlankCells(row []renderer.Cell) []renderer.Cell {
	end := len(row)
	for end > 0 && isBlank(row[end-1]) {
		end--
	}
	return append([]renderer.Cell(nil), row[:end]...)
}
func isBlank(c renderer.Cell) bool {
	return !c.Continuation && c.Rune == ' ' && c.Style.Equal(renderer.DefaultStyle())
}

func styleTable(p Pane) []renderer.Style {
	seen := map[renderer.Style]bool{}
	var styles []renderer.Style
	addRows := func(rows [][]renderer.Cell, trimBlankRows bool) {
		for _, row := range trimRows(rows, trimBlankRows) {
			for _, c := range row {
				if !seen[c.Style] {
					seen[c.Style] = true
					styles = append(styles, c.Style)
				}
			}
		}
	}
	addRows(p.Scrollback, false)
	addRows(p.Visible, true)
	return styles
}

func writeStyle(w *payloadWriter, s renderer.Style) {
	var flags uint8
	if s.Bold {
		flags |= 1
	}
	if s.Inverse {
		flags |= 2
	}
	if s.HasForegroundRGB {
		flags |= 4
	}
	if s.HasBackgroundRGB {
		flags |= 8
	}
	w.putUint8(flags)
	w.putUint32(uint32(int32(s.Foreground)))
	w.putUint32(uint32(int32(s.Background)))
	w.putUint8(s.ForegroundRGB.R)
	w.putUint8(s.ForegroundRGB.G)
	w.putUint8(s.ForegroundRGB.B)
	w.putUint8(s.BackgroundRGB.R)
	w.putUint8(s.BackgroundRGB.G)
	w.putUint8(s.BackgroundRGB.B)
}

func readStyle(r *payloadReader) (renderer.Style, error) {
	flags, err := r.getUint8()
	if err != nil {
		return renderer.Style{}, err
	}
	fg, err := r.getUint32()
	if err != nil {
		return renderer.Style{}, err
	}
	bg, err := r.getUint32()
	if err != nil {
		return renderer.Style{}, err
	}
	fr, err := r.getUint8()
	if err != nil {
		return renderer.Style{}, err
	}
	fgc, err := r.getUint8()
	if err != nil {
		return renderer.Style{}, err
	}
	fb, err := r.getUint8()
	if err != nil {
		return renderer.Style{}, err
	}
	br, err := r.getUint8()
	if err != nil {
		return renderer.Style{}, err
	}
	bgc, err := r.getUint8()
	if err != nil {
		return renderer.Style{}, err
	}
	bb, err := r.getUint8()
	if err != nil {
		return renderer.Style{}, err
	}
	return renderer.Style{Bold: flags&1 != 0, Inverse: flags&2 != 0, HasForegroundRGB: flags&4 != 0, HasBackgroundRGB: flags&8 != 0, Foreground: int(int32(fg)), Background: int(int32(bg)), ForegroundRGB: renderer.RGB{R: fr, G: fgc, B: fb}, BackgroundRGB: renderer.RGB{R: br, G: bgc, B: bb}}, nil
}

func validateTree(t *layout.Tree, ids map[layout.PaneID]struct{}) error {
	if t == nil || t.Root == nil {
		return nil
	}
	return validateNode(t.Root, ids)
}

func validateNode(n *layout.Node, ids map[layout.PaneID]struct{}) error {
	if n == nil {
		return nil
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
