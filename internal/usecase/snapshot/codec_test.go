package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"testing"

	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
)

func TestMarshalMinimalGolden(t *testing.T) {
	got, err := Marshal(Session{Name: "s", CreatedAt: 7})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	want := []byte{
		'V', 'E', 'V', 'S', 0, 2, 0, 0, 0, 0, 0, 15, 0xff, 0xc2, 0x2e, 0xcb,
		0, 1, 's', 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Marshal minimal bytes\ngot  % x\nwant % x", got, want)
	}
}

func TestRoundTripNilTreeAndRootDoNotMaterializeLeaf(t *testing.T) {
	cases := []struct {
		name string
		tab  Tab
	}{
		{name: "nil tree", tab: Tab{Tree: nil}},
		{name: "nil root", tab: Tab{Tree: &layout.Tree{Root: nil}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := Marshal(Session{Name: "s", Tabs: []Tab{tc.tab}})
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			got, err := Unmarshal(b)
			if err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if len(got.Tabs) != 1 {
				t.Fatalf("tabs len = %d, want 1", len(got.Tabs))
			}
			if got.Tabs[0].Tree == nil {
				t.Fatalf("Tree = nil, want tree with nil root")
			}
			if got.Tabs[0].Tree.Root != nil {
				t.Fatalf("Root = %#v, want nil", got.Tabs[0].Tree.Root)
			}
		})
	}
}

func TestRoundTripSessions(t *testing.T) {
	boldRGB := renderer.DefaultStyle()
	boldRGB.Bold = true
	boldRGB.HasForegroundRGB = true
	boldRGB.ForegroundRGB = renderer.RGB{R: 1, G: 2, B: 3}
	indexed := renderer.DefaultStyle()
	indexed.Foreground = 2
	indexed.Background = 4
	indexed.Inverse = true
	deepTree := &layout.Tree{Focus: "2", Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
		layout.NewLeaf("1"),
		&layout.Node{Kind: layout.Stack, Expanded: "2", Children: []*layout.Node{layout.NewLeaf("2"), layout.NewLeaf("3")}},
	}}}
	cjkVisible := [][]renderer.Cell{{
		{Rune: '好', Style: boldRGB},
		{Continuation: true, Style: boldRGB},
		{Rune: 'x', Style: indexed},
	}}
	multi := Session{Name: "named", CreatedAt: 42, Active: 1, Tabs: []Tab{
		{StableID: "t_stable", Cols: 100, Rows: 40, NextPaneID: 9, Focus: "2", Tree: deepTree, Panes: []Pane{{ID: "1", StableID: "p_one", Cwd: "/a", Scrollback: rows("abc"), Visible: rows("v"), Process: &Process{Argv: []string{"claude", "--resume"}, Strategy: "claude", Opts: ProcessOpts{AgentSessionID: "agent-123"}}}, {ID: "2", StableID: "p_two", Cwd: "/b", Visible: rows("focus")}, {ID: "3", StableID: "p_three", Cwd: "/c"}}},
		{Cols: 80, Rows: 24, NextPaneID: 2, Focus: "a", Tree: layout.NewTree("a"), Panes: []Pane{{ID: "a", Cwd: "/tmp", Visible: cjkVisible}}},
	}}
	empty := Session{Name: "blank", Tabs: []Tab{{Tree: layout.NewTree("p"), Focus: "p", Panes: []Pane{{ID: "p", Cwd: "/", Scrollback: nil, Visible: rows("   ", "")}}}}}
	large := Session{Name: "large", Tabs: []Tab{{Tree: layout.NewTree("p"), Focus: "p", Panes: []Pane{{ID: "p", Cwd: "/", Scrollback: manyRows(6000), Visible: rows("tail")}}}}}
	cases := []struct {
		name string
		s    Session
	}{
		{name: "multi tab deep tree", s: multi},
		{name: "empty and all blank", s: empty},
		{name: "large scrollback", s: large},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := Marshal(tc.s)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			got, err := Unmarshal(b)
			if err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if !equalSession(got, trimSession(tc.s)) {
				t.Fatalf("round trip mismatch\ngot  %#v\nwant %#v", got, trimSession(tc.s))
			}
		})
	}
}

func TestUnmarshalRejectsMalformedWithoutPanic(t *testing.T) {
	good, err := Marshal(Session{Name: "s", Tabs: []Tab{{Tree: layout.NewTree("p"), Focus: "p", Panes: []Pane{{ID: "p"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		data []byte
	}{
		{"bad magic", append([]byte("NOPE"), good[4:]...)},
		{"bad version", replaceU16(good, 4, 1)},
		{"bad crc", append([]byte(nil), good[:len(good)-1]...)},
		{"trailing", append(append([]byte(nil), good...), 0)},
		{"body len overrun", replaceU32(good, 8, 999999)},
		{"prefix", good[:len(good)/2]},
		{"style oob", styleOOBSnapshot(t)},
		{"unknown pane in tree", unknownPaneSnapshot(t)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Unmarshal panicked: %v", r)
				}
			}()
			if _, err := Unmarshal(tc.data); err == nil {
				t.Fatalf("Unmarshal() error = nil")
			}
		})
	}
	for i := 0; i < len(good); i++ {
		if _, err := Unmarshal(good[:i]); err == nil {
			t.Fatalf("prefix length %d unexpectedly succeeded", i)
		}
	}
}

func TestMarshalRejectsProcessWithoutArgv(t *testing.T) {
	_, err := Marshal(Session{Name: "s", Tabs: []Tab{{Tree: layout.NewTree("p"), Focus: "p", Panes: []Pane{{ID: "p", Process: &Process{Strategy: "generic"}}}}}})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("Marshal() error = %v, want %v", err, ErrInvalidData)
	}
}

func TestUnmarshalRejectsProcessWithoutArgv(t *testing.T) {
	var w payloadWriter
	_ = w.putString("x")
	w.putUint64(0)
	w.putUint16(0)
	w.putUint16(1)
	_ = w.putString("t")
	w.putUint16(80)
	w.putUint16(24)
	w.putUint64(1)
	_ = w.putString("p")
	_ = writeNode(&w, layout.NewLeaf("p"))
	w.putUint16(1)
	_ = w.putString("p")
	_ = w.putString("p_stable")
	_ = w.putString("")
	w.putUint16(0)
	w.putUint32(0)
	w.putUint32(0)
	w.putUint8(1)
	w.putUint16(0)
	_ = w.putString("generic")
	_ = w.putString("")
	_, err := Unmarshal(reheader(w.b, 0))
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("Unmarshal() error = %v, want %v", err, ErrInvalidData)
	}
}

func TestUnmarshalRejectsOversizedBodiesBeforeAllocation(t *testing.T) {
	body := []byte{0}
	oversizedRaw := reheader(body, 0)
	binary.BigEndian.PutUint32(oversizedRaw[8:12], uint32(maxDecodedBodySize+1))
	oversizedFlate := reheader(body, flagFlate)
	binary.BigEndian.PutUint32(oversizedFlate[8:12], uint32(maxDecodedBodySize+1))
	cases := []struct {
		name string
		data []byte
	}{
		{name: "raw", data: oversizedRaw},
		{name: "flate", data: oversizedFlate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Unmarshal(tc.data)
			if !errors.Is(err, ErrInvalidData) {
				t.Fatalf("Unmarshal() error = %v, want %v", err, ErrInvalidData)
			}
		})
	}
}

func TestUnmarshalRejectsMalformedRowCountsWithoutAllocation(t *testing.T) {
	cases := []struct {
		name string
		data []byte
		want error
	}{
		{name: "huge row count", data: malformedRowsSnapshot(math.MaxUint32, 0), want: ErrInvalidData},
		{name: "huge run count", data: malformedRowsSnapshot(1, math.MaxUint16), want: ErrShortPayload},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Unmarshal panicked: %v", r)
				}
			}()
			_, err := Unmarshal(tc.data)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Unmarshal() error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestFlateOnOff(t *testing.T) {
	small, err := Marshal(Session{Name: "small"})
	if err != nil {
		t.Fatal(err)
	}
	if flags := binary.BigEndian.Uint16(small[6:8]); flags != 0 {
		t.Fatalf("small flags = %#x, want 0", flags)
	}
	large, err := Marshal(Session{Name: "large", Tabs: []Tab{{Tree: layout.NewTree("p"), Focus: "p", Panes: []Pane{{ID: "p", Scrollback: manyRows(5000)}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if flags := binary.BigEndian.Uint16(large[6:8]); flags&1 == 0 {
		t.Fatalf("large flags = %#x, want flate bit", flags)
	}
}

func BenchmarkMarshal10KRows(b *testing.B) {
	s := Session{Name: "bench", Tabs: []Tab{{Tree: layout.NewTree("p"), Focus: "p", Panes: []Pane{{ID: "p", Cwd: "/tmp", Scrollback: manyRows(10000), Visible: rows("visible")}}}}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Marshal(s); err != nil {
			b.Fatal(err)
		}
	}
}

func rows(lines ...string) [][]renderer.Cell {
	out := make([][]renderer.Cell, len(lines))
	for i, line := range lines {
		for _, r := range line {
			out[i] = append(out[i], renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
		}
	}
	return out
}

func manyRows(n int) [][]renderer.Cell {
	out := make([][]renderer.Cell, n)
	for i := range out {
		out[i] = rows("row data row data row data")[0]
	}
	return out
}

func equalSession(a, b Session) bool {
	if a.Name != b.Name || a.CreatedAt != b.CreatedAt || a.Active != b.Active || len(a.Tabs) != len(b.Tabs) {
		return false
	}
	for i := range a.Tabs {
		if !equalTab(a.Tabs[i], b.Tabs[i]) {
			return false
		}
	}
	return true
}

func equalTab(a, b Tab) bool {
	if a.StableID != b.StableID || a.Cols != b.Cols || a.Rows != b.Rows || a.NextPaneID != b.NextPaneID || a.Focus != b.Focus || len(a.Panes) != len(b.Panes) || !equalNode(treeRoot(a.Tree), treeRoot(b.Tree)) {
		return false
	}
	for i := range a.Panes {
		if a.Panes[i].ID != b.Panes[i].ID || a.Panes[i].StableID != b.Panes[i].StableID || a.Panes[i].Cwd != b.Panes[i].Cwd || !equalProcess(a.Panes[i].Process, b.Panes[i].Process) || !equalRows(a.Panes[i].Scrollback, b.Panes[i].Scrollback) || !equalRows(a.Panes[i].Visible, b.Panes[i].Visible) {
			return false
		}
	}
	return true
}

func equalProcess(a, b *Process) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Strategy != b.Strategy || a.Opts.AgentSessionID != b.Opts.AgentSessionID || len(a.Argv) != len(b.Argv) {
		return false
	}
	for i := range a.Argv {
		if a.Argv[i] != b.Argv[i] {
			return false
		}
	}
	return true
}

func treeRoot(t *layout.Tree) *layout.Node {
	if t == nil {
		return nil
	}
	return t.Root
}

func equalNode(a, b *layout.Node) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Kind != b.Kind || a.Dir != b.Dir || a.Leaf != b.Leaf || a.Expanded != b.Expanded || len(a.Children) != len(b.Children) {
		return false
	}
	for i := range a.Children {
		if !equalNode(a.Children[i], b.Children[i]) {
			return false
		}
	}
	return true
}

func equalRows(a, b [][]renderer.Cell) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if !a[i][j].Equal(b[i][j]) {
				return false
			}
		}
	}
	return true
}

func trimSession(s Session) Session {
	for ti := range s.Tabs {
		for pi := range s.Tabs[ti].Panes {
			s.Tabs[ti].Panes[pi].Scrollback = trimRows(s.Tabs[ti].Panes[pi].Scrollback, false)
			s.Tabs[ti].Panes[pi].Visible = trimRows(s.Tabs[ti].Panes[pi].Visible, true)
		}
	}
	return s
}

func reheader(body []byte, flags uint16) []byte {
	out := make([]byte, 16+len(body))
	copy(out[:4], []byte("VEVS"))
	binary.BigEndian.PutUint16(out[4:6], version)
	binary.BigEndian.PutUint16(out[6:8], flags)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(body)))
	binary.BigEndian.PutUint32(out[12:16], crc32.ChecksumIEEE(body))
	copy(out[16:], body)
	return out
}

func replaceU16(b []byte, off int, v uint16) []byte {
	out := append([]byte(nil), b...)
	binary.BigEndian.PutUint16(out[off:], v)
	return out
}
func replaceU32(b []byte, off int, v uint32) []byte {
	out := append([]byte(nil), b...)
	binary.BigEndian.PutUint32(out[off:], v)
	return out
}

func malformedRowsSnapshot(rowCount uint32, runCount uint16) []byte {
	var w payloadWriter
	_ = w.putString("x")
	w.putUint64(0)
	w.putUint16(0)
	w.putUint16(1)
	_ = w.putString("")
	w.putUint16(0)
	w.putUint16(0)
	w.putUint64(0)
	_ = w.putString("p")
	_ = writeNode(&w, layout.NewLeaf("p"))
	w.putUint16(1)
	_ = w.putString("p")
	_ = w.putString("")
	_ = w.putString("")
	w.putUint16(0)
	w.putUint32(rowCount)
	if rowCount > 0 {
		w.putUint16(runCount)
	}
	return reheader(w.b, 0)
}

func styleOOBSnapshot(t *testing.T) []byte {
	b, err := Marshal(Session{Name: "x", Tabs: []Tab{{Tree: layout.NewTree("p"), Focus: "p", Panes: []Pane{{ID: "p", Visible: rows("x")}}}}})
	if err != nil {
		t.Fatal(err)
	}
	bodyLen := int(binary.BigEndian.Uint32(b[8:12]))
	body := append([]byte(nil), b[16:16+bodyLen]...)
	for i := 0; i < len(body)-5; i++ { // run len=1,rune='x', style=0,cflags=0
		if body[i] == 0 && body[i+1] == 1 && body[i+2] == 0 && body[i+3] == 0 && body[i+4] == 0 && body[i+5] == 'x' {
			body[i+6] = 0
			body[i+7] = 2
			return reheader(body, 0)
		}
	}
	t.Fatal("run not found")
	return nil
}

func unknownPaneSnapshot(t *testing.T) []byte {
	b, err := Marshal(Session{Name: "x", Tabs: []Tab{{Tree: layout.NewTree("missing"), Focus: "missing", Panes: []Pane{{ID: "p"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
