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
	"github.com/bnema/vev/pkg/vt"
)

func TestV3SnapshotRoundTripPreservesExactTerminalData(t *testing.T) {
	indexed := renderer.DefaultStyle()
	indexed.Bold, indexed.Inverse = true, true
	indexed.Attrs = renderer.AttrDim | renderer.AttrUnderline | renderer.AttrBlink | renderer.AttrStrikethrough
	indexed.Foreground, indexed.Background = 196, 17
	indexed.UnderlineStyle, indexed.HasUnderlineColor, indexed.UnderlineColor = renderer.UnderlineCurly, true, 203
	rgb := renderer.DefaultStyle()
	rgb.Italic, rgb.HasForegroundRGB, rgb.ForegroundRGB = true, true, renderer.RGB{R: 1, G: 2, B: 3}
	rgb.HasBackgroundRGB, rgb.BackgroundRGB = true, renderer.RGB{R: 4, G: 5, B: 6}
	rgb.UnderlineStyle, rgb.HasUnderlineColorRGB, rgb.UnderlineColorRGB = renderer.UnderlineDashed, true, renderer.RGB{R: 7, G: 8, B: 9}
	sealed, tail := historyBlobs(t, [][]renderer.Cell{{{Rune: 'I', Style: indexed}, {Rune: '好', Style: rgb}, {Continuation: true, Style: rgb}}})
	visible := visibleBlob(t, [][]renderer.Cell{{{Rune: 'V', Style: rgb}, {Rune: '界', Style: indexed}, {Continuation: true, Style: indexed}}})
	sealed2, tail2 := historyBlobs(t, [][]renderer.Cell{{{Rune: 'S', Style: rgb}}, {{Rune: '2', Style: indexed}}})
	visible2 := visibleBlob(t, [][]renderer.Cell{{{Rune: 'P', Style: indexed}, {Rune: '2', Style: rgb}}})
	want := Session{Name: "v3 exact", Tabs: []Tab{{Focus: "p", Tree: &layout.Tree{Focus: "p", Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("p"), layout.NewLeaf("p2")}}}, Panes: []Pane{
		{ID: "p", SealedChunks: sealed, Tail: tail, Visible: visible},
		{ID: "p2", SealedChunks: sealed2, Tail: tail2, Visible: visible2},
	}}}}
	encoded, err := Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	requireV3Envelope(t, encoded)
	got, err := Unmarshal(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Tabs) != 1 || len(got.Tabs[0].Panes) != 2 || got.Tabs[0].Panes[0].ID != "p" || got.Tabs[0].Panes[1].ID != "p2" {
		t.Fatalf("manifest round trip = %#v", got)
	}
	for i, wantPane := range want.Tabs[0].Panes {
		gotPane := got.Tabs[0].Panes[i]
		if len(gotPane.SealedChunks) != len(wantPane.SealedChunks) {
			t.Fatalf("pane %q sealed chunks = %d, want %d", wantPane.ID, len(gotPane.SealedChunks), len(wantPane.SealedChunks))
		}
		for j := range wantPane.SealedChunks {
			if !bytes.Equal(gotPane.SealedChunks[j], wantPane.SealedChunks[j]) {
				t.Fatalf("pane %q sealed chunk %d changed during round trip", wantPane.ID, j)
			}
		}
		if !bytes.Equal(gotPane.Tail, wantPane.Tail) {
			t.Fatalf("pane %q tail changed during round trip", wantPane.ID)
		}
		if !bytes.Equal(gotPane.Visible, wantPane.Visible) {
			t.Fatalf("pane %q visible blob changed during round trip", wantPane.ID)
		}
	}
	if _, err := vt.HistoryFromBlobs(vt.HistoryConfig{MaxRows: 8, ChunkRows: 2}, got.Tabs[0].Panes[0].SealedChunks, got.Tabs[0].Panes[0].Tail); err != nil {
		t.Fatalf("history decode: %v", err)
	}
	frame, err := vt.UnmarshalVisible(got.Tabs[0].Panes[0].Visible)
	if err != nil {
		t.Fatal(err)
	}
	if !frame.Row(0)[0].Equal(renderer.Cell{Rune: 'V', Style: rgb}) || !frame.Row(0)[2].Continuation {
		t.Fatalf("visible terminal data lost: %#v", frame.Row(0))
	}
}

func TestUnmarshalRejectsV3LegacyVersions(t *testing.T) {
	for _, version := range []uint16{1, 2} {
		if _, err := Unmarshal(v3Envelope(version, 0, nil)); !errors.Is(err, ErrBadVersion) {
			t.Fatalf("version %d: %v", version, err)
		}
	}
}

func TestUnmarshalRejectsMalformedV3EnvelopeBeforeAllocation(t *testing.T) {
	body := v3Manifest(0, nil, nil, 0, nil, 0, nil)
	valid := v3Envelope(version, 0, body)
	badCRC := append([]byte(nil), valid...)
	badCRC[15] ^= 1
	for _, tc := range []struct {
		name string
		data []byte
		want error
	}{
		{"short", valid[:15], ErrShortPayload}, {"magic", append([]byte("NOPE"), valid[4:]...), ErrBadMagic},
		{"future", v3Envelope(version+1, 0, body), ErrBadVersion}, {"flags", v3Envelope(version, 1, body), ErrInvalidData},
		{"crc", badCRC, ErrBadCRC}, {"body", v3EnvelopeWithLength(version, 0, nil, math.MaxUint32), ErrInvalidData},
	} {
		t.Run(tc.name, func(t *testing.T) { reject(t, tc.data, tc.want) })
	}
}

func TestUnmarshalRejectsHostileV3ManifestDeclarationsBeforeAllocation(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"count", v3Manifest(math.MaxUint32, nil, nil, 0, nil, 0, nil)},
		{"sealed", v3Manifest(1, []uint32{math.MaxUint32}, nil, 0, nil, 0, nil)},
		{"tail", v3Manifest(0, nil, nil, math.MaxUint32, nil, 0, nil)},
		{"visible", v3Manifest(0, nil, nil, 0, nil, math.MaxUint32, nil)},
	} {
		t.Run(tc.name, func(t *testing.T) { reject(t, v3Envelope(version, 0, tc.body), ErrInvalidData) })
	}
}

func TestUnmarshalRejectsEveryV3PrefixAndTrailingGarbage(t *testing.T) {
	sealed, tail := historyBlobs(t, [][]renderer.Cell{{{Rune: 'h'}}})
	visible := visibleBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	encoded, err := Marshal(Session{Name: "p", Tabs: []Tab{{Focus: "p", Tree: layout.NewTree("p"), Panes: []Pane{{ID: "p", SealedChunks: sealed, Tail: tail, Visible: visible}}}}})
	if err != nil {
		t.Fatal(err)
	}
	for n := range len(encoded) {
		reject(t, encoded[:n], nil)
	}
	reject(t, append(encoded, 0), ErrTrailingBytes)
}

func TestUnmarshalRejectsInvalidTreeReference(t *testing.T) {
	sealed, tail := historyBlobs(t, nil)
	visible := visibleBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	encoded, err := Marshal(Session{Name: "p", Tabs: []Tab{{Focus: "missing", Tree: layout.NewTree("missing"), Panes: []Pane{{ID: "p", SealedChunks: sealed, Tail: tail, Visible: visible}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Unmarshal(encoded); !errors.Is(err, ErrUnknownPane) {
		t.Fatalf("error = %v", err)
	}
}

func TestUnmarshalRejectsMalformedTerminalBlobWithoutPanic(t *testing.T) {
	tail := mustHistoryTail(t)
	visible := visibleBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	// The visible cell's underline-style byte is outside the renderer enum.
	visible[13+29] = 0xff
	body := v3Manifest(0, nil, nil, uint32(len(tail)), tail, uint32(len(visible)), visible)
	reject(t, v3Envelope(version, 0, body), ErrInvalidData)
}

func TestRoundTripNilTreeAndRootDoNotMaterializeLeaf(t *testing.T) {
	for _, tc := range []struct {
		name string
		tab  Tab
	}{
		{name: "nil tree", tab: Tab{Tree: nil}},
		{name: "nil root", tab: Tab{Tree: &layout.Tree{Root: nil}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := Marshal(Session{Name: "s", Tabs: []Tab{tc.tab}})
			requireNoError(t, err)
			got, err := Unmarshal(data)
			requireNoError(t, err)
			if got.Tabs[0].Tree == nil || got.Tabs[0].Tree.Root != nil {
				t.Fatalf("tree = %#v, want nonnil tree with nil root", got.Tabs[0].Tree)
			}
		})
	}
}

func TestRoundTripSessionMetadataAndProcess(t *testing.T) {
	sealed, tail := historyBlobs(t, [][]renderer.Cell{{{Rune: 'h'}}, {{Rune: 'i'}}})
	visible := visibleBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	want := Session{Name: "named", CreatedAt: 42, Active: 1, Tabs: []Tab{
		{
			StableID: "t1", Cols: 100, Rows: 40, NextPaneID: 9, Focus: "2",
			Tree: &layout.Tree{Focus: "2", Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
				layout.NewLeaf("1"), layout.NewLeaf("2"),
			}}},
			Panes: []Pane{
				{ID: "1", StableID: "p1", Cwd: "/one", SealedChunks: sealed, Tail: tail, Visible: visible, Process: &Process{Argv: []string{"pi", "--resume"}, Strategy: "pi", Opts: ProcessOpts{AgentSessionID: "agent-123"}}},
				{ID: "2", StableID: "p2", Cwd: "/two", SealedChunks: sealed, Tail: tail, Visible: visible},
			},
		},
		{StableID: "t2", Cols: 80, Rows: 24, NextPaneID: 2, Focus: "a", Tree: layout.NewTree("a"), Panes: []Pane{{ID: "a", Cwd: "/tmp", SealedChunks: sealed, Tail: tail, Visible: visible}}},
	}}
	data, err := Marshal(want)
	requireNoError(t, err)
	got, err := Unmarshal(data)
	requireNoError(t, err)
	if got.Name != want.Name || got.CreatedAt != want.CreatedAt || got.Active != want.Active || len(got.Tabs) != 2 {
		t.Fatalf("session metadata round trip = %#v", got)
	}
	pane := got.Tabs[0].Panes[0]
	if pane.ID != "1" || pane.StableID != "p1" || pane.Cwd != "/one" || pane.Process == nil || pane.Process.Opts.AgentSessionID != "agent-123" || got.Tabs[0].Tree.Root.Kind != layout.Split {
		t.Fatalf("pane metadata round trip = %#v", pane)
	}
}

func TestMarshalRejectsProcessWithoutArgv(t *testing.T) {
	_, err := Marshal(Session{Name: "s", Tabs: []Tab{
		{Tree: layout.NewTree("p"), Focus: "p", Panes: []Pane{{ID: "p", Process: &Process{Strategy: "generic"}}}},
	}})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("Marshal() error = %v, want %v", err, ErrInvalidData)
	}
}

func TestUnmarshalRejectsProcessWithoutArgv(t *testing.T) {
	tail := mustHistoryTail(t)
	visible := visibleBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	var w payloadWriter
	_ = w.putString("s")
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
	_ = w.putString("")
	_ = w.putString("")
	w.putUint32(0)
	w.putUint32(uint32(len(tail)))
	w.b = append(w.b, tail...)
	w.putUint32(uint32(len(visible)))
	w.b = append(w.b, visible...)
	w.putUint8(1)
	w.putUint16(0) // argv count
	_ = w.putString("generic")
	_ = w.putString("")
	if _, err := Unmarshal(v3Envelope(version, 0, w.b)); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("Unmarshal() error = %v, want %v", err, ErrInvalidData)
	}
}

func TestUnmarshalRejectsAggregateOpaqueBlobDeclarations(t *testing.T) {
	body := v3Manifest(math.MaxUint32, nil, nil, math.MaxUint32, nil, math.MaxUint32, nil)
	reject(t, v3Envelope(version, 0, body), ErrInvalidData)
}

func mustHistoryTail(t *testing.T) []byte {
	t.Helper()
	_, tail := historyBlobs(t, nil)
	return tail
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func historyBlobs(t *testing.T, rows [][]renderer.Cell) ([][]byte, []byte) {
	t.Helper()
	h := vt.NewHistory(vt.HistoryConfig{MaxRows: 128, ChunkRows: 2})
	for _, row := range rows {
		h.Append(row)
	}
	sealed, tail, err := vt.MarshalSealedHistory(h.SealAndView())
	if err != nil {
		t.Fatal(err)
	}
	return sealed, tail
}
func visibleBlob(t *testing.T, rows [][]renderer.Cell) []byte {
	t.Helper()
	width := 0
	for _, row := range rows {
		width = max(width, len(row))
	}
	f := renderer.NewFrame(width, len(rows))
	for y, row := range rows {
		copy(f.Row(y), row)
	}
	b, err := vt.MarshalVisible(f)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func requireV3Envelope(t *testing.T, data []byte) {
	t.Helper()
	if len(data) < 16 || binary.BigEndian.Uint16(data[4:6]) != version || binary.BigEndian.Uint16(data[6:8]) != 0 {
		t.Fatalf("not v3 envelope: % x", data)
	}
}
func reject(t *testing.T, data []byte, want error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic: %v", r)
		}
	}()
	_, err := Unmarshal(data)
	if err == nil {
		t.Fatal("accepted malformed input")
	}
	if want != nil && !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}
func v3Envelope(v, flags uint16, body []byte) []byte {
	return v3EnvelopeWithLength(v, flags, body, uint32(len(body)))
}
func v3EnvelopeWithLength(v, flags uint16, body []byte, length uint32) []byte {
	out := make([]byte, 16+len(body))
	copy(out, magic)
	binary.BigEndian.PutUint16(out[4:6], v)
	binary.BigEndian.PutUint16(out[6:8], flags)
	binary.BigEndian.PutUint32(out[8:12], length)
	binary.BigEndian.PutUint32(out[12:16], crc32.ChecksumIEEE(body))
	copy(out[16:], body)
	return out
}

// v3Manifest creates just enough session/tab/pane framing for hostile declarations.
func v3Manifest(sealed uint32, sealedLens []uint32, sealedData []byte, tailLen uint32, tailData []byte, visibleLen uint32, visibleData []byte) []byte {
	var b []byte
	putS := func(s string) { b = binary.BigEndian.AppendUint16(b, uint16(len(s))); b = append(b, s...) }
	putS("s")
	b = append(b, make([]byte, 8+2)...)
	b = binary.BigEndian.AppendUint16(b, 1)
	putS("")
	b = append(b, make([]byte, 2+2+8)...)
	putS("p")
	b = append(b, 0)
	putS("p")
	b = binary.BigEndian.AppendUint16(b, 1)
	putS("p")
	putS("")
	putS("")
	b = binary.BigEndian.AppendUint32(b, sealed)
	for _, l := range sealedLens {
		b = binary.BigEndian.AppendUint32(b, l)
	}
	b = append(b, sealedData...)
	b = binary.BigEndian.AppendUint32(b, tailLen)
	b = append(b, tailData...)
	b = binary.BigEndian.AppendUint32(b, visibleLen)
	b = append(b, visibleData...)
	return b
}
