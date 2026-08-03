package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"strconv"
	"testing"

	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

func TestPayloadReaderGetBytes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		input     []byte
		n         int
		want      []byte
		remaining []byte
		wantErr   error
	}{
		{name: "reads and advances", input: []byte{1, 2, 3}, n: 2, want: []byte{1, 2}, remaining: []byte{3}},
		{name: "reads empty", input: []byte{1}, n: 0, want: []byte{}, remaining: []byte{1}},
		{name: "short payload does not advance", input: []byte{1}, n: 2, remaining: []byte{1}, wantErr: ErrShortPayload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := payloadReader{b: append([]byte(nil), tc.input...)}
			got, err := r.getBytes(tc.n)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("getBytes(%d) error = %v, want %v", tc.n, err, tc.wantErr)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("getBytes(%d) = %v, want %v", tc.n, got, tc.want)
			}
			if !bytes.Equal(r.b, tc.remaining) {
				t.Fatalf("remaining bytes = %v, want %v", r.b, tc.remaining)
			}
		})
	}
}

func TestV4SnapshotRoundTripPreservesCanonicalTerminalData(t *testing.T) {
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
	transcript := transcriptBlob(t, [][]renderer.Cell{{{Rune: 'V', Style: rgb}, {Rune: '界', Style: indexed}, {Continuation: true, Style: indexed}}})
	sealed2, tail2 := historyBlobs(t, [][]renderer.Cell{{{Rune: 'S', Style: rgb}}, {{Rune: '2', Style: indexed}}})
	transcript2 := transcriptBlob(t, [][]renderer.Cell{{{Rune: 'P', Style: indexed}, {Rune: '2', Style: rgb}}})
	want := Session{Name: "v4 exact", Tabs: []Tab{{Focus: "p", Tree: &layout.Tree{Focus: "p", Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("p"), layout.NewLeaf("p2")}}}, Panes: []Pane{
		{ID: "p", SealedChunks: sealed, Tail: tail, Transcript: transcript},
		{ID: "p2", SealedChunks: sealed2, Tail: tail2, Transcript: transcript2},
	}}}}
	encoded, err := marshalTest(want)
	if err != nil {
		t.Fatal(err)
	}
	requireV4Envelope(t, encoded)
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
		if !bytes.Equal(gotPane.Transcript, wantPane.Transcript) {
			t.Fatalf("pane %q transcript blob changed during round trip", wantPane.ID)
		}
	}
	if _, err := vt.HistoryFromBlobs(vt.HistoryConfig{MaxRows: 8, ChunkRows: 2}, got.Tabs[0].Panes[0].SealedChunks, got.Tabs[0].Panes[0].Tail); err != nil {
		t.Fatalf("history decode: %v", err)
	}
	view, err := vt.UnmarshalHistory(got.Tabs[0].Panes[0].Transcript)
	if err != nil {
		t.Fatal(err)
	}
	if !view.Row(0)[0].Equal(renderer.Cell{Rune: 'V', Style: rgb}) || !view.Row(0)[2].Continuation {
		t.Fatalf("transcript terminal data lost: %#v", view.Row(0))
	}
}

func TestActiveTabReferenceMustBeExact(t *testing.T) {
	validTab := Tab{}
	for _, tc := range []struct {
		name  string
		s     Session
		valid bool
	}{
		{name: "zero tabs active zero", s: Session{Active: 0}, valid: true},
		{name: "zero tabs active nonzero", s: Session{Active: 1}},
		{name: "nonempty tabs active in range", s: Session{Active: 0, Tabs: []Tab{validTab}}, valid: true},
		{name: "nonempty tabs active past end", s: Session{Active: 1, Tabs: []Tab{validTab}}},
	} {
		t.Run("marshal "+tc.name, func(t *testing.T) {
			_, err := marshalTest(tc.s)
			if tc.valid {
				requireNoError(t, err)
				return
			}
			if !errors.Is(err, ErrInvalidData) {
				t.Fatalf("marshalTest() error = %v, want %v", err, ErrInvalidData)
			}
		})
	}

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "zero tabs active nonzero", data: v4ActiveManifest(1, 0)},
		{name: "nonempty tabs active past end", data: v4ActiveManifest(1, 1)},
	} {
		t.Run("unmarshal "+tc.name, func(t *testing.T) {
			reject(t, v4Envelope(version, 0, tc.data), ErrInvalidData)
		})
	}
}

func TestUnmarshalRejectsV4LegacyVersions(t *testing.T) {
	for _, version := range []uint16{1, 2, 3} {
		if _, err := Unmarshal(v4Envelope(version, 0, nil)); !errors.Is(err, ErrBadVersion) {
			t.Fatalf("version %d: %v", version, err)
		}
	}
}

func TestUnmarshalRejectsMalformedV4EnvelopeBeforeAllocation(t *testing.T) {
	body := v4Manifest(0, nil, nil, 0, nil, 0, nil)
	valid := v4Envelope(version, 0, body)
	badCRC := append([]byte(nil), valid...)
	badCRC[15] ^= 1
	for _, tc := range []struct {
		name string
		data []byte
		want error
	}{
		{"short", valid[:15], ErrShortPayload}, {"magic", append([]byte("NOPE"), valid[4:]...), ErrBadMagic},
		{"future", v4Envelope(version+1, 0, body), ErrBadVersion}, {"flags", v4Envelope(version, 1, body), ErrInvalidData},
		{"crc", badCRC, ErrBadCRC}, {"body", v4EnvelopeWithLength(version, 0, nil, math.MaxUint32), ErrInvalidData},
	} {
		t.Run(tc.name, func(t *testing.T) { reject(t, tc.data, tc.want) })
	}
}

func TestUnmarshalRejectsHostileV4ManifestDeclarationsBeforeAllocation(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"count", v4Manifest(math.MaxUint32, nil, nil, 0, nil, 0, nil)},
		{"sealed", v4Manifest(1, []uint32{math.MaxUint32}, nil, 0, nil, 0, nil)},
		{"tail", v4Manifest(0, nil, nil, math.MaxUint32, nil, 0, nil)},
		{"transcript", v4Manifest(0, nil, nil, 0, nil, math.MaxUint32, nil)},
	} {
		t.Run(tc.name, func(t *testing.T) { reject(t, v4Envelope(version, 0, tc.body), ErrInvalidData) })
	}
}

func TestUnmarshalRejectsEveryV4PrefixAndTrailingGarbage(t *testing.T) {
	sealed, tail := historyBlobs(t, [][]renderer.Cell{{{Rune: 'h'}}})
	transcript := transcriptBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	encoded, err := marshalTest(Session{Name: "p", Tabs: []Tab{{Focus: "p", Tree: layout.NewTree("p"), Panes: []Pane{{ID: "p", SealedChunks: sealed, Tail: tail, Transcript: transcript}}}}})
	if err != nil {
		t.Fatal(err)
	}
	for n := range len(encoded) {
		reject(t, encoded[:n], nil)
	}
	reject(t, append(encoded, 0), ErrTrailingBytes)
}

func TestUnmarshalRejectsEmptyAndDuplicatePaneIDsDuringPreflight(t *testing.T) {
	sealed, tail := historyBlobs(t, nil)
	transcript := transcriptBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	for _, tc := range []struct {
		name  string
		panes []Pane
	}{
		{name: "empty", panes: []Pane{{ID: "", Tail: tail, Transcript: transcript}}},
		{name: "duplicate", panes: []Pane{{ID: "p", SealedChunks: sealed, Tail: tail, Transcript: transcript}, {ID: "p", Tail: tail, Transcript: transcript}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := marshalTest(Session{Name: "s", Tabs: []Tab{{Panes: tc.panes}}})
			requireNoError(t, err)
			reject(t, data, ErrInvalidData)
		})
	}
}

func TestUnmarshalRejectsInvalidTreeReference(t *testing.T) {
	sealed, tail := historyBlobs(t, nil)
	transcript := transcriptBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	encoded, err := marshalTest(Session{Name: "p", Tabs: []Tab{{Focus: "missing", Tree: layout.NewTree("missing"), Panes: []Pane{{ID: "p", SealedChunks: sealed, Tail: tail, Transcript: transcript}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Unmarshal(encoded); !errors.Is(err, ErrUnknownPane) {
		t.Fatalf("error = %v", err)
	}
}

func TestUnmarshalEnforcesCanonicalHistoryBlobRoles(t *testing.T) {
	sealed, emptyTail := historyBlobs(t, [][]renderer.Cell{{{Rune: 'a'}}, {{Rune: 'b'}}, {{Rune: 'c'}}})
	if len(sealed) < 2 {
		t.Fatalf("expected at least two sealed chunks, got %d", len(sealed))
	}
	multiChunk := append([]byte(nil), sealed[0][:17]...)
	binary.BigEndian.PutUint32(multiChunk[5:9], 2)
	binary.BigEndian.PutUint64(multiChunk[9:17], 4)
	multiChunk = append(multiChunk, sealed[0][17:]...)
	multiChunk = append(multiChunk, sealed[1][17:]...)
	transcript := transcriptBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	emptyTranscript := transcriptBlob(t, nil)

	// Incremental captures retain one copied mutable tail chunk instead of
	// rotating it into the sealed history set. That one chunk is canonical and
	// must remain a valid v4 import payload.
	mutableHistory := vt.NewHistory(vt.HistoryConfig{MaxRows: 8, ChunkRows: 2})
	for _, row := range [][]renderer.Cell{{{Rune: 'a'}}, {{Rune: 'b'}}, {{Rune: 'c'}}} {
		requireNoError(t, mutableHistory.Append(row, vt.LineBound{End: len(row)}))
	}
	mutableTail, err := vt.MarshalHistoryTail(mutableHistory.SnapshotView())
	requireNoError(t, err)
	fullHistory, err := vt.MarshalHistory(mutableHistory.SealAndView())
	requireNoError(t, err)
	for _, tc := range []struct {
		name       string
		transcript []byte
	}{
		{name: "single chunk", transcript: transcript},
		{name: "empty", transcript: emptyTranscript},
		{name: "multiple chunks", transcript: multiChunk},
	} {
		t.Run("valid "+tc.name, func(t *testing.T) {
			data, err := marshalTest(Session{Name: "s", Tabs: []Tab{{Cols: 1, Rows: 1, Panes: []Pane{{ID: "p", SealedChunks: sealed[:1], Tail: mutableTail, Transcript: tc.transcript}}}}})
			requireNoError(t, err)
			roundTrip, err := Unmarshal(data)
			requireNoError(t, err)
			if got := roundTrip.Tabs[0].Panes[0].Tail; !bytes.Equal(got, mutableTail) {
				t.Fatal("mutable tail did not round trip")
			}
		})
	}

	for _, tc := range []struct {
		name string
		tab  Tab
	}{
		{name: "sealed multi-chunk", tab: Tab{Cols: 1, Rows: 1, Panes: []Pane{{ID: "p", SealedChunks: [][]byte{multiChunk}, Tail: emptyTail, Transcript: transcript}}}},
		{name: "tail has multiple chunks", tab: Tab{Cols: 1, Rows: 1, Panes: []Pane{{ID: "p", Tail: multiChunk, Transcript: transcript}}}},
		{name: "noncanonical full history tail", tab: Tab{Cols: 1, Rows: 1, Panes: []Pane{{ID: "p", Tail: fullHistory, Transcript: transcript}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := marshalTest(Session{Name: "s", Tabs: []Tab{tc.tab}})
			requireNoError(t, err)
			reject(t, data, ErrInvalidData)
		})
	}
}

func TestUnmarshalRejectsMalformedCanonicalVTBlobsWithoutPanic(t *testing.T) {
	tail := mustHistoryTail(t)
	transcript := transcriptBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	for _, tc := range []struct {
		name string
		blob []byte
	}{
		{name: "malformed", blob: append([]byte("NOPE"), transcript[4:]...)},
		{name: "truncated", blob: transcript[:len(transcript)-1]},
		{name: "trailing", blob: append(append([]byte(nil), transcript...), 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := v4Manifest(0, nil, nil, uint32(len(tail)), tail, uint32(len(tc.blob)), tc.blob)
			reject(t, v4Envelope(version, 0, body), ErrInvalidData)
		})
	}
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
			data, err := marshalTest(Session{Name: "s", Tabs: []Tab{tc.tab}})
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
	transcript := transcriptBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
	want := Session{Name: "named", CreatedAt: 42, Active: 1, Tabs: []Tab{
		{
			StableID: "t1", Cols: 100, Rows: 40, NextPaneID: 9, Focus: "2",
			Tree: &layout.Tree{Focus: "2", Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
				layout.NewLeaf("1"), layout.NewLeaf("2"),
			}}},
			Panes: []Pane{
				{ID: "1", StableID: "p1", Cwd: "/one", SealedChunks: sealed, Tail: tail, Transcript: transcript, Process: &Process{Argv: []string{"pi", "--resume"}, Strategy: "pi", Opts: ProcessOpts{AgentSessionID: "agent-123"}}},
				{ID: "2", StableID: "p2", Cwd: "/two", SealedChunks: sealed, Tail: tail, Transcript: transcript},
			},
		},
		{StableID: "t2", Cols: 80, Rows: 24, NextPaneID: 2, Focus: "a", Tree: layout.NewTree("a"), Panes: []Pane{{ID: "a", Cwd: "/tmp", SealedChunks: sealed, Tail: tail, Transcript: transcript}}},
	}}
	data, err := marshalTest(want)
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
	_, err := marshalTest(Session{Name: "s", Tabs: []Tab{
		{Tree: layout.NewTree("p"), Focus: "p", Panes: []Pane{{ID: "p", Process: &Process{Strategy: "generic"}}}},
	}})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("marshalTest() error = %v, want %v", err, ErrInvalidData)
	}
}

func TestUnmarshalRejectsProcessWithoutArgv(t *testing.T) {
	tail := mustHistoryTail(t)
	transcript := transcriptBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})
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
	w.putUint32(uint32(len(transcript)))
	w.b = append(w.b, transcript...)
	w.putUint8(1)
	w.putUint16(0) // argv count
	_ = w.putString("generic")
	_ = w.putString("")
	if _, err := Unmarshal(v4Envelope(version, 0, w.b)); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("Unmarshal() error = %v, want %v", err, ErrInvalidData)
	}
}

func TestUnmarshalGlobalBudgetRejectionDoesNotAllocatePerPane(t *testing.T) {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 256, ChunkRows: 256})
	for range 256 {
		requireNoError(t, history.Append([]renderer.Cell{{Rune: 'x'}}, vt.LineBound{End: 1}))
	}
	sealed, tail, err := vt.MarshalSealedHistory(history.SealAndView())
	requireNoError(t, err)
	transcript := transcriptBlob(t, [][]renderer.Cell{{{Rune: 'v'}}})

	const paneCount = maxSnapshotRows/256 + 1
	tabs := make([]Tab, paneCount)
	for i := range tabs {
		tabs[i] = Tab{Panes: []Pane{{ID: layout.PaneID(strconv.Itoa(i)), SealedChunks: sealed, Tail: tail, Transcript: transcript}}}
	}
	data, err := marshalTest(Session{Name: "over-budget", Tabs: tabs})
	requireNoError(t, err)

	allocs := testing.AllocsPerRun(3, func() {
		_, err := Unmarshal(data)
		if !errors.Is(err, ErrInvalidData) {
			panic(err)
		}
	})
	// The error and testing harness may allocate a small fixed amount. Rejecting
	// before semantic maps/slices are built must not scale with paneCount.
	if allocs > 8 {
		t.Fatalf("Unmarshal() allocations = %f, want fixed overhead only", allocs)
	}
}

func TestPreflightBudgetRejectsAggregateDecodedAllocation(t *testing.T) {
	budget := preflightBudget{alloc: maxSnapshotDecodedAllocation - 1}
	if budget.addProduct(1, 2) {
		t.Fatal("allocation budget accepted an over-limit aggregate")
	}
}

func TestPreflightRejectsBroadTreeBeforeSecondPassAllocation(t *testing.T) {
	body := v4WideTreeManifest(32_769)
	if err := preflightSession(body); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("preflightSession() error = %v, want %v", err, ErrInvalidData)
	}
}

func TestUnmarshalRejectsAggregateOpaqueBlobDeclarations(t *testing.T) {
	body := v4Manifest(math.MaxUint32, nil, nil, math.MaxUint32, nil, math.MaxUint32, nil)
	reject(t, v4Envelope(version, 0, body), ErrInvalidData)
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
		requireNoError(t, h.Append(row, vt.LineBound{End: len(row)}))
	}
	sealed, tail, err := vt.MarshalSealedHistory(h.SealAndView())
	if err != nil {
		t.Fatal(err)
	}
	return sealed, tail
}
func transcriptBlob(t *testing.T, rows [][]renderer.Cell) []byte {
	t.Helper()
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: max(1, len(rows)), ChunkRows: 2})
	for _, row := range rows {
		requireNoError(t, history.Append(row, vt.LineBound{End: len(row)}))
	}
	blob, err := vt.MarshalHistory(history.SealAndView())
	if err != nil {
		t.Fatal(err)
	}
	return blob
}
func requireV4Envelope(t *testing.T, data []byte) {
	t.Helper()
	if version != 4 || len(data) < 16 || binary.BigEndian.Uint16(data[4:6]) != 4 || binary.BigEndian.Uint16(data[6:8]) != 0 {
		t.Fatalf("not v4 envelope: % x", data)
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
func v4Envelope(v, flags uint16, body []byte) []byte {
	return v4EnvelopeWithLength(v, flags, body, uint32(len(body)))
}
func v4EnvelopeWithLength(v, flags uint16, body []byte, length uint32) []byte {
	out := make([]byte, 16+len(body))
	copy(out, magic)
	binary.BigEndian.PutUint16(out[4:6], v)
	binary.BigEndian.PutUint16(out[6:8], flags)
	binary.BigEndian.PutUint32(out[8:12], length)
	binary.BigEndian.PutUint32(out[12:16], crc32.ChecksumIEEE(body))
	copy(out[16:], body)
	return out
}

func v4ActiveManifest(active, tabs uint16) []byte {
	var b []byte
	putS := func(s string) { b = binary.BigEndian.AppendUint16(b, uint16(len(s))); b = append(b, s...) }
	putS("s")
	b = append(b, make([]byte, 8)...)
	b = binary.BigEndian.AppendUint16(b, active)
	b = binary.BigEndian.AppendUint16(b, tabs)
	for range tabs {
		putS("")
		b = append(b, make([]byte, 2+2+8)...)
		putS("")
		b = append(b, 3) // nil root
		b = binary.BigEndian.AppendUint16(b, 0)
	}
	return b
}

// v4Manifest creates just enough session/tab/pane framing for hostile declarations.
// v4WideTreeManifest declares a broad but compact tree without allocating a
// matching layout.Node graph. It exercises preflight's manifest budget.
func v4WideTreeManifest(children uint16) []byte {
	var b []byte
	putS := func(s string) { b = binary.BigEndian.AppendUint16(b, uint16(len(s))); b = append(b, s...) }
	putS("s")
	b = append(b, make([]byte, 8+2)...)
	b = binary.BigEndian.AppendUint16(b, 1)
	putS("")
	b = append(b, make([]byte, 2+2+8)...)
	putS("")
	b = append(b, 1, byte(layout.Horizontal)) // split node
	b = binary.BigEndian.AppendUint16(b, children)
	for range children {
		b = append(b, 0) // leaf
		putS("")
	}
	b = binary.BigEndian.AppendUint16(b, 0) // panes
	return b
}

func v4Manifest(sealed uint32, sealedLens []uint32, sealedData []byte, tailLen uint32, tailData []byte, transcriptLen uint32, transcriptData []byte) []byte {
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
	b = binary.BigEndian.AppendUint32(b, transcriptLen)
	b = append(b, transcriptData...)
	return b
}
