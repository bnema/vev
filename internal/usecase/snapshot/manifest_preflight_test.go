package snapshot

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestManifestGrammarRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		tree *layout.Tree
		proc *Process
	}{
		{"split without process", &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("one"), layout.NewLeaf("two")}}}, nil},
		{"stack with process argv", &layout.Tree{Root: &layout.Node{Kind: layout.Stack, Expanded: "one", Children: []*layout.Node{layout.NewLeaf("one"), layout.NewLeaf("two")}}}, &Process{Argv: []string{"sh", "-c", "echo"}, Strategy: "restore", Opts: ProcessOpts{AgentSessionID: "agent"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := MarshalManifest(testManifest(tc.tree, tc.proc))
			if err != nil {
				t.Fatal(err)
			}
			got, err := UnmarshalManifest(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if got.Tabs[0].Tree.Root.Kind != tc.tree.Root.Kind || (got.Tabs[0].Panes[0].Process == nil) != (tc.proc == nil) {
				t.Fatalf("decoded tree/process = %#v/%#v", got.Tabs[0].Tree, got.Tabs[0].Panes[0].Process)
			}
		})
	}
}

func TestManifestEnvelopePreflightParity(t *testing.T) {
	encoded, err := MarshalManifest(testManifest(layout.NewTree("one"), nil))
	if err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name string
		edit func([]byte) []byte
	}{
		{"valid", func(b []byte) []byte { return b }},
		{"magic", func(b []byte) []byte { b[0] ^= 1; return b }},
		{"version", func(b []byte) []byte { binary.BigEndian.PutUint16(b[4:6], ManifestVersion+1); return b }},
		{"flags", func(b []byte) []byte { b[7] = 1; return b }},
		{"body length", func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:12], uint32(len(b))); return b }},
		{"crc", func(b []byte) []byte { b[12] ^= 1; return b }},
		{"trailing", func(b []byte) []byte { return append(b, 0) }},
		{"body name", func(b []byte) []byte { b[26] ^= 1; binary.BigEndian.PutUint32(b[12:16], checksum(b[16:])); return b }},
		{"body generation", func(b []byte) []byte { b[16] ^= 1; binary.BigEndian.PutUint32(b[12:16], checksum(b[16:])); return b }},
		{"body node kind", func(b []byte) []byte { b[65] = 99; binary.BigEndian.PutUint32(b[12:16], checksum(b[16:])); return b }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			candidate := tc.edit(append([]byte(nil), encoded...))
			_, decodeErr := UnmarshalManifest(candidate)
			preflightErr := preflightManifestEnvelope(candidate)
			uncheckedErr := decodeManifestEnvelope(candidate)
			if (decodeErr == nil) != (preflightErr == nil) || (preflightErr == nil) != (uncheckedErr == nil) {
				t.Fatalf("preflight error = %v, unchecked decode error = %v, decode error = %v", preflightErr, uncheckedErr, decodeErr)
			}
		})
	}
}

func TestManifestObjectRefKinds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		got   ObjectKind
		want  ObjectKind
		valid bool
	}{
		{"chunk", HistoryChunk, HistoryChunk, true},
		{"tail", HistoryTail, HistoryTail, true},
		{"visible", Visible, Visible, true},
		{"invalid", ObjectKind(99), Visible, false},
		{"wrong role", HistoryTail, Visible, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var w payloadWriter
			writeObjectRef(&w, ObjectRef{Kind: tc.got, Digest: SnapshotDigest{1}, Size: minObjectEnvelopeSize})
			err := preflightObjectRef(&payloadReader{b: w.b}, tc.want)
			if (err == nil) != tc.valid {
				t.Fatalf("preflightObjectRef() error = %v, valid=%v", err, tc.valid)
			}
		})
	}
}

func TestManifestPreflightHostileDeclarations(t *testing.T) {
	t.Run("tab count", func(t *testing.T) {
		var w payloadWriter
		w.putUint64(1)
		if err := w.putString("name"); err != nil {
			t.Fatalf("putString(name): %v", err)
		}
		w.putUint64(1)
		w.putUint16(0)
		w.putUint16(^uint16(0))
		if err := preflightManifest(w.b); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("preflightManifest() error = %v", err)
		}
	})
	for _, tc := range []struct {
		name string
		fn   func(*payloadReader, *manifestPreflightBudget) error
		body []byte
	}{
		{"string length", func(r *payloadReader, b *manifestPreflightBudget) error { return skipManifestString(r, b) }, []byte{0, 1}},
		{"tree children", func(r *payloadReader, b *manifestPreflightBudget) error {
			return preflightManifestNode(r, 0, true, b, nil)
		}, []byte{manifestNodeSplit, byte(layout.Horizontal), 0xff, 0xff}},
		{"nil child", func(r *payloadReader, b *manifestPreflightBudget) error {
			return preflightManifestNode(r, 1, false, b, nil)
		}, []byte{manifestNodeNil}},
		{"process declaration", func(r *payloadReader, b *manifestPreflightBudget) error { return preflightManifestProcess(r, b) }, []byte{processPresent, 0, 1, 0, 2, 'x'}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn(&payloadReader{b: tc.body}, &manifestPreflightBudget{})
			if err == nil {
				t.Fatal("hostile declaration accepted")
			}
		})
	}
}

func TestSkipManifestStringDoesNotAllocate(t *testing.T) {
	raw := make([]byte, 2+1024)
	binary.BigEndian.PutUint16(raw, 1024)
	if allocations := testing.AllocsPerRun(100, func() {
		r := payloadReader{b: raw}
		if err := skipManifestString(&r, &manifestPreflightBudget{}); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("skipManifestString allocations = %v, want 0", allocations)
	}
}

func preflightManifestEnvelope(encoded []byte) error {
	body, err := unmarshalManifestEnvelope(encoded, manifestMagic)
	if err != nil {
		return err
	}
	return preflightManifest(body)
}

func decodeManifestEnvelope(encoded []byte) error {
	body, err := unmarshalManifestEnvelope(encoded, manifestMagic)
	if err != nil {
		return err
	}
	_, err = decodeManifest(body)
	return err
}

func checksum(body []byte) uint32 {
	return crc32.ChecksumIEEE(body)
}

func testManifest(tree *layout.Tree, process *Process) Manifest {
	return Manifest{Generation: 1, IncarnationID: domain.IncarnationID{1}, Name: "named", Tabs: []ManifestTab{{
		StableID: "tab", Cols: 80, Rows: 24, Focus: "one", Tree: tree,
		Panes: []ManifestPane{
			{ID: "one", StableID: "one", Cwd: "/one", Sealed: []ObjectRef{{Kind: HistoryChunk, Digest: SnapshotDigest{1}, Size: minObjectEnvelopeSize}}, Tail: ObjectRef{Kind: HistoryTail, Digest: SnapshotDigest{2}, Size: minObjectEnvelopeSize}, Visible: ObjectRef{Kind: Visible, Digest: SnapshotDigest{3}, Size: minObjectEnvelopeSize}, Process: process},
			{ID: "two", StableID: "two", Cwd: "/two", Tail: ObjectRef{Kind: HistoryTail, Digest: SnapshotDigest{4}, Size: minObjectEnvelopeSize}, Visible: ObjectRef{Kind: Visible, Digest: SnapshotDigest{5}, Size: minObjectEnvelopeSize}},
		},
	}}}
}
