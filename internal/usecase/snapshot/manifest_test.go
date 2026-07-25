package snapshot

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestManifestIncarnation(t *testing.T) {
	manifest := manifestWithTailSize(minObjectEnvelopeSize)
	manifest.Generation = 2
	manifest.IncarnationID = domain.IncarnationID{1}
	manifest.ParentCheckpoint = &domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{2}}

	encoded, err := MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalManifest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.IncarnationID != manifest.IncarnationID || decoded.ParentCheckpoint == nil || *decoded.ParentCheckpoint != *manifest.ParentCheckpoint {
		t.Fatalf("round trip = %#v", decoded)
	}
	reencoded, err := MarshalManifest(decoded)
	if err != nil || !bytes.Equal(encoded, reencoded) {
		t.Fatalf("manifest bytes are not canonical: %v", err)
	}
	if got := binary.BigEndian.Uint16(encoded[4:6]); got != ManifestVersion {
		t.Fatalf("manifest version = %d", got)
	}
	for n := range len(encoded) {
		if _, err := UnmarshalManifest(encoded[:n]); err == nil {
			t.Fatalf("prefix %d accepted", n)
		}
	}
	if _, err := UnmarshalManifest(append(encoded, 0)); err == nil {
		t.Fatal("trailing data accepted")
	}
	versionOne := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint16(versionOne[4:6], 1)
	if _, err := UnmarshalManifest(versionOne); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("manifest v1 error = %v", err)
	}
	manifest.IncarnationID = domain.IncarnationID{}
	if _, err := MarshalManifest(manifest); err == nil {
		t.Fatal("zero incarnation accepted")
	}
}

func TestManifestCodecUsesGenerationAndOrderedTypedReferences(t *testing.T) {
	chunk, err := MarshalObject(HistoryChunk, []byte("history"))
	if err != nil {
		t.Fatal(err)
	}
	tail, err := MarshalObject(HistoryTail, []byte("tail"))
	if err != nil {
		t.Fatal(err)
	}
	visible, err := MarshalObject(Visible, []byte("visible"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Generation: 7, IncarnationID: domain.IncarnationID{1}, Name: "named", CreatedAt: 9, Active: 0, Tabs: []ManifestTab{{
		StableID: "tab", Cols: 80, Rows: 24, NextPaneID: 2, Focus: "pane", Tree: layout.NewTree("pane"),
		Panes: []ManifestPane{{ID: "pane", StableID: "pane-stable", Cwd: "/tmp",
			Sealed:  []ObjectRef{{Kind: HistoryChunk, Digest: chunk.Digest, Size: uint32(len(chunk.Data))}},
			Tail:    ObjectRef{Kind: HistoryTail, Digest: tail.Digest, Size: uint32(len(tail.Data))},
			Visible: ObjectRef{Kind: Visible, Digest: visible.Digest, Size: uint32(len(visible.Data))},
		}},
	}}}
	encoded, err := MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalManifest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != manifest.Generation || got.Tabs[0].Panes[0].Tail != manifest.Tabs[0].Panes[0].Tail {
		t.Fatalf("round trip = %#v", got)
	}
	for n := range len(encoded) {
		if _, err := UnmarshalManifest(encoded[:n]); err == nil {
			t.Fatalf("prefix %d accepted", n)
		}
	}
	if _, err := UnmarshalManifest(append(encoded, 0)); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("trailing data error = %v", err)
	}

	head := Head{Generation: 7, ManifestDigest: sha256.Sum256(encoded)}
	if head.Generation != manifest.Generation || head.ManifestDigest != ManifestDigest(encoded) {
		t.Fatalf("head = %#v", head)
	}
}

func TestObjectCodecReturnsContentAddressedObjectAndPayload(t *testing.T) {
	object, err := MarshalObject(Visible, []byte("canonical-vt"))
	if err != nil {
		t.Fatal(err)
	}
	if object.Digest != sha256.Sum256(object.Data) {
		t.Fatalf("digest does not address VEVO bytes")
	}
	kind, payload, err := UnmarshalObject(object.Data)
	if err != nil || kind != Visible || !bytes.Equal(payload, []byte("canonical-vt")) {
		t.Fatalf("round trip = %v %q %v", kind, payload, err)
	}
	kind, payload, err = PreflightObject(object.Data)
	if err != nil || kind != Visible || !bytes.Equal(payload, []byte("canonical-vt")) {
		t.Fatalf("preflight = %v %q %v", kind, payload, err)
	}
	for n := range len(object.Data) {
		if _, _, err := UnmarshalObject(object.Data[:n]); err == nil {
			t.Fatalf("prefix %d accepted", n)
		}
	}
	if _, _, err := UnmarshalObject(append(object.Data, 0)); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("trailing data error = %v", err)
	}
}

func TestManifestDimensionBoundaries(t *testing.T) {
	ref := ObjectRef{Kind: HistoryTail, Digest: ports.SnapshotDigest{1}, Size: minObjectEnvelopeSize}
	visible := ObjectRef{Kind: Visible, Digest: ports.SnapshotDigest{2}, Size: minObjectEnvelopeSize}
	for _, tc := range []struct {
		name       string
		cols, rows uint16
		wantErr    bool
	}{
		{"max cols safe rows", math.MaxUint16, 1, false},
		{"max rows safe cols", 1, math.MaxUint16, false},
		{"hostile product", math.MaxUint16, math.MaxUint16, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := Manifest{Generation: 1, IncarnationID: domain.IncarnationID{1}, Name: "safe", Tabs: []ManifestTab{{Cols: tc.cols, Rows: tc.rows, Panes: []ManifestPane{{ID: "p", Tail: ref, Visible: visible}}}}}
			_, err := MarshalManifest(m)
			if (err != nil) != tc.wantErr {
				t.Fatalf("MarshalManifest() error = %v, want error=%v", err, tc.wantErr)
			}
		})
	}
}

func TestManifestRejectsImpossibleObjectEnvelopeSize(t *testing.T) {
	manifest := Manifest{Generation: 1, IncarnationID: domain.IncarnationID{1}, Name: "named", Tabs: []ManifestTab{{Cols: 1, Rows: 1, Panes: []ManifestPane{{
		ID:      "pane",
		Tail:    ObjectRef{Kind: HistoryTail, Digest: SnapshotDigest{1}, Size: 1},
		Visible: ObjectRef{Kind: Visible, Digest: SnapshotDigest{2}, Size: minObjectEnvelopeSize},
	}}}}}
	if _, err := MarshalManifest(manifest); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("MarshalManifest() error = %v, want invalid data", err)
	}
}

func TestObjectRefEnvelopeSizeBoundaries(t *testing.T) {
	const (
		minimum = manifestHeaderSize + 1 + 4 + 1
		maximum = manifestHeaderSize + maxDecodedBodySize
	)
	if minObjectEnvelopeSize != minimum || maxObjectEnvelopeSize != maximum {
		t.Fatalf("object envelope range = [%d, %d], want [%d, %d]", minObjectEnvelopeSize, maxObjectEnvelopeSize, minimum, maximum)
	}
	object, err := MarshalObject(HistoryTail, []byte{0})
	if err != nil {
		t.Fatalf("MarshalObject() error = %v", err)
	}
	if len(object.Data) != minimum {
		t.Fatalf("MarshalObject() size = %d, want %d", len(object.Data), minimum)
	}

	for _, tc := range []struct {
		name  string
		size  uint32
		valid bool
	}{
		{name: "one below minimum", size: minimum - 1},
		{name: "minimum", size: minimum, valid: true},
		{name: "maximum declaration", size: maximum, valid: true},
		{name: "one above maximum", size: maximum + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := manifestWithTailSize(tc.size)
			encoded, err := MarshalManifest(manifest)
			if tc.valid {
				if err != nil {
					t.Fatalf("MarshalManifest() error = %v", err)
				}
				if _, err := UnmarshalManifest(encoded); err != nil {
					t.Fatalf("UnmarshalManifest() error = %v", err)
				}
			} else if !errors.Is(err, ErrInvalidData) {
				t.Fatalf("MarshalManifest() error = %v, want invalid data", err)
			}

			var w payloadWriter
			writeObjectRef(&w, ObjectRef{Kind: HistoryTail, Digest: SnapshotDigest{1}, Size: tc.size})
			err = preflightObjectRef(&payloadReader{b: w.b}, HistoryTail)
			if tc.valid && err != nil {
				t.Fatalf("preflightObjectRef() error = %v", err)
			}
			if !tc.valid && !errors.Is(err, ErrInvalidData) {
				t.Fatalf("preflightObjectRef() error = %v, want invalid data", err)
			}
		})
	}
}

func manifestWithTailSize(size uint32) Manifest {
	return Manifest{Generation: 1, IncarnationID: domain.IncarnationID{1}, Name: "named", Tabs: []ManifestTab{{
		Cols: 1,
		Rows: 1,
		Panes: []ManifestPane{{
			ID:      "pane",
			Tail:    ObjectRef{Kind: HistoryTail, Digest: SnapshotDigest{1}, Size: size},
			Visible: ObjectRef{Kind: Visible, Digest: SnapshotDigest{2}, Size: minObjectEnvelopeSize},
		}},
	}}}
}
