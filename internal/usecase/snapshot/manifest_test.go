package snapshot

import (
	"bytes"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/usecase/layout"
)

func TestManifestAndHeadCodecsAreDeterministicAndStrict(t *testing.T) {
	digest := SnapshotDigest{1}
	manifest := Manifest{
		Name:      "named",
		CreatedAt: 9,
		Active:    0,
		Tabs: []Tab{{
			StableID: "tab", Cols: 80, Rows: 24, NextPaneID: 2, Focus: "pane",
			Tree: layout.NewTree("pane"),
			Panes: []Pane{{ID: "pane", StableID: "pane-stable", Cwd: "/tmp", Objects: []ObjectRef{
				{Role: ObjectRoleHistory, Digest: digest},
				{Role: ObjectRoleTail, Digest: SnapshotDigest{2}},
				{Role: ObjectRoleVisible, Digest: SnapshotDigest{3}},
			}}},
		}},
	}
	encoded, err := MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	encodedAgain, err := MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, encodedAgain) {
		t.Fatal("manifest encoding is not deterministic")
	}
	got, err := UnmarshalManifest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != manifest.Name || len(got.Tabs) != 1 || len(got.Tabs[0].Panes[0].Objects) != 3 {
		t.Fatalf("manifest round trip = %#v", got)
	}
	for n := range len(encoded) {
		if _, err := UnmarshalManifest(encoded[:n]); err == nil {
			t.Fatalf("prefix %d accepted", n)
		}
	}
	if _, err := UnmarshalManifest(append(encoded, 0)); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("trailing data error = %v", err)
	}

	head := Head{Name: "named", Generation: 7, Manifest: ManifestDigest(encoded)}
	headBytes, err := MarshalHead(head)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := UnmarshalHead(headBytes); err != nil || got != head {
		t.Fatalf("head round trip = %#v, %v", got, err)
	}
	for n := range len(headBytes) {
		if _, err := UnmarshalHead(headBytes[:n]); err == nil {
			t.Fatalf("head prefix %d accepted", n)
		}
	}
	if _, err := UnmarshalHead(append(headBytes, 0)); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("head trailing data error = %v", err)
	}
}

func TestObjectCodecIsTypedAndStrict(t *testing.T) {
	object := Object{Role: ObjectRoleVisible, Data: []byte("canonical-vt")}
	encoded, err := MarshalObject(object)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalObject(encoded)
	if err != nil || got.Role != object.Role || !bytes.Equal(got.Data, object.Data) {
		t.Fatalf("object round trip = %#v, %v", got, err)
	}
	if err := PreflightObject(encoded); err != nil {
		t.Fatal(err)
	}
	for n := range len(encoded) {
		if _, err := UnmarshalObject(encoded[:n]); err == nil {
			t.Fatalf("object prefix %d accepted", n)
		}
	}
	if _, err := UnmarshalObject(append(encoded, 0)); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("object trailing data error = %v", err)
	}
}

func TestManifestPreflightRejectsReferencesBeforeDecode(t *testing.T) {
	valid := Manifest{Name: "valid", Tabs: []Tab{{Cols: 1, Rows: 1, Panes: []Pane{{ID: "p", Objects: []ObjectRef{{Role: ObjectRoleTail, Digest: SnapshotDigest{1}}, {Role: ObjectRoleVisible, Digest: SnapshotDigest{2}}}}}}}}
	for _, tc := range []struct {
		name   string
		mutate func(*Manifest)
	}{
		{"zero digest", func(m *Manifest) { m.Tabs[0].Panes[0].Objects[0].Digest = SnapshotDigest{} }},
		{"unordered role", func(m *Manifest) { m.Tabs[0].Panes[0].Objects[0].Role = ObjectRoleVisible }},
		{"duplicate pane", func(m *Manifest) { m.Tabs[0].Panes = append(m.Tabs[0].Panes, m.Tabs[0].Panes[0]) }},
		{"dangling layout", func(m *Manifest) { m.Tabs[0].Tree = layout.NewTree("missing") }},
		{"zero dimensions", func(m *Manifest) { m.Tabs[0].Cols = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := valid
			m.Tabs = append([]Tab(nil), valid.Tabs...)
			m.Tabs[0].Panes = append([]Pane(nil), valid.Tabs[0].Panes...)
			m.Tabs[0].Panes[0].Objects = append([]ObjectRef(nil), valid.Tabs[0].Panes[0].Objects...)
			tc.mutate(&m)
			if _, err := MarshalManifest(m); !errors.Is(err, ErrInvalidData) && !errors.Is(err, ErrUnknownPane) {
				t.Fatalf("MarshalManifest() error = %v", err)
			}
		})
	}
}
