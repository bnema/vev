package ports

import "testing"

type snapshotRepositoryContract struct{}

func (snapshotRepositoryContract) Publish(SnapshotPublication) error   { return nil }
func (snapshotRepositoryContract) Load() ([]SnapshotGeneration, error) { return nil, nil }
func (snapshotRepositoryContract) Delete(string) error                 { return nil }

type legacySnapshotSourceContract struct{}

func (legacySnapshotSourceContract) LoadLegacySnapshots() ([]LegacySnapshot, error) { return nil, nil }

func TestSnapshotRepositoryContracts(t *testing.T) {
	var _ SnapshotRepository = snapshotRepositoryContract{}
	var _ LegacySnapshotSource = legacySnapshotSourceContract{}

	publication := SnapshotPublication{
		Name:    "named-session",
		Head:    []byte("head-envelope"),
		Objects: []SnapshotObject{{Digest: SnapshotDigest{1}, Data: []byte("object-envelope")}},
	}
	generation := SnapshotGeneration{Publication: publication, Manifest: []byte("manifest-envelope")}
	if publication.Name != "named-session" || string(generation.Manifest) != "manifest-envelope" {
		t.Fatalf("snapshot publication/generation = %#v", generation)
	}
	legacy := LegacySnapshot{Name: "old", Data: []byte("v3")}
	if legacy.Name != "old" || string(legacy.Data) != "v3" {
		t.Fatalf("legacy snapshot = %#v", legacy)
	}
}

type snapshotStoreContract struct{}

func (snapshotStoreContract) Write(name string, data []byte) error { return nil }
func (snapshotStoreContract) Load() ([]SnapshotBlob, error)        { return nil, nil }
func (snapshotStoreContract) Delete(name string) error             { return nil }

func TestSnapshotStoreContract(t *testing.T) {
	var _ SnapshotStore = snapshotStoreContract{}

	blob := SnapshotBlob{Name: "named-session", Data: []byte("snapshot-bytes")}
	if blob.Name != "named-session" {
		t.Fatalf("SnapshotBlob.Name = %q, want %q", blob.Name, "named-session")
	}
	if string(blob.Data) != "snapshot-bytes" {
		t.Fatalf("SnapshotBlob.Data = %q, want %q", string(blob.Data), "snapshot-bytes")
	}
}
