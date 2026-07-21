package ports

import (
	"context"
	"crypto/sha256"
	"testing"
)

type snapshotRepositoryContract struct{}

func (snapshotRepositoryContract) Publish(context.Context, SnapshotPublication) error { return nil }
func (snapshotRepositoryContract) List(context.Context) ([]string, error)             { return nil, nil }
func (snapshotRepositoryContract) Load(context.Context, string) (SnapshotGeneration, error) {
	return SnapshotGeneration{}, nil
}
func (snapshotRepositoryContract) Delete(context.Context, string) error { return nil }
func (snapshotRepositoryContract) Maintain(context.Context) error       { return nil }

type legacySnapshotSourceContract struct{}

func (legacySnapshotSourceContract) LoadLegacy(context.Context) ([]LegacySnapshot, error) {
	return nil, nil
}
func (legacySnapshotSourceContract) DeleteLegacy(context.Context, string) error { return nil }

func TestSnapshotRepositoryContracts(t *testing.T) {
	var _ SnapshotRepository = snapshotRepositoryContract{}
	var _ LegacySnapshotSource = legacySnapshotSourceContract{}

	publication := SnapshotPublication{
		Name:       "named-session",
		Generation: 7,
		Manifest:   []byte("VEVM"),
		Objects:    []SnapshotObject{{Digest: SnapshotDigest{1}, Data: []byte("VEVO")}},
	}
	generation := SnapshotGeneration{
		Name:       publication.Name,
		Generation: publication.Generation,
		Manifest:   publication.Manifest,
		Objects:    map[SnapshotDigest][]byte{publication.Objects[0].Digest: publication.Objects[0].Data},
		Fallback:   true,
	}
	if publication.Name != "named-session" || generation.Generation != 7 || !generation.Fallback {
		t.Fatalf("snapshot publication/generation = %#v", generation)
	}
	if len(SnapshotDigest{}) != sha256.Size {
		t.Fatalf("SnapshotDigest has length %d, want %d", len(SnapshotDigest{}), sha256.Size)
	}
}

type snapshotStoreContract struct{}

func (snapshotStoreContract) Write(name string, data []byte) error { return nil }
func (snapshotStoreContract) Load() ([]SnapshotBlob, error)        { return nil, nil }
func (snapshotStoreContract) Delete(name string) error             { return nil }

func TestSnapshotStoreContract(t *testing.T) {
	var _ SnapshotStore = snapshotStoreContract{}
}
