package persist

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/kv"
)

func TestCatalogue(t *testing.T) {
	t.Run("record-round-trip", testCatalogueRecordRoundTrip)
	t.Run("duplicate-incarnations", testCatalogueDuplicateIncarnations)
	t.Run("open-fails-closed", testCatalogueOpenFailsClosed)
	t.Run("apply-rename-replace", testCatalogueApplyRenameReplace)
	t.Run("read-only-malformed", testCatalogueLoadReadOnlyRejectsMalformedValue)
}

func testCatalogueRecordRoundTrip(t *testing.T) {
	ref1 := &domain.CheckpointRef{Generation: 9, ManifestDigest: [32]byte{1}}
	ref2 := &domain.CheckpointRef{Generation: 8, ManifestDigest: [32]byte{2}}
	ref3 := &domain.CheckpointRef{Generation: 7, ManifestDigest: [32]byte{3}}
	tests := []domain.CatalogueRecord{
		{Name: "fresh", IncarnationID: domain.IncarnationID{1}, Cwd: "/tmp", RecoveryState: domain.RecoveryFresh},
		{Name: "healthy", IncarnationID: domain.IncarnationID{2}, RecoveryState: domain.RecoveryHealthy, Committed: ref1},
		{Name: "healthy-one-fallback", IncarnationID: domain.IncarnationID{3}, RecoveryState: domain.RecoveryHealthy, Committed: ref1, Fallbacks: [2]*domain.CheckpointRef{ref2}},
		{Name: "degraded", IncarnationID: domain.IncarnationID{4}, RecoveryState: domain.RecoveryDegraded, Committed: ref1, Fallbacks: [2]*domain.CheckpointRef{ref2, ref3}, DegradedReason: "invalid manifest"},
		{Name: "deleting", IncarnationID: domain.IncarnationID{5}, RecoveryState: domain.RecoveryDeleting},
	}
	for _, record := range tests {
		t.Run(record.Name, func(t *testing.T) {
			encoded, err := encodeRecordValue(record)
			require.NoError(t, err)
			got, err := decodeRecordValue(record.Name, encoded)
			require.NoError(t, err)
			require.Equal(t, record, got)
			require.Error(t, func() error { _, err := decodeRecordValue(record.Name, append(encoded, 0)); return err }())
		})
	}
}

func testCatalogueDuplicateIncarnations(t *testing.T) {
	id := domain.IncarnationID{1}
	require.Error(t, validateUniqueIncarnations([]domain.CatalogueRecord{
		{Name: "one", IncarnationID: id, RecoveryState: domain.RecoveryFresh},
		{Name: "two", IncarnationID: id, RecoveryState: domain.RecoveryFresh},
	}))
}

func testCatalogueOpenFailsClosed(t *testing.T) {
	dir := privateDir(t)
	_, _, err := openCurrentCatalogue(dir, false)
	require.Error(t, err)

	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.WriteFile(StorePath(dir), rawCorruptCatalogueWAL(), 0o600))
	_, _, err = openCurrentCatalogue(dir, false)
	require.ErrorIs(t, err, kv.ErrCorruptWAL)

	require.NoError(t, os.Remove(StorePath(dir)))
	require.NoError(t, os.WriteFile(StorePath(dir)+".prev", rawCorruptCatalogueWAL(), 0o600))
	_, _, err = openCurrentCatalogue(dir, false)
	require.ErrorIs(t, err, kv.ErrCorruptWAL)
}

func testCatalogueApplyRenameReplace(t *testing.T) {
	dir := privateDir(t)
	p, records, err := openCurrentCatalogue(dir, true)
	require.NoError(t, err)
	require.Empty(t, records)
	defer func() { require.NoError(t, p.Close()) }()

	one := domain.CatalogueRecord{Name: "one", IncarnationID: domain.IncarnationID{1}, RecoveryState: domain.RecoveryFresh}
	two := domain.CatalogueRecord{Name: "two", IncarnationID: domain.IncarnationID{2}, RecoveryState: domain.RecoveryFresh}
	require.NoError(t, p.Apply(map[string]*domain.CatalogueRecord{"one": &one, "two": &two}))
	renamed := one
	renamed.Name = "renamed"
	require.NoError(t, p.Rename("one", renamed))
	two.Cwd = "/next"
	require.NoError(t, p.Replace("two", two))

	got, err := p.LoadCatalogue()
	require.NoError(t, err)
	require.ElementsMatch(t, []domain.CatalogueRecord{renamed, two}, got)
}

func rawCorruptCatalogueWAL() []byte {
	// A complete WAL record with an impossible CRC.
	return []byte{0, 0, 0, 3, 0, 0, 0, 0, 0xff, 0, 0}
}

func testCatalogueLoadReadOnlyRejectsMalformedValue(t *testing.T) {
	dir := privateDir(t)
	store, err := kv.Open(filepath.Join(dir, filename))
	require.NoError(t, err)
	require.NoError(t, store.Set([]byte("work"), []byte("malformed")))
	require.NoError(t, store.Close())
	_, err = LoadCatalogueReadOnly(dir)
	require.Error(t, err)
}
