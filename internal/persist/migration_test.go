package persist

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/kv"
)

const legacyRecordWireVersion = 5

func TestOpenOrCreateMigrationPreservesBackupAndIsIdempotent(t *testing.T) {
	t.Parallel()
	record := domain.CatalogueRecord{
		Name: "work", IncarnationID: domain.IncarnationID{1}, Cwd: "/workspace",
		CreatedAt: 11, UpdatedAt: 22, LastUsedSeq: 33,
		TabNames: []string{"shell"}, TabRecords: []domain.CatalogueTabRecord{{StableID: "tab-1", Name: "shell"}},
		Committed: &domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{3}},
	}
	legacy, err := encodeRecordValueForFormat(record, protocolRecordVersion, legacyRecordWireVersion)
	require.NoError(t, err)
	dir := privateDir(t)
	store, err := kv.Open(StorePath(dir))
	require.NoError(t, err)
	require.NoError(t, store.Set([]byte(record.Name), legacy))
	require.NoError(t, store.Close())
	original, err := os.ReadFile(StorePath(dir))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(StorePath(dir)+legacyCatalogueBackupSuffix+".tmp", []byte("stale"), 0o600))

	opened, err := OpenOrCreate(dir)
	require.NoError(t, err)
	require.Equal(t, []domain.CatalogueRecord{record}, opened.Records)
	require.True(t, opened.Migration.Performed)
	require.Equal(t, []uint16{protocolRecordVersion}, opened.Migration.SourceFormats)
	require.Equal(t, catalogueRecordVersion, opened.Migration.TargetFormat)
	require.Equal(t, 1, opened.Migration.RecordCount)
	require.NoError(t, opened.Catalogue.Close())

	backup, err := os.ReadFile(StorePath(dir) + legacyCatalogueBackupSuffix)
	require.NoError(t, err)
	require.Equal(t, original, backup)
	info, err := os.Stat(StorePath(dir) + legacyCatalogueBackupSuffix)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	migrated, err := os.ReadFile(StorePath(dir))
	require.NoError(t, err)

	reopened, err := OpenOrCreate(dir)
	require.NoError(t, err)
	require.False(t, reopened.Migration.Performed)
	require.Equal(t, []domain.CatalogueRecord{record}, reopened.Records)
	require.NoError(t, reopened.Catalogue.Close())
	after, err := os.ReadFile(StorePath(dir))
	require.NoError(t, err)
	require.Equal(t, migrated, after)
}

func TestOpenOrCreateMigratesV3Records(t *testing.T) {
	t.Parallel()
	legacyRecord := domain.CatalogueRecord{
		Name: "work", IncarnationID: domain.IncarnationID{1}, Cwd: "/workspace",
		CreatedAt: 11, UpdatedAt: 22, LastUsedSeq: 33, TabNames: []string{"shell"},
		Committed: &domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{3}},
	}
	legacy, err := encodeRecordValueForFormat(legacyRecord, legacyCatalogueRecordVersion, 0)
	require.NoError(t, err)
	dir := privateDir(t)
	store, err := kv.Open(StorePath(dir))
	require.NoError(t, err)
	require.NoError(t, store.Set([]byte(legacyRecord.Name), legacy))
	require.NoError(t, store.Close())
	original, err := os.ReadFile(StorePath(dir))
	require.NoError(t, err)

	opened, err := OpenOrCreate(dir)
	require.NoError(t, err)
	want := legacyRecord
	want.TabRecords = []domain.CatalogueTabRecord{{Name: "shell"}}
	require.Equal(t, []domain.CatalogueRecord{want}, opened.Records)
	require.Equal(t, []uint16{legacyCatalogueRecordVersion}, opened.Migration.SourceFormats)
	require.NoError(t, opened.Catalogue.Close())
	backup, err := os.ReadFile(StorePath(dir) + legacyCatalogueBackupSuffix)
	require.NoError(t, err)
	require.Equal(t, original, backup)
}

func TestOpenOrCreateMigrationRejectsConflictingBackupWithoutChangingCatalogue(t *testing.T) {
	t.Parallel()
	record := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{1}}
	legacy, err := encodeRecordValueForFormat(record, protocolRecordVersion, legacyRecordWireVersion)
	require.NoError(t, err)
	dir := privateDir(t)
	store, err := kv.Open(StorePath(dir))
	require.NoError(t, err)
	require.NoError(t, store.Set([]byte(record.Name), legacy))
	require.NoError(t, store.Close())
	original, err := os.ReadFile(StorePath(dir))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(StorePath(dir)+legacyCatalogueBackupSuffix, []byte("different"), 0o600))

	_, err = OpenOrCreate(dir)
	require.Error(t, err)
	after, readErr := os.ReadFile(StorePath(dir))
	require.NoError(t, readErr)
	require.Equal(t, original, after)
}

func TestOpenOrCreateMigrationRejectsSymlinkBackupWithoutChangingCatalogue(t *testing.T) {
	t.Parallel()
	record := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{1}}
	legacy, err := encodeRecordValueForFormat(record, protocolRecordVersion, legacyRecordWireVersion)
	require.NoError(t, err)
	dir := privateDir(t)
	store, err := kv.Open(StorePath(dir))
	require.NoError(t, err)
	require.NoError(t, store.Set([]byte(record.Name), legacy))
	require.NoError(t, store.Close())
	original, err := os.ReadFile(StorePath(dir))
	require.NoError(t, err)
	require.NoError(t, os.Symlink(StorePath(dir), StorePath(dir)+legacyCatalogueBackupSuffix))

	_, err = OpenOrCreate(dir)
	require.Error(t, err)
	after, readErr := os.ReadFile(StorePath(dir))
	require.NoError(t, readErr)
	require.Equal(t, original, after)
}
