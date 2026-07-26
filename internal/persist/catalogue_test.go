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
	t.Run("metadata-update-preserves-authority", testCatalogueMetadataUpdatePreservesAuthority)
	t.Run("metadata-update-is-deferred", testCatalogueMetadataUpdateIsDeferred)
	t.Run("read-only-malformed", testCatalogueLoadReadOnlyRejectsMalformedValue)
	t.Run("read-only-fresh-install", testCatalogueLoadReadOnlyFreshInstallHasNoSessions)
	t.Run("read-only-ignores-stray-tmp", testCatalogueLoadReadOnlyIgnoresStrayTmp)
}

func testCatalogueRecordRoundTrip(t *testing.T) {
	ref1 := &domain.CheckpointRef{Generation: 9, ManifestDigest: [32]byte{1}}
	tests := []domain.CatalogueRecord{
		{Name: "fresh", IncarnationID: domain.IncarnationID{1}, Cwd: "/tmp"},
		{Name: "healthy", IncarnationID: domain.IncarnationID{2}, Committed: ref1},
		{Name: "broken", IncarnationID: domain.IncarnationID{4}, Committed: ref1, DegradedReason: "invalid manifest"},
		{Name: "other-fresh", IncarnationID: domain.IncarnationID{5}},
	}
	for _, record := range tests {
		t.Run(record.Name, func(t *testing.T) {
			encoded, err := encodeRecordValue(record)
			require.NoError(t, err)
			got, err := decodeRecordValue(record.Name, encoded)
			require.NoError(t, err)
			require.Equal(t, record, got)
			for prefix := range len(encoded) {
				_, err := decodeRecordValue(record.Name, encoded[:prefix])
				require.Error(t, err, "prefix length %d", prefix)
			}
			require.Error(t, func() error { _, err := decodeRecordValue(record.Name, append(encoded, 0)); return err }())
		})
	}
}

func TestDecodeRecordValueRejectsMalformedCheckpointMarker(t *testing.T) {
	encoded, err := encodeRecordValue(validRecord("work", 1))
	require.NoError(t, err)
	encoded[checkpointMarkerOffset(t, encoded)] = 99

	_, err = decodeRecordValue("work", encoded)
	require.ErrorIs(t, err, errMalformedRecord)
}

func checkpointMarkerOffset(t *testing.T, encoded []byte) int {
	t.Helper()
	r := valueReader{data: encoded}
	_, ok := r.take(len(catalogueMagic) + 2 + len(domain.IncarnationID{}))
	require.True(t, ok)
	_, ok = r.str()
	require.True(t, ok)
	_, ok = r.take(8 + 8 + 8)
	require.True(t, ok)
	count, ok := r.u32()
	require.True(t, ok)
	for range count {
		_, ok = r.str()
		require.True(t, ok)
	}
	return len(encoded) - r.remaining()
}

func testCatalogueDuplicateIncarnations(t *testing.T) {
	id := domain.IncarnationID{1}
	require.Error(t, validateUniqueIncarnations([]domain.CatalogueRecord{
		{Name: "one", IncarnationID: id},
		{Name: "two", IncarnationID: id},
	}))
}

func testCatalogueOpenFailsClosed(t *testing.T) {
	dir := privateDir(t)
	_, _, err := openCurrentCatalogue(dir, false)
	require.Error(t, err)

	require.NoError(t, os.MkdirAll(dir, 0o700))
	raw := []byte("corrupt catalogue")
	require.NoError(t, os.WriteFile(StorePath(dir), raw, 0o600))
	_, _, err = openCurrentCatalogue(dir, false)
	require.ErrorIs(t, err, kv.ErrCorrupt)
	after, readErr := os.ReadFile(StorePath(dir))
	require.NoError(t, readErr)
	require.Equal(t, raw, after)
}

func testCatalogueApplyRenameReplace(t *testing.T) {
	dir := privateDir(t)
	p, records, err := openCurrentCatalogue(dir, true)
	require.NoError(t, err)
	require.Empty(t, records)
	defer func() { require.NoError(t, p.Close()) }()

	one := domain.CatalogueRecord{Name: "one", IncarnationID: domain.IncarnationID{1}}
	two := domain.CatalogueRecord{Name: "two", IncarnationID: domain.IncarnationID{2}}
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

func testCatalogueMetadataUpdatePreservesAuthority(t *testing.T) {
	dir := privateDir(t)
	p, _, err := openCurrentCatalogue(dir, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Close()) }()

	committed := &domain.CheckpointRef{Generation: 9, ManifestDigest: [32]byte{1}}
	original := domain.CatalogueRecord{
		Name: "work", IncarnationID: domain.IncarnationID{1}, Cwd: "/old", CreatedAt: 11,
		UpdatedAt: 12, LastUsedSeq: 13, TabNames: []string{"old"},
		Committed: committed, DegradedReason: "repair pending",
	}
	require.NoError(t, p.Create(original))

	next := domain.CatalogueRecord{Name: "work", IncarnationID: original.IncarnationID, Cwd: "/new", UpdatedAt: 22, LastUsedSeq: 23, TabNames: []string{"editor", "logs"}}
	update := next.MetadataUpdate()
	require.NoError(t, p.UpdateMetadata(update))
	got, ok, err := p.Record("work")
	require.NoError(t, err)
	require.True(t, ok)
	expected := original
	expected.Cwd, expected.UpdatedAt, expected.LastUsedSeq, expected.TabNames = next.Cwd, next.UpdatedAt, next.LastUsedSeq, next.TabNames
	require.Equal(t, expected, got)

	lastUsedSeq := uint64(24)
	require.NoError(t, p.UpdateMetadata(domain.CatalogueMetadataUpdate{
		Name: "work", IncarnationID: original.IncarnationID, LastUsedSeq: &lastUsedSeq,
	}))
	expected.LastUsedSeq = lastUsedSeq
	partiallyUpdated, ok, err := p.Record("work")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, expected, partiallyUpdated, "a partial metadata update must not clear other mutable or authority-owned fields")

	stale := update
	stale.IncarnationID = domain.IncarnationID{2}
	require.Error(t, p.UpdateMetadata(stale))
	unchanged, ok, err := p.Record("work")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, expected, unchanged)
}

func testCatalogueMetadataUpdateIsDeferred(t *testing.T) {
	dir := privateDir(t)
	p, _, err := openCurrentCatalogue(dir, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Close()) }()

	record := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{1}, Cwd: "/old"}
	require.NoError(t, p.Create(record))
	before, err := os.ReadFile(StorePath(dir))
	require.NoError(t, err)

	cwd := "/buffered"
	require.NoError(t, p.UpdateMetadata(domain.CatalogueMetadataUpdate{
		Name: record.Name, IncarnationID: record.IncarnationID, Cwd: &cwd,
	}))
	afterBuffered, err := os.ReadFile(StorePath(dir))
	require.NoError(t, err)
	require.Equal(t, before, afterBuffered, "metadata must remain buffered before Sync")

	require.NoError(t, p.Sync())
	afterSync, err := os.ReadFile(StorePath(dir))
	require.NoError(t, err)
	require.NotEqual(t, before, afterSync)

	cwd = "/next-identity"
	require.NoError(t, p.UpdateMetadata(domain.CatalogueMetadataUpdate{
		Name: record.Name, IncarnationID: record.IncarnationID, Cwd: &cwd,
	}))
	record.Cwd = cwd
	record.UpdatedAt = 2
	require.NoError(t, p.Replace(record.Name, record))
	afterIdentity, err := os.ReadFile(StorePath(dir))
	require.NoError(t, err)
	require.NotEqual(t, afterSync, afterIdentity, "identity writes must sync before returning")
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

// testCatalogueLoadReadOnlyFreshInstallHasNoSessions covers `vev ls` on a
// machine with no daemon and no catalogue. This must yield an empty catalogue,
// not an error.
func testCatalogueLoadReadOnlyFreshInstallHasNoSessions(t *testing.T) {
	dir := privateDir(t)
	records, err := LoadCatalogueReadOnly(dir)
	require.NoError(t, err)
	require.Empty(t, records)
}

func testCatalogueLoadReadOnlyIgnoresStrayTmp(t *testing.T) {
	dir := privateDir(t)
	p, _, err := openCurrentCatalogue(dir, true)
	require.NoError(t, err)
	want := validRecord("work", 1)
	require.NoError(t, p.Create(want))
	require.NoError(t, p.Close())
	require.NoError(t, os.WriteFile(StorePath(dir)+".tmp", []byte("partial rewrite"), 0o600))

	records, err := LoadCatalogueReadOnly(dir)
	require.NoError(t, err)
	require.Equal(t, []domain.CatalogueRecord{want}, records)
	tmp, err := os.ReadFile(StorePath(dir) + ".tmp")
	require.NoError(t, err)
	require.Equal(t, []byte("partial rewrite"), tmp)
}
