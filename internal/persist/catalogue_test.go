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
	t.Run("read-only-malformed", testCatalogueLoadReadOnlyRejectsMalformedValue)
	t.Run("read-only-fresh-install", testCatalogueLoadReadOnlyFreshInstallHasNoSessions)
	t.Run("read-only-recovers-crash-states", testCatalogueLoadReadOnlyRecoversCrashStates)
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

func testCatalogueMetadataUpdatePreservesAuthority(t *testing.T) {
	dir := privateDir(t)
	p, _, err := openCurrentCatalogue(dir, true)
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Close()) }()

	committed := &domain.CheckpointRef{Generation: 9, ManifestDigest: [32]byte{1}}
	fallback1 := &domain.CheckpointRef{Generation: 8, ManifestDigest: [32]byte{2}}
	fallback2 := &domain.CheckpointRef{Generation: 7, ManifestDigest: [32]byte{3}}
	original := domain.CatalogueRecord{
		Name: "work", IncarnationID: domain.IncarnationID{1}, Cwd: "/old", CreatedAt: 11,
		UpdatedAt: 12, LastUsedSeq: 13, TabNames: []string{"old"},
		RecoveryState: domain.RecoveryDegraded, Committed: committed,
		Fallbacks: [2]*domain.CheckpointRef{fallback1, fallback2}, DegradedReason: "repair pending",
	}
	require.NoError(t, p.Create(original))

	next := domain.CatalogueRecord{Name: "work", IncarnationID: original.IncarnationID, Cwd: "/new", UpdatedAt: 22, LastUsedSeq: 23, TabNames: []string{"editor", "logs"}}
	update := next.MetadataUpdate()
	require.NoError(t, p.UpdateMetadata(update))
	got, ok := p.Record("work")
	require.True(t, ok)
	expected := original
	expected.Cwd, expected.UpdatedAt, expected.LastUsedSeq, expected.TabNames = next.Cwd, next.UpdatedAt, next.LastUsedSeq, next.TabNames
	require.Equal(t, expected, got)

	lastUsedSeq := uint64(24)
	require.NoError(t, p.UpdateMetadata(domain.CatalogueMetadataUpdate{
		Name: "work", IncarnationID: original.IncarnationID, LastUsedSeq: &lastUsedSeq,
	}))
	expected.LastUsedSeq = lastUsedSeq
	partiallyUpdated, ok := p.Record("work")
	require.True(t, ok)
	require.Equal(t, expected, partiallyUpdated, "a partial metadata update must not clear other mutable or authority-owned fields")

	stale := update
	stale.IncarnationID = domain.IncarnationID{2}
	require.Error(t, p.UpdateMetadata(stale))
	unchanged, ok := p.Record("work")
	require.True(t, ok)
	require.Equal(t, expected, unchanged)
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

// testCatalogueLoadReadOnlyFreshInstallHasNoSessions covers `vev ls` on a
// machine with no daemon and no catalogue: none of sessions.kv, .next, or
// .prev exist. This must yield an empty catalogue, not an error (regression:
// a bare os.Stat propagated ErrNotExist and made `vev ls` exit 1).
func testCatalogueLoadReadOnlyFreshInstallHasNoSessions(t *testing.T) {
	dir := privateDir(t)
	records, err := LoadCatalogueReadOnly(dir)
	require.NoError(t, err)
	require.Empty(t, records)
}

// testCatalogueLoadReadOnlyRecoversCrashStates covers spec acceptance
// criterion 3 ("every valid catalogue record appears in vev list") for a
// listing taken while no daemon is running: whenever any compaction
// candidate exists, LoadCatalogueReadOnly must apply the same fixed-path
// recovery the daemon uses on startup rather than failing the read.
func testCatalogueLoadReadOnlyRecoversCrashStates(t *testing.T) {
	// Built through an unrelated scratch path and copied into place, rather
	// than kv.Open'd directly at the target path: kv.Open recovers its own
	// base path on every call, and target paths here deliberately coexist
	// with sibling .next/.prev candidates that would otherwise be recovered
	// away before the test gets to arrange its crash state.
	writeCandidate := func(t *testing.T, path string, records ...domain.CatalogueRecord) {
		t.Helper()
		scratch := filepath.Join(t.TempDir(), "scratch", "store.kv")
		store, err := kv.Open(scratch)
		require.NoError(t, err)
		for _, record := range records {
			value, err := encodeRecordValue(record)
			require.NoError(t, err)
			require.NoError(t, store.Set([]byte(record.Name), value))
		}
		require.NoError(t, store.Close())
		data, err := os.ReadFile(scratch)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, data, 0o600))
	}

	tests := []struct {
		name  string
		write func(t *testing.T, dir string, want domain.CatalogueRecord)
	}{
		{
			// Crash between "rename current->.prev" and "rename .next->current":
			// no current, both .prev (stale) and .next (authoritative) exist.
			name: "prev-and-next-recovers-from-next",
			write: func(t *testing.T, dir string, want domain.CatalogueRecord) {
				stale := want
				stale.IncarnationID = domain.IncarnationID{9}
				writeCandidate(t, StorePath(dir)+".prev", stale)
				writeCandidate(t, StorePath(dir)+".next", want)
			},
		},
		{
			// Crash leaves only a predecessor behind; no current, no successor.
			name: "prev-only-recovers-from-prev",
			write: func(t *testing.T, dir string, want domain.CatalogueRecord) {
				writeCandidate(t, StorePath(dir)+".prev", want)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := privateDir(t)
			require.NoError(t, os.MkdirAll(dir, 0o700))
			want := validRecord("recovered", 1)
			tt.write(t, dir, want)

			records, err := LoadCatalogueReadOnly(dir)
			require.NoError(t, err)
			require.ElementsMatch(t, []domain.CatalogueRecord{want}, records)

			_, statErr := os.Stat(StorePath(dir))
			require.NoError(t, statErr, "recovery must leave a valid current catalogue in place")
			_, nextErr := os.Stat(StorePath(dir) + ".next")
			require.True(t, os.IsNotExist(nextErr), ".next must not survive recovery")
			_, prevErr := os.Stat(StorePath(dir) + ".prev")
			require.True(t, os.IsNotExist(prevErr), ".prev must not survive recovery")
		})
	}
}
