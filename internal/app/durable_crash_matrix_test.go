package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/adapters/recoveryfs"
	"github.com/bnema/vev/internal/adapters/snapshot"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/recovery"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/stretchr/testify/require"
)

var errInjectedCrash = errors.New("injected crash after durable boundary")

type durableCrashHarness struct {
	stateDir    string
	snapshotDir string
}

func newDurableCrashHarness(t *testing.T) durableCrashHarness {
	t.Helper()
	stateDir := t.TempDir()
	require.NoError(t, os.Chmod(stateDir, 0o700))
	return durableCrashHarness{stateDir: stateDir, snapshotDir: filepath.Join(stateDir, "snapshots")}
}

func (h durableCrashHarness) open(t *testing.T) (*persist.Persister, *snapshot.Repository, *recoveryfs.Journal) {
	t.Helper()
	store, err := persist.OpenStore(persist.StorePath(h.stateDir))
	require.NoError(t, err)
	return persist.New(store), snapshot.NewRepository(h.snapshotDir), recoveryfs.New(h.stateDir)
}

func (h durableCrashHarness) recoverTwice(ctx context.Context, t *testing.T, assert func(*persist.Persister, *snapshot.Repository, *recoveryfs.Journal)) {
	t.Helper()
	for attempt := range 2 {
		catalogue, repository, journal := h.open(t)
		coordinator := recovery.NewCoordinator(catalogue, repository, journal, nil)
		require.NoError(t, coordinator.Recover(ctx), "recovery attempt %d", attempt+1)
		assert(catalogue, repository, journal)
		require.NoError(t, catalogue.Close())
	}
}

type durableBoundary struct {
	want      string
	triggered bool
}

func (b *durableBoundary) after(name string, err error) error {
	if err != nil || name != b.want {
		return err
	}
	b.triggered = true
	return errInjectedCrash
}

type crashCatalogue struct {
	ports.Catalogue
	boundary *durableBoundary
}

func (c crashCatalogue) Create(record domain.CatalogueRecord) error {
	return c.boundary.after("catalogue-create", c.Catalogue.Create(record))
}
func (c crashCatalogue) Replace(name string, record domain.CatalogueRecord) error {
	return c.boundary.after("catalogue-replace", c.Catalogue.Replace(name, record))
}
func (c crashCatalogue) Rename(oldName string, record domain.CatalogueRecord) error {
	return c.boundary.after("catalogue-rename", c.Catalogue.Rename(oldName, record))
}
func (c crashCatalogue) Delete(name string) error {
	return c.boundary.after("catalogue-delete", c.Catalogue.Delete(name))
}

type crashRepository struct {
	ports.SnapshotRepository
	boundary *durableBoundary
}

func (r crashRepository) Publish(ctx context.Context, publication ports.SnapshotPublication) error {
	return r.boundary.after("snapshot-publish", r.SnapshotRepository.Publish(ctx, publication))
}
func (r crashRepository) WriteDeletionTombstone(ctx context.Context, tombstone domain.DeletionTombstone) error {
	return r.boundary.after("deletion-tombstone", r.SnapshotRepository.WriteDeletionTombstone(ctx, tombstone))
}
func (r crashRepository) QuarantineDeletionSources(ctx context.Context, tombstone domain.DeletionTombstone, includeLegacy bool) error {
	return r.boundary.after("deletion-quarantine", r.SnapshotRepository.QuarantineDeletionSources(ctx, tombstone, includeLegacy))
}
func (r crashRepository) DeleteDeletionTombstone(ctx context.Context, id domain.IncarnationID) error {
	return r.boundary.after("deletion-tombstone-delete", r.SnapshotRepository.DeleteDeletionTombstone(ctx, id))
}
func (r crashRepository) SaveQuarantineDescriptor(ctx context.Context, descriptor domain.QuarantineDescriptor) error {
	return r.boundary.after("discard-descriptor", r.SnapshotRepository.SaveQuarantineDescriptor(ctx, descriptor))
}
func (r crashRepository) QuarantineIncarnation(ctx context.Context, id domain.IncarnationID) error {
	return r.boundary.after("discard-quarantine", r.SnapshotRepository.QuarantineIncarnation(ctx, id))
}

type crashJournal struct {
	ports.RecoveryJournal
	boundary *durableBoundary
}

func (j crashJournal) SaveDiscard(ctx context.Context, intent domain.DiscardIntent) error {
	return j.boundary.after("discard-intent", j.RecoveryJournal.SaveDiscard(ctx, intent))
}
func (j crashJournal) DeleteDiscard(ctx context.Context, id domain.IncarnationID) error {
	return j.boundary.after("discard-intent-delete", j.RecoveryJournal.DeleteDiscard(ctx, id))
}

func TestDurableTransactionCrashMatrix(t *testing.T) {
	ctx := context.Background()

	t.Run("create/catalogue-create", func(t *testing.T) {
		h := newDurableCrashHarness(t)
		catalogue, repository, journal := h.open(t)
		boundary := &durableBoundary{want: "catalogue-create"}
		coordinator := recovery.NewCoordinator(crashCatalogue{Catalogue: catalogue, boundary: boundary}, repository, journal, bytes.NewReader(bytes.Repeat([]byte{1}, 16)))
		_, err := coordinator.Create(ctx, domain.CatalogueRecord{Name: "work", Cwd: "/tmp"})
		require.ErrorIs(t, err, errInjectedCrash)
		require.True(t, boundary.triggered)
		require.NoError(t, catalogue.Close())

		h.recoverTwice(ctx, t, func(catalogue *persist.Persister, _ *snapshot.Repository, _ *recoveryfs.Journal) {
			record, exists, err := catalogue.Record("work")
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, domain.IncarnationID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, record.IncarnationID)
			require.Equal(t, domain.RecoveryFresh, record.RecoveryState)
		})
	})

	for _, boundaryName := range []string{"snapshot-publish", "catalogue-replace"} {
		t.Run("checkpoint/"+boundaryName, func(t *testing.T) {
			h := newDurableCrashHarness(t)
			catalogue, repository, journal := h.open(t)
			record := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{2}, RecoveryState: domain.RecoveryFresh}
			require.NoError(t, catalogue.Create(record))
			publication := durablePublication(t, record.Name, record.IncarnationID, []byte("checkpoint"))
			boundary := &durableBoundary{want: boundaryName}
			coordinator := recovery.NewCoordinator(
				crashCatalogue{Catalogue: catalogue, boundary: boundary},
				crashRepository{SnapshotRepository: repository, boundary: boundary},
				journal,
				nil,
			)
			_, err := coordinator.PublishCheckpoint(ctx, record.Name, publication)
			require.ErrorIs(t, err, errInjectedCrash)
			require.True(t, boundary.triggered)
			require.NoError(t, catalogue.Close())

			ref := domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(publication.Manifest)}
			h.recoverTwice(ctx, t, func(catalogue *persist.Persister, repository *snapshot.Repository, _ *recoveryfs.Journal) {
				stored, exists, err := catalogue.Record(record.Name)
				require.NoError(t, err)
				require.True(t, exists)
				if boundaryName == "snapshot-publish" {
					require.Equal(t, domain.RecoveryFresh, stored.RecoveryState)
					require.Nil(t, stored.Committed)
				} else {
					require.Equal(t, domain.RecoveryHealthy, stored.RecoveryState)
					require.Equal(t, ref, *stored.Committed)
				}
				generation, err := repository.LoadCheckpoint(ctx, record.IncarnationID, record.Name, ref)
				require.NoError(t, err)
				require.Equal(t, publication.Manifest, generation.Manifest)
			})
		})
	}

	t.Run("rename/catalogue-rename", func(t *testing.T) {
		h := newDurableCrashHarness(t)
		catalogue, repository, journal := h.open(t)
		record := domain.CatalogueRecord{Name: "old", IncarnationID: domain.IncarnationID{3}, RecoveryState: domain.RecoveryFresh}
		require.NoError(t, catalogue.Create(record))
		boundary := &durableBoundary{want: "catalogue-rename"}
		coordinator := recovery.NewCoordinator(crashCatalogue{Catalogue: catalogue, boundary: boundary}, repository, journal, nil)
		_, err := coordinator.Rename(ctx, "old", "new")
		require.ErrorIs(t, err, errInjectedCrash)
		require.True(t, boundary.triggered)
		require.NoError(t, catalogue.Close())

		h.recoverTwice(ctx, t, func(catalogue *persist.Persister, _ *snapshot.Repository, _ *recoveryfs.Journal) {
			_, oldExists, err := catalogue.Record("old")
			require.NoError(t, err)
			renamed, newExists, err := catalogue.Record("new")
			require.NoError(t, err)
			require.False(t, oldExists)
			require.True(t, newExists)
			require.Equal(t, record.IncarnationID, renamed.IncarnationID)
		})
	})

	deleteBoundaries := []string{"catalogue-replace", "deletion-tombstone", "deletion-quarantine", "catalogue-delete", "deletion-tombstone-delete", "transaction-complete"}
	for _, boundaryName := range deleteBoundaries {
		t.Run("delete/"+boundaryName, func(t *testing.T) {
			h := newDurableCrashHarness(t)
			catalogue, repository, journal := h.open(t)
			record := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{4}, RecoveryState: domain.RecoveryFresh}
			require.NoError(t, catalogue.Create(record))
			publication := durablePublication(t, record.Name, record.IncarnationID, []byte("delete-source"))
			require.NoError(t, repository.Publish(ctx, publication))
			legacy := []byte{0, 1, 2, 3, 0xff}
			require.NoError(t, os.WriteFile(filepath.Join(h.snapshotDir, "work.snap"), legacy, 0o600))
			sourceBytes := readTree(t, filepath.Join(h.snapshotDir, "sessions", record.IncarnationID.String()))

			boundary := &durableBoundary{want: boundaryName}
			coordinator := recovery.NewCoordinator(
				crashCatalogue{Catalogue: catalogue, boundary: boundary},
				crashRepository{SnapshotRepository: repository, boundary: boundary},
				journal,
				nil,
			)
			err := coordinator.Delete(ctx, record.Name)
			if boundaryName == "transaction-complete" {
				require.NoError(t, err)
				boundary.triggered = true // interruption before the caller transfers lifecycle ownership
			} else {
				require.ErrorIs(t, err, errInjectedCrash)
			}
			require.True(t, boundary.triggered)
			require.NoError(t, catalogue.Close())

			h.recoverTwice(ctx, t, func(catalogue *persist.Persister, repository *snapshot.Repository, _ *recoveryfs.Journal) {
				_, exists, err := catalogue.Record(record.Name)
				require.NoError(t, err)
				require.False(t, exists)
				assertNoTombstones(ctx, t, repository)
				require.NoDirExists(t, filepath.Join(h.snapshotDir, "sessions", record.IncarnationID.String()))
				require.Equal(t, sourceBytes, readTree(t, filepath.Join(h.snapshotDir, "quarantine", record.IncarnationID.String(), "snapshot")))
				require.Equal(t, legacy, mustReadFile(t, filepath.Join(h.snapshotDir, "quarantine", record.IncarnationID.String(), "legacy.snap")))
			})
		})
	}
}

func TestDurableDiscardCrashMatrix(t *testing.T) {
	ctx := context.Background()
	oldID := domain.IncarnationID{5}
	newID := domain.IncarnationID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	boundaries := []string{"discard-intent", "discard-descriptor", "discard-quarantine", "catalogue-replace", "discard-intent-delete", "runtime-exposure"}

	for _, boundaryName := range boundaries {
		t.Run(boundaryName, func(t *testing.T) {
			h := newDurableCrashHarness(t)
			catalogue, repository, journal := h.open(t)
			publication := durablePublication(t, "broken", oldID, []byte("discard-source"))
			require.NoError(t, repository.Publish(ctx, publication))
			ref := &domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(publication.Manifest)}
			old := domain.CatalogueRecord{Name: "broken", IncarnationID: oldID, RecoveryState: domain.RecoveryDegraded, Committed: ref, DegradedReason: "checkpoint unreadable"}
			require.NoError(t, catalogue.Create(old))
			sourceBytes := readTree(t, filepath.Join(h.snapshotDir, "sessions", oldID.String()))

			boundary := &durableBoundary{want: boundaryName}
			coordinator := recovery.NewCoordinator(
				crashCatalogue{Catalogue: catalogue, boundary: boundary},
				crashRepository{SnapshotRepository: repository, boundary: boundary},
				crashJournal{RecoveryJournal: journal, boundary: boundary},
				bytes.NewReader(newID[:]),
			)
			replacement, err := coordinator.Discard(ctx, old.Name, "explicit discard")
			if boundaryName == "runtime-exposure" {
				require.NoError(t, err)
				require.Equal(t, newID, replacement.IncarnationID)
				boundary.triggered = true // crash before the returned runtime is installed
			} else {
				require.ErrorIs(t, err, errInjectedCrash)
			}
			require.True(t, boundary.triggered)
			require.NoError(t, catalogue.Close())

			h.recoverTwice(ctx, t, func(catalogue *persist.Persister, _ *snapshot.Repository, journal *recoveryfs.Journal) {
				got, exists, err := catalogue.Record(old.Name)
				require.NoError(t, err)
				require.True(t, exists)
				require.Equal(t, newID, got.IncarnationID)
				require.Equal(t, domain.RecoveryFresh, got.RecoveryState)
				require.Nil(t, got.Committed)
				require.NoDirExists(t, filepath.Join(h.snapshotDir, "sessions", oldID.String()))
				require.Equal(t, sourceBytes, readTree(t, filepath.Join(h.snapshotDir, "quarantine", oldID.String(), "snapshot")))
				intents, err := journal.ListDiscards(ctx)
				require.NoError(t, err)
				require.Empty(t, intents)
				require.FileExists(t, filepath.Join(h.snapshotDir, "quarantine", oldID.String(), "record"))
			})
		})
	}
}

func TestDeletionRecoveryProtectsReusedNameFilesystemBytes(t *testing.T) {
	ctx := context.Background()
	h := newDurableCrashHarness(t)
	catalogue, repository, _ := h.open(t)
	oldID := domain.IncarnationID{6}
	replacementID := domain.IncarnationID{7}
	oldPublication := durablePublication(t, "work", oldID, []byte("old-incarnation"))
	replacementPublication := durablePublication(t, "work", replacementID, []byte("replacement-incarnation"))
	require.NoError(t, repository.Publish(ctx, oldPublication))
	require.NoError(t, repository.Publish(ctx, replacementPublication))
	replacement := domain.CatalogueRecord{Name: "work", IncarnationID: replacementID, RecoveryState: domain.RecoveryFresh}
	require.NoError(t, catalogue.Create(replacement))
	require.NoError(t, repository.WriteDeletionTombstone(ctx, domain.DeletionTombstone{Name: "work", IncarnationID: oldID}))
	legacy := []byte{0xde, 0xad, 0x00, 0xbe, 0xef}
	require.NoError(t, os.WriteFile(filepath.Join(h.snapshotDir, "work.snap"), legacy, 0o600))
	oldBytes := readTree(t, filepath.Join(h.snapshotDir, "sessions", oldID.String()))
	replacementPath := filepath.Join(h.snapshotDir, "sessions", replacementID.String())
	replacementBytes := readTree(t, replacementPath)
	require.NoError(t, catalogue.Close())

	h.recoverTwice(ctx, t, func(catalogue *persist.Persister, repository *snapshot.Repository, _ *recoveryfs.Journal) {
		got, exists, err := catalogue.Record("work")
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, replacement, got)
		require.Equal(t, replacementBytes, readTree(t, replacementPath))
		require.Equal(t, legacy, mustReadFile(t, filepath.Join(h.snapshotDir, "work.snap")))
		require.Equal(t, oldBytes, readTree(t, filepath.Join(h.snapshotDir, "quarantine", oldID.String(), "snapshot")))
		require.NoDirExists(t, filepath.Join(h.snapshotDir, "sessions", oldID.String()))
		assertNoTombstones(ctx, t, repository)
	})
}

func durablePublication(t *testing.T, name string, id domain.IncarnationID, payload []byte) ports.SnapshotPublication {
	t.Helper()
	tail, err := snapcodec.MarshalObject(snapcodec.HistoryTail, payload)
	require.NoError(t, err)
	visible, err := snapcodec.MarshalObject(snapcodec.Visible, []byte("visible"))
	require.NoError(t, err)
	manifest, err := snapcodec.MarshalManifest(snapcodec.Manifest{
		Generation: 1, IncarnationID: id, Name: name,
		Tabs: []snapcodec.ManifestTab{{Cols: 1, Rows: 1, Panes: []snapcodec.ManifestPane{{
			ID: "p", Tail: snapcodec.ObjectRef{Kind: snapcodec.HistoryTail, Digest: tail.Digest, Size: uint32(len(tail.Data))},
			Visible: snapcodec.ObjectRef{Kind: snapcodec.Visible, Digest: visible.Digest, Size: uint32(len(visible.Data))},
		}}}},
	})
	require.NoError(t, err)
	return ports.SnapshotPublication{IncarnationID: id, Name: name, Generation: 1, Manifest: manifest, Objects: []ports.SnapshotObject{tail, visible}}
}

func assertNoTombstones(ctx context.Context, t *testing.T, repository *snapshot.Repository) {
	t.Helper()
	page, err := repository.ListDeletionTombstones(ctx, ports.DeletionTombstoneCursor{}, ports.MaintenanceBudget{Entries: 64, Bytes: 64 << 10})
	require.NoError(t, err)
	require.True(t, page.Done)
	require.Empty(t, page.Tombstones)
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func readTree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[relative] = data
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, files)
	return files
}
