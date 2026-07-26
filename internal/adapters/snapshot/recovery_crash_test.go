package snapshot

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/adapters/recoveryfs"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/recovery"
	"github.com/stretchr/testify/require"
)

var errDeletionBoundaryCrash = errors.New("simulated process interruption")

type recoveryCrashHarness struct {
	stateDir    string
	snapshotDir string
}

func newRecoveryCrashHarness(t *testing.T) recoveryCrashHarness {
	t.Helper()
	stateDir := t.TempDir()
	require.NoError(t, os.Chmod(stateDir, 0o700))
	return recoveryCrashHarness{stateDir: stateDir, snapshotDir: filepath.Join(stateDir, "snapshots")}
}

func (h recoveryCrashHarness) open(t *testing.T) (*persist.Persister, *Repository, *recoveryfs.Journal) {
	t.Helper()
	store, err := persist.OpenStore(persist.StorePath(h.stateDir))
	require.NoError(t, err)
	return persist.New(store), NewRepository(h.snapshotDir), recoveryfs.New(h.stateDir)
}

func TestDeletionRestartsAtInternalQuarantineBoundaries(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		inject func(*Repository)
	}{
		{name: "after incarnation namespace", inject: func(repository *Repository) {
			repository.hooks.afterDeletionIncarnationQuarantine = func() error { return errDeletionBoundaryCrash }
		}},
		{name: "after legacy name", inject: func(repository *Repository) {
			repository.hooks.afterDeletionLegacyQuarantine = func() error { return errDeletionBoundaryCrash }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRecoveryCrashHarness(t)
			catalogue, repository, journal := h.open(t)
			oldID := domain.IncarnationID{1}
			record := domain.CatalogueRecord{Name: "work", IncarnationID: oldID, RecoveryState: domain.RecoveryFresh}
			require.NoError(t, catalogue.Create(record))
			sessionPath := repository.sessionPath(oldID)
			require.NoError(t, os.MkdirAll(sessionPath, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(sessionPath, "durable"), []byte("old namespace"), 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(h.snapshotDir, filenameForName(record.Name)), []byte("old legacy"), 0o600))
			tc.inject(repository)

			coordinator := recovery.NewCoordinator(catalogue, repository, journal, nil)
			require.ErrorIs(t, coordinator.Delete(ctx, record.Name), errDeletionBoundaryCrash)
			require.NoError(t, catalogue.Close())

			// Simulate a new process: none of the adapter or coordinator state survives.
			catalogue, repository, journal = h.open(t)
			require.NoError(t, recovery.NewCoordinator(catalogue, repository, journal, nil).Recover(ctx))
			_, exists := catalogue.Record(record.Name)
			require.False(t, exists)
			require.NoDirExists(t, repository.sessionPath(oldID))
			require.Equal(t, []byte("old namespace"), mustReadCrashFile(t, filepath.Join(h.snapshotDir, "quarantine", oldID.String(), "snapshot", "durable")))
			require.Equal(t, []byte("old legacy"), mustReadCrashFile(t, filepath.Join(h.snapshotDir, "quarantine", oldID.String(), "legacy.snap")))
			page, err := repository.ListDeletionTombstones(ctx, ports.DeletionTombstoneCursor{}, ports.MaintenanceBudget{Entries: 16, Bytes: 4096})
			require.NoError(t, err)
			require.Empty(t, page.Tombstones)

			// Reusing the name after roll-forward must be safe on another restart.
			newID := domain.IncarnationID{2}
			created, err := recovery.NewCoordinator(catalogue, repository, journal, bytes.NewReader(newID[:])).Create(ctx, domain.CatalogueRecord{Name: record.Name})
			require.NoError(t, err)
			require.Equal(t, newID, created.IncarnationID)
			newPath := repository.sessionPath(newID)
			require.NoError(t, os.MkdirAll(newPath, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(newPath, "durable"), []byte("new namespace"), 0o600))
			require.NoError(t, catalogue.Close())

			catalogue, repository, journal = h.open(t)
			require.NoError(t, recovery.NewCoordinator(catalogue, repository, journal, nil).Recover(ctx))
			got, exists := catalogue.Record(record.Name)
			require.True(t, exists)
			require.Equal(t, newID, got.IncarnationID)
			require.Equal(t, []byte("new namespace"), mustReadCrashFile(t, filepath.Join(repository.sessionPath(newID), "durable")))
			require.NoError(t, catalogue.Close())
		})
	}
}

type rejectCreateCatalogue struct {
	ports.Catalogue
}

func (c rejectCreateCatalogue) Create(domain.CatalogueRecord) error {
	return errDeletionBoundaryCrash
}

func TestCreatePreCommitRestartLeavesNoAdoptableNamespace(t *testing.T) {
	ctx := context.Background()
	h := newRecoveryCrashHarness(t)
	catalogue, repository, journal := h.open(t)
	interruptedID := domain.IncarnationID{3}
	_, err := recovery.NewCoordinator(rejectCreateCatalogue{Catalogue: catalogue}, repository, journal, bytes.NewReader(interruptedID[:])).Create(ctx, domain.CatalogueRecord{Name: "work"})
	require.ErrorIs(t, err, errDeletionBoundaryCrash)
	require.Empty(t, catalogue.Records())
	require.NoDirExists(t, filepath.Join(h.snapshotDir, repositorySessionsDir))
	require.NoError(t, catalogue.Close())

	catalogue, repository, journal = h.open(t)
	require.NoError(t, recovery.NewCoordinator(catalogue, repository, journal, nil).Recover(ctx))
	require.Empty(t, catalogue.Records())
	require.NoDirExists(t, repository.sessionPath(interruptedID))

	retryID := domain.IncarnationID{4}
	created, err := recovery.NewCoordinator(catalogue, repository, journal, bytes.NewReader(retryID[:])).Create(ctx, domain.CatalogueRecord{Name: "work"})
	require.NoError(t, err)
	require.Equal(t, retryID, created.IncarnationID)
	require.Len(t, catalogue.Records(), 1)
	require.NoDirExists(t, repository.sessionPath(interruptedID))
	require.NoError(t, catalogue.Close())
}

func mustReadCrashFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}
