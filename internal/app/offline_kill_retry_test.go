package app

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bnema/vev/internal/persist"
	"github.com/stretchr/testify/require"
)

type offlineSnapshotFake struct {
	calls              []string
	deleteErr          error
	deleteLegacyErr    error
	tombstoneErr       error
	deleteTombstoneErr error
}

func (f *offlineSnapshotFake) Tombstone(context.Context, string) error {
	f.calls = append(f.calls, "tombstone")
	return f.tombstoneErr
}

func (f *offlineSnapshotFake) Delete(context.Context, string) error {
	f.calls = append(f.calls, "incremental")
	return f.deleteErr
}

func (f *offlineSnapshotFake) DeleteLegacy(context.Context, string) error {
	f.calls = append(f.calls, "legacy")
	return f.deleteLegacyErr
}

func (f *offlineSnapshotFake) DeleteTombstone(context.Context, string) error {
	f.calls = append(f.calls, "delete tombstone")
	return f.deleteTombstoneErr
}

func TestRunKillOfflineRetainsTombstoneAndMetadataUntilBothSourcesAreDeleted(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)

	p, err := persist.Open(filepath.Join(stateRoot, "vev"))
	require.NoError(t, err)
	now := time.Now().UnixNano()
	require.NoError(t, p.Save(persist.Record{Name: "named", Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now}))
	require.NoError(t, p.Close())

	injected := errors.New("legacy unlink failed")
	fake := &offlineSnapshotFake{deleteLegacyErr: injected}
	originalFactory := newOfflineSnapshotRepository
	newOfflineSnapshotRepository = func(string) offlineSnapshotSource { return fake }
	t.Cleanup(func() { newOfflineSnapshotRepository = originalFactory })

	err = runKill(context.Background(), "named", false, false)
	require.ErrorIs(t, err, injected)
	require.Equal(t, []string{"tombstone", "incremental", "legacy"}, fake.calls)
	records, err := persist.LoadReadOnly(filepath.Join(stateRoot, "vev"))
	require.NoError(t, err)
	require.Len(t, records, 1, "a failed source delete must retain retry identity")

	fake.deleteLegacyErr = nil
	require.NoError(t, runKill(context.Background(), "named", false, false))
	require.Equal(t, []string{"tombstone", "incremental", "legacy", "tombstone", "incremental", "legacy", "delete tombstone"}, fake.calls)
	records, err = persist.LoadReadOnly(filepath.Join(stateRoot, "vev"))
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestRunKillOfflineRetriesAfterIncrementalDeleteFailure(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)

	p, err := persist.Open(filepath.Join(stateRoot, "vev"))
	require.NoError(t, err)
	now := time.Now().UnixNano()
	require.NoError(t, p.Save(persist.Record{Name: "named", Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now}))
	require.NoError(t, p.Close())

	injected := errors.New("incremental quarantine failed")
	fake := &offlineSnapshotFake{deleteErr: injected}
	originalFactory := newOfflineSnapshotRepository
	newOfflineSnapshotRepository = func(string) offlineSnapshotSource { return fake }
	t.Cleanup(func() { newOfflineSnapshotRepository = originalFactory })

	require.ErrorIs(t, runKill(context.Background(), "named", false, false), injected)
	require.Equal(t, []string{"tombstone", "incremental", "legacy"}, fake.calls)

	fake.deleteErr = nil
	require.NoError(t, runKill(context.Background(), "named", false, false))
	require.Equal(t, []string{"tombstone", "incremental", "legacy", "tombstone", "incremental", "legacy", "delete tombstone"}, fake.calls)
	records, err := persist.LoadReadOnly(filepath.Join(stateRoot, "vev"))
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestRunKillOfflineReportsBothSnapshotDeletionFailures(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)

	p, err := persist.Open(filepath.Join(stateRoot, "vev"))
	require.NoError(t, err)
	now := time.Now().UnixNano()
	require.NoError(t, p.Save(persist.Record{Name: "named", Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now}))
	require.NoError(t, p.Close())

	incrementalErr := errors.New("incremental quarantine failed")
	legacyErr := errors.New("legacy unlink failed")
	fake := &offlineSnapshotFake{deleteErr: incrementalErr, deleteLegacyErr: legacyErr}
	originalFactory := newOfflineSnapshotRepository
	newOfflineSnapshotRepository = func(string) offlineSnapshotSource { return fake }
	t.Cleanup(func() { newOfflineSnapshotRepository = originalFactory })

	err = runKill(context.Background(), "named", false, false)
	require.ErrorIs(t, err, incrementalErr)
	require.ErrorIs(t, err, legacyErr)
	require.Equal(t, []string{"tombstone", "incremental", "legacy"}, fake.calls)
}
