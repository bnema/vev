package app

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
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

func TestRunKillOfflineRetriesSnapshotDeletion(t *testing.T) {
	for _, tt := range []struct {
		name           string
		incrementalErr error
		legacyErr      error
	}{
		{name: "legacy failure", legacyErr: errors.New("legacy unlink failed")},
		{name: "incremental failure", incrementalErr: errors.New("incremental quarantine failed")},
		{name: "both failures", incrementalErr: errors.New("incremental quarantine failed"), legacyErr: errors.New("legacy unlink failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
			t.Setenv("XDG_STATE_HOME", stateRoot)
			t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)

			p := newTestPersister(t, filepath.Join(stateRoot, "vev"))
			now := time.Now().UnixNano()
			require.NoError(t, p.Save(persist.Record{Name: "named", IncarnationID: domain.IncarnationID{1}, Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now, RecoveryState: domain.RecoveryFresh}))
			require.NoError(t, p.Close())

			fake := &offlineSnapshotFake{deleteErr: tt.incrementalErr, deleteLegacyErr: tt.legacyErr}
			originalFactory := newOfflineSnapshotRepository
			newOfflineSnapshotRepository = func(string) offlineSnapshotSource { return fake }
			t.Cleanup(func() { newOfflineSnapshotRepository = originalFactory })

			err := runKill(context.Background(), "named", false, false)
			if tt.incrementalErr != nil {
				require.ErrorIs(t, err, tt.incrementalErr)
			}
			if tt.legacyErr != nil {
				require.ErrorIs(t, err, tt.legacyErr)
			}
			require.Equal(t, []string{"tombstone", "incremental", "legacy"}, fake.calls)
			records, err := persist.LoadReadOnly(filepath.Join(stateRoot, "vev"))
			require.NoError(t, err)
			require.Len(t, records, 1, "a failed source delete must retain retry identity")

			fake.deleteErr, fake.deleteLegacyErr = nil, nil
			require.NoError(t, runKill(context.Background(), "named", false, false))
			require.Equal(t, []string{"tombstone", "incremental", "legacy", "tombstone", "incremental", "legacy", "delete tombstone"}, fake.calls)
			records, err = persist.LoadReadOnly(filepath.Join(stateRoot, "vev"))
			require.NoError(t, err)
			require.Empty(t, records)
		})
	}
}
