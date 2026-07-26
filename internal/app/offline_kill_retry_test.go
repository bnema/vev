package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
)

func TestRunKillOfflineUsesIncarnationDeletionProtocol(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)

	id := domain.IncarnationID{1}
	p := newTestPersister(t, filepath.Join(stateRoot, "vev"))
	now := time.Now().UnixNano()
	require.NoError(t, p.Save(persist.Record{Name: "named", IncarnationID: id, Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now, RecoveryState: domain.RecoveryFresh}))
	require.NoError(t, p.Close())
	source := filepath.Join(stateRoot, "vev", "snapshots", "sessions", id.String())
	require.NoError(t, os.MkdirAll(source, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(source, "sentinel"), []byte("durable"), 0o600))

	require.NoError(t, runKill(context.Background(), "named", false, false))
	require.NoFileExists(t, filepath.Join(source, "sentinel"))
	require.FileExists(t, filepath.Join(stateRoot, "vev", "snapshots", "quarantine", id.String(), "snapshot", "sentinel"))
}

// TestRunKillOfflineFindsSessionWhenOnlyPrevCandidateExists covers the same
// bug class as persist.LoadCatalogueReadOnly's fresh-install regression: a
// crash mid-compaction can leave sessions.kv absent with only a .prev (or
// .next) candidate on disk. `vev kill <name>` must still find and delete the
// session rather than reporting "no such session" from a bare os.Stat
// precheck on the fixed sessions.kv path.
func TestRunKillOfflineFindsSessionWhenOnlyPrevCandidateExists(t *testing.T) {
	stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateRoot)
	t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)

	id := domain.IncarnationID{1}
	p := newTestPersister(t, filepath.Join(stateRoot, "vev"))
	now := time.Now().UnixNano()
	require.NoError(t, p.Save(persist.Record{Name: "crashed", IncarnationID: id, Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now, RecoveryState: domain.RecoveryFresh}))
	require.NoError(t, p.Close())
	source := filepath.Join(stateRoot, "vev", "snapshots", "sessions", id.String())
	require.NoError(t, os.MkdirAll(source, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(source, "sentinel"), []byte("durable"), 0o600))

	// Simulate a crash mid-compaction: current renamed away to .prev, no
	// sessions.kv on disk at all.
	storePath := persist.StorePath(filepath.Join(stateRoot, "vev"))
	require.NoError(t, os.Rename(storePath, storePath+".prev"))

	require.NoError(t, runKill(context.Background(), "crashed", false, false))
	require.NoFileExists(t, filepath.Join(source, "sentinel"))
	require.FileExists(t, filepath.Join(stateRoot, "vev", "snapshots", "quarantine", id.String(), "snapshot", "sentinel"))
}
