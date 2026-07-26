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
