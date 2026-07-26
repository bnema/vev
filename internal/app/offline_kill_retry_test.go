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
	tests := []struct {
		name      string
		crashHook func(*testing.T, string)
	}{
		{name: "normal"},
		{
			name: ".prev crash hook",
			crashHook: func(t *testing.T, stateDir string) {
				t.Helper()
				storePath := persist.StorePath(stateDir)
				require.NoError(t, os.Rename(storePath, storePath+".prev"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateRoot, runtimeRoot := t.TempDir(), t.TempDir()
			t.Setenv("XDG_STATE_HOME", stateRoot)
			t.Setenv("XDG_RUNTIME_DIR", runtimeRoot)
			stateDir := filepath.Join(stateRoot, "vev")

			id := domain.IncarnationID{1}
			p := newTestPersister(t, stateDir)
			now := time.Now().UnixNano()
			require.NoError(t, p.Save(persist.Record{Name: "named", IncarnationID: id, Cwd: t.TempDir(), CreatedAt: now, UpdatedAt: now, RecoveryState: domain.RecoveryFresh}))
			require.NoError(t, p.Close())
			source := filepath.Join(stateDir, "snapshots", "sessions", id.String())
			require.NoError(t, os.MkdirAll(source, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(source, "sentinel"), []byte("durable"), 0o600))
			if tt.crashHook != nil {
				tt.crashHook(t, stateDir)
			}

			require.NoError(t, runKill(context.Background(), "named", false, false))
			require.NoFileExists(t, filepath.Join(source, "sentinel"))
			require.FileExists(t, filepath.Join(stateDir, "snapshots", "quarantine", id.String(), "snapshot", "sentinel"))
		})
	}
}
