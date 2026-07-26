package snapshot

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestGCBudgetDiagnostic(t *testing.T) {
	var buffer bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buffer, nil))
	repository := NewRepository(t.TempDir(), log)
	id := domain.IncarnationID{1}
	done, err := repository.MaintainSession(context.Background(), ports.RetentionPlan{
		IncarnationID: id,
		Keep:          []ports.CheckpointRef{{Generation: 3}, {Generation: 2}, {Generation: 1}},
	}, ports.MaintenanceBudget{})
	require.NoError(t, err)
	require.False(t, done)

	scanner := bufio.NewScanner(bytes.NewReader(buffer.Bytes()))
	require.True(t, scanner.Scan())
	var entry map[string]any
	require.NoError(t, json.Unmarshal(scanner.Bytes(), &entry))
	require.NoError(t, scanner.Err())
	require.Equal(t, "snapshot_maintenance_progress", entry["msg"])
	require.Equal(t, id.String(), entry["incarnation"])
	require.EqualValues(t, 3, entry["retained"])
	require.Contains(t, entry, "cursor")
	require.Equal(t, true, entry["budget_exhausted"])
}
