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
	for _, tc := range []struct {
		name            string
		arrange         func(*testing.T, *Repository) ports.RetentionPlan
		budget          ports.MaintenanceBudget
		wantDone        bool
		wantErr         error
		wantExhausted   bool
		wantConsumption bool
	}{
		{
			name: "exhausted",
			arrange: func(_ *testing.T, _ *Repository) ports.RetentionPlan {
				return ports.RetentionPlan{IncarnationID: domain.IncarnationID{1}, Keep: []ports.CheckpointRef{{Generation: 3}, {Generation: 2}, {Generation: 1}}}
			},
			wantErr: ErrMaintenanceBudgetTooSmall,
		},
		{
			name: "sufficient",
			arrange: func(t *testing.T, repository *Repository) ports.RetentionPlan {
				publication := publishMaintenanceGenerations(t, repository, "diagnostic", 1)[0]
				return ports.RetentionPlan{IncarnationID: publication.IncarnationID}
			},
			budget:          ports.MaintenanceBudget{Entries: 64, Bytes: 8 << 20},
			wantDone:        true,
			wantConsumption: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buffer bytes.Buffer
			log := slog.New(slog.NewJSONHandler(&buffer, nil))
			repository := NewRepository(privateDir(t), log)
			plan := tc.arrange(t, repository)
			buffer.Reset()

			done, err := repository.MaintainSession(context.Background(), plan, tc.budget)
			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
			require.Equal(t, tc.wantDone, done)

			scanner := bufio.NewScanner(bytes.NewReader(buffer.Bytes()))
			require.True(t, scanner.Scan())
			var entry map[string]any
			require.NoError(t, json.Unmarshal(scanner.Bytes(), &entry))
			require.NoError(t, scanner.Err())
			require.Equal(t, "snapshot_maintenance_progress", entry["msg"])
			require.Equal(t, plan.IncarnationID.String(), entry["incarnation"])
			require.Contains(t, entry, "cursor")
			require.Equal(t, tc.wantExhausted, entry["budget_exhausted"])
			consumedEntries, entriesOK := entry["consumed_entries"].(float64)
			consumedBytes, bytesOK := entry["consumed_bytes"].(float64)
			require.True(t, entriesOK)
			require.True(t, bytesOK)
			if tc.wantConsumption {
				require.Positive(t, consumedEntries)
				require.Positive(t, consumedBytes)
			} else {
				require.Zero(t, consumedEntries)
				require.Zero(t, consumedBytes)
			}
		})
	}
}
