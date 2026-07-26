package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestReconciliationCursor(t *testing.T) {
	repo := NewRepository(privateDir(t))
	publications := publishMaintenanceGenerations(t, repo, "work", 2)
	committed := checkpointRefForPublication(publications[0])
	record := domain.CatalogueRecord{
		Name:          "work",
		IncarnationID: publications[0].IncarnationID,
		RecoveryState: domain.RecoveryHealthy,
		Committed:     &committed,
	}

	sessions := filepath.Join(repo.dir, repositorySessionsDir)
	for i := byte(20); i < 26; i++ {
		id := domain.IncarnationID{i}
		require.NoError(t, os.Mkdir(filepath.Join(sessions, id.String()), 0o700))
	}

	cursor := ports.ReconcileCursor{}
	var findings []ports.ReconcileFinding
	calls := 0
	for {
		next, page, err := repo.Reconcile(context.Background(), []domain.CatalogueRecord{record}, cursor, ports.MaintenanceBudget{Entries: 2, Bytes: 8 << 20})
		require.NoError(t, err)
		for _, finding := range page {
			require.LessOrEqual(t, finding.Consumed.Entries, uint64(2))
			require.LessOrEqual(t, finding.Consumed.Bytes, uint64(8<<20))
		}
		findings = append(findings, page...)
		calls++
		if next.DirectoryCookie == 0 {
			break
		}
		require.NotEqual(t, cursor, next, "reconciliation cursor must advance")
		cursor = next
		require.Less(t, calls, 10)
	}
	require.Greater(t, calls, 1, "top-level traversal must resume across bounded calls")

	unknown := 0
	adoptable := 0
	for _, finding := range findings {
		switch finding.Kind {
		case ports.ReconcileUnknownIncarnation:
			unknown++
		case ports.ReconcileForwardOrphan:
			require.Equal(t, ports.ReconcileValidated, finding.Status)
			require.Equal(t, uint64(2), finding.Candidate.Ref.Generation)
			require.Equal(t, committed, *finding.Candidate.Parent)
			adoptable++
		}
	}
	require.Equal(t, 6, unknown)
	require.Equal(t, 1, adoptable)
}
