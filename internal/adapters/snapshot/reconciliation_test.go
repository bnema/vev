package snapshot

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/safedir"
	"github.com/stretchr/testify/require"
)

func TestReconciliationReadsEachCandidatePayloadOnceWithinBudget(t *testing.T) {
	repo := NewRepository(privateDir(t))
	first := publishMaintenanceGenerations(t, repo, "first", 2)
	second := publishMaintenanceGenerations(t, repo, "second", 2)

	records := make([]domain.CatalogueRecord, 0, 2)
	expectedPaths := make(map[string]struct{})
	for _, publications := range [][]ports.SnapshotPublication{first, second} {
		committed := checkpointRefForPublication(publications[0])
		records = append(records, domain.CatalogueRecord{
			Name:          publications[0].Name,
			IncarnationID: publications[0].IncarnationID,
			RecoveryState: domain.RecoveryHealthy,
			Committed:     &committed,
		})
		candidate := checkpointRefForPublication(publications[1])
		expectedPaths[repo.headPath(publications[0].IncarnationID)] = struct{}{}
		expectedPaths[repo.manifestPath(publications[0].IncarnationID, candidate.Generation)] = struct{}{}
		manifest, err := codec.UnmarshalManifest(publications[1].Manifest)
		require.NoError(t, err)
		for digest := range manifestRefs(manifest) {
			expectedPaths[repo.objectPath(publications[0].IncarnationID, digest)] = struct{}{}
		}
	}

	var budgetBytes uint64
	for path := range expectedPaths {
		info, err := repo.stat(path)
		require.NoError(t, err)
		budgetBytes += uint64(info.Size())
	}
	reads := make(map[string]int)
	var actualBytes uint64
	repo.hooks.beforePayloadRead = func(path string) {
		info, err := repo.stat(path)
		require.NoError(t, err)
		reads[path]++
		actualBytes += uint64(info.Size())
	}

	next, findings, err := repo.Reconcile(context.Background(), records, ports.ReconcileCursor{}, ports.MaintenanceBudget{Entries: 16, Bytes: budgetBytes})
	require.NoError(t, err)
	require.Zero(t, next)
	require.Len(t, findings, 2)
	var consumedBytes uint64
	for _, finding := range findings {
		require.Equal(t, ports.ReconcileForwardOrphan, finding.Kind)
		require.Equal(t, ports.ReconcileValidated, finding.Status)
		consumedBytes += finding.Consumed.Bytes
	}
	require.Equal(t, consumedBytes, actualBytes, "every payload read must be charged")
	require.LessOrEqual(t, actualBytes, budgetBytes)
	require.Equal(t, expectedPaths, func() map[string]struct{} {
		got := make(map[string]struct{}, len(reads))
		for path := range reads {
			got[path] = struct{}{}
		}
		return got
	}())
	for path, count := range reads {
		require.Equalf(t, 1, count, "payload %q was reread", path)
	}

	reads = make(map[string]int)
	actualBytes = 0
	limitedBudget := budgetBytes - 1
	_, findings, err = repo.Reconcile(context.Background(), records, ports.ReconcileCursor{}, ports.MaintenanceBudget{Entries: 16, Bytes: limitedBudget})
	require.NoError(t, err)
	consumedBytes = 0
	exhausted := false
	for _, finding := range findings {
		consumedBytes += finding.Consumed.Bytes
		exhausted = exhausted || finding.Status == ports.ReconcileBudgetExhausted
	}
	require.True(t, exhausted, "reconciliation must yield rather than read past its budget")
	require.Equal(t, consumedBytes, actualBytes)
	require.LessOrEqual(t, actualBytes, limitedBudget)
	for path, count := range reads {
		require.Equalf(t, 1, count, "payload %q was reread in a budget-limited step", path)
	}
}

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
		require.NoError(t, safedir.EnsurePrivate(filepath.Join(sessions, id.String())))
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
