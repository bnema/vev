package snapshot

import (
	"context"
	"io"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/safedir"
	"github.com/stretchr/testify/require"
)

type fakeReconcileDrainDirectory struct {
	names    []string
	position int
}

func (d *fakeReconcileDrainDirectory) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekCurrent {
		return int64(d.position), nil
	}
	d.position = int(offset)
	return offset, nil
}

func (d *fakeReconcileDrainDirectory) ReadDirent(buffer []byte) (int, error) {
	if d.position >= len(d.names) {
		return 0, nil
	}
	batch := maintenanceDirentBatch(d.names[d.position:]...)
	d.position = len(d.names)
	return copy(buffer, batch), nil
}

func (d *fakeReconcileDrainDirectory) Close() error { return nil }

func configureDrainingReconcileScanner(repo *Repository, names []string) {
	repo.hooks.openMaintenanceDirectory = func(string) (maintenanceDirectory, error) {
		return &fakeReconcileDrainDirectory{names: names}, nil
	}
	repo.hooks.maintenanceDirentConfig = &maintenanceDirentConfig{
		drainBuffer: true,
		cookie: func(file maintenanceDirectory, _ *syscall.Dirent, remaining int) (int64, error) {
			end, err := file.Seek(0, io.SeekCurrent)
			return end - int64(remaining), err
		},
	}
}

func TestReconciliationDrainedBatchHasNoSkippedOrDuplicateEntries(t *testing.T) {
	repo := NewRepository(privateDir(t))
	sessions := filepath.Join(repo.dir, repositorySessionsDir)
	names := make([]string, 3)
	for i := range names {
		id := domain.IncarnationID{byte(i + 20)}
		names[i] = id.String()
		require.NoError(t, safedir.EnsurePrivate(filepath.Join(sessions, names[i])))
	}
	configureDrainingReconcileScanner(repo, names)

	cursor := ports.ReconcileCursor{}
	var got []string
	for calls := 0; ; calls++ {
		next, findings, err := repo.Reconcile(context.Background(), nil, cursor, ports.MaintenanceBudget{Entries: 2, Bytes: 1})
		require.NoError(t, err)
		for _, finding := range findings {
			got = append(got, finding.Candidate.Name)
		}
		if next.DirectoryCookie == 0 {
			break
		}
		require.Greater(t, next.DirectoryCookie, cursor.DirectoryCookie)
		cursor = next
		require.Less(t, calls, 3)
	}
	require.Equal(t, names, got)
}

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

func TestReconciliationIntrinsicallyOversizedCandidateMakesFixedBudgetProgress(t *testing.T) {
	tests := []struct {
		name   string
		budget ports.MaintenanceBudget
	}{
		{name: "entry cost", budget: ports.MaintenanceBudget{Entries: 1, Bytes: 8 << 20}},
		{name: "byte cost", budget: ports.MaintenanceBudget{Entries: 8, Bytes: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			publications := publishMaintenanceGenerations(t, repo, "work", 2)
			committed := checkpointRefForPublication(publications[0])
			record := domain.CatalogueRecord{Name: "work", IncarnationID: publications[0].IncarnationID, RecoveryState: domain.RecoveryHealthy, Committed: &committed}
			unknown := domain.IncarnationID{99}
			require.NoError(t, safedir.EnsurePrivate(filepath.Join(repo.dir, repositorySessionsDir, unknown.String())))
			configureDrainingReconcileScanner(repo, []string{record.IncarnationID.String(), unknown.String()})

			next, findings, err := repo.Reconcile(context.Background(), []domain.CatalogueRecord{record}, ports.ReconcileCursor{}, tc.budget)
			require.NoError(t, err)
			require.Equal(t, uint64(1), next.DirectoryCookie)
			require.Len(t, findings, 1)
			require.Equal(t, ports.ReconcileBudgetExhausted, findings[0].Status)
			require.LessOrEqual(t, findings[0].Consumed.Entries, tc.budget.Entries)
			require.LessOrEqual(t, findings[0].Consumed.Bytes, tc.budget.Bytes)

			later, findings, err := repo.Reconcile(context.Background(), []domain.CatalogueRecord{record}, next, tc.budget)
			require.NoError(t, err)
			require.Len(t, findings, 1)
			require.Equal(t, unknown, findings[0].Candidate.IncarnationID)
			require.Equal(t, ports.ReconcileQuarantined, findings[0].Status)
			if later.DirectoryCookie == 0 {
				return
			}
			require.Greater(t, later.DirectoryCookie, next.DirectoryCookie)

			done, findings, err := repo.Reconcile(context.Background(), []domain.CatalogueRecord{record}, later, tc.budget)
			require.NoError(t, err)
			require.Zero(t, done)
			require.Empty(t, findings, "oversized candidate must not be retried")
		})
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

	unknown := make(map[domain.IncarnationID]struct{})
	adoptable := 0
	for _, finding := range findings {
		switch finding.Kind {
		case ports.ReconcileUnknownIncarnation:
			require.Equal(t, ports.ReconcileQuarantined, finding.Status)
			_, duplicate := unknown[finding.Candidate.IncarnationID]
			require.False(t, duplicate, "unknown incarnation was returned twice")
			unknown[finding.Candidate.IncarnationID] = struct{}{}
		case ports.ReconcileForwardOrphan:
			if finding.Status == ports.ReconcileBudgetExhausted {
				continue
			}
			require.Equal(t, ports.ReconcileValidated, finding.Status)
			require.Equal(t, uint64(2), finding.Candidate.Ref.Generation)
			require.Equal(t, committed, *finding.Candidate.Parent)
			adoptable++
		}
	}
	require.Len(t, unknown, 6)
	require.Equal(t, 1, adoptable, "candidate must reach a terminal result exactly once")
}
