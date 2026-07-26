package recovery

import (
	"context"
	"fmt"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type reconcileRepository struct {
	ports.SnapshotRepository
	pages       map[uint64]reconcilePage
	seenCursors []ports.ReconcileCursor
	seenBudgets []ports.MaintenanceBudget
	repairs     []domain.CheckpointRef
	events      *[]string
}

type reconcilePage struct {
	next     ports.ReconcileCursor
	findings []ports.ReconcileFinding
	err      error
}

type reconcileConflictCatalogue struct {
	*transactionCatalogue
	stale domain.CatalogueRecord
	reads int
}

func (c *reconcileConflictCatalogue) Records() ([]domain.CatalogueRecord, error) {
	records, err := c.transactionCatalogue.Records()
	if err != nil {
		return nil, err
	}
	for i := range records {
		if records[i].Name == c.stale.Name {
			records[i] = c.stale
		}
	}
	return records, nil
}

func (c *reconcileConflictCatalogue) Record(name string) (domain.CatalogueRecord, bool, error) {
	if name == c.stale.Name && c.reads == 0 {
		c.reads++
		return c.stale, true, nil
	}
	return c.transactionCatalogue.Record(name)
}

func (r *reconcileRepository) Reconcile(_ context.Context, _ []domain.CatalogueRecord, cursor ports.ReconcileCursor, budget ports.MaintenanceBudget) (ports.ReconcileCursor, []ports.ReconcileFinding, error) {
	r.seenCursors = append(r.seenCursors, cursor)
	r.seenBudgets = append(r.seenBudgets, budget)
	page := r.pages[cursor.DirectoryCookie]
	return page.next, page.findings, page.err
}

func (r *reconcileRepository) RepairHEAD(_ context.Context, _ domain.IncarnationID, ref ports.CheckpointRef) error {
	if r.events != nil {
		*r.events = append(*r.events, "head")
	}
	r.repairs = append(r.repairs, ref)
	return nil
}

func TestReconciliationCursor(t *testing.T) {
	record := healthyReconcileRecord()
	catalogue := &checkpointCatalogue{record: record}
	candidate := forwardCandidate(record, 2, record.Committed)
	repository := &reconcileRepository{pages: map[uint64]reconcilePage{
		0:  {next: ports.ReconcileCursor{DirectoryCookie: 12}, findings: []ports.ReconcileFinding{{Status: ports.ReconcileBudgetExhausted, Cursor: ports.ReconcileCursor{DirectoryCookie: 12}, Consumed: ports.MaintenanceBudget{Entries: 1, Bytes: 16}}}},
		12: {next: ports.ReconcileCursor{DirectoryCookie: 24}, findings: []ports.ReconcileFinding{{Kind: ports.ReconcileForwardOrphan, Status: ports.ReconcileValidated, Candidate: candidate, Cursor: ports.ReconcileCursor{DirectoryCookie: 24}, Consumed: ports.MaintenanceBudget{Entries: 2, Bytes: 32}}}},
	}}
	coordinator := NewCoordinator(catalogue, repository, nil, nil)
	reconciler := &Reconciler{coordinator: coordinator, catalogue: catalogue, repository: repository, budget: ports.MaintenanceBudget{Entries: 2, Bytes: 32}}

	next, decisions, err := reconciler.Step(context.Background(), ports.ReconcileCursor{})
	require.NoError(t, err)
	require.Equal(t, uint64(12), next.DirectoryCookie)
	require.Equal(t, ports.ReconcileDefer, decisions[0].Kind)
	require.Equal(t, "budget-exhausted", decisions[0].ReasonCode)

	next, decisions, err = reconciler.Step(context.Background(), next)
	require.NoError(t, err)
	require.Equal(t, uint64(24), next.DirectoryCookie)
	require.Equal(t, ports.ReconcileAdopt, decisions[0].Kind)
	require.Equal(t, []ports.ReconcileCursor{{}, {DirectoryCookie: 12}}, repository.seenCursors)
	require.Equal(t, []ports.MaintenanceBudget{{Entries: 2, Bytes: 32}, {Entries: 2, Bytes: 32}}, repository.seenBudgets)
}

func TestReconciliationRejectsCumulativeAndOverflowingBudgets(t *testing.T) {
	record := healthyReconcileRecord()
	finding := ports.ReconcileFinding{Status: ports.ReconcileBudgetExhausted, Candidate: ports.ReconcileCandidate{Name: record.Name}}
	for _, tc := range []struct {
		name     string
		budget   ports.MaintenanceBudget
		consumed []ports.MaintenanceBudget
	}{
		{name: "cumulative entries", budget: ports.MaintenanceBudget{Entries: 3, Bytes: 10}, consumed: []ports.MaintenanceBudget{{Entries: 2, Bytes: 1}, {Entries: 2, Bytes: 1}}},
		{name: "cumulative bytes", budget: ports.MaintenanceBudget{Entries: 10, Bytes: 3}, consumed: []ports.MaintenanceBudget{{Entries: 1, Bytes: 2}, {Entries: 1, Bytes: 2}}},
		{name: "overflow safe", budget: ports.MaintenanceBudget{Entries: ^uint64(0), Bytes: ^uint64(0)}, consumed: []ports.MaintenanceBudget{{Entries: ^uint64(0), Bytes: ^uint64(0)}, {Entries: 1, Bytes: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			findings := make([]ports.ReconcileFinding, len(tc.consumed))
			for i, usage := range tc.consumed {
				findings[i] = finding
				findings[i].Consumed = usage
			}
			repository := &reconcileRepository{pages: map[uint64]reconcilePage{0: {findings: findings}}}
			catalogue := &checkpointCatalogue{record: record}
			reconciler := NewReconciler(NewCoordinator(catalogue, repository, nil, nil), catalogue, repository, tc.budget)
			_, _, err := reconciler.Step(context.Background(), ports.ReconcileCursor{})
			require.EqualError(t, err, "recovery: repository exceeded reconciliation budget")
		})
	}
}

func TestReconciliationDefersConflictAndContinues(t *testing.T) {
	first := healthyReconcileRecord()
	first.Name = "first"
	staleRecord := first
	stale := forwardCandidate(staleRecord, 2, staleRecord.Committed)
	first = shiftedCheckpoint(first, domain.CheckpointRef{Generation: 2, ManifestDigest: [32]byte{9}})
	second := healthyReconcileRecord()
	second.Name = "second"
	adoptable := forwardCandidate(second, 2, second.Committed)
	catalogue := &reconcileConflictCatalogue{transactionCatalogue: newTransactionCatalogue(first, second), stale: staleRecord}
	repository := &reconcileRepository{pages: map[uint64]reconcilePage{0: {findings: []ports.ReconcileFinding{
		{Kind: ports.ReconcileForwardOrphan, Status: ports.ReconcileValidated, Candidate: stale, Consumed: ports.MaintenanceBudget{Entries: 1, Bytes: 1}},
		{Kind: ports.ReconcileForwardOrphan, Status: ports.ReconcileValidated, Candidate: adoptable, Consumed: ports.MaintenanceBudget{Entries: 1, Bytes: 1}},
	}}}}
	reconciler := NewReconciler(NewCoordinator(catalogue, repository, nil, nil), catalogue, repository, ports.MaintenanceBudget{Entries: 2, Bytes: 2})

	_, decisions, err := reconciler.Step(context.Background(), ports.ReconcileCursor{})
	require.NoError(t, err)
	require.Equal(t, []ports.ReconcileDecisionKind{ports.ReconcileDefer, ports.ReconcileAdopt}, []ports.ReconcileDecisionKind{decisions[0].Kind, decisions[1].Kind})
	require.Equal(t, "catalogue-conflict", decisions[0].ReasonCode)
	require.Equal(t, adoptable.Ref, *catalogue.records[second.Name].Committed)
}

func TestForwardOrphanAdoption(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(domain.CatalogueRecord, *ports.ReconcileFinding)
		wantKind  ports.ReconcileDecisionKind
		wantCause string
	}{
		{name: "direct child", wantKind: ports.ReconcileAdopt},
		{name: "two parent chain", mutate: addReconcileAncestor, wantKind: ports.ReconcileAdopt},
		{name: "wrong incarnation", mutate: func(record domain.CatalogueRecord, finding *ports.ReconcileFinding) {
			finding.Candidate.IncarnationID = domain.IncarnationID{9}
		}, wantKind: ports.ReconcileKeepQuarantined, wantCause: "incarnation-mismatch"},
		{name: "wrong digest", mutate: func(record domain.CatalogueRecord, finding *ports.ReconcileFinding) {
			finding.Candidate.Parent.ManifestDigest[0]++
		}, wantKind: ports.ReconcileKeepQuarantined, wantCause: "invalid-chain"},
		{name: "missing link", mutate: func(record domain.CatalogueRecord, finding *ports.ReconcileFinding) { finding.Candidate.Parent = nil }, wantKind: ports.ReconcileKeepQuarantined, wantCause: "invalid-chain"},
		{name: "generation reversal", mutate: func(record domain.CatalogueRecord, finding *ports.ReconcileFinding) {
			finding.Candidate.Ref.Generation = record.Committed.Generation
		}, wantKind: ports.ReconcileKeepQuarantined, wantCause: "not-forward"},
		{name: "non-orphan invalid candidate", mutate: func(record domain.CatalogueRecord, finding *ports.ReconcileFinding) {
			finding.Kind = ports.ReconcileInvalidCandidate
		}, wantKind: ports.ReconcileKeepQuarantined, wantCause: "invalid-candidate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := healthyReconcileRecord()
			finding := ports.ReconcileFinding{Kind: ports.ReconcileForwardOrphan, Status: ports.ReconcileValidated, Candidate: forwardCandidate(record, 2, record.Committed)}
			if tc.mutate != nil {
				tc.mutate(record, &finding)
			}
			decision := classifyFinding(record, finding)
			require.Equal(t, tc.wantKind, decision.Kind)
			require.Equal(t, tc.wantCause, decision.ReasonCode)
		})
	}

	t.Run("validated head publishes without repository reread", func(t *testing.T) {
		record := healthyReconcileRecord()
		events := []string{}
		catalogue := &checkpointCatalogue{record: record, events: &events}
		repository := &reconcileRepository{events: &events}
		coordinator := NewCoordinator(catalogue, repository, nil, nil)
		candidate := forwardCandidate(record, 2, record.Committed)

		next, err := coordinator.PublishReconciledCheckpoint(context.Background(), record.Name, candidate, nil)
		require.NoError(t, err)
		require.Equal(t, candidate.Ref, *next.Committed)
		require.Equal(t, []string{"catalogue"}, events)
		require.Empty(t, repository.repairs)
	})

	for _, state := range []domain.RecoveryState{domain.RecoveryDegraded, domain.RecoveryDeleting} {
		t.Run(fmt.Sprintf("non-healthy record %d is not reconciled", state), func(t *testing.T) {
			record := healthyReconcileRecord()
			record.RecoveryState = state
			if state == domain.RecoveryDegraded {
				record.DegradedReason = "uncertain"
			} else {
				record.Committed = nil
			}
			candidate := forwardCandidate(healthyReconcileRecord(), 2, healthyReconcileRecord().Committed)
			coordinator := NewCoordinator(&checkpointCatalogue{record: record}, &reconcileRepository{}, nil, nil)

			_, err := coordinator.PublishReconciledCheckpoint(context.Background(), record.Name, candidate, nil)
			require.ErrorIs(t, err, ErrCheckpointConflict)
		})
	}

	t.Run("stale decision after concurrent commit", func(t *testing.T) {
		record := healthyReconcileRecord()
		candidate := forwardCandidate(record, 2, record.Committed)
		concurrent := domain.CheckpointRef{Generation: 2, ManifestDigest: [32]byte{7}}
		record = shiftedCheckpoint(record, concurrent)
		catalogue := &checkpointCatalogue{record: record}
		repository := &reconcileRepository{}
		coordinator := NewCoordinator(catalogue, repository, nil, nil)

		_, err := coordinator.PublishReconciledCheckpoint(context.Background(), record.Name, candidate, nil)
		require.ErrorIs(t, err, ErrCheckpointConflict)
		require.Empty(t, repository.repairs)
	})

	t.Run("unknown identity remains quarantined", func(t *testing.T) {
		finding := ports.ReconcileFinding{Kind: ports.ReconcileUnknownIncarnation, Status: ports.ReconcileQuarantined, Candidate: ports.ReconcileCandidate{Name: "unknown", IncarnationID: domain.IncarnationID{9}}}
		decision := classifyFinding(domain.CatalogueRecord{}, finding)
		require.Equal(t, ports.ReconcileKeepQuarantined, decision.Kind)
		require.Equal(t, "unknown-incarnation", decision.ReasonCode)
	})
}

func healthyReconcileRecord() domain.CatalogueRecord {
	ref := domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}
	return domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{1}, RecoveryState: domain.RecoveryHealthy, Committed: &ref}
}

func forwardCandidate(record domain.CatalogueRecord, generation uint64, parent *domain.CheckpointRef) ports.ReconcileCandidate {
	parentCopy := *parent
	return ports.ReconcileCandidate{IncarnationID: record.IncarnationID, Name: record.Name, Ref: domain.CheckpointRef{Generation: generation, ManifestDigest: [32]byte{byte(generation)}}, Parent: &parentCopy}
}

func addReconcileAncestor(record domain.CatalogueRecord, finding *ports.ReconcileFinding) {
	ancestor := ports.ValidatedCheckpoint{Ref: domain.CheckpointRef{Generation: 2, ManifestDigest: [32]byte{2}}, Parent: record.Committed}
	finding.Candidate = forwardCandidate(record, 3, &ancestor.Ref)
	finding.AncestorChain = []ports.ValidatedCheckpoint{ancestor}
}
