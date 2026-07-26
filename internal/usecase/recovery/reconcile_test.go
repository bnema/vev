package recovery

import (
	"context"
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
		{name: "not forward orphan", mutate: func(record domain.CatalogueRecord, finding *ports.ReconcileFinding) {
			finding.Kind = ports.ReconcileInvalidCandidate
		}, wantKind: ports.ReconcileKeepQuarantined, wantCause: "not-forward"},
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

	t.Run("catalogue replacement precedes head repair", func(t *testing.T) {
		record := healthyReconcileRecord()
		events := []string{}
		catalogue := &checkpointCatalogue{record: record, events: &events}
		repository := &reconcileRepository{events: &events}
		coordinator := NewCoordinator(catalogue, repository, nil, nil)
		candidate := forwardCandidate(record, 2, record.Committed)

		next, err := coordinator.PublishReconciledCheckpoint(context.Background(), record.Name, candidate, nil)
		require.NoError(t, err)
		require.Equal(t, candidate.Ref, *next.Committed)
		require.Equal(t, []string{"catalogue", "head"}, events)
	})

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
