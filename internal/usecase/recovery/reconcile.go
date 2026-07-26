package recovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Reconciler runs bounded repository maintenance after catalogue-driven
// restoration. Its cursor is caller-owned and can be resumed by a later pass.
type Reconciler struct {
	coordinator *Coordinator
	catalogue   ports.Catalogue
	repository  ports.SnapshotRepository
	budget      ports.MaintenanceBudget
}

func NewReconciler(coordinator *Coordinator, catalogue ports.Catalogue, repository ports.SnapshotRepository, budget ports.MaintenanceBudget) *Reconciler {
	return &Reconciler{coordinator: coordinator, catalogue: catalogue, repository: repository, budget: budget}
}

func (r *Reconciler) Step(ctx context.Context, cursor ports.ReconcileCursor) (ports.ReconcileCursor, []ports.ReconcileDecision, error) {
	if r == nil || r.coordinator == nil || r.catalogue == nil || r.repository == nil || r.budget.Entries == 0 || r.budget.Bytes == 0 {
		return cursor, nil, errors.New("recovery: incomplete reconciliation dependencies")
	}
	if err := ctx.Err(); err != nil {
		return cursor, nil, err
	}
	records, err := r.catalogue.Records()
	if err != nil {
		return cursor, nil, err
	}
	next, findings, err := r.repository.Reconcile(ctx, records, cursor, r.budget)
	if err != nil {
		return cursor, nil, err
	}
	decisions := make([]ports.ReconcileDecision, 0, len(findings))
	consumed := ports.MaintenanceBudget{}
	for _, finding := range findings {
		// Compare against the remainder before adding. This both enforces the
		// page-wide budget and avoids uint64 wraparound.
		if finding.Consumed.Entries > r.budget.Entries-consumed.Entries ||
			finding.Consumed.Bytes > r.budget.Bytes-consumed.Bytes {
			return cursor, nil, errors.New("recovery: repository exceeded reconciliation budget")
		}
		consumed.Entries += finding.Consumed.Entries
		consumed.Bytes += finding.Consumed.Bytes
		record, ok, err := r.catalogue.Record(finding.Candidate.Name)
		if err != nil {
			return cursor, nil, err
		}
		if !ok {
			record = domain.CatalogueRecord{}
		}
		decision := classifyFinding(record, finding)
		if decision.Kind == ports.ReconcileAdopt {
			if _, err := r.coordinator.PublishReconciledCheckpoint(ctx, finding.Candidate.Name, finding.Candidate, finding.AncestorChain); err != nil {
				if !errors.Is(err, ErrCheckpointConflict) && !errors.Is(err, ErrCheckpointRecordNotFound) {
					return cursor, nil, err
				}
				decision.Kind = ports.ReconcileDefer
				decision.ReasonCode = "catalogue-conflict"
				decision.RetentionResolved = false
			}
		}
		decisions = append(decisions, decision)
	}
	return next, decisions, nil
}

func classifyFinding(record domain.CatalogueRecord, finding ports.ReconcileFinding) ports.ReconcileDecision {
	decision := ports.ReconcileDecision{Name: finding.Candidate.Name}
	candidate := finding.Candidate.Ref
	decision.Candidate = &candidate
	if finding.Status == ports.ReconcileBudgetExhausted {
		decision.Kind = ports.ReconcileDefer
		decision.ReasonCode = "budget-exhausted"
		return decision
	}
	if finding.Kind == ports.ReconcileUnknownIncarnation || record.IncarnationID == (domain.IncarnationID{}) {
		decision.Kind = ports.ReconcileKeepQuarantined
		decision.ReasonCode = "unknown-incarnation"
		return decision
	}
	if finding.Candidate.IncarnationID != record.IncarnationID {
		decision.Kind = ports.ReconcileKeepQuarantined
		decision.ReasonCode = "incarnation-mismatch"
		return decision
	}
	if finding.Status == ports.ReconcileValidated && record.Committed != nil && (finding.Kind != ports.ReconcileForwardOrphan || finding.Candidate.Ref.Generation <= record.Committed.Generation) {
		decision.Kind = ports.ReconcileKeepQuarantined
		decision.ReasonCode = "not-forward"
		decision.RetentionResolved = true
		return decision
	}
	if finding.Kind != ports.ReconcileForwardOrphan || record.Committed == nil {
		decision.Kind = ports.ReconcileKeepQuarantined
		decision.ReasonCode = "invalid-candidate"
		return decision
	}
	if finding.Status != ports.ReconcileValidated || !validForwardChain(*record.Committed, finding.Candidate, finding.AncestorChain) {
		decision.Kind = ports.ReconcileKeepQuarantined
		decision.ReasonCode = "invalid-chain"
		return decision
	}
	decision.Kind = ports.ReconcileAdopt
	decision.RetentionResolved = true
	return decision
}

func validForwardChain(committed domain.CheckpointRef, candidate ports.ReconcileCandidate, ancestors []ports.ValidatedCheckpoint) bool {
	if committed.Generation == 0 || candidate.Ref.Generation <= committed.Generation || candidate.Parent == nil {
		return false
	}
	if len(ancestors) == 0 {
		return *candidate.Parent == committed
	}
	if *candidate.Parent != ancestors[0].Ref || candidate.Ref.Generation <= ancestors[0].Ref.Generation {
		return false
	}
	for i, ancestor := range ancestors {
		if ancestor.Parent == nil || ancestor.Ref.Generation <= committed.Generation {
			return false
		}
		if i+1 < len(ancestors) {
			if ancestor.Ref.Generation <= ancestors[i+1].Ref.Generation || *ancestor.Parent != ancestors[i+1].Ref {
				return false
			}
			continue
		}
		if *ancestor.Parent != committed {
			return false
		}
	}
	return true
}

// PublishReconciledCheckpoint rechecks the chain while holding the session
// transaction lock. The candidate is already the durable HEAD and its payloads
// were validated by the adapter, so publication performs no repository reread.
func (c *Coordinator) PublishReconciledCheckpoint(ctx context.Context, name string, candidate ports.ReconcileCandidate, ancestors []ports.ValidatedCheckpoint) (domain.CatalogueRecord, error) {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil {
		return domain.CatalogueRecord{}, errors.New("recovery: incomplete reconciled checkpoint dependencies")
	}
	unlock := c.locks.Lock([]string{name})
	defer unlock()
	if err := ctx.Err(); err != nil {
		return domain.CatalogueRecord{}, err
	}
	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return domain.CatalogueRecord{}, err
	}
	if !ok {
		return domain.CatalogueRecord{}, ErrCheckpointRecordNotFound
	}
	if candidate.Name != name || candidate.IncarnationID != record.IncarnationID || record.Committed == nil || !validForwardChain(*record.Committed, candidate, ancestors) {
		return domain.CatalogueRecord{}, ErrCheckpointConflict
	}
	next := shiftedCheckpoint(record, candidate.Ref)
	if err := next.Validate(); err != nil {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: invalid reconciled checkpoint transition: %w", err)
	}
	if err := c.catalogue.Replace(name, next); err != nil {
		return domain.CatalogueRecord{}, err
	}
	return next, nil
}
