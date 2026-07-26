package ports

import (
	"context"
	"errors"

	"github.com/bnema/vev/internal/domain"
)

type CheckpointRef = domain.CheckpointRef

type SnapshotPublication struct {
	IncarnationID    domain.IncarnationID
	Name             string
	Generation       uint64
	ParentCheckpoint *domain.CheckpointRef
	Manifest         []byte
	Objects          []SnapshotObject
}

type SnapshotGeneration struct {
	IncarnationID    domain.IncarnationID
	Name             string
	Generation       uint64
	ParentCheckpoint *domain.CheckpointRef
	Manifest         []byte
	Objects          map[SnapshotDigest][]byte
}

type RetentionPlan struct {
	IncarnationID domain.IncarnationID
	Keep          []CheckpointRef
	PinAll        bool
}

type MaintenanceBudget struct {
	Entries uint64
	Bytes   uint64
}

type ReconcileCursor struct{ DirectoryCookie uint64 }

type DeletionTombstoneCursor struct{ After string }

type DeletionTombstonePage struct {
	Tombstones []domain.DeletionTombstone
	Next       DeletionTombstoneCursor
	Done       bool
}

type ReconcileFindingKind uint8

const (
	ReconcileForwardOrphan ReconcileFindingKind = iota + 1
	ReconcileUnknownIncarnation
	ReconcileInvalidCandidate
)

type ReconcileValidationStatus uint8

const (
	ReconcileValidated ReconcileValidationStatus = iota + 1
	ReconcileQuarantined
	ReconcileBudgetExhausted
)

type ReconcileCandidate struct {
	IncarnationID domain.IncarnationID
	Name          string
	Ref           domain.CheckpointRef
	Parent        *domain.CheckpointRef
}

type ValidatedCheckpoint struct {
	Ref    domain.CheckpointRef
	Parent *domain.CheckpointRef
}

type ReconcileFinding struct {
	Kind          ReconcileFindingKind
	Status        ReconcileValidationStatus
	Candidate     ReconcileCandidate
	AncestorChain []ValidatedCheckpoint
	Cursor        ReconcileCursor
	Consumed      MaintenanceBudget
}

type ReconcileDecisionKind uint8

const (
	ReconcileAdopt ReconcileDecisionKind = iota + 1
	ReconcileKeepQuarantined
	ReconcileDefer
)

type ReconcileDecision struct {
	Kind       ReconcileDecisionKind
	Name       string
	Candidate  *domain.CheckpointRef
	ReasonCode string
	// RetentionResolved permits collection after a conclusive adoption or
	// not-forward decision. Its zero value conservatively pins known sessions.
	RetentionResolved bool
}

var ErrBudgetExhausted = errors.New("snapshot maintenance budget exhausted")

type RecoveryJournal interface {
	SaveDiscard(context.Context, domain.DiscardIntent) error
	ListDiscards(context.Context) ([]domain.DiscardIntent, error)
	DeleteDiscard(context.Context, domain.IncarnationID) error
}

type CheckpointCoordinator interface {
	PublishCheckpoint(context.Context, string, SnapshotPublication) (domain.CatalogueRecord, error)
}

// SessionLifecycleCoordinator owns durable named-session create, rename, and
// delete commit protocols. Runtime registries update only after these calls
// complete successfully.
type SessionLifecycleCoordinator interface {
	CheckpointCoordinator
	Create(context.Context, domain.CatalogueRecord) (domain.CatalogueRecord, error)
	Rename(context.Context, string, string) (domain.CatalogueRecord, error)
	Delete(context.Context, string) error
}

// DegradedRecoveryCoordinator exposes only the explicit operator discard action.
type DegradedRecoveryCoordinator interface {
	Discard(context.Context, string, string) (domain.CatalogueRecord, error)
}

type Catalogue interface {
	Records() ([]domain.CatalogueRecord, error)
	Record(string) (domain.CatalogueRecord, bool, error)
	Create(domain.CatalogueRecord) error
	UpdateMetadata(domain.CatalogueMetadataUpdate) error
	Replace(string, domain.CatalogueRecord) error
	Rename(string, domain.CatalogueRecord) error
	Delete(string) error
	Close() error
}

type SnapshotRepository interface {
	Publish(context.Context, SnapshotPublication) error
	LoadCheckpoint(context.Context, domain.IncarnationID, string, CheckpointRef) (SnapshotGeneration, error)
	RepairHEAD(context.Context, domain.IncarnationID, CheckpointRef) error
	WriteDeletionTombstone(context.Context, domain.DeletionTombstone) error
	ListDeletionTombstones(context.Context, DeletionTombstoneCursor, MaintenanceBudget) (DeletionTombstonePage, error)
	QuarantineDeletionSources(context.Context, domain.DeletionTombstone, bool) error
	DeleteDeletionTombstone(context.Context, domain.IncarnationID) error
	SaveQuarantineDescriptor(context.Context, domain.QuarantineDescriptor) error
	QuarantineIncarnation(context.Context, domain.IncarnationID) error
	DeleteIncarnation(context.Context, domain.IncarnationID) error
	MaintainSession(context.Context, RetentionPlan, MaintenanceBudget) (bool, error)
	Reconcile(context.Context, []domain.CatalogueRecord, ReconcileCursor, MaintenanceBudget) (ReconcileCursor, []ReconcileFinding, error)
}
