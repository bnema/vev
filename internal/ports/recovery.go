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

type DeletionTombstoneCursor struct{ After string }

type DeletionTombstonePage struct {
	Tombstones []domain.DeletionTombstone
	Next       DeletionTombstoneCursor
	Done       bool
}

var ErrBudgetExhausted = errors.New("snapshot maintenance budget exhausted")

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
	Discard(context.Context, string) error
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
	DeleteDeletionTombstone(context.Context, domain.IncarnationID) error
	DeleteIncarnation(context.Context, domain.IncarnationID) error
	MaintainSession(context.Context, RetentionPlan, MaintenanceBudget) (bool, error)
}
