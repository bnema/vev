package ports

import (
	"context"

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

type Catalogue interface {
	Records() ([]domain.CatalogueRecord, error)
	Record(string) (domain.CatalogueRecord, bool, error)
	Create(domain.CatalogueRecord) error
	UpdateMetadata(domain.CatalogueMetadataUpdate) error
	Replace(string, domain.CatalogueRecord) error
	Rename(string, domain.CatalogueRecord) error
	Delete(string) error
	// Sync makes buffered metadata durable. Identity operations sync internally.
	Sync() error
	Close() error
}

type SnapshotRepository interface {
	Publish(context.Context, SnapshotPublication) error
	LoadCheckpoint(context.Context, domain.IncarnationID, string, CheckpointRef) (SnapshotGeneration, error)
	ReconcileCheckpoint(context.Context, domain.IncarnationID, CheckpointRef) error
	DeleteIncarnation(context.Context, domain.IncarnationID) error
	// SnapshotIncarnations lists the incarnations present in the repository.
	SnapshotIncarnations(context.Context) ([]domain.IncarnationID, error)
	// CollectIncarnationGarbage applies retention to one incarnation. A nil
	// keep marks an orphan absent from a validated catalogue and removes it
	// whole; otherwise *keep is the committed checkpoint (zero when none).
	CollectIncarnationGarbage(ctx context.Context, id domain.IncarnationID, keep *domain.CheckpointRef) error
}
