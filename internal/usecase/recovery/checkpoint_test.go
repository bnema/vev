package recovery

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type checkpointCatalogue struct {
	mu         sync.Mutex
	record     domain.CatalogueRecord
	replaceErr error
	replaces   int
	events     *[]string
}

func (c *checkpointCatalogue) Records() ([]domain.CatalogueRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return []domain.CatalogueRecord{c.record}, nil
}
func (c *checkpointCatalogue) Record(name string) (domain.CatalogueRecord, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.record, c.record.Name == name, nil
}
func (c *checkpointCatalogue) Create(domain.CatalogueRecord) error { return nil }
func (c *checkpointCatalogue) UpdateMetadata(domain.CatalogueMetadataUpdate) error {
	return nil
}
func (c *checkpointCatalogue) Replace(name string, record domain.CatalogueRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replaces++
	if c.events != nil {
		*c.events = append(*c.events, "catalogue")
	}
	if c.replaceErr != nil {
		return c.replaceErr
	}
	if name != c.record.Name || record.Name != name {
		return errors.New("catalogue key mismatch")
	}
	c.record = record
	return nil
}
func (c *checkpointCatalogue) Rename(string, domain.CatalogueRecord) error { return nil }
func (c *checkpointCatalogue) Delete(string) error                         { return nil }
func (c *checkpointCatalogue) Close() error                                { return nil }

type checkpointRepository struct {
	ports.SnapshotRepository
	publications []ports.SnapshotPublication
	generations  map[uint64]ports.SnapshotGeneration
	loadErrors   map[uint64]error
	loads        []uint64
	repairs      []domain.CheckpointRef
	repairErr    error
	events       *[]string
}

func (r *checkpointRepository) Publish(_ context.Context, publication ports.SnapshotPublication) error {
	if r.events != nil {
		*r.events = append(*r.events, "repository")
	}
	r.publications = append(r.publications, publication)
	return nil
}

func (r *checkpointRepository) LoadCheckpoint(_ context.Context, _ domain.IncarnationID, _ string, ref ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	r.loads = append(r.loads, ref.Generation)
	if err := r.loadErrors[ref.Generation]; err != nil {
		return ports.SnapshotGeneration{}, err
	}
	generation, ok := r.generations[ref.Generation]
	if !ok {
		return ports.SnapshotGeneration{}, errors.New("checkpoint unavailable")
	}
	return generation, nil
}

func (r *checkpointRepository) RepairHEAD(_ context.Context, _ domain.IncarnationID, ref ports.CheckpointRef) error {
	r.repairs = append(r.repairs, ref)
	return r.repairErr
}

func checkpointRecord() domain.CatalogueRecord {
	return domain.CatalogueRecord{
		Name:          "work",
		IncarnationID: domain.IncarnationID{1},
		RecoveryState: domain.RecoveryFresh,
	}
}

func checkpointPublication(record domain.CatalogueRecord, generation uint64) ports.SnapshotPublication {
	return ports.SnapshotPublication{
		IncarnationID:    record.IncarnationID,
		Name:             record.Name,
		Generation:       generation,
		ParentCheckpoint: record.Committed,
		Manifest:         []byte{byte(generation), 0xa5},
	}
}

func runCheckpointCommit(t *testing.T, count int) (domain.CatalogueRecord, *checkpointCatalogue, *checkpointRepository) {
	t.Helper()
	events := make([]string, 0, count*2)
	catalogue := &checkpointCatalogue{record: checkpointRecord(), events: &events}
	repository := &checkpointRepository{events: &events}
	coordinator := NewCoordinator(catalogue, repository, nil)
	for generation := uint64(1); generation <= uint64(count); generation++ {
		var err error
		catalogue.record, err = coordinator.PublishCheckpoint(context.Background(), "work", checkpointPublication(catalogue.record, generation))
		require.NoError(t, err)
	}
	for i := range count {
		require.Equal(t, []string{"repository", "catalogue"}, events[i*2:i*2+2])
	}
	return catalogue.record, catalogue, repository
}

func TestCheckpointCommits(t *testing.T) {
	for _, tc := range []struct {
		name          string
		count         int
		wantCommitted uint64
	}{
		{name: "first", count: 1, wantCommitted: 1},
		{name: "second", count: 2, wantCommitted: 2},
		{name: "third", count: 3, wantCommitted: 3},
		{name: "fourth", count: 4, wantCommitted: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record, catalogue, _ := runCheckpointCommit(t, tc.count)
			require.Equal(t, tc.wantCommitted, record.Committed.Generation)
			require.Equal(t, domain.RecoveryHealthy, record.RecoveryState)
			require.Empty(t, record.DegradedReason)
			require.Equal(t, tc.count, catalogue.replaces)
		})
	}
}

func TestCheckpointCommitPublishOrphan(t *testing.T) {
	prior := checkpointRecord()
	priorRef := domain.CheckpointRef{Generation: 7, ManifestDigest: [32]byte{7}}
	prior.RecoveryState = domain.RecoveryHealthy
	prior.Committed = &priorRef
	cause := errors.New("catalogue sync failed")
	catalogue := &checkpointCatalogue{record: prior, replaceErr: cause}
	repository := &checkpointRepository{}
	coordinator := NewCoordinator(catalogue, repository, nil)
	publication := checkpointPublication(prior, 8)

	_, err := coordinator.PublishCheckpoint(context.Background(), prior.Name, publication)
	require.ErrorIs(t, err, cause)
	require.Equal(t, prior, catalogue.record, "failed catalogue commit must preserve the authoritative record")
	require.Len(t, repository.publications, 1, "the durable publication remains a forward orphan")
	require.Equal(t, publication, repository.publications[0])
}
