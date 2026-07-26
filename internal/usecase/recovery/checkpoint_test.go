package recovery

import (
	"context"
	"crypto/sha256"
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

func (c *checkpointCatalogue) Records() []domain.CatalogueRecord {
	return []domain.CatalogueRecord{c.record}
}
func (c *checkpointCatalogue) Record(name string) (domain.CatalogueRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.record, c.record.Name == name
}
func (c *checkpointCatalogue) Create(domain.CatalogueRecord) error { return nil }
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

func populatedGenerations(refs [2]*domain.CheckpointRef) []uint64 {
	var generations []uint64
	for _, ref := range refs {
		if ref != nil {
			generations = append(generations, ref.Generation)
		}
	}
	return generations
}

func runCheckpointCommit(t *testing.T, count int) (domain.CatalogueRecord, *checkpointCatalogue, *checkpointRepository) {
	t.Helper()
	events := make([]string, 0, count*2)
	catalogue := &checkpointCatalogue{record: checkpointRecord(), events: &events}
	repository := &checkpointRepository{events: &events}
	coordinator := NewCoordinator(catalogue, repository, nil, nil)
	for generation := uint64(1); generation <= uint64(count); generation++ {
		var err error
		catalogue.record, err = coordinator.PublishCheckpoint(context.Background(), "work", checkpointPublication(catalogue.record, generation))
		require.NoError(t, err)
	}
	for i := 0; i < count; i++ {
		require.Equal(t, []string{"repository", "catalogue"}, events[i*2:i*2+2])
	}
	return catalogue.record, catalogue, repository
}

func TestCheckpointCommitFirst(t *testing.T) {
	record, catalogue, _ := runCheckpointCommit(t, 1)
	require.Equal(t, uint64(1), record.Committed.Generation)
	require.Empty(t, populatedGenerations(record.Fallbacks))
	require.Equal(t, 1, catalogue.replaces)
}

func TestCheckpointCommitSecond(t *testing.T) {
	record, catalogue, _ := runCheckpointCommit(t, 2)
	require.Equal(t, uint64(2), record.Committed.Generation)
	require.Equal(t, []uint64{1}, populatedGenerations(record.Fallbacks))
	require.Equal(t, 2, catalogue.replaces)
}

func TestCheckpointCommitThird(t *testing.T) {
	record, catalogue, _ := runCheckpointCommit(t, 4)
	require.Equal(t, uint64(4), record.Committed.Generation)
	require.Equal(t, []uint64{3, 2}, populatedGenerations(record.Fallbacks))
	require.Equal(t, domain.RecoveryHealthy, record.RecoveryState)
	require.Empty(t, record.DegradedReason)
	require.Equal(t, 4, catalogue.replaces)
}

func TestCheckpointCommitThirdPromotesOnlyValidatedDirectFallbacks(t *testing.T) {
	refs := []domain.CheckpointRef{
		{Generation: 3, ManifestDigest: sha256.Sum256([]byte("third"))},
		{Generation: 2, ManifestDigest: sha256.Sum256([]byte("second"))},
		{Generation: 1, ManifestDigest: sha256.Sum256([]byte("first"))},
	}
	record := checkpointRecord()
	record.RecoveryState = domain.RecoveryHealthy
	record.Committed = &refs[0]
	record.Fallbacks = [2]*domain.CheckpointRef{&refs[1], &refs[2]}
	catalogue := &checkpointCatalogue{record: record}
	repository := &checkpointRepository{
		generations: map[uint64]ports.SnapshotGeneration{
			2: {IncarnationID: record.IncarnationID, Name: record.Name, Generation: 2, Manifest: []byte("second")},
			1: {IncarnationID: record.IncarnationID, Name: record.Name, Generation: 1, Manifest: []byte("first")},
		},
	}
	coordinator := NewCoordinator(catalogue, repository, nil, nil)

	promoted, err := coordinator.PromoteFallback(context.Background(), record.Name, refs[1])
	require.NoError(t, err)
	require.Equal(t, refs[1], *promoted.Record.Committed)
	require.Equal(t, []uint64{1}, populatedGenerations(promoted.Record.Fallbacks))
	require.Equal(t, []uint64{2, 1}, repository.loads, "only the selected and older catalogue-indexed fallbacks are read directly")
	require.Equal(t, []domain.CheckpointRef{refs[1]}, repository.repairs)
	require.Equal(t, 1, catalogue.replaces)
}

func TestPromoteFallbackReportsPostCommitHEADRepairFailure(t *testing.T) {
	selected := domain.CheckpointRef{Generation: 2, ManifestDigest: sha256.Sum256([]byte("second"))}
	record := checkpointRecord()
	record.RecoveryState = domain.RecoveryHealthy
	record.Committed = &domain.CheckpointRef{Generation: 3, ManifestDigest: sha256.Sum256([]byte("third"))}
	record.Fallbacks[0] = &selected
	catalogue := &checkpointCatalogue{record: record}
	repairErr := errors.New("head repair failed")
	repository := &checkpointRepository{
		generations: map[uint64]ports.SnapshotGeneration{
			2: {IncarnationID: record.IncarnationID, Name: record.Name, Generation: 2, Manifest: []byte("second")},
		},
		repairErr: repairErr,
	}
	coordinator := NewCoordinator(catalogue, repository, nil, nil)

	outcome, err := coordinator.PromoteFallback(context.Background(), record.Name, selected)
	require.NoError(t, err)
	require.True(t, outcome.CatalogueCommitted)
	require.ErrorIs(t, outcome.HEADRepairError, repairErr)
	require.Equal(t, selected, *outcome.Record.Committed)
	require.Equal(t, domain.RecoveryHealthy, catalogue.record.RecoveryState)
	require.Equal(t, selected, *catalogue.record.Committed)
}

func TestCheckpointCommitPublishOrphan(t *testing.T) {
	prior := checkpointRecord()
	priorRef := domain.CheckpointRef{Generation: 7, ManifestDigest: [32]byte{7}}
	prior.RecoveryState = domain.RecoveryHealthy
	prior.Committed = &priorRef
	cause := errors.New("catalogue sync failed")
	catalogue := &checkpointCatalogue{record: prior, replaceErr: cause}
	repository := &checkpointRepository{}
	coordinator := NewCoordinator(catalogue, repository, nil, nil)
	publication := checkpointPublication(prior, 8)

	_, err := coordinator.PublishCheckpoint(context.Background(), prior.Name, publication)
	require.ErrorIs(t, err, cause)
	require.Equal(t, prior, catalogue.record, "failed catalogue commit must preserve the authoritative record")
	require.Len(t, repository.publications, 1, "the durable publication remains a forward orphan")
	require.Equal(t, publication, repository.publications[0])
}
