package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

type checkpointCatalogueState struct {
	mu         sync.Mutex
	record     domain.CatalogueRecord
	replaceErr error
	replaces   int
	events     *[]string
}

type checkpointRepositoryState struct {
	publications []ports.SnapshotPublication
	generations  map[uint64]ports.SnapshotGeneration
	loadErrors   map[uint64]error
	loads        []uint64
	repairs      []domain.CheckpointRef
	repairErr    error
	events       *[]string
}

func checkpointRecord() domain.CatalogueRecord {
	return domain.CatalogueRecord{
		Name:          "work",
		IncarnationID: domain.IncarnationID{1},
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

func runCheckpointCommit(t *testing.T, count int) (domain.CatalogueRecord, *checkpointCatalogueState, *checkpointRepositoryState) {
	t.Helper()
	events := make([]string, 0, count*2)
	catalogue := &checkpointCatalogueState{record: checkpointRecord(), events: &events}
	repository := &checkpointRepositoryState{events: &events}
	coordinator := testCoordinator(t, catalogue, repository, nil)
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
			require.Empty(t, record.DegradedReason)
			require.Equal(t, tc.count, catalogue.replaces)
		})
	}
}

func TestCheckpointRejectsNonIncreasingGeneration(t *testing.T) {
	prior := checkpointRecord()
	priorRef := domain.CheckpointRef{Generation: 7, ManifestDigest: [32]byte{7}}
	prior.Committed = &priorRef

	for _, generation := range []uint64{7, 6} {
		t.Run(fmt.Sprintf("generation-%d", generation), func(t *testing.T) {
			catalogue := &checkpointCatalogueState{record: prior}
			repository := &checkpointRepositoryState{}
			coordinator := testCoordinator(t, catalogue, repository, nil)

			_, err := coordinator.PublishCheckpoint(context.Background(), prior.Name, checkpointPublication(prior, generation))

			require.ErrorIs(t, err, ErrCheckpointConflict)
			require.Empty(t, repository.publications)
			require.Zero(t, catalogue.replaces)
		})
	}
}

func TestCheckpointCommitPublishOrphan(t *testing.T) {
	prior := checkpointRecord()
	priorRef := domain.CheckpointRef{Generation: 7, ManifestDigest: [32]byte{7}}
	prior.Committed = &priorRef
	cause := errors.New("catalogue sync failed")
	catalogue := &checkpointCatalogueState{record: prior, replaceErr: cause}
	repository := &checkpointRepositoryState{}
	coordinator := testCoordinator(t, catalogue, repository, nil)
	publication := checkpointPublication(prior, 8)

	_, err := coordinator.PublishCheckpoint(context.Background(), prior.Name, publication)
	require.ErrorIs(t, err, cause)
	require.Equal(t, prior, catalogue.record, "failed catalogue commit must preserve the authoritative record")
	require.Len(t, repository.publications, 1, "the durable publication remains a forward orphan")
	require.Equal(t, publication, repository.publications[0])
}

func newCheckpointCatalogueStateMock(t *testing.T, c *checkpointCatalogueState) *portsmocks.MockCatalogue {
	t.Helper()
	m := portsmocks.NewMockCatalogue(t)
	if c.replaceErr != nil {
		withCheckpointReplaceError(c, c.replaceErr)(m)
	}
	m.EXPECT().Records().RunAndReturn(func() ([]domain.CatalogueRecord, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		return []domain.CatalogueRecord{c.record}, nil
	}).Maybe()
	m.EXPECT().Record(mock.Anything).RunAndReturn(func(name string) (domain.CatalogueRecord, bool, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.record, c.record.Name == name, nil
	}).Maybe()
	m.EXPECT().Create(mock.Anything).RunAndReturn(func(arg0 domain.CatalogueRecord) error { return nil }).Maybe()
	m.EXPECT().UpdateMetadata(mock.Anything).RunAndReturn(func(arg0 domain.CatalogueMetadataUpdate) error {
		return nil
	}).Maybe()
	m.EXPECT().Sync().RunAndReturn(func() error { return nil }).Maybe()
	m.EXPECT().Replace(mock.Anything, mock.Anything).RunAndReturn(func(name string, record domain.CatalogueRecord) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.replaces++
		if c.events != nil {
			*c.events = append(*c.events, "catalogue")
		}
		if name != c.record.Name || record.Name != name {
			return errors.New("catalogue key mismatch")
		}
		c.record = record
		return nil
	}).Maybe()
	m.EXPECT().Rename(mock.Anything, mock.Anything).RunAndReturn(func(arg0 string, arg1 domain.CatalogueRecord) error { return nil }).Maybe()
	m.EXPECT().Delete(mock.Anything).RunAndReturn(func(arg0 string) error { return nil }).Maybe()
	m.EXPECT().Close().RunAndReturn(func() error { return nil }).Maybe()
	return m
}

func newCheckpointRepositoryStateMock(t *testing.T, r *checkpointRepositoryState) *portsmocks.MockSnapshotRepository {
	t.Helper()
	m := portsmocks.NewMockSnapshotRepository(t)
	m.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(arg0 context.Context, publication ports.SnapshotPublication) error {
		if r.events != nil {
			*r.events = append(*r.events, "repository")
		}
		r.publications = append(r.publications, publication)
		return nil
	}).Maybe()
	m.EXPECT().LoadCheckpoint(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(arg0 context.Context, arg1 domain.IncarnationID, arg2 string, ref ports.CheckpointRef) (ports.SnapshotGeneration, error) {
		r.loads = append(r.loads, ref.Generation)
		if err := r.loadErrors[ref.Generation]; err != nil {
			return ports.SnapshotGeneration{}, err
		}
		generation, ok := r.generations[ref.Generation]
		if !ok {
			return ports.SnapshotGeneration{}, errors.New("checkpoint unavailable")
		}
		return generation, nil
	}).Maybe()
	m.EXPECT().ReconcileCheckpoint(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(arg0 context.Context, arg1 domain.IncarnationID, ref ports.CheckpointRef) error {
		r.repairs = append(r.repairs, ref)
		return r.repairErr
	}).Maybe()
	return m
}

func testCoordinator(t *testing.T, c any, r any, random io.Reader) *Coordinator {
	t.Helper()
	var catalogue ports.Catalogue
	switch c := c.(type) {
	case *checkpointCatalogueState:
		catalogue = newCheckpointCatalogueStateMock(t, c)
	case *transactionCatalogueState:
		catalogue = newTransactionCatalogueStateMock(t, c)
	}
	var repository ports.SnapshotRepository
	switch r := r.(type) {
	case *checkpointRepositoryState:
		repository = newCheckpointRepositoryStateMock(t, r)
	case *transactionRepositoryState:
		repository = newTransactionRepositoryStateMock(t, r)
	case *garbageCollectionState:
		repository = newGarbageCollectionStateMock(t, r)
	}
	return NewCoordinator(catalogue, repository, random)
}

// Register overrides before optional defaults so Mockery selects the variant first.
func withCheckpointReplaceError(c *checkpointCatalogueState, err error) func(*portsmocks.MockCatalogue) {
	return func(m *portsmocks.MockCatalogue) {
		m.EXPECT().Replace(mock.Anything, mock.Anything).RunAndReturn(func(string, domain.CatalogueRecord) error {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.replaces++
			if c.events != nil {
				*c.events = append(*c.events, "catalogue")
			}
			return err
		}).Once()
	}
}
