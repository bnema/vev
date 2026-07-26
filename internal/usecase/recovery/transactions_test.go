package recovery

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type transactionCatalogue struct {
	mu             sync.Mutex
	records        map[string]domain.CatalogueRecord
	events         []string
	replaceErrOnce error
	onCreate       func(domain.CatalogueRecord)
}

func newTransactionCatalogue(records ...domain.CatalogueRecord) *transactionCatalogue {
	catalogue := &transactionCatalogue{records: make(map[string]domain.CatalogueRecord)}
	for _, record := range records {
		catalogue.records[record.Name] = record
	}
	return catalogue
}

func (c *transactionCatalogue) Records() ([]domain.CatalogueRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]domain.CatalogueRecord, 0, len(c.records))
	for _, record := range c.records {
		out = append(out, record)
	}
	return out, nil
}

func (c *transactionCatalogue) Record(name string) (domain.CatalogueRecord, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.records[name]
	return record, ok, nil
}

func (c *transactionCatalogue) Create(record domain.CatalogueRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.records[record.Name]; exists {
		return errors.New("record exists")
	}
	c.records[record.Name] = record
	c.events = append(c.events, "create")
	if c.onCreate != nil {
		c.onCreate(record)
	}
	return nil
}

func (c *transactionCatalogue) UpdateMetadata(domain.CatalogueMetadataUpdate) error { return nil }
func (c *transactionCatalogue) Sync() error                                         { return nil }

func (c *transactionCatalogue) Replace(name string, record domain.CatalogueRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.replaceErrOnce != nil {
		err := c.replaceErrOnce
		c.replaceErrOnce = nil
		return err
	}
	if _, exists := c.records[name]; !exists {
		return errors.New("record missing")
	}
	c.records[name] = record
	c.events = append(c.events, "replace:"+name)
	return nil
}

func (c *transactionCatalogue) Rename(oldName string, record domain.CatalogueRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.records[oldName]; !exists {
		return errors.New("record missing")
	}
	if _, exists := c.records[record.Name]; exists && record.Name != oldName {
		return errors.New("record exists")
	}
	delete(c.records, oldName)
	c.records[record.Name] = record
	c.events = append(c.events, "rename:"+oldName+":"+record.Name)
	return nil
}

func (c *transactionCatalogue) Delete(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.records, name)
	c.events = append(c.events, "delete:"+name)
	return nil
}

func (c *transactionCatalogue) Close() error { return nil }

type transactionRepository struct {
	ports.SnapshotRepository
	mu            sync.Mutex
	deleteErrOnce error
	deletedIDs    []domain.IncarnationID
}

type garbageCollectionFenceRepository struct {
	ports.SnapshotRepository
	mu                 sync.Mutex
	events             []string
	collectionEntered  chan struct{}
	releaseCollection  chan struct{}
	collectionFinished bool
	publishBeforeGC    bool
	incarnations       map[domain.IncarnationID]struct{}
	generations        map[domain.IncarnationID]map[uint64]struct{}
}

func (r *garbageCollectionFenceRepository) CollectGarbage(_ context.Context, keep map[domain.IncarnationID]domain.CheckpointRef) error {
	close(r.collectionEntered)
	<-r.releaseCollection
	r.mu.Lock()
	defer r.mu.Unlock()
	for id := range r.incarnations {
		if _, known := keep[id]; !known {
			delete(r.incarnations, id)
		}
	}
	for id, generations := range r.generations {
		committed, known := keep[id]
		if !known {
			delete(r.generations, id)
			continue
		}
		for generation := range generations {
			if generation != committed.Generation && generation+1 != committed.Generation {
				delete(generations, generation)
			}
		}
	}
	r.collectionFinished = true
	r.events = append(r.events, "collect")
	return nil
}

func (r *garbageCollectionFenceRepository) Publish(_ context.Context, publication ports.SnapshotPublication) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.collectionFinished {
		r.publishBeforeGC = true
	}
	if r.generations == nil {
		r.generations = make(map[domain.IncarnationID]map[uint64]struct{})
	}
	if r.generations[publication.IncarnationID] == nil {
		r.generations[publication.IncarnationID] = make(map[uint64]struct{})
	}
	r.generations[publication.IncarnationID][publication.Generation] = struct{}{}
	r.events = append(r.events, "publish")
	return nil
}

func (r *garbageCollectionFenceRepository) addIncarnation(record domain.CatalogueRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.incarnations == nil {
		r.incarnations = make(map[domain.IncarnationID]struct{})
	}
	r.incarnations[record.IncarnationID] = struct{}{}
}

func (r *garbageCollectionFenceRepository) hasIncarnation(id domain.IncarnationID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.incarnations[id]
	return ok
}

func (r *garbageCollectionFenceRepository) hasGeneration(id domain.IncarnationID, generation uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.generations[id][generation]
	return ok
}

func (r *garbageCollectionFenceRepository) eventLog() ([]string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...), r.publishBeforeGC
}

func (r *transactionRepository) DeleteIncarnation(_ context.Context, id domain.IncarnationID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deleteErrOnce != nil {
		err := r.deleteErrOnce
		r.deleteErrOnce = nil
		return err
	}
	r.deletedIDs = append(r.deletedIDs, id)
	return nil
}

func degradedTransactionRecord() domain.CatalogueRecord {
	return domain.CatalogueRecord{
		Name: "broken", IncarnationID: domain.IncarnationID{1}, Committed: &domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{3}},
		DegradedReason: "checkpoint unreadable",
	}
}

func TestStartupGarbageCollectionFencesNewIncarnationsAndCheckpointPublications(t *testing.T) {
	t.Parallel()

	t.Run("a stale keep snapshot cannot remove a newly created incarnation", func(t *testing.T) {
		t.Parallel()
		catalogue := newTransactionCatalogue()
		repository := &garbageCollectionFenceRepository{
			collectionEntered: make(chan struct{}),
			releaseCollection: make(chan struct{}),
		}
		catalogue.onCreate = repository.addIncarnation
		coordinator := NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{2}, 16)))

		collected := make(chan error, 1)
		go func() {
			_, err := coordinator.CollectGarbage(context.Background())
			collected <- err
		}()
		<-repository.collectionEntered

		created := make(chan error, 1)
		go func() {
			_, err := coordinator.Create(context.Background(), domain.CatalogueRecord{Name: "new"})
			created <- err
		}()
		close(repository.releaseCollection)

		require.NoError(t, <-collected)
		require.NoError(t, <-created)
		got, ok, err := catalogue.Record("new")
		require.NoError(t, err)
		require.True(t, ok)
		require.NotZero(t, got.IncarnationID)
		require.True(t, repository.hasIncarnation(got.IncarnationID), "GC's stale keep snapshot must finish before the new incarnation is published")
		events, _ := repository.eventLog()
		require.Equal(t, []string{"collect"}, events)
	})

	t.Run("a stale keep snapshot cannot prune a newly committed generation", func(t *testing.T) {
		t.Parallel()
		committed := domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}
		record := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{1}, Committed: &committed}
		catalogue := newTransactionCatalogue(record)
		repository := &garbageCollectionFenceRepository{
			collectionEntered: make(chan struct{}),
			releaseCollection: make(chan struct{}),
			generations: map[domain.IncarnationID]map[uint64]struct{}{
				record.IncarnationID: {1: {}},
			},
		}
		coordinator := NewCoordinator(catalogue, repository, bytes.NewReader(nil))

		collected := make(chan error, 1)
		go func() {
			_, err := coordinator.CollectGarbage(context.Background())
			collected <- err
		}()
		<-repository.collectionEntered

		published := make(chan error, 1)
		go func() {
			_, err := coordinator.PublishCheckpoint(context.Background(), "work", ports.SnapshotPublication{
				Name:             "work",
				IncarnationID:    record.IncarnationID,
				Generation:       2,
				ParentCheckpoint: &committed,
				Manifest:         []byte("generation two"),
			})
			published <- err
		}()
		close(repository.releaseCollection)

		require.NoError(t, <-collected)
		require.NoError(t, <-published)
		events, publishedBeforeGC := repository.eventLog()
		require.Equal(t, []string{"collect", "publish"}, events)
		require.False(t, publishedBeforeGC)
		require.True(t, repository.hasGeneration(record.IncarnationID, 2), "GC's stale keep snapshot must finish before generation two is published")
	})
}

func TestDiscardIsRetryIdempotent(t *testing.T) {
	t.Parallel()
	failure := errors.New("injected discard failure")
	tests := []struct {
		name       string
		failBefore bool
	}{
		{name: "retry from old catalogue record", failBefore: true},
		{name: "retry from fresh catalogue record"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			old := degradedTransactionRecord()
			catalogue := newTransactionCatalogue(old)
			repository := &transactionRepository{}
			if tt.failBefore {
				catalogue.replaceErrOnce = failure
			} else {
				repository.deleteErrOnce = failure
			}
			ids := append(bytes.Repeat([]byte{2}, 16), bytes.Repeat([]byte{3}, 16)...)
			coordinator := NewCoordinator(catalogue, repository, bytes.NewReader(ids))

			err := coordinator.Discard(context.Background(), old.Name)
			require.ErrorIs(t, err, failure)
			require.NoError(t, coordinator.Discard(context.Background(), old.Name))

			got, ok, err := catalogue.Record(old.Name)
			require.NoError(t, err)
			require.True(t, ok)
			require.Nil(t, got.Committed)
			require.NotEqual(t, old.IncarnationID, got.IncarnationID)
			if tt.failBefore {
				require.Contains(t, repository.deletedIDs, old.IncarnationID)
			} else {
				require.Empty(t, repository.deletedIDs, "startup garbage collection owns the orphaned old directory")
			}
		})
	}
}
