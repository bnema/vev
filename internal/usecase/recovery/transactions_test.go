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
	deleteCalls   []domain.IncarnationID
	deletedIDs    []domain.IncarnationID
	deleteEntered chan struct{}
	releaseDelete chan struct{}
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
	if r.deleteEntered != nil {
		close(r.deleteEntered)
		<-r.releaseDelete
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleteCalls = append(r.deleteCalls, id)
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

func healthyTransactionRecord() domain.CatalogueRecord {
	return domain.CatalogueRecord{
		Name:          "work",
		IncarnationID: domain.IncarnationID{1},
		Cwd:           "/workspace",
		CreatedAt:     11,
		UpdatedAt:     22,
		LastUsedSeq:   33,
		TabNames:      []string{"shell", "logs"},
		Committed:     &domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{3}},
	}
}

func TestResetIncompatibleReplacesExactHealthyCheckpoint(t *testing.T) {
	t.Parallel()
	old := healthyTransactionRecord()
	catalogue := newTransactionCatalogue(old)
	repository := &transactionRepository{}
	coordinator := NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{2}, 16)))

	fresh, committed, err := coordinator.ResetIncompatible(context.Background(), old.Name, old.IncarnationID, *old.Committed)

	require.NoError(t, err)
	require.True(t, committed)
	require.NotZero(t, fresh.IncarnationID)
	require.NotEqual(t, old.IncarnationID, fresh.IncarnationID)
	require.Equal(t, old.Name, fresh.Name)
	require.Equal(t, old.Cwd, fresh.Cwd)
	require.Equal(t, old.CreatedAt, fresh.CreatedAt)
	require.Equal(t, old.UpdatedAt, fresh.UpdatedAt)
	require.Equal(t, old.LastUsedSeq, fresh.LastUsedSeq)
	require.Empty(t, fresh.TabNames)
	require.Nil(t, fresh.Committed)
	require.Empty(t, fresh.DegradedReason)
	stored, ok, recordErr := catalogue.Record(old.Name)
	require.NoError(t, recordErr)
	require.True(t, ok)
	require.True(t, fresh.Equal(stored))
	require.Equal(t, []domain.IncarnationID{old.IncarnationID}, repository.deleteCalls)
	require.Equal(t, []domain.IncarnationID{old.IncarnationID}, repository.deletedIDs)
}

func TestResetIncompatibleDeleteFailureReturnsCommittedFreshAuthority(t *testing.T) {
	t.Parallel()
	old := healthyTransactionRecord()
	cause := errors.New("repository delete failed")
	catalogue := newTransactionCatalogue(old)
	repository := &transactionRepository{deleteErrOnce: cause}
	coordinator := NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{2}, 16)))

	fresh, committed, err := coordinator.ResetIncompatible(context.Background(), old.Name, old.IncarnationID, *old.Committed)

	require.ErrorIs(t, err, cause)
	require.True(t, committed)
	require.NotEqual(t, old.IncarnationID, fresh.IncarnationID)
	stored, ok, recordErr := catalogue.Record(old.Name)
	require.NoError(t, recordErr)
	require.True(t, ok)
	require.True(t, fresh.Equal(stored))
	require.Equal(t, []domain.IncarnationID{old.IncarnationID}, repository.deleteCalls)
	require.Empty(t, repository.deletedIDs)
}

func TestResetIncompatibleReplaceFailurePreservesOldAuthorityAndRepository(t *testing.T) {
	t.Parallel()
	old := healthyTransactionRecord()
	cause := errors.New("catalogue replace failed")
	catalogue := newTransactionCatalogue(old)
	catalogue.replaceErrOnce = cause
	repository := &transactionRepository{}
	coordinator := NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{2}, 16)))

	fresh, committed, err := coordinator.ResetIncompatible(context.Background(), old.Name, old.IncarnationID, *old.Committed)

	require.ErrorIs(t, err, cause)
	require.False(t, committed)
	require.Equal(t, domain.CatalogueRecord{}, fresh)
	stored, ok, recordErr := catalogue.Record(old.Name)
	require.NoError(t, recordErr)
	require.True(t, ok)
	require.True(t, old.Equal(stored))
	require.Empty(t, repository.deleteCalls)
}

func TestResetIncompatibleStaleAuthorityIsNoOp(t *testing.T) {
	t.Parallel()
	old := healthyTransactionRecord()
	otherGeneration := *old.Committed
	otherGeneration.Generation++
	otherDigest := *old.Committed
	otherDigest.ManifestDigest[0]++
	uncommitted := old
	uncommitted.Committed = nil
	tests := []struct {
		name     string
		record   *domain.CatalogueRecord
		expected domain.IncarnationID
		ref      domain.CheckpointRef
	}{
		{name: "missing record", expected: old.IncarnationID, ref: *old.Committed},
		{name: "different incarnation", record: &old, expected: domain.IncarnationID{9}, ref: *old.Committed},
		{name: "missing checkpoint", record: &uncommitted, expected: old.IncarnationID, ref: *old.Committed},
		{name: "different checkpoint generation", record: &old, expected: old.IncarnationID, ref: otherGeneration},
		{name: "different checkpoint digest", record: &old, expected: old.IncarnationID, ref: otherDigest},
		{name: "degraded record", record: func() *domain.CatalogueRecord {
			degraded := old
			degraded.DegradedReason = "restore failed"
			return &degraded
		}(), expected: old.IncarnationID, ref: *old.Committed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			catalogue := newTransactionCatalogue()
			if tt.record != nil {
				catalogue.records[tt.record.Name] = *tt.record
			}
			repository := &transactionRepository{}
			coordinator := NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{2}, 16)))

			fresh, committed, err := coordinator.ResetIncompatible(context.Background(), old.Name, tt.expected, tt.ref)

			require.NoError(t, err)
			require.False(t, committed)
			require.Equal(t, domain.CatalogueRecord{}, fresh)
			require.Empty(t, catalogue.events)
			require.Empty(t, repository.deleteCalls)
		})
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

func TestMarkBrokenCannotOverwriteDiscardReplacement(t *testing.T) {
	t.Parallel()
	old := degradedTransactionRecord()
	catalogue := newTransactionCatalogue(old)
	repository := &transactionRepository{deleteEntered: make(chan struct{}), releaseDelete: make(chan struct{})}
	coordinator := NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{2}, 16)))

	discarded := make(chan error, 1)
	go func() { discarded <- coordinator.Discard(t.Context(), old.Name) }()
	<-repository.deleteEntered // Replace committed the fresh incarnation while Discard retains the mutation fence.

	marked := make(chan struct {
		marked bool
		err    error
	}, 1)
	go func() {
		_, markedSession, err := coordinator.MarkBroken(t.Context(), old.Name, old.IncarnationID, "late restore failure")
		marked <- struct {
			marked bool
			err    error
		}{markedSession, err}
	}()

	close(repository.releaseDelete)
	require.NoError(t, <-discarded)
	result := <-marked
	require.NoError(t, result.err)
	require.False(t, result.marked, "a late restore must not degrade the replacement incarnation")

	got, ok, err := catalogue.Record(old.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEqual(t, old.IncarnationID, got.IncarnationID)
	require.Nil(t, got.Committed)
	require.Empty(t, got.DegradedReason)
}

func TestDiscardPublicPreconditions(t *testing.T) {
	t.Parallel()
	uncommitted := degradedTransactionRecord()
	uncommitted.Name = "work"
	uncommitted.Committed = nil
	uncommitted.DegradedReason = ""
	healthy := healthyTransactionRecord()
	tests := []struct {
		name       string
		record     *domain.CatalogueRecord
		incomplete bool
		cancelled  bool
		wantErr    error
	}{
		{name: "incomplete dependencies", incomplete: true, wantErr: errors.New("recovery: incomplete discard dependencies")},
		{name: "cancelled context", record: &healthy, cancelled: true, wantErr: context.Canceled},
		{name: "missing record", wantErr: ErrRecoveryRecordNotFound},
		{name: "already fresh incarnation", record: &uncommitted},
		{name: "healthy checkpoint", record: &healthy, wantErr: ErrSessionNotBroken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tt.cancelled {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}
			var coordinator *Coordinator
			var catalogue *transactionCatalogue
			var repository *transactionRepository
			if tt.incomplete {
				coordinator = &Coordinator{}
			} else {
				catalogue = newTransactionCatalogue()
				if tt.record != nil {
					catalogue.records[tt.record.Name] = *tt.record
				}
				repository = &transactionRepository{}
				coordinator = NewCoordinator(catalogue, repository, bytes.NewReader(bytes.Repeat([]byte{2}, 16)))
			}

			err := coordinator.Discard(ctx, "work")

			if tt.wantErr == nil {
				require.NoError(t, err)
			} else if tt.incomplete {
				require.EqualError(t, err, tt.wantErr.Error())
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
			if !tt.incomplete {
				require.Empty(t, catalogue.events)
				require.Empty(t, repository.deleteCalls)
			}
		})
	}
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
			old.TabNames = []string{"shell"}
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
			require.Equal(t, old.TabNames, got.TabNames)
			if tt.failBefore {
				require.Contains(t, repository.deletedIDs, old.IncarnationID)
			} else {
				require.Empty(t, repository.deletedIDs, "startup garbage collection owns the orphaned old directory")
			}
		})
	}
}
