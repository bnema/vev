package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type transactionCatalogue struct {
	mu      sync.Mutex
	records map[string]domain.CatalogueRecord
	events  []string
}

func newTransactionCatalogue(records ...domain.CatalogueRecord) *transactionCatalogue {
	catalogue := &transactionCatalogue{records: make(map[string]domain.CatalogueRecord)}
	for _, record := range records {
		catalogue.records[record.Name] = record
	}
	return catalogue
}

func (c *transactionCatalogue) Records() []domain.CatalogueRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]domain.CatalogueRecord, 0, len(c.records))
	for _, record := range c.records {
		out = append(out, record)
	}
	return out
}
func (c *transactionCatalogue) Record(name string) (domain.CatalogueRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.records[name]
	return record, ok
}
func (c *transactionCatalogue) Create(record domain.CatalogueRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.records[record.Name]; exists {
		return errors.New("record exists")
	}
	c.records[record.Name] = record
	c.events = append(c.events, "create")
	return nil
}
func (c *transactionCatalogue) UpdateMetadata(domain.CatalogueMetadataUpdate) error { return nil }
func (c *transactionCatalogue) Replace(name string, record domain.CatalogueRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	mu             sync.Mutex
	tombstones     map[domain.IncarnationID]domain.DeletionTombstone
	generations    map[domain.CheckpointRef]ports.SnapshotGeneration
	loadErr        error
	events         []string
	quarantine     []bool
	descriptors    []domain.QuarantineDescriptor
	quarantinedIDs []domain.IncarnationID
	repaired       []domain.CheckpointRef
}

func newTransactionRepository() *transactionRepository {
	return &transactionRepository{
		tombstones:  make(map[domain.IncarnationID]domain.DeletionTombstone),
		generations: make(map[domain.CheckpointRef]ports.SnapshotGeneration),
	}
}
func (r *transactionRepository) LoadCheckpoint(_ context.Context, _ domain.IncarnationID, _ string, ref domain.CheckpointRef) (ports.SnapshotGeneration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "load")
	if r.loadErr != nil {
		return ports.SnapshotGeneration{}, r.loadErr
	}
	generation, ok := r.generations[ref]
	if !ok {
		return ports.SnapshotGeneration{}, errors.New("checkpoint missing")
	}
	return generation, nil
}
func (r *transactionRepository) RepairHEAD(_ context.Context, _ domain.IncarnationID, ref domain.CheckpointRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.repaired = append(r.repaired, ref)
	r.events = append(r.events, "repair-head")
	return nil
}
func (r *transactionRepository) SaveQuarantineDescriptor(_ context.Context, descriptor domain.QuarantineDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.descriptors = append(r.descriptors, descriptor)
	r.events = append(r.events, "descriptor")
	return nil
}
func (r *transactionRepository) QuarantineIncarnation(_ context.Context, id domain.IncarnationID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.quarantinedIDs = append(r.quarantinedIDs, id)
	r.events = append(r.events, "quarantine-incarnation")
	return nil
}
func (r *transactionRepository) WriteDeletionTombstone(_ context.Context, tombstone domain.DeletionTombstone) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tombstones[tombstone.IncarnationID] = tombstone
	r.events = append(r.events, "tombstone")
	return nil
}
func (r *transactionRepository) ListDeletionTombstones(_ context.Context, cursor ports.DeletionTombstoneCursor, _ ports.MaintenanceBudget) (ports.DeletionTombstonePage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cursor.After != "" || len(r.tombstones) == 0 {
		return ports.DeletionTombstonePage{Done: true}, nil
	}
	out := make([]domain.DeletionTombstone, 0, len(r.tombstones))
	for _, tombstone := range r.tombstones {
		out = append(out, tombstone)
	}
	return ports.DeletionTombstonePage{Tombstones: out, Done: true}, nil
}
func (r *transactionRepository) QuarantineDeletionSources(_ context.Context, _ domain.DeletionTombstone, includeLegacyName bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "quarantine")
	r.quarantine = append(r.quarantine, includeLegacyName)
	return nil
}
func (r *transactionRepository) DeleteDeletionTombstone(_ context.Context, id domain.IncarnationID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tombstones, id)
	r.events = append(r.events, "remove-tombstone")
	return nil
}

func TestTransactionCrashMatrix(t *testing.T) {
	ctx := context.Background()
	t.Run("create", func(t *testing.T) {
		catalogue := newTransactionCatalogue()
		coordinator := NewCoordinator(catalogue, newTransactionRepository(), journalStub{}, bytes.NewReader(bytes.Repeat([]byte{1}, 16)))
		record, err := coordinator.Create(ctx, domain.CatalogueRecord{Name: "work", Cwd: "/tmp"})
		require.NoError(t, err)
		require.Equal(t, domain.RecoveryFresh, record.RecoveryState)
		require.Equal(t, domain.IncarnationID{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, record.IncarnationID)
		require.Equal(t, record, catalogue.records["work"])
	})
	t.Run("checkpoint", func(t *testing.T) {
		record := checkpointRecord()
		catalogue := newTransactionCatalogue(record)
		repository := &checkpointRepository{}
		coordinator := NewCoordinator(catalogue, repository, journalStub{}, nil)
		next, err := coordinator.PublishCheckpoint(ctx, record.Name, checkpointPublication(record, 1))
		require.NoError(t, err)
		require.Equal(t, next, catalogue.records[record.Name])
	})
	t.Run("rename", func(t *testing.T) {
		ref := &domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{1}}
		record := domain.CatalogueRecord{Name: "old", IncarnationID: domain.IncarnationID{2}, RecoveryState: domain.RecoveryHealthy, Committed: ref}
		catalogue := newTransactionCatalogue(record)
		coordinator := NewCoordinator(catalogue, newTransactionRepository(), journalStub{}, nil)
		next, err := coordinator.Rename(ctx, "old", "new")
		require.NoError(t, err)
		_, oldExists := catalogue.records["old"]
		require.False(t, oldExists)
		require.Equal(t, record.IncarnationID, catalogue.records["new"].IncarnationID)
		require.Equal(t, record.Committed, next.Committed)
	})
	t.Run("delete", func(t *testing.T) {
		record := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{3}, RecoveryState: domain.RecoveryFresh}
		tombstone := domain.DeletionTombstone{Name: record.Name, IncarnationID: record.IncarnationID}
		for crashAfter := 1; crashAfter <= 6; crashAfter++ {
			t.Run(string(rune('0'+crashAfter)), func(t *testing.T) {
				catalogue := newTransactionCatalogue()
				repository := newTransactionRepository()
				switch crashAfter {
				case 1:
					deleting := record
					deleting.RecoveryState = domain.RecoveryDeleting
					catalogue.records[record.Name] = deleting
				case 2, 3, 4:
					deleting := record
					deleting.RecoveryState = domain.RecoveryDeleting
					catalogue.records[record.Name] = deleting
					repository.tombstones[record.IncarnationID] = tombstone
				case 5:
					repository.tombstones[record.IncarnationID] = tombstone
				case 6:
					// The complete transaction has no durable work left.
				}
				coordinator := NewCoordinator(catalogue, repository, journalStub{}, nil)
				require.NoError(t, coordinator.Recover(ctx))
				_, exists := catalogue.records[record.Name]
				require.False(t, exists)
				require.Empty(t, repository.tombstones)
			})
		}

		catalogue := newTransactionCatalogue(record)
		repository := newTransactionRepository()
		coordinator := NewCoordinator(catalogue, repository, journalStub{}, nil)
		require.NoError(t, coordinator.Delete(ctx, "work"))
		require.Equal(t, []string{"replace:work", "delete:work"}, catalogue.events)
		require.Equal(t, []string{"tombstone", "quarantine", "remove-tombstone"}, repository.events)
		require.Equal(t, []bool{true}, repository.quarantine)
	})
}

type transactionJournal struct {
	mu      sync.Mutex
	intents map[domain.IncarnationID]domain.DiscardIntent
	events  []string
}

func newTransactionJournal() *transactionJournal {
	return &transactionJournal{intents: make(map[domain.IncarnationID]domain.DiscardIntent)}
}
func (j *transactionJournal) SaveDiscard(_ context.Context, intent domain.DiscardIntent) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.intents[intent.OldIncarnation] = intent
	j.events = append(j.events, "intent")
	return nil
}
func (j *transactionJournal) ListDiscards(context.Context) ([]domain.DiscardIntent, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]domain.DiscardIntent, 0, len(j.intents))
	for _, intent := range j.intents {
		out = append(out, intent)
	}
	return out, nil
}
func (j *transactionJournal) DeleteDiscard(_ context.Context, id domain.IncarnationID) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.intents, id)
	j.events = append(j.events, "delete-intent")
	return nil
}

func degradedTransactionRecord() domain.CatalogueRecord {
	return domain.CatalogueRecord{
		Name: "broken", IncarnationID: domain.IncarnationID{1}, RecoveryState: domain.RecoveryDegraded,
		Committed:      &domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{3}},
		Fallbacks:      [2]*domain.CheckpointRef{{Generation: 2, ManifestDigest: [32]byte{2}}, {Generation: 1, ManifestDigest: [32]byte{1}}},
		DegradedReason: "checkpoint unreadable",
	}
}

func TestDegradedRetryIsReadOnly(t *testing.T) {
	record := degradedTransactionRecord()
	catalogue := newTransactionCatalogue(record)
	repository := newTransactionRepository()
	repository.loadErr = errors.New("corrupt checkpoint")
	coordinator := NewCoordinator(catalogue, repository, newTransactionJournal(), bytes.NewReader(bytes.Repeat([]byte{9}, 16)))

	require.Error(t, coordinator.Retry(context.Background(), record.Name))
	require.Equal(t, record, catalogue.records[record.Name])
	require.Empty(t, catalogue.events)
}

func TestFallbackPromotion(t *testing.T) {
	record := degradedTransactionRecord()
	fallback := *record.Fallbacks[0]
	catalogue := newTransactionCatalogue(record)
	repository := newTransactionRepository()
	repository.generations[fallback] = ports.SnapshotGeneration{
		IncarnationID: record.IncarnationID, Name: record.Name, Generation: fallback.Generation,
		Manifest: []byte("fallback"),
	}
	fallback.ManifestDigest = checkpointDigest(repository.generations[*record.Fallbacks[0]].Manifest)
	record.Fallbacks[0] = &fallback
	catalogue.records[record.Name] = record
	repository.generations[fallback] = ports.SnapshotGeneration{
		IncarnationID: record.IncarnationID, Name: record.Name, Generation: fallback.Generation,
		Manifest: []byte("fallback"),
	}
	coordinator := NewCoordinator(catalogue, repository, newTransactionJournal(), nil)

	require.NoError(t, coordinator.RestoreFallback(context.Background(), record.Name, fallback))
	got := catalogue.records[record.Name]
	require.Equal(t, domain.RecoveryHealthy, got.RecoveryState)
	require.Equal(t, fallback, *got.Committed)
	require.Equal(t, []domain.CheckpointRef{fallback}, repository.repaired)
	require.Equal(t, []string{"load", "load", "repair-head"}, repository.events)
}

func checkpointDigest(data []byte) [32]byte {
	return sha256.Sum256(data)
}

func TestDegradedExportIsReadOnly(t *testing.T) {
	record := degradedTransactionRecord()
	manifest := []byte("manifest")
	ref := *record.Committed
	ref.ManifestDigest = checkpointDigest(manifest)
	record.Committed = &ref
	catalogue := newTransactionCatalogue(record)
	repository := newTransactionRepository()
	repository.generations[ref] = ports.SnapshotGeneration{
		IncarnationID: record.IncarnationID, Name: record.Name, Generation: ref.Generation,
		Manifest: manifest, Objects: map[ports.SnapshotDigest][]byte{{2}: []byte("second"), {1}: []byte("first")},
	}
	coordinator := NewCoordinator(catalogue, repository, newTransactionJournal(), nil)
	var exported, repeated bytes.Buffer

	require.NoError(t, coordinator.Export(context.Background(), record.Name, &exported))
	require.NoError(t, coordinator.Export(context.Background(), record.Name, &repeated))
	require.NotEmpty(t, exported.Bytes())
	require.Equal(t, exported.Bytes(), repeated.Bytes())
	require.Equal(t, record, catalogue.records[record.Name])
	require.Empty(t, catalogue.events)
}

func TestDiscardCrashMatrix(t *testing.T) {
	ctx := context.Background()
	old := degradedTransactionRecord()
	newID := domain.IncarnationID{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	intent := domain.DiscardIntent{OldRecord: old, OldIncarnation: old.IncarnationID, NewIncarnation: newID, SessionName: old.Name, Reason: "explicit discard"}

	t.Run("commit order", func(t *testing.T) {
		catalogue := newTransactionCatalogue(old)
		repository := newTransactionRepository()
		journal := newTransactionJournal()
		coordinator := NewCoordinator(catalogue, repository, journal, bytes.NewReader(newID[:]))
		got, err := coordinator.Discard(ctx, old.Name, intent.Reason)
		require.NoError(t, err)
		require.Equal(t, freshReplacement(intent), got)
		require.Equal(t, []string{"intent", "delete-intent"}, journal.events)
		require.Equal(t, []string{"descriptor", "quarantine-incarnation"}, repository.events)
		require.Equal(t, []string{"replace:" + old.Name}, catalogue.events)
		require.Empty(t, journal.intents)
	})

	for step := 1; step <= 6; step++ {
		t.Run(strconv.Itoa(step), func(t *testing.T) {
			catalogue := newTransactionCatalogue(old)
			repository := newTransactionRepository()
			journal := newTransactionJournal()
			if step >= 1 {
				journal.intents[old.IncarnationID] = intent
			}
			if step >= 2 {
				repository.descriptors = append(repository.descriptors, quarantineDescriptor(intent))
			}
			if step >= 3 {
				repository.quarantinedIDs = append(repository.quarantinedIDs, old.IncarnationID)
			}
			if step >= 4 {
				catalogue.records[old.Name] = freshReplacement(intent)
			}
			if step >= 5 {
				delete(journal.intents, old.IncarnationID)
			}
			if step == 6 {
				// Runtime exposure is deliberately outside durable recovery.
			}
			coordinator := NewCoordinator(catalogue, repository, journal, nil)
			require.NoError(t, coordinator.Recover(ctx))
			got := catalogue.records[old.Name]
			require.Equal(t, newID, got.IncarnationID)
			require.Equal(t, domain.RecoveryFresh, got.RecoveryState)
			require.NotEmpty(t, repository.descriptors)
			require.NotEmpty(t, repository.quarantinedIDs)
			require.Empty(t, journal.intents)
		})
	}
}

func TestDiscardIncarnationConflict(t *testing.T) {
	old := degradedTransactionRecord()
	intent := domain.DiscardIntent{OldRecord: old, OldIncarnation: old.IncarnationID, NewIncarnation: domain.IncarnationID{2}, SessionName: old.Name, Reason: "discard"}
	conflict := old
	conflict.IncarnationID = domain.IncarnationID{3}
	catalogue := newTransactionCatalogue(conflict)
	journal := newTransactionJournal()
	journal.intents[old.IncarnationID] = intent
	coordinator := NewCoordinator(catalogue, newTransactionRepository(), journal, nil)

	require.Error(t, coordinator.Recover(context.Background()))
	require.Equal(t, conflict, catalogue.records[old.Name])
	require.Contains(t, journal.intents, old.IncarnationID)
}

func TestDeleteRecoveryAfterCatalogueRemoval(t *testing.T) {
	ctx := context.Background()
	old := domain.DeletionTombstone{Name: "work", IncarnationID: domain.IncarnationID{4}}
	t.Run("absent catalogue record", func(t *testing.T) {
		catalogue := newTransactionCatalogue()
		repository := newTransactionRepository()
		repository.tombstones[old.IncarnationID] = old
		coordinator := NewCoordinator(catalogue, repository, journalStub{}, nil)
		require.NoError(t, coordinator.Recover(ctx))
		require.Empty(t, repository.tombstones)
		require.Equal(t, []bool{true}, repository.quarantine)
	})
	t.Run("reused name preserves new incarnation", func(t *testing.T) {
		replacement := domain.CatalogueRecord{Name: old.Name, IncarnationID: domain.IncarnationID{9}, RecoveryState: domain.RecoveryFresh}
		catalogue := newTransactionCatalogue(replacement)
		repository := newTransactionRepository()
		repository.tombstones[old.IncarnationID] = old
		coordinator := NewCoordinator(catalogue, repository, journalStub{}, nil)
		require.NoError(t, coordinator.Recover(ctx))
		require.Equal(t, replacement, catalogue.records[old.Name])
		require.Equal(t, []bool{false}, repository.quarantine)
		require.Empty(t, repository.tombstones)
	})
}
