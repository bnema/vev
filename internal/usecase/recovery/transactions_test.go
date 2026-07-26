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
	mu         sync.Mutex
	tombstones map[domain.IncarnationID]domain.DeletionTombstone
	events     []string
	quarantine []bool
}

func newTransactionRepository() *transactionRepository {
	return &transactionRepository{tombstones: make(map[domain.IncarnationID]domain.DeletionTombstone)}
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
