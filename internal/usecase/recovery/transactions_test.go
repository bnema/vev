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
	return nil
}

func (c *transactionCatalogue) UpdateMetadata(domain.CatalogueMetadataUpdate) error { return nil }

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
