package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	quarantineErr  error
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
	return r.quarantineErr
}
func (r *transactionRepository) DeleteDeletionTombstone(_ context.Context, id domain.IncarnationID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tombstones, id)
	r.events = append(r.events, "remove-tombstone")
	return nil
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
	manifest := []byte("fallback")
	fallback.ManifestDigest = checkpointDigest(manifest)
	record.Fallbacks[0] = &fallback
	catalogue.records[record.Name] = record
	repository.generations[fallback] = ports.SnapshotGeneration{
		IncarnationID: record.IncarnationID, Name: record.Name, Generation: fallback.Generation,
		Manifest: manifest,
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

// A third incarnation still fails closed for that session: its intent and
// record are retained untouched, but startup is no longer aborted.
func TestDiscardRecoveryRemovesIntentWhenCatalogueRecordIsAbsent(t *testing.T) {
	old := degradedTransactionRecord()
	intent := domain.DiscardIntent{OldRecord: old, OldIncarnation: old.IncarnationID, NewIncarnation: domain.IncarnationID{2}, SessionName: old.Name, Reason: "discard"}
	catalogue := newTransactionCatalogue()
	journal := newTransactionJournal()
	journal.intents[old.IncarnationID] = intent
	coordinator := NewCoordinator(catalogue, newTransactionRepository(), journal, nil)

	require.NoError(t, coordinator.Recover(context.Background()))
	require.NotContains(t, journal.intents, old.IncarnationID)
	require.Equal(t, []string{"delete-intent"}, journal.events)
	require.Empty(t, coordinator.Conflicts())
}

func TestDiscardIncarnationConflict(t *testing.T) {
	old := degradedTransactionRecord()
	intent := domain.DiscardIntent{OldRecord: old, OldIncarnation: old.IncarnationID, NewIncarnation: domain.IncarnationID{2}, SessionName: old.Name, Reason: "discard"}
	conflict := old
	conflict.IncarnationID = domain.IncarnationID{3}
	catalogue := newTransactionCatalogue(conflict)
	journal := newTransactionJournal()
	journal.intents[old.IncarnationID] = intent
	repository := newTransactionRepository()
	coordinator := NewCoordinator(catalogue, repository, journal, nil)

	require.NoError(t, coordinator.Recover(context.Background()))
	require.Equal(t, conflict, catalogue.records[old.Name])
	require.Contains(t, journal.intents, old.IncarnationID)
	require.Empty(t, repository.events)
	conflicts := coordinator.Conflicts()
	require.Len(t, conflicts, 1)
	require.Equal(t, old.Name, conflicts[0].Session)
	require.Equal(t, "discard-intent", conflicts[0].Kind)
	require.ErrorIs(t, conflicts[0].Err, ErrDiscardConflict)
}

// A discard whose replacement is already durable, live, and checkpointed must
// roll forward. Requiring byte equality with the pristine replacement would
// leave the intent unresolvable and abort every future startup.
func TestDiscardRecoveryRollsForwardEvolvedReplacement(t *testing.T) {
	old := degradedTransactionRecord()
	intent := domain.DiscardIntent{OldRecord: old, OldIncarnation: old.IncarnationID, NewIncarnation: domain.IncarnationID{2}, SessionName: old.Name, Reason: "discard"}
	evolved := freshReplacement(intent)
	evolved.RecoveryState = domain.RecoveryHealthy
	evolved.Committed = &domain.CheckpointRef{Generation: 7, ManifestDigest: [32]byte{7}}
	evolved.UpdatedAt = 1234
	evolved.LastUsedSeq = 9
	evolved.TabNames = []string{"one"}
	require.NoError(t, evolved.Validate())

	catalogue := newTransactionCatalogue(evolved)
	journal := newTransactionJournal()
	journal.intents[old.IncarnationID] = intent
	repository := newTransactionRepository()
	coordinator := NewCoordinator(catalogue, repository, journal, nil)

	require.NoError(t, coordinator.Recover(context.Background()))
	require.Empty(t, coordinator.Conflicts())
	require.Equal(t, evolved, catalogue.records[old.Name], "committed replacement state must survive roll-forward")
	require.Empty(t, catalogue.events, "the live replacement record must not be rewritten")
	require.Equal(t, []domain.QuarantineDescriptor{quarantineDescriptor(intent)}, repository.descriptors)
	require.Equal(t, []domain.IncarnationID{intent.OldIncarnation}, repository.quarantinedIDs)
	require.NotContains(t, journal.intents, old.IncarnationID)
}

// Goal 3: one session's self-inconsistent durable state must not stop a healthy
// neighbour from being recovered, and must not silently resolve itself either.
func TestRecoverFencesOneSessionAndContinues(t *testing.T) {
	ctx := context.Background()
	broken := degradedTransactionRecord()
	intent := domain.DiscardIntent{OldRecord: broken, OldIncarnation: broken.IncarnationID, NewIncarnation: domain.IncarnationID{2}, SessionName: broken.Name, Reason: "discard"}
	conflict := broken
	conflict.IncarnationID = domain.IncarnationID{3}
	deleting := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{4}, RecoveryState: domain.RecoveryDeleting}

	catalogue := newTransactionCatalogue(conflict, deleting)
	journal := newTransactionJournal()
	journal.intents[broken.IncarnationID] = intent
	repository := newTransactionRepository()
	coordinator := NewCoordinator(catalogue, repository, journal, nil)

	require.NoError(t, coordinator.Recover(ctx))
	require.NotContains(t, catalogue.records, deleting.Name, "healthy deletion must complete next to a fenced session")
	require.Empty(t, repository.tombstones)
	require.Equal(t, conflict, catalogue.records[broken.Name])
	require.Contains(t, journal.intents, broken.IncarnationID)
	require.Len(t, coordinator.Conflicts(), 1)
}

// A tombstone that disagrees with a live healthy record degrades exactly that
// record, keeps the tombstone, and leaves every sibling tombstone processed.
func TestRecoverDegradesSessionOnTombstoneConflict(t *testing.T) {
	ctx := context.Background()
	live := domain.CatalogueRecord{
		Name: "live", IncarnationID: domain.IncarnationID{5}, RecoveryState: domain.RecoveryHealthy,
		Committed: &domain.CheckpointRef{Generation: 2, ManifestDigest: [32]byte{2}},
	}
	conflicting := domain.DeletionTombstone{Name: live.Name, IncarnationID: live.IncarnationID}
	orphan := domain.DeletionTombstone{Name: "gone", IncarnationID: domain.IncarnationID{6}}

	catalogue := newTransactionCatalogue(live)
	repository := newTransactionRepository()
	repository.tombstones[conflicting.IncarnationID] = conflicting
	repository.tombstones[orphan.IncarnationID] = orphan
	coordinator := NewCoordinator(catalogue, repository, journalStub{}, nil)

	require.NoError(t, coordinator.Recover(ctx))
	got := catalogue.records[live.Name]
	require.Equal(t, domain.RecoveryDegraded, got.RecoveryState)
	require.Equal(t, degradedReasonTombstone, got.DegradedReason)
	require.Equal(t, live.Committed, got.Committed)
	require.Contains(t, repository.tombstones, conflicting.IncarnationID, "a fenced tombstone is never removed")
	require.NotContains(t, repository.tombstones, orphan.IncarnationID)
	conflicts := coordinator.Conflicts()
	require.Len(t, conflicts, 1)
	require.Equal(t, "deletion-tombstone", conflicts[0].Kind)
	require.ErrorIs(t, conflicts[0].Err, ErrDeletionTombstoneConflict)
}

// Infrastructure failures still abort startup: they say nothing about any one
// session, so continuing would mutate durable state on a broken substrate.
func TestRecoverFailsOnInfrastructureErrors(t *testing.T) {
	ctx := context.Background()
	cause := errors.New("io failure")
	tombstone := domain.DeletionTombstone{Name: "work", IncarnationID: domain.IncarnationID{4}}

	t.Run("journal listing", func(t *testing.T) {
		coordinator := NewCoordinator(newTransactionCatalogue(), newTransactionRepository(), journalStub{err: cause}, nil)
		require.ErrorIs(t, coordinator.Recover(ctx), cause)
	})
	t.Run("quarantine write", func(t *testing.T) {
		repository := newTransactionRepository()
		repository.tombstones[tombstone.IncarnationID] = tombstone
		repository.quarantineErr = cause
		coordinator := NewCoordinator(newTransactionCatalogue(), repository, journalStub{}, nil)
		require.ErrorIs(t, coordinator.Recover(ctx), cause)
		require.Empty(t, coordinator.Conflicts())
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
