package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

// newSnapshotRepositoryMock returns a generated ports.SnapshotRepository mock.
// configure runs first, so its expectations take precedence (testify matches
// the first registered expectation); the no-op defaults registered afterwards
// are what a sink that only observes publication or deletion needs.
func newSnapshotRepositoryMock(t testing.TB, configure ...func(*portsmocks.MockSnapshotRepository)) *portsmocks.MockSnapshotRepository {
	t.Helper()
	repository := portsmocks.NewMockSnapshotRepository(t)
	for _, apply := range configure {
		apply(repository)
	}
	repository.EXPECT().Publish(mock.Anything, mock.Anything).Return(nil).Maybe()
	repository.EXPECT().LoadCheckpoint(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(ports.SnapshotGeneration{}, errors.New("unused")).Maybe()
	repository.EXPECT().ReconcileCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	repository.EXPECT().DeleteIncarnation(mock.Anything, mock.Anything).Return(nil).Maybe()
	repository.EXPECT().SnapshotIncarnations(mock.Anything).Return(nil, nil).Maybe()
	repository.EXPECT().CollectIncarnationGarbage(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	return repository
}

// recoveryCatalogueState is the in-memory catalogue a recovery catalogue mock
// serves. It is a plain recorder that deliberately lacks Rename and Close, so it
// does not implement ports.Catalogue; the generated mock's closures call it.
type recoveryCatalogueState struct {
	mu              sync.Mutex
	records         map[string]domain.CatalogueRecord
	metadataUpdates []domain.CatalogueMetadataUpdate
	replaceErr      error
	syncs           int
}

func newRecoveryCatalogueState(records []domain.CatalogueRecord) *recoveryCatalogueState {
	state := &recoveryCatalogueState{records: make(map[string]domain.CatalogueRecord, len(records))}
	for _, record := range records {
		state.records[record.Name] = record
	}
	return state
}

func (s *recoveryCatalogueState) Records() ([]domain.CatalogueRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]domain.CatalogueRecord, 0, len(s.records))
	for _, record := range s.records {
		records = append(records, record)
	}
	return records, nil
}

func (s *recoveryCatalogueState) Record(name string) (domain.CatalogueRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[name]
	return record, ok, nil
}

func (s *recoveryCatalogueState) Create(record domain.CatalogueRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.records[record.Name]; exists {
		return errors.New("catalogue record already exists")
	}
	s.records[record.Name] = record
	return nil
}

func (s *recoveryCatalogueState) Delete(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, name)
	return nil
}

func (s *recoveryCatalogueState) Replace(name string, record domain.CatalogueRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.replaceErr != nil {
		return s.replaceErr
	}
	if name != record.Name {
		return errors.New("catalogue key mismatch")
	}
	s.records[name] = record
	return nil
}

func (s *recoveryCatalogueState) UpdateMetadata(update domain.CatalogueMetadataUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[update.Name]
	if !ok {
		return errors.New("catalogue record not found")
	}
	if update.IncarnationID == (domain.IncarnationID{}) || record.IncarnationID != update.IncarnationID {
		return errors.New("catalogue incarnation changed")
	}
	if update.Cwd != nil {
		record.Cwd = *update.Cwd
	}
	if update.UpdatedAt != nil {
		record.UpdatedAt = *update.UpdatedAt
	}
	if update.LastUsedSeq != nil {
		record.LastUsedSeq = *update.LastUsedSeq
	}
	if update.TabNames != nil {
		record.TabNames = append([]string(nil), (*update.TabNames)...)
	}
	s.records[update.Name] = record
	s.metadataUpdates = append(s.metadataUpdates, update)
	return nil
}

func (s *recoveryCatalogueState) Sync() error {
	s.mu.Lock()
	s.syncs++
	s.mu.Unlock()
	return nil
}

// put stores record directly, bypassing the catalogue port.
func (s *recoveryCatalogueState) put(record domain.CatalogueRecord) {
	s.mu.Lock()
	s.records[record.Name] = record
	s.mu.Unlock()
}

// drop removes the named record directly, bypassing the catalogue port.
func (s *recoveryCatalogueState) drop(name string) {
	s.mu.Lock()
	delete(s.records, name)
	s.mu.Unlock()
}

func (s *recoveryCatalogueState) setReplaceErr(err error) {
	s.mu.Lock()
	s.replaceErr = err
	s.mu.Unlock()
}

// MetadataUpdates returns a copy of every accepted metadata update.
func (s *recoveryCatalogueState) MetadataUpdates() []domain.CatalogueMetadataUpdate {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.CatalogueMetadataUpdate(nil), s.metadataUpdates...)
}

// syncCount reports how many times the catalogue was synced.
func (s *recoveryCatalogueState) syncCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncs
}

// newRecoveryCatalogueMock returns a ports.Catalogue mock backed by an
// in-memory state seeded with records. configure runs first (see
// newSnapshotRepositoryMock) and may override any method over the shared state.
func newRecoveryCatalogueMock(t testing.TB, records []domain.CatalogueRecord, configure ...func(*portsmocks.MockCatalogue, *recoveryCatalogueState)) (*portsmocks.MockCatalogue, *recoveryCatalogueState) {
	t.Helper()
	state := newRecoveryCatalogueState(records)
	return newRecoveryCatalogueMockOver(t, state, configure...), state
}

// newRecoveryCatalogueMockOver is newRecoveryCatalogueMock over an existing
// state, so a test can swap a differently-behaving catalogue over the same
// records.
func newRecoveryCatalogueMockOver(t testing.TB, state *recoveryCatalogueState, configure ...func(*portsmocks.MockCatalogue, *recoveryCatalogueState)) *portsmocks.MockCatalogue {
	t.Helper()
	catalogue := portsmocks.NewMockCatalogue(t)
	for _, apply := range configure {
		apply(catalogue, state)
	}
	catalogue.EXPECT().Records().RunAndReturn(state.Records).Maybe()
	catalogue.EXPECT().Record(mock.Anything).RunAndReturn(state.Record).Maybe()
	catalogue.EXPECT().Create(mock.Anything).RunAndReturn(state.Create).Maybe()
	catalogue.EXPECT().Delete(mock.Anything).RunAndReturn(state.Delete).Maybe()
	catalogue.EXPECT().Replace(mock.Anything, mock.Anything).RunAndReturn(state.Replace).Maybe()
	catalogue.EXPECT().UpdateMetadata(mock.Anything).RunAndReturn(state.UpdateMetadata).Maybe()
	catalogue.EXPECT().Sync().RunAndReturn(state.Sync).Maybe()
	catalogue.EXPECT().Close().Return(nil).Maybe()
	return catalogue
}

// mustDurableRecords lists the catalogue's records.
func mustDurableRecords(t *testing.T, c interface {
	Records() ([]domain.CatalogueRecord, error)
}) []domain.CatalogueRecord {
	t.Helper()
	records, err := c.Records()
	if err != nil {
		t.Fatalf("listing catalogue records: %v", err)
	}
	return records
}

// recoveryRepositoryState is the in-memory checkpoint store a recovery
// repository mock serves, plus the calls it observed. It is a plain recorder;
// tests build it as a literal and read its fields after the run.
type recoveryRepositoryState struct {
	mu          sync.Mutex
	generations map[string]ports.SnapshotGeneration
	errors      map[string]error
	loads       map[string]int
	repairs     map[string]domain.CheckpointRef
	deleted     []domain.IncarnationID
	onLoad      func()
	repairErr   error
	deleteErr   error
	repairCalls int
}

func (r *recoveryRepositoryState) load(name string, ref ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loads == nil {
		r.loads = make(map[string]int)
	}
	r.loads[name]++
	if r.onLoad != nil {
		r.onLoad()
	}
	if err := r.errors[name]; err != nil {
		return ports.SnapshotGeneration{}, err
	}
	generation, ok := r.generations[name]
	if !ok || generation.Generation != ref.Generation {
		return ports.SnapshotGeneration{}, errors.New("checkpoint unavailable")
	}
	return generation, nil
}

func (r *recoveryRepositoryState) reconcile(id domain.IncarnationID, ref ports.CheckpointRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.repairCalls++
	if r.repairErr != nil {
		return r.repairErr
	}
	if r.repairs == nil {
		r.repairs = make(map[string]domain.CheckpointRef)
	}
	r.repairs[id.String()] = ref
	return nil
}

func (r *recoveryRepositoryState) deleteIncarnation(id domain.IncarnationID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted = append(r.deleted, id)
	return r.deleteErr
}

// newRecoveryRepositoryMock returns a ports.SnapshotRepository mock whose
// LoadCheckpoint, ReconcileCheckpoint and DeleteIncarnation run against state
// (nil allocates an empty one); the other methods keep the
// newSnapshotRepositoryMock no-op defaults. configure runs first and may
// override any method.
func newRecoveryRepositoryMock(t testing.TB, state *recoveryRepositoryState, configure ...func(*portsmocks.MockSnapshotRepository, *recoveryRepositoryState)) *portsmocks.MockSnapshotRepository {
	t.Helper()
	if state == nil {
		state = &recoveryRepositoryState{}
	}
	var overrides []func(*portsmocks.MockSnapshotRepository)
	for _, apply := range configure {
		overrides = append(overrides, func(m *portsmocks.MockSnapshotRepository) { apply(m, state) })
	}
	overrides = append(overrides, func(m *portsmocks.MockSnapshotRepository) {
		m.EXPECT().LoadCheckpoint(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, _ domain.IncarnationID, name string, ref ports.CheckpointRef) (ports.SnapshotGeneration, error) {
				return state.load(name, ref)
			}).Maybe()
		m.EXPECT().ReconcileCheckpoint(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, id domain.IncarnationID, ref ports.CheckpointRef) error {
				return state.reconcile(id, ref)
			}).Maybe()
		m.EXPECT().DeleteIncarnation(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, id domain.IncarnationID) error {
				return state.deleteIncarnation(id)
			}).Maybe()
	})
	return newSnapshotRepositoryMock(t, overrides...)
}

// newRecoveryCatalogueOnly returns just the catalogue mock for call sites that
// never touch its in-memory state.
func newRecoveryCatalogueOnly(t testing.TB, records []domain.CatalogueRecord) *portsmocks.MockCatalogue {
	t.Helper()
	catalogue, _ := newRecoveryCatalogueMock(t, records)
	return catalogue
}
