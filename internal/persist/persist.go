package persist

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/kv"
)

const filename = "sessions.kv"

var errPersistenceUnavailable = errors.New("persist: catalogue unavailable")

func StorePath(dir string) string { return filepath.Join(dir, filename) }

type Persister struct {
	store ports.Store
	mu    sync.Mutex

	incarnationOwners map[domain.IncarnationID]string
	nameIncarnations  map[string]domain.IncarnationID
	incarnationIndex  bool
}

// KVStore adapts the reusable WAL implementation to the persistence port.
type KVStore struct{ store *kv.Store }

func OpenStore(path string) (*KVStore, error) {
	store, err := kv.Open(path)
	if err != nil {
		return nil, err
	}
	return &KVStore{store: store}, nil
}

func (s *KVStore) Get(key []byte) ([]byte, bool) { return s.store.Get(key) }
func (s *KVStore) Set(key, value []byte) error   { return s.store.Set(key, value) }
func (s *KVStore) Delete(key []byte) error       { return s.store.Delete(key) }
func (s *KVStore) Range(fn func(key, value []byte) bool) {
	s.store.Range(fn)
}
func (s *KVStore) Sync() error  { return s.store.Sync() }
func (s *KVStore) Close() error { return s.store.Close() }
func (s *KVStore) Batch(changes []ports.StoreChange) error {
	batch := make([]kv.BatchChange, len(changes))
	for i, change := range changes {
		batch[i] = kv.BatchChange{Key: change.Key, Value: change.Value, Delete: change.Delete}
	}
	return s.store.Batch(batch)
}

// Open opens an existing strict catalogue and never creates an unproven empty one.
func Open(dir string) (*Persister, error) {
	p, _, err := openCurrentCatalogue(dir, false)
	return p, err
}
func New(store ports.Store) *Persister { return &Persister{store: store} }

func openCurrentCatalogue(dir string, createProvenEmpty bool) (*Persister, []domain.CatalogueRecord, error) {
	path := StorePath(dir)
	present, err := catalogueCandidatesPresent(path)
	if err != nil {
		return nil, nil, err
	}
	if !present && !createProvenEmpty {
		return nil, nil, fmt.Errorf("%w: no catalogue candidates", errPersistenceUnavailable)
	}
	store, err := OpenStore(path)
	if err != nil {
		return nil, nil, err
	}
	p := New(store)
	records, err := p.LoadCatalogue()
	if err != nil {
		return nil, nil, errors.Join(err, p.Close())
	}
	return p, records, nil
}

func LoadReadOnly(dir string) ([]domain.CatalogueRecord, error) { return LoadCatalogueReadOnly(dir) }

// LoadCatalogueReadOnly loads the catalogue without retaining a store handle.
// A fresh install with no catalogue candidate at all is not an error: it
// returns an empty slice. Whenever any candidate (sessions.kv, .next, or
// .prev) exists, it goes through the same fixed-path compaction recovery the
// daemon applies on startup (via openCurrentCatalogue / kv.Open), so a
// .next-only or .prev-only crash state still yields every valid record.
// Malformed or corrupt data is still a hard error.
func LoadCatalogueReadOnly(dir string) ([]domain.CatalogueRecord, error) {
	p, records, err := openCurrentCatalogue(dir, false)
	if err != nil {
		if errors.Is(err, errPersistenceUnavailable) {
			return []domain.CatalogueRecord{}, nil
		}
		return nil, err
	}
	if err := p.Close(); err != nil {
		return nil, err
	}
	return records, nil
}

func (p *Persister) Save(record domain.CatalogueRecord) error { return p.Replace(record.Name, record) }
func (p *Persister) Create(record domain.CatalogueRecord) error {
	if p == nil || p.store == nil {
		return errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.store.Get([]byte(record.Name)); exists {
		return errors.New("persist: session already exists")
	}
	return p.applyLocked(map[string]*domain.CatalogueRecord{record.Name: &record})
}

func (p *Persister) Records() ([]domain.CatalogueRecord, error) { return p.LoadCatalogue() }
func (p *Persister) Record(name string) (domain.CatalogueRecord, bool, error) {
	if p == nil || p.store == nil {
		return domain.CatalogueRecord{}, false, errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	value, ok := p.store.Get([]byte(name))
	if !ok {
		return domain.CatalogueRecord{}, false, nil
	}
	record, err := decodeRecordValue(name, value)
	if err != nil {
		return domain.CatalogueRecord{}, false, err
	}
	return record, true, nil
}
func (p *Persister) Apply(records map[string]*domain.CatalogueRecord) error {
	if p == nil {
		return nil
	}
	if p.store == nil {
		return errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.applyLocked(records)
}
func (p *Persister) applyLocked(records map[string]*domain.CatalogueRecord) error {
	if len(records) == 0 {
		return errors.New("persist: empty catalogue batch")
	}
	if err := p.ensureIncarnationIndexLocked(); err != nil {
		return err
	}
	changes := make([]ports.StoreChange, 0, len(records))
	keys := make([]string, 0, len(records))
	for name := range records {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	for _, name := range keys {
		record := records[name]
		if err := domain.ValidateSessionName(name); err != nil {
			return err
		}
		if record == nil {
			changes = append(changes, ports.StoreChange{Key: []byte(name), Delete: true})
			continue
		}
		if record.Name != name {
			return errors.New("persist: catalogue key/name mismatch")
		}
		value, err := encodeRecordValue(*record)
		if err != nil {
			return err
		}
		changes = append(changes, ports.StoreChange{Key: []byte(name), Value: value})
	}
	incoming := make(map[domain.IncarnationID]string, len(records))
	for name, record := range records {
		if record == nil {
			continue
		}
		if previous, ok := incoming[record.IncarnationID]; ok && previous != name {
			return fmt.Errorf("persist: duplicate incarnation for %q and %q", previous, name)
		}
		incoming[record.IncarnationID] = name
		if owner, ok := p.incarnationOwners[record.IncarnationID]; ok && owner != name {
			ownerNext, ownerTouched := records[owner]
			if !ownerTouched || (ownerNext != nil && ownerNext.IncarnationID == record.IncarnationID) {
				return fmt.Errorf("persist: duplicate incarnation for %q and %q", owner, name)
			}
		}
	}
	if err := p.store.Batch(changes); err != nil {
		p.incarnationIndex = false
		return err
	}
	if err := p.store.Sync(); err != nil {
		p.incarnationIndex = false
		return err
	}
	for name := range records {
		if id, ok := p.nameIncarnations[name]; ok {
			delete(p.incarnationOwners, id)
			delete(p.nameIncarnations, name)
		}
	}
	for name, record := range records {
		if record != nil {
			p.incarnationOwners[record.IncarnationID] = name
			p.nameIncarnations[name] = record.IncarnationID
		}
	}
	return nil
}

func (p *Persister) ensureIncarnationIndexLocked() error {
	if p.incarnationIndex {
		return nil
	}
	owners := make(map[domain.IncarnationID]string)
	names := make(map[string]domain.IncarnationID)
	var indexErr error
	p.store.Range(func(key, value []byte) bool {
		name := string(key)
		record, err := decodeRecordValue(name, value)
		if err != nil {
			indexErr = err
			return false
		}
		if previous, ok := owners[record.IncarnationID]; ok {
			indexErr = fmt.Errorf("persist: duplicate incarnation for %q and %q", previous, name)
			return false
		}
		owners[record.IncarnationID] = name
		names[name] = record.IncarnationID
		return true
	})
	if indexErr != nil {
		return indexErr
	}
	p.incarnationOwners = owners
	p.nameIncarnations = names
	p.incarnationIndex = true
	return nil
}
func (p *Persister) Rename(oldName string, next domain.CatalogueRecord) error {
	if p == nil {
		return nil
	}
	if p.store == nil {
		return errPersistenceUnavailable
	}
	if oldName == next.Name {
		return errors.New("persist: rename requires distinct names")
	}
	return p.Apply(map[string]*domain.CatalogueRecord{oldName: nil, next.Name: &next})
}
func (p *Persister) Replace(name string, next domain.CatalogueRecord) error {
	return p.Apply(map[string]*domain.CatalogueRecord{name: &next})
}

// UpdateMetadata atomically updates ordinary session metadata on the existing
// authoritative incarnation. Recovery and transaction fields are retained from
// the stored record rather than accepted from a potentially stale runtime copy.
func (p *Persister) UpdateMetadata(update domain.CatalogueMetadataUpdate) error {
	if p == nil {
		return nil
	}
	if p.store == nil {
		return errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	value, ok := p.store.Get([]byte(update.Name))
	if !ok {
		return errors.New("persist: session not found")
	}
	current, err := decodeRecordValue(update.Name, value)
	if err != nil {
		return err
	}
	if update.IncarnationID == (domain.IncarnationID{}) || current.IncarnationID != update.IncarnationID {
		return errors.New("persist: session incarnation changed")
	}
	if update.Cwd != nil {
		current.Cwd = *update.Cwd
	}
	if update.UpdatedAt != nil {
		current.UpdatedAt = *update.UpdatedAt
	}
	if update.LastUsedSeq != nil {
		current.LastUsedSeq = *update.LastUsedSeq
	}
	if update.TabNames != nil {
		current.TabNames = append([]string(nil), (*update.TabNames)...)
	}
	return p.applyLocked(map[string]*domain.CatalogueRecord{current.Name: &current})
}

func (p *Persister) Delete(name string) error {
	return p.Apply(map[string]*domain.CatalogueRecord{name: nil})
}
func (p *Persister) LoadAll() ([]domain.CatalogueRecord, error) { return p.LoadCatalogue() }
func (p *Persister) LoadCatalogue() ([]domain.CatalogueRecord, error) {
	if p == nil {
		return []domain.CatalogueRecord{}, nil
	}
	if p.store == nil {
		return nil, errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	data := make(map[string][]byte)
	p.store.Range(func(key, value []byte) bool { data[string(key)] = append([]byte(nil), value...); return true })
	return decodeAll(data)
}
func (p *Persister) Close() error {
	if p == nil || p.store == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.store.Close()
}

func decodeAll(data map[string][]byte) ([]domain.CatalogueRecord, error) {
	records := make([]domain.CatalogueRecord, 0, len(data))
	for name, value := range data {
		record, err := decodeRecordValue(name, value)
		if err != nil {
			return nil, fmt.Errorf("persist: decode catalogue key %q: %w", name, err)
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Name < records[j].Name })
	if err := validateUniqueIncarnations(records); err != nil {
		return nil, err
	}
	return records, nil
}
func validateUniqueIncarnations(records []domain.CatalogueRecord) error {
	seen := make(map[domain.IncarnationID]string, len(records))
	for _, record := range records {
		if err := record.Validate(); err != nil {
			return err
		}
		if previous, ok := seen[record.IncarnationID]; ok {
			return fmt.Errorf("persist: duplicate incarnation for %q and %q", previous, record.Name)
		}
		seen[record.IncarnationID] = record.Name
	}
	return nil
}
