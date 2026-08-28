package persist

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/pkg/kv"
)

const filename = "sessions.kv"

var errPersistenceUnavailable = errors.New("persist: catalogue unavailable")

// ErrCatalogueDurability reports a failed identity write whose durability is
// ambiguous. A Persister remains fenced after this error so rejected state
// cannot be made durable by a later flush.
var ErrCatalogueDurability = errors.New("persist: catalogue durability rejected")

// ErrCatalogueUnreadable reports durable state that exists but cannot be
// decoded. vev never repairs or erases it; the operator resets explicitly.
var ErrCatalogueUnreadable = errors.New("persist: catalogue unreadable")

// OpenResult is the outcome of opening durable session state at startup.
type OpenResult struct {
	Catalogue           *Persister
	Records             []domain.CatalogueRecord
	IncompatibleRecords []domain.CatalogueRecord
	NewInstall          bool
}

func StorePath(dir string) string { return filepath.Join(dir, filename) }

type Persister struct {
	store ports.Store
	mu    sync.Mutex

	incarnationOwners map[domain.IncarnationID]string
	nameIncarnations  map[string]domain.IncarnationID
	incarnationIndex  bool
	terminalErr       error
}

// KVStore adapts the reusable whole-file store to the persistence port.
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

func (s *KVStore) CloseWithoutSync() error { return s.store.CloseWithoutSync() }

// Open opens an existing strict catalogue and never creates an unproven empty one.
func Open(dir string) (*Persister, error) {
	p, _, err := openCurrentCatalogue(dir, false)
	return p, err
}

// OpenOrCreate opens the catalogue, or creates a proven-empty one when no
// durable state exists. Any state that exists but does not decode is returned
// as ErrCatalogueUnreadable and left untouched on disk.
func OpenOrCreate(dir string) (OpenResult, error) {
	path := StorePath(dir)
	_, statErr := os.Stat(path)
	existed := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return OpenResult{}, fmt.Errorf("%w: %s: %w", ErrCatalogueUnreadable, path, statErr)
	}
	catalogue, records, err := openCurrentCatalogue(dir, true)
	if err != nil {
		return OpenResult{}, fmt.Errorf("%w: %s: %w", ErrCatalogueUnreadable, path, err)
	}
	incompatible, err := catalogue.loadIncompatibleRecords()
	if err != nil {
		return OpenResult{}, errors.Join(fmt.Errorf("%w: %s: %w", ErrCatalogueUnreadable, path, err), catalogue.Close())
	}
	return OpenResult{Catalogue: catalogue, Records: records, IncompatibleRecords: incompatible, NewInstall: !existed}, nil
}

func New(store ports.Store) *Persister { return &Persister{store: store} }

func cataloguePresent(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return false, nil
}

func openCurrentCatalogue(dir string, createProvenEmpty bool) (*Persister, []domain.CatalogueRecord, error) {
	path := StorePath(dir)
	present, err := cataloguePresent(path)
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
// A fresh install with no main catalogue is not an error. Stray temporary
// files are ignored, while malformed main files fail closed.
func LoadCatalogueReadOnly(dir string) ([]domain.CatalogueRecord, error) {
	p, records, err := openCurrentCatalogue(dir, false)
	if err != nil {
		if errors.Is(err, errPersistenceUnavailable) {
			return []domain.CatalogueRecord{}, nil
		}
		return nil, fmt.Errorf("%w: %s: %w", ErrCatalogueUnreadable, StorePath(dir), err)
	}
	if err := p.Close(); err != nil {
		return nil, err
	}
	return records, nil
}

func (p *Persister) Save(record domain.CatalogueRecord) error { return p.Replace(record.Name, record) }
func (p *Persister) Create(record domain.CatalogueRecord) error {
	if p == nil {
		return nil
	}
	if p.store == nil {
		return errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.terminalLocked(); err != nil {
		return err
	}
	if _, exists := p.store.Get([]byte(record.Name)); exists {
		return errors.New("persist: session already exists")
	}
	return p.applyLocked(map[string]*domain.CatalogueRecord{record.Name: &record}, true)
}

func (p *Persister) Records() ([]domain.CatalogueRecord, error) { return p.LoadCatalogue() }
func (p *Persister) Record(name string) (domain.CatalogueRecord, bool, error) {
	if p == nil {
		return domain.CatalogueRecord{}, false, nil
	}
	if p.store == nil {
		return domain.CatalogueRecord{}, false, errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.terminalLocked(); err != nil {
		return domain.CatalogueRecord{}, false, err
	}
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
	if err := p.terminalLocked(); err != nil {
		return err
	}
	return p.applyLocked(records, true)
}
func (p *Persister) applyLocked(records map[string]*domain.CatalogueRecord, durable bool) error {
	if len(records) == 0 {
		return errors.New("persist: empty catalogue batch")
	}
	if err := p.ensureIncarnationIndexLocked(); err != nil {
		return err
	}
	type mutation struct {
		key    []byte
		value  []byte
		delete bool
	}
	changes := make([]mutation, 0, len(records))
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
			changes = append(changes, mutation{key: []byte(name), delete: true})
			continue
		}
		if record.Name != name {
			return errors.New("persist: catalogue key/name mismatch")
		}
		value, err := encodeRecordValue(*record)
		if err != nil {
			return err
		}
		changes = append(changes, mutation{key: []byte(name), value: value})
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
	previous := make(map[string]storedValue, len(changes))
	for _, change := range changes {
		value, ok := p.store.Get(change.key)
		previous[string(change.key)] = storedValue{value: value, exists: ok}
	}
	for _, change := range changes {
		var err error
		if change.delete {
			err = p.store.Delete(change.key)
		} else {
			err = p.store.Set(change.key, change.value)
		}
		if err != nil {
			return p.fenceLocked(errors.Join(err, p.restoreLocked(previous)))
		}
	}
	if durable {
		if err := p.store.Sync(); err != nil {
			return p.fenceLocked(errors.Join(err, p.restoreLocked(previous)))
		}
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

type storedValue struct {
	value  []byte
	exists bool
}

// restoreLocked restores only keys affected by the rejected identity mutation.
// It intentionally does not Sync: the Persister is fenced immediately after.
func (p *Persister) restoreLocked(previous map[string]storedValue) error {
	var err error
	for key, old := range previous {
		if old.exists {
			err = errors.Join(err, p.store.Set([]byte(key), old.value))
		} else {
			err = errors.Join(err, p.store.Delete([]byte(key)))
		}
	}
	return err
}

func (p *Persister) fenceLocked(cause error) error {
	if p.terminalErr == nil {
		p.terminalErr = fmt.Errorf("%w: %w", ErrCatalogueDurability, cause)
	}
	p.incarnationIndex = false
	return p.terminalErr
}

func (p *Persister) terminalLocked() error { return p.terminalErr }

func (p *Persister) ensureIncarnationIndexLocked() error {
	if err := p.terminalLocked(); err != nil {
		return err
	}
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

// UpdateMetadata buffers ordinary session metadata on the existing authoritative
// incarnation. Recovery and transaction fields are retained from the stored
// record rather than accepted from a potentially stale runtime copy. Sync or the
// next identity operation makes the update durable.
func (p *Persister) UpdateMetadata(update domain.CatalogueMetadataUpdate) error {
	if p == nil {
		return nil
	}
	if p.store == nil {
		return errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.terminalLocked(); err != nil {
		return err
	}
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
	if update.TabRecords != nil {
		current.TabRecords = append([]domain.CatalogueTabRecord(nil), (*update.TabRecords)...)
	}
	return p.applyLocked(map[string]*domain.CatalogueRecord{current.Name: &current}, false)
}

// Sync makes every buffered catalogue mutation durable.
func (p *Persister) Sync() error {
	if p == nil {
		return nil
	}
	if p.store == nil {
		return errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.terminalLocked(); err != nil {
		return err
	}
	if err := p.store.Sync(); err != nil {
		return p.fenceLocked(err)
	}
	return nil
}

func (p *Persister) Delete(name string) error {
	return p.Apply(map[string]*domain.CatalogueRecord{name: nil})
}
func (p *Persister) LoadAll() ([]domain.CatalogueRecord, error) { return p.LoadCatalogue() }

func (p *Persister) loadIncompatibleRecords() ([]domain.CatalogueRecord, error) {
	if p == nil || p.store == nil {
		return nil, errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.terminalLocked(); err != nil {
		return nil, err
	}
	var records []domain.CatalogueRecord
	var decodeErr error
	p.store.Range(func(key, value []byte) bool {
		stored, err := decodeStoredRecordValue(string(key), value)
		if err != nil {
			decodeErr = err
			return false
		}
		if stored.protocolVersion != protocol.Version {
			records = append(records, stored.record)
		}
		return true
	})
	sort.Slice(records, func(i, j int) bool { return records[i].Name < records[j].Name })
	return records, decodeErr
}

func (p *Persister) LoadCatalogue() ([]domain.CatalogueRecord, error) {
	if p == nil {
		return []domain.CatalogueRecord{}, nil
	}
	if p.store == nil {
		return nil, errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.terminalLocked(); err != nil {
		return nil, err
	}
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
	if err := p.terminalLocked(); err != nil {
		if closeErr := p.store.CloseWithoutSync(); closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	}
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
