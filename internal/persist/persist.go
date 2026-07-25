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
	"github.com/bnema/vev/pkg/kv"
)

const filename = "sessions.kv"

var errPersistenceUnavailable = errors.New("persist: catalogue unavailable")

func StorePath(dir string) string { return filepath.Join(dir, filename) }

type Persister struct {
	store ports.Store
	mu    sync.Mutex
}

// Open opens an existing strict catalogue and never creates an unproven empty one.
func Open(dir string) (*Persister, error) {
	p, _, err := openCurrentCatalogue(dir, false)
	return p, err
}
func New(store ports.Store) *Persister { return &Persister{store: store} }

func openCurrentCatalogue(dir string, createProvenEmpty bool) (*Persister, []domain.CatalogueRecord, error) {
	path := StorePath(dir)
	absent := true
	for _, candidate := range []string{path, path + ".next", path + ".prev"} {
		_, err := os.Stat(candidate)
		if err == nil {
			absent = false
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, nil, err
		}
	}
	if absent && !createProvenEmpty {
		return nil, nil, fmt.Errorf("%w: no catalogue candidates", errPersistenceUnavailable)
	}
	store, err := kv.Open(path)
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
func LoadCatalogueReadOnly(dir string) ([]domain.CatalogueRecord, error) {
	path := StorePath(dir)
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	data, err := kv.Replay(path)
	if err != nil {
		return nil, err
	}
	return decodeAll(data)
}

func (p *Persister) Save(record domain.CatalogueRecord) error { return p.Replace(record.Name, record) }
func (p *Persister) Apply(records map[string]*domain.CatalogueRecord) error {
	if p == nil || p.store == nil {
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
	projected := make(map[string]domain.CatalogueRecord)
	var decodeErr error
	p.store.Range(func(key, value []byte) bool {
		record, err := decodeRecordValue(string(key), value)
		if err != nil {
			decodeErr = err
			return false
		}
		projected[string(key)] = record
		return true
	})
	if decodeErr != nil {
		return decodeErr
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
			delete(projected, name)
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
		projected[name] = *record
		changes = append(changes, ports.StoreChange{Key: []byte(name), Value: value})
	}
	all := make([]domain.CatalogueRecord, 0, len(projected))
	for _, record := range projected {
		all = append(all, record)
	}
	if err := validateUniqueIncarnations(all); err != nil {
		return err
	}
	if err := p.store.Batch(changes); err != nil {
		return err
	}
	return p.store.Sync()
}
func (p *Persister) Rename(oldName string, next domain.CatalogueRecord) error {
	if oldName == next.Name {
		return errors.New("persist: rename requires distinct names")
	}
	return p.Apply(map[string]*domain.CatalogueRecord{oldName: nil, next.Name: &next})
}
func (p *Persister) Replace(name string, next domain.CatalogueRecord) error {
	return p.Apply(map[string]*domain.CatalogueRecord{name: &next})
}

// Touch updates metadata while retaining the durable identity and recovery state.
func (p *Persister) Touch(name, cwd string, at int64) error {
	if p == nil || p.store == nil {
		return errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	value, ok := p.store.Get([]byte(name))
	if !ok {
		return errors.New("persist: session not found")
	}
	record, err := decodeRecordValue(name, value)
	if err != nil {
		return err
	}
	record.Cwd, record.UpdatedAt = cwd, at
	encoded, err := encodeRecordValue(record)
	if err != nil {
		return err
	}
	return p.store.Set([]byte(name), encoded)
}
func (p *Persister) TouchMRU(name string, sequence uint64) error {
	if p == nil || p.store == nil {
		return errPersistenceUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	value, ok := p.store.Get([]byte(name))
	if !ok {
		return errors.New("persist: session not found")
	}
	record, err := decodeRecordValue(name, value)
	if err != nil {
		return err
	}
	record.LastUsedSeq = sequence
	encoded, err := encodeRecordValue(record)
	if err != nil {
		return err
	}
	return p.store.Set([]byte(name), encoded)
}
func (p *Persister) Delete(name string) error {
	return p.Apply(map[string]*domain.CatalogueRecord{name: nil})
}
func (p *Persister) LoadAll() ([]domain.CatalogueRecord, error) { return p.LoadCatalogue() }
func (p *Persister) LoadCatalogue() ([]domain.CatalogueRecord, error) {
	if p == nil || p.store == nil {
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
		return errPersistenceUnavailable
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
