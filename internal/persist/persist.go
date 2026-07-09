package persist

import (
	"log/slog"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/kv"
)

const filename = "sessions.kv"

// StorePath returns the canonical path for the persisted session metadata store.
func StorePath(dir string) string {
	return filepath.Join(dir, filename)
}

// Persister stores named session metadata. A nil Store makes all mutating
// operations no-ops, so callers can keep persistence optional.
type Persister struct {
	store ports.Store
	mu    sync.Mutex
}

// Open opens the session persister under dir.
func Open(dir string) (*Persister, error) {
	store, err := kv.Open(StorePath(dir))
	if err != nil {
		return nil, err
	}
	return New(store), nil
}

// New wraps store in a Persister. Passing nil creates a no-op persister.
func New(store ports.Store) *Persister {
	return &Persister{store: store}
}

// LoadReadOnly replays the session store under dir without mutating it.
func LoadReadOnly(dir string) ([]Record, error) {
	data, err := kv.Replay(StorePath(dir))
	if err != nil {
		return nil, err
	}
	return decodeAll(data)
}

// Save persists a structural session record and syncs it to disk.
func (p *Persister) Save(r Record) error {
	if p == nil || p.store == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	value, err := encodeRecordValue(r)
	if err != nil {
		return err
	}
	if err := p.store.Set([]byte(r.Name), value); err != nil {
		return err
	}
	return p.store.Sync()
}

// Touch updates hot-path session cwd metadata without forcing an fsync.
func (p *Persister) Touch(name, cwd string, at int64) error {
	if p == nil || p.store == nil {
		return nil
	}
	if name == "" {
		return errEmptyName
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	createdAt := at
	lastUsedSeq := uint64(0)
	var tabNames []string
	if v, ok := p.store.Get([]byte(name)); ok {
		if r, err := decodeRecordValue(name, v); err == nil {
			createdAt = r.CreatedAt
			lastUsedSeq = r.LastUsedSeq
			tabNames = r.TabNames
		}
	}

	value, err := encodeRecordValue(Record{Name: name, Cwd: cwd, CreatedAt: createdAt, UpdatedAt: at, LastUsedSeq: lastUsedSeq, TabNames: tabNames})
	if err != nil {
		return err
	}
	return p.store.Set([]byte(name), value)
}

// TouchMRU updates hot-path session recency without forcing an fsync.
func (p *Persister) TouchMRU(name string, lastUsedSeq uint64) error {
	if p == nil || p.store == nil {
		return nil
	}
	if name == "" {
		return errEmptyName
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	r := Record{Name: name, LastUsedSeq: lastUsedSeq}
	if v, ok := p.store.Get([]byte(name)); ok {
		if decoded, err := decodeRecordValue(name, v); err == nil {
			r = decoded
			r.LastUsedSeq = lastUsedSeq
		}
	}
	value, err := encodeRecordValue(r)
	if err != nil {
		return err
	}
	return p.store.Set([]byte(name), value)
}

// Delete removes a persisted session record and syncs the structural change.
func (p *Persister) Delete(name string) error {
	if p == nil || p.store == nil {
		return nil
	}
	if name == "" {
		return errEmptyName
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.store.Delete([]byte(name)); err != nil {
		return err
	}
	return p.store.Sync()
}

// LoadAll returns all persisted session records sorted by name.
func (p *Persister) LoadAll() ([]Record, error) {
	if p == nil || p.store == nil {
		return nil, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	data := make(map[string][]byte)
	p.store.Range(func(k, v []byte) bool {
		data[string(k)] = append([]byte(nil), v...)
		return true
	})
	return decodeAll(data)
}

// Close closes the underlying store.
func (p *Persister) Close() error {
	if p == nil || p.store == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.store.Close()
}

func decodeAll(data map[string][]byte) ([]Record, error) {
	records := make([]Record, 0, len(data))
	for name, value := range data {
		r, err := decodeRecordValue(name, value)
		if err != nil {
			slog.Warn("skipping malformed persisted session", "session", name, "err", err)
			continue
		}
		records = append(records, r)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Name < records[j].Name })
	return records, nil
}
