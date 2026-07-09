package persist

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func privateDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "vev")
}

func TestRecordCodecRoundTrip(t *testing.T) {
	r := Record{Name: "work", Cwd: "/tmp/project", CreatedAt: 11, UpdatedAt: 22, LastUsedSeq: 33, TabNames: []string{"shell", "logs"}}
	value, err := encodeRecordValue(r)
	require.NoError(t, err)

	got, err := decodeRecordValue(r.Name, value)
	require.NoError(t, err)
	require.Equal(t, r, got)
}

func TestRecordCodecOldRecordDefaultsLastUsed(t *testing.T) {
	old := []byte{0, 0, 0, 4, '/', 't', 'm', 'p'}
	old = append(old, 0, 0, 0, 0, 0, 0, 0, 11)
	old = append(old, 0, 0, 0, 0, 0, 0, 0, 22)

	got, err := decodeRecordValue("work", old)
	require.NoError(t, err)
	require.Equal(t, Record{Name: "work", Cwd: "/tmp", CreatedAt: 11, UpdatedAt: 22}, got)
}

func TestRecordCodecRejectsMalformedData(t *testing.T) {
	_, err := encodeRecordValue(Record{Cwd: "/tmp"})
	require.ErrorIs(t, err, errEmptyName)

	badValues := [][]byte{
		nil,
		{0, 0, 0},
		{0, 0, 0, 5, 'a'},
		{0, 0, 0, 1, 'a', 0},
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	for _, value := range badValues {
		_, err := decodeRecordValue("name", value)
		require.ErrorIs(t, err, errMalformedRecord)
	}

	_, err = decodeRecordValue("", make([]byte, 20))
	require.ErrorIs(t, err, errEmptyName)
}

func TestNilPersisterNoOps(t *testing.T) {
	var p *Persister
	require.NoError(t, p.Save(Record{Name: "a", Cwd: "/tmp", CreatedAt: 1, UpdatedAt: 1}))
	require.NoError(t, p.Touch("a", "/tmp/next", 2))
	require.NoError(t, p.Delete("a"))
	records, err := p.LoadAll()
	require.NoError(t, err)
	require.Empty(t, records)
	require.NoError(t, p.Close())

	p = New(nil)
	require.NoError(t, p.Save(Record{Name: "a", Cwd: "/tmp", CreatedAt: 1, UpdatedAt: 1}))
	require.NoError(t, p.Touch("a", "/tmp/next", 2))
	require.NoError(t, p.Delete("a"))
	records, err = p.LoadAll()
	require.NoError(t, err)
	require.Empty(t, records)
	require.NoError(t, p.Close())
}

func TestPersisterSaveTouchDeleteLoadAll(t *testing.T) {
	dir := privateDir(t)
	p, err := Open(dir)
	require.NoError(t, err)

	require.NoError(t, p.Save(Record{Name: "b", Cwd: "/b", CreatedAt: 10, UpdatedAt: 10, LastUsedSeq: 40, TabNames: []string{"shell"}}))
	require.NoError(t, p.Save(Record{Name: "a", Cwd: "/a", CreatedAt: 20, UpdatedAt: 20, LastUsedSeq: 50}))
	require.NoError(t, p.Touch("b", "/b/next", 30))
	require.NoError(t, p.TouchMRU("b", 60))

	records, err := p.LoadAll()
	require.NoError(t, err)
	require.Equal(t, []Record{
		{Name: "a", Cwd: "/a", CreatedAt: 20, UpdatedAt: 20, LastUsedSeq: 50},
		{Name: "b", Cwd: "/b/next", CreatedAt: 10, UpdatedAt: 30, LastUsedSeq: 60, TabNames: []string{"shell"}},
	}, records)

	require.NoError(t, p.Delete("a"))
	records, err = p.LoadAll()
	require.NoError(t, err)
	require.Equal(t, []Record{{Name: "b", Cwd: "/b/next", CreatedAt: 10, UpdatedAt: 30, LastUsedSeq: 60, TabNames: []string{"shell"}}}, records)
	require.NoError(t, p.Close())
}

func TestDecodeAllSkipsMalformedRecords(t *testing.T) {
	good, err := encodeRecordValue(Record{Name: "good", Cwd: "/ok", CreatedAt: 1, UpdatedAt: 2, LastUsedSeq: 3})
	require.NoError(t, err)

	records, err := decodeAll(map[string][]byte{
		"bad":  []byte{0, 0, 0, 5, 'x'},
		"good": good,
	})
	require.NoError(t, err)
	require.Equal(t, []Record{{Name: "good", Cwd: "/ok", CreatedAt: 1, UpdatedAt: 2, LastUsedSeq: 3}}, records)
}

func TestLoadReadOnlyUsesReplay(t *testing.T) {
	dir := privateDir(t)
	p, err := Open(dir)
	require.NoError(t, err)
	require.NoError(t, p.Save(Record{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 2, LastUsedSeq: 3}))
	require.NoError(t, p.Close())

	records, err := LoadReadOnly(dir)
	require.NoError(t, err)
	require.Equal(t, []Record{{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 2, LastUsedSeq: 3}}, records)

	// Missing stores replay as empty rather than creating the WAL.
	missing := filepath.Join(t.TempDir(), "missing")
	records, err = LoadReadOnly(missing)
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestSyncBehavior(t *testing.T) {
	store, state := newMockStore(t)
	p := New(store)

	require.NoError(t, p.Save(Record{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 1}))
	require.Equal(t, 1, state.sets)
	require.Equal(t, 1, state.syncs)

	require.NoError(t, p.Touch("work", "/work/next", 2))
	require.Equal(t, 2, state.sets)
	require.Equal(t, 1, state.syncs)

	require.NoError(t, p.TouchMRU("work", 3))
	require.Equal(t, 3, state.sets)
	require.Equal(t, 1, state.syncs)

	require.NoError(t, p.Delete("work"))
	require.Equal(t, 1, state.deletes)
	require.Equal(t, 2, state.syncs)
}

func TestPersisterSerializesReadModifyWriteWithStructuralSave(t *testing.T) {
	store := newBlockingGetStore()
	p := New(store)
	require.NoError(t, p.Save(Record{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 1, TabNames: []string{"old"}}))
	store.blockNextGet()

	touchDone := make(chan error, 1)
	go func() { touchDone <- p.TouchMRU("work", 2) }()
	<-store.getStarted

	saveDone := make(chan error, 1)
	go func() {
		saveDone <- p.Save(Record{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 3, LastUsedSeq: 2, TabNames: []string{"new"}})
	}()
	select {
	case err := <-saveDone:
		t.Fatalf("structural save interleaved with TouchMRU: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(store.releaseGet)
	require.NoError(t, <-touchDone)
	require.NoError(t, <-saveDone)

	records, err := p.LoadAll()
	require.NoError(t, err)
	require.Equal(t, []string{"new"}, records[0].TabNames)
}

func TestSavePropagatesSetErrorWithoutSync(t *testing.T) {
	store, state := newMockStore(t)
	state.errSet = errors.New("set failed")
	p := New(store)

	err := p.Save(Record{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 1})
	require.ErrorIs(t, err, state.errSet)
	require.Zero(t, state.syncs)
}

type mockStoreState struct {
	data    map[string][]byte
	sets    int
	deletes int
	syncs   int
	closed  bool
	errSet  error
}

type blockingGetStore struct {
	mu           sync.Mutex
	data         map[string][]byte
	block        bool
	getStarted   chan struct{}
	releaseGet   chan struct{}
	getStartOnce sync.Once
}

func newBlockingGetStore() *blockingGetStore {
	return &blockingGetStore{data: make(map[string][]byte), getStarted: make(chan struct{}), releaseGet: make(chan struct{})}
}

func (s *blockingGetStore) blockNextGet() {
	s.mu.Lock()
	s.block = true
	s.mu.Unlock()
}

func (s *blockingGetStore) Get(key []byte) ([]byte, bool) {
	s.mu.Lock()
	v, ok := s.data[string(key)]
	v = append([]byte(nil), v...)
	block := s.block
	if block {
		s.block = false
	}
	s.mu.Unlock()
	if block {
		s.getStartOnce.Do(func() { close(s.getStarted) })
		<-s.releaseGet
	}
	return v, ok
}

func (s *blockingGetStore) Set(key, value []byte) error {
	s.mu.Lock()
	s.data[string(key)] = append([]byte(nil), value...)
	s.mu.Unlock()
	return nil
}

func (s *blockingGetStore) Delete(key []byte) error {
	s.mu.Lock()
	delete(s.data, string(key))
	s.mu.Unlock()
	return nil
}

func (s *blockingGetStore) Range(fn func(k, v []byte) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, value := range s.data {
		if !fn([]byte(key), append([]byte(nil), value...)) {
			return
		}
	}
}

func (*blockingGetStore) Sync() error  { return nil }
func (*blockingGetStore) Close() error { return nil }

func newMockStore(t *testing.T) (*portsmocks.MockStore, *mockStoreState) {
	t.Helper()
	state := &mockStoreState{data: make(map[string][]byte)}
	store := portsmocks.NewMockStore(t)
	store.EXPECT().Get(mock.Anything).RunAndReturn(func(key []byte) ([]byte, bool) {
		v, ok := state.data[string(key)]
		return append([]byte(nil), v...), ok
	}).Maybe()
	store.EXPECT().Set(mock.Anything, mock.Anything).RunAndReturn(func(key, val []byte) error {
		state.sets++
		if state.errSet != nil {
			return state.errSet
		}
		state.data[string(key)] = append([]byte(nil), val...)
		return nil
	}).Maybe()
	store.EXPECT().Delete(mock.Anything).RunAndReturn(func(key []byte) error {
		state.deletes++
		delete(state.data, string(key))
		return nil
	}).Maybe()
	store.EXPECT().Range(mock.Anything).Run(func(fn func(k, v []byte) bool) {
		for k, v := range state.data {
			if !fn([]byte(k), append([]byte(nil), v...)) {
				return
			}
		}
	}).Maybe()
	store.EXPECT().Sync().RunAndReturn(func() error {
		state.syncs++
		return nil
	}).Maybe()
	store.EXPECT().Close().RunAndReturn(func() error {
		state.closed = true
		return nil
	}).Maybe()
	return store, state
}
