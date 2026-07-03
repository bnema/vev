package persist

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordCodecRoundTrip(t *testing.T) {
	r := Record{Name: "work", Cwd: "/tmp/project", CreatedAt: 11, UpdatedAt: 22}
	value, err := encodeRecordValue(r)
	require.NoError(t, err)

	got, err := decodeRecordValue(r.Name, value)
	require.NoError(t, err)
	require.Equal(t, r, got)
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
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
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
	dir := t.TempDir()
	p, err := Open(dir)
	require.NoError(t, err)

	require.NoError(t, p.Save(Record{Name: "b", Cwd: "/b", CreatedAt: 10, UpdatedAt: 10}))
	require.NoError(t, p.Save(Record{Name: "a", Cwd: "/a", CreatedAt: 20, UpdatedAt: 20}))
	require.NoError(t, p.Touch("b", "/b/next", 30))

	records, err := p.LoadAll()
	require.NoError(t, err)
	require.Equal(t, []Record{
		{Name: "a", Cwd: "/a", CreatedAt: 20, UpdatedAt: 20},
		{Name: "b", Cwd: "/b/next", CreatedAt: 10, UpdatedAt: 30},
	}, records)

	require.NoError(t, p.Delete("a"))
	records, err = p.LoadAll()
	require.NoError(t, err)
	require.Equal(t, []Record{{Name: "b", Cwd: "/b/next", CreatedAt: 10, UpdatedAt: 30}}, records)
	require.NoError(t, p.Close())
}

func TestLoadReadOnlyUsesReplay(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(dir)
	require.NoError(t, err)
	require.NoError(t, p.Save(Record{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 2}))
	require.NoError(t, p.Close())

	records, err := LoadReadOnly(dir)
	require.NoError(t, err)
	require.Equal(t, []Record{{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 2}}, records)

	// Missing stores replay as empty rather than creating the WAL.
	missing := filepath.Join(t.TempDir(), "missing")
	records, err = LoadReadOnly(missing)
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestSyncBehavior(t *testing.T) {
	store := newFakeStore()
	p := New(store)

	require.NoError(t, p.Save(Record{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 1}))
	require.Equal(t, 1, store.sets)
	require.Equal(t, 1, store.syncs)

	require.NoError(t, p.Touch("work", "/work/next", 2))
	require.Equal(t, 2, store.sets)
	require.Equal(t, 1, store.syncs)

	require.NoError(t, p.Delete("work"))
	require.Equal(t, 1, store.deletes)
	require.Equal(t, 2, store.syncs)
}

func TestSavePropagatesSetErrorWithoutSync(t *testing.T) {
	store := newFakeStore()
	store.errSet = errors.New("set failed")
	p := New(store)

	err := p.Save(Record{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 1})
	require.ErrorIs(t, err, store.errSet)
	require.Zero(t, store.syncs)
}

type fakeStore struct {
	data    map[string][]byte
	sets    int
	deletes int
	syncs   int
	closed  bool
	errSet  error
}

func newFakeStore() *fakeStore {
	return &fakeStore{data: make(map[string][]byte)}
}

func (s *fakeStore) Set(key, val []byte) error {
	s.sets++
	if s.errSet != nil {
		return s.errSet
	}
	s.data[string(key)] = append([]byte(nil), val...)
	return nil
}

func (s *fakeStore) Delete(key []byte) error {
	s.deletes++
	delete(s.data, string(key))
	return nil
}

func (s *fakeStore) Range(fn func(k, v []byte) bool) {
	for k, v := range s.data {
		if !fn([]byte(k), append([]byte(nil), v...)) {
			return
		}
	}
}

func (s *fakeStore) Sync() error {
	s.syncs++
	return nil
}

func (s *fakeStore) Close() error {
	s.closed = true
	return nil
}
