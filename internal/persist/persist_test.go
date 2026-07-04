package persist

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestRecordCodecRoundTrip(t *testing.T) {
	r := Record{Name: "work", Cwd: "/tmp/project", CreatedAt: 11, UpdatedAt: 22, TabNames: []string{"shell", "logs"}}
	value, err := encodeRecordValue(r)
	require.NoError(t, err)

	got, err := decodeRecordValue(r.Name, value)
	require.NoError(t, err)
	require.Equal(t, r, got)
}

func TestRecordCodecReadsLegacyRecordWithoutTabNames(t *testing.T) {
	cwd := "/tmp/project"
	value := make([]byte, 4+len(cwd)+8+8)
	binary.BigEndian.PutUint32(value[:4], uint32(len(cwd)))
	copy(value[4:], cwd)
	off := 4 + len(cwd)
	binary.BigEndian.PutUint64(value[off:off+8], 11)
	binary.BigEndian.PutUint64(value[off+8:off+16], 22)

	got, err := decodeRecordValue("work", value)
	require.NoError(t, err)
	require.Equal(t, Record{Name: "work", Cwd: cwd, CreatedAt: 11, UpdatedAt: 22}, got)
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
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255},
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

	require.NoError(t, p.Save(Record{Name: "b", Cwd: "/b", CreatedAt: 10, UpdatedAt: 10, TabNames: []string{"shell", "logs"}}))
	require.NoError(t, p.Save(Record{Name: "a", Cwd: "/a", CreatedAt: 20, UpdatedAt: 20}))
	require.NoError(t, p.Touch("b", "/b/next", 30))

	records, err := p.LoadAll()
	require.NoError(t, err)
	require.Equal(t, []Record{
		{Name: "a", Cwd: "/a", CreatedAt: 20, UpdatedAt: 20},
		{Name: "b", Cwd: "/b/next", CreatedAt: 10, UpdatedAt: 30, TabNames: []string{"shell", "logs"}},
	}, records)

	require.NoError(t, p.Delete("a"))
	records, err = p.LoadAll()
	require.NoError(t, err)
	require.Equal(t, []Record{{Name: "b", Cwd: "/b/next", CreatedAt: 10, UpdatedAt: 30, TabNames: []string{"shell", "logs"}}}, records)
	require.NoError(t, p.Close())
}

func TestDecodeAllSkipsMalformedRecords(t *testing.T) {
	good, err := encodeRecordValue(Record{Name: "good", Cwd: "/ok", CreatedAt: 1, UpdatedAt: 2})
	require.NoError(t, err)

	records, err := decodeAll(map[string][]byte{
		"bad":  []byte{0, 0, 0, 5, 'x'},
		"good": good,
	})
	require.NoError(t, err)
	require.Equal(t, []Record{{Name: "good", Cwd: "/ok", CreatedAt: 1, UpdatedAt: 2}}, records)
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
	store, state := newMockStore(t)
	p := New(store)

	require.NoError(t, p.Save(Record{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 1}))
	require.Equal(t, 1, state.sets)
	require.Equal(t, 1, state.syncs)

	require.NoError(t, p.Touch("work", "/work/next", 2))
	require.Equal(t, 2, state.sets)
	require.Equal(t, 1, state.syncs)

	require.NoError(t, p.Delete("work"))
	require.Equal(t, 1, state.deletes)
	require.Equal(t, 2, state.syncs)
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
