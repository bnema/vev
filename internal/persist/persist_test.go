package persist

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func privateDir(t *testing.T) string { t.Helper(); return filepath.Join(t.TempDir(), "vev") }
func validRecord(name string, id byte) domain.CatalogueRecord {
	return domain.CatalogueRecord{Name: name, IncarnationID: domain.IncarnationID{id}}
}

func TestOpenOrCreate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		write   []byte // nil means "no file at all"
		wantNew bool
		wantErr error
	}{
		{name: "absent file starts empty", write: nil, wantNew: true},
		{name: "garbage fails closed", write: []byte("not a vev catalogue"), wantErr: ErrCatalogueUnreadable},
		{name: "legacy headerless record fails closed", write: []byte{0x08, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef}, wantErr: ErrCatalogueUnreadable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := privateDir(t)
			if tt.write != nil {
				require.NoError(t, os.MkdirAll(dir, 0o700))
				require.NoError(t, os.WriteFile(StorePath(dir), tt.write, 0o600))
			}
			got, err := OpenOrCreate(dir)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				after, readErr := os.ReadFile(StorePath(dir))
				require.NoError(t, readErr)
				require.Equal(t, tt.write, after, "unreadable catalogue must remain untouched")
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { _ = got.Catalogue.Close() })
			require.Equal(t, tt.wantNew, got.NewInstall)
			require.Empty(t, got.Records)
		})
	}
}

func TestOpenOrCreateRejectsNonCurrentDurableStateWithoutMutation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		files map[string][]byte
	}{
		{
			name: "old VEVK version",
			files: map[string][]byte{
				filename: {'V', 'E', 'V', 'K', 0, 1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := privateDir(t)
			require.NoError(t, os.MkdirAll(dir, 0o700))
			for name, data := range tt.files {
				require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o600))
			}

			before := directoryBytes(t, dir)
			_, err := OpenOrCreate(dir)
			require.ErrorIs(t, err, ErrCatalogueUnreadable)
			after := directoryBytes(t, dir)
			for name, data := range before {
				require.Equal(t, data, after[name], "failure must preserve durable file %q", name)
			}
		})
	}
}

func directoryBytes(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	files := make(map[string][]byte, len(entries))
	for _, entry := range entries {
		require.False(t, entry.IsDir(), "unexpected directory %q", entry.Name())
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		files[entry.Name()] = data
	}
	return files
}

func TestCatalogueDecodeAllFailsOnMalformedRecord(t *testing.T) {
	good, err := encodeRecordValue(validRecord("good", 1))
	require.NoError(t, err)
	_, err = decodeAll(map[string][]byte{"bad": {0, 1}, "good": good})
	require.Error(t, err)
}

func TestCatalogueMutationsUseOneSync(t *testing.T) {
	store := portsmocks.NewMockStore(t)
	state := map[string][]byte{}
	store.EXPECT().Range(mock.Anything).Run(func(fn func([]byte, []byte) bool) {
		for key, value := range state {
			if !fn([]byte(key), value) {
				return
			}
		}
	}).Once()
	store.EXPECT().Get(mock.Anything).RunAndReturn(func(key []byte) ([]byte, bool) {
		value, ok := state[string(key)]
		return append([]byte(nil), value...), ok
	}).Times(3)
	store.EXPECT().Set(mock.Anything, mock.Anything).RunAndReturn(func(key, value []byte) error {
		state[string(key)] = append([]byte(nil), value...)
		return nil
	}).Times(2)
	store.EXPECT().Delete([]byte("one")).RunAndReturn(func(key []byte) error {
		delete(state, string(key))
		return nil
	}).Once()
	store.EXPECT().Sync().Return(nil).Twice()
	p := New(store)
	one := validRecord("one", 1)
	require.NoError(t, p.Replace("one", one))
	renamed := one
	renamed.Name = "renamed"
	renamed.Cwd = "/renamed"
	require.NoError(t, p.Rename("one", renamed))
	require.NotContains(t, state, "one")
	encoded, ok := state["renamed"]
	require.True(t, ok)
	got, err := decodeRecordValue("renamed", encoded)
	require.NoError(t, err)
	require.Equal(t, renamed, got)
}

func TestCatalogueMutationFailureDoesNotSync(t *testing.T) {
	store := portsmocks.NewMockStore(t)
	setErr := errors.New("set failed")
	store.EXPECT().Range(mock.Anything).Run(func(func([]byte, []byte) bool) {})
	store.EXPECT().Get([]byte("one")).Return(nil, false).Once()
	store.EXPECT().Set(mock.Anything, mock.Anything).Return(setErr)
	store.EXPECT().Delete([]byte("one")).Return(nil).Once()
	p := New(store)
	err := p.Replace("one", validRecord("one", 1))
	require.ErrorIs(t, err, setErr)
}

// TestCatalogueNilPersisterIsNoOp covers the ephemeral/optional-persistence
// sentinel: a nil *Persister. Every mutating and reading method must be a
// silent no-op so callers that never enable persistence don't need nil
// checks of their own.
func TestCatalogueNilPersisterIsNoOp(t *testing.T) {
	var p *Persister
	require.NoError(t, p.Save(validRecord("one", 1)))
	require.NoError(t, p.Apply(map[string]*domain.CatalogueRecord{"one": nil}))
	require.NoError(t, p.UpdateMetadata(validRecord("one", 1).MetadataUpdate()))
	require.NoError(t, p.Delete("one"))
	records, err := p.LoadAll()
	require.NoError(t, err)
	require.Empty(t, records)
	require.NoError(t, p.Close())
	got, err := p.Records()
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestCatalogueUnavailableStoreErrors covers a non-nil Persister with no
// backing store (e.g. New(nil)). Unlike the nil-persister sentinel above,
// this is a Persister that was supposed to have a store: silently no-oping
// here previously made recovery.Coordinator's checkpoint publish/retry/
// discard paths believe a durable commit happened when nothing was written.
// Every mutation and read-with-error must surface errPersistenceUnavailable,
// matching Create; only Records() (which ports.Catalogue gives no error
// return for) degrades to an empty slice, with the failure logged instead.
func TestCatalogueUnavailableStoreErrors(t *testing.T) {
	p := New(nil)
	require.ErrorIs(t, p.Save(validRecord("one", 1)), errPersistenceUnavailable)
	require.ErrorIs(t, p.Apply(map[string]*domain.CatalogueRecord{"one": nil}), errPersistenceUnavailable)
	require.ErrorIs(t, p.Rename("one", validRecord("two", 1)), errPersistenceUnavailable)
	require.ErrorIs(t, p.UpdateMetadata(validRecord("one", 1).MetadataUpdate()), errPersistenceUnavailable)
	require.ErrorIs(t, p.Delete("one"), errPersistenceUnavailable)
	require.ErrorIs(t, p.Create(validRecord("one", 1)), errPersistenceUnavailable)
	records, err := p.LoadAll()
	require.ErrorIs(t, err, errPersistenceUnavailable)
	require.Empty(t, records)
	got, err := p.Records()
	require.ErrorIs(t, err, errPersistenceUnavailable)
	require.Empty(t, got)
	require.NoError(t, p.Close())
}

func TestCatalogueRecordsPropagatesMalformedRecord(t *testing.T) {
	store := portsmocks.NewMockStore(t)
	store.EXPECT().Range(mock.Anything).Run(func(fn func([]byte, []byte) bool) {
		fn([]byte("bad"), []byte{0, 1})
	})
	records, err := New(store).Records()
	require.Error(t, err)
	require.Empty(t, records)
}

type failClosedStore struct {
	data    map[string][]byte
	durable map[string][]byte

	setFailures           int
	deleteFailures        int
	syncFailures          int
	restoreSetFailures    int
	restoreDeleteFailures int
	restoring             bool
	failure               error
	restoreFailure        error

	closeFlushed bool
	closeAborted bool
}

func newFailClosedStore(data map[string][]byte) *failClosedStore {
	return &failClosedStore{
		data:           copyStoreData(data),
		durable:        copyStoreData(data),
		failure:        errors.New("store write failed"),
		restoreFailure: errors.New("store restore failed"),
	}
}

func (s *failClosedStore) Get(key []byte) ([]byte, bool) {
	value, ok := s.data[string(key)]
	return append([]byte(nil), value...), ok
}

func (s *failClosedStore) Set(key, value []byte) error {
	s.data[string(key)] = append([]byte(nil), value...)
	if s.restoring && s.restoreSetFailures > 0 {
		s.restoreSetFailures--
		return s.restoreFailure
	}
	if s.setFailures > 0 {
		s.setFailures--
		return s.failure
	}
	return nil
}

func (s *failClosedStore) Delete(key []byte) error {
	delete(s.data, string(key))
	if s.restoring && s.restoreDeleteFailures > 0 {
		s.restoreDeleteFailures--
		return s.restoreFailure
	}
	if s.deleteFailures > 0 {
		s.deleteFailures--
		return s.failure
	}
	return nil
}

func (s *failClosedStore) Range(fn func([]byte, []byte) bool) {
	for key, value := range s.data {
		if !fn([]byte(key), append([]byte(nil), value...)) {
			return
		}
	}
}

func (s *failClosedStore) Sync() error {
	if s.syncFailures > 0 {
		s.syncFailures--
		s.restoring = true
		return s.failure
	}
	s.durable = copyStoreData(s.data)
	return nil
}

func (s *failClosedStore) Close() error {
	s.closeFlushed = true
	s.durable = copyStoreData(s.data)
	return nil
}

func (s *failClosedStore) CloseWithoutSync() error {
	s.closeAborted = true
	return nil
}

func copyStoreData(data map[string][]byte) map[string][]byte {
	copy := make(map[string][]byte, len(data))
	for key, value := range data {
		copy[key] = append([]byte(nil), value...)
	}
	return copy
}

func TestRejectedIdentityWriteFencesCatalogue(t *testing.T) {
	t.Parallel()
	one, err := encodeRecordValue(validRecord("work", 1))
	require.NoError(t, err)

	tests := []struct {
		name           string
		initial        map[string][]byte
		setFailures    int
		deleteFailures int
		syncFailures   int
		mutate         func(*Persister) error
	}{
		{
			name:        "create set failure",
			initial:     map[string][]byte{},
			setFailures: 1,
			mutate: func(p *Persister) error {
				return p.Create(validRecord("new", 2))
			},
		},
		{
			name:         "create sync failure",
			initial:      map[string][]byte{},
			syncFailures: 1,
			mutate: func(p *Persister) error {
				return p.Create(validRecord("new", 2))
			},
		},
		{
			name:        "replace set failure",
			initial:     map[string][]byte{"work": one},
			setFailures: 1,
			mutate: func(p *Persister) error {
				next := validRecord("work", 1)
				next.Cwd = "/next"
				return p.Replace("work", next)
			},
		},
		{
			name:         "replace sync failure",
			initial:      map[string][]byte{"work": one},
			syncFailures: 1,
			mutate: func(p *Persister) error {
				next := validRecord("work", 1)
				next.Cwd = "/next"
				return p.Replace("work", next)
			},
		},
		{
			name:        "rename set failure",
			initial:     map[string][]byte{"work": one},
			setFailures: 1,
			mutate: func(p *Persister) error {
				next := validRecord("renamed", 1)
				return p.Rename("work", next)
			},
		},
		{
			name:           "rename delete failure",
			initial:        map[string][]byte{"work": one},
			deleteFailures: 1,
			mutate: func(p *Persister) error {
				next := validRecord("renamed", 1)
				return p.Rename("work", next)
			},
		},
		{
			name:         "rename sync failure",
			initial:      map[string][]byte{"work": one},
			syncFailures: 1,
			mutate: func(p *Persister) error {
				next := validRecord("renamed", 1)
				return p.Rename("work", next)
			},
		},
		{
			name:           "delete delete failure",
			initial:        map[string][]byte{"work": one},
			deleteFailures: 1,
			mutate: func(p *Persister) error {
				return p.Delete("work")
			},
		},
		{
			name:         "delete sync failure",
			initial:      map[string][]byte{"work": one},
			syncFailures: 1,
			mutate: func(p *Persister) error {
				return p.Delete("work")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := newFailClosedStore(tt.initial)
			store.setFailures = tt.setFailures
			store.deleteFailures = tt.deleteFailures
			store.syncFailures = tt.syncFailures
			p := New(store)

			err := tt.mutate(p)
			require.ErrorIs(t, err, ErrCatalogueDurability)
			require.ErrorIs(t, err, store.failure)
			require.Equal(t, tt.initial, store.data, "rejected identity keys must be restored in memory")
			require.Equal(t, tt.initial, store.durable, "rejected identity write must not reach disk")

			_, _, recordErr := p.Record("work")
			require.ErrorIs(t, recordErr, ErrCatalogueDurability)
			require.ErrorIs(t, p.Sync(), ErrCatalogueDurability)
			require.ErrorIs(t, p.Create(validRecord("later", 3)), ErrCatalogueDurability)
			require.Equal(t, tt.initial, store.durable, "later Sync must be fenced")

			require.ErrorIs(t, p.Close(), ErrCatalogueDurability)
			require.True(t, store.closeAborted, "Close must release without flushing rejected state")
			require.False(t, store.closeFlushed)
			require.Equal(t, tt.initial, store.durable, "Close must not make rejected state durable")
		})
	}
}

func TestFailedSyncRetainsRepeatedRollbackFailuresAndAborts(t *testing.T) {
	t.Parallel()
	encoded, err := encodeRecordValue(validRecord("work", 1))
	require.NoError(t, err)
	store := newFailClosedStore(map[string][]byte{"work": encoded})
	store.syncFailures = 1
	store.restoreSetFailures = 1
	store.restoreDeleteFailures = 1
	p := New(store)

	err = p.Rename("work", validRecord("renamed", 1))
	require.ErrorIs(t, err, ErrCatalogueDurability)
	require.ErrorIs(t, err, store.failure)
	require.ErrorIs(t, err, store.restoreFailure)
	require.Zero(t, store.restoreSetFailures, "rollback Set failure must be observed")
	require.Zero(t, store.restoreDeleteFailures, "rollback Delete failure must be observed")

	terminal := p.Sync()
	require.Same(t, err, terminal, "the fenced terminal error must remain stable")
	require.ErrorIs(t, terminal, store.failure)
	require.ErrorIs(t, terminal, store.restoreFailure)
	require.ErrorIs(t, p.Close(), ErrCatalogueDurability)
	require.True(t, store.closeAborted)
	require.False(t, store.closeFlushed)
	require.Equal(t, map[string][]byte{"work": encoded}, store.durable,
		"aborting a fenced store must not persist dirty rejected state")
}

func TestRejectedIdentityWriteRestoresBufferedMetadata(t *testing.T) {
	t.Parallel()
	encoded, err := encodeRecordValue(validRecord("work", 1))
	require.NoError(t, err)
	store := newFailClosedStore(map[string][]byte{"work": encoded})
	p := New(store)

	cwd := "/buffered"
	require.NoError(t, p.UpdateMetadata(domain.CatalogueMetadataUpdate{
		Name: "work", IncarnationID: domain.IncarnationID{1}, Cwd: &cwd,
	}))
	buffered := copyStoreData(store.data)
	store.syncFailures = 1

	require.ErrorIs(t, p.Rename("work", validRecord("renamed", 1)), ErrCatalogueDurability)
	require.Equal(t, buffered, store.data, "rollback must preserve already-buffered metadata on the affected key")
	require.Equal(t, map[string][]byte{"work": encoded}, store.durable)
	require.ErrorIs(t, p.Close(), ErrCatalogueDurability)
	require.Equal(t, map[string][]byte{"work": encoded}, store.durable)
}
