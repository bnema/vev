package persist

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func privateDir(t *testing.T) string { t.Helper(); return filepath.Join(t.TempDir(), "vev") }
func validRecord(name string, id byte) domain.CatalogueRecord {
	return domain.CatalogueRecord{Name: name, IncarnationID: domain.IncarnationID{id}, RecoveryState: domain.RecoveryFresh}
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
		{
			name: "next companion without catalogue",
			files: map[string][]byte{
				filename + ".next": []byte("uncommitted catalogue candidate"),
			},
		},
		{
			name: "previous companion without catalogue",
			files: map[string][]byte{
				filename + ".prev": []byte("previous catalogue candidate"),
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
			require.Equal(t, before, directoryBytes(t, dir), "failure must preserve every durable path and byte")
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

func TestCatalogueBatchSyncBehavior(t *testing.T) {
	store := portsmocks.NewMockStore(t)
	state := map[string][]byte{}
	store.EXPECT().Range(mock.Anything).Run(func(fn func([]byte, []byte) bool) {
		for key, value := range state {
			if !fn([]byte(key), value) {
				return
			}
		}
	}).Once()
	store.EXPECT().Batch(mock.Anything).RunAndReturn(func(changes []ports.StoreChange) error {
		for _, change := range changes {
			if change.Delete {
				delete(state, string(change.Key))
			} else {
				state[string(change.Key)] = append([]byte(nil), change.Value...)
			}
		}
		return nil
	}).Twice()
	store.EXPECT().Sync().Return(nil).Twice()
	p := New(store)
	one := validRecord("one", 1)
	require.NoError(t, p.Replace("one", one))
	require.Contains(t, state, "one")
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

func TestCatalogueBatchFailureDoesNotSync(t *testing.T) {
	store := portsmocks.NewMockStore(t)
	batchErr := errors.New("batch failed")
	store.EXPECT().Range(mock.Anything).Run(func(func([]byte, []byte) bool) {})
	store.EXPECT().Batch(mock.Anything).Return(batchErr)
	p := New(store)
	err := p.Replace("one", validRecord("one", 1))
	require.ErrorIs(t, err, batchErr)
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
