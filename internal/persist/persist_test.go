package persist

import (
	"errors"
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
