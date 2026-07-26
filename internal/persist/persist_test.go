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
	}).Twice()
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
	renamed := one
	renamed.Name = "renamed"
	require.NoError(t, p.Rename("one", renamed))
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

func TestCatalogueNilStoreIsNoOp(t *testing.T) {
	persisters := []*Persister{nil, New(nil)}
	for _, p := range persisters {
		require.NoError(t, p.Save(validRecord("one", 1)))
		require.NoError(t, p.Apply(map[string]*domain.CatalogueRecord{"one": nil}))
		require.NoError(t, p.UpdateMetadata(validRecord("one", 1).MetadataUpdate()))
		require.NoError(t, p.Delete("one"))
		records, err := p.LoadAll()
		require.NoError(t, err)
		require.Empty(t, records)
		require.NoError(t, p.Close())
	}
}
