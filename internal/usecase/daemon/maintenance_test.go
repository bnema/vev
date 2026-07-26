package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestStartupGarbageCollectionUsesValidatedCatalogue(t *testing.T) {
	committed := domain.CheckpointRef{Generation: 3, ManifestDigest: [32]byte{3}}
	records := []domain.CatalogueRecord{
		{Name: "work", IncarnationID: domain.IncarnationID{1}, Committed: &committed},
		{Name: "fresh", IncarnationID: domain.IncarnationID{2}},
	}
	catalogue := portsmocks.NewMockCatalogue(t)
	catalogue.EXPECT().Records().Return(records, nil).Once()
	repository := portsmocks.NewMockSnapshotRepository(t)
	repository.EXPECT().CollectGarbage(mock.Anything, mock.MatchedBy(func(keep map[domain.IncarnationID]domain.CheckpointRef) bool {
		return len(keep) == 2 && keep[records[0].IncarnationID] == committed && keep[records[1].IncarnationID] == (domain.CheckpointRef{})
	})).Return(nil).Once()
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithDurableMaintenance(catalogue, repository)(d)

	require.NoError(t, d.CollectStartupGarbage(context.Background()))
}

func TestStartupGarbageCollectionSkipsUnknownCatalogueState(t *testing.T) {
	catalogue := portsmocks.NewMockCatalogue(t)
	catalogue.EXPECT().Records().Return(nil, errors.New("catalogue read failed")).Once()
	repository := portsmocks.NewMockSnapshotRepository(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithDurableMaintenance(catalogue, repository)(d)

	require.Error(t, d.CollectStartupGarbage(context.Background()))
	repository.AssertNotCalled(t, "CollectGarbage", mock.Anything, mock.Anything)
	require.NotNil(t, d.maintenance.catalogue)
}
