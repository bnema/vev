package app

import (
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol/wire"
	wiremocks "github.com/bnema/vev/internal/protocol/wire/mocks"
)

// The forwarding constructors wrap one real implementation in a generated mock
// whose every method calls through to it. Options register the overrides first
// (a blocking Close, a gated Publish, ...), then the forwarding defaults are
// registered with Maybe, and testify matches the first registered expectation.

// newForwardingCatalogue is a ports.Catalogue forwarding to inner.
func newForwardingCatalogue(t *testing.T, inner ports.Catalogue, opts ...func(*portsmocks.MockCatalogue)) *portsmocks.MockCatalogue {
	t.Helper()
	catalogue := portsmocks.NewMockCatalogue(t)
	for _, opt := range opts {
		opt(catalogue)
	}
	catalogue.EXPECT().Records().RunAndReturn(inner.Records).Maybe()
	catalogue.EXPECT().Record(mock.Anything).RunAndReturn(inner.Record).Maybe()
	catalogue.EXPECT().Create(mock.Anything).RunAndReturn(inner.Create).Maybe()
	catalogue.EXPECT().UpdateMetadata(mock.Anything).RunAndReturn(inner.UpdateMetadata).Maybe()
	catalogue.EXPECT().Replace(mock.Anything, mock.Anything).RunAndReturn(inner.Replace).Maybe()
	catalogue.EXPECT().Rename(mock.Anything, mock.Anything).RunAndReturn(inner.Rename).Maybe()
	catalogue.EXPECT().Delete(mock.Anything).RunAndReturn(inner.Delete).Maybe()
	catalogue.EXPECT().Sync().RunAndReturn(inner.Sync).Maybe()
	catalogue.EXPECT().Close().RunAndReturn(inner.Close).Maybe()
	return catalogue
}

// newForwardingRepository is a ports.SnapshotRepository forwarding to inner.
func newForwardingRepository(t *testing.T, inner ports.SnapshotRepository, opts ...func(*portsmocks.MockSnapshotRepository)) *portsmocks.MockSnapshotRepository {
	t.Helper()
	repository := portsmocks.NewMockSnapshotRepository(t)
	for _, opt := range opts {
		opt(repository)
	}
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(inner.Publish).Maybe()
	repository.EXPECT().LoadCheckpoint(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(inner.LoadCheckpoint).Maybe()
	repository.EXPECT().ReconcileCheckpoint(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(inner.ReconcileCheckpoint).Maybe()
	repository.EXPECT().DeleteIncarnation(mock.Anything, mock.Anything).RunAndReturn(inner.DeleteIncarnation).Maybe()
	repository.EXPECT().SnapshotIncarnations(mock.Anything).RunAndReturn(inner.SnapshotIncarnations).Maybe()
	repository.EXPECT().CollectIncarnationGarbage(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(inner.CollectIncarnationGarbage).Maybe()
	return repository
}

// newForwardingListener is a wire.Listener forwarding to inner.
func newForwardingListener(t *testing.T, inner wire.Listener, opts ...func(*wiremocks.MockListener)) *wiremocks.MockListener {
	t.Helper()
	listener := wiremocks.NewMockListener(t)
	for _, opt := range opts {
		opt(listener)
	}
	listener.EXPECT().Accept().RunAndReturn(inner.Accept).Maybe()
	listener.EXPECT().Close().RunAndReturn(inner.Close).Maybe()
	listener.EXPECT().Addr().RunAndReturn(inner.Addr).Maybe()
	return listener
}

// blockingRepositoryState is the signaling state of a repository whose call
// parks until released, ignoring cancellation on purpose: lifecycle ownership
// must outlive that call.
type blockingRepositoryState struct {
	entered  chan struct{}
	release  chan struct{}
	returned chan struct{}
}

func newBlockingRepositoryState() *blockingRepositoryState {
	return &blockingRepositoryState{
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
}

// park signals entry, waits for release, and signals return. Each signal
// closes its channel once.
func (s *blockingRepositoryState) park() {
	select {
	case <-s.entered:
	default:
		close(s.entered)
	}
	<-s.release // Intentionally ignore cancellation: lifecycle ownership must outlive this call.
	select {
	case <-s.returned:
	default:
		close(s.returned)
	}
}
