package client

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

// Real connector integration (Plan 001 P5.1a, GO-007). These tests wire the
// production brokeripc Connector into the autonomous supervisor over a real
// AF_UNIX broker endpoint, so the readiness handshake, the established-
// connection loss, and the cancellation lifetime are pinned against the true
// adapter and carriage rather than a scripted fake.

const integrationEpoch = ports.BrokerEpoch(0x71)

// integrationCore is the state of the minimal admitted broker service one
// accepted connection is bound to: a fixed-epoch publication and a coalescing
// subscription, which is all the supervisor's readiness handshake needs.
type integrationCore struct {
	port    *portsmocks.MockBrokerCoreService
	id      ports.BrokerConnectionID
	epoch   ports.BrokerEpoch
	changed chan struct{}
	done    chan struct{}
	closed  chan struct{}
	once    sync.Once

	mu         sync.Mutex
	nextStream ports.BrokerStreamID
}

// newIntegrationCore builds the core mock: overrides first, then the optional
// defaults, with all state in the plain struct.
func newIntegrationCore(t *testing.T, id ports.BrokerConnectionID, epoch ports.BrokerEpoch, opts ...func(*portsmocks.MockBrokerCoreService)) *integrationCore {
	c := &integrationCore{
		id:      id,
		epoch:   epoch,
		changed: make(chan struct{}),
		done:    make(chan struct{}),
		closed:  make(chan struct{}),
	}
	c.port = portsmocks.NewMockBrokerCoreService(t)
	for _, opt := range opts {
		opt(c.port)
	}
	c.port.EXPECT().ConnectionID().Return(c.id).Maybe()
	c.port.EXPECT().Done().Return((<-chan struct{})(c.done)).Maybe()
	c.port.EXPECT().Err().Return(nil).Maybe()
	c.port.EXPECT().NextStreamID().RunAndReturn(c.allocateStream).Maybe()
	c.port.EXPECT().Snapshot().Return(ports.BrokerSnapshot{Epoch: c.epoch, Revision: 1}).Maybe()
	c.port.EXPECT().Subscribe().RunAndReturn(func() (ports.BrokerSubscription, error) {
		// Not registered for AssertExpectations: it is created on the
		// supervisor's goroutine and every expectation is optional.
		sub := &portsmocks.MockBrokerSubscription{}
		sub.Mock.Test(t)
		sub.EXPECT().Changed().Return((<-chan struct{})(c.changed)).Maybe()
		sub.EXPECT().Close().Return().Maybe()
		return sub, nil
	}).Maybe()
	c.port.EXPECT().SubscribePreview(mock.Anything).Return(nil, errors.New("integration core does not support preview")).Maybe()
	c.port.EXPECT().OpenEnvelopeStream(mock.Anything, mock.Anything).Return(nil, errors.New("integration core does not support streams")).Maybe()
	c.port.EXPECT().CloseStream(mock.Anything, mock.Anything).Return(nil).Maybe()
	c.port.EXPECT().AddHost(mock.Anything, mock.Anything, mock.Anything).Return(domain.RemoteRegistration{}, nil).Maybe()
	c.port.EXPECT().RemoveHost(mock.Anything, mock.Anything).Return(false, nil).Maybe()
	c.port.EXPECT().UpdateHostPolicy(mock.Anything, mock.Anything, mock.Anything).Return(domain.RemoteRegistration{}, nil).Maybe()
	c.port.EXPECT().RequestReconcile(mock.Anything).Return().Maybe()
	c.port.EXPECT().Close().RunAndReturn(c.close).Maybe()
	return c
}

// allocateStream mirrors the production allocator so the supervisor's single
// allocation seam is exercised over the real carriage.
func (c *integrationCore) allocateStream() (ports.BrokerStreamID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextStream++
	return c.nextStream, nil
}

func (c *integrationCore) close() error {
	c.once.Do(func() {
		close(c.done)
		close(c.closed)
	})
	return nil
}

// integrationAuthority admits one connection per accepted carriage and records
// every admitted core, so a test can observe the broker-side end of the
// supervisor's connection lifetime.
type integrationAuthority struct {
	port  *portsmocks.MockBrokerAuthority
	epoch ports.BrokerEpoch

	mu    sync.Mutex
	next  uint64
	cores []*integrationCore
}

func newIntegrationAuthority(t *testing.T, epoch ports.BrokerEpoch) *integrationAuthority {
	a := &integrationAuthority{epoch: epoch}
	a.port = portsmocks.NewMockBrokerAuthority(t)
	a.port.EXPECT().AdmitClient(mock.Anything).RunAndReturn(func(ctx context.Context) (ports.BrokerCoreService, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a.mu.Lock()
		a.next++
		sequence := a.next
		a.mu.Unlock()
		var id ports.BrokerConnectionID
		binary.BigEndian.PutUint64(id[:8], uint64(a.epoch))
		binary.BigEndian.PutUint64(id[8:], sequence)
		core := newIntegrationCore(t, id, a.epoch)
		a.mu.Lock()
		a.cores = append(a.cores, core)
		a.mu.Unlock()
		return core.port, nil
	}).Maybe()
	return a
}

func (a *integrationAuthority) lastCore() *integrationCore {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.cores) == 0 {
		return nil
	}
	return a.cores[len(a.cores)-1]
}

// startIntegrationBroker binds one real broker IPC endpoint in a short private
// directory, so the AF_UNIX path stays well within the platform limit.
func startIntegrationBroker(t *testing.T) (*integrationAuthority, ports.BrokerListener, string) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "v")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	path := brokeripc.SocketPath(filepath.Join(root, "vev"))
	authority := newIntegrationAuthority(t, integrationEpoch)
	listener, err := brokeripc.Listen(path, integrationEpoch, authority.port, brokeripc.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return authority, listener, path
}

// integrationSupervisor builds one autonomous supervisor over the production
// connector and a pinned clock with a controlled terminal.
func integrationSupervisor(t *testing.T, path string) (*Supervisor, *supervisorTestClock, *supervisorTestTerminal, <-chan State) {
	t.Helper()
	states := make(chan State, 64)
	clock := newSupervisorTestClock(t)
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	terminal := newSupervisorTestTerminal(t, reader)
	sup := mustSupervisor(t, SupervisorConfig{
		Connector: brokeripc.NewConnector(path, brokeripc.Config{}).Navigator(),
		Terminal:  terminal.port,
		Clock:     clock.port,
		Jitter:    func() float64 { return 0 },
		Render: func(state State) {
			select {
			case states <- state:
			default:
			}
		},
	})
	return sup, clock, terminal, states
}

// awaitIntegrationState blocks on the supervisor's published states until one
// satisfies match. The guard is a single bounded failure deadline: the broker
// endpoint is a real socket, so progress arrives on a channel, never by polling.
func awaitIntegrationState(t *testing.T, sup *Supervisor, states <-chan State, match func(State) bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for !match(sup.State()) {
		select {
		case <-states:
		case <-deadline:
			t.Fatalf("supervisor never reached the expected state, last %+v", sup.State())
		}
	}
}

// TestSupervisorRealConnectorReadinessAndLoss pins the readiness handshake and
// the established-connection loss against a real broker endpoint: the
// supervisor becomes Ready only after the broker publishes the admitted
// connection's epoch, and a retired connection is observed through Done/Err and
// retried with a typed unavailable cause.
// real I/O: the broker endpoint is a real AF_UNIX socket, which cannot block durably in a bubble.
func TestSupervisorRealConnectorReadinessAndLoss(t *testing.T) {
	_, listener, path := startIntegrationBroker(t)
	sup, _, terminal, states := integrationSupervisor(t, path)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()

	awaitIntegrationState(t, sup, states, func(st State) bool { return st.Connectivity == ConnectivityReady })
	require.Equal(t, uint64(1), sup.State().Generation)
	require.Equal(t, uint64(0), sup.State().Attempt)
	require.NoError(t, sup.State().Err)

	// The broker retires the accepted connection; the supervisor observes the
	// carriage loss through Done/Err and waits the cadence before retrying.
	server, err := listener.Accept()
	require.NoError(t, err)
	require.NoError(t, server.Close())

	awaitIntegrationState(t, sup, states, func(st State) bool { return st.Connectivity == ConnectivityRetryWait })
	var typed ports.BrokerError
	require.ErrorAs(t, sup.State().Err, &typed)
	require.Equal(t, ports.BrokerErrorUnavailable, typed.Code)

	cancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
	require.Equal(t, 1, terminal.restoreCount())
}

// TestSupervisorRealConnectorCancelClosesConnection pins the cancellation
// lifetime: cancelling the run closes the supervisor's adopted connection, ends
// the broker-side session, and retires the admitted core exactly once.
// real I/O: the broker endpoint is a real AF_UNIX socket, which cannot block durably in a bubble.
func TestSupervisorRealConnectorCancelClosesConnection(t *testing.T) {
	authority, _, path := startIntegrationBroker(t)
	sup, _, terminal, states := integrationSupervisor(t, path)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()

	awaitIntegrationState(t, sup, states, func(st State) bool { return st.Connectivity == ConnectivityReady })
	core := authority.lastCore()
	require.NotNil(t, core, "the broker must have admitted the connection")

	cancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
	require.Equal(t, 1, terminal.restoreCount())

	select {
	case <-core.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelling the supervisor must close the broker connection")
	}
}
