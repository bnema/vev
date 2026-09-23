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

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Real connector integration (Plan 001 P5.1a, GO-007). These tests wire the
// production brokeripc Connector into the autonomous supervisor over a real
// AF_UNIX broker endpoint, so the readiness handshake, the established-
// connection loss, and the cancellation lifetime are pinned against the true
// adapter and carriage rather than a scripted fake.

const integrationEpoch = ports.BrokerEpoch(0x71)

// integrationCore is the minimal admitted broker service one accepted
// connection is bound to: a fixed-epoch publication and a coalescing
// subscription, which is all the supervisor's readiness handshake needs.
type integrationCore struct {
	id      ports.BrokerConnectionID
	epoch   ports.BrokerEpoch
	changed chan struct{}
	done    chan struct{}
	closed  chan struct{}
	once    sync.Once

	mu         sync.Mutex
	nextStream ports.BrokerStreamID
}

func (c *integrationCore) ConnectionID() ports.BrokerConnectionID { return c.id }
func (c *integrationCore) Done() <-chan struct{}                  { return c.done }
func (c *integrationCore) Err() error                             { return nil }

// NextStreamID mirrors the production allocator so the supervisor's single
// allocation seam is exercised over the real carriage.
func (c *integrationCore) NextStreamID() (ports.BrokerStreamID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextStream++
	return c.nextStream, nil
}

func (c *integrationCore) Snapshot() ports.BrokerSnapshot {
	return ports.BrokerSnapshot{Epoch: c.epoch, Revision: 1}
}

func (c *integrationCore) Subscribe() (ports.BrokerSubscription, error) {
	return integrationSubscription{changed: c.changed}, nil
}

func (c *integrationCore) SubscribePreview(ports.BrokerPreviewRequest) (ports.BrokerPreviewSubscription, error) {
	return nil, errors.New("integration core does not support preview")
}

func (c *integrationCore) OpenEnvelopeStream(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerEnvelopeStream, error) {
	return nil, errors.New("integration core does not support streams")
}

func (c *integrationCore) CloseStream(ports.BrokerConnectionID, ports.BrokerStreamID) error {
	return nil
}
func (c *integrationCore) AddHost(context.Context, string, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	return domain.RemoteRegistration{}, nil
}
func (c *integrationCore) RemoveHost(context.Context, domain.RemoteRegistration) (bool, error) {
	return false, nil
}
func (c *integrationCore) UpdateHostPolicy(context.Context, domain.RemoteRegistration, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	return domain.RemoteRegistration{}, nil
}
func (c *integrationCore) RequestReconcile(string) {}

func (c *integrationCore) Close() error {
	c.once.Do(func() {
		close(c.done)
		close(c.closed)
	})
	return nil
}

// integrationSubscription is one idle coalescing subscription.
type integrationSubscription struct{ changed chan struct{} }

func (s integrationSubscription) Changed() <-chan struct{} { return s.changed }
func (s integrationSubscription) Close()                   {}

// integrationAuthority admits one connection per accepted carriage and records
// every admitted core, so a test can observe the broker-side end of the
// supervisor's connection lifetime.
type integrationAuthority struct {
	epoch ports.BrokerEpoch

	mu    sync.Mutex
	next  uint64
	cores []*integrationCore
}

var _ ports.BrokerAuthority = (*integrationAuthority)(nil)

func (a *integrationAuthority) AdmitClient(ctx context.Context) (ports.BrokerCoreService, error) {
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
	core := &integrationCore{
		id:      id,
		epoch:   a.epoch,
		changed: make(chan struct{}),
		done:    make(chan struct{}),
		closed:  make(chan struct{}),
	}
	a.mu.Lock()
	a.cores = append(a.cores, core)
	a.mu.Unlock()
	return core, nil
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
	authority := &integrationAuthority{epoch: integrationEpoch}
	listener, err := brokeripc.Listen(path, integrationEpoch, authority, brokeripc.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return authority, listener, path
}

// integrationSupervisor builds one autonomous supervisor over the production
// connector and a pinned clock with a controlled terminal.
func integrationSupervisor(t *testing.T, path string) (*Supervisor, *supervisorTestClock, *supervisorTestTerminal) {
	t.Helper()
	clock := newSupervisorTestClock()
	reader := newSupervisorTestReader()
	t.Cleanup(reader.unblock)
	terminal := newSupervisorTestTerminal(reader)
	sup := mustSupervisor(t, SupervisorConfig{
		Connector: brokeripc.NewConnector(path, brokeripc.Config{}),
		Terminal:  terminal,
		Clock:     clock,
		Jitter:    func() float64 { return 0 },
	})
	return sup, clock, terminal
}

// TestSupervisorRealConnectorReadinessAndLoss pins the readiness handshake and
// the established-connection loss against a real broker endpoint: the
// supervisor becomes Ready only after the broker publishes the admitted
// connection's epoch, and a retired connection is observed through Done/Err and
// retried with a typed unavailable cause.
func TestSupervisorRealConnectorReadinessAndLoss(t *testing.T) {
	_, listener, path := startIntegrationBroker(t)
	sup, _, terminal := integrationSupervisor(t, path)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()

	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)
	require.Equal(t, uint64(1), sup.State().Generation)
	require.Equal(t, uint64(0), sup.State().Attempt)
	require.NoError(t, sup.State().Err)

	// The broker retires the accepted connection; the supervisor observes the
	// carriage loss through Done/Err and waits the cadence before retrying.
	server, err := listener.Accept()
	require.NoError(t, err)
	require.NoError(t, server.Close())

	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityRetryWait }, 5*time.Second, time.Millisecond)
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
func TestSupervisorRealConnectorCancelClosesConnection(t *testing.T) {
	authority, _, path := startIntegrationBroker(t)
	sup, _, terminal := integrationSupervisor(t, path)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()

	require.Eventually(t, func() bool { return sup.State().Connectivity == ConnectivityReady }, 5*time.Second, time.Millisecond)
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
