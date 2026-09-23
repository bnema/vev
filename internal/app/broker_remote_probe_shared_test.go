package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/broker"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestBrokerRemoteProbeBorrowsWarmTransport proves observation reuses the
// broker pool's attached or warm transport instead of bootstrapping SSH/QUIC
// again, and redials only when the borrowed transport has retired.
func TestBrokerRemoteProbeBorrowsWarmTransport(t *testing.T) {
	registration := domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1}
	policy := remoteBrokerPolicy("quic")
	const identity ports.BrokerDaemonIdentity = "remote-daemon"
	dialErr := errors.New("dial refused")
	tests := []struct {
		name      string
		bound     bool
		shared    bool
		openErr   error
		retired   bool
		wantDials int
		wantErr   error
	}{
		{name: "warm transport observes without dialing", bound: true, shared: true},
		{name: "unbound identity dials", bound: false, shared: true, wantDials: 1, wantErr: dialErr},
		{name: "no pooled transport dials", bound: true, wantDials: 1, wantErr: dialErr},
		{name: "live transport refusal is reported without redial", bound: true, shared: true, openErr: errors.New("stream refused")},
		{name: "retired transport redials", bound: true, shared: true, openErr: errors.New("physical closed"), retired: true, wantDials: 1, wantErr: dialErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := ports.BrokerHostRecord{Registration: registration, Policy: policy}
			if tt.bound {
				record.Identity = identity
			}
			routes := portsmocks.NewMockBrokerRouteAuthority(t)
			routes.EXPECT().ResolveDialTarget(mock.Anything, mock.Anything).Return(ports.BrokerDialTarget{
				Fence: ports.BrokerEndpointFence{Registration: registration}, Policy: policy, Address: "opaque", StartMode: ports.BrokerDaemonExistingOnly,
				ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: record.Identity, Bound: tt.bound},
			}, nil)
			connector := portsmocks.NewMockBrokerEndpointConnector(t)
			if tt.wantDials > 0 {
				connector.EXPECT().Connect(mock.Anything, mock.Anything).Return(nil, dialErr).Times(tt.wantDials)
			}
			physical := portsmocks.NewMockBrokerPhysicalConnection(t)
			codec := portsmocks.NewMockSessionCodec(t)
			done := make(chan struct{})
			if tt.retired {
				close(done)
			}
			if tt.bound && tt.shared {
				if tt.openErr != nil {
					if !tt.retired {
						physical.EXPECT().OpenStream(mock.Anything, mock.Anything).Return(nil, tt.openErr)
						physical.EXPECT().Done().Return(done)
					}
				} else {
					logical := portsmocks.NewMockBrokerEnvelopeStream(t)
					typed := portsmocks.NewMockClientConnection(t)
					codec.EXPECT().Client(logical).Return(typed)
					var sent protocol.CommandRequest
					typed.EXPECT().SendClient(mock.Anything).RunAndReturn(func(message protocol.ClientMessage) error {
						sent = message.(protocol.CommandRequest)
						return nil
					})
					typed.EXPECT().ReceiveServer().RunAndReturn(func() (protocol.ServerMessage, error) {
						return protocol.CommandResult{RequestID: sent.RequestID, Outcome: protocol.CommandSucceeded, Output: testLocalCatalogue(t)}, nil
					})
					logical.EXPECT().Close().Return(nil)
					physical.EXPECT().OpenStream(mock.Anything, mock.Anything).Return(logical, nil)
					physical.EXPECT().Identity().Return(identity)
					physical.EXPECT().Incarnation().Return(ports.BrokerDaemonIncarnation{1})
				}
			}
			probe := &brokerRemoteProbe{
				epoch: 7, routes: routes, binder: portsmocks.NewMockBrokerIdentityBinder(t), connector: connector,
				hosts: &routeTestHosts{record: record, found: true}, codec: codec,
			}
			// Use the real pool for borrowing. Stateful pool retirement is
			// covered with a fake physical in broker's worker tests.
			pool, err := broker.NewPool(7, routes, probe.binder, connector, clock.New(), broker.PoolLimits{Physical: 1, Clients: 1, Streams: 2, StreamsPerClient: 2, Warm: 1})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, pool.Close()) })
			if tt.shared && tt.bound && !tt.retired {
				physical.EXPECT().Policy().Return(policy).Maybe()
				physical.EXPECT().Identity().Return(identity).Maybe()
				physical.EXPECT().Incarnation().Return(ports.BrokerDaemonIncarnation{1}).Maybe()
				physical.EXPECT().Done().Return(done).Maybe()
				physical.EXPECT().Close().Return(nil).Maybe()
				require.True(t, pool.AdoptPhysical(physical, policy))
			}
			probe.shared.share(pool)

			observation, err := probe.Probe(context.Background(), registration)
			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case tt.openErr != nil:
				require.ErrorIs(t, err, tt.openErr)
			default:
				require.NoError(t, err)
				require.Equal(t, identity, observation.Identity)
				require.True(t, observation.InventoryKnown)
			}
		})
	}
}

// TestBrokerRemoteProbeSilentBorrowedTransportNeverRedials proves a probe on a
// healthy borrowed transport whose daemon never answers is bounded by the
// attempt context (the registry's ports.Clock deadline cancels it) and returns
// without bootstrapping a second transport: a live pooled physical is never
// abandoned for a redial just because one observation went unanswered.
func TestBrokerRemoteProbeSilentBorrowedTransportNeverRedials(t *testing.T) {
	registration := domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1}
	policy := remoteBrokerPolicy("quic")
	const identity ports.BrokerDaemonIdentity = "remote-daemon"
	tests := []struct {
		name string
		// silentOpen: the daemon never answers the observation open; otherwise
		// the stream opens and the catalogue command is never answered.
		silentOpen bool
	}{
		{name: "unanswered observation open", silentOpen: true},
		{name: "unanswered catalogue command"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := ports.BrokerHostRecord{Registration: registration, Policy: policy, Identity: identity}
			routes := portsmocks.NewMockBrokerRouteAuthority(t)
			routes.EXPECT().ResolveDialTarget(mock.Anything, mock.Anything).Return(ports.BrokerDialTarget{
				Fence: ports.BrokerEndpointFence{Registration: registration}, Policy: policy, Address: "opaque", StartMode: ports.BrokerDaemonExistingOnly,
				ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: identity, Bound: true},
			}, nil)
			// No Connect expectation: any redial fails the mock.
			connector := portsmocks.NewMockBrokerEndpointConnector(t)
			physical := portsmocks.NewMockBrokerPhysicalConnection(t)
			codec := portsmocks.NewMockSessionCodec(t)
			healthy := make(chan struct{})
			physical.EXPECT().Policy().Return(policy).Maybe()
			physical.EXPECT().Identity().Return(identity).Maybe()
			physical.EXPECT().Incarnation().Return(ports.BrokerDaemonIncarnation{1}).Maybe()
			physical.EXPECT().Done().Return(healthy).Maybe()
			physical.EXPECT().Close().Return(nil).Maybe()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// waiting signals that the probe is parked on the silent daemon; the
			// worker only signals, the test goroutine cancels and asserts.
			waiting := make(chan struct{}, 1)
			if tt.silentOpen {
				physical.EXPECT().OpenStream(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerEnvelopeStream, error) {
					waiting <- struct{}{}
					<-ctx.Done()
					return nil, ctx.Err()
				})
			} else {
				logical := portsmocks.NewMockBrokerEnvelopeStream(t)
				typed := portsmocks.NewMockClientConnection(t)
				codec.EXPECT().Client(logical).Return(typed)
				closed := make(chan struct{})
				var once sync.Once
				typed.EXPECT().SendClient(mock.Anything).Return(nil)
				typed.EXPECT().ReceiveServer().RunAndReturn(func() (protocol.ServerMessage, error) {
					waiting <- struct{}{}
					<-closed
					return nil, errors.New("stream closed")
				})
				logical.EXPECT().Close().RunAndReturn(func() error { once.Do(func() { close(closed) }); return nil })
				physical.EXPECT().OpenStream(mock.Anything, mock.Anything).Return(logical, nil)
			}
			probe := &brokerRemoteProbe{
				epoch: 7, routes: routes, binder: portsmocks.NewMockBrokerIdentityBinder(t), connector: connector,
				hosts: &routeTestHosts{record: record, found: true}, codec: codec,
			}
			pool, err := broker.NewPool(7, routes, probe.binder, connector, clock.New(), broker.PoolLimits{Physical: 1, Clients: 1, Streams: 2, StreamsPerClient: 2, Warm: 1})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, pool.Close()) })
			require.True(t, pool.AdoptPhysical(physical, policy))
			probe.shared.share(pool)

			type outcome struct{ err error }
			done := make(chan outcome, 1)
			go func() {
				_, err := probe.Probe(ctx, registration)
				done <- outcome{err: err}
			}()
			select {
			case <-waiting:
			case <-time.After(2 * time.Second):
				t.Fatal("probe never reached the silent daemon")
			}
			cancel()
			select {
			case result := <-done:
				require.ErrorIs(t, result.err, context.Canceled)
			case <-time.After(2 * time.Second):
				t.Fatal("probe outlived its attempt context")
			}
		})
	}
}
