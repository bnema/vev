package app

import (
	"context"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// sharedPhysicalsFunc adapts a closure to the probe's pool lending seam.
type sharedPhysicalsFunc func(ports.BrokerDaemonIdentity, ports.BrokerPolicy) (ports.BrokerPhysicalConnection, bool)

func (f sharedPhysicalsFunc) SharedPhysical(identity ports.BrokerDaemonIdentity, policy ports.BrokerPolicy) (ports.BrokerPhysicalConnection, bool) {
	return f(identity, policy)
}

func (sharedPhysicalsFunc) AdoptPhysical(ports.BrokerPhysicalConnection, ports.BrokerPolicy) bool {
	return false
}

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
			done := make(chan struct{})
			if tt.retired {
				close(done)
			}
			if tt.bound && tt.shared {
				if tt.openErr != nil {
					physical.EXPECT().OpenStream(mock.Anything, mock.Anything).Return(nil, tt.openErr)
					physical.EXPECT().Done().Return(done)
				} else {
					logical := portsmocks.NewMockBrokerLogicalConnection(t)
					var sent protocol.CommandRequest
					logical.EXPECT().SendClient(mock.Anything).RunAndReturn(func(message protocol.ClientMessage) error {
						sent = message.(protocol.CommandRequest)
						return nil
					})
					logical.EXPECT().ReceiveServer().RunAndReturn(func() (protocol.ServerMessage, error) {
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
				hosts: &routeTestHosts{record: record, found: true},
			}
			probe.shared.share(sharedPhysicalsFunc(func(gotIdentity ports.BrokerDaemonIdentity, gotPolicy ports.BrokerPolicy) (ports.BrokerPhysicalConnection, bool) {
				if !tt.shared || gotIdentity != identity || gotPolicy != policy {
					return nil, false
				}
				return physical, true
			}))

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
