package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// routeHosts is a mutable committed-membership view, so a test can change the
// record between resolution and dial.
type routeHosts struct {
	record ports.BrokerHostRecord
	found  bool
}

func (h *routeHosts) LookupHost(ctx context.Context, endpoint string) (ports.BrokerHostRecord, bool, error) {
	return h.record, h.found && endpoint == h.record.Registration.Endpoint, ctx.Err()
}

// testRouteAddress derives a stable address from the route contents.
func testRouteAddress(spec ports.BrokerRouteSpec) (string, error) {
	if err := spec.Validate(); err != nil {
		return "", err
	}
	return "addr:" + string(spec.Kind) + ":" + spec.Path + spec.Target, nil
}

func testRouteRecord(t *testing.T) ports.BrokerHostRecord {
	t.Helper()
	registration := domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1}
	policy := observationPolicy()
	spec, err := ports.BrokerRouteForTransport(policy.Transport, registration.Endpoint)
	require.NoError(t, err)
	return ports.BrokerHostRecord{Registration: registration, Policy: policy, Route: spec, Identity: "daemon"}
}

func testRouteRequest(record ports.BrokerHostRecord) ports.BrokerOpenStreamRequest {
	return ports.BrokerOpenStreamRequest{Endpoint: record.Registration.Endpoint, Registration: record.Registration, Policy: record.Policy, StartMode: ports.BrokerDaemonExistingOnly}
}

func TestRoutesResolveRemoteTarget(t *testing.T) {
	record := testRouteRecord(t)
	tests := []struct {
		name     string
		change   func(*routeHosts, *ports.BrokerOpenStreamRequest)
		wantCode ports.BrokerErrorCode
	}{
		{name: "committed record resolves"},
		{name: "unknown host", change: func(h *routeHosts, _ *ports.BrokerOpenStreamRequest) { h.found = false }, wantCode: ports.BrokerErrorUnavailable},
		{name: "stale registration", change: func(_ *routeHosts, r *ports.BrokerOpenStreamRequest) { r.Registration.Generation++ }, wantCode: ports.BrokerErrorUnavailable},
		{name: "conflicting policy", change: func(_ *routeHosts, r *ports.BrokerOpenStreamRequest) { r.Policy.Trust = "other" }, wantCode: ports.BrokerErrorConflictingPolicy},
		{name: "invalid durable route", change: func(h *routeHosts, _ *ports.BrokerOpenStreamRequest) { h.record.Route = ports.BrokerRouteSpec{} }, wantCode: ports.BrokerErrorIncompatible},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hosts := &routeHosts{record: record, found: true}
			request := testRouteRequest(record)
			if tt.change != nil {
				tt.change(hosts, &request)
			}
			routes := &Routes{Hosts: hosts, Address: testRouteAddress}
			target, err := routes.ResolveDialTarget(context.Background(), request)
			if tt.wantCode != 0 {
				var brokerErr ports.BrokerError
				require.True(t, errors.As(err, &brokerErr), "error %v", err)
				require.Equal(t, tt.wantCode, brokerErr.Code)
				return
			}
			require.NoError(t, err)
			address, _ := testRouteAddress(record.Route)
			require.Equal(t, address, target.Address)
			require.Equal(t, record.Registration, target.Fence.Registration)
			require.Equal(t, ports.BrokerExpectedIdentity{Identity: "daemon", Bound: true}, target.ExpectedIdentity)
			require.Equal(t, request.StartMode, target.StartMode)
		})
	}
}

// A resolved remote target is re-authorized against committed membership
// immediately before dialing: any change since resolution refuses the dial.
func TestRoutesAuthorizeRemoteDialRejectsChangedAuthority(t *testing.T) {
	record := testRouteRecord(t)
	tests := []struct {
		name    string
		change  func(*routeHosts)
		wantErr bool
	}{
		{name: "unchanged", change: func(*routeHosts) {}},
		{name: "removed", change: func(h *routeHosts) { h.found = false }, wantErr: true},
		{name: "readded", change: func(h *routeHosts) { h.record.Registration.Incarnation[0]++ }, wantErr: true},
		{name: "policy", change: func(h *routeHosts) { h.record.Policy.Trust = "other" }, wantErr: true},
		{name: "route", change: func(h *routeHosts) { h.record.Route.Target = "other@example.test" }, wantErr: true},
		{name: "identity", change: func(h *routeHosts) { h.record.Identity = "other-daemon" }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hosts := &routeHosts{record: record, found: true}
			routes := &Routes{Hosts: hosts, Address: testRouteAddress}
			target, err := routes.ResolveDialTarget(context.Background(), testRouteRequest(record))
			require.NoError(t, err)
			tt.change(hosts)
			spec, err := routes.AuthorizeRemoteDial(context.Background(), target)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, record.Route, spec)
		})
	}
}

func TestRoutesLocalDelegatesAndFailsClosed(t *testing.T) {
	request := ports.BrokerOpenStreamRequest{Local: true, Policy: observationPolicy(), StartMode: ports.BrokerDaemonExistingOnly}
	want := ports.BrokerDialTarget{Fence: ports.BrokerEndpointFence{Local: true}, Address: "local"}
	local := portsmocks.NewMockBrokerRouteAuthority(t)
	local.EXPECT().ResolveDialTarget(mock.Anything, request).Return(want, nil)

	got, err := (&Routes{Local: local}).ResolveDialTarget(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, want, got)

	_, err = (&Routes{}).ResolveDialTarget(context.Background(), request)
	require.Error(t, err, "no local binding authorizes no local dial")
	_, err = (&Routes{}).ResolveDialTarget(context.Background(), testRouteRequest(testRouteRecord(t)))
	require.Error(t, err, "no committed membership authorizes no remote dial")
	_, err = (&Routes{Hosts: &routeHosts{}, Address: testRouteAddress}).AuthorizeRemoteDial(context.Background(), want)
	require.Error(t, err, "a local target never selects a remote route")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = (&Routes{}).ResolveDialTarget(cancelled, request)
	require.ErrorIs(t, err, context.Canceled)
}
