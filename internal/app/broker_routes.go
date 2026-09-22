package app

import (
	"context"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/ports"
)

// brokerRoutes splits authority deliberately: local routing is provisioned by
// composition; remote routing is read from committed registry membership, never
// from its advisory snapshot or the startup import. hosts is wired before Run.
type brokerRoutes struct {
	local ports.BrokerRouteAuthority
	hosts ports.BrokerHostAuthorityReader
}

func (r *brokerRoutes) ResolveDialTarget(ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
	if err := ctx.Err(); err != nil {
		return ports.BrokerDialTarget{}, err
	}
	if request.Local {
		return r.local.ResolveDialTarget(ctx, request)
	}
	record, found, err := r.hosts.LookupHost(ctx, request.Endpoint)
	if err != nil {
		return ports.BrokerDialTarget{}, err
	}
	if !found || !record.Registration.Equal(request.Registration) {
		return ports.BrokerDialTarget{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "unknown or stale registration"}
	}
	if !record.Policy.Compatible(request.Policy) {
		return ports.BrokerDialTarget{}, ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy}
	}
	route, err := brokerconfig.RouteFromSpec(record.Route)
	if err != nil {
		return ports.BrokerDialTarget{}, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}
	target := ports.BrokerDialTarget{
		Fence:  ports.BrokerEndpointFence{Registration: record.Registration},
		Policy: record.Policy, Address: route.Address(), StartMode: request.StartMode,
		ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: record.Identity, Bound: record.Identity != ""},
	}
	return target, target.Validate()
}

// remoteRoute rechecks the whole target immediately before dialing. A removed,
// replaced, rebound, or rerouted record cannot authorize a stale dial target.
func (r *brokerRoutes) remoteRoute(ctx context.Context, target ports.BrokerDialTarget) (brokerconfig.Route, error) {
	record, found, err := r.hosts.LookupHost(ctx, target.Fence.Registration.Endpoint)
	if err != nil {
		return brokerconfig.Route{}, err
	}
	if !found || !record.Registration.Equal(target.Fence.Registration) || record.Policy != target.Policy || record.Identity != target.ExpectedIdentity.Identity {
		return brokerconfig.Route{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "route authority changed"}
	}
	route, err := brokerconfig.RouteFromSpec(record.Route)
	if err != nil {
		return brokerconfig.Route{}, err
	}
	if route.Address() != target.Address {
		return brokerconfig.Route{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "route authority changed"}
	}
	return route, nil
}
