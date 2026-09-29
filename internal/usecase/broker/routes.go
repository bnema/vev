package broker

import (
	"context"
	"errors"

	"github.com/bnema/vev/internal/ports"
)

// RouteAddress derives the opaque pool address of one durable route. The
// carriage adapter owns the derivation; routing only compares its results.
type RouteAddress func(ports.BrokerRouteSpec) (string, error)

// Routes is the broker's single route authorization policy. Local routing is
// provisioned by composition; remote routing is read from committed registry
// membership, never from an advisory snapshot or a startup import. A remote
// target is authorized twice against the same committed record: once when it is
// resolved, and again immediately before dialing, so a removed, replaced,
// rebound, or rerouted host can never authorize a stale target.
//
// Hosts may be assigned after the registry is built, but before it runs.
type Routes struct {
	Local   ports.BrokerRouteAuthority
	Hosts   ports.BrokerHostAuthorityReader
	Address RouteAddress
}

var _ ports.BrokerRouteAuthority = (*Routes)(nil)

var errRouteAuthorityChanged = ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "route authority changed"}

func (r *Routes) ResolveDialTarget(ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
	if err := ctx.Err(); err != nil {
		return ports.BrokerDialTarget{}, err
	}
	if request.Local {
		if r.Local == nil {
			return ports.BrokerDialTarget{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "no local route"}
		}
		return r.Local.ResolveDialTarget(ctx, request)
	}
	if r.Hosts == nil || r.Address == nil {
		return ports.BrokerDialTarget{}, errors.New("broker: incomplete route authority")
	}
	record, found, err := r.Hosts.LookupHost(ctx, request.Endpoint)
	if err != nil {
		return ports.BrokerDialTarget{}, err
	}
	if !found || !record.Registration.Equal(request.Registration) {
		return ports.BrokerDialTarget{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "unknown or stale registration"}
	}
	if !record.Policy.Compatible(request.Policy) {
		return ports.BrokerDialTarget{}, ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy}
	}
	address, err := r.Address(record.Route)
	if err != nil {
		return ports.BrokerDialTarget{}, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}
	target := ports.BrokerDialTarget{
		Fence:  ports.BrokerEndpointFence{Registration: record.Registration},
		Policy: record.Policy, Address: address, StartMode: request.StartMode,
		ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: record.Identity, Bound: record.Identity != ""},
	}
	return target, target.Validate()
}

// AuthorizeRemoteDial rechecks a resolved remote target against the committed
// record immediately before dialing and returns the route to dial. Any change
// to registration, policy, identity, or route since resolution refuses it.
func (r *Routes) AuthorizeRemoteDial(ctx context.Context, target ports.BrokerDialTarget) (ports.BrokerRouteSpec, error) {
	if target.Fence.Local {
		return ports.BrokerRouteSpec{}, errors.New("broker: local target has no remote route")
	}
	if r.Hosts == nil || r.Address == nil {
		return ports.BrokerRouteSpec{}, errors.New("broker: incomplete route authority")
	}
	record, found, err := r.Hosts.LookupHost(ctx, target.Fence.Registration.Endpoint)
	if err != nil {
		return ports.BrokerRouteSpec{}, err
	}
	if !found || !record.Registration.Equal(target.Fence.Registration) || record.Policy != target.Policy || record.Identity != target.ExpectedIdentity.Identity {
		return ports.BrokerRouteSpec{}, errRouteAuthorityChanged
	}
	address, err := r.Address(record.Route)
	if err != nil {
		return ports.BrokerRouteSpec{}, err
	}
	if address != target.Address {
		return ports.BrokerRouteSpec{}, errRouteAuthorityChanged
	}
	return record.Route, nil
}
