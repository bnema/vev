package brokerconfig

import (
	"context"
	"errors"

	"github.com/bnema/vev/internal/ports"
)

// Resolver is the immutable ports.BrokerEndpointResolver over one parsed
// offline configuration. It holds no mutable state and performs no I/O: every
// Resolve call fences the request against the provisioned registration and
// returns the configured identity, policy, and opaque route address. A request
// can therefore never select an address, identity, or policy of its own.
//
// A remote request is fenced against its provisioned registration (endpoint,
// incarnation, and generation) and policy. A local request carries no endpoint
// and no registration, so it is fenced against the broker-owned local binding:
// the configured local identity, policy, and Unix daemonmux route. Both paths
// return authority chosen by the configuration alone, and neither can widen,
// narrow, or replace the provisioned identity or policy.
type Resolver struct {
	byEndpoint map[string]Registration
	local      *LocalBinding
}

var _ ports.BrokerEndpointResolver = (*Resolver)(nil)

// Resolve fences one open-stream request against the immutable configuration.
//
// A remote request must name a configured endpoint, carry that endpoint's
// exact registration (endpoint, incarnation, and generation), and request a
// policy compatible with the provisioned policy. A local request must instead
// carry an empty endpoint and zero registration (ports enforces that) and is
// fenced against the provisioned local binding: without one it is refused, and
// with one its requested policy must be exactly compatible with the
// provisioned local policy. Either way the resolved identity, policy, and
// opaque route address come from the configuration, never from the request.
//
// Unknown, stale, conflicting, or unprovisioned requests are refused with a
// typed broker error and no dial is attempted; a cancelled context is reported
// as such so the pool classifies it as cancellation rather than unavailability.
func (r *Resolver) Resolve(ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerResolvedEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return ports.BrokerResolvedEndpoint{}, err
	}
	if request.Local {
		return r.resolveLocal(request)
	}
	provisioned, ok := r.byEndpoint[request.Endpoint]
	if !ok {
		return ports.BrokerResolvedEndpoint{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: errUnknownEndpoint}
	}
	if !request.Registration.Equal(provisioned.Registration) {
		return ports.BrokerResolvedEndpoint{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: errStaleRegistration}
	}
	if !provisioned.Policy.Compatible(request.Policy) {
		return ports.BrokerResolvedEndpoint{}, ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy, Cause: errors.New("brokerconfig: request policy conflicts with the provisioned policy")}
	}
	resolved := ports.BrokerResolvedEndpoint{Identity: provisioned.Identity, Policy: provisioned.Policy, Address: provisioned.Route.Address()}
	if err := resolved.Validate(); err != nil {
		return ports.BrokerResolvedEndpoint{}, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}
	return resolved, nil
}

// resolveLocal fences one local request against the broker-owned local
// binding. A configuration that provisions no local route refuses the request
// as unavailable; a request whose policy is not exactly compatible with the
// provisioned local policy is refused as a conflicting policy. The resolved
// identity, policy, and address are the binding's own, so the request supplies
// none of them.
func (r *Resolver) resolveLocal(request ports.BrokerOpenStreamRequest) (ports.BrokerResolvedEndpoint, error) {
	if r.local == nil {
		return ports.BrokerResolvedEndpoint{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: errNoLocalRoute}
	}
	if !r.local.Policy.Compatible(request.Policy) {
		return ports.BrokerResolvedEndpoint{}, ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy, Cause: errLocalPolicyConflict}
	}
	resolved := ports.BrokerResolvedEndpoint{Identity: r.local.Identity, Policy: r.local.Policy, Address: r.local.Route.Address()}
	if err := resolved.Validate(); err != nil {
		return ports.BrokerResolvedEndpoint{}, ports.BrokerError{Code: ports.BrokerErrorIncompatible, Cause: err}
	}
	return resolved, nil
}
