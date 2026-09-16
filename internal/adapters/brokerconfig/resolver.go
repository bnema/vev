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
type Resolver struct {
	byEndpoint map[string]Registration
}

var _ ports.BrokerEndpointResolver = (*Resolver)(nil)

// Resolve fences one open-stream request against the immutable configuration.
//
// The request must name a configured endpoint, carry that endpoint's exact
// registration (endpoint, incarnation, and generation), and request a policy
// compatible with the provisioned policy. A local request is refused because
// the offline sandbox provisions remote routes only. Unknown, stale,
// or conflicting requests are refused with a typed broker error and no dial is
// attempted; a cancelled context is reported as such so the pool classifies it
// as cancellation rather than unavailability.
func (r *Resolver) Resolve(ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerResolvedEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return ports.BrokerResolvedEndpoint{}, err
	}
	if request.Local {
		return ports.BrokerResolvedEndpoint{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: errNoLocalRoute}
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
