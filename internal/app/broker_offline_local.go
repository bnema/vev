package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/broker"
)

// Broker-owned local observation composition.
//
// The broker observes its own machine daemon through the same daemonmux
// carriage a local logical stream is dialed over. broker.LocalDaemonProbe owns
// the observation itself; this file only proves the configured route is a local
// Unix socket (so probing can never spawn a process) and wires the broker's
// already owned endpoint connector and session codec into it.

// localCarrierDialer establishes one raw framed daemonmux carriage for one
// resolved dial target. It is the same seam the pooled mux connector uses.
type localCarrierDialer func(ctx context.Context, target ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error)

func newDynamicLocalRouteProbe(route brokerconfig.Route, policy ports.BrokerPolicy, epoch ports.BrokerEpoch, connector ports.BrokerEndpointConnector, loadIdentity broker.LocalIdentityLoader) (*broker.LocalDaemonProbe, error) {
	// Probing must never spawn a process, so only a Unix route this process
	// dials directly is acceptable. brokerconfig refuses any other kind at load
	// time; this re-checks the exported binding a caller may have built itself,
	// because the dial seam routes an ssh address to a child process.
	if !route.IsLocal() {
		return nil, errors.New("vev: broker local probe requires a local route")
	}
	if err := route.Validate(); err != nil {
		return nil, fmt.Errorf("vev: broker local probe: %w", err)
	}
	return broker.NewLocalDaemonProbe(epoch, route.Address(), policy, connector, sessionwire.BrokerCodec{}, loadIdentity)
}

// brokerLocalProbe builds the broker-owned local probe over one validated
// configuration and the broker's already owned endpoint connector.
func brokerLocalProbe(config *brokerconfig.Config, epoch ports.BrokerEpoch, connector ports.BrokerEndpointConnector, loaders ...broker.LocalIdentityLoader) (*broker.LocalDaemonProbe, error) {
	binding, ok := config.LocalBinding()
	if !ok {
		return nil, errors.New("vev: broker local probe requires a provisioned local binding")
	}
	loader := broker.LocalIdentityLoader(func() (ports.BrokerDaemonIdentity, error) { return binding.Identity, nil })
	if len(loaders) != 0 {
		loader = loaders[0]
	}
	return newDynamicLocalRouteProbe(binding.Route, binding.Policy, epoch, connector, loader)
}

// brokerLocalObservation composes the registry's local producer: the
// configured display origin and policy become the local entry's authority,
// and the probe supplies observed state only.
func brokerLocalObservation(config *brokerconfig.Config, epoch ports.BrokerEpoch, connector ports.BrokerEndpointConnector, loaders ...broker.LocalIdentityLoader) (*broker.LocalObservation, error) {
	binding, ok := config.LocalBinding()
	if !ok {
		return nil, errors.New("vev: broker local observation requires a provisioned local binding")
	}
	probe, err := brokerLocalProbe(config, epoch, connector, loaders...)
	if err != nil {
		return nil, err
	}
	return &broker.LocalObservation{DisplayOrigin: binding.DisplayOrigin, Policy: binding.Policy, Probe: probe}, nil
}
