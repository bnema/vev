package app

import (
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/ports"
)

// newLocalRouteProbe fences one broker-owned local binding into a probe over a
// dial seam. The binding's identity, policy, and route address must validate,
// the ceilings must be a valid advertisement, the epoch must be the broker
// process incarnation every local stream is scoped to, and a nil dial is
// refused: a probe that cannot establish a carriage must not exist. It builds
// its own endpoint connector; a caller that already owns the pooled connector
// uses newLocalRouteProbeWithConnector instead, exactly as brokerLocalProbe
// does in production.
func newLocalRouteProbe(binding brokerconfig.LocalBinding, epoch ports.BrokerEpoch, dial localCarrierDialer, ceilings daemonmux.MuxCeilings) (*localRouteProbe, error) {
	if dial == nil {
		return nil, errors.New("vev: broker local probe requires a dialer")
	}
	connector, err := daemonmux.NewEndpointConnector(daemonmux.RawCarrierDialer(dial), ceilings)
	if err != nil {
		return nil, fmt.Errorf("vev: broker local probe: %w", err)
	}
	return newLocalRouteProbeWithConnector(binding, epoch, connector)
}

// newLocalRouteProbeWithConnector fences one broker-owned local binding into a
// probe over an existing connector, delegating to the same
// newDynamicLocalRouteProbe production constructor brokerLocalProbe uses.
func newLocalRouteProbeWithConnector(binding brokerconfig.LocalBinding, epoch ports.BrokerEpoch, connector ports.BrokerEndpointConnector) (*localRouteProbe, error) {
	return newDynamicLocalRouteProbe(binding.Route, binding.Policy, epoch, connector, func() (ports.BrokerDaemonIdentity, error) {
		return binding.Identity, nil
	})
}
