package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/broker"
)

// Broker-owned local observation composition (Plan 001 P5.3a).
//
// The offline broker observes its own machine daemon through the same daemonmux
// carriage a local logical stream is dialed over. The probe completes only the
// physical preamble: it dials the provisioned local Unix route, negotiates
// identity, incarnation, and policy, and closes the carriage. It deliberately
// opens no logical stream, so observing the local daemon never attaches to a
// session, and it only ever dials an existing local Unix socket, so it never
// starts a stopped daemon. The composition is isolated from production state:
// it derives the probe from the sandbox configuration and never touches the
// production runtime or state directories.
//
// The hidden offline sandbox selects this producer whenever its strict config
// provisions a local binding. Ordinary production composition does not select
// it. The sandbox owns no remote membership, and probing remains no-spawn and
// attachment-free even though it runs on the registry's observation cadence.

// localCarrierDialer establishes one raw framed daemonmux carriage for the
// opaque route address the broker-owned local binding published. It is the same
// seam the pooled mux connector uses, so the probe can never select a transport
// or an address of its own.
type localCarrierDialer func(ctx context.Context, address string) (daemonmux.RawFramedTransport, error)

// localRouteProbe is the broker-owned ports-independent local observation
// probe. It holds the resolved local endpoint authority (identity, policy, and
// opaque address) and dials it exactly once per observation, then closes the
// carriage: no logical stream is ever opened and no daemon process is ever
// started.
type localRouteProbe struct {
	endpoint ports.BrokerResolvedEndpoint
	dial     localCarrierDialer
	ceilings daemonmux.MuxCeilings
}

var _ broker.LocalProbe = (*localRouteProbe)(nil)

// newLocalRouteProbe fences one broker-owned local binding into a probe. The
// binding's identity, policy, and route address must validate, the ceilings
// must be a valid advertisement, and a nil dial is refused: a probe that cannot
// establish a carriage must not exist.
func newLocalRouteProbe(binding brokerconfig.LocalBinding, dial localCarrierDialer, ceilings daemonmux.MuxCeilings) (*localRouteProbe, error) {
	if dial == nil {
		return nil, errors.New("vev: broker local probe requires a dialer")
	}
	// Probing must never spawn a process, so only a Unix route this process
	// dials directly is acceptable. brokerconfig refuses any other kind at load
	// time; this re-checks the exported binding a caller may have built itself,
	// because the dial seam routes an ssh address to a child process.
	if !binding.Route.IsLocal() {
		return nil, errors.New("vev: broker local probe requires a local route")
	}
	if err := ceilings.Validate(); err != nil {
		return nil, fmt.Errorf("vev: broker local probe: %w", err)
	}
	endpoint := ports.BrokerResolvedEndpoint{Identity: binding.Identity, Policy: binding.Policy, Address: binding.Route.Address()}
	if err := endpoint.Validate(); err != nil {
		return nil, fmt.Errorf("vev: broker local probe: %w", err)
	}
	return &localRouteProbe{endpoint: endpoint, dial: dial, ceilings: ceilings}, nil
}

// ProbeLocal observes the local daemon by completing the daemonmux physical
// preamble and closing the carriage. A successful preamble reports the only
// observed state the carriage exposes: the authenticated identity, the daemon
// incarnation, the protocol version the daemon's accepted policy negotiated,
// and reachability. Capabilities and the session catalogue are not carried by
// the physical preamble, so they stay unknown (zero) rather than being
// invented, and every failure reports an explicit availability with zero
// observed identity: a probe never manufactures identity, version, or
// inventory it did not observe.
func (p *localRouteProbe) ProbeLocal(ctx context.Context) (ports.BrokerDaemonObservation, error) {
	if p == nil {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, errors.New("vev: broker local probe is nil")
	}
	raw, err := p.dial(ctx, p.endpoint.Address)
	if err != nil {
		if raw != nil {
			_ = raw.Close()
		}
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, err
	}
	if raw == nil {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, errors.New("vev: broker local probe: nil carriage")
	}
	bridge, err := daemonmux.NewPreambleCarrier(raw)
	if err != nil {
		_ = raw.Close()
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, err
	}
	// RunClientHandshake closes the carriage on every failure; on success the
	// probe closes it here, because it has nothing further to send. The
	// handshake never opens a logical stream.
	result, err := daemonmux.RunClientHandshake(ctx, bridge, p.endpoint, p.ceilings)
	_ = bridge.Close()
	if err != nil {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, err
	}
	return ports.BrokerDaemonObservation{
		Identity:        result.Identity,
		Incarnation:     result.Incarnation,
		ProtocolVersion: result.Policy.ProtocolVersion,
		Availability:    domain.RemoteAvailabilityReachable,
	}, nil
}

// brokerLocalProbe builds the broker-owned local probe over one validated
// offline configuration. The dialer looks the opaque address up in the same
// immutable configuration the pooled connector uses, so a probe can never reach
// an address the configuration did not provision.
func brokerLocalProbe(config *brokerconfig.Config, log *slog.Logger) (*localRouteProbe, error) {
	binding, ok := config.LocalBinding()
	if !ok {
		return nil, errors.New("vev: broker local probe requires a provisioned local binding")
	}
	dial := func(ctx context.Context, address string) (daemonmux.RawFramedTransport, error) {
		route, ok := config.RouteByAddress(address)
		if !ok {
			return nil, errors.New("vev: broker local probe: unknown route address")
		}
		return dialBrokerRoute(ctx, route, log)
	}
	return newLocalRouteProbe(binding, dial, daemonmux.DefaultMuxCeilings())
}

// brokerLocalObservation composes the registry's local producer from one
// validated offline configuration: the configured display origin and policy
// become the local entry's authority, and the probe supplies observed state
// only. A configuration without a local binding provisions no local producer.
func brokerLocalObservation(config *brokerconfig.Config, log *slog.Logger) (*broker.LocalObservation, error) {
	binding, ok := config.LocalBinding()
	if !ok {
		return nil, errors.New("vev: broker local observation requires a provisioned local binding")
	}
	probe, err := brokerLocalProbe(config, log)
	if err != nil {
		return nil, err
	}
	return &broker.LocalObservation{DisplayOrigin: binding.DisplayOrigin, Policy: binding.Policy, Probe: probe}, nil
}
