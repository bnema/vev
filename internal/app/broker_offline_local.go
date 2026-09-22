package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/broker"
)

// Broker-owned local observation composition (Plan 001 P5.3a, P7.4b).
//
// The offline broker observes its own machine daemon through the same daemonmux
// carriage a local logical stream is dialed over. The probe completes the
// authenticated physical preamble (identity, incarnation, and policy), opens
// exactly one logical BrokerStreamObservation stream, sends one typed
// remote-catalog control request, validates the catalogue the daemon answers,
// and closes the stream and carriage. It never sends a Hello, never claims a
// geometry, never requests an attachment admission, and never creates a
// session or a process, and it only ever dials an existing local Unix socket, so
// observing the local daemon never attaches to a session and never starts a
// stopped daemon.
//
// The composition is isolated from production state: it derives the probe from
// the sandbox configuration and never touches the production runtime or state
// directories. Remote and local observation share one semantic implementation
// (observeDaemonCatalogue) through the same daemonmux endpoint connector seam
// the broker already owns, so neither producer becomes a second transport
// owner and the two cannot drift apart.
//
// The hidden offline sandbox selects this producer whenever its strict config
// provisions a local binding. Ordinary production composition does not select
// it. The sandbox owns no remote membership, and probing remains no-spawn and
// attachment-free even though it runs on the registry's observation cadence.

// localCarrierDialer establishes one raw framed daemonmux carriage for one
// resolved dial target. It is the same seam the pooled mux connector uses, so
// the probe can never select a transport or an address of its own, and the
// target travels whole: the address selects the route while the start
// authorization, policy, and fence decide what the carriage owner may do.
type localCarrierDialer func(ctx context.Context, target ports.BrokerDialTarget) (daemonmux.RawFramedTransport, error)

// localRouteProbe is the broker-owned ports-independent local observation
// probe. It holds the resolved local endpoint authority (identity, policy, and
// opaque address) and one daemonmux endpoint connector over the same dial and
// ceiling seam the pooled connector uses, and observes the daemon through it.
// Every probe dials one already authenticated local carriage, opens exactly one
// observation stream, and closes both; no logical attachment stream is ever
// opened and no daemon process is ever started.
type localIdentityLoader func() (ports.BrokerDaemonIdentity, error)

type localRouteProbe struct {
	epoch        ports.BrokerEpoch
	route        brokerconfig.Route
	policy       ports.BrokerPolicy
	loadIdentity localIdentityLoader
	connector    ports.BrokerEndpointConnector
	shared       pooledPhysicals
}

var _ broker.LocalProbe = (*localRouteProbe)(nil)

// newLocalRouteProbe fences one broker-owned local binding into a probe over a
// dial seam. The binding's identity, policy, and route address must validate,
// the ceilings must be a valid advertisement, the epoch must be the broker
// process incarnation every local stream is scoped to, and a nil dial is
// refused: a probe that cannot establish a carriage must not exist. It builds
// its own endpoint connector; a composition that already owns the pooled
// connector passes it to newLocalRouteProbeWithConnector instead, so one broker
// process never holds two connector owners for the same route.
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
// probe over an existing connector. The connector is the same setup half a
// pooled physical connection uses, so the probe composes exactly the transport
// owner the broker already has and never becomes a second one. The binding's
// identity, policy, and route must validate and the epoch must be nonzero.
func newLocalRouteProbeWithConnector(binding brokerconfig.LocalBinding, epoch ports.BrokerEpoch, connector ports.BrokerEndpointConnector) (*localRouteProbe, error) {
	return newDynamicLocalRouteProbe(binding.Route, binding.Policy, epoch, connector, func() (ports.BrokerDaemonIdentity, error) {
		return binding.Identity, nil
	})
}

func newDynamicLocalRouteProbe(route brokerconfig.Route, policy ports.BrokerPolicy, epoch ports.BrokerEpoch, connector ports.BrokerEndpointConnector, loadIdentity localIdentityLoader) (*localRouteProbe, error) {
	if epoch == 0 {
		return nil, errors.New("vev: broker local probe requires a broker epoch")
	}
	if connector == nil {
		return nil, errors.New("vev: broker local probe requires a connector")
	}
	if loadIdentity == nil {
		return nil, errors.New("vev: broker local probe requires an identity loader")
	}
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
	if err := policy.Validate(); err != nil {
		return nil, fmt.Errorf("vev: broker local probe: %w", err)
	}
	return &localRouteProbe{epoch: epoch, route: route, policy: policy, loadIdentity: loadIdentity, connector: connector}, nil
}

// ProbeLocal observes the local daemon by authenticating the daemonmux physical
// preamble and then opening exactly one observation stream that asks the daemon
// for its own catalogue. A successful observation reports the authenticated
// identity, the daemon incarnation, the protocol version the catalogue schema
// negotiated, reachability, and the exact session inventory the daemon
// answered. Capabilities are not carried by the catalogue command, so they stay
// unknown (zero) rather than being invented, and every failure reports an
// explicit availability with zero observed identity: a probe never manufactures
// identity, version, or inventory it did not observe, and it never creates an
// attachment, a session, a process, or durable identity.
//
// The local daemon owns no configured membership, so the probe commits no
// durable identity binding: the binding's configured identity is the fence the
// physical preamble already verified.
func (p *localRouteProbe) ProbeLocal(ctx context.Context) (ports.BrokerDaemonObservation, error) {
	if p == nil || p.connector == nil {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, errors.New("vev: broker local probe is nil")
	}
	identity, err := p.loadIdentity()
	if err != nil {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, err
	}
	if identity == "" {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnknown}, nil
	}
	endpoint := ports.BrokerDialTarget{
		Fence: ports.BrokerEndpointFence{Local: true}, Policy: p.policy, Address: p.route.Address(),
		StartMode:        ports.BrokerDaemonExistingOnly,
		ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: identity, Bound: true},
	}
	if err := endpoint.Validate(); err != nil {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, fmt.Errorf("vev: broker local probe: %w", err)
	}
	request, err := p.request()
	if err != nil {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, err
	}
	if observation, handled, err := p.shared.observe(ctx, identity, p.policy, request); handled {
		if err != nil {
			return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, err
		}
		return observation, nil
	}
	physical, err := p.connector.Connect(ctx, endpoint)
	if err != nil {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, err
	}
	defer func() { _ = physical.Close() }()
	// The endpoint stays authoritative: re-verify the accepted authority even
	// though the connector already checked it, so a future connector change can
	// never weaken what the probe publishes.
	if physical.Identity() != endpoint.ExpectedIdentity.Identity || !physical.Policy().Compatible(endpoint.Policy) {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, errors.New("vev: broker local probe authenticated binding mismatch")
	}
	observation, err := observeDaemonCatalogue(ctx, physical, request)
	if err != nil {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}, err
	}
	return observation, nil
}

// request builds the one observation open this probe performs: a local
// observation stream that may never start the daemon. Connection and stream
// identities are fresh per attempt so a replayed or duplicated open is refused
// by the peer's anti-replay admission window. The request carries no endpoint,
// no registration, no admission, no session name, no exact target, and no
// environment: the stream asks the daemon for its own catalogue and nothing
// else.
func (p *localRouteProbe) request() (ports.BrokerOpenStreamRequest, error) {
	connection, err := randomConnectionID()
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	stream, err := randomNonzeroUint64()
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	return ports.BrokerOpenStreamRequest{
		Epoch:      p.epoch,
		Purpose:    ports.BrokerStreamObservation,
		Local:      true,
		Connection: connection,
		Stream:     ports.BrokerStreamID(stream),
		Policy:     p.policy,
		// Observation never starts the target: an ExistingOnly authorization is
		// what ports enforces for an observation stream, and a start-if-needed
		// one would be refused rather than silently widened into a spawn.
		StartMode: ports.BrokerDaemonExistingOnly,
	}, nil
}

// brokerLocalProbe builds the broker-owned local probe over one validated
// offline configuration and the broker's already owned endpoint connector, so
// the probe composes the exact transport owner the pool does instead of
// creating a second one.
func brokerLocalProbe(config *brokerconfig.Config, epoch ports.BrokerEpoch, connector ports.BrokerEndpointConnector, loaders ...localIdentityLoader) (*localRouteProbe, error) {
	binding, ok := config.LocalBinding()
	if !ok {
		return nil, errors.New("vev: broker local probe requires a provisioned local binding")
	}
	loader := localIdentityLoader(func() (ports.BrokerDaemonIdentity, error) { return binding.Identity, nil })
	if len(loaders) != 0 {
		loader = loaders[0]
	}
	return newDynamicLocalRouteProbe(binding.Route, binding.Policy, epoch, connector, loader)
}

// brokerLocalObservation composes the registry's local producer from one
// validated offline configuration: the configured display origin and policy
// become the local entry's authority, and the probe supplies observed state
// only. A configuration without a local binding provisions no local producer.
func brokerLocalObservation(config *brokerconfig.Config, epoch ports.BrokerEpoch, connector ports.BrokerEndpointConnector, loaders ...localIdentityLoader) (*broker.LocalObservation, error) {
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
