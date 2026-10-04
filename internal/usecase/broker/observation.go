package broker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Daemon observation.
//
// RemoteProbe and LocalDaemonProbe observe one daemon over the broker-owned
// multiplexed carriage. Both open an observation stream, never an attachment,
// and ask the daemon for its own catalogue through the typed command protocol.
// Authenticated identity and incarnation come from the physical handshake,
// never from a configured hostname. Both share observeDaemonCatalogue, so the
// two producers cannot drift apart, and both borrow the pool's transport
// before dialing their own, so neither becomes a second transport owner.

// pooledPhysicals lets a probe borrow the broker pool's physical transport
// instead of dialing (and, for a remote, bootstrapping SSH/QUIC) again. The
// pool is published after composition because it binds identities through the
// registry that runs the probes. A probe dials only when nothing is pooled, or
// when the borrowed transport retired under it.
type pooledPhysicals struct{ pool atomic.Pointer[Pool] }

func (s *pooledPhysicals) adopt(physical ports.BrokerPhysicalConnection, policy ports.BrokerPolicy) bool {
	pool := s.pool.Load()
	return pool != nil && pool.AdoptPhysical(physical, policy)
}

// observe reports handled=false when the probe must dial its own transport.
func (s *pooledPhysicals) observe(ctx context.Context, identity ports.BrokerDaemonIdentity, policy ports.BrokerPolicy, request ports.BrokerOpenStreamRequest, codec ports.SessionCodec) (ports.BrokerDaemonObservation, bool, error) {
	pool := s.pool.Load()
	if pool == nil || identity == "" {
		return ports.BrokerDaemonObservation{}, false, nil
	}
	physical, ok := pool.SharedPhysical(identity, policy)
	if !ok {
		return ports.BrokerDaemonObservation{}, false, nil
	}
	observation, err := observeDaemonCatalogue(ctx, physical, request, codec)
	if err == nil || ctx.Err() != nil {
		return observation, true, err
	}
	select {
	case <-physical.Done():
		return ports.BrokerDaemonObservation{}, false, nil
	default:
		return observation, true, err
	}
}

// RemoteProbe observes one configured remote daemon. Routes, Binder, and Hosts
// may be assigned after the registry that runs the probe is built, but before
// the registry starts; composition is cyclic because the registry is both the
// host authority and the probe's caller.
type RemoteProbe struct {
	Epoch     ports.BrokerEpoch
	Routes    ports.BrokerRouteAuthority
	Binder    ports.BrokerIdentityBinder
	Connector ports.BrokerEndpointConnector
	Hosts     ports.BrokerHostAuthorityReader
	Codec     ports.SessionCodec

	shared pooledPhysicals
}

var _ ports.BrokerHostProbe = (*RemoteProbe)(nil)

// SharePool lets later probes borrow the pool's attached or warm transports.
func (p *RemoteProbe) SharePool(pool *Pool) { p.shared.pool.Store(pool) }

func (p *RemoteProbe) Probe(ctx context.Context, registration domain.RemoteRegistration) (ports.BrokerDaemonObservation, error) {
	if p == nil || p.Epoch == 0 || p.Routes == nil || p.Binder == nil || p.Connector == nil {
		return ports.BrokerDaemonObservation{}, errors.New("incomplete broker remote probe")
	}
	request, err := p.request(ctx, registration)
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	endpoint, err := p.Routes.ResolveDialTarget(ctx, request)
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	// Borrow the pooled transport of an attached or warm daemon instead of
	// bootstrapping SSH/QUIC again. A borrowed transport that retires mid-probe
	// falls back to a fresh dial.
	if endpoint.ExpectedIdentity.Bound {
		if observation, handled, err := p.shared.observe(ctx, endpoint.ExpectedIdentity.Identity, endpoint.Policy, request, p.Codec); handled {
			return observation, err
		}
	}
	physical, err := p.Connector.Connect(ctx, endpoint)
	if err != nil {
		var brokerErr ports.BrokerError
		if errors.As(err, &brokerErr) && brokerErr.Code == ports.BrokerErrorNoDaemon {
			// This typed dial-time result means the SSH helper ran on the remote
			// machine but its local daemon carriage was absent. Do not infer this
			// from a later stream loss on an already authenticated connection.
			return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityNoDaemon}, nil
		}
		return ports.BrokerDaemonObservation{}, err
	}
	adopted := false
	defer func() {
		if !adopted {
			_ = physical.Close()
		}
	}()
	if (endpoint.ExpectedIdentity.Bound && physical.Identity() != endpoint.ExpectedIdentity.Identity) || physical.Policy() != endpoint.Policy {
		return ports.BrokerDaemonObservation{}, errors.New("remote probe authenticated binding mismatch")
	}
	identity, err := p.Binder.BindAuthenticatedIdentity(ctx, ports.BrokerIdentityBindingRequest{Fence: endpoint.Fence, Policy: endpoint.Policy, Identity: physical.Identity()})
	if err != nil || identity != physical.Identity() {
		return ports.BrokerDaemonObservation{}, errors.New("remote probe could not commit authenticated binding")
	}
	observation, err := observeDaemonCatalogue(ctx, physical, request, p.Codec)
	if err == nil && ctx.Err() == nil {
		adopted = p.shared.adopt(physical, endpoint.Policy)
	}
	return observation, err
}

func (p *RemoteProbe) request(ctx context.Context, registration domain.RemoteRegistration) (ports.BrokerOpenStreamRequest, error) {
	if p.Hosts == nil {
		return ports.BrokerOpenStreamRequest{}, errors.New("remote probe has no policy authority")
	}
	record, ok, err := p.Hosts.LookupHost(ctx, registration.Endpoint)
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	if !ok || !record.Registration.Equal(registration) {
		return ports.BrokerOpenStreamRequest{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "unknown or stale probe registration"}
	}
	request, err := newObservationRequest(p.Epoch, record.Policy)
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	request.Endpoint = registration.Endpoint
	request.Registration = registration
	return request, nil
}

// LocalIdentityLoader returns the local daemon's persisted identity, or ""
// when no daemon has ever published one.
type LocalIdentityLoader func() (ports.BrokerDaemonIdentity, error)

// LocalDaemonProbe observes the broker's own machine daemon over one local
// address. It commits no durable identity binding: the local daemon owns no
// configured membership, and the identity the physical preamble verified is
// the fence.
type LocalDaemonProbe struct {
	epoch        ports.BrokerEpoch
	address      string
	policy       ports.BrokerPolicy
	loadIdentity LocalIdentityLoader
	connector    ports.BrokerEndpointConnector
	codec        ports.SessionCodec
	shared       pooledPhysicals
}

var _ LocalProbe = (*LocalDaemonProbe)(nil)

// NewLocalDaemonProbe validates one local observation route. The caller must
// have proven the address is a local socket this process dials directly:
// probing must never spawn a process.
func NewLocalDaemonProbe(epoch ports.BrokerEpoch, address string, policy ports.BrokerPolicy, connector ports.BrokerEndpointConnector, codec ports.SessionCodec, loadIdentity LocalIdentityLoader) (*LocalDaemonProbe, error) {
	if epoch == 0 {
		return nil, errors.New("broker local probe requires a broker epoch")
	}
	if nilDependency(connector) {
		return nil, errors.New("broker local probe requires a connector")
	}
	if nilDependency(codec) {
		return nil, errors.New("broker local probe requires a codec")
	}
	if loadIdentity == nil {
		return nil, errors.New("broker local probe requires an identity loader")
	}
	if address == "" {
		return nil, errors.New("broker local probe requires an address")
	}
	if err := policy.Validate(); err != nil {
		return nil, fmt.Errorf("broker local probe: %w", err)
	}
	return &LocalDaemonProbe{epoch: epoch, address: address, policy: policy, loadIdentity: loadIdentity, connector: connector, codec: codec}, nil
}

// SharePool lets later probes borrow the pool's attached or warm transports.
func (p *LocalDaemonProbe) SharePool(pool *Pool) { p.shared.pool.Store(pool) }

// ProbeLocal authenticates the daemonmux physical preamble and then opens
// exactly one observation stream that asks the daemon for its own catalogue.
// Capabilities are not carried by the catalogue command, so they stay zero.
// Every failure reports an explicit availability with zero observed identity:
// a probe never manufactures identity, version, or inventory it did not
// observe, and it never creates an attachment, a session, or a process.
func (p *LocalDaemonProbe) ProbeLocal(ctx context.Context) (ports.BrokerDaemonObservation, error) {
	unreachable := ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}
	if p == nil || p.connector == nil {
		return unreachable, errors.New("broker local probe is nil")
	}
	identity, err := p.loadIdentity()
	if err != nil {
		return unreachable, err
	}
	if identity == "" {
		return ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnknown}, nil
	}
	endpoint := ports.BrokerDialTarget{
		Fence: ports.BrokerEndpointFence{Local: true}, Policy: p.policy, Address: p.address,
		StartMode:        ports.BrokerDaemonExistingOnly,
		ExpectedIdentity: ports.BrokerExpectedIdentity{Identity: identity, Bound: true},
	}
	if err := endpoint.Validate(); err != nil {
		return unreachable, fmt.Errorf("broker local probe: %w", err)
	}
	request, err := p.request()
	if err != nil {
		return unreachable, err
	}
	if observation, handled, err := p.shared.observe(ctx, identity, p.policy, request, p.codec); handled {
		if err != nil {
			return unreachable, err
		}
		return observation, nil
	}
	physical, err := p.connector.Connect(ctx, endpoint)
	if err != nil {
		return unreachable, err
	}
	defer func() { _ = physical.Close() }()
	// The endpoint stays authoritative: re-verify the accepted authority even
	// though the connector already checked it, so a future connector change can
	// never weaken what the probe publishes.
	if physical.Identity() != endpoint.ExpectedIdentity.Identity || !physical.Policy().Compatible(endpoint.Policy) {
		return unreachable, errors.New("broker local probe authenticated binding mismatch")
	}
	observation, err := observeDaemonCatalogue(ctx, physical, request, p.codec)
	if err != nil {
		return unreachable, err
	}
	return observation, nil
}

// request builds the one local observation open this probe performs. It
// carries no endpoint, registration, admission, name, target, or environment.
func (p *LocalDaemonProbe) request() (ports.BrokerOpenStreamRequest, error) {
	request, err := newObservationRequest(p.epoch, p.policy)
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	request.Local = true
	return request, nil
}

// newObservationRequest builds an observation open with fresh connection and
// stream identities, so a replayed or duplicated open is refused by the peer's
// anti-replay window. Observation never starts the target: it always carries
// the existing-only authorization, and a start-if-needed one is refused by
// validation rather than silently widened into a spawn.
func newObservationRequest(epoch ports.BrokerEpoch, policy ports.BrokerPolicy) (ports.BrokerOpenStreamRequest, error) {
	connection, err := randomConnectionID()
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	stream, err := randomNonzeroUint64()
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	return ports.BrokerOpenStreamRequest{
		Epoch: epoch, Purpose: ports.BrokerStreamObservation,
		Connection: connection, Stream: ports.BrokerStreamID(stream),
		Policy: policy, StartMode: ports.BrokerDaemonExistingOnly,
	}, nil
}

// observeDaemonCatalogue observes one already authenticated daemon by opening
// exactly one observation logical stream and asking the daemon for its own
// catalogue. It sends no Hello, claims no geometry, requests no admission, and
// creates no session or process.
//
// A refused open, an unmatched or invalid result, a failed outcome, a malformed
// catalogue, and a catalogue outside the exact current schema all fail closed:
// the caller receives an error and no observation it could publish.
func observeDaemonCatalogue(ctx context.Context, physical ports.BrokerPhysicalConnection, request ports.BrokerOpenStreamRequest, codec ports.SessionCodec) (ports.BrokerDaemonObservation, error) {
	if physical == nil || codec == nil || request.Purpose != ports.BrokerStreamObservation {
		return ports.BrokerDaemonObservation{}, errors.New("invalid daemon observation request")
	}
	logical, err := physical.OpenStream(ctx, request)
	if err != nil {
		// A connection that completed the authenticated physical handshake had
		// a daemon. Losing its logical stream is a transport failure, not proof
		// that the daemon is absent; preserve the failure so pooled callers can
		// discard a dead connection and retry with a fresh dial.
		return ports.BrokerDaemonObservation{}, err
	}
	if logical == nil {
		return ports.BrokerDaemonObservation{}, errors.New("daemon observation opened no logical stream")
	}
	defer func() { _ = logical.Close() }()
	commandID, err := randomNonzeroUint64()
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	reply, err := exchangeObservationCommand(ctx, codec.Client(logical), logical, protocol.CommandRequest{RequestID: commandID, Version: protocol.Version, Slug: "remote-catalog", JSON: true})
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	result, ok := reply.(protocol.CommandResult)
	if !ok || result.RequestID != commandID || !result.Valid() {
		return ports.BrokerDaemonObservation{}, errors.New("invalid daemon observation result")
	}
	if result.Outcome != protocol.CommandSucceeded {
		return ports.BrokerDaemonObservation{}, fmt.Errorf("daemon observation failed: %s", result.Text)
	}
	var catalog catalogue.RemoteCatalog
	if err := json.Unmarshal([]byte(result.Output), &catalog); err != nil {
		return ports.BrokerDaemonObservation{}, fmt.Errorf("decode daemon observation: %w", err)
	}
	if err := catalogue.ValidateRemoteCatalog(catalog); err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	return ports.BrokerDaemonObservation{
		Identity:        physical.Identity(),
		Incarnation:     physical.Incarnation(),
		ProtocolVersion: catalog.ProtocolVersion,
		Availability:    domain.RemoteAvailabilityReachable,
		InventoryKnown:  true,
		Sessions:        catalog.Sessions,
	}, nil
}

// exchangeObservationCommand sends one typed command and waits for one reply
// under the caller's context. A cancelled or expired context closes exactly
// this logical stream, which unblocks the pending read, and the worker is
// joined before returning, so no observation goroutine outlives its attempt.
func exchangeObservationCommand(ctx context.Context, typed ports.ClientConnection, logical ports.BrokerEnvelopeStream, command protocol.CommandRequest) (protocol.ServerMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	type outcome struct {
		message protocol.ServerMessage
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		if err := typed.SendClient(command); err != nil {
			done <- outcome{err: err}
			return
		}
		message, err := typed.ReceiveServer()
		done <- outcome{message: message, err: err}
	}()
	select {
	case result := <-done:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return result.message, result.err
	case <-ctx.Done():
		_ = logical.Close()
		<-done
		return nil, ctx.Err()
	}
}

func randomConnectionID() (ports.BrokerConnectionID, error) {
	var id ports.BrokerConnectionID
	if _, err := rand.Read(id[:]); err != nil {
		return id, err
	}
	if err := id.Validate(); err != nil {
		return randomConnectionID()
	}
	return id, nil
}

func randomNonzeroUint64() (uint64, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return 0, err
	}
	value := binary.BigEndian.Uint64(raw[:])
	if value == 0 {
		return randomNonzeroUint64()
	}
	return value, nil
}
