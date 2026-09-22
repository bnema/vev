package app

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

// Remote daemon observation composition (Plan 001 P7.4b).
//
// brokerRemoteProbe observes one configured daemon over the broker-owned
// multiplexed carriage. It opens an observation stream, never an attachment,
// and asks the daemon for its local catalogue through the existing typed
// command protocol. Authenticated daemon identity and process incarnation come
// from the physical handshake rather than from the configured hostname.
//
// Remote and local observation share one semantic implementation below:
// observeDaemonCatalogue is the single "observe an already authenticated
// daemon" path, so the two producers cannot drift apart and neither becomes a
// second owner of a transport. The broker-owned local probe composes the same
// daemonmux carriage and the same pooled connector the broker already owns
// rather than dialing or authenticating a daemon itself.
type brokerRemoteProbe struct {
	epoch     ports.BrokerEpoch
	routes    ports.BrokerRouteAuthority
	binder    ports.BrokerIdentityBinder
	connector ports.BrokerEndpointConnector
	hosts     ports.BrokerHostAuthorityReader
	shared    pooledPhysicals
}

// sharedPhysicals lends the pooled transport of an attached or warm daemon.
type sharedPhysicals interface {
	SharedPhysical(ports.BrokerDaemonIdentity, ports.BrokerPolicy) (ports.BrokerPhysicalConnection, bool)
}

// pooledPhysicals lets an observation probe borrow the broker pool's physical
// transport instead of dialing (and, for a remote, bootstrapping SSH/QUIC)
// again: the broker exists to share connections. The pool is published after
// composition because it binds identities through the registry that runs the
// probes. A probe dials only when nothing is pooled, or when the borrowed
// transport retired under it.
type pooledPhysicals struct{ pool atomic.Value }

func (s *pooledPhysicals) share(pool sharedPhysicals) { s.pool.Store(pool) }

// observe reports handled=false when the probe must dial its own transport.
func (s *pooledPhysicals) observe(ctx context.Context, identity ports.BrokerDaemonIdentity, policy ports.BrokerPolicy, request ports.BrokerOpenStreamRequest) (ports.BrokerDaemonObservation, bool, error) {
	pool, _ := s.pool.Load().(sharedPhysicals)
	if pool == nil || identity == "" {
		return ports.BrokerDaemonObservation{}, false, nil
	}
	physical, ok := pool.SharedPhysical(identity, policy)
	if !ok {
		return ports.BrokerDaemonObservation{}, false, nil
	}
	observation, err := observeDaemonCatalogue(ctx, physical, request)
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

var _ ports.BrokerHostProbe = (*brokerRemoteProbe)(nil)

func (p *brokerRemoteProbe) Probe(ctx context.Context, registration domain.RemoteRegistration) (ports.BrokerDaemonObservation, error) {
	if p == nil || p.epoch == 0 || p.routes == nil || p.binder == nil || p.connector == nil {
		return ports.BrokerDaemonObservation{}, errors.New("vev: incomplete broker remote probe")
	}
	request, err := p.request(ctx, registration)
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	endpoint, err := p.routes.ResolveDialTarget(ctx, request)
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	// Borrow the pooled transport of an attached or warm daemon instead of
	// bootstrapping SSH/QUIC again. A borrowed transport that retires mid-probe
	// falls back to a fresh dial.
	if endpoint.ExpectedIdentity.Bound {
		if observation, handled, err := p.shared.observe(ctx, endpoint.ExpectedIdentity.Identity, endpoint.Policy, request); handled {
			return observation, err
		}
	}
	physical, err := p.connector.Connect(ctx, endpoint)
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	defer physical.Close()
	if (endpoint.ExpectedIdentity.Bound && physical.Identity() != endpoint.ExpectedIdentity.Identity) || physical.Policy() != endpoint.Policy {
		return ports.BrokerDaemonObservation{}, errors.New("vev: remote probe authenticated binding mismatch")
	}
	identity, err := p.binder.BindAuthenticatedIdentity(ctx, ports.BrokerIdentityBindingRequest{Fence: endpoint.Fence, Policy: endpoint.Policy, Identity: physical.Identity()})
	if err != nil || identity != physical.Identity() {
		return ports.BrokerDaemonObservation{}, errors.New("vev: remote probe could not commit authenticated binding")
	}
	return observeDaemonCatalogue(ctx, physical, request)
}

// observeDaemonCatalogue observes one already authenticated daemon over its
// physical connection by opening exactly one observation logical stream and
// asking the daemon for its own catalogue through the existing typed command
// protocol. It sends no Hello, claims no geometry, requests no admission, and
// creates no session or process: the only traffic on the stream is the
// observation open and the one correlated remote-catalog command. The observed
// identity and incarnation are the physical handshake's authenticated values,
// never a configured hostname.
//
// A refused open, an unmatched or invalid result, a failed outcome, a malformed
// catalogue, and a catalogue outside the exact current schema all fail closed:
// the caller receives an error and no observation it could publish, so a probe
// never invents inventory it did not observe.
func observeDaemonCatalogue(ctx context.Context, physical ports.BrokerPhysicalConnection, request ports.BrokerOpenStreamRequest) (ports.BrokerDaemonObservation, error) {
	if physical == nil || request.Purpose != ports.BrokerStreamObservation {
		return ports.BrokerDaemonObservation{}, errors.New("vev: invalid daemon observation request")
	}
	logical, err := physical.OpenStream(ctx, request)
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	if logical == nil {
		return ports.BrokerDaemonObservation{}, errors.New("vev: daemon observation opened no logical stream")
	}
	defer func() { _ = logical.Close() }()
	commandID, err := randomNonzeroUint64()
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	reply, err := exchangeObservationCommand(ctx, logical, protocol.CommandRequest{RequestID: commandID, Version: protocol.Version, Slug: "remote-catalog", JSON: true})
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	result, ok := reply.(protocol.CommandResult)
	if !ok || result.RequestID != commandID || !result.Valid() {
		return ports.BrokerDaemonObservation{}, errors.New("vev: invalid daemon observation result")
	}
	if result.Outcome != protocol.CommandSucceeded {
		return ports.BrokerDaemonObservation{}, fmt.Errorf("vev: daemon observation failed: %s", result.Text)
	}
	var catalog catalogue.RemoteCatalog
	if err := json.Unmarshal([]byte(result.Output), &catalog); err != nil {
		return ports.BrokerDaemonObservation{}, fmt.Errorf("vev: decode daemon observation: %w", err)
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
// joined before returning, so an observation always returns promptly once its
// attempt is retired instead of parking the daemon's reader (or a probe
// attempt) on a daemon that answers nothing.
func exchangeObservationCommand(ctx context.Context, logical ports.BrokerLogicalConnection, command protocol.CommandRequest) (protocol.ServerMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	type outcome struct {
		message protocol.ServerMessage
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		if err := logical.SendClient(command); err != nil {
			done <- outcome{err: err}
			return
		}
		message, err := logical.ReceiveServer()
		done <- outcome{message: message, err: err}
	}()
	select {
	case result := <-done:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return result.message, result.err
	case <-ctx.Done():
		// Closing the stream unblocks the admitted read; the worker is joined so
		// no observation goroutine outlives its attempt.
		_ = logical.Close()
		<-done
		return nil, ctx.Err()
	}
}

func (p *brokerRemoteProbe) request(ctx context.Context, registration domain.RemoteRegistration) (ports.BrokerOpenStreamRequest, error) {
	if p.hosts == nil {
		return ports.BrokerOpenStreamRequest{}, errors.New("vev: remote probe has no policy authority")
	}
	record, ok, err := p.hosts.LookupHost(ctx, registration.Endpoint)
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	if !ok || !record.Registration.Equal(registration) {
		return ports.BrokerOpenStreamRequest{}, ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "unknown or stale probe registration"}
	}
	policy := record.Policy
	connection, err := randomConnectionID()
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	stream, err := randomNonzeroUint64()
	if err != nil {
		return ports.BrokerOpenStreamRequest{}, err
	}
	return ports.BrokerOpenStreamRequest{
		Epoch: p.epoch, Purpose: ports.BrokerStreamObservation,
		Connection: connection, Stream: ports.BrokerStreamID(stream),
		Endpoint: registration.Endpoint, Registration: registration, Policy: policy,
		// Observation must never start a stopped daemon, so it always carries
		// the existing-only authorization.
		StartMode: ports.BrokerDaemonExistingOnly,
	}, nil
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
