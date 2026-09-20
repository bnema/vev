package app

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// brokerRemoteProbe observes one configured daemon over the broker-owned
// multiplexed carriage. It opens an observation stream, never an attachment,
// and asks the daemon for its local catalogue through the existing typed
// command protocol. Authenticated daemon identity and process incarnation come
// from the physical handshake rather than from the configured hostname.
type brokerRemoteProbe struct {
	epoch     ports.BrokerEpoch
	resolver  ports.BrokerEndpointResolver
	connector ports.BrokerEndpointConnector
	policy    func(string) (ports.BrokerPolicy, bool)
}

var _ ports.BrokerHostProbe = (*brokerRemoteProbe)(nil)

func (p *brokerRemoteProbe) Probe(ctx context.Context, registration domain.RemoteRegistration) (ports.BrokerDaemonObservation, error) {
	if p == nil || p.epoch == 0 || p.resolver == nil || p.connector == nil {
		return ports.BrokerDaemonObservation{}, errors.New("vev: incomplete broker remote probe")
	}
	request, err := p.request(registration)
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	endpoint, err := p.resolver.Resolve(ctx, request)
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	physical, err := p.connector.Connect(ctx, endpoint)
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	defer physical.Close()
	if physical.Identity() != endpoint.Identity || physical.Policy() != endpoint.Policy {
		return ports.BrokerDaemonObservation{}, errors.New("vev: remote probe authenticated binding mismatch")
	}
	logical, err := physical.OpenStream(ctx, request)
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	defer logical.Close()
	commandID, err := randomNonzeroUint64()
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	if err := logical.SendClient(protocol.CommandRequest{RequestID: commandID, Version: protocol.Version, Slug: "remote-catalog", JSON: true}); err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	reply, err := logical.ReceiveServer()
	if err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	result, ok := reply.(protocol.CommandResult)
	if !ok || result.RequestID != commandID || !result.Valid() {
		return ports.BrokerDaemonObservation{}, errors.New("vev: invalid remote observation result")
	}
	if result.Outcome != protocol.CommandSucceeded {
		return ports.BrokerDaemonObservation{}, fmt.Errorf("vev: remote observation failed: %s", result.Text)
	}
	var catalog catalogue.RemoteCatalog
	if err := json.Unmarshal([]byte(result.Output), &catalog); err != nil {
		return ports.BrokerDaemonObservation{}, fmt.Errorf("vev: decode remote observation: %w", err)
	}
	if err := catalogue.ValidateRemoteCatalog(catalog); err != nil {
		return ports.BrokerDaemonObservation{}, err
	}
	return ports.BrokerDaemonObservation{
		Identity:        physical.Identity(),
		Incarnation:     physical.Incarnation(),
		ProtocolVersion: catalog.ProtocolVersion,
		Capabilities:    0,
		Availability:    domain.RemoteAvailabilityReachable,
		InventoryKnown:  true,
		Sessions:        catalog.Sessions,
	}, nil
}

func (p *brokerRemoteProbe) request(registration domain.RemoteRegistration) (ports.BrokerOpenStreamRequest, error) {
	if p.policy == nil {
		return ports.BrokerOpenStreamRequest{}, errors.New("vev: remote probe has no policy authority")
	}
	policy, ok := p.policy(registration.Endpoint)
	if !ok {
		return ports.BrokerOpenStreamRequest{}, errors.New("vev: remote probe endpoint is not configured")
	}
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
