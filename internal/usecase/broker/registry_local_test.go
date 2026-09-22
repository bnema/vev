package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// scriptedLocalProbe scripts one local observation per received call: the test
// owns the completion, so the local probe never depends on wall time.
type scriptedLocalProbe struct {
	calls    chan localProbeCall
	canceled chan struct{}
}

type localProbeCall struct {
	result chan localProbeAnswer
}

type localProbeAnswer struct {
	snapshot ports.BrokerDaemonObservation
	err      error
}

func newScriptedLocalProbe(capacity int) *scriptedLocalProbe {
	return &scriptedLocalProbe{calls: make(chan localProbeCall, capacity), canceled: make(chan struct{}, 1)}
}

func (p *scriptedLocalProbe) ProbeLocal(ctx context.Context) (ports.BrokerDaemonObservation, error) {
	call := localProbeCall{result: make(chan localProbeAnswer, 1)}
	select {
	case p.calls <- call:
	case <-ctx.Done():
		p.noteCanceled()
		return ports.BrokerDaemonObservation{}, ctx.Err()
	}
	select {
	case answer := <-call.result:
		return answer.snapshot, answer.err
	case <-ctx.Done():
		p.noteCanceled()
		return ports.BrokerDaemonObservation{}, ctx.Err()
	}
}

func (p *scriptedLocalProbe) noteCanceled() {
	select {
	case p.canceled <- struct{}{}:
	default:
	}
}

func receiveLocalCall(t *testing.T, probe *scriptedLocalProbe) localProbeCall {
	t.Helper()
	select {
	case call := <-probe.calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for local probe")
		return localProbeCall{}
	}
}

// newLocalTestRegistry builds a registry that observes the broker's own machine
// daemon through localProbe, beside an optional remote probe.
func newLocalTestRegistry(t *testing.T, store ports.BrokerHostStore, remoteProbe ports.BrokerHostProbe, localProbe LocalProbe, clock *manualClock) *Registry {
	t.Helper()
	registry, err := NewRegistryWithConfig(1, store, remoteProbe, clock, nil, RegistryConfig{
		Local: &LocalObservation{DisplayOrigin: "local", Policy: poolPolicy(), Probe: localProbe},
	})
	require.NoError(t, err)
	registry.jitter = identityJitter
	return registry
}

// waitLocal waits for the published local entry (always index zero) to satisfy
// predicate and returns it.
func waitLocal(t *testing.T, r *Registry, predicate func(ports.BrokerDaemonObservation) bool) ports.BrokerDaemonObservation {
	t.Helper()
	snapshot := waitSnapshot(t, r, func(snapshot ports.BrokerSnapshot) bool {
		return len(snapshot.Daemons) > 0 && snapshot.Daemons[0].Local && predicate(snapshot.Daemons[0])
	})
	return snapshot.Daemons[0]
}

// TestRegistryPublishesObservedLocalDaemonFirst proves the local observation is
// published at index zero ahead of every remote, carries the configured
// authority plus the observed identity/incarnation/version, and is never
// durable: the store persists the remote observations only.
func TestRegistryPublishesObservedLocalDaemonFirst(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	store := newTestStore()
	local := newScriptedLocalProbe(1)
	registry := newLocalTestRegistry(t, store, newTestProbe(1), local, clock)
	reg := registration(t, "user@host:22", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	call := receiveLocalCall(t, local)
	call.result <- localProbeAnswer{snapshot: ports.BrokerDaemonObservation{
		Identity:        "local-daemon",
		Incarnation:     ports.BrokerDaemonIncarnation{9},
		ProtocolVersion: protocol.Version,
		Availability:    domain.RemoteAvailabilityReachable,
	}}

	host := waitLocal(t, registry, func(o ports.BrokerDaemonObservation) bool {
		return !o.Checking && o.Availability == domain.RemoteAvailabilityReachable
	})
	require.True(t, host.Local)
	require.Zero(t, host.Endpoint)
	require.Equal(t, domain.RemoteRegistration{}, host.Registration)
	require.Equal(t, "local", host.DisplayOrigin)
	require.Equal(t, poolPolicy(), host.Policy)
	require.Zero(t, host.Rank)
	require.Equal(t, ports.BrokerDaemonIdentity("local-daemon"), host.Identity)
	require.Equal(t, ports.BrokerDaemonIncarnation{9}, host.Incarnation)
	require.Equal(t, protocol.Version, host.ProtocolVersion)

	snapshot := registry.Snapshot()
	require.True(t, snapshot.Daemons[0].Local)
	require.Equal(t, reg.Endpoint, snapshot.Daemons[1].Endpoint)

	// Never durable: the store receives remote observations only, and the local
	// entry is dropped before persistence.
	stored := waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		return len(snapshot.Daemons) == 1 && !snapshot.Daemons[0].Local
	})
	require.Equal(t, reg.Endpoint, stored.Daemons[0].Endpoint)
}

// TestRegistryLocalObservationNeverInventsState proves an unreachable local
// daemon is published with the explicit unknown availability and zero observed
// identity, and a hostile probe result cannot supply local authority,
// endpoint, registration, policy, or display origin.
func TestRegistryLocalObservationNeverInventsState(t *testing.T) {
	t.Run("failure keeps zero identity and typed availability", func(t *testing.T) {
		clock := newManualClock(time.Unix(100, 0))
		local := newScriptedLocalProbe(1)
		registry := newLocalTestRegistry(t, newTestStore(), newTestProbe(1), local, clock)
		startRegistry(t, registry)

		call := receiveLocalCall(t, local)
		call.result <- localProbeAnswer{err: errors.New("local carriage refused")}

		host := waitLocal(t, registry, func(o ports.BrokerDaemonObservation) bool {
			return !o.Checking && o.Availability == domain.RemoteAvailabilityUnreachable
		})
		require.True(t, host.Local)
		require.Zero(t, host.Endpoint)
		require.Equal(t, ports.BrokerDaemonIdentity(""), host.Identity)
		require.True(t, host.Incarnation.IsZero())
		require.Zero(t, host.ProtocolVersion)
		require.Equal(t, domain.RemoteFailureTransport, host.LastFailure.Kind)
		require.Equal(t, 1, len(registry.Snapshot().Daemons))
	})

	t.Run("hostile probe cannot supply local authority", func(t *testing.T) {
		clock := newManualClock(time.Unix(100, 0))
		local := newScriptedLocalProbe(1)
		registry := newLocalTestRegistry(t, newTestStore(), newTestProbe(1), local, clock)
		startRegistry(t, registry)

		hostilePolicy := poolPolicy()
		hostilePolicy.Trust = "attacker"
		call := receiveLocalCall(t, local)
		call.result <- localProbeAnswer{snapshot: ports.BrokerDaemonObservation{
			Local:           false,
			Endpoint:        "attacker@evil",
			DisplayOrigin:   "evil",
			Rank:            42,
			Registration:    registration(t, "attacker@evil", 9),
			Policy:          hostilePolicy,
			Identity:        "local-daemon",
			Incarnation:     ports.BrokerDaemonIncarnation{9},
			ProtocolVersion: protocol.Version,
			Availability:    domain.RemoteAvailabilityReachable,
		}}

		host := waitLocal(t, registry, func(o ports.BrokerDaemonObservation) bool {
			return !o.Checking && o.Availability == domain.RemoteAvailabilityReachable
		})
		require.True(t, host.Local)
		require.Zero(t, host.Endpoint)
		require.Equal(t, domain.RemoteRegistration{}, host.Registration)
		require.Equal(t, "local", host.DisplayOrigin)
		require.Equal(t, poolPolicy(), host.Policy)
		require.NotEqual(t, hostilePolicy, host.Policy)
		require.Zero(t, host.Rank)
	})
}

// TestRegistryLocalObservationValidatesProbeResults proves an invalid observed
// projection is classified as an invalid response, never published as an
// observation the wire refuses: a partial identity (identity without a version)
// is rejected without inventing the missing half.
func TestRegistryLocalObservationValidatesProbeResults(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	local := newScriptedLocalProbe(1)
	registry := newLocalTestRegistry(t, newTestStore(), newTestProbe(1), local, clock)
	startRegistry(t, registry)

	call := receiveLocalCall(t, local)
	call.result <- localProbeAnswer{snapshot: ports.BrokerDaemonObservation{
		Identity:     "local-daemon", // no protocol version: partial identity
		Incarnation:  ports.BrokerDaemonIncarnation{9},
		Availability: domain.RemoteAvailabilityReachable,
	}}

	host := waitLocal(t, registry, func(o ports.BrokerDaemonObservation) bool {
		return !o.Checking && o.Availability == domain.RemoteAvailabilityInvalidResponse
	})
	require.True(t, host.Local)
	require.Equal(t, domain.RemoteFailureInvalidResponse, host.LastFailure.Kind)
}

// TestRegistryLocalObservationConstruction pins the composition refusals: a
// read-only registry owns no local producer, a local producer requires a probe,
// and its policy and display origin are validated before anything is published.
func TestRegistryLocalObservationConstruction(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	valid := &LocalObservation{DisplayOrigin: "local", Policy: poolPolicy(), Probe: newScriptedLocalProbe(1)}

	invalidPolicy := poolPolicy()
	invalidPolicy.ProtocolVersion = 0

	tests := []struct {
		name        string
		cfg         RegistryConfig
		remoteProbe ports.BrokerHostProbe
	}{
		{
			name:        "observation-disabled registry cannot observe locally",
			cfg:         RegistryConfig{ObservationDisabled: true, Local: valid},
			remoteProbe: newTestProbe(1),
		},
		{
			name:        "local producer requires a probe",
			cfg:         RegistryConfig{Local: &LocalObservation{DisplayOrigin: "local", Policy: poolPolicy()}},
			remoteProbe: newTestProbe(1),
		},
		{
			name:        "local authority policy is validated",
			cfg:         RegistryConfig{Local: &LocalObservation{DisplayOrigin: "local", Policy: invalidPolicy, Probe: newScriptedLocalProbe(1)}},
			remoteProbe: newTestProbe(1),
		},
		{
			name:        "local display origin is required",
			cfg:         RegistryConfig{Local: &LocalObservation{DisplayOrigin: "", Policy: poolPolicy(), Probe: newScriptedLocalProbe(1)}},
			remoteProbe: newTestProbe(1),
		},
		{
			name:        "local display origin must be normalized",
			cfg:         RegistryConfig{Local: &LocalObservation{DisplayOrigin: "ev\u202eil", Policy: poolPolicy(), Probe: newScriptedLocalProbe(1)}},
			remoteProbe: newTestProbe(1),
		},
		{
			name:        "no producer at all is still refused",
			cfg:         RegistryConfig{},
			remoteProbe: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRegistryWithConfig(1, newTestStore(), tt.remoteProbe, clock, nil, tt.cfg)
			require.Error(t, err)
		})
	}
}

// TestRegistryLocalOnlyRequiresNoRemoteProbe proves a composition that observes
// only the broker's own machine daemon needs no remote probe, and that such a
// registry refuses remote membership rather than silently never probing it.
func TestRegistryLocalOnlyRequiresNoRemoteProbe(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	local := newScriptedLocalProbe(1)
	registry, err := NewRegistryWithConfig(1, newTestStore(), nil, clock, nil, RegistryConfig{
		Local: &LocalObservation{DisplayOrigin: "local", Policy: poolPolicy(), Probe: local},
	})
	require.NoError(t, err)
	registry.jitter = identityJitter

	reg := registration(t, "user@host:22", 1)
	require.Error(t, registry.setHosts(hostRecords(reg)))
	require.Error(t, registry.ReplaceHosts(hostRecords(reg)))

	startRegistry(t, registry)
	call := receiveLocalCall(t, local)
	call.result <- localProbeAnswer{snapshot: ports.BrokerDaemonObservation{
		Identity:        "local-daemon",
		Incarnation:     ports.BrokerDaemonIncarnation{9},
		ProtocolVersion: protocol.Version,
		Availability:    domain.RemoteAvailabilityReachable,
	}}
	host := waitLocal(t, registry, func(o ports.BrokerDaemonObservation) bool {
		return !o.Checking && o.Availability == domain.RemoteAvailabilityReachable
	})
	require.True(t, host.Local)
	require.Len(t, registry.Snapshot().Daemons, 1)
}

// localEntryCount counts the published local entries. A valid snapshot carries
// at most one, so this is also the assertion that no second local entry is
// ever invented.
func localEntryCount(snapshot ports.BrokerSnapshot) int {
	count := 0
	for _, daemon := range snapshot.Daemons {
		if daemon.Local {
			count++
		}
	}
	return count
}

// remoteEndpoints lists the published remote endpoints in publication order,
// excluding the local entry.
func remoteEndpoints(snapshot ports.BrokerSnapshot) []string {
	var endpoints []string
	for _, daemon := range snapshot.Daemons {
		if daemon.Local {
			continue
		}
		endpoints = append(endpoints, daemon.Endpoint)
	}
	return endpoints
}

// TestRegistryLocalEntryStaysFirstAcrossMembershipChanges proves the published
// snapshot always carries exactly one local entry at index zero while the
// remote membership churns through add, reorder, removal, re-add, and clear,
// with the remotes in registration order behind it. The local entry stays first
// because the registry owns it separately from durable membership: it is never
// reordered by a membership change and never replaced by a remote.
func TestRegistryLocalEntryStaysFirstAcrossMembershipChanges(t *testing.T) {
	first := registration(t, "user@first:22", 1)
	second := registration(t, "user@second:22", 2)
	third := registration(t, "user@third:22", 3)

	steps := []struct {
		name    string
		apply   func(t *testing.T, r *Registry)
		remotes []string
	}{
		{
			name:  "no remotes publishes the local entry alone",
			apply: func(*testing.T, *Registry) {},
		},
		{
			name:    "adding remotes keeps the local entry first",
			apply:   func(t *testing.T, r *Registry) { require.NoError(t, r.setHosts(hostRecords(first, second))) },
			remotes: []string{first.Endpoint, second.Endpoint},
		},
		{
			name:    "reordering remotes keeps the local entry first",
			apply:   func(t *testing.T, r *Registry) { require.NoError(t, r.setHosts(hostRecords(second, first))) },
			remotes: []string{second.Endpoint, first.Endpoint},
		},
		{
			name:    "durable replacement keeps the local entry first",
			apply:   func(t *testing.T, r *Registry) { require.NoError(t, r.ReplaceHosts(hostRecords(first))) },
			remotes: []string{first.Endpoint},
		},
		{
			name: "removing then re-adding a remote keeps the local entry first",
			apply: func(t *testing.T, r *Registry) {
				require.NoError(t, r.ReplaceHosts(hostRecords()))
				require.NoError(t, r.ReplaceHosts(hostRecords(first, third)))
			},
			remotes: []string{first.Endpoint, third.Endpoint},
		},
		{
			name:    "clearing durable membership leaves the local entry alone",
			apply:   func(t *testing.T, r *Registry) { require.NoError(t, r.ReplaceHosts(hostRecords())) },
			remotes: nil,
		},
	}
	registry := newLocalTestRegistry(t, newTestStore(), newTestProbe(1), newScriptedLocalProbe(1), newManualClock(time.Unix(100, 0)))
	for _, tt := range steps {
		t.Run(tt.name, func(t *testing.T) {
			tt.apply(t, registry)
			snapshot := registry.Snapshot()
			require.NotEmpty(t, snapshot.Daemons)
			require.True(t, snapshot.Daemons[0].Local, "the local entry is always at index zero")
			require.Equal(t, 1, localEntryCount(snapshot), "exactly one local entry is published")
			require.Equal(t, tt.remotes, remoteEndpoints(snapshot), "remotes keep registration order behind the local entry")
		})
	}
}

// TestRegistryLocalOnlyRefusesRestoredRemoteMembership proves a local-only
// registry (a configured local observation and no remote probe) refuses restored
// durable remote membership at the projectMembership boundary, so it never owns
// a remote host it could never probe. The setHosts/ReplaceHosts refusals are
// covered beside this test.
func TestRegistryLocalOnlyRefusesRestoredRemoteMembership(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	local := func() *LocalObservation {
		return &LocalObservation{DisplayOrigin: "local", Policy: poolPolicy(), Probe: newScriptedLocalProbe(1)}
	}
	reg := registration(t, "user@host:22", 1)

	t.Run("construction refuses a store with restored remote membership", func(t *testing.T) {
		store := newTestStore()
		store.hosts = ports.BrokerHosts{Revision: 1, Hosts: hostRecords(reg)}
		_, err := NewRegistryWithConfig(1, store, nil, clock, nil, RegistryConfig{Local: local()})
		require.Error(t, err)
		require.Contains(t, err.Error(), "remote hosts require a host probe")
	})

	t.Run("projectMembership refuses restored remote membership directly", func(t *testing.T) {
		registry, err := NewRegistryWithConfig(1, newTestStore(), nil, clock, nil, RegistryConfig{Local: local()})
		require.NoError(t, err)
		// Empty restored membership is accepted: the local-only registry owns no
		// remote host and needs no remote probe.
		require.NoError(t, registry.projectMembership(ports.BrokerHosts{Revision: 1}))
		err = registry.projectMembership(ports.BrokerHosts{Revision: 1, Hosts: hostRecords(reg)})
		require.Error(t, err)
		require.Contains(t, err.Error(), "remote hosts require a host probe")
		require.Empty(t, remoteEndpoints(registry.Snapshot()), "the refused membership is never published")
	})
}

// TestPoolLocalStreamsShareOnePhysicalEntry proves two local open-stream
// requests under one broker-owned local binding resolve to the same pooled
// (identity, policy) physical entry: the pool dials one daemonmux carriage,
// both logical streams ride it, and closing one never closes its sibling. It
// lives here because pool_test.go is outside this change's write scope; the
// fake connector/resolver harness it reuses is defined there.
func TestPoolLocalStreamsShareOnePhysicalEntry(t *testing.T) {
	var connects atomic.Int32
	var physical *fakePhysical
	pool, _ := setupPool(t, func(_ context.Context, endpoint ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
		connects.Add(1)
		physical = &fakePhysical{endpoint: endpoint, done: make(chan struct{})}
		return physical, nil
	})
	id, err := pool.RegisterClient()
	require.NoError(t, err)
	first, err := pool.OpenStream(context.Background(), poolRequest(id, 1))
	require.NoError(t, err)
	second, err := pool.OpenStream(context.Background(), poolRequest(id, 2))
	require.NoError(t, err)
	require.EqualValues(t, 1, connects.Load(), "both local streams share one physical carriage")
	require.NoError(t, first.Close())
	select {
	case <-second.Done():
		t.Fatal("closing one local stream closed its sibling")
	default:
	}
	require.NoError(t, second.Close())
	require.NoError(t, physical.Close())
}

// TestRegistryMutableAddHostLocalOnlyNoRemoteProbe proves a mutable registry that
// observes only the broker's own machine daemon (a configured local observation
// and no remote probe) refuses a remote addition outright: the refusal is
// typed, it never reaches the durable store CAS, authority and projection stay
// exactly as loaded, and the registry keeps serving its local entry instead of
// panicking on the absent remote probe. A mutable registry is the interesting
// case because the immutable default refuses before the membership mode is even
// consulted.
func TestRegistryMutableAddHostLocalOnlyNoRemoteProbe(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	store := newMembershipStore()
	local := newScriptedLocalProbe(1)
	registry, err := NewRegistryWithConfig(1, store, nil, clock, nil, RegistryConfig{
		MembershipMode:       MembershipMutable,
		IncarnationGenerator: (&incarnationSequencer{}).generate,
		Local:                &LocalObservation{DisplayOrigin: "local", Policy: poolPolicy(), Probe: local},
	})
	require.NoError(t, err)
	registry.jitter = identityJitter

	authority := registry.authority
	before := registry.Snapshot()
	require.NotPanics(t, func() {
		_, err = registry.AddHost(context.Background(), "user@host:22", poolPolicy())
	}, "an absent remote probe must be refused, never dereferenced")
	require.Error(t, err)
	require.Contains(t, err.Error(), "remote hosts require a host probe")
	require.Equal(t, 0, store.casCalls(), "the refusal happens before the store CAS")
	require.Equal(t, authority, registry.authority, "a refused add never moves authority")
	require.Equal(t, before, registry.Snapshot(), "a refused add never publishes")

	// The local entry stays observable: the refusal never leaves the registry
	// unable to publish what it still owns.
	startRegistry(t, registry)
	call := receiveLocalCall(t, local)
	call.result <- localProbeAnswer{snapshot: ports.BrokerDaemonObservation{
		Identity:        "local-daemon",
		Incarnation:     ports.BrokerDaemonIncarnation{9},
		ProtocolVersion: protocol.Version,
		Availability:    domain.RemoteAvailabilityReachable,
	}}
	host := waitLocal(t, registry, func(o ports.BrokerDaemonObservation) bool {
		return !o.Checking && o.Availability == domain.RemoteAvailabilityReachable
	})
	require.True(t, host.Local)
	require.Len(t, registry.Snapshot().Daemons, 1)
}
