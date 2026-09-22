package broker

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/brokerstore"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

// Durable mutable membership: the same public API exercised against the real
// offline store, which owns the compare-and-swap and the snapshot validator.
// Every stage reopens both owners, so membership and its revision token must
// come from disk rather than from the previous process's memory.

// newDurableMutableRegistry opens a mutable registry over a real store. The
// registry is observation-disabled: no probe, timer, or reconciliation runs, so
// the test observes only the durable membership path.
func newDurableMutableRegistry(t *testing.T, epoch ports.BrokerEpoch, store ports.BrokerHostStore) *Registry {
	t.Helper()
	registry, err := NewRegistryWithConfig(epoch, store, nil, newManualClock(time.Unix(100, 0)), nil, RegistryConfig{
		MembershipMode:      MembershipMutable,
		ObservationDisabled: true,
	})
	require.NoError(t, err)
	registry.jitter = identityJitter
	return registry
}

func openDurableStore(t *testing.T, dir string, fault func(string) error) *brokerstore.Store {
	t.Helper()
	store, err := brokerstore.Open(brokerstore.Options{Dir: dir, Fault: fault})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// TestRegistryMutableMembershipDurableRestart proves a committed add, update,
// and remove all survive a restart through the real store, and that the
// remove/re-add fence is minted fresh by the mutation path rather than restored.
func TestRegistryMutableMembershipDurableRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	const endpoint = "user@arch:22"
	first := poolPolicy()
	changed := policyWithTrust("changed-trust")

	var (
		added    domain.RemoteRegistration
		updated  domain.RemoteRegistration
		readded  domain.RemoteRegistration
		revision uint64
	)

	t.Run("add commits durably", func(t *testing.T) {
		store := openDurableStore(t, dir, nil)
		registry := newDurableMutableRegistry(t, 1, store)
		defer registry.settle()
		require.Empty(t, registry.authority.Hosts)
		revision = registry.authority.Revision

		reg, err := registry.AddHost(context.Background(), endpoint, first)
		require.NoError(t, err)
		require.Equal(t, domain.RemoteGeneration(1), reg.Generation)
		require.False(t, reg.IsZero())
		added = reg

		revision++
		require.Equal(t, revision, registry.authority.Revision)
		authority, err := store.LoadHosts()
		require.NoError(t, err)
		require.Equal(t, registry.authority, authority)
		require.Equal(t, []ports.BrokerHostRecord{{Registration: reg, Pinned: true, Policy: first, Route: canonicalRoute(first, reg.Endpoint)}}, authority.Hosts)
		registry.settle()
	})

	t.Run("restart restores the add and admits an update", func(t *testing.T) {
		store := openDurableStore(t, dir, nil)
		registry := newDurableMutableRegistry(t, 2, store)
		defer registry.settle()
		require.Equal(t, revision, registry.authority.Revision)
		require.Equal(t, []ports.BrokerHostRecord{{Registration: added, Pinned: true, Policy: first, Route: canonicalRoute(first, added.Endpoint)}}, registry.authority.Hosts)
		host, ok := registry.Snapshot().Find(endpoint)
		require.True(t, ok)
		require.Equal(t, added, host.Registration)
		require.Equal(t, first, host.Policy, "policy is re-stamped from restored membership")
		require.Equal(t, domain.RemoteAvailabilityUnknown, host.Availability)

		reg, err := registry.UpdateHostPolicy(context.Background(), added, changed)
		require.NoError(t, err)
		require.Equal(t, added.Generation+1, reg.Generation)
		require.Equal(t, added.Incarnation, reg.Incarnation)
		updated = reg

		revision++
		require.Equal(t, revision, registry.authority.Revision)
		authority, err := store.LoadHosts()
		require.NoError(t, err)
		require.Equal(t, registry.authority, authority)
		require.Equal(t, []ports.BrokerHostRecord{{Registration: updated, Pinned: true, Policy: changed, Route: canonicalRoute(changed, updated.Endpoint)}}, authority.Hosts)
		registry.settle()
	})

	t.Run("restart restores the update and admits a remove", func(t *testing.T) {
		store := openDurableStore(t, dir, nil)
		registry := newDurableMutableRegistry(t, 3, store)
		defer registry.settle()
		require.Equal(t, revision, registry.authority.Revision)
		require.Equal(t, []ports.BrokerHostRecord{{Registration: updated, Pinned: true, Policy: changed, Route: canonicalRoute(changed, updated.Endpoint)}}, registry.authority.Hosts)

		removed, err := registry.RemoveHost(context.Background(), updated)
		require.NoError(t, err)
		require.True(t, removed)

		revision++
		require.Equal(t, revision, registry.authority.Revision)
		require.Empty(t, registry.authority.Hosts)
		authority, err := store.LoadHosts()
		require.NoError(t, err)
		require.Equal(t, registry.authority, authority)
		require.Empty(t, authority.Hosts)
		require.Empty(t, registry.Snapshot().Daemons)
		registry.settle()
	})

	t.Run("restart restores the removal and re-adds with a fresh incarnation", func(t *testing.T) {
		store := openDurableStore(t, dir, nil)
		registry := newDurableMutableRegistry(t, 4, store)
		defer registry.settle()
		require.Equal(t, revision, registry.authority.Revision)
		require.Empty(t, registry.authority.Hosts)

		reg, err := registry.AddHost(context.Background(), endpoint, changed)
		require.NoError(t, err)
		require.False(t, updated.Equal(reg), "a re-add mints a fresh identity instead of reviving the retired one")
		require.NotEqual(t, updated.Incarnation, reg.Incarnation)
		require.Equal(t, domain.RemoteGeneration(1), reg.Generation)
		readded = reg

		revision++
		require.Equal(t, revision, registry.authority.Revision)
		authority, err := store.LoadHosts()
		require.NoError(t, err)
		require.Equal(t, registry.authority, authority)
		registry.settle()
	})

	t.Run("restart restores the re-add", func(t *testing.T) {
		store := openDurableStore(t, dir, nil)
		registry := newDurableMutableRegistry(t, 5, store)
		defer registry.settle()
		require.Equal(t, revision, registry.authority.Revision)
		require.Equal(t, []ports.BrokerHostRecord{{Registration: readded, Pinned: true, Policy: changed, Route: canonicalRoute(changed, readded.Endpoint)}}, registry.authority.Hosts)
		host, ok := registry.Snapshot().Find(endpoint)
		require.True(t, ok)
		require.Equal(t, readded, host.Registration)
		require.False(t, updated.Equal(host.Registration), "the retired incarnation is never restored")
	})
}

// TestRegistryReplaceHostsRefusesUnpublishableMembershipBeforeCAS proves the
// public replacement path shares the runtime mutations' validated commit rule: a
// caller-supplied target whose derived display hint is empty after the display
// rule drops its bidi/control runes is refused by projection validation, before
// the store CAS. Nothing is persisted, nothing is published, and a restart still
// loads valid membership. The routing validator accepts the target, so this is
// exactly the membership the store alone would have committed.
func TestRegistryReplaceHostsRefusesUnpublishableMembershipBeforeCAS(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	valid := ports.BrokerHostRecord{Registration: registration(t, "user@arch:22", 1), Pinned: true, Policy: poolPolicy()}
	valid.Route = canonicalRoute(valid.Policy, valid.Registration.Endpoint)
	store := openDurableStore(t, dir, nil)
	require.NoError(t, store.ReplaceHosts(1, []ports.BrokerHostRecord{valid}))

	registry, err := NewRegistryWithConfig(1, store, nil, newManualClock(time.Unix(100, 0)), nil, RegistryConfig{ObservationDisabled: true})
	require.NoError(t, err)
	defer registry.settle()
	authority := registry.authority
	before := registry.Snapshot()
	revision := registry.revision
	persistedBefore, err := store.Load()
	require.NoError(t, err)
	require.NotZero(t, authority.Revision)
	require.Len(t, before.Daemons, 1)

	poisoned := ports.BrokerHostRecord{Registration: registration(t, "user@\u202e", 2), Pinned: true, Policy: poolPolicy()}
	poisoned.Route = canonicalRoute(poisoned.Policy, poisoned.Registration.Endpoint)
	require.NoError(t, domain.ValidateRemoteHostTarget(poisoned.Registration.Endpoint), "the routing validator accepts the target")
	require.Empty(t, hostDisplayOrigin(poisoned.Registration.Endpoint), "only the derived hint is empty after sanitizing")

	require.ErrorContains(t, registry.ReplaceHosts([]ports.BrokerHostRecord{poisoned}), "not publishable")
	require.Equal(t, authority, registry.authority, "a refused replacement never moves authority")
	require.Equal(t, before, registry.Snapshot(), "a refused replacement never publishes")
	require.Equal(t, revision, registry.revision, "a refused replacement never advances the publication series")

	// Nothing reached the durable store, even after the writer flushes: the CAS
	// was never crossed, and the advisory snapshot is still the published one.
	registry.settle()
	durableHosts, err := store.LoadHosts()
	require.NoError(t, err)
	require.Equal(t, authority, durableHosts)
	require.Equal(t, authority.Revision, durableHosts.Revision, "the refused replacement never crossed the store CAS")
	persisted, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, persistedBefore, persisted, "the refused replacement never wrote a snapshot")

	// A restart remains valid: the refused membership was never stored, so
	// reopening restores and publishes the original host.
	require.NoError(t, store.Close())
	reopened := openDurableStore(t, dir, nil)
	restarted, err := NewRegistryWithConfig(2, reopened, nil, newManualClock(time.Unix(200, 0)), nil, RegistryConfig{ObservationDisabled: true})
	require.NoError(t, err)
	defer restarted.settle()
	require.Equal(t, authority, restarted.authority)
	restored := restarted.Snapshot()
	require.NoError(t, restored.Validate())
	require.Len(t, restored.Daemons, 1)
	require.Equal(t, valid.Registration, restored.Daemons[0].Registration)
}

// TestRegistryMutableOutcomeUnknownRequiresReopen pins the indeterminate-outcome
// contract against the real store: a failure after the durable commit point is
// classified through the registry, is never acknowledged as membership, and
// leaves the store poisoned until it is reopened and authority is reloaded.
// Reopening reveals that the mutation did commit, which is exactly why the
// registry may not report it either way.
func TestRegistryMutableOutcomeUnknownRequiresReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	fault := errors.New("power loss")
	// The fault is armed only after Open so the store can be created and
	// seeded normally; state.json:rename fires after the rename, so the commit
	// point was already crossed when the failure is reported.
	var armed atomic.Bool
	store := openDurableStore(t, dir, func(point string) error {
		if armed.Load() && point == "state.json:rename" {
			return fault
		}
		return nil
	})
	registry := newDurableMutableRegistry(t, 1, store)
	defer registry.settle()
	before := registry.Snapshot()
	authority := registry.authority
	armed.Store(true)

	_, err := registry.AddHost(context.Background(), "user@arch:22", poolPolicy())
	require.Error(t, err)
	var unknown ports.BrokerStoreOutcomeUnknownError
	require.ErrorAs(t, err, &unknown, "a post-commit failure is classified as an unknown outcome")
	require.ErrorIs(t, err, fault, "the underlying store failure stays inspectable")
	require.Equal(t, authority, registry.authority, "an unknown outcome never becomes acknowledged membership")
	require.Equal(t, before, registry.Snapshot())

	// The store handle is poisoned: every later operation refuses instead of
	// reading or writing a state it cannot vouch for.
	_, err = store.LoadHosts()
	require.ErrorContains(t, err, "reopen required")
	require.ErrorAs(t, err, &unknown)
	require.Empty(t, registry.Snapshot().Daemons)

	// Reopening reloads authority from disk: the rename already published the
	// new membership, so the only safe recovery is to reopen rather than retry.
	require.NoError(t, store.Close())
	armed.Store(false)
	reopened := openDurableStore(t, dir, nil)
	hosts, err := reopened.LoadHosts()
	require.NoError(t, err)
	require.Equal(t, authority.Revision+1, hosts.Revision)
	require.Len(t, hosts.Hosts, 1)
	require.Equal(t, "user@arch:22", hosts.Hosts[0].Registration.Endpoint)
	require.Equal(t, poolPolicy(), hosts.Hosts[0].Policy)

	recovered := newDurableMutableRegistry(t, 2, reopened)
	defer recovered.settle()
	require.Len(t, recovered.authority.Hosts, 1)
	require.Equal(t, hosts.Hosts[0].Registration, recovered.authority.Hosts[0].Registration)
}
