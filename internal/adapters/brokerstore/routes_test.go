package brokerstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

// canonicalRoutes are the exact routes production derives for the closed remote
// policy transport vocabulary the fixtures use.
func canonicalQUICRoute(endpoint string) ports.BrokerRouteSpec {
	return ports.BrokerRouteSpec{Kind: ports.BrokerRouteSSHQUIC, Target: endpoint, Argv: []string{"vev", "_broker-mux-quic-bootstrap", "--production"}}
}

// TestInitialImportEmptyCompletesAndNeverReimports pins the explicit initial
// import seam: when the caller supplies an empty initial membership, the import
// is recorded as completed with no records, and a later reopen never falls back
// to re-importing the legacy source that is still named in Options.
func TestInitialImportEmptyCompletesAndNeverReimports(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	o := options(t)
	o.Dir = dir
	o.InitialHosts = nil
	o.InitialImportProvided = true

	store, err := Open(o)
	require.NoError(t, err)
	hosts, err := store.LoadHosts()
	require.NoError(t, err)
	require.Empty(t, hosts.Hosts, "an empty initial import seeds no membership")

	raw, err := readBounded(filepath.Join(dir, "recovery.json"))
	require.NoError(t, err)
	var record recovery
	require.NoError(t, strict(raw, &record))
	require.Equal(t, 1, record.State.Manifest.ImportVersion)
	require.True(t, record.State.Manifest.ImportCompleted, "the initial import is completed even when empty")

	// Remove-all CAS-verifies the empty authority and still commits.
	require.NoError(t, store.ReplaceHosts(hosts.Revision, nil))
	removed, err := store.LoadHosts()
	require.NoError(t, err)
	require.Empty(t, removed.Hosts)
	require.Equal(t, hosts.Revision+1, removed.Revision)
	require.NoError(t, store.Close())

	// Reopening names the legacy source again, but state.json exists, so no
	// migration runs: the empty, completed import stays authoritative.
	store, err = Open(o)
	require.NoError(t, err)
	defer store.Close()
	again, err := store.LoadHosts()
	require.NoError(t, err)
	require.Empty(t, again.Hosts, "a supplied legacy source is never re-imported after the import completed")
	require.Equal(t, removed.Revision, again.Revision)
}

// TestRecoveryRetainsRoutes proves the retained recovery record carries the
// canonical routes as durable authority: reopening from recovery alone (state.json
// removed) restores the same routed membership.
func TestRecoveryRetainsRoutes(t *testing.T) {
	o := options(t)
	store, err := Open(o)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	raw, err := readBounded(filepath.Join(o.Dir, "recovery.json"))
	require.NoError(t, err)
	var record recovery
	require.NoError(t, strict(raw, &record))
	require.Len(t, record.State.Hosts.Hosts, 1)
	require.Equal(t, canonicalQUICRoute("user@arch"), record.State.Hosts.Hosts[0].Route)

	// Drop the committed state so the recovery record is the only source.
	require.NoError(t, os.Remove(filepath.Join(o.Dir, "state.json")))
	o.InitialHosts = nil
	reopened, err := Open(o)
	require.NoError(t, err)
	defer reopened.Close()
	hosts, err := reopened.LoadHosts()
	require.NoError(t, err)
	require.Len(t, hosts.Hosts, 1)
	require.Equal(t, canonicalQUICRoute("user@arch"), hosts.Hosts[0].Route)
}

// TestCorruptRouteStateFailsClosed proves a committed state whose route is
// invalid is never silently replaced or repaired: Open reports invalid durable
// state and leaves the exact bytes it read in place for offline recovery.
func TestCorruptRouteStateFailsClosed(t *testing.T) {
	o := options(t)
	store, err := Open(o)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	statePath := filepath.Join(o.Dir, "state.json")
	raw, err := os.ReadFile(statePath)
	require.NoError(t, err)
	poisoned := bytes.Replace(raw, []byte(`"ssh-quic"`), []byte(`"bogus"`), 1)
	require.NotEqual(t, raw, poisoned, "the fixture must carry a route kind to corrupt")
	require.NoError(t, os.WriteFile(statePath, poisoned, 0600))

	reopened, err := Open(o)
	require.Error(t, err)
	require.Nil(t, reopened)
	require.ErrorIs(t, err, ErrInvalidState)
	require.ErrorIs(t, err, ports.ErrBrokerStoreInvalidState)

	after, err := os.ReadFile(statePath)
	require.NoError(t, err)
	require.Equal(t, poisoned, after, "invalid durable state is never silently replaced")
}

// TestLegacyRemoteUnixAndUnknownTransportRefused proves the migration route
// upgrade is closed over the approved remote vocabulary: a legacy policy naming
// a unix or unknown transport is refused before any state is committed, so a
// legacy local route can never become broker membership.
// TestRouteUpgradeIsAtomicAcrossReopen proves the pre-route upgrade is a durable
// migration: a state written without routes is upgraded exactly once, a fault at
// any upgrade boundary never leaves an unreadable state behind, and reopening
// completes the upgrade so the restored membership carries canonical routes.
func TestRouteUpgradeIsAtomicAcrossReopen(t *testing.T) {
	// seedStripped opens and closes a valid routed store, then rewrites its
	// committed state without routes, exactly as a pre-route process would have
	// written it. Each case gets its own directory so a completed upgrade in one
	// case never hides the migration in the next.
	seedStripped := func(t *testing.T) string {
		t.Helper()
		dir := filepath.Join(t.TempDir(), "broker")
		o := options(t)
		o.Dir = dir
		store, err := Open(o)
		require.NoError(t, err)
		require.NoError(t, store.Close())
		statePath := filepath.Join(dir, "state.json")
		raw, err := os.ReadFile(statePath)
		require.NoError(t, err)
		var legacyState state
		require.NoError(t, strict(raw, &legacyState))
		require.Len(t, legacyState.Hosts.Hosts, 1)
		legacyState.Hosts.Hosts[0].Route = ports.BrokerRouteSpec{}
		stripped, err := json.Marshal(legacyState)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(statePath, stripped, 0600))
		return dir
	}

	for _, point := range []string{"recovery.json:write", "state.json:write", "state.json:rename", "state.json:dirsync"} {
		t.Run(point, func(t *testing.T) {
			dir := seedStripped(t)
			fault := errors.New("power loss")
			failed := Options{Dir: dir, Fault: func(p string) error {
				if p == point {
					return fault
				}
				return nil
			}}
			failedStore, err := Open(failed)
			require.ErrorIs(t, err, fault)
			require.Nil(t, failedStore)

			// The upgrade is atomic: reopening without the fault always yields a
			// readable, routed store, whether or not the failed attempt renamed.
			store, err := Open(Options{Dir: dir})
			require.NoError(t, err)
			hosts, err := store.LoadHosts()
			require.NoError(t, err)
			require.Len(t, hosts.Hosts, 1)
			require.Equal(t, canonicalQUICRoute("user@arch"), hosts.Hosts[0].Route)
			require.NoError(t, hosts.Validate())
			require.NoError(t, store.Close())

			// The upgrade is idempotent: a further reopen keeps the same authority.
			store, err = Open(Options{Dir: dir})
			require.NoError(t, err)
			defer store.Close()
			again, err := store.LoadHosts()
			require.NoError(t, err)
			require.Equal(t, hosts, again)
		})
	}
}

// TestRouteChangeRequiresFreshGenerationAndRefusesOldFence proves a policy
// change that re-routes an endpoint is admitted only with a fresh registration
// generation, is persisted with the canonical route, and fences the retired
// registration: a publication bound to the old registration is refused.
func TestRouteChangeRequiresFreshGenerationAndRefusesOldFence(t *testing.T) {
	o := options(t)
	store, err := Open(o)
	require.NoError(t, err)
	defer store.Close()
	h, err := store.LoadHosts()
	require.NoError(t, err)
	require.Len(t, h.Hosts, 1)
	base := h.Hosts[0]
	require.Equal(t, canonicalQUICRoute(base.Registration.Endpoint), base.Route)

	stdio := base.Policy
	stdio.Transport = "stdio"

	// A re-route without a fresh generation is refused as a stale authority
	// change: a queued observation must never be accepted under the new route.
	changed := append([]ports.BrokerHostRecord(nil), h.Hosts...)
	changed[0].Policy = stdio
	require.ErrorIs(t, store.ReplaceHosts(h.Revision, changed), ErrStale)

	// The same change with a fresh generation is accepted and durable, with the
	// route the mutation path derives from the new transport.
	changed[0].Registration.Generation++
	changed[0].Route = ports.BrokerRouteSpec{Kind: ports.BrokerRouteSSHStdio, Target: base.Registration.Endpoint, Argv: []string{"vev", "_broker-mux-stdio", "--production"}}
	require.NoError(t, store.ReplaceHosts(h.Revision, changed))
	after, err := store.LoadHosts()
	require.NoError(t, err)
	require.Equal(t, base.Registration.Generation+1, after.Hosts[0].Registration.Generation)
	require.Equal(t, stdio, after.Hosts[0].Policy)
	require.Equal(t, changed[0].Route, after.Hosts[0].Route)

	// The retired registration is fenced: a publication naming it is refused even
	// though the endpoint is still live under a newer generation.
	retired := ports.BrokerDaemonObservation{
		Endpoint:      base.Registration.Endpoint,
		DisplayOrigin: domain.RemoteDisplayOrigin(base.Registration.Endpoint),
		Registration:  base.Registration,
		Availability:  domain.RemoteAvailabilityReachable,
	}
	require.ErrorIs(t, store.Store(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{retired}}), ErrStale)
}
