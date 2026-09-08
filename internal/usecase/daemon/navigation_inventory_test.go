package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// addInventorySession inserts a minimal live session without PTY allocation.
// Snapshot paths only read registry fields, so no tab or PTY is needed.
func addInventorySession(d *Daemon, name string, lifecycle domain.SessionLifecycleID, ephemeral bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	sess := &session{sessionCore: sessionCore{id: domain.SessionID("sess-" + name), name: name, ephemeral: ephemeral, incarnation: lifecycle}}
	sess.mruAt.Store(1)
	if d.sessions == nil {
		d.sessions = make(map[domain.SessionID]*session)
	}
	d.sessions[sess.id] = sess
}

func inventorySnapshot(t *testing.T, d *Daemon, requestID uint64) protocol.NavigationInventoryResponse {
	t.Helper()
	response := d.answerNavigationInventory(protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: requestID, Operation: protocol.NavigationInventorySnapshot})
	require.Equal(t, protocol.NavigationInventoryOK, response.Status)
	require.NoError(t, protocol.ValidateNavigationInventoryResponse(response))
	return response
}

func TestNavigationInventorySnapshotHasNoSideEffects(t *testing.T) {
	now := time.Unix(1_000, 0)
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
	current := addControlSession(d, "current", "tab-1", "pane-1")
	current.ephemeral = false
	current.incarnation = domain.SessionLifecycleID{11}
	other := addControlSession(d, "other", "tab-2", "pane-2")
	other.ephemeral = false
	other.incarnation = domain.SessionLifecycleID{12}
	seedRemoteDirectory(t, d, reachableDirectoryHost("user@arch", now, catalogue.RemoteCatalogSession{
		LifecycleID: domain.SessionLifecycleID{21}, Name: "shared", State: catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-remote", Index: 0, Name: "shell"}},
		ActiveTabID: "tab-remote",
	}))

	before := len(d.sessions)
	response := inventorySnapshot(t, d, 1)
	require.Len(t, response.Groups, 2)
	require.Equal(t, protocol.NavigationInventoryLocalSourceKey, response.Groups[0].SourceKey)
	require.Equal(t, "user@arch", response.Groups[1].SourceKey)
	require.Len(t, d.sessions, before, "snapshot must not create sessions")
	require.NotNil(t, current)
}

func TestNavigationInventoryLocalBeyondRemoteBound(t *testing.T) {
	now := time.Unix(1_000, 0)
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
	for i := range 257 {
		name := string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('0'+i/676))
		addInventorySession(d, name, domain.SessionLifecycleID{byte(i), byte(i >> 8)}, false)
	}
	remoteSessions := make([]catalogue.RemoteCatalogSession, 0, 257)
	for i := range 257 {
		remoteSessions = append(remoteSessions, catalogue.RemoteCatalogSession{
			LifecycleID: domain.SessionLifecycleID{byte(i + 1)}, Name: "remote-session",
			State: catalogue.RemoteCatalogSessionUp,
			Tabs:  []catalogue.RemoteCatalogTab{{ID: "tab", Index: 0, Name: "shell"}},
		})
	}
	seedRemoteDirectory(t, d, reachableDirectoryHost("user@arch", now, remoteSessions...))

	response := inventorySnapshot(t, d, 2)
	require.Equal(t, protocol.NavigationInventoryOK, response.Status)
	local := response.Groups[0]
	require.Equal(t, protocol.NavigationInventoryLocalSourceKey, local.SourceKey)
	require.Equal(t, protocol.NavigationInventorySourceOK, local.Status)
	require.Len(t, local.Entries, 257, "257 local sessions are not rejected by the remote 256 bound")
	remote := response.Groups[1]
	require.Equal(t, protocol.NavigationInventorySourceTooLarge, remote.Status)
	require.Empty(t, remote.Entries)
}

func TestNavigationInventoryResolveLocalAndStale(t *testing.T) {
	now := time.Unix(1_000, 0)
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
	addInventorySession(d, "work", domain.SessionLifecycleID{5}, false)

	response := inventorySnapshot(t, d, 3)
	var entryKey string
	for _, entry := range response.Groups[0].Entries {
		if entry.Name == "work" {
			entryKey = entry.EntryKey
		}
	}
	require.NotEmpty(t, entryKey)

	resolved := d.answerNavigationInventory(protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 4, Operation: protocol.NavigationInventoryResolve, SourceKey: protocol.NavigationInventoryLocalSourceKey, EntryKey: entryKey})
	require.Equal(t, protocol.NavigationInventoryOK, resolved.Status)
	require.NotNil(t, resolved.Resolved)
	require.Equal(t, "work", resolved.Resolved.Session)
	require.NotNil(t, resolved.Resolved.ExactTarget)
	require.Equal(t, domain.SessionLifecycleID{5}, resolved.Resolved.ExactTarget.LifecycleID)

	unknown := d.answerNavigationInventory(protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 5, Operation: protocol.NavigationInventoryResolve, SourceKey: protocol.NavigationInventoryLocalSourceKey, EntryKey: "deadbeef/missing"})
	require.NotEqual(t, protocol.NavigationInventoryOK, unknown.Status)
	require.Nil(t, unknown.Resolved, "stale keys never resolve to a same-name replacement")
}

func TestNavigationInventoryResolveRegistrationFencesReRegistration(t *testing.T) {
	now := time.Unix(1_000, 0)
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
	lifecycle := domain.SessionLifecycleID{21}
	registration, err := domain.NewRemoteRegistration("user@arch", [16]byte{9})
	require.NoError(t, err)
	host := reachableDirectoryHost("user@arch", now, catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "shared", State: catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-remote", Index: 0, Name: "shell"}},
		ActiveTabID: "tab-remote",
	})
	host.Registration = registration
	seedRemoteDirectory(t, d, host)

	response := inventorySnapshot(t, d, 6)
	var entryKey string
	for _, group := range response.Groups {
		if group.SourceKey != "user@arch" {
			continue
		}
		for _, entry := range group.Entries {
			if entry.Name == "shared" {
				entryKey = entry.EntryKey
			}
		}
	}
	require.NotEmpty(t, entryKey)

	good := d.answerNavigationInventory(protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 7, Operation: protocol.NavigationInventoryResolve, SourceKey: "user@arch", EntryKey: entryKey, Registration: registration})
	require.Equal(t, protocol.NavigationInventoryOK, good.Status)
	require.NotNil(t, good.Resolved)
	require.Equal(t, "user@arch", good.Resolved.Endpoint)
	require.Equal(t, protocol.EnvironmentPolicyDaemonOwned, good.Resolved.EnvironmentPolicy)

	// Removal before resolve rejects.
	seedRemoteDirectory(t, d)
	gone := d.answerNavigationInventory(protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 8, Operation: protocol.NavigationInventoryResolve, SourceKey: "user@arch", EntryKey: entryKey, Registration: registration})
	require.NotEqual(t, protocol.NavigationInventoryOK, gone.Status)
	require.Nil(t, gone.Resolved)

	// Re-adding the endpoint mints a new incarnation; the old registration rejects.
	fresh, err := domain.NewRemoteRegistration("user@arch", [16]byte{10})
	require.NoError(t, err)
	readded := reachableDirectoryHost("user@arch", now, catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "shared", State: catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-remote", Index: 0, Name: "shell"}},
		ActiveTabID: "tab-remote",
	})
	readded.Registration = fresh
	seedRemoteDirectory(t, d, readded)
	stale := d.answerNavigationInventory(protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 9, Operation: protocol.NavigationInventoryResolve, SourceKey: "user@arch", EntryKey: entryKey, Registration: registration})
	require.NotEqual(t, protocol.NavigationInventoryOK, stale.Status)
	require.Nil(t, stale.Resolved)
}

func TestNavigationInventoryStaleObservationStaysAttemptable(t *testing.T) {
	now := time.Unix(1_000, 0)
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
	lifecycle := domain.SessionLifecycleID{21}
	registration, err := domain.NewRemoteRegistration("user@arch", [16]byte{9})
	require.NoError(t, err)
	host := reachableDirectoryHost("user@arch", now.Add(-time.Hour), catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "cached", State: catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "shell"}},
		ActiveTabID: "tab-1",
	})
	host.Registration = registration
	host.Availability = domain.RemoteAvailabilityUnreachable
	host.InventoryKnown = true
	seedRemoteDirectory(t, d, host)

	response := inventorySnapshot(t, d, 10)
	var entryKey, reason string
	for _, group := range response.Groups {
		if group.SourceKey != "user@arch" {
			continue
		}
		for _, entry := range group.Entries {
			if entry.Name == "cached" {
				entryKey, reason = entry.EntryKey, entry.Reason
			}
		}
	}
	require.NotEmpty(t, entryKey, "stale observations keep their rows")
	require.NotEmpty(t, reason)

	resolved := d.answerNavigationInventory(protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 11, Operation: protocol.NavigationInventoryResolve, SourceKey: "user@arch", EntryKey: entryKey, Registration: registration})
	require.Equal(t, protocol.NavigationInventoryOK, resolved.Status, "known valid cached targets stay attemptable")
}

func TestNavigationInventoryStoppedBecomingLiveResolves(t *testing.T) {
	now := time.Unix(1_000, 0)
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
	lifecycle := domain.SessionLifecycleID{7}
	d.mu.Lock()
	d.inactive["work"] = inactiveSession{name: "work", cwd: "/tmp/work", createdAt: 1, incarnation: lifecycle, state: protocol.SessionDown}
	d.mu.Unlock()

	response := inventorySnapshot(t, d, 12)
	var entryKey string
	for _, entry := range response.Groups[0].Entries {
		if entry.Name == "work" {
			entryKey = entry.EntryKey
		}
	}
	require.NotEmpty(t, entryKey)

	// The stopped target becomes live with the same lifecycle and selector.
	addInventorySession(d, "work", lifecycle, false)
	d.mu.Lock()
	delete(d.inactive, "work")
	d.mu.Unlock()

	resolved := d.answerNavigationInventory(protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 13, Operation: protocol.NavigationInventoryResolve, SourceKey: protocol.NavigationInventoryLocalSourceKey, EntryKey: entryKey})
	require.Equal(t, protocol.NavigationInventoryOK, resolved.Status)
}

func TestNavigationInventoryVersionMismatchAndMalformed(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	mismatch := d.answerNavigationInventory(protocol.NavigationInventoryRequest{Version: protocol.Version + 1, RequestID: 1, Operation: protocol.NavigationInventorySnapshot})
	require.Equal(t, protocol.NavigationInventoryVersionMismatch, mismatch.Status)
	require.Nil(t, mismatch.Resolved)
	require.Empty(t, mismatch.Groups)

	malformed := d.answerNavigationInventory(protocol.NavigationInventoryRequest{Operation: protocol.NavigationInventorySnapshot})
	require.Equal(t, protocol.NavigationInventoryInvalid, malformed.Status)
	require.Nil(t, malformed.Resolved)
}

// TestNeverVisitedLocalSessionRequiresRelay is the P0.2 known-red product
// case: a palette served by daemon A cannot list daemon L's sessions, so a
// local session the client never visited is unreachable from remote. It
// currently asserts the deficiency (absence); the P4 relay must invert it
// to presence plus successful local commit. No pass is claimed here.
func TestNeverVisitedLocalSessionRequiresRelay(t *testing.T) {
	local := newTestDaemon(t, nil, stubClock{})
	addInventorySession(local, "local-unvisited", domain.SessionLifecycleID{33}, false)

	remote := newTestDaemon(t, nil, stubClock{})
	serving := addControlSession(remote, "remote-one", "tab-1", "pane-1")
	serving.ephemeral = false
	serving.incarnation = domain.SessionLifecycleID{44}

	results := remote.paletteResults(serving, nil, protocol.RecentRouteSnapshot{})
	for _, result := range results {
		if name, ok := result.SessionName(); ok {
			require.NotEqual(t, "local-unvisited", name, "cross-daemon leak would be a privacy violation, not the fix")
		}
		if _, ok := result.RemoteSessionTarget(); ok {
			continue
		}
	}
	localGroups := local.answerNavigationInventory(protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 1, Operation: protocol.NavigationInventorySnapshot})
	require.Equal(t, protocol.NavigationInventoryOK, localGroups.Status)
	found := false
	for _, entry := range localGroups.Groups[0].Entries {
		if entry.Name == "local-unvisited" {
			found = true
		}
	}
	require.True(t, found, "the source control daemon exports the session; only the relay to the serving daemon is missing")
}
