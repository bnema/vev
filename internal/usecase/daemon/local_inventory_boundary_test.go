package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/usecase/picker"
)

// failPTYFactory fails the test if a local projection ever opens a PTY.
type failPTYFactory struct{ t *testing.T }

func (f failPTYFactory) Open(context.Context, string, []string, []string, string, domain.Geometry) (ports.PTY, error) {
	f.t.Fatal("local projection must not open a PTY")
	return nil, nil
}

func localNavigationRequest(entryKey string) protocol.NavigationInventoryRequest {
	return protocol.NavigationInventoryRequest{
		Version: protocol.Version, RequestID: 1, Operation: protocol.NavigationInventoryResolve,
		SourceKey: protocol.NavigationInventoryLocalSourceKey, EntryKey: entryKey,
	}
}

func seedLocalNamedSession(d *Daemon, name string, incarnation domain.SessionLifecycleID) *session {
	sess := addControlSession(d, name, "tab-"+name, "pane-"+name)
	sess.ephemeral = false
	sess.incarnation = incarnation
	return sess
}

// TestRemoteCatalogLocalCaptureIgnoresRemoteDirectory proves the catalogue
// export is a local-only projection: a populated and a failing remote
// directory both leave the exported document byte-identical to a daemon with
// no remote directory, and neither is ever read.
func TestRemoteCatalogLocalCaptureIgnoresRemoteDirectory(t *testing.T) {
	now := time.Unix(1_000, 0)
	populateLocal := func(d *Daemon) {
		seedLocalNamedSession(d, "named", domain.SessionLifecycleID{0x01})
		ephemeral := addControlSession(d, "ephemeral", "tab-eph", "pane-eph")
		ephemeral.ephemeral = true
		ephemeral.incarnation = domain.SessionLifecycleID{0x02}
		d.mu.Lock()
		d.inactive["stopped"] = inactiveSession{name: "stopped", cwd: "/tmp/stopped", createdAt: 7, incarnation: domain.SessionLifecycleID{0x03}, state: protocol.SessionDown}
		d.mu.Unlock()
	}

	reference := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
	populateLocal(reference)
	want, err := controlExec{d: reference}.RemoteCatalog(true)
	require.NoError(t, err)
	require.Contains(t, want, `"named"`)

	cases := []struct {
		name      string
		directory func() *countingRemoteDirectory
	}{
		{
			name: "populated",
			directory: func() *countingRemoteDirectory {
				return &countingRemoteDirectory{snapshot: ports.RemoteDirectorySnapshot{
					Revision: 3, Initialized: true,
					Hosts: []ports.RemoteHostSnapshot{reachableDirectoryHost("user@arch", now, catalogue.RemoteCatalogSession{
						LifecycleID: domain.SessionLifecycleID{0x40}, Name: "remote-only", State: catalogue.RemoteCatalogSessionUp,
						Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-remote", Index: 0, Name: "main"}},
					})},
				}}
			},
		},
		{
			name: "failing",
			directory: func() *countingRemoteDirectory {
				return &countingRemoteDirectory{snapshot: ports.RemoteDirectorySnapshot{
					Revision: 4, Initialized: true,
					Hosts: []ports.RemoteHostSnapshot{{
						Endpoint: "user@gone", Availability: domain.RemoteAvailabilityUnreachable,
						LastFailure:         domain.RemoteFailure{Kind: domain.RemoteFailureAuthentication, Err: errors.New("ssh refused")},
						ConsecutiveFailures: 2, FailureEpisode: 5,
					}},
				}}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
			populateLocal(d)
			directory := tc.directory()
			d.remoteDirectory = directory

			got, err := controlExec{d: d}.RemoteCatalog(true)
			require.NoError(t, err)
			require.Equal(t, want, got, "local catalogue must not depend on remote monitoring")
			require.Equal(t, 0, directory.snapshots, "local catalogue must never read the remote directory")
			require.NotContains(t, got, "remote-only")
			require.NotContains(t, got, "user@gone")
		})
	}
}

// TestLocalNavigationInventorySnapshotIsRemoteFree proves the prepared local
// navigation projection publishes exactly one local group with named live
// sessions and resumable stopped sessions only, while the hybrid snapshot still
// composes the foreign group.
func TestLocalNavigationInventorySnapshotIsRemoteFree(t *testing.T) {
	now := time.Unix(1_000, 0)
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
	seedLocalNamedSession(d, "named", domain.SessionLifecycleID{0x10})
	ephemeral := addControlSession(d, "ephemeral", "tab-eph", "pane-eph")
	ephemeral.ephemeral = true
	ephemeral.incarnation = domain.SessionLifecycleID{0x11}
	d.mu.Lock()
	d.inactive["down"] = inactiveSession{name: "down", cwd: "/tmp/down", createdAt: 1, incarnation: domain.SessionLifecycleID{0x20}, state: protocol.SessionDown}
	d.inactive["broken"] = inactiveSession{name: "broken", cwd: "/tmp/broken", createdAt: 2, incarnation: domain.SessionLifecycleID{0x21}, state: protocol.SessionBroken}
	d.inactive["purging"] = inactiveSession{name: "purging", cwd: "/tmp/purging", createdAt: 3, incarnation: domain.SessionLifecycleID{0x22}, state: protocol.SessionDown, purging: true}
	d.mu.Unlock()
	counting := &countingRemoteDirectory{snapshot: ports.RemoteDirectorySnapshot{
		Revision: 2, Initialized: true,
		Hosts: []ports.RemoteHostSnapshot{reachableDirectoryHost("user@arch", now, catalogue.RemoteCatalogSession{
			LifecycleID: domain.SessionLifecycleID{0x40}, Name: "remote", State: catalogue.RemoteCatalogSessionUp,
			Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-remote", Index: 0, Name: "main"}},
		})},
	}}
	d.remoteDirectory = counting

	response := d.localNavigationInventorySnapshot(1)
	require.NoError(t, protocol.ValidateNavigationInventoryResponse(response))
	require.Len(t, response.Groups, 1, "prepared local snapshot carries exactly one source group")
	require.Equal(t, 0, counting.snapshots, "prepared local snapshot must never read the remote directory")
	group := response.Groups[0]
	require.Equal(t, protocol.NavigationInventoryLocalSourceKey, group.SourceKey)
	require.Equal(t, protocol.NavigationInventorySourceOK, group.Status)

	names := map[string]int{}
	for _, entry := range group.Entries {
		names[entry.Name]++
		require.NotEqual(t, "remote", entry.Name, "foreign rows never enter the local group")
	}
	require.Equal(t, 1, names["named"], "named live sessions are exported")
	require.NotContains(t, names, "ephemeral", "ephemeral live sessions are never exported")
	require.Equal(t, 1, names["down"], "resumable stopped sessions are exported")
	require.NotContains(t, names, "broken", "broken records are not resumable")
	require.NotContains(t, names, "purging", "purging records are not visible")

	// The hybrid snapshot still composes local first then the foreign source.
	hybrid := d.snapshotNavigationInventory(2)
	require.NoError(t, protocol.ValidateNavigationInventoryResponse(hybrid))
	require.Len(t, hybrid.Groups, 2)
	require.Equal(t, protocol.NavigationInventoryLocalSourceKey, hybrid.Groups[0].SourceKey)
	require.Equal(t, remoteInventorySourceKey("user@arch"), hybrid.Groups[1].SourceKey)
}

// TestLocalInventoryOverlapIsKeyedByLifecycle proves an overlapping live and
// stopped record stay distinct by lifecycle-qualified key (never by name), the
// stopped key resolves to the stopped representation while it exists and the
// live key to the live one, and the catalogue export deduplicates the
// overlapping stopped record by name.
func TestLocalInventoryOverlapIsKeyedByLifecycle(t *testing.T) {
	now := time.Unix(1_000, 0)
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
	live := seedLocalNamedSession(d, "work", domain.SessionLifecycleID{0x41})
	d.mu.Lock()
	d.inactive["work"] = inactiveSession{name: "work", cwd: "/tmp/work", createdAt: 5, incarnation: domain.SessionLifecycleID{0x42}, state: protocol.SessionDown}
	d.mu.Unlock()

	response := d.localNavigationInventorySnapshot(1)
	require.NoError(t, protocol.ValidateNavigationInventoryResponse(response))
	var liveKey, stoppedKey string
	for _, entry := range response.Groups[0].Entries {
		if entry.Name != "work" {
			continue
		}
		switch entry.State {
		case "up":
			liveKey = entry.EntryKey
		case "stopped":
			stoppedKey = entry.EntryKey
		}
	}
	require.NotEmpty(t, liveKey)
	require.NotEmpty(t, stoppedKey)
	require.NotEqual(t, liveKey, stoppedKey, "overlapping representations never share a key")

	liveTarget, ok := d.localNavigationResolve(localNavigationRequest(liveKey))
	require.True(t, ok)
	require.Equal(t, live.incarnation, liveTarget.LifecycleID)
	stoppedTarget, ok := d.localNavigationResolve(localNavigationRequest(stoppedKey))
	require.True(t, ok)
	require.Equal(t, domain.SessionLifecycleID{0x42}, stoppedTarget.LifecycleID)

	out, err := controlExec{d: d}.RemoteCatalog(true)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(out, `"work"`), "catalogue export deduplicates an overlapping stopped record by name")
}

// TestLocalNavigationResolveExactLifecycle covers the prepared resolver's
// lifecycle discipline: broken and purging records reject, a stopped target
// that becomes live with the same lifecycle stays resolvable, and a replaced
// lifecycle never falls back to a same-name session.
func TestLocalNavigationResolveExactLifecycle(t *testing.T) {
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: time.Unix(1_000, 0)})
	// Broken and purging records are never resumable.
	d.mu.Lock()
	d.inactive["broken"] = inactiveSession{name: "broken", cwd: "/tmp/broken", createdAt: 1, incarnation: domain.SessionLifecycleID{0x61}, state: protocol.SessionBroken}
	d.inactive["purging"] = inactiveSession{name: "purging", cwd: "/tmp/purging", createdAt: 2, incarnation: domain.SessionLifecycleID{0x62}, state: protocol.SessionDown, purging: true}
	// A stale key that will be replaced by a fresh lifecycle.
	d.inactive["gone"] = inactiveSession{name: "gone", cwd: "/tmp/gone", createdAt: 3, incarnation: domain.SessionLifecycleID{0x51}, state: protocol.SessionDown}
	d.mu.Unlock()

	_, ok := d.localNavigationResolve(localNavigationRequest(navigationInventoryEntryKey(domain.SessionLifecycleID{0x61}, "broken")))
	require.False(t, ok, "broken records never resolve")
	_, ok = d.localNavigationResolve(localNavigationRequest(navigationInventoryEntryKey(domain.SessionLifecycleID{0x62}, "purging")))
	require.False(t, ok, "purging records never resolve")

	// Stop-to-live in place: the same lifecycle stays addressable by its key.
	d.mu.Lock()
	d.inactive["live"] = inactiveSession{name: "live", cwd: "/tmp/live", createdAt: 4, incarnation: domain.SessionLifecycleID{0x53}, state: protocol.SessionDown}
	d.mu.Unlock()
	liveKey := navigationInventoryEntryKey(domain.SessionLifecycleID{0x53}, "live")
	stoppedTarget, ok := d.localNavigationResolve(localNavigationRequest(liveKey))
	require.True(t, ok)
	require.Equal(t, domain.SessionLifecycleID{0x53}, stoppedTarget.LifecycleID)
	addInventorySession(d, "live", domain.SessionLifecycleID{0x53}, false)
	d.mu.Lock()
	delete(d.inactive, "live")
	d.mu.Unlock()
	resumedTarget, ok := d.localNavigationResolve(localNavigationRequest(liveKey))
	require.True(t, ok, "a stopped target that becomes live with the same lifecycle stays resolvable")
	require.Equal(t, domain.SessionLifecycleID{0x53}, resumedTarget.LifecycleID)

	// Replacement: the old lifecycle key must not match the new incarnation.
	oldKey := navigationInventoryEntryKey(domain.SessionLifecycleID{0x51}, "gone")
	_, ok = d.localNavigationResolve(localNavigationRequest(oldKey))
	require.True(t, ok)
	d.mu.Lock()
	delete(d.inactive, "gone")
	d.mu.Unlock()
	addInventorySession(d, "gone", domain.SessionLifecycleID{0x52}, false)
	_, ok = d.localNavigationResolve(localNavigationRequest(oldKey))
	require.False(t, ok, "a stale lifecycle never resolves to a same-name replacement")
	replacement, ok := d.localNavigationResolve(localNavigationRequest(navigationInventoryEntryKey(domain.SessionLifecycleID{0x52}, "gone")))
	require.True(t, ok)
	require.Equal(t, domain.SessionLifecycleID{0x52}, replacement.LifecycleID)
}

// TestLocalNavigationResolveRejectsForeignBeforeNameLookup proves a foreign
// source key or a structured foreign registration is rejected even when a
// local session shares the row's name.
func TestLocalNavigationResolveRejectsForeignBeforeNameLookup(t *testing.T) {
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: time.Unix(1_000, 0)})
	addInventorySession(d, "shared", domain.SessionLifecycleID{0x71}, false)
	key := navigationInventoryEntryKey(domain.SessionLifecycleID{0x71}, "shared")

	_, ok := d.localNavigationResolve(localNavigationRequest(key))
	require.True(t, ok, "the exact local key resolves")

	foreignSource := localNavigationRequest(key)
	foreignSource.SourceKey = remoteInventorySourceKey("user@arch")
	_, ok = d.localNavigationResolve(foreignSource)
	require.False(t, ok, "a foreign source key is never a local resolve")

	registration, err := domain.NewRemoteRegistration("user@arch", [16]byte{7})
	require.NoError(t, err)
	foreignRegistration := localNavigationRequest(key)
	foreignRegistration.Registration = registration
	_, ok = d.localNavigationResolve(foreignRegistration)
	require.False(t, ok, "a structured foreign registration is rejected even with a same-name local session")
}

// TestLocalPickerControlResolveExactness covers the prepared local control
// resolver: exact lifecycle, stale tab rejection, and foreign target
// rejection, with no name fallback.
func TestLocalPickerControlResolveExactness(t *testing.T) {
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: time.Unix(1_000, 0)})
	sess := seedLocalNamedSession(d, "work", domain.SessionLifecycleID{0x81})

	resolved, ok := d.localPickerControlAttachTarget(picker.Target{Session: sess.id, Incarnation: sess.incarnation, TabID: "tab-work"})
	require.True(t, ok, "the current exact lifecycle and tab resolve")
	require.Equal(t, "work", resolved.Session)
	require.NotNil(t, resolved.ExactTarget)
	require.Equal(t, domain.SessionLifecycleID{0x81}, resolved.ExactTarget.LifecycleID)
	require.Equal(t, domain.TabStableID("tab-work"), resolved.PreferredTabID)

	_, ok = d.localPickerControlAttachTarget(picker.Target{Session: sess.id, Incarnation: sess.incarnation})
	require.True(t, ok, "a session header with no preferred tab resolves")

	_, ok = d.localPickerControlAttachTarget(picker.Target{Session: sess.id, Incarnation: sess.incarnation, TabID: "tab-stale"})
	require.False(t, ok, "a stale tab rejects instead of forwarding a dangling tab ID")

	_, ok = d.localPickerControlAttachTarget(picker.Target{Session: sess.id, Incarnation: domain.IncarnationID{0x82}, TabID: "tab-work"})
	require.False(t, ok, "a replaced lifecycle never resolves by name")

	_, ok = d.localPickerControlAttachTarget(picker.Target{Session: sess.id, Incarnation: sess.incarnation, RemoteTarget: &domain.RemoteSessionTarget{}})
	require.False(t, ok, "a structured foreign target is rejected before any lookup")
	_, ok = d.localPickerControlAttachTarget(picker.Target{Session: sess.id, Incarnation: sess.incarnation, RemoteKey: &domain.RemoteSessionKey{Host: "user@arch", Name: "work"}})
	require.False(t, ok, "a foreign session key is rejected before any lookup")
}

// TestLocalProjectionsPreserveInventoryCounts proves the prepared local
// projections and resolvers, driven with no presenting attachment, leave the
// live session registry, the stopped records, and the seeded session's
// attachment and tab slices unchanged, and never open a PTY. The attached
// picker path (non-nil cur/ac) is deliberately not exercised here:
// localPickerViews may repair that attachment's own view, which is
// presentation state rather than shared inventory.
func TestLocalProjectionsPreserveInventoryCounts(t *testing.T) {
	d := newTestDaemon(t, failPTYFactory{t}, stubClock{})
	sess := seedLocalNamedSession(d, "work", domain.SessionLifecycleID{0x91})
	d.mu.Lock()
	d.inactive["stopped"] = inactiveSession{name: "stopped", cwd: "/tmp/stopped", createdAt: 1, incarnation: domain.SessionLifecycleID{0x92}, state: protocol.SessionDown}
	sessionsBefore := len(d.sessions)
	stoppedBefore := len(d.inactive)
	d.mu.Unlock()
	sess.mu.Lock()
	attachmentsBefore := len(sess.attachments)
	tabsBefore := len(sess.tabs)
	sess.mu.Unlock()

	response := d.localNavigationInventorySnapshot(1)
	require.NoError(t, protocol.ValidateNavigationInventoryResponse(response))
	_, ok := d.localNavigationResolve(localNavigationRequest(navigationInventoryEntryKey(domain.SessionLifecycleID{0x91}, "work")))
	require.True(t, ok)
	_, _, _ = d.localPickerViewProjections(nil, nil)
	_, ok = d.localPickerControlAttachTarget(picker.Target{Session: sess.id, Incarnation: sess.incarnation, TabID: "tab-work"})
	require.True(t, ok)

	d.mu.Lock()
	require.Equal(t, sessionsBefore, len(d.sessions), "local projections never create or remove sessions")
	require.Equal(t, stoppedBefore, len(d.inactive), "local projections never restore or purge stopped records")
	d.mu.Unlock()
	sess.mu.Lock()
	require.Equal(t, attachmentsBefore, len(sess.attachments), "local projections never attach")
	require.Equal(t, tabsBefore, len(sess.tabs), "local projections never reshape geometry")
	sess.mu.Unlock()
}

// TestLocalProjectionMatchesHybridWithoutRemote proves the prepared local
// picker projection is exactly what the hybrid projection produces when no
// remote directory is installed, so the P7 cutover can drop the foreign rows
// without changing local presentation.
func TestLocalProjectionMatchesHybridWithoutRemote(t *testing.T) {
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: time.Unix(1_000, 0)})
	seedLocalNamedSession(d, "work", domain.SessionLifecycleID{0xA1})
	other := seedLocalNamedSession(d, "other", domain.SessionLifecycleID{0xA2})
	other.mruAt.Store(2)
	d.mu.Lock()
	d.inactive["stopped"] = inactiveSession{name: "stopped", cwd: "/tmp/stopped", createdAt: 1, incarnation: domain.SessionLifecycleID{0xA3}, state: protocol.SessionDown}
	d.mu.Unlock()
	require.Nil(t, d.remoteDirectory)

	localRecent, localGrouped, localCurrent := d.localPickerViewProjections(nil, nil)
	hybridRecent, hybridGrouped, hybridCurrent := d.pickerViewProjections(nil, nil)
	require.Equal(t, hybridRecent, localRecent)
	require.Equal(t, hybridGrouped, localGrouped)
	require.Equal(t, hybridCurrent, localCurrent)
}

// TestLocalProjectionIgnoresDirectoryAndMatchesFilteredHybrid proves the
// prepared local picker projection is independent of remote monitoring and is
// exactly the hybrid projection with its foreign rows removed. The local call
// must never read the directory (zero snapshots), must not change when the
// directory is installed or removed, and — compared row-for-row after dropping
// exactly the foreign rows by ID — must preserve the hybrid's local ordering.
func TestLocalProjectionIgnoresDirectoryAndMatchesFilteredHybrid(t *testing.T) {
	now := time.Unix(1_000, 0)
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
	seedLocalNamedSession(d, "alpha", domain.SessionLifecycleID{0xC1})
	d.mu.Lock()
	d.inactive["zulu"] = inactiveSession{name: "zulu", cwd: "/tmp/zulu", createdAt: 5, incarnation: domain.SessionLifecycleID{0xC2}, state: protocol.SessionDown}
	d.mu.Unlock()
	directory := &countingRemoteDirectory{snapshot: ports.RemoteDirectorySnapshot{Revision: 1, Initialized: true,
		Hosts: []ports.RemoteHostSnapshot{reachableDirectoryHost("user@arch", now, catalogue.RemoteCatalogSession{
			LifecycleID: domain.SessionLifecycleID{0xC3}, Name: "delta", State: catalogue.RemoteCatalogSessionUp,
			Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-delta", Index: 0, Name: "main"}}, ActiveTabID: "tab-delta",
		})}}}
	d.remoteDirectory = directory

	localRecent, localGrouped, localCurrent := d.localPickerViewProjections(nil, nil)
	require.Zero(t, directory.snapshots, "the prepared local projection must never read the remote directory")

	// Dependency: an installed directory cannot change the local projection.
	d.remoteDirectory = nil
	bareRecent, bareGrouped, bareCurrent := d.localPickerViewProjections(nil, nil)
	require.Equal(t, localRecent, bareRecent, "local rows never depend on the directory")
	require.Equal(t, localGrouped, bareGrouped, "local sections never depend on the directory")
	require.Equal(t, localCurrent, bareCurrent)

	// The hybrid projection composes the same local rows with the foreign ones.
	d.remoteDirectory = directory
	hybridRecent, hybridGrouped, hybridCurrent := d.pickerViewProjections(nil, nil)
	require.Positive(t, directory.snapshots, "the hybrid projection does read the installed directory")
	require.Equal(t, localCurrent, hybridCurrent)

	foreign := d.foreignPickerViews(now)
	foreignIDs := map[domain.SessionID]struct{}{}
	for _, row := range foreign.recent {
		foreignIDs[row.ID] = struct{}{}
	}
	for _, row := range foreign.grouped {
		foreignIDs[row.ID] = struct{}{}
	}
	require.NotEmpty(t, foreignIDs)
	// Drop exactly the foreign rows by ID — never by name or section — so a
	// local row cannot be hidden and the surviving order is compared in full.
	require.Equal(t, localRecent, dropPickerRows(hybridRecent, foreignIDs), "filtered recent order matches the local projection")
	require.Equal(t, localGrouped, dropPickerRows(hybridGrouped, foreignIDs), "filtered grouped order matches the local projection")
}

// dropPickerRows returns rows with none of ids, preserving order.
func dropPickerRows(rows []pickerSessionView, ids map[domain.SessionID]struct{}) []pickerSessionView {
	kept := make([]pickerSessionView, 0, len(rows))
	for _, row := range rows {
		if _, ok := ids[row.ID]; ok {
			continue
		}
		kept = append(kept, row)
	}
	return kept
}

// pickerRowShape is the exact slice element the hybrid ordering tests compare:
// the rendered row name, its section label, and whether it is a stopped row.
type pickerRowShape struct {
	Name    string
	Section string
	Stopped bool
}

func pickerRowShapes(views []pickerSessionView) []pickerRowShape {
	shapes := make([]pickerRowShape, 0, len(views))
	for _, view := range views {
		shapes = append(shapes, pickerRowShape{Name: view.Name, Section: view.Section, Stopped: view.Stopped})
	}
	return shapes
}

// TestHybridPickerOrderingAcrossRemoteDirectoryStates pins the exact Recent and
// Grouped hybrid projections across the remote-directory states: a populated
// directory, an installed-but-never-initialized directory that publishes the
// checking placeholder, and no installed directory at all. The foreign rows
// interleave between the local live and stopped groups, the first published row
// of each remote host carries the "REMOTE  <endpoint>" label, the checking
// placeholder carries "REMOTE", and any raw catalogue row — including an
// invalid-key one that publishes no row of its own — suppresses the LOCAL
// label on a trailing stopped row (the `catalogRows > 0 || checking` rule the
// refactor re-derived). With no installed directory a stopped-only row still
// opens the grouped LOCAL section while the recent projection stays
// section-free. The prepared local projection is this same shape with the
// foreign rows removed, so these cases fix the row-for-row contract the P7
// cutover must preserve.
func TestHybridPickerOrderingAcrossRemoteDirectoryStates(t *testing.T) {
	now := time.Unix(1_000, 0)
	remoteSession := catalogue.RemoteCatalogSession{
		LifecycleID: domain.SessionLifecycleID{0xD1}, Name: "delta", State: catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-delta", Index: 0, Name: "main"}},
		ActiveTabID: "tab-delta",
	}
	// A catalogue session whose name cannot form a valid remote key: the row is
	// never published, but the raw entry still counts toward catalogRows.
	invalidKeySession := catalogue.RemoteCatalogSession{
		LifecycleID: domain.SessionLifecycleID{0xD2}, Name: "", State: catalogue.RemoteCatalogSessionUp,
	}
	seedStoppedZulu := func(d *Daemon) {
		d.mu.Lock()
		d.inactive["zulu"] = inactiveSession{name: "zulu", cwd: "/tmp/zulu", createdAt: 5, incarnation: domain.SessionLifecycleID{0xB2}, state: protocol.SessionDown}
		d.mu.Unlock()
	}

	cases := []struct {
		name        string
		seed        func(d *Daemon)
		directory   func() ports.RemoteDirectory
		wantRecent  []pickerRowShape
		wantGrouped []pickerRowShape
	}{
		{
			name: "live+remote+stopped",
			seed: func(d *Daemon) {
				seedLocalNamedSession(d, "alpha", domain.SessionLifecycleID{0xB1})
				seedStoppedZulu(d)
			},
			directory: func() ports.RemoteDirectory {
				return &countingRemoteDirectory{snapshot: ports.RemoteDirectorySnapshot{Revision: 1, Initialized: true,
					Hosts: []ports.RemoteHostSnapshot{reachableDirectoryHost("user@arch", now, remoteSession)}}}
			},
			wantRecent:  []pickerRowShape{{Name: "alpha"}, {Name: "delta@arch"}, {Name: "zulu", Stopped: true}},
			wantGrouped: []pickerRowShape{{Name: "alpha", Section: "LOCAL"}, {Name: "delta@arch", Section: "REMOTE  user@arch"}, {Name: "zulu", Stopped: true}},
		},
		{
			name: "remote-only",
			seed: func(*Daemon) {},
			directory: func() ports.RemoteDirectory {
				return &countingRemoteDirectory{snapshot: ports.RemoteDirectorySnapshot{Revision: 2, Initialized: true,
					Hosts: []ports.RemoteHostSnapshot{reachableDirectoryHost("user@arch", now, remoteSession)}}}
			},
			wantRecent:  []pickerRowShape{{Name: "delta@arch"}},
			wantGrouped: []pickerRowShape{{Name: "delta@arch", Section: "REMOTE  user@arch"}},
		},
		{
			name: "checking-only",
			seed: func(*Daemon) {},
			directory: func() ports.RemoteDirectory {
				return &countingRemoteDirectory{snapshot: ports.RemoteDirectorySnapshot{Revision: 3}}
			},
			wantRecent:  []pickerRowShape{{Name: "checking remotes\u2026"}},
			wantGrouped: []pickerRowShape{{Name: "checking remotes\u2026", Section: "REMOTE"}},
		},
		{
			name: "stopped-only, no directory",
			seed: seedStoppedZulu,
			directory: func() ports.RemoteDirectory {
				return nil
			},
			// With no installed directory the stopped row opens the grouped
			// LOCAL section, and the recent projection stays section-free.
			wantRecent:  []pickerRowShape{{Name: "zulu", Stopped: true}},
			wantGrouped: []pickerRowShape{{Name: "zulu", Section: "LOCAL", Stopped: true}},
		},
		{
			name: "checking+stopped",
			seed: seedStoppedZulu,
			directory: func() ports.RemoteDirectory {
				return &countingRemoteDirectory{snapshot: ports.RemoteDirectorySnapshot{Revision: 5}}
			},
			// The checking placeholder precedes the stopped row, and its raw
			// state suppresses the LOCAL label, so the stopped row keeps no
			// section.
			wantRecent:  []pickerRowShape{{Name: "checking remotes\u2026"}, {Name: "zulu", Stopped: true}},
			wantGrouped: []pickerRowShape{{Name: "checking remotes\u2026", Section: "REMOTE"}, {Name: "zulu", Stopped: true}},
		},
		{
			name: "invalid-key host",
			seed: seedStoppedZulu,
			directory: func() ports.RemoteDirectory {
				return &countingRemoteDirectory{snapshot: ports.RemoteDirectorySnapshot{Revision: 4, Initialized: true,
					Hosts: []ports.RemoteHostSnapshot{reachableDirectoryHost("user@arch", now, invalidKeySession)}}}
			},
			// The invalid-key host publishes no remote row, but its raw catalogue
			// entry still counts, so the trailing stopped row loses its LOCAL
			// label exactly as the pre-refactor projection did.
			wantRecent:  []pickerRowShape{{Name: "zulu", Stopped: true}},
			wantGrouped: []pickerRowShape{{Name: "zulu", Stopped: true}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
			tc.seed(d)
			d.remoteDirectory = tc.directory()

			recent, grouped, _ := d.pickerViewProjections(nil, nil)
			require.Equal(t, tc.wantRecent, pickerRowShapes(recent), "recent order and sections")
			require.Equal(t, tc.wantGrouped, pickerRowShapes(grouped), "grouped order and sections")
		})
	}
}

// TestHybridPickerControlAttachTargetFallsBackOnVanishedTab is the GO-001
// TOCTOU case: the picker snapshot named a tab that is still present, the tab
// closes before the resolve commits, and the hybrid wrapper must still attach
// the exact local lifecycle with no preferred tab so the daemon performs the
// same first-tab repair the client applies to a dangling id. The prepared
// strict resolver keeps rejecting the stale tab, and a replaced lifecycle
// still never falls back by name.
func TestHybridPickerControlAttachTargetFallsBackOnVanishedTab(t *testing.T) {
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: time.Unix(1_000, 0)})
	sess := seedLocalNamedSession(d, "work", domain.SessionLifecycleID{0x85})
	target := picker.Target{Session: sess.id, Incarnation: sess.incarnation, TabID: "tab-work"}

	// Snapshot time: both paths forward the live tab.
	strict, ok := d.localPickerControlAttachTarget(target)
	require.True(t, ok)
	require.Equal(t, domain.TabStableID("tab-work"), strict.PreferredTabID)
	hybrid, ok := d.pickerControlAttachTarget(target)
	require.True(t, ok)
	require.Equal(t, domain.TabStableID("tab-work"), hybrid.PreferredTabID)

	// The tab closes between the snapshot and the resolve.
	sess.mu.Lock()
	sess.tabs = nil
	sess.mu.Unlock()

	_, ok = d.localPickerControlAttachTarget(target)
	require.False(t, ok, "the prepared strict resolver rejects the vanished tab")

	hybrid, ok = d.pickerControlAttachTarget(target)
	require.True(t, ok, "the hybrid wrapper preserves the live attach")
	require.Empty(t, hybrid.PreferredTabID, "the vanished tab falls back to the client's missing-tab repair")
	require.NotNil(t, hybrid.ExactTarget)
	require.Equal(t, domain.SessionLifecycleID{0x85}, hybrid.ExactTarget.LifecycleID)
	require.Equal(t, "work", hybrid.Session)

	// The fallback still never resolves a same-name replacement by name.
	_, ok = d.pickerControlAttachTarget(picker.Target{Session: sess.id, Incarnation: domain.IncarnationID{0x86}, TabID: "tab-work"})
	require.False(t, ok, "a replaced lifecycle rejects even with the vanished-tab fallback")
}
