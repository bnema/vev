package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/stretchr/testify/require"
)

func TestPaletteLocalQualificationDependsOnConfiguredHosts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hosts []ports.RemoteHostSnapshot
		want  string
	}{
		{name: "local only", want: "Switch to session sample"},
		{name: "offline host without sessions", hosts: []ports.RemoteHostSnapshot{{Endpoint: "host-a"}}, want: "Switch to session sample@local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			sess := addControlSession(d, "sample", "tab-1", "pane-1")
			sess.ephemeral = false
			sess.incarnation = domain.SessionLifecycleID{1}
			seedRemoteDirectory(t, d, tc.hosts...)
			results := d.paletteResults(nil, nil, protocol.RecentRouteSnapshot{})
			var labels []string
			for _, result := range results {
				if _, ok := result.SessionTarget(); ok {
					labels = append(labels, result.DisplayText())
				}
			}
			require.Equal(t, []string{tc.want}, labels)
		})
	}
}

func TestPaletteHistoryUsesClientRelativeOrigin(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	snapshot := protocol.RecentRouteSnapshot{Generation: 1, Entries: []protocol.RecentRouteEntry{
		{Key: 1, Generation: 1, Name: "sample", Kind: protocol.RouteKindLocal, Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "sample"}},
		{Key: 2, Generation: 1, Name: "sample", Kind: protocol.RouteKindRemote, HostLabel: "host-a", Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "sample"}},
	}}
	results := d.paletteResults(nil, nil, snapshot)
	require.Len(t, results, 2)
	require.Equal(t, "Switch to session sample@local", results[0].DisplayText())
	require.Equal(t, "Switch to session sample@host-a", results[1].DisplayText())
}

func TestPaletteImportedLocalSessionReplacesDuplicateHistoryRoute(t *testing.T) {
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "sample"}
	action := protocol.RouteNavigationAction{SnapshotGeneration: 1, Key: 1, Generation: 1}
	ac := &attachedClient{overlays: &overlayRuntime{
		paletteInventoryOpen:   true,
		paletteRouteSnapshot:   protocol.RecentRouteSnapshot{Entries: []protocol.RecentRouteEntry{{Key: 1, Generation: 1, Kind: protocol.RouteKindLocal, Target: target}}},
		paletteInventoryGroups: []protocol.NavigationInventorySourceGroup{{SourceKey: "local", Status: protocol.NavigationInventorySourceOK, Entries: []protocol.NavigationInventoryEntry{{SourceKey: "local", EntryKey: navigationInventoryEntryKey(target.LifecycleID, target.SessionName), Name: "sample", DisplayOrigin: "local", State: "up"}}}},
	}}
	results := appendImportedResults([]palette.Result{palette.NewRecentRouteResult("sample", "sample", action)}, ac)
	require.Len(t, results, 1, "history and inventory represent one exact local destination")
	require.Equal(t, palette.ResultKindImportedSession, results[0].Kind())

	for _, tc := range []struct {
		name   string
		kind   protocol.RouteKind
		target protocol.ExactSessionTarget
	}{
		{"replacement lifecycle", protocol.RouteKindLocal, protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "sample"}},
		{"remote homonym", protocol.RouteKindRemote, target},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ac.overlays.paletteRouteSnapshot.Entries[0].Kind = tc.kind
			ac.overlays.paletteRouteSnapshot.Entries[0].Target = tc.target
			results := appendImportedResults([]palette.Result{palette.NewRecentRouteResult("sample", "sample", action)}, ac)
			require.Len(t, results, 2, "presentation homonyms must not erase distinct identities")
		})
	}
}
