package client

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Route ledger (Plan 003 C4/E3), ported from main's routes_test observation
// and ordering cases onto the broker catalogue.

const routeTestRemote = "vev@box"

func routeTestSnapshot(daemons ...ports.BrokerDaemonObservation) ports.BrokerSnapshot {
	return ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: daemons}
}

func routeTestLocal(sessions ...catalogue.RemoteCatalogSession) ports.BrokerDaemonObservation {
	return pickerTestLocalObservation(time.Unix(1000, 0), sessions...)
}

func routeTestRemoteHost(sessions ...catalogue.RemoteCatalogSession) ports.BrokerDaemonObservation {
	return pickerTestRemoteObservation(routeTestRemote, 1, 2, time.Unix(1000, 0), sessions...)
}

func routeTestLive(name string, seed byte, lastUsed uint64, attention ...bool) catalogue.RemoteCatalogSession {
	tabs := make([]catalogue.RemoteCatalogTab, 0, len(attention))
	for i, bell := range attention {
		tabs = append(tabs, pickerTestTab(fmt.Sprintf("t%d", i), fmt.Sprintf("tab%d", i), "", bell))
	}
	return pickerTestTabbed(name, seed, catalogue.RemoteCatalogSessionUp, lastUsed, "", tabs...)
}

func routeTestStopped(name string, seed byte, lastUsed uint64) catalogue.RemoteCatalogSession {
	return pickerTestTabbed(name, seed, catalogue.RemoteCatalogSessionDown, lastUsed, "")
}

func routeTestActive(local bool, name string, seed byte) routeActive {
	authority := routeAuthority{local: true}
	if !local {
		authority = routeAuthority{endpoint: routeTestRemote}
	}
	return routeActive{known: true, authority: authority, target: protocol.ExactSessionTarget{LifecycleID: pickerTestLifecycle(seed), SessionName: name}}
}

func routeEntryNames(snapshot protocol.RecentRouteSnapshot) []string {
	names := make([]string, 0, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		names = append(names, entry.Name)
	}
	return names
}

func TestRouteLedgerOrdering(t *testing.T) {
	tests := []struct {
		name   string
		visits []routeActive
		build  ports.BrokerSnapshot
		active routeActive
		want   []string
	}{
		{
			name: "live local by recency, then remote in catalogue order, then stopped",
			build: routeTestSnapshot(
				routeTestLocal(routeTestLive("old", 1, 1), routeTestStopped("gone", 2, 9), routeTestLive("new", 3, 5), routeTestLive("here", 4, 7)),
				routeTestRemoteHost(routeTestLive("r2", 5, 0), routeTestLive("r1", 6, 0), routeTestStopped("rs", 7, 0)),
			),
			active: routeTestActive(true, "here", 4),
			want:   []string{"new", "old", "r2", "r1", "gone", "rs"},
		},
		{
			name:   "this client's attach recency comes first",
			visits: []routeActive{routeTestActive(false, "r1", 6), routeTestActive(true, "old", 1)},
			build: routeTestSnapshot(
				routeTestLocal(routeTestLive("old", 1, 1), routeTestLive("new", 3, 5), routeTestLive("here", 4, 7)),
				routeTestRemoteHost(routeTestLive("r2", 5, 0), routeTestLive("r1", 6, 0)),
			),
			active: routeTestActive(true, "here", 4),
			want:   []string{"old", "r1", "new", "r2"},
		},
		{
			name:   "broken sessions are never routes",
			build:  routeTestSnapshot(routeTestLocal(routeTestLive("ok", 1, 1), pickerTestTabbed("bad", 2, catalogue.RemoteCatalogSessionBroken, 5, ""))),
			active: routeTestActive(true, "here", 4),
			want:   []string{"ok"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ledger := newRouteLedger()
			for _, visit := range tt.visits {
				ledger.build(tt.build, visit)
			}
			snapshot, _ := ledger.build(tt.build, tt.active)
			require.NoError(t, snapshot.Validate())
			require.Equal(t, tt.want, routeEntryNames(snapshot))
			require.Equal(t, tt.active.target, snapshot.ActiveEntry.Target, "the attached lifecycle is metadata only")
		})
	}
}

func TestRouteLedgerActivePreviousAndHome(t *testing.T) {
	build := routeTestSnapshot(
		routeTestLocal(routeTestLive("a", 1, 2), routeTestLive("b", 2, 1)),
		routeTestRemoteHost(routeTestLive("r", 3, 0)),
	)
	tests := []struct {
		name         string
		active       routeActive
		wantKind     protocol.RouteKind
		wantLabel    string
		wantHome     bool
		wantPrevious string
		wantHosts    []protocol.RouteKind
	}{
		{name: "local attachment is home", active: routeTestActive(true, "a", 1), wantKind: protocol.RouteKindLocal, wantHome: true, wantPrevious: "r", wantHosts: []protocol.RouteKind{protocol.RouteKindRemote}},
		{name: "remote attachment offers the local host", active: routeTestActive(false, "r", 3), wantKind: protocol.RouteKindRemote, wantLabel: routeTestRemote, wantPrevious: "a", wantHosts: []protocol.RouteKind{protocol.RouteKindLocal}},
		{name: "an unobserved attachment is synthesized", active: routeTestActive(true, "fresh", 9), wantKind: protocol.RouteKindLocal, wantHome: true, wantPrevious: "r", wantHosts: []protocol.RouteKind{protocol.RouteKindRemote}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ledger := newRouteLedger()
			// Visit the other kind first so Previous names it.
			if tt.active.authority.local {
				ledger.build(build, routeTestActive(false, "r", 3))
			} else {
				ledger.build(build, routeTestActive(true, "a", 1))
			}
			snapshot, changed := ledger.build(build, tt.active)
			require.True(t, changed)
			require.NoError(t, snapshot.Validate())
			require.Equal(t, tt.wantKind, snapshot.ActiveEntry.Kind)
			require.Equal(t, tt.wantLabel, snapshot.ActiveEntry.HostLabel)
			require.Equal(t, snapshot.Active, protocol.RouteRef{Key: snapshot.ActiveEntry.Key, Generation: snapshot.ActiveEntry.Generation})
			if tt.wantHome {
				require.Equal(t, snapshot.Active, snapshot.Home)
			} else {
				require.True(t, snapshot.Home.IsZero())
			}
			previous, ok := ledger.resolve(snapshot.Previous)
			require.True(t, ok)
			require.Equal(t, tt.wantPrevious, previous.target.SessionName)
			kinds := make([]protocol.RouteKind, 0, len(snapshot.Hosts))
			for _, host := range snapshot.Hosts {
				kinds = append(kinds, host.Kind)
				resolved, ok := ledger.resolve(protocol.RouteRef{Key: host.Key, Generation: host.Generation})
				require.True(t, ok)
				require.True(t, resolved.host)
				require.NotEqual(t, tt.active.authority, resolved.authority, "the serving daemon is never a host row")
			}
			require.Equal(t, tt.wantHosts, kinds)
		})
	}
}

func TestRouteLedgerStableReferencesAndGeneration(t *testing.T) {
	ledger := newRouteLedger()
	active := routeTestActive(true, "here", 4)
	first, changed := ledger.build(routeTestSnapshot(routeTestLocal(routeTestLive("a", 1, 1), routeTestLive("here", 4, 2))), active)
	require.True(t, changed)
	require.Equal(t, uint64(1), first.Generation)

	same, changed := ledger.build(routeTestSnapshot(routeTestLocal(routeTestLive("a", 1, 1), routeTestLive("here", 4, 2))), active)
	require.False(t, changed, "an unchanged publication never advances the generation")
	require.Equal(t, first, same)

	grown, changed := ledger.build(routeTestSnapshot(routeTestLocal(routeTestLive("a", 1, 1), routeTestLive("b", 2, 3), routeTestLive("here", 4, 2))), active)
	require.True(t, changed)
	require.Equal(t, uint64(2), grown.Generation)
	require.Equal(t, first.Active, grown.Active, "a lifecycle keeps its reference")
	require.Equal(t, first.Entries[0].Key, grown.Entries[1].Key)

	replaced, _ := ledger.build(routeTestSnapshot(routeTestLocal(routeTestLive("a", 5, 1), routeTestLive("here", 4, 2))), active)
	require.NotEqual(t, first.Entries[0].Key, replaced.Entries[0].Key, "a new lifecycle of the same name is a new reference")
	_, ok := ledger.resolve(protocol.RouteRef{Key: first.Entries[0].Key, Generation: 1})
	require.False(t, ok, "a retired lifecycle no longer resolves")
}

// TestRouteLedgerAttention ports main's local and remote attention
// observations: a session carries attention while any of its tabs does, and
// onsets are ordered as the client observed them across daemons.
func TestRouteLedgerAttention(t *testing.T) {
	active := routeTestActive(true, "here", 4)
	steps := []struct {
		name  string
		build ports.BrokerSnapshot
		want  map[string]bool
		order []string
	}{
		{
			name:  "quiet",
			build: routeTestSnapshot(routeTestLocal(routeTestLive("here", 4, 9), routeTestLive("loc", 1, 1, false, false)), routeTestRemoteHost(routeTestLive("rem", 2, 0, false))),
			want:  map[string]bool{"loc": false, "rem": false},
		},
		{
			name:  "remote bell first",
			build: routeTestSnapshot(routeTestLocal(routeTestLive("here", 4, 9), routeTestLive("loc", 1, 1, false, false)), routeTestRemoteHost(routeTestLive("rem", 2, 0, true))),
			want:  map[string]bool{"loc": false, "rem": true},
			order: []string{"rem"},
		},
		{
			name:  "then a local tab bell",
			build: routeTestSnapshot(routeTestLocal(routeTestLive("here", 4, 9), routeTestLive("loc", 1, 1, false, true)), routeTestRemoteHost(routeTestLive("rem", 2, 0, true))),
			want:  map[string]bool{"loc": true, "rem": true},
			order: []string{"rem", "loc"},
		},
		{
			name:  "remote cleared, then rings again later",
			build: routeTestSnapshot(routeTestLocal(routeTestLive("here", 4, 9), routeTestLive("loc", 1, 1, false, true)), routeTestRemoteHost(routeTestLive("rem", 2, 0, false))),
			want:  map[string]bool{"loc": true, "rem": false},
			order: []string{"loc"},
		},
		{
			name:  "rings again",
			build: routeTestSnapshot(routeTestLocal(routeTestLive("here", 4, 9), routeTestLive("loc", 1, 1, false, true)), routeTestRemoteHost(routeTestLive("rem", 2, 0, true))),
			want:  map[string]bool{"loc": true, "rem": true},
			order: []string{"loc", "rem"},
		},
	}
	ledger := newRouteLedger()
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			snapshot, _ := ledger.build(step.build, active)
			require.NoError(t, snapshot.Validate())
			got := map[string]bool{}
			seq := map[string]uint64{}
			for _, entry := range snapshot.Entries {
				got[entry.Name] = entry.Attention
				if entry.Attention {
					require.NotZero(t, entry.AttentionSeq)
					seq[entry.Name] = entry.AttentionSeq
				} else {
					require.Zero(t, entry.AttentionSeq)
				}
			}
			require.Equal(t, step.want, got)
			for i := 1; i < len(step.order); i++ {
				require.Less(t, seq[step.order[i-1]], seq[step.order[i]], "%s rang before %s", step.order[i-1], step.order[i])
			}
		})
	}
}

func TestRouteLedgerBounds(t *testing.T) {
	sessions := make([]catalogue.RemoteCatalogSession, 0, protocol.RouteSnapshotMaxEntries+5)
	for i := 0; i < protocol.RouteSnapshotMaxEntries+5; i++ {
		sessions = append(sessions, routeTestLive(fmt.Sprintf("s%02d", i), byte(i+10), uint64(i)))
	}
	incompatible := pickerTestRemoteObservation("old@box", 3, 4, time.Unix(1000, 0))
	incompatible.Availability = domain.RemoteAvailabilityIncompatible
	ledger := newRouteLedger()
	snapshot, changed := ledger.build(routeTestSnapshot(routeTestLocal(sessions...), incompatible), routeTestActive(true, "s00", 10))
	require.True(t, changed)
	require.NoError(t, snapshot.Validate())
	require.Len(t, snapshot.Entries, protocol.RouteSnapshotMaxEntries)
	require.Empty(t, snapshot.Hosts, "an incompatible daemon is never a creation host")
}
