package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// TestSessionInventorySharedCapture verifies the daemon-owned common capture
// feeds palette, picker, and catalog export with identical lifecycle facts,
// while preserving each projection's intentional cost and filtering policy.
func TestSessionInventorySharedCapture(t *testing.T) {
	now := time.Unix(1_000, 0)
	d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
	current := addControlSession(d, "current", "tab-1", "pane-1")
	current.ephemeral = false
	current.incarnation = domain.SessionLifecycleID{11}
	other := addControlSession(d, "other", "tab-2", "pane-2")
	other.ephemeral = false
	other.incarnation = domain.SessionLifecycleID{12}
	remoteLifecycle := domain.SessionLifecycleID{21}
	seedRemoteDirectory(t, d, reachableDirectoryHost("user@arch", now, catalogue.RemoteCatalogSession{
		LifecycleID: remoteLifecycle, Name: "shared", State: catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-remote", Index: 0, Name: "shell"}},
		ActiveTabID: "tab-remote",
	}))

	cheap := d.captureSessionInventory(viewOptions{}, false)
	rich := d.captureSessionInventory(viewOptions{tabDetails: true, focusedTitles: true}, true)

	require.Len(t, cheap.live, 2)
	require.Len(t, rich.live, 2)
	require.Len(t, cheap.hosts, 1)
	require.Equal(t, cheap.hosts[0].Endpoint, rich.hosts[0].Endpoint)
	// Cheap palette capture avoids tab details; rich picker/catalog capture includes them.
	require.Empty(t, cheap.live[0].view.tabs)
	require.NotEmpty(t, rich.live[0].view.tabs)

	results := d.paletteResults(current, nil, protocol.RecentRouteSnapshot{})
	foundOther := false
	foundRemote := false
	for _, result := range results {
		if name, ok := result.SessionName(); ok && name == "other" {
			foundOther = true
		}
		if _, ok := result.RemoteSessionTarget(); ok {
			foundRemote = true
		}
	}
	require.True(t, foundOther, "palette must project shared live sessions")
	require.True(t, foundRemote, "palette must project shared remote sessions")

	views, _ := d.pickerViews(current, nil)
	foundPickerOther := false
	foundPickerRemote := false
	for _, view := range views {
		if view.Name == "other" {
			foundPickerOther = true
		}
		if view.RemoteTarget != nil && view.RemoteHost == "user@arch" {
			foundPickerRemote = true
		}
	}
	require.True(t, foundPickerOther)
	require.True(t, foundPickerRemote)

	catalog, err := controlExec{d: d}.RemoteCatalog(true)
	require.NoError(t, err)
	require.Contains(t, catalog, `"other"`)
}

// TestSessionInventoryStoppedFiltering documents the intentional projection
// difference: palette shows only resumable sessions while picker and catalog
// observe every visible stopped session through the shared capture.
func TestSessionInventoryStoppedFiltering(t *testing.T) {
	tests := []struct {
		name       string
		projection string
	}{
		{name: "palette", projection: "palette"},
		{name: "picker", projection: "picker"},
		{name: "catalog", projection: "catalog"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Unix(2_000, 0)
			d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
			current := addControlSession(d, "current", "tab-1", "pane-1")
			current.ephemeral = false
			current.incarnation = domain.SessionLifecycleID{13}
			d.mu.Lock()
			d.inactive["resumable"] = inactiveSession{name: "resumable", cwd: "/tmp/resumable", createdAt: 1, incarnation: domain.IncarnationID{3}, state: protocol.SessionDown}
			d.inactive["broken"] = inactiveSession{name: "broken", cwd: "/tmp/broken", createdAt: 2, incarnation: domain.IncarnationID{4}, state: protocol.SessionBroken}
			d.mu.Unlock()

			inv := d.captureSessionInventory(viewOptions{}, false)
			require.NotEmpty(t, inv.visibleStopped())
			require.NotEmpty(t, inv.resumableStopped())
			require.GreaterOrEqual(t, len(inv.visibleStopped()), len(inv.resumableStopped()))

			switch tt.projection {
			case "palette":
				results := d.paletteResults(current, nil, testRecentRouteSnapshot())
				names := map[string]bool{}
				for _, result := range results {
					if name, ok := result.SessionName(); ok {
						names[name] = true
					}
				}
				require.True(t, names["resumable"])
				require.False(t, names["broken"])
			case "picker":
				views, _ := d.pickerViews(current, nil)
				names := map[string]bool{}
				for _, view := range views {
					names[view.Name] = true
				}
				require.True(t, names["resumable"])
				require.True(t, names["broken"])
			case "catalog":
				out, err := controlExec{d: d}.RemoteCatalog(true)
				require.NoError(t, err)
				require.Contains(t, out, `"resumable"`)
				require.Contains(t, out, `"broken"`)
			}
		})
	}
}
