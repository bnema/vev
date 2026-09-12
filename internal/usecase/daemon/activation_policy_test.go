package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// TestActivationPolicyTable drives the D5 contract through the picker
// projection (not a pure helper): broken and incompatible rows are not
// activatable but keep their diagnostic representation, while known valid
// cached targets with stale or failed observations stay attemptable with a
// presentation reason. Destination attach still validates exactly.
func TestActivationPolicyTable(t *testing.T) {
	now := time.Unix(1_000, 0)
	lifecycle := domain.SessionLifecycleID{21}
	tabs := []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "shell"}}

	tests := []struct {
		name       string
		host       ports.RemoteHostSnapshot
		session    catalogue.RemoteCatalogSession
		activation pickerRemoteActivation
		reason     string
		rowPresent bool
	}{
		{
			name:       "live reachable attaches",
			host:       reachableDirectoryHost("user@arch", now),
			session:    catalogue.RemoteCatalogSession{LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionUp, Tabs: tabs, ActiveTabID: "tab-1"},
			activation: pickerRemoteAttach,
			rowPresent: true,
		},
		{
			name:       "stopped reachable restarts",
			host:       reachableDirectoryHost("user@arch", now),
			session:    catalogue.RemoteCatalogSession{LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionDown, Tabs: tabs},
			activation: pickerRemoteRestart,
			rowPresent: true,
		},
		{
			name:       "broken is diagnostic only",
			host:       reachableDirectoryHost("user@arch", now),
			session:    catalogue.RemoteCatalogSession{LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionBroken, Tabs: tabs},
			activation: pickerRemoteUnavailable,
			reason:     domain.RemoteReasonSessionBroken,
			rowPresent: true,
		},
		{
			name: "incompatible is diagnostic only",
			host: ports.RemoteHostSnapshot{
				Endpoint: "user@arch", Availability: domain.RemoteAvailabilityIncompatible,
				LastSuccess: now, InventoryKnown: true,
			},
			session:    catalogue.RemoteCatalogSession{LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionUp, Tabs: tabs, ActiveTabID: "tab-1"},
			activation: pickerRemoteUnavailable,
			reason:     domain.RemoteReasonVersionMismatch,
			rowPresent: true,
		},
		{
			name: "stale inventory stays attemptable",
			host: func() ports.RemoteHostSnapshot {
				host := reachableDirectoryHost("user@arch", now.Add(-time.Hour))
				return host
			}(),
			session:    catalogue.RemoteCatalogSession{LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionUp, Tabs: tabs, ActiveTabID: "tab-1"},
			activation: pickerRemoteAttach,
			rowPresent: true,
		},
		{
			name: "unreachable cached stays attemptable with reason",
			host: ports.RemoteHostSnapshot{
				Endpoint: "user@arch", Availability: domain.RemoteAvailabilityUnreachable,
				LastSuccess: now.Add(-time.Hour), InventoryKnown: true,
			},
			session:    catalogue.RemoteCatalogSession{LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionUp, Tabs: tabs, ActiveTabID: "tab-1"},
			activation: pickerRemoteAttach,
			reason:     domain.RemoteReasonHostUnreachable,
			rowPresent: true,
		},
		{
			name: "auth failure stays attemptable with reason",
			host: ports.RemoteHostSnapshot{
				Endpoint: "user@arch", Availability: domain.RemoteAvailabilityAuthFailed,
				LastSuccess: now.Add(-time.Hour), InventoryKnown: true,
			},
			session:    catalogue.RemoteCatalogSession{LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionUp, Tabs: tabs, ActiveTabID: "tab-1"},
			activation: pickerRemoteAttach,
			reason:     domain.RemoteReasonAuthFailure,
			rowPresent: true,
		},
		{
			name: "malformed catalog stays attemptable with reason",
			host: ports.RemoteHostSnapshot{
				Endpoint: "user@arch", Availability: domain.RemoteAvailabilityInvalidResponse,
				LastSuccess: now.Add(-time.Hour), InventoryKnown: true,
			},
			session:    catalogue.RemoteCatalogSession{LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionUp, Tabs: tabs, ActiveTabID: "tab-1"},
			activation: pickerRemoteAttach,
			reason:     domain.RemoteReasonMalformed,
			rowPresent: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, fixedRemoteRefreshClock{now: now})
			current := addControlSession(d, "current", "tab-1", "pane-1")
			current.ephemeral = false
			host := tt.host
			host.Sessions = []catalogue.RemoteCatalogSession{tt.session}
			seedRemoteDirectory(t, d, host)

			views, _ := d.pickerViews(current, nil)
			var found *pickerSessionView
			for i, view := range views {
				if view.RemoteHost == "user@arch" && view.Name == "work@arch" {
					found = &views[i]
				}
			}
			require.NotNil(t, found, "row must be present for exact-origin diagnosis")
			require.Equal(t, tt.activation, found.RemoteActivation)
			if tt.reason != "" {
				require.Equal(t, tt.reason, found.RemoteReason)
			}
		})
	}
}
