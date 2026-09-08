package daemon

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// navigationInventoryEntryKey derives a stable opaque key for an unchanged
// private exact target (authority, lifecycle, name). Presentation-only
// updates keep the key; a changed or removed target retires it and a
// reappearing target gets a new key through its new lifecycle.
func navigationInventoryEntryKey(lifecycle domain.SessionLifecycleID, name string) string {
	return hex.EncodeToString(lifecycle[:]) + "/" + name
}

// handleNavigationInventory answers a one-shot source-control query without a
// Hello session, attachment, startup, PTY, or geometry effect. It closes the
// exact connection on every exit so blocked operations unwind.
func (d *Daemon) handleNavigationInventory(tr ports.ServerConnection, request protocol.NavigationInventoryRequest) {
	defer func() { _ = tr.Close() }()
	response := d.answerNavigationInventory(request)
	_ = d.boundedControlSend(tr, response)
}

// answerNavigationInventory builds the typed response for a validated or
// rejected inventory request.
func (d *Daemon) answerNavigationInventory(request protocol.NavigationInventoryRequest) protocol.NavigationInventoryResponse {
	if err := protocol.ValidateNavigationInventoryRequest(request); err != nil {
		return protocol.NavigationInventoryResponse{RequestID: request.RequestID, Operation: request.Operation, Status: protocol.NavigationInventoryInvalid}
	}
	if request.Version != protocol.Version {
		return protocol.NavigationInventoryResponse{RequestID: request.RequestID, Operation: request.Operation, Status: protocol.NavigationInventoryVersionMismatch}
	}
	if request.Operation == protocol.NavigationInventoryResolve {
		return d.resolveNavigationInventory(request)
	}
	return d.snapshotNavigationInventory(request.RequestID)
}

// snapshotNavigationInventory packs complete source groups, local first, then
// deterministic host order. A source exceeding its row limit becomes an
// explicit too_large row with no activatable entries; it never invalidates
// other complete groups. Encoded-byte overflow degrades the largest group to
// status-only instead of silently truncating.
func (d *Daemon) snapshotNavigationInventory(requestID uint64) protocol.NavigationInventoryResponse {
	inv := d.captureSessionInventory(viewOptions{}, false)
	groups := make([]protocol.NavigationInventorySourceGroup, 0, len(inv.hosts)+1)
	groups = append(groups, d.localInventoryGroup(inv))
	for _, host := range inv.hosts {
		groups = append(groups, remoteInventoryGroup(host))
	}
	if len(groups) > protocol.NavigationInventoryMaxSourceGroups {
		groups = groups[:protocol.NavigationInventoryMaxSourceGroups]
	}
	response := protocol.NavigationInventoryResponse{RequestID: requestID, Operation: protocol.NavigationInventorySnapshot, Status: protocol.NavigationInventoryOK, Groups: groups}
	return fitNavigationInventoryResponse(response)
}

// localInventoryGroup projects live named sessions and resumable stopped
// sessions. Observation failures do not exist for local state; broken
// records are excluded here because they are never resumable.
func (d *Daemon) localInventoryGroup(inv sessionInventory) protocol.NavigationInventorySourceGroup {
	group := protocol.NavigationInventorySourceGroup{SourceKey: protocol.NavigationInventoryLocalSourceKey, Status: protocol.NavigationInventorySourceOK}
	for _, item := range inv.live {
		view := item.view
		if view.name == "" || view.ephemeral {
			continue
		}
		group.Entries = append(group.Entries, protocol.NavigationInventoryEntry{
			SourceKey: group.SourceKey,
			EntryKey:  navigationInventoryEntryKey(view.incarnation, view.name),
			Name:      view.name,
			State:     "up",
		})
	}
	for _, entry := range inv.resumableStopped() {
		group.Entries = append(group.Entries, protocol.NavigationInventoryEntry{
			SourceKey: group.SourceKey,
			EntryKey:  navigationInventoryEntryKey(entry.incarnation, entry.name),
			Name:      entry.name,
			State:     "stopped",
		})
	}
	if len(group.Entries) > protocol.NavigationInventoryMaxLocalEntries {
		return protocol.NavigationInventorySourceGroup{SourceKey: group.SourceKey, Status: protocol.NavigationInventorySourceTooLarge}
	}
	return group
}

// remoteInventoryGroup projects one configured remote source with its own
// status. Broken sessions keep a diagnostic row; stale, unreachable,
// auth-failed, and malformed observations stay attemptable with a
// presentation reason, per the shared activation policy.
func remoteInventoryGroup(host ports.RemoteHostSnapshot) protocol.NavigationInventorySourceGroup {
	if host.Endpoint == "" {
		return protocol.NavigationInventorySourceGroup{Status: protocol.NavigationInventorySourceUnavailable}
	}
	// Remote source keys are opaque identifiers, never raw endpoints: the
	// relay forwards them to the serving daemon, which only ever sees
	// display origins. The endpoint hash keeps keys stable across polls
	// (so refreshes do not churn admissions) while a host literally named
	// "local" cannot collide with the reserved local source. Resolve maps
	// back through the request registration, never the key.
	group := protocol.NavigationInventorySourceGroup{SourceKey: remoteInventorySourceKey(host.Endpoint), Status: protocol.NavigationInventorySourceOK}
	eligible := 0
	for _, session := range host.Sessions {
		if session.Ephemeral {
			continue
		}
		eligible++
	}
	if eligible > protocol.NavigationInventoryMaxRemoteEntriesPerSource {
		return protocol.NavigationInventorySourceGroup{SourceKey: group.SourceKey, Status: protocol.NavigationInventorySourceTooLarge}
	}
	for _, session := range host.Sessions {
		if session.Ephemeral {
			continue
		}
		key, target := remoteCatalogSessionTarget(domain.RemoteSessionKey{Host: host.Endpoint, Name: session.Name}, session)
		if key.Validate() != nil {
			continue
		}
		reason := directorySessionReason(host, session, target)
		state := string(session.State)
		if session.State == catalogue.RemoteCatalogSessionBroken || target.Validate() != nil {
			state = string(catalogue.RemoteCatalogSessionBroken)
		}
		group.Entries = append(group.Entries, protocol.NavigationInventoryEntry{
			SourceKey:     group.SourceKey,
			EntryKey:      navigationInventoryEntryKey(session.LifecycleID, session.Name),
			Name:          session.Name,
			DisplayOrigin: key.DisplayOrigin,
			State:         state,
			Reason:        reason,
		})
	}
	if host.Availability != domain.RemoteAvailabilityReachable && len(group.Entries) == 0 {
		group.Status = protocol.NavigationInventorySourceUnavailable
	}
	return group
}

// remoteInventorySourceKey derives the stable opaque identifier for a
// configured remote endpoint. The hash domain-separates inventory keys so
// relayed identifiers disclose nothing about local SSH configuration.
func remoteInventorySourceKey(endpoint string) string {
	sum := sha256.Sum256([]byte("vev-inventory-remote\x00" + endpoint))
	return protocol.NavigationInventoryRemoteSourcePrefix + hex.EncodeToString(sum[:12])
}

// navigationInventoryEncodedBudget mirrors the wire export budget without
// importing the codec layer. The daemon never encodes frames itself; the
// estimate below upper-bounds the real payload so fit degrades early rather
// than emitting an oversized frame.
const navigationInventoryEncodedBudget = 4 << 20

func navigationInventorySnapshotSize(response protocol.NavigationInventoryResponse) int {
	size := 10 + 2
	for _, group := range response.Groups {
		size += 2 + len(group.SourceKey) + 1 + 4
		for _, entry := range group.Entries {
			size += 2 + len(entry.EntryKey) + 2 + len(entry.Name) + 2 + len(entry.DisplayOrigin) + 2 + len(entry.State) + 2 + len(entry.Reason)
		}
	}
	return size
}

// fitNavigationInventoryResponse degrades the largest complete group to
// status-only until the response fits the encoded budget. It never emits a
// silently truncated group or a claim of complete inventory.
func fitNavigationInventoryResponse(response protocol.NavigationInventoryResponse) protocol.NavigationInventoryResponse {
	if protocol.ValidateNavigationInventoryResponse(response) != nil {
		response.Groups = nil
		response.Status = protocol.NavigationInventoryInvalid
		return response
	}
	for range len(response.Groups) + 1 {
		if navigationInventorySnapshotSize(response) <= navigationInventoryEncodedBudget {
			return response
		}
		largest := -1
		for i, group := range response.Groups {
			if group.Status != protocol.NavigationInventorySourceOK || len(group.Entries) == 0 {
				continue
			}
			if largest == -1 || len(group.Entries) > len(response.Groups[largest].Entries) {
				largest = i
			}
		}
		if largest == -1 {
			response.Groups = nil
			response.Status = protocol.NavigationInventoryUnavailable
			return response
		}
		source := response.Groups[largest].SourceKey
		response.Groups[largest] = protocol.NavigationInventorySourceGroup{SourceKey: source, Status: protocol.NavigationInventorySourceTooLarge}
	}
	response.Groups = nil
	response.Status = protocol.NavigationInventoryUnavailable
	return response
}

// resolveNavigationInventory revalidates the selected key against a fresh
// capture and returns a non-mutating AttachTarget. Registration is checked at
// resolve: a removed or re-registered source rejects without attaching a
// same-name replacement.
func (d *Daemon) resolveNavigationInventory(request protocol.NavigationInventoryRequest) protocol.NavigationInventoryResponse {
	response := protocol.NavigationInventoryResponse{RequestID: request.RequestID, Operation: protocol.NavigationInventoryResolve}
	if request.SourceKey == protocol.NavigationInventoryLocalSourceKey {
		target, ok := d.resolveLocalInventoryEntry(request.EntryKey)
		if !ok {
			response.Status = protocol.NavigationInventoryUnavailable
			return response
		}
		response.Status = protocol.NavigationInventoryOK
		resolved := protocol.AttachTarget{Session: target.SessionName, Intent: protocol.IntentAttach, ExactTarget: &target}
		response.Resolved = &resolved
		return response
	}
	target, ok := d.resolveRemoteInventoryEntry(request)
	if !ok {
		response.Status = protocol.NavigationInventoryUnavailable
		return response
	}
	if target.Validate() != nil {
		response.Status = protocol.NavigationInventoryInvalid
		return response
	}
	response.Status = protocol.NavigationInventoryOK
	resolved := protocol.AttachTarget{
		Endpoint: request.Registration.Endpoint, Session: target.SessionName,
		Intent: protocol.IntentAttach, RemoteTarget: &target,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}
	response.Resolved = &resolved
	return response
}

// resolveLocalInventoryEntry matches the opaque key against fresh live and
// resumable stopped state. A stopped target that is now live with the same
// lifecycle and selector stays attachable; a changed lifecycle rejects.
func (d *Daemon) resolveLocalInventoryEntry(entryKey string) (protocol.ExactSessionTarget, bool) {
	inv := d.captureSessionInventory(viewOptions{}, false)
	for _, item := range inv.live {
		view := item.view
		if view.name == "" || view.ephemeral {
			continue
		}
		if navigationInventoryEntryKey(view.incarnation, view.name) != entryKey {
			continue
		}
		return protocol.ExactSessionTarget{LifecycleID: view.incarnation, SessionName: view.name}, true
	}
	for _, entry := range inv.resumableStopped() {
		if navigationInventoryEntryKey(entry.incarnation, entry.name) != entryKey {
			continue
		}
		return protocol.ExactSessionTarget{LifecycleID: entry.incarnation, SessionName: entry.name}, true
	}
	return protocol.ExactSessionTarget{}, false
}

// resolveRemoteInventoryEntry checks the stored registration identity, then
// matches the opaque key against the source's current sessions. Known valid
// cached targets with stale or failed observations stay attemptable;
// destination attach still validates exactly.
func (d *Daemon) resolveRemoteInventoryEntry(request protocol.NavigationInventoryRequest) (domain.RemoteSessionTarget, bool) {
	snapshot := d.remoteDirectorySnapshot()
	host, ok := snapshot.Find(request.Registration.Endpoint)
	if !ok || !host.Registration.Equal(request.Registration) {
		return domain.RemoteSessionTarget{}, false
	}
	for _, session := range host.Sessions {
		if session.Ephemeral {
			continue
		}
		if navigationInventoryEntryKey(session.LifecycleID, session.Name) != request.EntryKey {
			continue
		}
		_, target := remoteCatalogSessionTarget(domain.RemoteSessionKey{Host: host.Endpoint, Name: session.Name}, session)
		if session.State == catalogue.RemoteCatalogSessionBroken {
			return domain.RemoteSessionTarget{}, false
		}
		return target, true
	}
	return domain.RemoteSessionTarget{}, false
}
