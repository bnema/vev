package daemon

import (
	"encoding/hex"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
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

// snapshotNavigationInventory answers with this daemon's own sessions only:
// other daemons are observed by the client's broker, never relayed here.
func (d *Daemon) snapshotNavigationInventory(requestID uint64) protocol.NavigationInventoryResponse {
	return d.localNavigationInventorySnapshot(requestID)
}

// localNavigationInventorySnapshot is the prepared local-only navigation
// projection (Plan 001 P4.3): exactly one local source group, built from the
// daemon's own capture.
func (d *Daemon) localNavigationInventorySnapshot(requestID uint64) protocol.NavigationInventoryResponse {
	inv := d.captureLocalSessionInventory(viewOptions{}, false)
	response := protocol.NavigationInventoryResponse{
		RequestID: requestID, Operation: protocol.NavigationInventorySnapshot, Status: protocol.NavigationInventoryOK,
		Groups: []protocol.NavigationInventorySourceGroup{d.localInventoryGroup(inv)},
	}
	return fitNavigationInventoryResponse(response)
}

// localInventoryGroup projects live named sessions and resumable stopped
// sessions. Observation failures do not exist for local state; broken and
// purging records are excluded here because they are never resumable. It reads
// only the remote-free local capture.
func (d *Daemon) localInventoryGroup(inv localSessionInventory) protocol.NavigationInventorySourceGroup {
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
		target, ok := d.localNavigationResolve(request)
		if !ok {
			response.Status = protocol.NavigationInventoryUnavailable
			return response
		}
		response.Status = protocol.NavigationInventoryOK
		resolved := protocol.AttachTarget{Session: target.SessionName, Intent: protocol.IntentAttach, ExactTarget: &target}
		response.Resolved = &resolved
		return response
	}
	// Remote sources are the client broker's; this daemon resolves only its
	// own sessions.
	response.Status = protocol.NavigationInventoryUnavailable
	return response
}

// localNavigationResolve is the prepared local-only navigation resolver
// (Plan 001 P4.3). It rejects a structured foreign source key or registration
// before any lookup, so a local resolve can never be satisfied by a remote
// endpoint or a foreign registration even when a local session shares the
// row's name. Matching is by the opaque lifecycle-qualified key only: a stale
// or replaced lifecycle never falls back to a same-name session.
func (d *Daemon) localNavigationResolve(request protocol.NavigationInventoryRequest) (protocol.ExactSessionTarget, bool) {
	if request.SourceKey != protocol.NavigationInventoryLocalSourceKey || !request.Registration.IsZero() {
		return protocol.ExactSessionTarget{}, false
	}
	return localNavigationResolveEntry(d.captureLocalSessionInventory(viewOptions{}, false), request.EntryKey)
}

// localNavigationResolveEntry matches the opaque key against fresh live and
// resumable stopped state. A stopped target that is now live with the same
// lifecycle and selector stays attachable; a broken or purging record is not
// resumable and rejects; a changed lifecycle rejects.
func localNavigationResolveEntry(inv localSessionInventory, entryKey string) (protocol.ExactSessionTarget, bool) {
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
