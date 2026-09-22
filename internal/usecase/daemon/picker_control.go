package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
)

const controlPickerInteractionID uint64 = 1

// handlePickerControl serves the stable local authority without creating an
// attachment. The snapshot and resolve paths rebuild from the same daemon
// projection, so eligibility and opaque-key ownership never move client-side.
func (d *Daemon) handlePickerControl(tr ports.ServerConnection, request protocol.PickerControlRequest) {
	defer tr.Close()
	response := d.answerPickerControl(request)
	_ = d.boundedControlSend(tr, response)
}

func (d *Daemon) answerPickerControl(request protocol.PickerControlRequest) protocol.PickerControlResponse {
	response := protocol.PickerControlResponse{RequestID: request.RequestID, Operation: request.Operation, Status: protocol.PickerSourceUnavailable}
	if protocol.ValidatePickerControlRequest(request) != nil || request.Version != protocol.Version {
		return response
	}
	recentViews, groupedViews, _ := d.pickerViewProjections(nil, nil)
	recent := pickerLineSetFor(recentViews, protocol.PickerIntentNavigation, pickerSourceFilter{}, pickerSourceFilter{})
	grouped := pickerLineSetFor(groupedViews, protocol.PickerIntentNavigation, pickerSourceFilter{}, pickerSourceFilter{})
	// Resolution revalidates the opaque key against the current catalogue.
	// A constant revision avoids rejecting an otherwise current target merely
	// because unrelated presentation text changed after the client snapshot.
	revision := uint64(1)
	if request.Operation == protocol.PickerControlSnapshot {
		snapshot := protocol.PickerSnapshot{InteractionID: controlPickerInteractionID, SourceID: protocol.PickerHomeSourceID, SourceRevision: revision, Status: protocol.PickerSourceOK, Lines: recent.lines, Cursor: recent.cursor, Recent: protocol.PickerProjection{Lines: recent.lines, Cursor: recent.cursor}, Grouped: protocol.PickerProjection{Lines: grouped.lines, Cursor: grouped.cursor}}
		response.Status, response.Snapshot = protocol.PickerSourceOK, &snapshot
		return response
	}
	if request.Operation == protocol.PickerControlObserve {
		response.Status = protocol.PickerSourceOK
		response.Observations = d.observePickerRoutes(request.Targets)
		return response
	}
	if request.SourceID != protocol.PickerHomeSourceID {
		return response
	}
	target, ok := recent.keys[request.Key]
	if !ok || !d.pickerTargetCurrent(target) {
		return response
	}
	resolved, ok := d.pickerControlAttachTarget(target)
	if !ok {
		return response
	}
	response.Status, response.Resolved = protocol.PickerSourceOK, &resolved
	return response
}

func (d *Daemon) observePickerRoutes(targets []protocol.ExactSessionTarget) []protocol.PickerRouteObservation {
	observations := make([]protocol.PickerRouteObservation, len(targets))
	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	d.mu.Unlock()
	for i, target := range targets {
		observation := protocol.PickerRouteObservation{Target: target, Presence: protocol.PickerRouteAbsent}
		for _, sess := range sessions {
			view := sess.snapshotView(viewOptions{})
			if view.incarnation != target.LifecycleID || view.name != target.SessionName {
				continue
			}
			observation.Presence = protocol.PickerRoutePresent
			observation.Attention = view.hasAttention
			break
		}
		observations[i] = observation
	}
	return observations
}

func (d *Daemon) pickerControlAttachTarget(target picker.Target) (protocol.AttachTarget, bool) {
	// A remote target names an endpoint-qualified remote session, not a local
	// one. The structured route and lifecycle are revalidated against the
	// remote catalogue by remoteCatalogTargetReady; the local resolver never
	// sees a foreign target.
	if target.RemoteTarget != nil && target.RemoteKey != nil {
		remote := *target.RemoteTarget
		if !d.remoteCatalogTargetReady(remote) {
			return protocol.AttachTarget{}, false
		}
		return protocol.AttachTarget{Endpoint: remote.Endpoint, Session: remote.SessionName, Intent: protocol.IntentAttach, SessionTarget: ptrSessionAttachTarget(protocol.SessionAttachTargetFromRemote(remote)), EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, true
	}
	return d.pickerControlAttachTargetLocal(target)
}

// pickerControlAttachTargetLocal preserves the hybrid live behavior for a
// local target while keeping the prepared resolver strict. The picker
// snapshot's tab is a best-effort cursor, so a tab that vanished between the
// snapshot and the resolve must not fail the whole attach: the client's own
// missing-tab fallback would have attached the session's first tab anyway.
// The wrapper therefore retries once without a preferred tab, which can only
// succeed when the exact local lifecycle is still current, so a replaced or
// renamed session and a foreign target still reject.
func (d *Daemon) pickerControlAttachTargetLocal(target picker.Target) (protocol.AttachTarget, bool) {
	resolved, ok := d.localPickerControlAttachTarget(target)
	if ok || target.TabID == "" {
		return resolved, ok
	}
	target.TabID = ""
	return d.localPickerControlAttachTarget(target)
}

// localPickerControlAttachTarget is the prepared local-only control resolver
// (Plan 001 P4.3). It rejects a structured foreign target (remote route or
// remote session key) before any lookup, so a local resolve can never be
// satisfied by a foreign row. It then matches the exact session identity and
// lifecycle; a replaced lifecycle or a renamed session never falls back to a
// same-name session. A non-empty preferred tab must still exist in the current
// session, so a stale tab rejects instead of forwarding a dangling tab ID; the
// hybrid pickerControlAttachTarget retains its vanished-tab fallback on top of
// this strict check.
// Broken and purging stopped records are unreachable here: only live registry
// sessions are eligible.
func (d *Daemon) localPickerControlAttachTarget(target picker.Target) (protocol.AttachTarget, bool) {
	if target.RemoteTarget != nil || target.RemoteKey != nil {
		return protocol.AttachTarget{}, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	sess := d.sessions[target.Session]
	if sess == nil {
		return protocol.AttachTarget{}, false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !targetMatchesLifecycle(target, sess.name, sess.createdAt, sess.incarnation) {
		return protocol.AttachTarget{}, false
	}
	if target.TabID != "" && !sessionHasStableTabLocked(sess, target.TabID) {
		return protocol.AttachTarget{}, false
	}
	exact := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	return protocol.AttachTarget{Session: exact.SessionName, Intent: protocol.IntentAttach, ExactTarget: &exact, PreferredTabID: target.TabID, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, true
}

// sessionHasStableTabLocked reports whether an exact stable tab identity is
// still present on a session. Caller holds the session lock.
func sessionHasStableTabLocked(sess *session, tabID domain.TabStableID) bool {
	for _, tab := range sess.tabs {
		if tab != nil && tab.stableID == string(tabID) {
			return true
		}
	}
	return false
}
