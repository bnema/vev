package daemon

import (
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

func (d *Daemon) pickerControlAttachTarget(target picker.Target) (protocol.AttachTarget, bool) {
	if target.RemoteTarget != nil && target.RemoteKey != nil {
		remote := *target.RemoteTarget
		if !d.remoteCatalogTargetReady(remote) {
			return protocol.AttachTarget{}, false
		}
		return protocol.AttachTarget{Endpoint: remote.Endpoint, Session: remote.SessionName, Intent: protocol.IntentAttach, RemoteTarget: &remote, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	sess := d.sessions[target.Session]
	if sess == nil {
		return protocol.AttachTarget{}, false
	}
	sess.mu.Lock()
	exact := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	sess.mu.Unlock()
	return protocol.AttachTarget{Session: exact.SessionName, Intent: protocol.IntentAttach, ExactTarget: &exact, PreferredTabID: target.TabID, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, true
}
