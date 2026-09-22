package sessionwire

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func routeRefToWire(ref protocol.RouteRef) *wire.RouteRef {
	return &wire.RouteRef{Key: ref.Key, Generation: ref.Generation}
}

func routeRefFromWire(message *wire.RouteRef) protocol.RouteRef {
	if message == nil {
		return protocol.RouteRef{}
	}
	return protocol.RouteRef{Key: message.GetKey(), Generation: message.GetGeneration()}
}

func attentionTargetToWire(target protocol.RouteAttentionTarget) (*wire.RouteAttentionTarget, error) {
	if err := protocol.ValidateRouteLabel(target.SourceKey, true); err != nil {
		return nil, err
	}
	if err := target.Ref.Validate(); err != nil {
		return nil, err
	}
	if err := target.Target.Validate(); err != nil {
		return nil, err
	}
	return &wire.RouteAttentionTarget{Ref: routeRefToWire(target.Ref), Target: exactTargetToWire(&target.Target), SourceKey: target.SourceKey}, nil
}

func attentionTargetFromWire(message *wire.RouteAttentionTarget) (protocol.RouteAttentionTarget, error) {
	var target protocol.RouteAttentionTarget
	if message == nil {
		return target, protocol.ErrInvalidRouteWire
	}
	target.Ref = routeRefFromWire(message.GetRef())
	exact, err := exactTargetFromWire(message.GetTarget())
	if err != nil || exact == nil {
		return protocol.RouteAttentionTarget{}, protocol.ErrInvalidRouteWire
	}
	target.Target = *exact
	target.SourceKey = message.GetSourceKey()
	return target, nil
}

func attentionSubscriptionToWire(message protocol.RouteAttentionSubscription) (*wire.RouteAttentionSubscription, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	out := &wire.RouteAttentionSubscription{}
	for _, target := range message.Targets {
		converted, err := attentionTargetToWire(target)
		if err != nil {
			return nil, err
		}
		out.Targets = append(out.Targets, converted)
	}
	return out, nil
}

func recentRouteEntryToWire(entry protocol.RecentRouteEntry) (*wire.RecentRouteEntry, error) {
	return &wire.RecentRouteEntry{
		Key:          entry.Key,
		Generation:   entry.Generation,
		Target:       exactTargetToWire(&entry.Target),
		Name:         entry.Name,
		HostLabel:    entry.HostLabel,
		Kind:         uint32(entry.Kind),
		Ephemeral:    entry.Ephemeral,
		Attention:    entry.Attention,
		Reachability: uint32(entry.Reachability),
	}, nil
}

func recentRouteEntryFromWire(message *wire.RecentRouteEntry) (protocol.RecentRouteEntry, error) {
	var entry protocol.RecentRouteEntry
	if message == nil {
		return entry, protocol.ErrInvalidRouteWire
	}
	entry.Key = message.GetKey()
	entry.Generation = message.GetGeneration()
	target, err := exactTargetFromWire(message.GetTarget())
	if err != nil || target == nil {
		return protocol.RecentRouteEntry{}, protocol.ErrInvalidRouteWire
	}
	entry.Target = *target
	entry.Name = message.GetName()
	entry.HostLabel = message.GetHostLabel()
	entry.Kind, err = enum8[protocol.RouteKind](message.GetKind())
	if err != nil {
		return protocol.RecentRouteEntry{}, err
	}
	entry.Ephemeral = message.GetEphemeral()
	entry.Attention = message.GetAttention()
	entry.Reachability, err = enum8[protocol.RouteReachability](message.GetReachability())
	if err != nil {
		return protocol.RecentRouteEntry{}, err
	}
	return entry, nil
}

func navigationFailureToWire(failure protocol.RouteNavigationFailure) (*wire.RouteNavigationFailure, error) {
	if err := failure.Validate(); err != nil {
		return nil, err
	}
	return &wire.RouteNavigationFailure{Key: failure.Key, Generation: failure.Generation, Code: uint32(failure.Code)}, nil
}

func navigationFailureFromWire(message *wire.RouteNavigationFailure) (protocol.RouteNavigationFailure, error) {
	var failure protocol.RouteNavigationFailure
	if message == nil {
		return failure, protocol.ErrInvalidRouteWire
	}
	failure.Key = message.GetKey()
	failure.Generation = message.GetGeneration()
	code, err := enum8[protocol.RouteFailureCode](message.GetCode())
	if err != nil {
		return protocol.RouteNavigationFailure{}, err
	}
	failure.Code = code
	if err := failure.Validate(); err != nil {
		return protocol.RouteNavigationFailure{}, err
	}
	return failure, nil
}

func inventoryGroupsToWire(groups []protocol.NavigationInventorySourceGroup) ([]*wire.InventorySourceGroup, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	out := make([]*wire.InventorySourceGroup, 0, len(groups))
	for _, group := range groups {
		converted := &wire.InventorySourceGroup{SourceKey: group.SourceKey, Status: uint32(group.Status)}
		for _, entry := range group.Entries {
			converted.Entries = append(converted.Entries, &wire.InventoryEntry{
				SourceKey:     entry.SourceKey,
				EntryKey:      entry.EntryKey,
				Name:          entry.Name,
				DisplayOrigin: entry.DisplayOrigin,
				State:         entry.State,
				Reason:        entry.Reason,
			})
		}
		out = append(out, converted)
	}
	return out, nil
}

func inventoryGroupsFromWire(groups []*wire.InventorySourceGroup) ([]protocol.NavigationInventorySourceGroup, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	out := make([]protocol.NavigationInventorySourceGroup, 0, len(groups))
	for _, group := range groups {
		status, err := enum8[protocol.NavigationInventorySourceStatus](group.GetStatus())
		if err != nil {
			return nil, err
		}
		converted := protocol.NavigationInventorySourceGroup{SourceKey: group.GetSourceKey(), Status: status}
		for _, entry := range group.GetEntries() {
			converted.Entries = append(converted.Entries, protocol.NavigationInventoryEntry{
				SourceKey:     entry.GetSourceKey(),
				EntryKey:      entry.GetEntryKey(),
				Name:          entry.GetName(),
				DisplayOrigin: entry.GetDisplayOrigin(),
				State:         entry.GetState(),
				Reason:        entry.GetReason(),
			})
		}
		out = append(out, converted)
	}
	return out, nil
}

func inventoryRequestToWire(message protocol.NavigationInventoryRequest) (*wire.NavigationInventoryRequest, error) {
	if err := protocol.ValidateNavigationInventoryRequest(message); err != nil {
		return nil, err
	}
	var registration *wire.RemoteRegistration
	if !message.Registration.IsZero() {
		registration = registrationToWire(message.Registration)
	}
	return &wire.NavigationInventoryRequest{
		Version:      uint32(message.Version),
		RequestId:    message.RequestID,
		Operation:    uint32(message.Operation),
		SourceKey:    message.SourceKey,
		EntryKey:     message.EntryKey,
		Registration: registration,
	}, nil
}

func inventoryRequestFromWire(message *wire.NavigationInventoryRequest) (protocol.NavigationInventoryRequest, error) {
	var request protocol.NavigationInventoryRequest
	if message == nil {
		return request, protocol.ErrInvalidNavigation
	}
	version, err := mustUint16(message.GetVersion())
	if err != nil {
		return protocol.NavigationInventoryRequest{}, protocol.ErrInvalidNavigation
	}
	request.Version = version
	request.RequestID = message.GetRequestId()
	request.Operation, err = enum8[protocol.NavigationInventoryOperation](message.GetOperation())
	if err != nil {
		return protocol.NavigationInventoryRequest{}, protocol.ErrInvalidNavigation
	}
	request.SourceKey = message.GetSourceKey()
	request.EntryKey = message.GetEntryKey()
	registration, err := registrationFromWire(message.GetRegistration())
	if err != nil {
		return protocol.NavigationInventoryRequest{}, protocol.ErrInvalidNavigation
	}
	request.Registration = registration
	if request.Registration == (domain.RemoteRegistration{}) && message.GetRegistration() != nil {
		return protocol.NavigationInventoryRequest{}, protocol.ErrInvalidNavigation
	}
	if err := protocol.ValidateNavigationInventoryRequest(request); err != nil {
		return protocol.NavigationInventoryRequest{}, err
	}
	return request, nil
}

func pickerLineToWire(line protocol.PickerLine) *wire.PickerLine {
	return &wire.PickerLine{
		Key:          line.Key,
		Kind:         uint32(line.Kind),
		Label:        line.Label,
		Detail:       line.Detail,
		Status:       uint32(line.Status),
		StatusDetail: line.StatusDetail,
		Attention:    line.Attention,
		Stopped:      line.Stopped,
		Dim:          line.Dim,
		Focusable:    line.Focusable,
		Actions:      uint32(line.Actions),
		Ephemeral:    line.Ephemeral,
	}
}

func pickerLineFromWire(message *wire.PickerLine) (protocol.PickerLine, error) {
	var line protocol.PickerLine
	if message == nil {
		return line, protocol.ErrInvalidNavigation
	}
	line.Key = message.GetKey()
	kind, err := enum8[protocol.PickerLineKind](message.GetKind())
	if err != nil {
		return protocol.PickerLine{}, protocol.ErrInvalidNavigation
	}
	line.Kind = kind
	line.Label = message.GetLabel()
	line.Detail = message.GetDetail()
	status, err := enum8[protocol.PickerLineStatus](message.GetStatus())
	if err != nil {
		return protocol.PickerLine{}, protocol.ErrInvalidNavigation
	}
	line.Status = status
	line.StatusDetail = message.GetStatusDetail()
	line.Attention = message.GetAttention()
	line.Stopped = message.GetStopped()
	line.Dim = message.GetDim()
	line.Focusable = message.GetFocusable()
	actions, err := enum8[protocol.PickerLineActions](message.GetActions())
	if err != nil {
		return protocol.PickerLine{}, protocol.ErrInvalidNavigation
	}
	line.Actions = actions
	line.Ephemeral = message.GetEphemeral()
	return line, nil
}

func pickerCursorToWire(cursor protocol.PickerCursor) *wire.PickerCursor {
	return &wire.PickerCursor{Key: cursor.Key, Index: int64(cursor.Index)}
}

func pickerCursorFromWire(message *wire.PickerCursor) (protocol.PickerCursor, error) {
	var cursor protocol.PickerCursor
	if message == nil {
		return cursor, nil
	}
	index := message.GetIndex()
	if index < 0 || index > int64(int(^uint(0)>>1)) {
		return protocol.PickerCursor{}, errProtoConvertRange
	}
	return protocol.PickerCursor{Key: message.GetKey(), Index: int(index)}, nil
}

func pickerProjectionToWire(projection protocol.PickerProjection) (*wire.PickerProjection, error) {
	out := &wire.PickerProjection{Cursor: pickerCursorToWire(projection.Cursor)}
	for _, line := range projection.Lines {
		out.Lines = append(out.Lines, pickerLineToWire(line))
	}
	return out, nil
}

func pickerProjectionFromWire(message *wire.PickerProjection) (protocol.PickerProjection, error) {
	var projection protocol.PickerProjection
	if message == nil {
		return projection, nil
	}
	cursor, err := pickerCursorFromWire(message.GetCursor())
	if err != nil {
		return protocol.PickerProjection{}, err
	}
	projection.Cursor = cursor
	for _, line := range message.GetLines() {
		converted, err := pickerLineFromWire(line)
		if err != nil {
			return protocol.PickerProjection{}, err
		}
		projection.Lines = append(projection.Lines, converted)
	}
	return projection, nil
}

func pickerSnapshotToWire(snapshot protocol.PickerSnapshot) (*wire.PickerSnapshot, error) {
	snapshot = protocol.NormalizePickerSnapshot(snapshot)
	if err := protocol.ValidatePickerSnapshot(snapshot); err != nil {
		return nil, err
	}
	recent, err := pickerProjectionToWire(snapshot.Recent)
	if err != nil {
		return nil, err
	}
	grouped, err := pickerProjectionToWire(snapshot.Grouped)
	if err != nil {
		return nil, err
	}
	return &wire.PickerSnapshot{
		InteractionId:  snapshot.InteractionID,
		SourceId:       snapshot.SourceID,
		SourceRevision: snapshot.SourceRevision,
		Status:         uint32(snapshot.Status),
		StatusDetail:   snapshot.StatusDetail,
		Recent:         recent,
		Grouped:        grouped,
	}, nil
}

func pickerSnapshotFromWire(message *wire.PickerSnapshot) (protocol.PickerSnapshot, error) {
	var snapshot protocol.PickerSnapshot
	if message == nil {
		return snapshot, protocol.ErrInvalidNavigation
	}
	snapshot.InteractionID = message.GetInteractionId()
	snapshot.SourceID = message.GetSourceId()
	snapshot.SourceRevision = message.GetSourceRevision()
	status, err := enum8[protocol.PickerSourceStatus](message.GetStatus())
	if err != nil {
		return protocol.PickerSnapshot{}, err
	}
	snapshot.Status = status
	snapshot.StatusDetail = message.GetStatusDetail()
	recent, err := pickerProjectionFromWire(message.GetRecent())
	if err != nil {
		return protocol.PickerSnapshot{}, err
	}
	snapshot.Recent = recent
	grouped, err := pickerProjectionFromWire(message.GetGrouped())
	if err != nil {
		return protocol.PickerSnapshot{}, err
	}
	snapshot.Grouped = grouped
	snapshot = protocol.NormalizePickerSnapshot(snapshot)
	if err := protocol.ValidatePickerSnapshot(snapshot); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	// Lines/Cursor are construction aliases: repopulate them from the
	// normalized projections so decoded snapshots behave like the
	// snapshots the daemon built before encoding.
	snapshot.Lines = append([]protocol.PickerLine(nil), snapshot.Recent.Lines...)
	snapshot.Cursor = snapshot.Recent.Cursor
	return snapshot, nil
}
