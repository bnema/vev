package sessionwire

import (
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func routeNavigationActionToWire(message protocol.RouteNavigationAction) (*wire.RouteNavigationAction, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return &wire.RouteNavigationAction{
		CauseActionId:      message.CauseActionID,
		SnapshotGeneration: message.SnapshotGeneration,
		Key:                message.Key,
		Generation:         message.Generation,
	}, nil
}

func routeNavigationActionFromWire(message *wire.RouteNavigationAction) (protocol.RouteNavigationAction, error) {
	if message == nil {
		return protocol.RouteNavigationAction{}, protocol.ErrInvalidRouteWire
	}
	action := protocol.RouteNavigationAction{
		CauseActionID:      message.GetCauseActionId(),
		SnapshotGeneration: message.GetSnapshotGeneration(),
		Key:                message.GetKey(),
		Generation:         message.GetGeneration(),
	}
	if err := action.Validate(); err != nil {
		return protocol.RouteNavigationAction{}, err
	}
	return action, nil
}

func routeCreateSessionActionToWire(message protocol.RouteCreateSessionAction) (*wire.RouteCreateSessionAction, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return &wire.RouteCreateSessionAction{
		CauseActionId:      message.CauseActionID,
		RequestId:          message.RequestID,
		SnapshotGeneration: message.SnapshotGeneration,
		Key:                message.Key,
		Generation:         message.Generation,
		SessionName:        message.SessionName,
	}, nil
}

func routeCreateSessionActionFromWire(message *wire.RouteCreateSessionAction) (protocol.RouteCreateSessionAction, error) {
	if message == nil {
		return protocol.RouteCreateSessionAction{}, protocol.ErrInvalidRouteWire
	}
	action := protocol.RouteCreateSessionAction{
		CauseActionID:      message.GetCauseActionId(),
		RequestID:          message.GetRequestId(),
		SnapshotGeneration: message.GetSnapshotGeneration(),
		Key:                message.GetKey(),
		Generation:         message.GetGeneration(),
		SessionName:        message.GetSessionName(),
	}
	if err := action.Validate(); err != nil {
		return protocol.RouteCreateSessionAction{}, err
	}
	return action, nil
}

func routeRetiredToWire(message protocol.RouteRetired) (*wire.RouteRetired, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return &wire.RouteRetired{Ref: routeRefToWire(message.Ref), Target: exactTargetToWire(&message.Target)}, nil
}

func routeRetiredFromWire(message *wire.RouteRetired) (protocol.RouteRetired, error) {
	if message == nil {
		return protocol.RouteRetired{}, protocol.ErrInvalidRouteWire
	}
	target, err := exactTargetFromWire(message.GetTarget())
	if err != nil || target == nil {
		return protocol.RouteRetired{}, protocol.ErrInvalidRouteWire
	}
	retired := protocol.RouteRetired{Ref: routeRefFromWire(message.GetRef()), Target: *target}
	if err := retired.Validate(); err != nil {
		return protocol.RouteRetired{}, err
	}
	return retired, nil
}

func routePositionToWire(message protocol.RoutePosition) (*wire.RoutePosition, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return &wire.RoutePosition{Target: exactTargetToWire(&message.Target), ActiveTabId: string(message.ActiveTabID)}, nil
}

func routePositionFromWire(message *wire.RoutePosition) (protocol.RoutePosition, error) {
	if message == nil {
		return protocol.RoutePosition{}, protocol.ErrInvalidRouteWire
	}
	target, err := exactTargetFromWire(message.GetTarget())
	if err != nil || target == nil {
		return protocol.RoutePosition{}, protocol.ErrInvalidRouteWire
	}
	position := protocol.RoutePosition{Target: *target, ActiveTabID: mustTabIDString(message.GetActiveTabId())}
	if err := position.Validate(); err != nil {
		return protocol.RoutePosition{}, err
	}
	return position, nil
}

func samePeerSwitchRequestToWire(message protocol.SamePeerSwitchRequest) (*wire.SamePeerSwitchRequest, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return &wire.SamePeerSwitchRequest{RequestId: message.RequestID, Target: exactTargetToWire(&message.Target), PreferredTabId: string(message.PreferredTabID)}, nil
}

func samePeerSwitchRequestFromWire(message *wire.SamePeerSwitchRequest) (protocol.SamePeerSwitchRequest, error) {
	if message == nil {
		return protocol.SamePeerSwitchRequest{}, protocol.ErrInvalidRouteWire
	}
	target, err := exactTargetFromWire(message.GetTarget())
	if err != nil || target == nil {
		return protocol.SamePeerSwitchRequest{}, protocol.ErrInvalidRouteWire
	}
	request := protocol.SamePeerSwitchRequest{RequestID: message.GetRequestId(), Target: *target, PreferredTabID: mustTabIDString(message.GetPreferredTabId())}
	if err := request.Validate(); err != nil {
		return protocol.SamePeerSwitchRequest{}, err
	}
	return request, nil
}

func samePeerSwitchFailureToWire(message protocol.SamePeerSwitchFailure) (*wire.SamePeerSwitchFailure, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return &wire.SamePeerSwitchFailure{RequestId: message.RequestID, Code: uint32(message.Code)}, nil
}

func samePeerSwitchFailureFromWire(message *wire.SamePeerSwitchFailure) (protocol.SamePeerSwitchFailure, error) {
	if message == nil {
		return protocol.SamePeerSwitchFailure{}, protocol.ErrInvalidRouteWire
	}
	code, err := enum8[protocol.SamePeerSwitchFailureCode](message.GetCode())
	if err != nil {
		return protocol.SamePeerSwitchFailure{}, protocol.ErrInvalidRouteWire
	}
	failure := protocol.SamePeerSwitchFailure{RequestID: message.GetRequestId(), Code: code}
	if err := failure.Validate(); err != nil {
		return protocol.SamePeerSwitchFailure{}, err
	}
	return failure, nil
}

func recentRouteSnapshotToWire(message protocol.RecentRouteSnapshot) (*wire.RecentRouteSnapshot, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	activeEntry, err := recentRouteEntryToWire(message.ActiveEntry)
	if err != nil {
		return nil, err
	}
	out := &wire.RecentRouteSnapshot{
		Generation:  message.Generation,
		Active:      routeRefToWire(message.Active),
		ActiveEntry: activeEntry,
		Previous:    routeRefToWire(message.Previous),
		Home:        routeRefToWire(message.Home),
	}
	for _, entry := range message.Entries {
		converted, err := recentRouteEntryToWire(entry)
		if err != nil {
			return nil, err
		}
		out.Entries = append(out.Entries, converted)
	}
	return out, nil
}

func recentRouteSnapshotFromWire(message *wire.RecentRouteSnapshot) (protocol.RecentRouteSnapshot, error) {
	var snapshot protocol.RecentRouteSnapshot
	if message == nil {
		return snapshot, protocol.ErrInvalidRouteWire
	}
	snapshot.Generation = message.GetGeneration()
	snapshot.Active = routeRefFromWire(message.GetActive())
	activeEntry, err := recentRouteEntryFromWire(message.GetActiveEntry())
	if err != nil {
		return protocol.RecentRouteSnapshot{}, err
	}
	snapshot.ActiveEntry = activeEntry
	snapshot.Previous = routeRefFromWire(message.GetPrevious())
	snapshot.Home = routeRefFromWire(message.GetHome())
	for _, entry := range message.GetEntries() {
		converted, err := recentRouteEntryFromWire(entry)
		if err != nil {
			return protocol.RecentRouteSnapshot{}, err
		}
		snapshot.Entries = append(snapshot.Entries, converted)
	}
	if err := snapshot.Validate(); err != nil {
		return protocol.RecentRouteSnapshot{}, err
	}
	return snapshot, nil
}

func sessionCreationFailureToWire(message protocol.SessionCreationFailure) (*wire.SessionCreationFailure, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return &wire.SessionCreationFailure{RequestId: message.RequestID, Code: uint32(message.Code)}, nil
}

func sessionCreationFailureFromWire(message *wire.SessionCreationFailure) (protocol.SessionCreationFailure, error) {
	if message == nil {
		return protocol.SessionCreationFailure{}, protocol.ErrInvalidRouteWire
	}
	code, err := enum8[protocol.RouteFailureCode](message.GetCode())
	if err != nil {
		return protocol.SessionCreationFailure{}, protocol.ErrInvalidRouteWire
	}
	failure := protocol.SessionCreationFailure{RequestID: message.GetRequestId(), Code: code}
	if err := failure.Validate(); err != nil {
		return protocol.SessionCreationFailure{}, err
	}
	return failure, nil
}

func attentionSubscriptionFromWire(message *wire.RouteAttentionSubscription) (protocol.RouteAttentionSubscription, error) {
	var subscription protocol.RouteAttentionSubscription
	if message == nil {
		return subscription, nil
	}
	for _, target := range message.GetTargets() {
		converted, err := attentionTargetFromWire(target)
		if err != nil {
			return protocol.RouteAttentionSubscription{}, err
		}
		subscription.Targets = append(subscription.Targets, converted)
	}
	if err := subscription.Validate(); err != nil {
		return protocol.RouteAttentionSubscription{}, err
	}
	return subscription, nil
}

func inventoryResponseToWire(message protocol.NavigationInventoryResponse) (*wire.NavigationInventoryResponse, error) {
	if err := protocol.ValidateNavigationInventoryResponse(message); err != nil {
		return nil, err
	}
	groups, err := inventoryGroupsToWire(message.Groups)
	if err != nil {
		return nil, err
	}
	var resolved *wire.AttachTarget
	if message.Resolved != nil {
		converted, err := attachTargetToWire(*message.Resolved)
		if err != nil {
			return nil, err
		}
		resolved = converted
	}
	return &wire.NavigationInventoryResponse{
		RequestId: message.RequestID,
		Operation: uint32(message.Operation),
		Status:    uint32(message.Status),
		Groups:    groups,
		Resolved:  resolved,
	}, nil
}

func inventoryResponseFromWire(message *wire.NavigationInventoryResponse) (protocol.NavigationInventoryResponse, error) {
	var response protocol.NavigationInventoryResponse
	if message == nil {
		return response, protocol.ErrInvalidNavigation
	}
	response.RequestID = message.GetRequestId()
	operation, err := enum8[protocol.NavigationInventoryOperation](message.GetOperation())
	if err != nil {
		return protocol.NavigationInventoryResponse{}, protocol.ErrInvalidNavigation
	}
	response.Operation = operation
	status, err := enum8[protocol.NavigationInventoryStatus](message.GetStatus())
	if err != nil {
		return protocol.NavigationInventoryResponse{}, protocol.ErrInvalidNavigation
	}
	response.Status = status
	groups, err := inventoryGroupsFromWire(message.GetGroups())
	if err != nil {
		return protocol.NavigationInventoryResponse{}, err
	}
	response.Groups = groups
	if message.GetResolved() != nil {
		resolved, err := attachTargetFromWire(message.GetResolved())
		if err != nil {
			return protocol.NavigationInventoryResponse{}, err
		}
		response.Resolved = &resolved
	}
	if err := protocol.ValidateNavigationInventoryResponse(response); err != nil {
		return protocol.NavigationInventoryResponse{}, err
	}
	return response, nil
}

func inventoryDemandToWire(message protocol.NavigationInventoryDemand) (*wire.NavigationInventoryDemand, error) {
	if err := protocol.ValidateNavigationInventoryDemand(message); err != nil {
		return nil, err
	}
	return &wire.NavigationInventoryDemand{InteractionGeneration: message.InteractionGeneration, Open: message.Open}, nil
}

func inventoryDemandFromWire(message *wire.NavigationInventoryDemand) (protocol.NavigationInventoryDemand, error) {
	if message == nil {
		return protocol.NavigationInventoryDemand{}, protocol.ErrInvalidNavigation
	}
	demand := protocol.NavigationInventoryDemand{InteractionGeneration: message.GetInteractionGeneration(), Open: message.GetOpen()}
	if err := protocol.ValidateNavigationInventoryDemand(demand); err != nil {
		return protocol.NavigationInventoryDemand{}, err
	}
	return demand, nil
}

func inventorySelectionToWire(message protocol.NavigationInventorySelection) (*wire.NavigationInventorySelection, error) {
	if err := protocol.ValidateNavigationInventorySelection(message); err != nil {
		return nil, err
	}
	return &wire.NavigationInventorySelection{
		CauseActionId:         message.CauseActionID,
		InteractionGeneration: message.InteractionGeneration,
		PublicationGeneration: message.PublicationGeneration,
		SourceKey:             message.SourceKey,
		EntryKey:              message.EntryKey,
	}, nil
}

func inventorySelectionFromWire(message *wire.NavigationInventorySelection) (protocol.NavigationInventorySelection, error) {
	if message == nil {
		return protocol.NavigationInventorySelection{}, protocol.ErrInvalidNavigation
	}
	selection := protocol.NavigationInventorySelection{
		CauseActionID:         message.GetCauseActionId(),
		InteractionGeneration: message.GetInteractionGeneration(),
		PublicationGeneration: message.GetPublicationGeneration(),
		SourceKey:             message.GetSourceKey(),
		EntryKey:              message.GetEntryKey(),
	}
	if err := protocol.ValidateNavigationInventorySelection(selection); err != nil {
		return protocol.NavigationInventorySelection{}, err
	}
	return selection, nil
}

func inventoryPublicationToWire(message protocol.NavigationInventoryPublication) (*wire.NavigationInventoryPublication, error) {
	if err := protocol.ValidateNavigationInventoryPublication(message); err != nil {
		return nil, err
	}
	groups, err := inventoryGroupsToWire(message.Groups)
	if err != nil {
		return nil, err
	}
	return &wire.NavigationInventoryPublication{InteractionGeneration: message.InteractionGeneration, PublicationGeneration: message.PublicationGeneration, Groups: groups}, nil
}

func inventoryPublicationFromWire(message *wire.NavigationInventoryPublication) (protocol.NavigationInventoryPublication, error) {
	if message == nil {
		return protocol.NavigationInventoryPublication{}, protocol.ErrInvalidNavigation
	}
	publication := protocol.NavigationInventoryPublication{InteractionGeneration: message.GetInteractionGeneration(), PublicationGeneration: message.GetPublicationGeneration()}
	groups, err := inventoryGroupsFromWire(message.GetGroups())
	if err != nil {
		return protocol.NavigationInventoryPublication{}, err
	}
	publication.Groups = groups
	if err := protocol.ValidateNavigationInventoryPublication(publication); err != nil {
		return protocol.NavigationInventoryPublication{}, err
	}
	return publication, nil
}

func inventoryFailureToWire(message protocol.NavigationInventoryFailure) (*wire.NavigationInventoryFailure, error) {
	if err := protocol.ValidateNavigationInventoryFailure(message); err != nil {
		return nil, err
	}
	return &wire.NavigationInventoryFailure{
		CauseActionId:         message.CauseActionID,
		InteractionGeneration: message.InteractionGeneration,
		SourceKey:             message.SourceKey,
		EntryKey:              message.EntryKey,
		Code:                  uint32(message.Code),
	}, nil
}

func inventoryFailureFromWire(message *wire.NavigationInventoryFailure) (protocol.NavigationInventoryFailure, error) {
	if message == nil {
		return protocol.NavigationInventoryFailure{}, protocol.ErrInvalidNavigation
	}
	code, err := enum8[protocol.NavigationInventoryFailureCode](message.GetCode())
	if err != nil {
		return protocol.NavigationInventoryFailure{}, protocol.ErrInvalidNavigation
	}
	failure := protocol.NavigationInventoryFailure{
		CauseActionID:         message.GetCauseActionId(),
		InteractionGeneration: message.GetInteractionGeneration(),
		SourceKey:             message.GetSourceKey(),
		EntryKey:              message.GetEntryKey(),
		Code:                  code,
	}
	if err := protocol.ValidateNavigationInventoryFailure(failure); err != nil {
		return protocol.NavigationInventoryFailure{}, err
	}
	return failure, nil
}
