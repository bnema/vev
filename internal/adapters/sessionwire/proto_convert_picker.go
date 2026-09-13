package sessionwire

import (
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func pickerOfferToWire(message protocol.PickerOffer) (*wire.PickerOffer, error) {
	if err := protocol.ValidatePickerOffer(message); err != nil {
		return nil, err
	}
	return &wire.PickerOffer{
		InteractionId: message.InteractionID,
		RequestId:     message.RequestID,
		Intent:        uint32(message.Intent),
		MoveSourceKey: message.MoveSourceKey,
		BarrierEpoch:  message.BarrierEpoch,
		BarrierState:  message.BarrierState,
		SizeEpoch:     message.SizeEpoch,
		Title:         message.Title,
	}, nil
}

func pickerOfferFromWire(message *wire.PickerOffer) (protocol.PickerOffer, error) {
	if message == nil {
		return protocol.PickerOffer{}, protocol.ErrInvalidNavigation
	}
	intent, err := enum8[protocol.PickerIntent](message.GetIntent())
	if err != nil {
		return protocol.PickerOffer{}, err
	}
	offer := protocol.PickerOffer{
		InteractionID: message.GetInteractionId(),
		RequestID:     message.GetRequestId(),
		Intent:        intent,
		MoveSourceKey: message.GetMoveSourceKey(),
		BarrierEpoch:  message.GetBarrierEpoch(),
		BarrierState:  message.GetBarrierState(),
		SizeEpoch:     message.GetSizeEpoch(),
		Title:         message.GetTitle(),
	}
	if err := protocol.ValidatePickerOffer(offer); err != nil {
		return protocol.PickerOffer{}, err
	}
	return offer, nil
}

func pickerBeginToWire(message protocol.PickerBegin) (*wire.PickerBegin, error) {
	if err := protocol.ValidatePickerBegin(message); err != nil {
		return nil, err
	}
	return &wire.PickerBegin{RequestId: message.RequestID, Intent: uint32(message.Intent)}, nil
}

func pickerBeginFromWire(message *wire.PickerBegin) (protocol.PickerBegin, error) {
	if message == nil {
		return protocol.PickerBegin{}, protocol.ErrInvalidNavigation
	}
	intent, err := enum8[protocol.PickerIntent](message.GetIntent())
	if err != nil {
		return protocol.PickerBegin{}, err
	}
	begin := protocol.PickerBegin{RequestID: message.GetRequestId(), Intent: intent}
	if err := protocol.ValidatePickerBegin(begin); err != nil {
		return protocol.PickerBegin{}, err
	}
	return begin, nil
}

func pickerCloseToWire(message protocol.PickerClose) (*wire.PickerClose, error) {
	if err := protocol.ValidatePickerClose(message); err != nil {
		return nil, err
	}
	return &wire.PickerClose{InteractionId: message.InteractionID, RequestId: message.RequestID}, nil
}

func pickerCloseFromWire(message *wire.PickerClose) (protocol.PickerClose, error) {
	if message == nil {
		return protocol.PickerClose{}, protocol.ErrInvalidNavigation
	}
	closed := protocol.PickerClose{InteractionID: message.GetInteractionId(), RequestID: message.GetRequestId()}
	if err := protocol.ValidatePickerClose(closed); err != nil {
		return protocol.PickerClose{}, err
	}
	return closed, nil
}

func pickerClosedToWire(message protocol.PickerClosed) (*wire.PickerClosed, error) {
	if err := protocol.ValidatePickerClosed(message); err != nil {
		return nil, err
	}
	return &wire.PickerClosed{InteractionId: message.InteractionID, BarrierEpoch: message.BarrierEpoch, BarrierState: message.BarrierState}, nil
}

func pickerClosedFromWire(message *wire.PickerClosed) (protocol.PickerClosed, error) {
	if message == nil {
		return protocol.PickerClosed{}, protocol.ErrInvalidNavigation
	}
	closed := protocol.PickerClosed{InteractionID: message.GetInteractionId(), BarrierEpoch: message.GetBarrierEpoch(), BarrierState: message.GetBarrierState()}
	if err := protocol.ValidatePickerClosed(closed); err != nil {
		return protocol.PickerClosed{}, err
	}
	return closed, nil
}

func pickerSelectionToWire(message protocol.PickerSelection) (*wire.PickerSelection, error) {
	if err := protocol.ValidatePickerSelection(message); err != nil {
		return nil, err
	}
	return &wire.PickerSelection{
		CauseActionId:  message.CauseActionID,
		RequestId:      message.RequestID,
		InteractionId:  message.InteractionID,
		SourceId:       message.SourceID,
		SourceRevision: message.SourceRevision,
		Key:            message.Key,
		Action:         uint32(message.Action),
	}, nil
}

func pickerSelectionFromWire(message *wire.PickerSelection) (protocol.PickerSelection, error) {
	if message == nil {
		return protocol.PickerSelection{}, protocol.ErrInvalidNavigation
	}
	action, err := enum8[protocol.PickerAction](message.GetAction())
	if err != nil {
		return protocol.PickerSelection{}, err
	}
	selection := protocol.PickerSelection{
		CauseActionID:  message.GetCauseActionId(),
		RequestID:      message.GetRequestId(),
		InteractionID:  message.GetInteractionId(),
		SourceID:       message.GetSourceId(),
		SourceRevision: message.GetSourceRevision(),
		Key:            message.GetKey(),
		Action:         action,
	}
	if err := protocol.ValidatePickerSelection(selection); err != nil {
		return protocol.PickerSelection{}, err
	}
	return selection, nil
}

func pickerResultToWire(message protocol.PickerResult) (*wire.PickerResult, error) {
	if err := protocol.ValidatePickerResult(message); err != nil {
		return nil, err
	}
	return &wire.PickerResult{
		CauseActionId: message.CauseActionID,
		RequestId:     message.RequestID,
		InteractionId: message.InteractionID,
		SourceId:      message.SourceID,
		Key:           message.Key,
		Action:        uint32(message.Action),
	}, nil
}

func pickerResultFromWire(message *wire.PickerResult) (protocol.PickerResult, error) {
	if message == nil {
		return protocol.PickerResult{}, protocol.ErrInvalidNavigation
	}
	action, err := enum8[protocol.PickerAction](message.GetAction())
	if err != nil {
		return protocol.PickerResult{}, err
	}
	result := protocol.PickerResult{
		CauseActionID: message.GetCauseActionId(),
		RequestID:     message.GetRequestId(),
		InteractionID: message.GetInteractionId(),
		SourceID:      message.GetSourceId(),
		Key:           message.GetKey(),
		Action:        action,
	}
	if err := protocol.ValidatePickerResult(result); err != nil {
		return protocol.PickerResult{}, err
	}
	return result, nil
}

func pickerFailureToWire(message protocol.PickerFailure) (*wire.PickerFailure, error) {
	if err := protocol.ValidatePickerFailure(message); err != nil {
		return nil, err
	}
	return &wire.PickerFailure{
		CauseActionId: message.CauseActionID,
		RequestId:     message.RequestID,
		InteractionId: message.InteractionID,
		SourceId:      message.SourceID,
		Key:           message.Key,
		Action:        uint32(message.Action),
		Code:          uint32(message.Code),
	}, nil
}

func pickerFailureFromWire(message *wire.PickerFailure) (protocol.PickerFailure, error) {
	if message == nil {
		return protocol.PickerFailure{}, protocol.ErrInvalidNavigation
	}
	action, err := enum8[protocol.PickerAction](message.GetAction())
	if err != nil {
		return protocol.PickerFailure{}, err
	}
	code, err := enum8[protocol.PickerFailureCode](message.GetCode())
	if err != nil {
		return protocol.PickerFailure{}, err
	}
	failure := protocol.PickerFailure{
		CauseActionID: message.GetCauseActionId(),
		RequestID:     message.GetRequestId(),
		InteractionID: message.GetInteractionId(),
		SourceID:      message.GetSourceId(),
		Key:           message.GetKey(),
		Action:        action,
		Code:          code,
	}
	if err := protocol.ValidatePickerFailure(failure); err != nil {
		return protocol.PickerFailure{}, err
	}
	return failure, nil
}

func pickerControlResponseToWire(message protocol.PickerControlResponse) (*wire.PickerControlResponse, error) {
	if err := protocol.ValidatePickerControlResponse(message); err != nil {
		return nil, err
	}
	out := &wire.PickerControlResponse{RequestId: message.RequestID, Operation: uint32(message.Operation), Status: uint32(message.Status)}
	if message.Snapshot != nil {
		snapshot, err := pickerSnapshotToWire(*message.Snapshot)
		if err != nil {
			return nil, err
		}
		out.Snapshot = snapshot
	}
	if message.Resolved != nil {
		resolved, err := attachTargetToWire(*message.Resolved)
		if err != nil {
			return nil, err
		}
		out.Resolved = resolved
	}
	for _, observation := range message.Observations {
		observation := observation
		target := exactTargetToWire(&observation.Target)
		out.Observations = append(out.Observations, &wire.PickerRouteObservation{Target: target, Presence: uint32(observation.Presence), Attention: observation.Attention})
	}
	return out, nil
}

func pickerControlResponseFromWire(message *wire.PickerControlResponse) (protocol.PickerControlResponse, error) {
	var response protocol.PickerControlResponse
	if message == nil {
		return response, protocol.ErrInvalidNavigation
	}
	response.RequestID = message.GetRequestId()
	operation, err := enum8[protocol.PickerControlOperation](message.GetOperation())
	if err != nil {
		return protocol.PickerControlResponse{}, protocol.ErrInvalidNavigation
	}
	response.Operation = operation
	status, err := enum8[protocol.PickerSourceStatus](message.GetStatus())
	if err != nil {
		return protocol.PickerControlResponse{}, protocol.ErrInvalidNavigation
	}
	response.Status = status
	if message.GetSnapshot() != nil {
		snapshot, err := pickerSnapshotFromWire(message.GetSnapshot())
		if err != nil {
			return protocol.PickerControlResponse{}, err
		}
		response.Snapshot = &snapshot
	}
	if message.GetResolved() != nil {
		resolved, err := attachTargetFromWire(message.GetResolved())
		if err != nil {
			return protocol.PickerControlResponse{}, err
		}
		response.Resolved = &resolved
	}
	for _, observation := range message.GetObservations() {
		target, err := exactTargetFromWire(observation.GetTarget())
		if err != nil || target == nil {
			return protocol.PickerControlResponse{}, protocol.ErrInvalidNavigation
		}
		presence, err := enum8[protocol.PickerRoutePresence](observation.GetPresence())
		if err != nil {
			return protocol.PickerControlResponse{}, protocol.ErrInvalidNavigation
		}
		response.Observations = append(response.Observations, protocol.PickerRouteObservation{
			Target:    *target,
			Presence:  presence,
			Attention: observation.GetAttention(),
		})
	}
	if err := protocol.ValidatePickerControlResponse(response); err != nil {
		return protocol.PickerControlResponse{}, err
	}
	return response, nil
}
