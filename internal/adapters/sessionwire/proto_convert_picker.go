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
