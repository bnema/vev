package sessionwire

// Directional envelope adaptation (P2.2/P3.1, wired at the P3.3 cutover).
//
// encodeProtoClient/encodeProtoServer wrap one semantic message in its
// directional envelope; decodeProtoClient/decodeProtoServer unwrap and
// validate. Wrong-direction payloads fail with ErrWrongDirection before any
// mutation. The typed connection adapters call these on every send and
// receive; serialized envelope ceilings are enforced by proto_limits.go.

import (
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func encodeProtoClient(message protocol.ClientMessage) (*wire.ClientEnvelope, error) {
	switch m := message.(type) {
	case protocol.SuspendAttachment:
		converted, err := suspendAttachmentToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_SuspendAttachment{SuspendAttachment: converted}}, nil
	case *protocol.SuspendAttachment:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.ActivateAttachment:
		converted, err := activateAttachmentToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_ActivateAttachment{ActivateAttachment: converted}}, nil
	case *protocol.ActivateAttachment:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.Hello:
		converted, err := helloToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_Hello{Hello: converted}}, nil
	case *protocol.Hello:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.Input:
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_Input{Input: inputToWire(m)}}, nil
	case *protocol.Input:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.Resize:
		converted, err := resizeToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_Resize{Resize: converted}}, nil
	case *protocol.Resize:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.Detach:
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_Detach{Detach: &wire.Detach{}}}, nil
	case *protocol.Detach:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.Ping:
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_Ping{Ping: &wire.Ping{}}}, nil
	case *protocol.Ping:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.List:
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_List{List: &wire.List{}}}, nil
	case *protocol.List:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.Kill:
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_Kill{Kill: killToWire(m)}}, nil
	case *protocol.Kill:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.Theme:
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_Theme{Theme: themeToWire(m)}}, nil
	case *protocol.Theme:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.Ack:
		converted, err := ackToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_Ack{Ack: converted}}, nil
	case *protocol.Ack:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.ImagePush:
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_ImagePush{ImagePush: imagePushToWire(m)}}, nil
	case *protocol.ImagePush:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.ClientNotice:
		converted, err := clientNoticeToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_ClientNotice{ClientNotice: converted}}, nil
	case *protocol.ClientNotice:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.CommandRequest:
		converted, err := commandRequestToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_CommandRequest{CommandRequest: converted}}, nil
	case *protocol.CommandRequest:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.OutputResetRequest:
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_OutputResetRequest{OutputResetRequest: &wire.OutputResetRequest{}}}, nil
	case *protocol.OutputResetRequest:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.UIFence:
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_UiFence{UiFence: uiFenceToWire(m)}}, nil
	case *protocol.UIFence:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.RemotePreviewRequest:
		converted, err := remotePreviewRequestToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_RemotePreviewRequest{RemotePreviewRequest: converted}}, nil
	case *protocol.RemotePreviewRequest:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.RouteAttentionSubscription:
		converted, err := attentionSubscriptionToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_RouteAttentionSubscription{RouteAttentionSubscription: converted}}, nil
	case *protocol.RouteAttentionSubscription:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.SamePeerSwitchRequest:
		converted, err := samePeerSwitchRequestToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_SamePeerSwitchRequest{SamePeerSwitchRequest: converted}}, nil
	case *protocol.SamePeerSwitchRequest:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.NavigationInventoryRequest:
		converted, err := inventoryRequestToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_NavigationInventoryRequest{NavigationInventoryRequest: converted}}, nil
	case *protocol.NavigationInventoryRequest:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.NavigationInventoryPublication:
		converted, err := inventoryPublicationToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_NavigationInventoryPublication{NavigationInventoryPublication: converted}}, nil
	case *protocol.NavigationInventoryPublication:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.NavigationInventoryFailure:
		converted, err := inventoryFailureToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_NavigationInventoryFailure{NavigationInventoryFailure: converted}}, nil
	case *protocol.NavigationInventoryFailure:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.PickerBegin:
		converted, err := pickerBeginToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_PickerBegin{PickerBegin: converted}}, nil
	case *protocol.PickerBegin:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.PickerClose:
		converted, err := pickerCloseToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_PickerClose{PickerClose: converted}}, nil
	case *protocol.PickerClose:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.PickerSelection:
		converted, err := pickerSelectionToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_PickerSelection{PickerSelection: converted}}, nil
	case *protocol.PickerSelection:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.PickerControlRequest:
		converted, err := pickerControlRequestToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_PickerControlRequest{PickerControlRequest: converted}}, nil
	case *protocol.PickerControlRequest:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.PickerPreviewRequest:
		converted, err := pickerPreviewRequestToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_PickerPreviewRequest{PickerPreviewRequest: converted}}, nil
	case *protocol.PickerPreviewRequest:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.RecentRouteSnapshot:
		converted, err := recentRouteSnapshotToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_RecentRouteSnapshot{RecentRouteSnapshot: converted}}, nil
	case *protocol.RecentRouteSnapshot:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.RouteNavigationFailure:
		converted, err := navigationFailureToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_RouteNavigationFailure{RouteNavigationFailure: converted}}, nil
	case *protocol.RouteNavigationFailure:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	case protocol.SessionCreationFailure:
		converted, err := sessionCreationFailureToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ClientEnvelope{Payload: &wire.ClientEnvelope_SessionCreationFailure{SessionCreationFailure: converted}}, nil
	case *protocol.SessionCreationFailure:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoClient(*m)
	default:
		return nil, ErrWrongDirection
	}
}

func encodeProtoServer(message protocol.ServerMessage) (*wire.ServerEnvelope, error) {
	switch m := message.(type) {
	case protocol.AttachmentSuspended:
		converted, err := attachmentSuspendedToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_AttachmentSuspended{AttachmentSuspended: converted}}, nil
	case *protocol.AttachmentSuspended:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.AttachmentActivated:
		converted, err := attachmentActivatedToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_AttachmentActivated{AttachmentActivated: converted}}, nil
	case *protocol.AttachmentActivated:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.Welcome:
		converted, err := welcomeToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_Welcome{Welcome: converted}}, nil
	case *protocol.Welcome:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.ErrorMsg:
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_Error{Error: errorToWire(m)}}, nil
	case *protocol.ErrorMsg:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.Output:
		converted, err := outputToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_Output{Output: converted}}, nil
	case *protocol.Output:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.Detached:
		converted, err := detachedToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_Detached{Detached: converted}}, nil
	case *protocol.Detached:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.Pong:
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_Pong{Pong: &wire.Pong{}}}, nil
	case *protocol.Pong:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.Sessions:
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_Sessions{Sessions: sessionsToWire(m)}}, nil
	case *protocol.Sessions:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.CommandResult:
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_CommandResult{CommandResult: commandResultToWire(m)}}, nil
	case *protocol.CommandResult:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.AttachTarget:
		converted, err := attachTargetToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_AttachTarget{AttachTarget: converted}}, nil
	case *protocol.AttachTarget:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.RemotePreview:
		converted, err := remotePreviewToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_RemotePreview{RemotePreview: converted}}, nil
	case *protocol.RemotePreview:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.CommittedRouteIdentity:
		converted, err := committedIdentityToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_CommittedRouteIdentity{CommittedRouteIdentity: converted}}, nil
	case *protocol.CommittedRouteIdentity:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.RouteNavigationAction:
		converted, err := routeNavigationActionToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_RouteNavigationAction{RouteNavigationAction: converted}}, nil
	case *protocol.RouteNavigationAction:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.RouteCreateSessionAction:
		converted, err := routeCreateSessionActionToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_RouteCreateSessionAction{RouteCreateSessionAction: converted}}, nil
	case *protocol.RouteCreateSessionAction:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.RouteNavigationFailure:
		converted, err := navigationFailureToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_RouteNavigationFailure{RouteNavigationFailure: converted}}, nil
	case *protocol.RouteNavigationFailure:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.RouteRetired:
		converted, err := routeRetiredToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_RouteRetired{RouteRetired: converted}}, nil
	case *protocol.RouteRetired:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.RoutePosition:
		converted, err := routePositionToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_RoutePosition{RoutePosition: converted}}, nil
	case *protocol.RoutePosition:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.SamePeerSwitchFailure:
		converted, err := samePeerSwitchFailureToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_SamePeerSwitchFailure{SamePeerSwitchFailure: converted}}, nil
	case *protocol.SamePeerSwitchFailure:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.UIReceipt:
		converted, err := uiReceiptToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_UiReceipt{UiReceipt: converted}}, nil
	case *protocol.UIReceipt:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.UIViewUpdate:
		converted, err := uiViewUpdateToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_UiViewUpdate{UiViewUpdate: converted}}, nil
	case *protocol.UIViewUpdate:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.NavigationInventoryResponse:
		converted, err := inventoryResponseToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_NavigationInventoryResponse{NavigationInventoryResponse: converted}}, nil
	case *protocol.NavigationInventoryResponse:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.NavigationInventoryDemand:
		converted, err := inventoryDemandToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_NavigationInventoryDemand{NavigationInventoryDemand: converted}}, nil
	case *protocol.NavigationInventoryDemand:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.NavigationInventorySelection:
		converted, err := inventorySelectionToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_NavigationInventorySelection{NavigationInventorySelection: converted}}, nil
	case *protocol.NavigationInventorySelection:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.PickerOffer:
		converted, err := pickerOfferToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_PickerOffer{PickerOffer: converted}}, nil
	case *protocol.PickerOffer:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.PickerSnapshot:
		converted, err := pickerSnapshotToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_PickerSnapshot{PickerSnapshot: converted}}, nil
	case *protocol.PickerSnapshot:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.PickerClosed:
		converted, err := pickerClosedToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_PickerClosed{PickerClosed: converted}}, nil
	case *protocol.PickerClosed:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.PickerResult:
		converted, err := pickerResultToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_PickerResult{PickerResult: converted}}, nil
	case *protocol.PickerResult:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.PickerFailure:
		converted, err := pickerFailureToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_PickerFailure{PickerFailure: converted}}, nil
	case *protocol.PickerFailure:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.PickerPreview:
		converted, err := pickerPreviewToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_PickerPreview{PickerPreview: converted}}, nil
	case *protocol.PickerPreview:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	case protocol.PickerControlResponse:
		converted, err := pickerControlResponseToWire(m)
		if err != nil {
			return nil, err
		}
		return &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_PickerControlResponse{PickerControlResponse: converted}}, nil
	case *protocol.PickerControlResponse:
		if m == nil {
			return nil, ErrInvalidMessage
		}
		return encodeProtoServer(*m)
	default:
		return nil, ErrWrongDirection
	}
}

// decodeProtoClient unwraps one scanned client envelope into its semantic
// message. The payload must already have passed wire.ScanEnvelope.
func decodeProtoClient(envelope *wire.ClientEnvelope) (protocol.ClientMessage, error) {
	if envelope == nil {
		return nil, ErrInvalidMessage
	}
	switch payload := envelope.Payload.(type) {
	case *wire.ClientEnvelope_SuspendAttachment:
		return suspendAttachmentFromWire(payload.SuspendAttachment)
	case *wire.ClientEnvelope_ActivateAttachment:
		return activateAttachmentFromWire(payload.ActivateAttachment)
	case *wire.ClientEnvelope_Hello:
		return helloFromWire(payload.Hello)
	case *wire.ClientEnvelope_Input:
		return inputFromWire(payload.Input)
	case *wire.ClientEnvelope_Resize:
		return resizeFromWire(payload.Resize)
	case *wire.ClientEnvelope_Detach:
		return protocol.Detach{}, nil
	case *wire.ClientEnvelope_Ping:
		return protocol.Ping{}, nil
	case *wire.ClientEnvelope_List:
		return protocol.List{}, nil
	case *wire.ClientEnvelope_Kill:
		return killFromWire(payload.Kill)
	case *wire.ClientEnvelope_Theme:
		return themeFromWire(payload.Theme)
	case *wire.ClientEnvelope_Ack:
		return ackFromWire(payload.Ack)
	case *wire.ClientEnvelope_ImagePush:
		return imagePushFromWire(payload.ImagePush)
	case *wire.ClientEnvelope_ClientNotice:
		return clientNoticeFromWire(payload.ClientNotice)
	case *wire.ClientEnvelope_CommandRequest:
		return commandRequestFromWire(payload.CommandRequest)
	case *wire.ClientEnvelope_OutputResetRequest:
		return protocol.OutputResetRequest{}, nil
	case *wire.ClientEnvelope_UiFence:
		return uiFenceFromWire(payload.UiFence)
	case *wire.ClientEnvelope_RemotePreviewRequest:
		return remotePreviewRequestFromWire(payload.RemotePreviewRequest)
	case *wire.ClientEnvelope_RouteAttentionSubscription:
		return attentionSubscriptionFromWire(payload.RouteAttentionSubscription)
	case *wire.ClientEnvelope_SamePeerSwitchRequest:
		return samePeerSwitchRequestFromWire(payload.SamePeerSwitchRequest)
	case *wire.ClientEnvelope_RecentRouteSnapshot:
		return recentRouteSnapshotFromWire(payload.RecentRouteSnapshot)
	case *wire.ClientEnvelope_RouteNavigationFailure:
		return navigationFailureFromWire(payload.RouteNavigationFailure)
	case *wire.ClientEnvelope_SessionCreationFailure:
		return sessionCreationFailureFromWire(payload.SessionCreationFailure)
	case *wire.ClientEnvelope_NavigationInventoryRequest:
		return inventoryRequestFromWire(payload.NavigationInventoryRequest)
	case *wire.ClientEnvelope_NavigationInventoryPublication:
		return inventoryPublicationFromWire(payload.NavigationInventoryPublication)
	case *wire.ClientEnvelope_NavigationInventoryFailure:
		return inventoryFailureFromWire(payload.NavigationInventoryFailure)
	case *wire.ClientEnvelope_PickerBegin:
		return pickerBeginFromWire(payload.PickerBegin)
	case *wire.ClientEnvelope_PickerClose:
		return pickerCloseFromWire(payload.PickerClose)
	case *wire.ClientEnvelope_PickerSelection:
		return pickerSelectionFromWire(payload.PickerSelection)
	case *wire.ClientEnvelope_PickerPreviewRequest:
		return pickerPreviewRequestFromWire(payload.PickerPreviewRequest)
	case *wire.ClientEnvelope_PickerControlRequest:
		return pickerControlRequestFromWire(payload.PickerControlRequest)
	default:
		return nil, ErrWrongDirection
	}
}

// decodeProtoServer unwraps one scanned server envelope into its semantic
// message. The payload must already have passed wire.ScanEnvelope.
func decodeProtoServer(envelope *wire.ServerEnvelope) (protocol.ServerMessage, error) {
	if envelope == nil {
		return nil, ErrInvalidMessage
	}
	switch payload := envelope.Payload.(type) {
	case *wire.ServerEnvelope_AttachmentSuspended:
		return attachmentSuspendedFromWire(payload.AttachmentSuspended)
	case *wire.ServerEnvelope_AttachmentActivated:
		return attachmentActivatedFromWire(payload.AttachmentActivated)
	case *wire.ServerEnvelope_Welcome:
		return welcomeFromWire(payload.Welcome)
	case *wire.ServerEnvelope_Error:
		return errorFromWire(payload.Error)
	case *wire.ServerEnvelope_Output:
		return outputFromWire(payload.Output)
	case *wire.ServerEnvelope_Detached:
		return detachedFromWire(payload.Detached)
	case *wire.ServerEnvelope_Pong:
		return protocol.Pong{}, nil
	case *wire.ServerEnvelope_Sessions:
		return sessionsFromWire(payload.Sessions)
	case *wire.ServerEnvelope_CommandResult:
		return commandResultFromWire(payload.CommandResult)
	case *wire.ServerEnvelope_AttachTarget:
		return attachTargetFromWire(payload.AttachTarget)
	case *wire.ServerEnvelope_RemotePreview:
		return remotePreviewFromWire(payload.RemotePreview)
	case *wire.ServerEnvelope_CommittedRouteIdentity:
		return committedIdentityFromWire(payload.CommittedRouteIdentity)
	case *wire.ServerEnvelope_RouteNavigationAction:
		return routeNavigationActionFromWire(payload.RouteNavigationAction)
	case *wire.ServerEnvelope_RouteCreateSessionAction:
		return routeCreateSessionActionFromWire(payload.RouteCreateSessionAction)
	case *wire.ServerEnvelope_RouteNavigationFailure:
		return navigationFailureFromWire(payload.RouteNavigationFailure)
	case *wire.ServerEnvelope_RoutePosition:
		return routePositionFromWire(payload.RoutePosition)
	case *wire.ServerEnvelope_RouteRetired:
		return routeRetiredFromWire(payload.RouteRetired)
	case *wire.ServerEnvelope_SamePeerSwitchFailure:
		return samePeerSwitchFailureFromWire(payload.SamePeerSwitchFailure)
	case *wire.ServerEnvelope_UiReceipt:
		return uiReceiptFromWire(payload.UiReceipt)
	case *wire.ServerEnvelope_UiViewUpdate:
		return uiViewUpdateFromWire(payload.UiViewUpdate)
	case *wire.ServerEnvelope_NavigationInventoryResponse:
		return inventoryResponseFromWire(payload.NavigationInventoryResponse)
	case *wire.ServerEnvelope_NavigationInventoryDemand:
		return inventoryDemandFromWire(payload.NavigationInventoryDemand)
	case *wire.ServerEnvelope_NavigationInventorySelection:
		return inventorySelectionFromWire(payload.NavigationInventorySelection)
	case *wire.ServerEnvelope_PickerOffer:
		return pickerOfferFromWire(payload.PickerOffer)
	case *wire.ServerEnvelope_PickerSnapshot:
		return pickerSnapshotFromWire(payload.PickerSnapshot)
	case *wire.ServerEnvelope_PickerClosed:
		return pickerClosedFromWire(payload.PickerClosed)
	case *wire.ServerEnvelope_PickerResult:
		return pickerResultFromWire(payload.PickerResult)
	case *wire.ServerEnvelope_PickerFailure:
		return pickerFailureFromWire(payload.PickerFailure)
	case *wire.ServerEnvelope_PickerPreview:
		return pickerPreviewFromWire(payload.PickerPreview)
	case *wire.ServerEnvelope_PickerControlResponse:
		return pickerControlResponseFromWire(payload.PickerControlResponse)
	default:
		return nil, ErrWrongDirection
	}
}
