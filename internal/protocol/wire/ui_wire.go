package wire

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

var errInvalidUIMessage = errors.New("invalid UI protocol message")

func marshalViewContext(w *payloadWriter, context protocol.ViewContext) error {
	if context.Validate() != nil {
		return errInvalidUIMessage
	}
	w.putUint64(context.Publication)
	marshalCommittedRouteIdentityFields(w, context.Route)
	w.putString(string(context.TabID))
	w.putString(string(context.FocusedPaneID))
	return nil
}

func unmarshalViewContext(r *payloadReader) (protocol.ViewContext, error) {
	var context protocol.ViewContext
	var err error
	if context.Publication, err = r.getUint64(); err != nil {
		return protocol.ViewContext{}, err
	}
	if context.Route.Target, err = unmarshalExactSessionTarget(r); err != nil {
		return protocol.ViewContext{}, err
	}
	if context.Route.Ephemeral, err = r.getBool(); err != nil {
		return protocol.ViewContext{}, err
	}
	tabID, err := r.getString()
	if err != nil {
		return protocol.ViewContext{}, err
	}
	paneID, err := r.getString()
	if err != nil {
		return protocol.ViewContext{}, err
	}
	context.TabID = domain.TabStableID(tabID)
	context.FocusedPaneID = domain.PaneStableID(paneID)
	if context.Validate() != nil {
		return protocol.ViewContext{}, errInvalidUIMessage
	}
	return context, nil
}

func MarshalUIFence(message protocol.UIFence) ([]byte, error) {
	if message.ActionID == 0 {
		return nil, errInvalidUIMessage
	}
	w := payloadWriter{}
	w.putUint64(message.ActionID)
	return w.b, nil
}

func UnmarshalUIFence(data []byte) (protocol.UIFence, error) {
	r := payloadReader{b: data}
	actionID, err := r.getUint64()
	if err != nil || actionID == 0 {
		return protocol.UIFence{}, errInvalidUIMessage
	}
	if err := r.done(); err != nil {
		return protocol.UIFence{}, err
	}
	return protocol.UIFence{ActionID: actionID}, nil
}

func MarshalUIReceipt(message protocol.UIReceipt) ([]byte, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	w := payloadWriter{}
	w.putUint64(message.ActionID)
	w.putUint64(message.Epoch)
	w.putUint64(message.State)
	w.putUint64(message.ViewPublication)
	w.putUint8(uint8(message.Outcome))
	return w.b, nil
}

func UnmarshalUIReceipt(data []byte) (protocol.UIReceipt, error) {
	r := payloadReader{b: data}
	var message protocol.UIReceipt
	var err error
	if message.ActionID, err = r.getUint64(); err != nil {
		return protocol.UIReceipt{}, err
	}
	if message.Epoch, err = r.getUint64(); err != nil {
		return protocol.UIReceipt{}, err
	}
	if message.State, err = r.getUint64(); err != nil {
		return protocol.UIReceipt{}, err
	}
	if message.ViewPublication, err = r.getUint64(); err != nil {
		return protocol.UIReceipt{}, err
	}
	outcome, err := r.getUint8()
	if err != nil {
		return protocol.UIReceipt{}, err
	}
	message.Outcome = protocol.UIReceiptOutcome(outcome)
	if err := r.done(); err != nil {
		return protocol.UIReceipt{}, err
	}
	if _, err := MarshalUIReceipt(message); err != nil {
		return protocol.UIReceipt{}, err
	}
	return message, nil
}

func MarshalUIViewUpdate(message protocol.UIViewUpdate) ([]byte, error) {
	if message.Epoch == 0 {
		return nil, errInvalidUIMessage
	}
	w := payloadWriter{}
	w.putUint64(message.Epoch)
	w.putUint64(message.State)
	if err := marshalViewContext(&w, message.Context); err != nil {
		return nil, err
	}
	return w.b, nil
}

func UnmarshalUIViewUpdate(data []byte) (protocol.UIViewUpdate, error) {
	r := payloadReader{b: data}
	var message protocol.UIViewUpdate
	var err error
	if message.Epoch, err = r.getUint64(); err != nil {
		return protocol.UIViewUpdate{}, err
	}
	if message.State, err = r.getUint64(); err != nil {
		return protocol.UIViewUpdate{}, err
	}
	if message.Context, err = unmarshalViewContext(&r); err != nil {
		return protocol.UIViewUpdate{}, err
	}
	if err := r.done(); err != nil {
		return protocol.UIViewUpdate{}, err
	}
	if message.Epoch == 0 {
		return protocol.UIViewUpdate{}, errInvalidUIMessage
	}
	return message, nil
}
