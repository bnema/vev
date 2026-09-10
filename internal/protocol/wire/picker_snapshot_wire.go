package wire

import (
	"github.com/bnema/vev/internal/protocol"
)

// marshalPickerRows encodes opaque rows with display safety enforced by the
// semantic validator first.
func marshalPickerRows(w *payloadWriter, rows []protocol.PickerRow) bool {
	if len(rows) > protocol.PickerInteractionMaxRows {
		return false
	}
	w.putUint16(uint16(len(rows)))
	for _, row := range rows {
		w.putString(row.Key)
		w.putString(row.Display)
		w.putString(row.Detail)
		w.putBool(row.Stopped)
	}
	return true
}

func unmarshalPickerRows(r *payloadReader) ([]protocol.PickerRow, error) {
	count, err := r.getUint16()
	if err != nil {
		return nil, err
	}
	if int(count) > protocol.PickerInteractionMaxRows {
		return nil, protocol.ErrInvalidNavigation
	}
	rows := make([]protocol.PickerRow, 0, int(count))
	for range int(count) {
		var row protocol.PickerRow
		if row.Key, err = r.getString(); err != nil {
			return nil, err
		}
		if row.Display, err = r.getString(); err != nil {
			return nil, err
		}
		if row.Detail, err = r.getString(); err != nil {
			return nil, err
		}
		if row.Stopped, err = r.getBool(); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// MarshalPickerOpen encodes a client-generated interaction open.
func MarshalPickerOpen(open protocol.PickerOpen) []byte {
	if protocol.ValidatePickerOpen(open) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(open.InteractionID)
	return w.b
}

// UnmarshalPickerOpen decodes a strict interaction open.
func UnmarshalPickerOpen(data []byte) (protocol.PickerOpen, error) {
	r := payloadReader{b: data}
	var open protocol.PickerOpen
	var err error
	if open.InteractionID, err = r.getUint64(); err != nil {
		return protocol.PickerOpen{}, err
	}
	if err := r.done(); err != nil {
		return protocol.PickerOpen{}, err
	}
	if err := protocol.ValidatePickerOpen(open); err != nil {
		return protocol.PickerOpen{}, err
	}
	return open, nil
}

// MarshalPickerSnapshot encodes a full model replacement. Row order is
// preserved verbatim.
func MarshalPickerSnapshot(snapshot protocol.PickerSnapshot) []byte {
	if protocol.ValidatePickerSnapshot(snapshot) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(snapshot.InteractionID)
	w.putUint64(snapshot.Revision)
	w.putString(snapshot.Title)
	if !marshalPickerRows(&w, snapshot.Rows) {
		return nil
	}
	w.putString(snapshot.Cursor.Key)
	w.putUint64(uint64(snapshot.Cursor.Index))
	w.putUint64(snapshot.BarrierEpoch)
	w.putUint64(snapshot.BarrierState)
	w.putUint64(snapshot.SizeEpoch)
	if len(w.b) > protocol.PickerInteractionMaxEncodedBytes {
		return nil
	}
	return w.b
}

// UnmarshalPickerSnapshot decodes a strict model replacement.
func UnmarshalPickerSnapshot(data []byte) (protocol.PickerSnapshot, error) {
	if len(data) > protocol.PickerInteractionMaxEncodedBytes {
		return protocol.PickerSnapshot{}, protocol.ErrInvalidNavigation
	}
	r := payloadReader{b: data}
	var snapshot protocol.PickerSnapshot
	var err error
	if snapshot.InteractionID, err = r.getUint64(); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	if snapshot.Revision, err = r.getUint64(); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	if snapshot.Title, err = r.getString(); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	if snapshot.Rows, err = unmarshalPickerRows(&r); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	if snapshot.Cursor.Key, err = r.getString(); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	index, err := r.getUint64()
	if err != nil {
		return protocol.PickerSnapshot{}, err
	}
	snapshot.Cursor.Index = int(index)
	if snapshot.BarrierEpoch, err = r.getUint64(); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	if snapshot.BarrierState, err = r.getUint64(); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	if snapshot.SizeEpoch, err = r.getUint64(); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	if err := r.done(); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	if err := protocol.ValidatePickerSnapshot(snapshot); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	return snapshot, nil
}

// MarshalPickerClose encodes an interaction close from either side.
func MarshalPickerClose(close protocol.PickerClose) []byte {
	if protocol.ValidatePickerClose(close) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(close.InteractionID)
	w.putUint64(close.Revision)
	return w.b
}

// UnmarshalPickerClose decodes a strict interaction close.
func UnmarshalPickerClose(data []byte) (protocol.PickerClose, error) {
	r := payloadReader{b: data}
	var close protocol.PickerClose
	var err error
	if close.InteractionID, err = r.getUint64(); err != nil {
		return protocol.PickerClose{}, err
	}
	if close.Revision, err = r.getUint64(); err != nil {
		return protocol.PickerClose{}, err
	}
	if err := r.done(); err != nil {
		return protocol.PickerClose{}, err
	}
	if err := protocol.ValidatePickerClose(close); err != nil {
		return protocol.PickerClose{}, err
	}
	return close, nil
}

// MarshalPickerSelection encodes an opaque row-key commit request.
func MarshalPickerSelection(selection protocol.PickerSelection) []byte {
	if protocol.ValidatePickerSelection(selection) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(selection.CauseActionID)
	w.putUint64(selection.InteractionID)
	w.putUint64(selection.Revision)
	w.putString(selection.Key)
	return w.b
}

// UnmarshalPickerSelection decodes a strict commit request.
func UnmarshalPickerSelection(data []byte) (protocol.PickerSelection, error) {
	r := payloadReader{b: data}
	var selection protocol.PickerSelection
	var err error
	if selection.CauseActionID, err = r.getUint64(); err != nil {
		return protocol.PickerSelection{}, err
	}
	if selection.InteractionID, err = r.getUint64(); err != nil {
		return protocol.PickerSelection{}, err
	}
	if selection.Revision, err = r.getUint64(); err != nil {
		return protocol.PickerSelection{}, err
	}
	if selection.Key, err = r.getString(); err != nil {
		return protocol.PickerSelection{}, err
	}
	if err := r.done(); err != nil {
		return protocol.PickerSelection{}, err
	}
	if err := protocol.ValidatePickerSelection(selection); err != nil {
		return protocol.PickerSelection{}, err
	}
	return selection, nil
}

// MarshalPickerFailure encodes a bounded selection failure.
func MarshalPickerFailure(failure protocol.PickerFailure) []byte {
	if protocol.ValidatePickerFailure(failure) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(failure.CauseActionID)
	w.putUint64(failure.InteractionID)
	w.putString(failure.Key)
	w.putUint8(uint8(failure.Code))
	return w.b
}

// UnmarshalPickerFailure decodes a strict selection failure.
func UnmarshalPickerFailure(data []byte) (protocol.PickerFailure, error) {
	r := payloadReader{b: data}
	var failure protocol.PickerFailure
	var err error
	if failure.CauseActionID, err = r.getUint64(); err != nil {
		return protocol.PickerFailure{}, err
	}
	if failure.InteractionID, err = r.getUint64(); err != nil {
		return protocol.PickerFailure{}, err
	}
	if failure.Key, err = r.getString(); err != nil {
		return protocol.PickerFailure{}, err
	}
	code, err := r.getUint8()
	if err != nil {
		return protocol.PickerFailure{}, err
	}
	failure.Code = protocol.PickerFailureCode(code)
	if err := r.done(); err != nil {
		return protocol.PickerFailure{}, err
	}
	if err := protocol.ValidatePickerFailure(failure); err != nil {
		return protocol.PickerFailure{}, err
	}
	return failure, nil
}
