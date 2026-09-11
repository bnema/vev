package wire

import (
	"github.com/bnema/vev/internal/protocol"
)

// pickerLineFlags packs the three independent line booleans into one byte so
// the wire layout stays compact and explicit.
const (
	pickerLineAttention uint8 = 1 << iota
	pickerLineStopped
	pickerLineDim
)

// marshalPickerLines encodes structured lines, enforcing the semantic
// validator first so an invalid list never reaches the wire.
func marshalPickerLines(w *payloadWriter, lines []protocol.PickerLine) bool {
	if len(lines) > protocol.PickerInteractionMaxLines {
		return false
	}
	w.putUint16(uint16(len(lines)))
	for _, line := range lines {
		w.putString(line.Key)
		w.putUint8(uint8(line.Kind))
		w.putString(line.Label)
		w.putString(line.Detail)
		w.putUint8(uint8(line.Status))
		w.putString(line.StatusDetail)
		var flags uint8
		if line.Attention {
			flags |= pickerLineAttention
		}
		if line.Stopped {
			flags |= pickerLineStopped
		}
		if line.Dim {
			flags |= pickerLineDim
		}
		w.putUint8(flags)
		w.putUint8(uint8(line.Actions))
		w.putBool(line.Ephemeral)
	}
	return true
}

func unmarshalPickerLines(r *payloadReader) ([]protocol.PickerLine, error) {
	count, err := r.getUint16()
	if err != nil {
		return nil, err
	}
	if int(count) > protocol.PickerInteractionMaxLines {
		return nil, protocol.ErrInvalidNavigation
	}
	lines := make([]protocol.PickerLine, 0, int(count))
	for range int(count) {
		var line protocol.PickerLine
		if line.Key, err = r.getString(); err != nil {
			return nil, err
		}
		kind, err := r.getUint8()
		if err != nil {
			return nil, err
		}
		line.Kind = protocol.PickerLineKind(kind)
		if line.Label, err = r.getString(); err != nil {
			return nil, err
		}
		if line.Detail, err = r.getString(); err != nil {
			return nil, err
		}
		status, err := r.getUint8()
		if err != nil {
			return nil, err
		}
		line.Status = protocol.PickerLineStatus(status)
		if line.StatusDetail, err = r.getString(); err != nil {
			return nil, err
		}
		flags, err := r.getUint8()
		if err != nil {
			return nil, err
		}
		line.Attention = flags&pickerLineAttention != 0
		line.Stopped = flags&pickerLineStopped != 0
		line.Dim = flags&pickerLineDim != 0
		actions, err := r.getUint8()
		if err != nil {
			return nil, err
		}
		line.Actions = protocol.PickerLineActions(actions)
		if line.Ephemeral, err = r.getBool(); err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	return lines, nil
}

func marshalPickerCursor(w *payloadWriter, cursor protocol.PickerCursor) {
	w.putString(cursor.Key)
	w.putUint64(uint64(cursor.Index))
}

func unmarshalPickerCursor(r *payloadReader) (protocol.PickerCursor, error) {
	var cursor protocol.PickerCursor
	var err error
	if cursor.Key, err = r.getString(); err != nil {
		return protocol.PickerCursor{}, err
	}
	index, err := r.getUint64()
	if err != nil {
		return protocol.PickerCursor{}, err
	}
	cursor.Index = int(index)
	return cursor, nil
}

// MarshalPickerOffer encodes one interaction offer.
func MarshalPickerOffer(offer protocol.PickerOffer) []byte {
	if protocol.ValidatePickerOffer(offer) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(offer.InteractionID)
	w.putUint64(offer.RequestID)
	w.putUint8(uint8(offer.Intent))
	w.putString(offer.MoveSourceKey)
	w.putUint64(offer.BarrierEpoch)
	w.putUint64(offer.BarrierState)
	w.putUint64(offer.SizeEpoch)
	w.putString(offer.Title)
	return w.b
}

// UnmarshalPickerOffer decodes a strict interaction offer.
func UnmarshalPickerOffer(data []byte) (protocol.PickerOffer, error) {
	r := payloadReader{b: data}
	var offer protocol.PickerOffer
	var err error
	if offer.InteractionID, err = r.getUint64(); err != nil {
		return protocol.PickerOffer{}, err
	}
	if offer.RequestID, err = r.getUint64(); err != nil {
		return protocol.PickerOffer{}, err
	}
	intent, err := r.getUint8()
	if err != nil {
		return protocol.PickerOffer{}, err
	}
	offer.Intent = protocol.PickerIntent(intent)
	if offer.MoveSourceKey, err = r.getString(); err != nil {
		return protocol.PickerOffer{}, err
	}
	if offer.BarrierEpoch, err = r.getUint64(); err != nil {
		return protocol.PickerOffer{}, err
	}
	if offer.BarrierState, err = r.getUint64(); err != nil {
		return protocol.PickerOffer{}, err
	}
	if offer.SizeEpoch, err = r.getUint64(); err != nil {
		return protocol.PickerOffer{}, err
	}
	if offer.Title, err = r.getString(); err != nil {
		return protocol.PickerOffer{}, err
	}
	if err := r.done(); err != nil {
		return protocol.PickerOffer{}, err
	}
	if err := protocol.ValidatePickerOffer(offer); err != nil {
		return protocol.PickerOffer{}, err
	}
	return offer, nil
}

// MarshalPickerBegin encodes one interaction request.
func MarshalPickerBegin(begin protocol.PickerBegin) []byte {
	if protocol.ValidatePickerBegin(begin) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(begin.RequestID)
	w.putUint8(uint8(begin.Intent))
	return w.b
}

// UnmarshalPickerBegin decodes a strict interaction request.
func UnmarshalPickerBegin(data []byte) (protocol.PickerBegin, error) {
	r := payloadReader{b: data}
	var begin protocol.PickerBegin
	var err error
	if begin.RequestID, err = r.getUint64(); err != nil {
		return protocol.PickerBegin{}, err
	}
	intent, err := r.getUint8()
	if err != nil {
		return protocol.PickerBegin{}, err
	}
	begin.Intent = protocol.PickerIntent(intent)
	if err := r.done(); err != nil {
		return protocol.PickerBegin{}, err
	}
	if err := protocol.ValidatePickerBegin(begin); err != nil {
		return protocol.PickerBegin{}, err
	}
	return begin, nil
}

// MarshalPickerSnapshot encodes one source publication. Line order is
// preserved verbatim.
func MarshalPickerSnapshot(snapshot protocol.PickerSnapshot) []byte {
	if protocol.ValidatePickerSnapshot(snapshot) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(snapshot.InteractionID)
	w.putString(snapshot.SourceID)
	w.putUint64(snapshot.SourceRevision)
	w.putUint8(uint8(snapshot.Status))
	w.putString(snapshot.StatusDetail)
	if !marshalPickerLines(&w, snapshot.Lines) {
		return nil
	}
	marshalPickerCursor(&w, snapshot.Cursor)
	if len(w.b) > protocol.PickerInteractionMaxEncodedBytes {
		return nil
	}
	return w.b
}

// UnmarshalPickerSnapshot decodes a strict source publication.
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
	if snapshot.SourceID, err = r.getString(); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	if snapshot.SourceRevision, err = r.getUint64(); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	status, err := r.getUint8()
	if err != nil {
		return protocol.PickerSnapshot{}, err
	}
	snapshot.Status = protocol.PickerSourceStatus(status)
	if snapshot.StatusDetail, err = r.getString(); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	if snapshot.Lines, err = unmarshalPickerLines(&r); err != nil {
		return protocol.PickerSnapshot{}, err
	}
	if snapshot.Cursor, err = unmarshalPickerCursor(&r); err != nil {
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

// MarshalPickerClose encodes a client interaction close.
func MarshalPickerClose(close protocol.PickerClose) []byte {
	if protocol.ValidatePickerClose(close) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(close.InteractionID)
	w.putUint64(close.RequestID)
	return w.b
}

// UnmarshalPickerClose decodes a strict client interaction close.
func UnmarshalPickerClose(data []byte) (protocol.PickerClose, error) {
	r := payloadReader{b: data}
	var close protocol.PickerClose
	var err error
	if close.InteractionID, err = r.getUint64(); err != nil {
		return protocol.PickerClose{}, err
	}
	if close.RequestID, err = r.getUint64(); err != nil {
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

// MarshalPickerClosed encodes the serving daemon's close confirmation.
func MarshalPickerClosed(closed protocol.PickerClosed) []byte {
	if protocol.ValidatePickerClosed(closed) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(closed.InteractionID)
	w.putUint64(closed.BarrierEpoch)
	w.putUint64(closed.BarrierState)
	return w.b
}

// UnmarshalPickerClosed decodes a strict close confirmation.
func UnmarshalPickerClosed(data []byte) (protocol.PickerClosed, error) {
	r := payloadReader{b: data}
	var closed protocol.PickerClosed
	var err error
	if closed.InteractionID, err = r.getUint64(); err != nil {
		return protocol.PickerClosed{}, err
	}
	if closed.BarrierEpoch, err = r.getUint64(); err != nil {
		return protocol.PickerClosed{}, err
	}
	if closed.BarrierState, err = r.getUint64(); err != nil {
		return protocol.PickerClosed{}, err
	}
	if err := r.done(); err != nil {
		return protocol.PickerClosed{}, err
	}
	if err := protocol.ValidatePickerClosed(closed); err != nil {
		return protocol.PickerClosed{}, err
	}
	return closed, nil
}

// MarshalPickerSelection encodes an opaque row-key commit request.
func MarshalPickerSelection(selection protocol.PickerSelection) []byte {
	if protocol.ValidatePickerSelection(selection) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(selection.CauseActionID)
	w.putUint64(selection.RequestID)
	w.putUint64(selection.InteractionID)
	w.putString(selection.SourceID)
	w.putUint64(selection.SourceRevision)
	w.putString(selection.Key)
	w.putUint8(uint8(selection.Action))
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
	if selection.RequestID, err = r.getUint64(); err != nil {
		return protocol.PickerSelection{}, err
	}
	if selection.InteractionID, err = r.getUint64(); err != nil {
		return protocol.PickerSelection{}, err
	}
	if selection.SourceID, err = r.getString(); err != nil {
		return protocol.PickerSelection{}, err
	}
	if selection.SourceRevision, err = r.getUint64(); err != nil {
		return protocol.PickerSelection{}, err
	}
	if selection.Key, err = r.getString(); err != nil {
		return protocol.PickerSelection{}, err
	}
	action, err := r.getUint8()
	if err != nil {
		return protocol.PickerSelection{}, err
	}
	selection.Action = protocol.PickerAction(action)
	if err := r.done(); err != nil {
		return protocol.PickerSelection{}, err
	}
	if err := protocol.ValidatePickerSelection(selection); err != nil {
		return protocol.PickerSelection{}, err
	}
	return selection, nil
}

// MarshalPickerResult encodes a completed mutation report.
func MarshalPickerResult(result protocol.PickerResult) []byte {
	if protocol.ValidatePickerResult(result) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(result.CauseActionID)
	w.putUint64(result.RequestID)
	w.putUint64(result.InteractionID)
	w.putString(result.SourceID)
	w.putString(result.Key)
	w.putUint8(uint8(result.Action))
	return w.b
}

// UnmarshalPickerResult decodes a strict mutation report.
func UnmarshalPickerResult(data []byte) (protocol.PickerResult, error) {
	r := payloadReader{b: data}
	var result protocol.PickerResult
	var err error
	if result.CauseActionID, err = r.getUint64(); err != nil {
		return protocol.PickerResult{}, err
	}
	if result.RequestID, err = r.getUint64(); err != nil {
		return protocol.PickerResult{}, err
	}
	if result.InteractionID, err = r.getUint64(); err != nil {
		return protocol.PickerResult{}, err
	}
	if result.SourceID, err = r.getString(); err != nil {
		return protocol.PickerResult{}, err
	}
	if result.Key, err = r.getString(); err != nil {
		return protocol.PickerResult{}, err
	}
	action, err := r.getUint8()
	if err != nil {
		return protocol.PickerResult{}, err
	}
	result.Action = protocol.PickerAction(action)
	if err := r.done(); err != nil {
		return protocol.PickerResult{}, err
	}
	if err := protocol.ValidatePickerResult(result); err != nil {
		return protocol.PickerResult{}, err
	}
	return result, nil
}

// MarshalPickerFailure encodes a bounded selection failure.
func MarshalPickerFailure(failure protocol.PickerFailure) []byte {
	if protocol.ValidatePickerFailure(failure) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(failure.CauseActionID)
	w.putUint64(failure.RequestID)
	w.putUint64(failure.InteractionID)
	w.putString(failure.SourceID)
	w.putString(failure.Key)
	w.putUint8(uint8(failure.Action))
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
	if failure.RequestID, err = r.getUint64(); err != nil {
		return protocol.PickerFailure{}, err
	}
	if failure.InteractionID, err = r.getUint64(); err != nil {
		return protocol.PickerFailure{}, err
	}
	if failure.SourceID, err = r.getString(); err != nil {
		return protocol.PickerFailure{}, err
	}
	if failure.Key, err = r.getString(); err != nil {
		return protocol.PickerFailure{}, err
	}
	action, err := r.getUint8()
	if err != nil {
		return protocol.PickerFailure{}, err
	}
	failure.Action = protocol.PickerAction(action)
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
