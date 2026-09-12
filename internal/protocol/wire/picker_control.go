package wire

import (
	"encoding/binary"

	"github.com/bnema/vev/internal/protocol"
)

func PeekPickerControlVersion(data []byte) (uint16, bool) {
	if len(data) < 2 {
		return 0, false
	}
	return binary.BigEndian.Uint16(data), true
}

// marshalPickerControlTargets encodes a bounded exact-target list. The count
// guard mirrors semantic validation so a malformed caller can never write an
// unbounded frame.
func marshalPickerControlTargets(w *payloadWriter, targets []protocol.ExactSessionTarget) bool {
	if len(targets) > protocol.PickerControlMaxTargets {
		return false
	}
	w.putUint16(uint16(len(targets)))
	for _, target := range targets {
		marshalExactSessionTarget(w, target)
	}
	return true
}

func unmarshalPickerControlTargets(r *payloadReader) ([]protocol.ExactSessionTarget, error) {
	count, err := r.getUint16()
	if err != nil {
		return nil, err
	}
	if int(count) > protocol.PickerControlMaxTargets {
		return nil, protocol.ErrInvalidNavigation
	}
	if count == 0 {
		return nil, nil
	}
	targets := make([]protocol.ExactSessionTarget, 0, int(count))
	for range int(count) {
		target, err := unmarshalExactSessionTarget(r)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	return targets, nil
}

// marshalPickerRouteObservations encodes the observation payload carried inside
// the response union. The exact-target helper keeps the target layout shared
// with the rest of the wire.
func marshalPickerRouteObservations(w *payloadWriter, observations []protocol.PickerRouteObservation) bool {
	if len(observations) > protocol.PickerControlMaxTargets {
		return false
	}
	w.putUint16(uint16(len(observations)))
	for _, observation := range observations {
		marshalExactSessionTarget(w, observation.Target)
		w.putUint8(uint8(observation.Presence))
		w.putBool(observation.Attention)
	}
	return true
}

// unmarshalPickerRouteObservations decodes the observation payload carried
// inside the response union and rejects an overlong count or trailing bytes.
// Semantic checks stay with ValidatePickerControlResponse, which runs after the
// complete response is decoded.
func unmarshalPickerRouteObservations(data []byte) ([]protocol.PickerRouteObservation, error) {
	r := payloadReader{b: data}
	count, err := r.getUint16()
	if err != nil {
		return nil, err
	}
	if int(count) > protocol.PickerControlMaxTargets {
		return nil, protocol.ErrInvalidNavigation
	}
	if count == 0 {
		return nil, nil
	}
	observations := make([]protocol.PickerRouteObservation, 0, int(count))
	for range int(count) {
		var observation protocol.PickerRouteObservation
		target, err := unmarshalExactSessionTarget(&r)
		if err != nil {
			return nil, err
		}
		observation.Target = target
		presence, err := r.getUint8()
		if err != nil {
			return nil, err
		}
		observation.Presence = protocol.PickerRoutePresence(presence)
		if observation.Attention, err = r.getBool(); err != nil {
			return nil, err
		}
		observations = append(observations, observation)
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return observations, nil
}

func MarshalPickerControlRequest(request protocol.PickerControlRequest) []byte {
	if protocol.ValidatePickerControlRequest(request) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint16(request.Version)
	w.putUint64(request.RequestID)
	w.putUint8(uint8(request.Operation))
	w.putUint64(request.SourceRevision)
	w.putString(request.SourceID)
	w.putString(request.Key)
	if !marshalPickerControlTargets(&w, request.Targets) {
		return nil
	}
	return w.b
}

func UnmarshalPickerControlRequest(data []byte) (protocol.PickerControlRequest, error) {
	r := payloadReader{b: data}
	var request protocol.PickerControlRequest
	var err error
	if request.Version, err = r.getUint16(); err != nil {
		return request, err
	}
	if request.RequestID, err = r.getUint64(); err != nil {
		return request, err
	}
	op, err := r.getUint8()
	if err != nil {
		return request, err
	}
	request.Operation = protocol.PickerControlOperation(op)
	if request.SourceRevision, err = r.getUint64(); err != nil {
		return request, err
	}
	if request.SourceID, err = r.getString(); err != nil {
		return request, err
	}
	if request.Key, err = r.getString(); err != nil {
		return request, err
	}
	if request.Targets, err = unmarshalPickerControlTargets(&r); err != nil {
		return request, err
	}
	if err := r.done(); err != nil {
		return request, err
	}
	if err := protocol.ValidatePickerControlRequest(request); err != nil {
		return request, err
	}
	return request, nil
}

func MarshalPickerControlResponse(response protocol.PickerControlResponse) []byte {
	if protocol.ValidatePickerControlResponse(response) != nil {
		return nil
	}
	w := payloadWriter{}
	w.putUint64(response.RequestID)
	w.putUint8(uint8(response.Operation))
	w.putUint8(uint8(response.Status))
	if response.Snapshot != nil {
		w.putLongBytes(MarshalPickerSnapshot(*response.Snapshot))
	} else {
		w.putLongBytes(nil)
	}
	if response.Resolved != nil {
		payload := MarshalAttachTarget(*response.Resolved)
		w.putLongBytes(payload)
	} else {
		w.putLongBytes(nil)
	}
	if len(response.Observations) != 0 {
		inner := payloadWriter{}
		if !marshalPickerRouteObservations(&inner, response.Observations) {
			return nil
		}
		w.putLongBytes(inner.b)
	} else {
		w.putLongBytes(nil)
	}
	return w.b
}

func UnmarshalPickerControlResponse(data []byte) (protocol.PickerControlResponse, error) {
	r := payloadReader{b: data}
	var response protocol.PickerControlResponse
	var err error
	if response.RequestID, err = r.getUint64(); err != nil {
		return response, err
	}
	op, err := r.getUint8()
	if err != nil {
		return response, err
	}
	response.Operation = protocol.PickerControlOperation(op)
	status, err := r.getUint8()
	if err != nil {
		return response, err
	}
	response.Status = protocol.PickerSourceStatus(status)
	snapshotData, err := r.getLongBytes()
	if err != nil {
		return response, err
	}
	if len(snapshotData) != 0 {
		snapshot, err := UnmarshalPickerSnapshot(snapshotData)
		if err != nil {
			return response, err
		}
		response.Snapshot = &snapshot
	}
	targetData, err := r.getLongBytes()
	if err != nil {
		return response, err
	}
	if len(targetData) != 0 {
		target, err := UnmarshalAttachTarget(targetData)
		if err != nil {
			return response, err
		}
		response.Resolved = &target
	}
	observationsData, err := r.getLongBytes()
	if err != nil {
		return response, err
	}
	if len(observationsData) != 0 {
		observations, err := unmarshalPickerRouteObservations(observationsData)
		if err != nil {
			return response, err
		}
		response.Observations = observations
	}
	if err := r.done(); err != nil {
		return response, err
	}
	if err := protocol.ValidatePickerControlResponse(response); err != nil {
		return response, err
	}
	return response, nil
}
