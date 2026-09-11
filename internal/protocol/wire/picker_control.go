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
	if err := r.done(); err != nil {
		return response, err
	}
	if err := protocol.ValidatePickerControlResponse(response); err != nil {
		return response, err
	}
	return response, nil
}
