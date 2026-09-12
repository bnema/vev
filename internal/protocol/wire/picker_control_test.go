package wire

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestPickerControlRequestRoundTripStrict(t *testing.T) {
	request := protocol.PickerControlRequest{Version: protocol.Version, RequestID: 7, Operation: protocol.PickerControlResolve, SourceRevision: 3, SourceID: protocol.PickerHomeSourceID, Key: "a/b"}
	payload := MarshalPickerControlRequest(request)
	require.NotNil(t, payload)
	decoded, err := UnmarshalPickerControlRequest(payload)
	require.NoError(t, err)
	require.Equal(t, request, decoded)
	assertAllPrefixesFail(t, payload, UnmarshalPickerControlRequest)
	assertTrailingGarbageFails(t, payload, UnmarshalPickerControlRequest)
}

func TestPickerControlResponseRoundTripStrict(t *testing.T) {
	snapshot := pickerSnapshotForTest()
	snapshot.SourceID = protocol.PickerHomeSourceID
	snapshot = protocol.NormalizePickerSnapshot(snapshot)
	response := protocol.PickerControlResponse{RequestID: 8, Operation: protocol.PickerControlSnapshot, Status: protocol.PickerSourceOK, Snapshot: &snapshot}
	payload := MarshalPickerControlResponse(response)
	require.NotNil(t, payload)
	decoded, err := UnmarshalPickerControlResponse(payload)
	require.NoError(t, err)
	require.Equal(t, response, decoded)
	assertAllPrefixesFail(t, payload, UnmarshalPickerControlResponse)
	assertTrailingGarbageFails(t, payload, UnmarshalPickerControlResponse)
}

func pickerObserveTargetsForTest(n int) []protocol.ExactSessionTarget {
	targets := make([]protocol.ExactSessionTarget, 0, n)
	for i := range n {
		targets = append(targets, protocol.ExactSessionTarget{
			LifecycleID: domain.SessionLifecycleID{byte(i + 1)},
			SessionName: "work",
		})
	}
	return targets
}

func TestPickerControlObserveRequestRoundTripStrict(t *testing.T) {
	request := protocol.PickerControlRequest{
		Version:   protocol.Version,
		RequestID: 11,
		Operation: protocol.PickerControlObserve,
		Targets:   pickerObserveTargetsForTest(2),
	}
	payload := MarshalPickerControlRequest(request)
	require.NotNil(t, payload)
	decoded, err := UnmarshalPickerControlRequest(payload)
	require.NoError(t, err)
	require.Equal(t, request, decoded)
	assertAllPrefixesFail(t, payload, UnmarshalPickerControlRequest)
	assertTrailingGarbageFails(t, payload, UnmarshalPickerControlRequest)
}

func TestPickerControlObserveResponseRoundTripStrict(t *testing.T) {
	targets := pickerObserveTargetsForTest(3)
	response := protocol.PickerControlResponse{
		RequestID: 11,
		Operation: protocol.PickerControlObserve,
		Status:    protocol.PickerSourceOK,
		Observations: []protocol.PickerRouteObservation{
			{Target: targets[0], Presence: protocol.PickerRoutePresent, Attention: true},
			{Target: targets[1], Presence: protocol.PickerRouteAbsent},
			{Target: targets[2], Presence: protocol.PickerRouteUnknown},
		},
	}
	payload := MarshalPickerControlResponse(response)
	require.NotNil(t, payload)
	decoded, err := UnmarshalPickerControlResponse(payload)
	require.NoError(t, err)
	require.Equal(t, response, decoded)
	assertAllPrefixesFail(t, payload, UnmarshalPickerControlResponse)
	assertTrailingGarbageFails(t, payload, UnmarshalPickerControlResponse)
}

func TestPickerControlRejectsExclusivePayloadViolations(t *testing.T) {
	targets := pickerObserveTargetsForTest(1)
	// Inspect and resolve never carry targets; observe never carries source
	// identity.
	require.Nil(t, MarshalPickerControlRequest(protocol.PickerControlRequest{
		Version: protocol.Version, RequestID: 1, Operation: protocol.PickerControlSnapshot, Targets: targets,
	}))
	require.Nil(t, MarshalPickerControlRequest(protocol.PickerControlRequest{
		Version: protocol.Version, RequestID: 1, Operation: protocol.PickerControlResolve, SourceRevision: 1,
		SourceID: protocol.PickerHomeSourceID, Key: "a/b", Targets: targets,
	}))
	require.Nil(t, MarshalPickerControlRequest(protocol.PickerControlRequest{
		Version: protocol.Version, RequestID: 1, Operation: protocol.PickerControlObserve,
		SourceID: protocol.PickerHomeSourceID, Targets: targets,
	}))
	snapshot := pickerSnapshotForTest()
	snapshot.SourceID = protocol.PickerHomeSourceID
	// A snapshot response never carries observations.
	require.Nil(t, MarshalPickerControlResponse(protocol.PickerControlResponse{
		RequestID: 1, Operation: protocol.PickerControlSnapshot, Status: protocol.PickerSourceOK, Snapshot: &snapshot,
		Observations: []protocol.PickerRouteObservation{{Target: targets[0], Presence: protocol.PickerRoutePresent}},
	}))
}

func pickerObserveResponseEnvelope(observations []byte) []byte {
	w := payloadWriter{}
	w.putUint64(1)
	w.putUint8(uint8(protocol.PickerControlObserve))
	w.putUint8(uint8(protocol.PickerSourceOK))
	w.putLongBytes(nil)
	w.putLongBytes(nil)
	w.putLongBytes(observations)
	return w.b
}

func TestPickerControlObserveWireRejectsUnboundedOrEmptyPayloads(t *testing.T) {
	// An observe request claiming more targets than the bound fails during
	// decoding, before semantic validation ever runs.
	request := payloadWriter{}
	request.putUint16(protocol.Version)
	request.putUint64(1)
	request.putUint8(uint8(protocol.PickerControlObserve))
	request.putUint64(0)
	request.putString("")
	request.putString("")
	request.putUint16(protocol.PickerControlMaxTargets + 1)
	_, err := UnmarshalPickerControlRequest(request.b)
	require.Error(t, err)

	// A missing observation payload cannot answer an observe request.
	_, err = UnmarshalPickerControlResponse(pickerObserveResponseEnvelope(nil))
	require.Error(t, err)

	// A present but empty observation payload is rejected: the exclusive union
	// member is set and must carry at least one observation.
	empty := payloadWriter{}
	empty.putUint16(0)
	_, err = UnmarshalPickerControlResponse(pickerObserveResponseEnvelope(empty.b))
	require.Error(t, err)

	// An observation payload claiming more entries than the bound is rejected
	// at the nested decode boundary.
	overlong := payloadWriter{}
	overlong.putUint16(protocol.PickerControlMaxTargets + 1)
	_, err = UnmarshalPickerControlResponse(pickerObserveResponseEnvelope(overlong.b))
	require.Error(t, err)
}
