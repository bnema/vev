package wire

import (
	"testing"

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
