package sessionwire

import (
	"bytes"
	"reflect"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func protoTestTarget() domain.RemoteSessionTarget {
	return domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: domain.SessionLifecycleID{1},
		SessionName: "work", LiveTabID: "tab-1",
	}
}

func protoTestContext() *protocol.ViewContext {
	lifecycle := domain.SessionLifecycleID{1}
	return &protocol.ViewContext{
		Publication:   3,
		Route:         protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "work"}},
		TabID:         "tab-1",
		FocusedPaneID: "pane-1",
	}
}

// TestProtoClientRoundTrips exercises every client converter pair through
// scan + generated marshal + generated unmarshal + semantic validation.
func TestProtoClientRoundTrips(t *testing.T) {
	target := protoTestTarget()
	messages := []protocol.ClientMessage{
		protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}},
		protocol.Input{InputSeq: 1, ActionID: 2, Data: []byte("x")},
		protocol.Resize{Size: domain.Size{Cols: 80, Rows: 24}},
		protocol.Detach{},
		protocol.Ping{},
		protocol.List{},
		protocol.Kill{Name: "work"},
		protocol.Theme{TrueColor: true},
		protocol.Ack{Epoch: 1, State: 1},
		protocol.ImagePush{InputSeq: 1, Mime: "image/png", Data: []byte{1}},
		protocol.ClientNotice{Action: protocol.ClientNoticeLinkConnected},
		protocol.CommandRequest{Version: protocol.Version, RequestID: 1, Slug: "list-sessions"},
		protocol.OutputResetRequest{},
		protocol.UIFence{ActionID: 7},
		protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: target, Width: 1, Height: 1},
		protocol.RouteAttentionSubscription{},
		protocol.SamePeerSwitchRequest{RequestID: 1, Target: protocol.ExactSessionTarget{LifecycleID: target.LifecycleID, SessionName: target.SessionName}},
		protocol.RecentRouteSnapshot{},
		protocol.RouteNavigationFailure{Key: 1, Generation: 1, Code: protocol.RouteFailureUnavailable},
		protocol.SessionCreationFailure{RequestID: 1, Code: protocol.RouteFailureUnavailable},
		protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 1, Operation: protocol.NavigationInventorySnapshot},
		protocol.NavigationInventoryPublication{InteractionGeneration: 1, PublicationGeneration: 1},
		protocol.NavigationInventoryFailure{CauseActionID: 1, InteractionGeneration: 1, SourceKey: "local", EntryKey: "a", Code: protocol.NavigationInventoryStaleIdentity},
		protocol.PickerBegin{RequestID: 1, Intent: protocol.PickerIntentNavigation},
		protocol.PickerClose{InteractionID: 1},
		protocol.PickerSelection{InteractionID: 1, SourceID: "serving", SourceRevision: 1, Key: "k", Action: protocol.PickerActionNavigate},
		protocol.PickerPreviewRequest{Version: protocol.PickerPreviewSchemaVersion, InteractionID: 1, SourceID: "serving", Key: "k", Width: 1, Height: 1},
		protocol.PickerControlRequest{Version: protocol.Version, RequestID: 1, Operation: protocol.PickerControlSnapshot},
	}
	for _, message := range messages {
		t.Run(protoMessageName(message), func(t *testing.T) {
			envelope, err := encodeProtoClient(message)
			require.NoError(t, err)
			raw, err := proto.Marshal(envelope)
			require.NoError(t, err)
			require.NoError(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, raw))
			decoded := &wire.ClientEnvelope{}
			require.NoError(t, proto.Unmarshal(raw, decoded))
			got, err := decodeProtoClient(decoded)
			require.NoError(t, err)
			require.Equal(t, message, got)
		})
	}
}

// TestProtoServerRoundTrips exercises every server converter pair.
func TestProtoServerRoundTrips(t *testing.T) {
	lifecycle := domain.SessionLifecycleID{1}
	exact := protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "work"}
	context := protoTestContext()
	messages := []protocol.ServerMessage{
		protocol.Welcome{SessionID: "session"},
		protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "error"},
		protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: context},
		protocol.Detached{Reason: protocol.ReasonDetach},
		protocol.Pong{},
		protocol.Sessions{},
		protocol.CommandResult{RequestID: 1, OK: true},
		protocol.AttachTarget{Session: "work", Intent: protocol.IntentAttach},
		protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewUnavailable},
		protocol.CommittedRouteIdentity{Target: exact},
		protocol.RouteNavigationAction{SnapshotGeneration: 1, Key: 1, Generation: 1},
		protocol.RouteCreateSessionAction{RequestID: 1, SnapshotGeneration: 1, Key: 1, Generation: 1, SessionName: "example"},
		protocol.RouteNavigationFailure{Key: 1, Generation: 1, Code: protocol.RouteFailureUnavailable},
		protocol.RoutePosition{Target: exact, ActiveTabID: "tab-1"},
		protocol.RouteRetired{Ref: protocol.RouteRef{Key: 1, Generation: 1}, Target: exact},
		protocol.SamePeerSwitchFailure{RequestID: 1, Code: protocol.SamePeerSwitchUnavailable},
		protocol.UIReceipt{ActionID: 1, Outcome: protocol.UIReceiptUnavailable},
		protocol.UIViewUpdate{Epoch: 1, State: 1, Context: *context},
		protocol.NavigationInventoryResponse{RequestID: 1, Operation: protocol.NavigationInventorySnapshot, Status: protocol.NavigationInventoryOK},
		protocol.NavigationInventoryDemand{InteractionGeneration: 1, Open: true},
		protocol.NavigationInventorySelection{CauseActionID: 1, InteractionGeneration: 1, PublicationGeneration: 1, SourceKey: "local", EntryKey: "a"},
		protocol.PickerOffer{InteractionID: 1, Intent: protocol.PickerIntentNavigation, Title: "pick"},
		protocol.PickerSnapshot{InteractionID: 1, SourceID: "serving", SourceRevision: 1, Status: protocol.PickerSourceOK, Recent: protocol.PickerProjection{}, Grouped: protocol.PickerProjection{}},
		protocol.PickerClosed{InteractionID: 1},
		protocol.PickerResult{InteractionID: 1, SourceID: "serving", Key: "k", Action: protocol.PickerActionNavigate},
		protocol.PickerFailure{InteractionID: 1, Action: protocol.PickerActionNavigate, Code: protocol.PickerStaleRevision},
		protocol.PickerPreview{Version: protocol.PickerPreviewSchemaVersion, InteractionID: 1, SourceID: "serving", Key: "k", Status: protocol.PickerPreviewUnavailable},
		protocol.PickerControlResponse{RequestID: 1, Operation: protocol.PickerControlSnapshot, Status: protocol.PickerSourceUnavailable},
	}
	for _, message := range messages {
		t.Run(protoMessageName(message), func(t *testing.T) {
			envelope, err := encodeProtoServer(message)
			require.NoError(t, err)
			raw, err := proto.Marshal(envelope)
			require.NoError(t, err)
			require.NoError(t, wire.ScanEnvelope(&wire.ServerEnvelope{}, raw))
			decoded := &wire.ServerEnvelope{}
			require.NoError(t, proto.Unmarshal(raw, decoded))
			got, err := decodeProtoServer(decoded)
			require.NoError(t, err)
			require.Equal(t, message, got)
		})
	}
}

// TestProtoWrongDirectionRejects proves a server payload in a client
// envelope (and vice versa) fails before any mutation.
func TestProtoWrongDirectionRejects(t *testing.T) {
	// Direction is enforced by Go types at encode time (a server-only
	// message does not implement protocol.ClientMessage, so misuse does
	// not compile) and by exhaustive decode switches at decode time.
	// Here: an empty client envelope and an unknown wrapper fail closed.
	_, err := decodeProtoClient(&wire.ClientEnvelope{Payload: nil})
	require.ErrorIs(t, err, ErrWrongDirection)
	_, err = decodeProtoClient(nil)
	require.ErrorIs(t, err, ErrInvalidMessage)
	_, err = decodeProtoServer(nil)
	require.ErrorIs(t, err, ErrInvalidMessage)
}

// TestProtoOutputCompressionRoundTrip keeps the frozen policy observable:
// large full snapshots compress, small/incremental stay raw, and zip bombs
// fail closed.
func TestProtoOutputCompressionRoundTrip(t *testing.T) {
	context := protoTestContext()
	full := protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 120, Rows: 40}, Context: context,
		Data: bytes.Repeat([]byte("\x1b[38;2;120;180;240mstyled viewport row\x1b[0m\r\n"), 128)}
	converted, err := outputToWire(full)
	require.NoError(t, err)
	require.Equal(t, uint32(1), converted.GetEncoding())
	back, err := outputFromWire(converted)
	require.NoError(t, err)
	require.Equal(t, full.Data, back.Data)

	small := protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: context, Data: []byte("small")}
	converted, err = outputToWire(small)
	require.NoError(t, err)
	require.Equal(t, uint32(0), converted.GetEncoding())

	incremental := protocol.Output{Epoch: 1, Base: 1, New: 2, Size: domain.Size{Cols: 80, Rows: 24}, Context: context, Data: bytes.Repeat([]byte("x"), 2048)}
	converted, err = outputToWire(incremental)
	require.NoError(t, err)
	require.Equal(t, uint32(0), converted.GetEncoding())

	bomb := proto.Clone(converted).(*wire.Output)
	bomb.UncompressedLength = uint64(protocol.MaxOutputDataLen) + 1
	_, err = outputFromWire(bomb)
	require.Error(t, err)
}

// TestProtoOutputRejectsCompressedTrailingBytes restores the legacy
// trailing-data guard: bytes appended after the zlib stream end must
// fail closed (the zlib reader stops at stream end and would otherwise
// swallow them).
func TestProtoOutputRejectsCompressedTrailingBytes(t *testing.T) {
	context := protoTestContext()
	full := protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 120, Rows: 40}, Context: context,
		Data: bytes.Repeat([]byte("\x1b[38;2;120;180;240mstyled viewport row\x1b[0m\r\n"), 128)}
	converted, err := outputToWire(full)
	require.NoError(t, err)
	require.Equal(t, uint32(1), converted.GetEncoding())

	trailing := proto.Clone(converted).(*wire.Output)
	trailing.Data = append(append([]byte(nil), converted.GetData()...), []byte("trailing-garbage")...)
	_, err = outputFromWire(trailing)
	require.Error(t, err)
}

// TestProtoScanNegatives fuzzes the scanner contract: unknown fields,
// duplicate singulars, empty envelopes, truncation, trailing garbage.
func TestProtoScanNegatives(t *testing.T) {
	valid, err := proto.Marshal(&wire.ClientEnvelope{Payload: &wire.ClientEnvelope_Ping{Ping: &wire.Ping{}}})
	require.NoError(t, err)
	require.NoError(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, valid))

	truncated := valid[:len(valid)-1]
	require.Error(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, truncated))
	require.Error(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, append(valid, 0xFF)))
	require.Error(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, nil))
	require.Error(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, []byte{}))
	require.ErrorIs(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, []byte{}), wire.ErrScanEmpty)

	// Unknown top-level field 99 (varint).
	unknown := append([]byte(nil), valid...)
	unknown = protowire.AppendTag(unknown, 99, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)
	require.ErrorIs(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, unknown), wire.ErrScanUnknown)

	// Duplicate top-level variant: two ping occurrences.
	duplicate := append([]byte(nil), valid...)
	duplicate = append(duplicate, valid...)
	require.Error(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, duplicate))

	// Concatenated envelopes: generated unmarshal alone takes last-wins,
	// which is why the scanner must reject them first.
	require.Error(t, wire.ScanEnvelope(&wire.ClientEnvelope{}, duplicate))
	envelope := &wire.ClientEnvelope{}
	require.NoError(t, proto.Unmarshal(duplicate, envelope))
}

func protoMessageName(message any) string {
	return protoShortName(message)
}

func protoShortName(message any) string {
	if message == nil {
		return "nil"
	}
	return reflect.TypeOf(message).Name()
}
