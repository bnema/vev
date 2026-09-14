package sessionwire

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestAttachmentEnvelopes(t *testing.T) {
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}
	for _, tt := range []struct {
		name    string
		message any
		golden  string
	}{
		{"suspend", protocol.SuspendAttachment{RequestID: 1}, "ea01020801"},
		{"suspended", protocol.AttachmentSuspended{RequestID: 1, Target: target}, "fa011e0801121a0a120a10010000000000000000000000000000001204776f726b"},
		{"activate", protocol.ActivateAttachment{RequestID: 2, Target: target, Size: domain.Size{Cols: 80, Rows: 24}}, "f201220802121a0a120a10010000000000000000000000000000001204776f726b18502018"},
		{"activated", protocol.AttachmentActivated{RequestID: 2, Identity: protocol.CommittedRouteIdentity{Target: target}, Epoch: 2, State: 1, ViewPublication: 3}, "8202260802121c0a1a0a120a10010000000000000000000000000000001204776f726b180220012803"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var raw []byte
			var err error
			var decode func([]byte) (any, error)
			var wrong func([]byte) (any, error)
			if m, ok := tt.message.(protocol.ClientMessage); ok {
				raw, err = EncodeClientMessage(m)
				decode = func(b []byte) (any, error) { return DecodeClientEnvelope(b) }
				wrong = func(b []byte) (any, error) { return DecodeServerEnvelope(b) }
			} else {
				raw, err = EncodeServerMessage(tt.message.(protocol.ServerMessage))
				decode = func(b []byte) (any, error) { return DecodeServerEnvelope(b) }
				wrong = func(b []byte) (any, error) { return DecodeClientEnvelope(b) }
			}
			require.NoError(t, err)
			require.Equal(t, tt.golden, hex.EncodeToString(raw))
			got, err := decode(raw)
			require.NoError(t, err)
			require.Equal(t, tt.message, got)
			_, err = wrong(raw)
			require.Error(t, err)
			for i := 0; i < len(raw); i++ {
				_, err = decode(raw[:i])
				require.Error(t, err, "prefix %d", i)
			}
			for _, tail := range [][]byte{{0}, {0x80}, raw, {0xf8, 0x07, 0x01}} {
				_, err = decode(append(append([]byte(nil), raw...), tail...))
				require.Error(t, err)
			}
		})
	}
}

// Clearing any required field must fail on both conversion directions. Direct
// protobuf marshal deliberately bypasses the semantic encoder for hostile input.
func TestAttachmentRequiredFields(t *testing.T) {
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}
	for _, message := range []any{
		protocol.SuspendAttachment{RequestID: 1},
		protocol.AttachmentSuspended{RequestID: 1, Target: target},
		protocol.ActivateAttachment{RequestID: 2, Target: target, Size: domain.Size{Cols: 80, Rows: 24}},
		protocol.AttachmentActivated{RequestID: 2, Identity: protocol.CommittedRouteIdentity{Target: target}, Epoch: 2, State: 1, ViewPublication: 3},
	} {
		t.Run(reflect.TypeOf(message).Name(), func(t *testing.T) {
			value := reflect.ValueOf(message)
			// Pointer forms share the same validated path; typed nil pointers fail.
			pointer := reflect.New(value.Type())
			pointer.Elem().Set(value)
			nilPointer := reflect.Zero(pointer.Type()).Interface()
			encode := func(v any) ([]byte, error) {
				if m, ok := v.(protocol.ClientMessage); ok {
					return EncodeClientMessage(m)
				}
				return EncodeServerMessage(v.(protocol.ServerMessage))
			}
			_, err := encode(pointer.Interface())
			require.NoError(t, err)
			_, err = encode(nilPointer)
			require.Error(t, err)
			for i := 0; i < value.NumField(); i++ {
				if value.Type().Field(i).Name == "PixelWidth" || value.Type().Field(i).Name == "PixelHeight" {
					continue
				}
				bad := reflect.New(value.Type()).Elem()
				bad.Set(value)
				bad.Field(i).SetZero()
				_, err = encode(bad.Interface())
				require.Error(t, err, value.Type().Field(i).Name)
			}
			var envelope proto.Message
			var decode func([]byte) (any, error)
			if m, ok := message.(protocol.ClientMessage); ok {
				envelope, err = encodeProtoClient(m)
				decode = func(b []byte) (any, error) { return DecodeClientEnvelope(b) }
			} else {
				envelope, err = encodeProtoServer(message.(protocol.ServerMessage))
				decode = func(b []byte) (any, error) { return DecodeServerEnvelope(b) }
			}
			require.NoError(t, err)
			descriptor := envelope.ProtoReflect().Descriptor().Oneofs().Get(0)
			variant := envelope.ProtoReflect().WhichOneof(descriptor)
			payload := envelope.ProtoReflect().Get(variant).Message()
			payload.Range(func(field protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
				clone := proto.Clone(envelope)
				clone.ProtoReflect().Get(variant).Message().Clear(field)
				raw, err := proto.Marshal(clone)
				require.NoError(t, err)
				_, err = decode(raw)
				require.Error(t, err, string(field.Name()))
				return true
			})
		})
	}
}

func TestAttachmentActivationRejectsMalformedGeometryAndIdentity(t *testing.T) {
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}
	for _, mutate := range []func(*wire.ActivateAttachment){
		func(m *wire.ActivateAttachment) { m.Cols = 1 << 16 },
		func(m *wire.ActivateAttachment) { m.Rows = 1 << 16 },
		func(m *wire.ActivateAttachment) { m.PixelWidth = 1 },
		func(m *wire.ActivateAttachment) { m.PixelWidth = 1 << 16; m.PixelHeight = 1 },
		func(m *wire.ActivateAttachment) { m.Target.LifecycleId.Value = []byte{1} },
		func(m *wire.ActivateAttachment) { m.Target.SessionName = "" },
	} {
		m := &wire.ActivateAttachment{RequestId: 2, Target: exactTargetToWire(&target), Cols: 80, Rows: 24}
		mutate(m)
		raw, err := proto.Marshal(&wire.ClientEnvelope{Payload: &wire.ClientEnvelope_ActivateAttachment{ActivateAttachment: m}})
		require.NoError(t, err)
		_, err = DecodeClientEnvelope(raw)
		require.Error(t, err)
	}
}

// This is a typed connection contract test, not a daemon lifecycle test. The
// scripted peer supplies the barrier and full-publication ordering Phase 2
// must enforce in the real daemon.
func TestAttachmentTypedConnectionSequence(t *testing.T) {
	context := testUIOutputContext()
	target := context.Route.Target
	suspend := protocol.SuspendAttachment{RequestID: 10}
	suspended := protocol.AttachmentSuspended{RequestID: 10, Target: target}
	activate := protocol.ActivateAttachment{RequestID: 11, Target: target, Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480}
	full := protocol.Output{Epoch: 2, New: 1, Full: true, Size: activate.Size, Context: context, Data: []byte("full")}
	activated := protocol.AttachmentActivated{RequestID: 11, Identity: context.Route, Epoch: full.Epoch, State: full.New, ViewPublication: context.Publication}
	clientRaw := &scriptedTransport{recv: []wire.Envelope{mustPreambleResponse(t)}}
	serverRaw := &scriptedTransport{recv: []wire.Envelope{mustPreambleRequest(t)}}
	client := NewClientConnection(clientRaw)
	server := NewServerConnection(serverRaw)
	t.Cleanup(func() { require.NoError(t, client.Close()); require.NoError(t, server.Close()) })
	sendClient := func(message protocol.ClientMessage) {
		require.NoError(t, client.SendClient(message))
		serverRaw.recv = append(serverRaw.recv, wire.Envelope{Payload: clientRaw.sentPayload(clientRaw.sentLen() - 1)})
		got, err := server.ReceiveClient()
		require.NoError(t, err)
		require.Equal(t, message, got)
	}
	sendServer := func(message protocol.ServerMessage) {
		require.NoError(t, server.SendServer(message))
		clientRaw.recv = append(clientRaw.recv, wire.Envelope{Payload: serverRaw.sentPayload(serverRaw.sentLen() - 1)})
		got, err := client.ReceiveServer()
		require.NoError(t, err)
		require.Equal(t, message, got)
	}
	sendClient(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Name: target.SessionName, ExactTarget: &target, Size: activate.Size})
	sendServer(protocol.Welcome{SessionID: "session", SessionName: target.SessionName, CommittedIdentity: &context.Route})
	initial := full
	initial.Epoch = 1
	sendServer(initial)
	sendClient(suspend)
	sendServer(suspended)
	sendClient(protocol.Ping{})
	sendServer(protocol.Pong{})
	sendClient(activate)
	sendServer(full)
	sendServer(activated)
	require.NotEqual(t, initial.Epoch, activated.Epoch)
	require.Equal(t, activate.RequestID, activated.RequestID)
	require.Equal(t, activate.Target, activated.Identity.Target)
	sendClient(protocol.Input{InputSeq: 1, Data: []byte("x")})
}
