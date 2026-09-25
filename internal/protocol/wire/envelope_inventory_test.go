package wire

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestEnvelopeInventoryProvesDirectionAndSemanticPath is the P2.1 schema
// inventory: every ClientEnvelope variant names a protocol.ClientMessage,
// every ServerEnvelope variant names a protocol.ServerMessage, neither side
// shares a payload type except route navigation failure, and the preamble
// constants agree with the schema authority.
func TestEnvelopeInventoryProvesDirectionAndSemanticPath(t *testing.T) {
	if protocol.Version != 61 {
		t.Fatalf("protocol.Version = %d, want 61", protocol.Version)
	}
	if ProtocolEpoch != 1 {
		t.Fatalf("ProtocolEpoch = %d, want 1", ProtocolEpoch)
	}
	if PreambleMagic != 0x56455631 {
		t.Fatalf("PreambleMagic = %#x, want VEV1", PreambleMagic)
	}

	clientVariants := envelopeVariants(t, (&ClientEnvelope{}).ProtoReflect())
	serverVariants := envelopeVariants(t, (&ServerEnvelope{}).ProtoReflect())

	// Closed semantic unions, in wire-variant spelling.
	clientSemantic := map[string]bool{
		"suspend_attachment": true, "activate_attachment": true,
		"hello": true, "input": true, "resize": true, "detach": true, "ping": true,
		"list": true, "kill": true, "theme": true, "ack": true, "image_push": true,
		"client_notice": true, "command_request": true, "output_reset_request": true,
		"remote_preview_watch": true, "route_attention_subscription": true,
		"same_peer_switch_request": true, "recent_route_snapshot": true,
		"route_navigation_failure": true, "session_creation_failure": true,
		"ui_fence": true, "navigation_inventory_request": true,
		"navigation_inventory_publication": true, "navigation_inventory_failure": true,
		"picker_close": true, "picker_selection": true,
		"select_tab": true,
	}
	serverSemantic := map[string]bool{
		"attachment_suspended": true, "attachment_activated": true,
		"welcome": true, "error": true, "output": true, "detached": true, "pong": true,
		"sessions": true, "command_result": true, "attach_target": true,
		"remote_preview": true, "committed_route_identity": true,
		"route_navigation_action": true, "route_create_session_action": true,
		"route_navigation_failure": true, "route_position": true, "route_retired": true,
		"same_peer_switch_failure": true, "ui_receipt": true, "ui_view_update": true,
		"navigation_inventory_response": true, "navigation_inventory_demand": true,
		"navigation_inventory_selection": true,
		"picker_offer":                   true, "picker_snapshot": true, "picker_closed": true,
		"picker_result": true, "picker_failure": true,
		"kill_result": true,
	}

	if len(clientVariants) != len(clientSemantic) {
		t.Fatalf("client envelope has %d variants, want %d (%v)", len(clientVariants), len(clientSemantic), clientVariants)
	}
	for _, name := range clientVariants {
		if !clientSemantic[name] {
			t.Fatalf("client variant %q has no semantic validation path", name)
		}
	}
	if len(serverVariants) != len(serverSemantic) {
		t.Fatalf("server envelope has %d variants, want %d (%v)", len(serverVariants), len(serverSemantic), serverVariants)
	}
	for _, name := range serverVariants {
		if !serverSemantic[name] {
			t.Fatalf("server variant %q has no semantic validation path", name)
		}
	}

	// Direction closure: exactly one shared payload may appear on both sides.
	clientSet := map[string]bool{}
	for _, name := range clientVariants {
		clientSet[name] = true
	}
	shared := []string{}
	for _, name := range serverVariants {
		if clientSet[name] {
			shared = append(shared, name)
		}
	}
	if len(shared) != 1 || shared[0] != "route_navigation_failure" {
		t.Fatalf("shared client/server payloads = %v, want [route_navigation_failure]", shared)
	}

	// Every variant must be a distinct generated message type (no generic
	// escape hatch: no Any, no shared catch-all payload).
	// Every variant within one envelope must be a distinct generated message
	// type (no generic escape hatch: no Any, no catch-all). The one shared
	// payload (route navigation failure) is allowed across envelopes only;
	// direction is enforced by which envelope wraps it.
	for _, envelope := range []proto.Message{&ClientEnvelope{}, &ServerEnvelope{}} {
		seen := map[string]string{}
		descriptor := envelope.ProtoReflect().Descriptor()
		if descriptor.Oneofs().Len() != 1 {
			t.Fatalf("envelope has %d oneofs, want exactly the payload oneof", descriptor.Oneofs().Len())
		}
		oneof := descriptor.Oneofs().Get(0)
		if string(oneof.Name()) != "payload" {
			t.Fatalf("envelope oneof %q is not the payload oneof", oneof.Name())
		}
		for j := range oneof.Fields().Len() {
			variant := oneof.Fields().Get(j)
			message := variant.Message()
			if message == nil {
				t.Fatalf("variant %q is not a message", variant.Name())
			}
			key := string(message.FullName())
			if prev, dup := seen[key]; dup {
				t.Fatalf("payload type %v shared by %q and %q", key, prev, variant.Name())
			}
			seen[key] = string(variant.Name())
			if variant.Number() == 0 {
				t.Fatalf("variant %q has no field number", variant.Name())
			}
		}
	}
}

func envelopeVariants(t *testing.T, message protoreflect.Message) []string {
	t.Helper()
	descriptor := message.Descriptor()
	if descriptor.Oneofs().Len() != 1 {
		t.Fatalf("envelope has %d oneofs, want exactly the payload oneof", descriptor.Oneofs().Len())
	}
	oneof := descriptor.Oneofs().Get(0)
	if string(oneof.Name()) != "payload" {
		t.Fatalf("envelope oneof %q is not the payload oneof", oneof.Name())
	}
	var names []string
	for i := range oneof.Fields().Len() {
		names = append(names, strings.ToLower(string(oneof.Fields().Get(i).Name())))
	}
	return names
}
