package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func TestFinalProtocolVersionAndNoV21Hello(t *testing.T) {
	if protocol.Version != 41 {
		t.Fatalf("ProtocolVersion = %d, want 41", protocol.Version)
	}
	payload := MarshalHello(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}})
	if len(payload) < 2 {
		t.Fatal("MarshalHello produced a truncated valid payload")
	}
	binary.BigEndian.PutUint16(payload, 21)
	if _, err := UnmarshalHello(payload); !errors.Is(err, protocol.ErrInvalidHello) {
		t.Fatalf("UnmarshalHello(v21) error = %v, want ErrInvalidHello", err)
	}
}

func TestFinalHelloGoldenStrict(t *testing.T) {
	msg := protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}
	want := append([]byte{0, 41, 2}, make([]byte, 16+8)...)
	want = append(want, 0, 0, 0, 80, 0, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	want = append(want, 0, 0)
	got := MarshalHello(msg)
	if !bytes.Equal(got, want) {
		t.Fatalf("Hello bytes = %x, want %x", got, want)
	}
	back, err := UnmarshalHello(got)
	if err != nil {
		t.Fatalf("UnmarshalHello() error = %v", err)
	}
	if !reflect.DeepEqual(back, msg) {
		t.Fatalf("Hello = %+v, want %+v", back, msg)
	}
	assertAllPrefixesFail(t, got, UnmarshalHello)
	assertTrailingGarbageFails(t, got, UnmarshalHello)
}

func TestHelloDeclaresKittyDirectGraphicsAtWireTail(t *testing.T) {
	msg := protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}, KittyDirectGraphics: true}
	payload := MarshalHello(msg)
	if len(payload) == 0 || payload[len(payload)-1] != 1 {
		t.Fatalf("Hello capability tail = %x, want trailing true declaration", payload)
	}
	decoded, err := UnmarshalHello(payload)
	if err != nil {
		t.Fatalf("UnmarshalHello() error = %v", err)
	}
	if !decoded.KittyDirectGraphics {
		t.Fatal("decoded KittyDirectGraphics = false, want true")
	}
}

func TestFinalHelloSemanticValidation(t *testing.T) {
	valid := protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}
	for _, tt := range []struct {
		name   string
		mutate func(*protocol.Hello)
	}{
		{name: "intent", mutate: func(h *protocol.Hello) { h.Intent = 99 }},
		{name: "zero columns", mutate: func(h *protocol.Hello) { h.Size.Cols = 0 }},
		{name: "zero rows", mutate: func(h *protocol.Hello) { h.Size.Rows = 0 }},
		{name: "area", mutate: func(h *protocol.Hello) { h.Size = domain.Size{Cols: 513, Rows: 512} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			msg := valid
			tt.mutate(&msg)
			if err := protocol.ValidateHello(msg); err == nil {
				t.Fatal("ValidateHello accepted malformed semantics")
			}
		})
	}
}

func TestFinalResizeGoldenStrict(t *testing.T) {
	msg := protocol.Resize{Size: domain.Size{Cols: 80, Rows: 24}}
	want := []byte{0, 80, 0, 24, 0, 0, 0, 0}
	got, err := MarshalResize(msg)
	if err != nil {
		t.Fatalf("MarshalResize() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Resize bytes = %x, want %x", got, want)
	}
	back, err := UnmarshalResize(got)
	if err != nil || back != msg {
		t.Fatalf("Resize = %+v, error %v, want %+v", back, err, msg)
	}
	assertAllPrefixesFail(t, got, UnmarshalResize)
	assertTrailingGarbageFails(t, got, UnmarshalResize)
	for _, size := range []domain.Size{{}, {Cols: 513, Rows: 512}} {
		if got, err := MarshalResize(protocol.Resize{Size: size}); err == nil || got != nil {
			t.Fatalf("MarshalResize(%+v) = (%x, %v), want nil payload and error", size, got, err)
		}
	}
}

func TestFinalOutputGoldenStrict(t *testing.T) {
	msg := protocol.Output{
		Epoch: 1, Base: 0, New: 7, Echo: 8, ViewRevision: 9,
		Size: domain.Size{Cols: 80, Rows: 24}, Full: true, Data: []byte("ok"),
	}
	want := []byte{
		0, 0, 0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 7,
		0, 0, 0, 0, 0, 0, 0, 8,
		0, 0, 0, 0, 0, 0, 0, 9,
		0, 80, 0, 24, 1, 0,
		0, 0, 0, 0, 2,
		0, 0, 0, 2, 'o', 'k',
	}
	got, err := MarshalOutput(msg)
	if err != nil {
		t.Fatalf("MarshalOutput() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Output bytes = %x, want %x", got, want)
	}
	back, err := UnmarshalOutput(got)
	if err != nil || back.Epoch != msg.Epoch || back.Base != msg.Base || back.New != msg.New || back.Echo != msg.Echo || back.ViewRevision != msg.ViewRevision || back.Size != msg.Size || back.Full != msg.Full || !bytes.Equal(back.Data, msg.Data) {
		t.Fatalf("Output = %+v, error %v, want %+v", back, err, msg)
	}
	assertAllPrefixesFail(t, got, UnmarshalOutput)
	assertTrailingGarbageFails(t, got, UnmarshalOutput)
}

func TestFinalOutputSemanticValidationBeforeDataAllocation(t *testing.T) {
	valid := protocol.Output{Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 80, Rows: 24}, Full: true}
	for _, bad := range []protocol.Output{
		{},
		{Epoch: 1, Base: 0, New: 1, Size: valid.Size},
		{Epoch: 1, Base: 0, New: 1, Full: true},
	} {
		if got, err := MarshalOutput(bad); err == nil || got != nil {
			t.Fatalf("MarshalOutput(%+v) = (%x, %v), want nil payload and error", bad, got, err)
		}
	}
	payload, err := MarshalOutput(valid)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "zero epoch", mutate: func(b []byte) { b[7] = 0 }},
		{name: "new does not advance", mutate: func(b []byte) { binary.BigEndian.PutUint64(b[16:24], 0) }},
		{name: "size zero", mutate: func(b []byte) { binary.BigEndian.PutUint16(b[40:42], 0) }},
		{name: "full flag false for reset", mutate: func(b []byte) { b[44] = 0 }},
		{name: "invalid bool", mutate: func(b []byte) { b[44] = 2 }},
		{name: "impossible data length", mutate: func(b []byte) { binary.BigEndian.PutUint32(b[46:50], ^uint32(0)) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bad := append([]byte(nil), payload...)
			tt.mutate(bad)
			if _, err := UnmarshalOutput(bad); err == nil {
				t.Fatal("UnmarshalOutput accepted malformed semantics")
			}
		})
	}
}

func TestFinalAckGoldenStrict(t *testing.T) {
	msg := protocol.Ack{Epoch: 2, State: 7}
	want := []byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 7}
	got, err := MarshalAck(msg)
	if err != nil {
		t.Fatalf("MarshalAck() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Ack bytes = %x, want %x", got, want)
	}
	back, err := UnmarshalAck(got)
	if err != nil || back != (protocol.Ack{Epoch: 2, State: 7}) {
		t.Fatalf("Ack = %+v, error %v", back, err)
	}
	assertAllPrefixesFail(t, got, UnmarshalAck)
	assertTrailingGarbageFails(t, got, UnmarshalAck)
	if _, err := UnmarshalAck(make([]byte, 16)); err == nil {
		t.Fatal("UnmarshalAck accepted zero epoch")
	}
	if got, err := MarshalAck(protocol.Ack{State: 7}); err == nil || got != nil {
		t.Fatalf("MarshalAck accepted zero epoch: payload=%x err=%v", got, err)
	}
}

func TestFinalAttachTargetGoldenStrict(t *testing.T) {
	msg := protocol.AttachTarget{Endpoint: "host", Session: "work", Intent: protocol.IntentAttach}
	want := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 'h', 'o', 's', 't', 0, 4, 'w', 'o', 'r', 'k', 2, 0, 0, 0, 0, 0, 0}
	want = append(want, make([]byte, 8)...)
	got := MarshalAttachTarget(msg)
	if !bytes.Equal(got, want) {
		t.Fatalf("AttachTarget bytes = %x, want %x", got, want)
	}
	back, err := UnmarshalAttachTarget(got)
	if err != nil || back != msg {
		t.Fatalf("AttachTarget = %+v, error %v, want %+v", back, err, msg)
	}
	assertAllPrefixesFail(t, got, UnmarshalAttachTarget)
	assertTrailingGarbageFails(t, got, UnmarshalAttachTarget)
	local := protocol.AttachTarget{Session: "work", Intent: protocol.IntentAttach}
	localPayload := MarshalAttachTarget(local)
	if localPayload == nil {
		t.Fatal("MarshalAttachTarget rejected same-peer route handoff")
	}
	wantLocal := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 'w', 'o', 'r', 'k', protocol.IntentAttach, 0, 0, 0, 0, 0, 0}
	wantLocal = append(wantLocal, make([]byte, 8)...)
	if !bytes.Equal(localPayload, wantLocal) {
		t.Fatalf("same-peer bytes = %x, want %x", localPayload, wantLocal)
	}
	decodedLocal, err := UnmarshalAttachTarget(localPayload)
	if err != nil || decodedLocal != local {
		t.Fatalf("same-peer target = %+v, error %v, want %+v", decodedLocal, err, local)
	}
	assertAllPrefixesFail(t, localPayload, UnmarshalAttachTarget)
	assertTrailingGarbageFails(t, localPayload, UnmarshalAttachTarget)
	creation := protocol.AttachTarget{RequestID: 7, Endpoint: "host", Session: "example", Intent: protocol.IntentNew, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned}
	creationPayload := MarshalAttachTarget(creation)
	if creationPayload == nil {
		t.Fatal("MarshalAttachTarget rejected correlated remote creation")
	}
	decodedCreation, err := UnmarshalAttachTarget(creationPayload)
	if err != nil || decodedCreation != creation {
		t.Fatalf("creation target = %+v, error %v, want %+v", decodedCreation, err, creation)
	}
	assertAllPrefixesFail(t, creationPayload, UnmarshalAttachTarget)
	assertTrailingGarbageFails(t, creationPayload, UnmarshalAttachTarget)

	for _, bad := range []protocol.AttachTarget{{Endpoint: "host", Intent: protocol.IntentAttach}, {Endpoint: "host", Session: "work", Intent: 99}, {Endpoint: "host", Session: "example", Intent: protocol.IntentNew}} {
		if got := MarshalAttachTarget(bad); got != nil {
			t.Fatalf("MarshalAttachTarget(%+v) = %x, want nil", bad, got)
		}
	}
}

func TestAttachTargetExactTargetWireStrict(t *testing.T) {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 1
	target := protocol.AttachTarget{Session: "work", Intent: protocol.IntentAttach, ExactTarget: &protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "work"}, SamePeer: true}
	payload := MarshalAttachTarget(target)
	want := append(make([]byte, 8), []byte{0, 0, 0, 4, 'w', 'o', 'r', 'k', protocol.IntentAttach, 0, 0, 1, 1}...)
	want = append(want, make([]byte, 15)...)
	want = append(want, 0, 4, 'w', 'o', 'r', 'k', 1, 0, 0)
	want = append(want, make([]byte, 8)...)
	if !bytes.Equal(payload, want) {
		t.Fatalf("exact target bytes = %x, want %x", payload, want)
	}
	back, err := UnmarshalAttachTarget(payload)
	if err != nil || !reflect.DeepEqual(back, target) {
		t.Fatalf("exact target = %+v, error %v, want %+v", back, err, target)
	}
	assertAllPrefixesFail(t, payload, UnmarshalAttachTarget)
	assertTrailingGarbageFails(t, payload, UnmarshalAttachTarget)
	mismatched := target
	mismatched.ExactTarget = &protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "other"}
	if got := MarshalAttachTarget(mismatched); got != nil {
		t.Fatalf("MarshalAttachTarget accepted mismatched target: %x", got)
	}
}

func TestFinalClosedWireValuesRejectUnknownEnumsAndBooleans(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		decode  func([]byte) error
	}{
		{
			name: "hello boolean",
			payload: func() []byte {
				b := MarshalHello(protocol.Hello{Version: protocol.Version, Size: domain.Size{Cols: 1, Rows: 1}})
				b[len(b)-8] = 2
				return b
			}(),
			decode: func(b []byte) error { _, err := UnmarshalHello(b); return err },
		},
		{
			name: "hello remote boolean",
			payload: func() []byte {
				b := MarshalHello(protocol.Hello{Version: protocol.Version, Size: domain.Size{Cols: 1, Rows: 1}})
				b[len(b)-2] = 2
				return b
			}(),
			decode: func(b []byte) error { _, err := UnmarshalHello(b); return err },
		},
		{
			name: "hello Kitty graphics boolean",
			payload: func() []byte {
				b := MarshalHello(protocol.Hello{Version: protocol.Version, Size: domain.Size{Cols: 1, Rows: 1}})
				b[len(b)-1] = 2
				return b
			}(),
			decode: func(b []byte) error { _, err := UnmarshalHello(b); return err },
		},
		{
			name: "attach target same-peer boolean",
			payload: func() []byte {
				b := MarshalAttachTarget(protocol.AttachTarget{Endpoint: "host", Session: "work", Intent: protocol.IntentAttach})
				b[len(b)-11] = 2
				return b
			}(),
			decode: func(b []byte) error { _, err := UnmarshalAttachTarget(b); return err },
		},
		{
			name: "welcome boolean",
			payload: func() []byte {
				b := MarshalWelcome(protocol.Welcome{SessionID: "id"})
				b[6] = 2
				return b
			}(),
			decode: func(b []byte) error { _, err := UnmarshalWelcome(b); return err },
		},
		{name: "detached reason", payload: []byte{0xff}, decode: func(b []byte) error { _, err := UnmarshalDetached(b); return err }},
		{
			name: "kill scope",
			payload: func() []byte {
				b := MarshalKill(protocol.Kill{Scope: protocol.KillAll})
				b[len(b)-1] = 3
				return b
			}(),
			decode: func(b []byte) error { _, err := UnmarshalKill(b); return err },
		},
		{
			name: "sessions ephemeral boolean",
			payload: func() []byte {
				b := MarshalSessions(protocol.Sessions{Sessions: []protocol.SessionInfo{{State: protocol.SessionUp}}})
				b[6] = 2
				return b
			}(),
			decode: func(b []byte) error { _, err := UnmarshalSessions(b); return err },
		},
		{
			name: "sessions attached boolean",
			payload: func() []byte {
				b := MarshalSessions(protocol.Sessions{Sessions: []protocol.SessionInfo{{State: protocol.SessionUp}}})
				b[9] = 2
				return b
			}(),
			decode: func(b []byte) error { _, err := UnmarshalSessions(b); return err },
		},
		{name: "theme flags", payload: func() []byte { b := make([]byte, 57); b[0] = 0x20; return b }(), decode: func(b []byte) error { _, err := UnmarshalTheme(b); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.decode(tt.payload); err == nil {
				t.Fatal("decoder accepted unknown closed wire value")
			}
		})
	}
}
