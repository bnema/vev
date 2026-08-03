package ports

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestFinalProtocolVersionAndNoV21Hello(t *testing.T) {
	if ProtocolVersion != 23 {
		t.Fatalf("ProtocolVersion = %d, want 23", ProtocolVersion)
	}
	payload := MarshalHello(Hello{Version: 21, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}})
	if _, err := UnmarshalHello(payload); err == nil {
		t.Fatal("UnmarshalHello accepted protocol v21")
	}
}

func TestFinalHelloGoldenStrict(t *testing.T) {
	msg := Hello{Version: ProtocolVersion, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}
	want := append([]byte{0, 23, 2}, make([]byte, 16+8)...)
	want = append(want, 0, 0, 0, 80, 0, 24, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
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

func TestFinalHelloSemanticValidation(t *testing.T) {
	valid := Hello{Version: ProtocolVersion, Intent: IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}
	for _, tt := range []struct {
		name   string
		mutate func(*Hello)
	}{
		{name: "intent", mutate: func(h *Hello) { h.Intent = 99 }},
		{name: "zero columns", mutate: func(h *Hello) { h.Size.Cols = 0 }},
		{name: "zero rows", mutate: func(h *Hello) { h.Size.Rows = 0 }},
		{name: "area", mutate: func(h *Hello) { h.Size = domain.Size{Cols: 513, Rows: 512} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			msg := valid
			tt.mutate(&msg)
			if err := ValidateHello(msg); err == nil {
				t.Fatal("ValidateHello accepted malformed semantics")
			}
		})
	}
}

func TestFinalResizeGoldenStrict(t *testing.T) {
	msg := Resize{Size: domain.Size{Cols: 80, Rows: 24}}
	want := []byte{0, 80, 0, 24}
	got := MarshalResize(msg)
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
		if got := MarshalResize(Resize{Size: size}); got != nil {
			t.Fatalf("MarshalResize(%+v) = %x, want nil", size, got)
		}
	}
}

func TestFinalOutputGoldenStrict(t *testing.T) {
	msg := Output{
		Epoch: 1, Base: 0, New: 7, Echo: 8, ViewRevision: 9,
		Size: domain.Size{Cols: 80, Rows: 24}, Full: true, Data: []byte("ok"),
	}
	want := []byte{
		0, 0, 0, 0, 0, 0, 0, 1,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 7,
		0, 0, 0, 0, 0, 0, 0, 8,
		0, 0, 0, 0, 0, 0, 0, 9,
		0, 80, 0, 24, 1,
		0, 0, 0, 2, 'o', 'k',
	}
	got := MarshalOutput(msg)
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
	valid := Output{Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 80, Rows: 24}, Full: true}
	payload := MarshalOutput(valid)
	for _, tt := range []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "zero epoch", mutate: func(b []byte) { b[7] = 0 }},
		{name: "new does not advance", mutate: func(b []byte) { binary.BigEndian.PutUint64(b[16:24], 0) }},
		{name: "size zero", mutate: func(b []byte) { binary.BigEndian.PutUint16(b[40:42], 0) }},
		{name: "full flag false for reset", mutate: func(b []byte) { b[44] = 0 }},
		{name: "invalid bool", mutate: func(b []byte) { b[44] = 2 }},
		{name: "impossible data length", mutate: func(b []byte) { binary.BigEndian.PutUint32(b[45:49], ^uint32(0)) }},
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
	msg := Ack{Epoch: 2, State: 7}
	want := []byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 7}
	got := MarshalAck(msg)
	if !bytes.Equal(got, want) {
		t.Fatalf("Ack bytes = %x, want %x", got, want)
	}
	back, err := UnmarshalAck(got)
	if err != nil || back != (Ack{Epoch: 2, State: 7}) {
		t.Fatalf("Ack = %+v, error %v", back, err)
	}
	assertAllPrefixesFail(t, got, UnmarshalAck)
	assertTrailingGarbageFails(t, got, UnmarshalAck)
	if _, err := UnmarshalAck(make([]byte, 16)); err == nil {
		t.Fatal("UnmarshalAck accepted zero epoch")
	}
}

func TestFinalAttachTargetGoldenStrict(t *testing.T) {
	msg := AttachTarget{Endpoint: "host", Session: "work", Intent: IntentAttach}
	want := []byte{0, 4, 'h', 'o', 's', 't', 0, 4, 'w', 'o', 'r', 'k', 2}
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
	for _, bad := range []AttachTarget{{Session: "work", Intent: IntentAttach}, {Endpoint: "host", Intent: IntentAttach}, {Endpoint: "host", Session: "work", Intent: 99}} {
		if got := MarshalAttachTarget(bad); got != nil {
			t.Fatalf("MarshalAttachTarget(%+v) = %x, want nil", bad, got)
		}
	}
}
