package wire

import (
	"bytes"
	"errors"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The daemonmux conversation is separate from the session and broker
// conversations: its tags are disjoint from both, both directions are
// closed oneof unions, the strict scanner treats it exactly like the other
// envelopes, and the shared physical preamble pair is not an envelope.

// muxClientTags is the frozen client tag inventory.
var muxClientTags = map[string]protoreflect.FieldNumber{
	"open": 301, "data": 302, "close": 303, "reset": 304,
}

// muxServerTags is the frozen server tag inventory.
var muxServerTags = map[string]protoreflect.FieldNumber{
	"opened": 401, "refused": 402, "data": 403, "close": 404, "reset": 405,
}

func muxStreamRef() *MuxStreamRef {
	return &MuxStreamRef{
		PhysicalStreamId: 5,
		BrokerEpoch:      7,
		ConnectionId:     bytes.Repeat([]byte{0x11}, 16),
		ClientStreamId:   3,
	}
}

func muxRegistration() *RemoteRegistration {
	return &RemoteRegistration{Endpoint: "dev@host:22", Incarnation: bytes.Repeat([]byte{0x22}, 16), Generation: 9}
}

func muxPolicy() *BrokerWirePolicy {
	return &BrokerWirePolicy{
		ProtocolVersion: 53, CatalogueVersion: 3, EnvironmentPolicy: 1,
		Transport: "quic", Trust: "pinned", Launch: "agent", Isolation: "user",
	}
}

func muxErrorDetail() *BrokerErrorDetail {
	return &BrokerErrorDetail{Code: 2, Text: "incompatible", AdmissionCode: 1, FailureKind: 3}
}

// muxClientSamples returns one populated envelope per client variant.
func muxClientSamples() map[string]*MuxClientEnvelope {
	return map[string]*MuxClientEnvelope{
		"open": {Payload: &MuxClientEnvelope_Open{Open: &MuxOpen{
			Ref: muxStreamRef(), Purpose: 1, Local: false, Endpoint: "dev@host:22",
			Registration: muxRegistration(),
			Target:       &ExactTarget{LifecycleId: &LifecycleID{Value: bytes.Repeat([]byte{0x55}, 16)}, SessionName: "work"},
			Env:          []string{"TERM=xterm-256color", "LANG=C.UTF-8"},
			Policy:       muxPolicy(),
		}}},
		"data":  {Payload: &MuxClientEnvelope_Data{Data: &MuxData{PhysicalStreamId: 5, Data: bytes.Repeat([]byte{0x66}, 24)}}},
		"close": {Payload: &MuxClientEnvelope_Close{Close: &MuxClose{PhysicalStreamId: 5}}},
		"reset": {Payload: &MuxClientEnvelope_Reset_{Reset_: &MuxReset{PhysicalStreamId: 5, Error: muxErrorDetail()}}},
	}
}

// muxServerSamples returns one populated envelope per server variant.
func muxServerSamples() map[string]*MuxServerEnvelope {
	return map[string]*MuxServerEnvelope{
		"opened":  {Payload: &MuxServerEnvelope_Opened{Opened: &MuxOpened{Ref: muxStreamRef()}}},
		"refused": {Payload: &MuxServerEnvelope_Refused{Refused: &MuxRefused{Ref: muxStreamRef(), Error: muxErrorDetail()}}},
		"data":    {Payload: &MuxServerEnvelope_Data{Data: &MuxData{PhysicalStreamId: 5, Data: bytes.Repeat([]byte{0x77}, 24)}}},
		"close":   {Payload: &MuxServerEnvelope_Close{Close: &MuxClose{PhysicalStreamId: 5}}},
		"reset":   {Payload: &MuxServerEnvelope_Reset_{Reset_: &MuxReset{PhysicalStreamId: 5}}},
	}
}

func mustMarshalMux(t *testing.T, message proto.Message) []byte {
	t.Helper()
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestMuxEnvelopeInventory proves both daemonmux envelopes carry exactly the
// frozen tag inventory and share no tag with the session envelopes, the
// broker envelopes, or each other.
func TestMuxEnvelopeInventory(t *testing.T) {
	client := oneofTags(t, &MuxClientEnvelope{})
	server := oneofTags(t, &MuxServerEnvelope{})

	if len(client) != len(muxClientTags) {
		t.Fatalf("mux client envelope has %d variants, want %d (%v)", len(client), len(muxClientTags), client)
	}
	for name, number := range muxClientTags {
		if got, ok := client[name]; !ok || got != number {
			t.Fatalf("mux client variant %q = %d (present %t), want %d", name, got, ok, number)
		}
	}
	if len(server) != len(muxServerTags) {
		t.Fatalf("mux server envelope has %d variants, want %d (%v)", len(server), len(muxServerTags), server)
	}
	for name, number := range muxServerTags {
		if got, ok := server[name]; !ok || got != number {
			t.Fatalf("mux server variant %q = %d (present %t), want %d", name, got, ok, number)
		}
	}

	// The mux tag space is disjoint from every session tag and every broker
	// tag, in both directions.
	otherTags := map[protoreflect.FieldNumber]string{}
	for _, envelope := range []proto.Message{&ClientEnvelope{}, &ServerEnvelope{}} {
		for name, number := range oneofTags(t, envelope) {
			otherTags[number] = "session " + name
		}
	}
	for _, envelope := range []proto.Message{&BrokerClientEnvelope{}, &BrokerServerEnvelope{}} {
		for name, number := range oneofTags(t, envelope) {
			otherTags[number] = "broker " + name
		}
	}
	clientByTag := map[protoreflect.FieldNumber]string{}
	for name, number := range muxClientTags {
		clientByTag[number] = name
		if other, taken := otherTags[number]; taken {
			t.Fatalf("mux client tag %d (%s) collides with %s", number, name, other)
		}
	}
	for name, number := range muxServerTags {
		if other, taken := otherTags[number]; taken {
			t.Fatalf("mux server tag %d (%s) collides with %s", number, name, other)
		}
		if peer, taken := clientByTag[number]; taken {
			t.Fatalf("mux server tag %d (%s) collides with mux client variant %s", number, name, peer)
		}
	}

	for _, envelope := range []proto.Message{&MuxClientEnvelope{}, &MuxServerEnvelope{}} {
		if !isEnvelopeDescriptor(envelope.ProtoReflect().Descriptor()) {
			t.Fatalf("%v is not recognized as a directional envelope", envelope.ProtoReflect().Descriptor().FullName())
		}
	}
	// The physical preamble and the shared payloads are never envelopes.
	for _, payload := range []proto.Message{&MuxPreambleRequest{}, &MuxPreambleResponse{}, &MuxOpen{}, &MuxData{}, &MuxClose{}, &MuxReset{}, &MuxOpened{}, &MuxRefused{}, &MuxStreamRef{}} {
		if isEnvelopeDescriptor(payload.ProtoReflect().Descriptor()) {
			t.Fatalf("payload %v is wrongly recognized as a directional envelope", payload.ProtoReflect().Descriptor().FullName())
		}
	}
}

// TestMuxEnvelopeRoundTrip proves every variant survives strict scan,
// generated unmarshal, and equality, in both directions.
func TestMuxEnvelopeRoundTrip(t *testing.T) {
	for name, sample := range muxClientSamples() {
		t.Run("client/"+name, func(t *testing.T) {
			raw := mustMarshalMux(t, sample)
			decoded := &MuxClientEnvelope{}
			if err := validatedUnmarshal(decoded, raw); err != nil {
				t.Fatalf("validatedUnmarshal() = %v", err)
			}
			if !proto.Equal(sample, decoded) {
				t.Fatalf("roundtrip mismatch:\n got %v\nwant %v", decoded, sample)
			}
		})
	}
	for name, sample := range muxServerSamples() {
		t.Run("server/"+name, func(t *testing.T) {
			raw := mustMarshalMux(t, sample)
			decoded := &MuxServerEnvelope{}
			if err := validatedUnmarshal(decoded, raw); err != nil {
				t.Fatalf("validatedUnmarshal() = %v", err)
			}
			if !proto.Equal(sample, decoded) {
				t.Fatalf("roundtrip mismatch:\n got %v\nwant %v", decoded, sample)
			}
		})
	}
}

// TestMuxEnvelopeRequiredVariant proves both mux envelopes demand exactly
// one payload variant, like the session and broker envelopes.
func TestMuxEnvelopeRequiredVariant(t *testing.T) {
	for _, message := range []proto.Message{&MuxClientEnvelope{}, &MuxServerEnvelope{}} {
		if err := ScanEnvelope(message, mustMarshalMux(t, message)); !errors.Is(err, ErrScanEmpty) {
			t.Fatalf("ScanEnvelope(%T empty) = %v, want ErrScanEmpty", message, err)
		}
	}
}

// TestMuxEnvelopeWrongDirection proves a payload serialized in one direction
// is rejected when scanned as the other direction, and that session and
// broker payloads are unknown on the mux conversation.
func TestMuxEnvelopeWrongDirection(t *testing.T) {
	client := mustMarshalMux(t, muxClientSamples()["open"])
	if err := ScanEnvelope(&MuxServerEnvelope{}, client); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("client payload as server envelope = %v, want ErrScanUnknown", err)
	}
	server := mustMarshalMux(t, muxServerSamples()["opened"])
	if err := ScanEnvelope(&MuxClientEnvelope{}, server); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("server payload as client envelope = %v, want ErrScanUnknown", err)
	}
	session := mustMarshalMux(t, &ClientEnvelope{Payload: &ClientEnvelope_Ping{Ping: &Ping{}}})
	if err := ScanEnvelope(&MuxClientEnvelope{}, session); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("session payload as mux envelope = %v, want ErrScanUnknown", err)
	}
	broker := mustMarshalMux(t, &BrokerClientEnvelope{Payload: &BrokerClientEnvelope_Register{Register: &Register{}}})
	if err := ScanEnvelope(&MuxClientEnvelope{}, broker); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("broker payload as mux envelope = %v, want ErrScanUnknown", err)
	}
}

// TestMuxEnvelopeDuplicateAlternatives proves a second oneof occurrence is
// rejected whether it repeats the same variant or switches alternatives.
func TestMuxEnvelopeDuplicateAlternatives(t *testing.T) {
	closeRaw := mustMarshalMux(t, muxClientSamples()["close"])
	dataRaw := mustMarshalMux(t, muxClientSamples()["data"])
	if err := ScanEnvelope(&MuxClientEnvelope{}, append(append([]byte(nil), closeRaw...), dataRaw...)); !errors.Is(err, ErrScanDuplicate) {
		t.Fatalf("alternate alternatives = %v, want ErrScanDuplicate", err)
	}
	if err := ScanEnvelope(&MuxClientEnvelope{}, append(append([]byte(nil), dataRaw...), dataRaw...)); !errors.Is(err, ErrScanDuplicate) {
		t.Fatalf("duplicate variant = %v, want ErrScanDuplicate", err)
	}
	// The shared payload types are exclusive per direction as well.
	serverClose := mustMarshalMux(t, muxServerSamples()["close"])
	serverReset := mustMarshalMux(t, muxServerSamples()["reset"])
	if err := ScanEnvelope(&MuxServerEnvelope{}, append(append([]byte(nil), serverClose...), serverReset...)); !errors.Is(err, ErrScanDuplicate) {
		t.Fatalf("duplicate server alternatives = %v, want ErrScanDuplicate", err)
	}
}

// TestMuxEnvelopeRejectsUnknownAndWrongTypes proves unknown and mistyped
// fields are rejected recursively, including inside the nested stream ref
// and the shared error detail.
func TestMuxEnvelopeRejectsUnknownAndWrongTypes(t *testing.T) {
	// Unknown field on the envelope itself.
	raw := mustMarshalMux(t, muxClientSamples()["close"])
	raw = protowire.AppendTag(raw, 399, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 1)
	if err := ScanEnvelope(&MuxClientEnvelope{}, raw); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("unknown envelope field = %v, want ErrScanUnknown", err)
	}

	// Unknown field two levels down: MuxClientEnvelope → MuxOpen → MuxStreamRef.
	ref := protowire.AppendTag(nil, 1, protowire.VarintType) // MuxStreamRef.physical_stream_id
	ref = protowire.AppendVarint(ref, 5)
	ref = protowire.AppendTag(ref, 9, protowire.VarintType) // unknown in MuxStreamRef
	ref = protowire.AppendVarint(ref, 1)
	open := protowire.AppendTag(nil, 1, protowire.BytesType) // MuxOpen.ref
	open = protowire.AppendBytes(open, ref)
	outer := protowire.AppendTag(nil, 301, protowire.BytesType) // MuxClientEnvelope.open
	outer = protowire.AppendBytes(outer, open)
	if err := ScanEnvelope(&MuxClientEnvelope{}, outer); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("nested unknown field = %v, want ErrScanUnknown", err)
	}

	// Unknown field inside the shared error detail of a reset.
	reset := protowire.AppendTag(nil, 1, protowire.VarintType) // MuxReset.physical_stream_id
	reset = protowire.AppendVarint(reset, 5)
	detail := protowire.AppendTag(nil, 1, protowire.VarintType) // BrokerErrorDetail.code
	detail = protowire.AppendVarint(detail, 2)
	detail = protowire.AppendTag(detail, 99, protowire.VarintType)
	detail = protowire.AppendVarint(detail, 1)
	reset = protowire.AppendTag(reset, 2, protowire.BytesType) // MuxReset.error
	reset = protowire.AppendBytes(reset, detail)
	raw = protowire.AppendTag(nil, 405, protowire.BytesType) // MuxServerEnvelope.reset
	raw = protowire.AppendBytes(raw, reset)
	if err := ScanEnvelope(&MuxServerEnvelope{}, raw); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("unknown nested error field = %v, want ErrScanUnknown", err)
	}

	// Wrong wire type: MuxClientEnvelope.close (bytes) as a varint.
	wrongType := protowire.AppendTag(nil, 303, protowire.VarintType)
	wrongType = protowire.AppendVarint(wrongType, 1)
	if err := ScanEnvelope(&MuxClientEnvelope{}, wrongType); !errors.Is(err, ErrScanWireType) {
		t.Fatalf("wrong wire type = %v, want ErrScanWireType", err)
	}

	// A raw varint wider than the declared uint32 is rejected before
	// generated unmarshal can truncate its high bits.
	open = protowire.AppendTag(nil, 2, protowire.VarintType) // MuxOpen.purpose
	open = protowire.AppendVarint(open, 1<<33)
	outer = protowire.AppendTag(nil, 301, protowire.BytesType)
	outer = protowire.AppendBytes(outer, open)
	if err := ScanEnvelope(&MuxClientEnvelope{}, outer); !errors.Is(err, ErrScanVarintRange) {
		t.Fatalf("uint32 overflow = %v, want ErrScanVarintRange", err)
	}
}

// TestMuxEnvelopeTruncation proves truncated tags, varints, lengths, and
// every strict prefix of a valid envelope are rejected.
func TestMuxEnvelopeTruncation(t *testing.T) {
	if err := ScanEnvelope(&MuxClientEnvelope{}, []byte{0xFF}); !errors.Is(err, ErrScanTruncated) {
		t.Fatalf("truncated tag = %v, want ErrScanTruncated", err)
	}
	overlong := protowire.AppendTag(nil, 301, protowire.BytesType)
	overlong = protowire.AppendBytes(overlong, make([]byte, 8))
	overlong = overlong[:len(overlong)-4]
	if err := ScanEnvelope(&MuxClientEnvelope{}, overlong); !errors.Is(err, ErrScanTruncated) {
		t.Fatalf("overlong length = %v, want ErrScanTruncated", err)
	}
	partialVarint := protowire.AppendTag(nil, 302, protowire.BytesType) // MuxClientEnvelope.data
	data := protowire.AppendTag(nil, 1, protowire.VarintType)           // MuxData.physical_stream_id
	partialVarint = protowire.AppendBytes(partialVarint, append(data, 0x80))
	if err := ScanEnvelope(&MuxClientEnvelope{}, partialVarint); !errors.Is(err, ErrScanTruncated) {
		t.Fatalf("truncated varint = %v, want ErrScanTruncated", err)
	}

	for name, sample := range muxClientSamples() {
		raw := mustMarshalMux(t, sample)
		for size := range len(raw) {
			if err := ScanEnvelope(&MuxClientEnvelope{}, raw[:size]); err == nil {
				t.Fatalf("client prefix %s[:%d] accepted", name, size)
			}
		}
	}
	for name, sample := range muxServerSamples() {
		raw := mustMarshalMux(t, sample)
		for size := range len(raw) {
			if err := ScanEnvelope(&MuxServerEnvelope{}, raw[:size]); err == nil {
				t.Fatalf("server prefix %s[:%d] accepted", name, size)
			}
		}
	}
}

// TestMuxEnvelopeTrailingGarbage proves trailing bytes and a second
// concatenated envelope are rejected.
func TestMuxEnvelopeTrailingGarbage(t *testing.T) {
	raw := mustMarshalMux(t, muxServerSamples()["opened"])
	for _, tail := range [][]byte{
		{0x00},
		protowire.AppendTag(nil, 405, protowire.VarintType),
		mustMarshalMux(t, muxServerSamples()["close"]),
	} {
		payload := append(append([]byte(nil), raw...), tail...)
		if err := ScanEnvelope(&MuxServerEnvelope{}, payload); err == nil {
			t.Fatalf("trailing %x accepted", tail)
		}
	}
}
