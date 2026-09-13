package wire

import (
	"errors"
	"math"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// TestScanEnvelopeTable covers every scanner family: valid envelopes,
// unknown fields, wrong wire types, duplicates, truncation, trailing
// garbage, oversize lengths, and repeated-element abuse.
func TestScanEnvelopeTable(t *testing.T) {
	mustMarshal := func(message proto.Message) []byte {
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	ping := mustMarshal(&ClientEnvelope{Payload: &ClientEnvelope_Ping{Ping: &Ping{}}})

	input := mustMarshal(&ClientEnvelope{Payload: &ClientEnvelope_Input{Input: &Input{InputSeq: 1, Data: []byte("x")}}})
	snapshot := mustMarshal(&ServerEnvelope{Payload: &ServerEnvelope_PickerSnapshot{PickerSnapshot: &PickerSnapshot{
		InteractionId: 1, SourceId: "serving", SourceRevision: 1, Status: 1,
		Recent: &PickerProjection{}, Grouped: &PickerProjection{},
	}}})

	// Wrong wire type: Hello.version (field 1, varint) as fixed32.
	wrongType := []byte{0x0D, 0x01, 0x00, 0x00, 0x00}
	// Truncated varint tag.
	truncatedTag := []byte{0xFF}
	// Length-delimited field claiming more than available.
	overlong := protowire.AppendTag(nil, 2, protowire.BytesType)
	overlong = protowire.AppendBytes(overlong, make([]byte, 8))
	overlong = overlong[:len(overlong)-4]

	tests := []struct {
		name    string
		message proto.Message
		payload []byte
		wantErr error
	}{
		{name: "valid ping", message: &ClientEnvelope{}, payload: ping},
		{name: "valid input", message: &ClientEnvelope{}, payload: input},
		{name: "valid snapshot", message: &ServerEnvelope{}, payload: snapshot},
		{name: "nil message", message: nil, payload: ping, wantErr: ErrScanNotMessage},
		{name: "empty payload", message: &ClientEnvelope{}, payload: nil, wantErr: ErrScanEmpty},
		{name: "empty envelope", message: &ClientEnvelope{}, payload: []byte{}, wantErr: ErrScanEmpty},
		{name: "truncated", message: &ClientEnvelope{}, payload: ping[:len(ping)-1], wantErr: ErrScanTruncated},
		{name: "truncated tag", message: &ClientEnvelope{}, payload: truncatedTag, wantErr: ErrScanTruncated},
		{name: "concatenated envelopes", message: &ClientEnvelope{}, payload: append(append([]byte(nil), ping...), ping...), wantErr: ErrScanDuplicate},
		{name: "wrong wire type", message: &ClientEnvelope{}, payload: wrongType, wantErr: ErrScanWireType},
		{name: "overlong bytes", message: &ClientEnvelope{}, payload: overlong, wantErr: ErrScanTruncated},
		{name: "duplicate variant", message: &ClientEnvelope{}, payload: append(append([]byte(nil), ping...), ping...), wantErr: ErrScanDuplicate},
		{name: "oversize envelope", message: &ClientEnvelope{}, payload: make([]byte, AbsoluteEnvelopeLimit+1), wantErr: ErrScanLength},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ScanEnvelope(tt.message, tt.payload)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("ScanEnvelope() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ScanEnvelope() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestScanEnvelopeUnknownField appends an unknown varint field to a valid
// envelope and requires ErrScanUnknown.
func TestScanEnvelopeUnknownField(t *testing.T) {
	valid, err := proto.Marshal(&ClientEnvelope{Payload: &ClientEnvelope_Ping{Ping: &Ping{}}})
	if err != nil {
		t.Fatal(err)
	}
	unknown := append([]byte(nil), valid...)
	unknown = protowire.AppendTag(unknown, 99, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 1)
	if err := ScanEnvelope(&ClientEnvelope{}, unknown); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("ScanEnvelope() = %v, want ErrScanUnknown", err)
	}
}

// TestScanEnvelopeRejectsUint32Overflow proves a raw varint wider than the
// declared uint32 field is rejected before generated unmarshal can truncate
// its high bits into a legitimate value.
func TestScanEnvelopeRejectsUint32Overflow(t *testing.T) {
	hello := protowire.AppendTag(nil, 1, protowire.VarintType) // Hello.version
	hello = protowire.AppendVarint(hello, math.MaxUint32+1)
	raw := protowire.AppendTag(nil, 1, protowire.BytesType) // ClientEnvelope.hello
	raw = protowire.AppendBytes(raw, hello)
	if err := ScanEnvelope(&ClientEnvelope{}, raw); !errors.Is(err, ErrScanVarintRange) {
		t.Fatalf("ScanEnvelope() = %v, want ErrScanVarintRange", err)
	}
}

// TestScanEnvelopeAllowsSignExtendedInt32 proves a canonical negative int32
// (ten-byte sign-extended varint) still passes: it is a real value, not a
// truncating overflow.
func TestScanEnvelopeAllowsSignExtendedInt32(t *testing.T) {
	style := protowire.AppendTag(nil, 5, protowire.VarintType) // CellStyle.foreground
	style = protowire.AppendVarint(style, math.MaxUint64)      // -1
	if err := ScanEnvelope(&CellStyle{}, style); err != nil {
		t.Fatalf("ScanEnvelope() = %v, want nil", err)
	}
}

// TestScanEnvelopeRepeatedElements fills a flat repeated field past the
// scanner budget with minimal one-byte elements.
func TestScanEnvelopeRepeatedElements(t *testing.T) {
	hello := &Hello{Version: 1, Intent: 1}
	for range maxScanRepeated + 1 {
		hello.Env = append(hello.Env, "x")
	}
	raw, err := proto.Marshal(&ClientEnvelope{Payload: &ClientEnvelope_Hello{Hello: hello}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ScanEnvelope(&ClientEnvelope{}, raw); !errors.Is(err, ErrScanRepeated) {
		t.Fatalf("ScanEnvelope() = %v, want ErrScanRepeated", err)
	}
}

// TestValidatedUnmarshalRoundTrip proves scan + unmarshal accept a valid
// envelope and reject a mutated one.
func TestValidatedUnmarshalRoundTrip(t *testing.T) {
	raw, err := proto.Marshal(&ClientEnvelope{Payload: &ClientEnvelope_Ack{Ack: &Ack{Epoch: 1, State: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	decoded := &ClientEnvelope{}
	if err := validatedUnmarshal(decoded, raw); err != nil {
		t.Fatalf("validatedUnmarshal() = %v", err)
	}
	bad := append([]byte(nil), raw...)
	bad[len(bad)-1] ^= 0xFF
	if err := validatedUnmarshal(&ClientEnvelope{}, bad); err == nil {
		t.Fatal("validatedUnmarshal accepted mutated payload")
	}
	for size := range len(raw) {
		decoded := &ClientEnvelope{}
		if err := validatedUnmarshal(decoded, raw[:size]); err == nil {
			t.Fatalf("validatedUnmarshal accepted prefix %d", size)
		}
	}
	pong, err := proto.Marshal(&ClientEnvelope{Payload: &ClientEnvelope_Ping{Ping: &Ping{}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := validatedUnmarshal(&ClientEnvelope{}, append(append([]byte(nil), raw...), pong...)); err == nil {
		t.Fatal("validatedUnmarshal accepted trailing envelope")
	}
}
