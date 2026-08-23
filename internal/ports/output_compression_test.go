package ports

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestOutputCompression(t *testing.T) {
	full := Output{
		Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 120, Rows: 40}, Full: true,
		Data: bytes.Repeat([]byte("\x1b[38;2;120;180;240mstyled viewport row\x1b[0m\r\n"), 128),
	}
	incremental := full
	incremental.Base = 1
	incremental.New = 2
	incremental.Full = false

	for _, tt := range []struct {
		name     string
		output   Output
		wantKind byte
	}{
		{name: "large full snapshot", output: full, wantKind: outputCompressionZlib},
		{name: "incremental output", output: incremental, wantKind: outputCompressionNone},
		{name: "small full snapshot", output: Output{Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 80, Rows: 24}, Full: true, Data: []byte("small")}, wantKind: outputCompressionNone},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := MarshalOutput(tt.output)
			if err != nil {
				t.Fatal(err)
			}
			if got := payload[outputHeaderLen]; got != tt.wantKind {
				t.Fatalf("compression kind = %d, want %d", got, tt.wantKind)
			}
			decoded, err := UnmarshalOutput(payload)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(decoded.Data, tt.output.Data) {
				t.Fatalf("decoded data differs\n got: %q\nwant: %q", decoded.Data, tt.output.Data)
			}
		})
	}
}

func TestCompressedOutputRejectsMalformedPayloads(t *testing.T) {
	output := Output{
		Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 120, Rows: 40}, Full: true,
		Data: bytes.Repeat([]byte("\x1b[31mcompressed\x1b[0m\r\n"), 256),
	}
	payload, err := MarshalOutput(output)
	if err != nil {
		t.Fatal(err)
	}
	if payload[outputHeaderLen] != outputCompressionZlib {
		t.Fatal("test fixture was not compressed")
	}

	for _, tt := range []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "unknown kind", mutate: func(b []byte) { b[outputHeaderLen] = 99 }},
		{name: "decoded length mismatch", mutate: func(b []byte) {
			binary.BigEndian.PutUint32(b[outputHeaderLen+1:outputHeaderLen+5], uint32(len(output.Data)-1))
		}},
		{name: "compressed incremental", mutate: func(b []byte) {
			b[outputHeaderLen-1] = 0
			binary.BigEndian.PutUint64(b[16:24], 2)
			binary.BigEndian.PutUint64(b[8:16], 1)
		}},
		{name: "corrupt stream", mutate: func(b []byte) { b[len(b)-1] ^= 0xff }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bad := append([]byte(nil), payload...)
			tt.mutate(bad)
			if _, err := UnmarshalOutput(bad); err == nil {
				t.Fatal("UnmarshalOutput accepted malformed compressed payload")
			}
		})
	}
	trailingCompressed := append(append([]byte(nil), payload...), 0)
	binary.BigEndian.PutUint32(trailingCompressed[outputHeaderLen+5:outputHeaderLen+9], uint32(len(trailingCompressed)-(outputHeaderLen+9)))
	if _, err := UnmarshalOutput(trailingCompressed); err == nil {
		t.Fatal("UnmarshalOutput accepted trailing compressed bytes")
	}
	assertAllPrefixesFail(t, payload, UnmarshalOutput)
	assertTrailingGarbageFails(t, payload, UnmarshalOutput)
}

func BenchmarkMarshalOutput(b *testing.B) {
	fixtures := map[string]Output{
		"empty-120x40": {
			Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 120, Rows: 40}, Full: true,
			Data: bytes.Repeat([]byte(" "), 120*40),
		},
		"styled-120x40": {
			Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 120, Rows: 40}, Full: true,
			Data: bytes.Repeat([]byte("\x1b[38;2;120;180;240mstyled viewport row\x1b[0m\r\n"), 128),
		},
		"styled-200x60": {
			Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 200, Rows: 60}, Full: true,
			Data: bytes.Repeat([]byte("\x1b[48;2;24;32;48m\x1b[38;2;200;220;255mlarge styled viewport row\x1b[0m\r\n"), 256),
		},
	}
	for name, output := range fixtures {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(output.Data)))
			var encoded []byte
			for b.Loop() {
				var err error
				encoded, err = MarshalOutput(output)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(encoded)), "encoded-bytes/op")
		})
	}
}
