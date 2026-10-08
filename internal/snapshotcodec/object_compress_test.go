package snapshotcodec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

// reframeObject rewrites the VEVO body and fixes length and CRC so a test
// reaches the payload checks instead of failing on framing.
func reframeObject(t *testing.T, encoded []byte, mutate func(body []byte) []byte) []byte {
	t.Helper()
	body := mutate(append([]byte(nil), encoded[manifestHeaderSize:]...))
	out := append([]byte(nil), encoded[:manifestHeaderSize]...)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(body)))
	binary.BigEndian.PutUint32(out[12:16], crc32.ChecksumIEEE(body))
	return append(out, body...)
}

func TestObjectCompressionRejectsMalformedPayloads(t *testing.T) {
	payload := bytes.Repeat([]byte("history row "), 512)
	object, err := MarshalObject(HistoryChunk, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(object.Data) >= len(payload)/4 {
		t.Fatalf("object = %d bytes, want well under %d raw bytes", len(object.Data), len(payload))
	}
	again, err := MarshalObject(HistoryChunk, payload)
	if err != nil || !bytes.Equal(again.Data, object.Data) || again.Digest != object.Digest {
		t.Fatal("equal payloads must share one deterministic content address")
	}

	for _, tc := range []struct {
		name   string
		mutate func(body []byte) []byte
		want   error
	}{
		{name: "declared shorter", mutate: func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[1:5], uint32(len(payload)-1))
			return b
		}, want: ErrInvalidData},
		{name: "declared longer", mutate: func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[1:5], uint32(len(payload)+1))
			return b
		}, want: ErrInvalidData},
		{name: "truncated stream", mutate: func(b []byte) []byte { return b[:len(b)-1] }, want: ErrInvalidData},
		{name: "corrupt stream", mutate: func(b []byte) []byte { b[len(b)-6] ^= 0xff; return b }, want: ErrInvalidData},
		{name: "trailing garbage", mutate: func(b []byte) []byte { return append(b, 0) }, want: ErrTrailingBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := reframeObject(t, object.Data, tc.mutate)
			if _, err := PreflightObject(bad); err != nil {
				t.Fatalf("preflight must accept valid framing, got %v", err)
			}
			if _, _, err := UnmarshalObject(bad); !errors.Is(err, tc.want) {
				t.Fatalf("UnmarshalObject() error = %v, want %v", err, tc.want)
			}
		})
	}
}
