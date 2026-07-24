package dgram

import (
	"bytes"
	"testing"
)

func TestErase(t *testing.T) {
	tests := []struct {
		name string
		buf  []byte
	}{
		{name: "nil", buf: nil},
		{name: "empty", buf: []byte{}},
		{name: "key", buf: bytes.Repeat([]byte{0xa5}, KeySize)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Erase(tt.buf)

			if !bytes.Equal(tt.buf, make([]byte, len(tt.buf))) {
				t.Fatalf("Erase() left data in buffer: %x", tt.buf)
			}
		})
	}
}
