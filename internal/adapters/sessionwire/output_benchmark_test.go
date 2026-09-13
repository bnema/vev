package sessionwire

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// BenchmarkOutputProductionRoundTrip measures the complete production-owned
// Output path: semantic validation/conversion, optional compression, Protobuf
// marshal, strict scan/unmarshal, decompression, and semantic conversion.
func BenchmarkOutputProductionRoundTrip(b *testing.B) {
	fixtures := []struct {
		name string
		base uint64
		new  uint64
		full bool
		data []byte
	}{
		{name: "incremental", base: 1, new: 2, data: []byte("prompt output\r\n")},
		{name: "compressible_snapshot", new: 1, full: true, data: repeatedOutput(8 << 10)},
		{name: "incompressible_snapshot", new: 1, full: true, data: randomOutput(8 << 10)},
	}
	for _, fixture := range fixtures {
		b.Run(fixture.name, func(b *testing.B) {
			message := protocol.Output{
				Epoch:   1,
				Base:    fixture.base,
				New:     fixture.new,
				Full:    fixture.full,
				Size:    domain.Size{Cols: 120, Rows: 40},
				Context: testUIOutputContext(),
				Data:    fixture.data,
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(fixture.data)))
			for b.Loop() {
				encoded, err := EncodeServerMessage(message)
				if err != nil {
					b.Fatal(err)
				}
				decoded, err := DecodeServerEnvelope(encoded)
				if err != nil {
					b.Fatal(err)
				}
				output, ok := decoded.(protocol.Output)
				if !ok {
					b.Fatalf("decoded Output = %T", decoded)
				}
				if !reflect.DeepEqual(output, message) || !bytes.Equal(output.Data, fixture.data) {
					b.Fatalf("decoded Output differs from input")
				}
			}
		})
	}
}

func repeatedOutput(size int) []byte {
	pattern := []byte("\x1b[38;2;120;180;240mstyled viewport row\x1b[0m\r\n")
	data := make([]byte, size)
	for offset := 0; offset < len(data); offset += copy(data[offset:], pattern) {
	}
	return data
}

func randomOutput(size int) []byte {
	data := make([]byte, size)
	_, _ = rand.New(rand.NewSource(1)).Read(data)
	return data
}
