package theme

import (
	"bytes"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestScannerBatchBoundaryPreservesBytesAndReuse(t *testing.T) {
	for _, test := range []struct {
		name, input string
		pending     bool
	}{
		{"empty", "", false},
		{"ordinary", "hello", false},
		{"standalone escape", "\x1b", false},
		{"split color", "x\x1b]10;#12", true},
		{"split scheme", "\x1b[?997;", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var scanner Scanner
			var out bytes.Buffer
			callbacks := 0
			scan := func(data string) {
				scanner.Scan([]byte(data), func(int, renderer.RGB) { callbacks++ }, func(int, renderer.RGB) { callbacks++ }, func(bool) { callbacks++ }, func(data []byte) { out.Write(data) })
			}
			require.False(t, scanner.Pending())
			scan(test.input)
			require.Equal(t, test.pending, scanner.Pending())
			scanner.EndBatch(func(data []byte) { out.Write(data) })
			require.False(t, scanner.Pending())
			require.Equal(t, test.input, out.String(), "ending a batch flushes undecided bytes, not fabricated replies")
			require.Zero(t, callbacks)
			scanner.EndBatch(func(data []byte) { out.Write(data) })
			require.Equal(t, test.input, out.String(), "a boundary cannot replay bytes")
			scan("\x1b]10;#123456\a")
			require.Equal(t, 1, callbacks, "the reusable scanner still handles normal terminal responses")
			require.False(t, scanner.Pending())
		})
	}
}
