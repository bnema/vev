package client

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// failingReader ends the terminal read with a fixed cause.
type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

// TestTerminalReadCauseIsReportedExactly proves the input lifetime publishes the
// cause the terminal reader ended with rather than a hardcoded EOF.
//
// A read failure reported as an orderly EOF makes the supervisor return a clean
// status for a terminal that failed. The reader enqueues its final result for a
// consumer to report, but the exit watcher can win that race, so the pump
// records the cause and the watcher publishes it. The nil consumer here leaves
// that result unconsumed, which is exactly the racing shape.
func TestTerminalReadCauseIsReportedExactly(t *testing.T) {
	failure := errors.New("terminal read failed")

	tests := []struct {
		name   string
		reader io.Reader
		want   error
	}{
		{name: "read failure is not an orderly exit", reader: failingReader{err: failure}, want: failure},
		{name: "orderly end of input stays eof", reader: failingReader{err: io.EOF}, want: io.EOF},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lifetime := startTerminalInputLifetime(tc.reader, nil)
			t.Cleanup(lifetime.stop)

			select {
			case err := <-lifetime.EOF():
				require.ErrorIs(t, err, tc.want)
			case <-time.After(5 * time.Second):
				t.Fatal("terminal cause was never published")
			}
		})
	}
}
