package brokeripc

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStreamPipeCloseWithPreservesQueuedDataOnOrderlyClose proves closeWith
// keeps chunks that are already queued when its cause is nil, an orderly
// close: Read drains every queued byte before it returns io.EOF. This is
// exactly what a reply queued just ahead of an orderly StreamClosed needs -
// the reply chunk must survive the close instead of being thrown away
// underneath its reader, which is root cause 1 of the broker reply loss
// ("vev ls" reporting a lost/unknown outcome on a successful call). An error
// cause is the opposite: the queued content can no longer be trusted once the
// pipe is torn down for cause, so it is discarded and Read observes the error
// immediately instead of replaying stale data.
func TestStreamPipeCloseWithPreservesQueuedDataOnOrderlyClose(t *testing.T) {
	errCause := errors.New("stream pipe test: cause")

	tests := []struct {
		name     string
		chunks   [][]byte
		cause    error
		readSize int
		wantData []byte
		wantErr  error
	}{
		{
			name:     "orderly close keeps one queued chunk",
			chunks:   [][]byte{[]byte("hello")},
			cause:    nil,
			readSize: 64,
			wantData: []byte("hello"),
			wantErr:  io.EOF,
		},
		{
			name:   "orderly close keeps three chunks split across a frame boundary",
			chunks: [][]byte{[]byte("AB"), []byte("CD"), []byte("EF")},
			cause:  nil,
			// Smaller than the combined payload, so one logical frame's worth of
			// queued data is drained across several Read calls, exactly like
			// io.ReadFull reading a header then a body out of the same queue.
			readSize: 3,
			wantData: []byte("ABCDEF"),
			wantErr:  io.EOF,
		},
		{
			name:     "error cause discards the queue and Read returns the error",
			chunks:   [][]byte{[]byte("hello")},
			cause:    errCause,
			readSize: 64,
			wantData: nil,
			wantErr:  errCause,
		},
		{
			name:     "orderly close with no queued data is immediate EOF",
			chunks:   nil,
			cause:    nil,
			readSize: 64,
			wantData: nil,
			wantErr:  io.EOF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newStreamPipe(0, 0, func([]byte) error { return nil })
			for _, chunk := range tt.chunks {
				require.NoError(t, p.deliver(chunk))
			}
			p.closeWith(tt.cause)

			var got bytes.Buffer
			buf := make([]byte, tt.readSize)
			var readErr error
			for {
				n, err := p.Read(buf)
				got.Write(buf[:n])
				if err != nil {
					readErr = err
					break
				}
			}
			require.Equal(t, tt.wantData, got.Bytes())
			if errors.Is(tt.wantErr, io.EOF) {
				require.ErrorIs(t, readErr, io.EOF)
			} else {
				require.Equal(t, tt.wantErr, readErr)
			}

			// closeWith is idempotent: a second call with a different cause never
			// resurrects a discarded queue or changes the already-observed outcome.
			p.closeWith(errors.New("stream pipe test: second cause"))
			n, err := p.Read(buf)
			require.Zero(t, n)
			if errors.Is(tt.wantErr, io.EOF) {
				require.ErrorIs(t, err, io.EOF)
			} else {
				require.Equal(t, tt.wantErr, err)
			}
		})
	}
}
