package brokeripc

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/protocol"
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
			p := newTestPipe()
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

// newTestPipe is a pipe whose outbound writes and credit grants go nowhere.
func newTestPipe() *streamPipe {
	return newStreamPipe(func([]byte) error { return nil }, func(uint64) error { return nil })
}

// TestStreamWindowCoversTheSessionOutputWindow pins the invariant that lets a
// broker stream carry a full session output window without the daemon ever
// waiting on the stream instead of on its own ACK window: the daemon's
// unacknowledged byte budget plus one maximum Output fits in one stream window.
// When it does not, credit still keeps every stream correct, only slower.
func TestStreamWindowCoversTheSessionOutputWindow(t *testing.T) {
	require.GreaterOrEqual(t, brokerwire.StreamWindowBytes, uint64(protocol.MaxOutputWindowBytes),
		"one stream window must hold the daemon's whole unacknowledged byte budget")
}

// TestStreamPipeCredit covers the receive and send sides of stream credit:
// a peer may send up to one window, the consumer's reads return credit in
// batches, a sender waits instead of failing while it has none, and grants or
// data beyond the window are flow-control violations.
func TestStreamPipeCredit(t *testing.T) {
	chunk := int(brokerwire.MaxStreamChunkBytes)
	cost := brokerwire.StreamChunkCredit(chunk)
	perWindow := int(brokerwire.StreamWindowBytes / cost)

	t.Run("a full window is accepted and one more frame is a violation", func(t *testing.T) {
		p := newTestPipe()
		for range perWindow {
			require.NoError(t, p.deliver(make([]byte, chunk)))
		}
		require.ErrorIs(t, p.deliver(make([]byte, chunk)), ErrStreamCredit)
	})

	t.Run("reads return credit in batches", func(t *testing.T) {
		var grants []uint64
		p := newStreamPipe(func([]byte) error { return nil }, func(c uint64) error { grants = append(grants, c); return nil })
		for range perWindow {
			require.NoError(t, p.deliver(make([]byte, chunk)))
		}
		buf := make([]byte, chunk)
		for range perWindow {
			_, err := io.ReadFull(p, buf)
			require.NoError(t, err)
		}
		require.NotEmpty(t, grants)
		total := uint64(0)
		for _, g := range grants {
			require.GreaterOrEqual(t, g, streamCreditReturnAt(brokerwire.StreamWindowBytes))
			total += g
		}
		require.LessOrEqual(t, total, uint64(perWindow)*cost)
		// Once the returned credit reaches the peer, the window is open again.
		require.NoError(t, p.deliver(make([]byte, chunk)))
	})

	t.Run("a sender waits for credit and resumes on a grant", func(t *testing.T) {
		p := newTestPipe()
		for range perWindow {
			require.NoError(t, p.reserve(chunk))
		}
		reserved := make(chan error, 1)
		go func() { reserved <- p.reserve(chunk) }()
		select {
		case err := <-reserved:
			t.Fatalf("reserve returned %v without credit", err)
		case <-time.After(20 * time.Millisecond):
		}
		require.NoError(t, p.granted(cost))
		select {
		case err := <-reserved:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("a grant must release a sender waiting for credit")
		}
	})

	t.Run("close releases a sender waiting for credit", func(t *testing.T) {
		p := newTestPipe()
		for range perWindow {
			require.NoError(t, p.reserve(chunk))
		}
		reserved := make(chan error, 1)
		go func() { reserved <- p.reserve(chunk) }()
		p.closeWith(ErrStreamGone)
		select {
		case err := <-reserved:
			require.ErrorIs(t, err, ErrStreamGone)
		case <-time.After(5 * time.Second):
			t.Fatal("close must release a sender waiting for credit")
		}
	})

	t.Run("a grant beyond the window is a violation", func(t *testing.T) {
		p := newTestPipe()
		require.ErrorIs(t, p.granted(1), ErrStreamCredit)
		require.NoError(t, p.reserve(chunk))
		require.NoError(t, p.granted(cost))
		require.ErrorIs(t, p.granted(1), ErrStreamCredit)
	})
}

// TestClientStreamPrefersTheFullOutputWindow pins what a real broker-routed
// attachment reports: its carriage is a reliable stream, not a datagram link,
// so the client claims the full output window and frames pipeline.
func TestClientStreamPrefersTheFullOutputWindow(t *testing.T) {
	st, err := newClientStream(&client{}, 1)
	require.NoError(t, err)
	require.Equal(t, uint8(protocol.MaxOutputWindow), st.Capabilities().PreferredOutputWindow)
}
