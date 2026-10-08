package brokeripc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/ports"
)

// TestStreamCreditGrantDispatch proves how the session settles one client
// credit grant: a grant past the window settles only its stream, a grant for a
// retired stream is discarded, and a grant for a stream that was never opened
// fails the whole connection.
func TestStreamCreditGrantDispatch(t *testing.T) {
	open := func(t *testing.T, raw *rawCarriage, scope brokerwire.Scope, id ports.BrokerStreamID) {
		t.Helper()
		raw.send(t, brokerwire.OpenStream{
			Epoch: scope.Epoch, Connection: scope.Connection, Stream: id,
			Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(),
			StartMode: ports.BrokerDaemonStartIfNeeded,
		})
		opened, ok := raw.recv(t).(brokerwire.StreamOpened)
		require.True(t, ok, "stream %d must open", id)
		require.Equal(t, id, opened.Stream)
	}
	grant := func(t *testing.T, raw *rawCarriage, scope brokerwire.Scope, id ports.BrokerStreamID) {
		t.Helper()
		raw.send(t, brokerwire.StreamWindowUpdate{Epoch: scope.Epoch, Connection: scope.Connection, Stream: id, Credit: 1})
	}
	requireStreamFailed := func(t *testing.T, raw *rawCarriage, id ports.BrokerStreamID) {
		t.Helper()
		closed, ok := raw.recv(t).(brokerwire.StreamClosed)
		require.True(t, ok, "an over-window grant must be answered with StreamClosed")
		require.Equal(t, id, closed.Stream)
		require.True(t, closed.HasError)
	}

	for _, tc := range []struct {
		name string
		run  func(t *testing.T, raw *rawCarriage, scope brokerwire.Scope)
	}{
		{"grant over the window settles only its stream", func(t *testing.T, raw *rawCarriage, scope brokerwire.Scope) {
			open(t, raw, scope, 1)
			open(t, raw, scope, 2)
			// A fresh stream already holds the whole window, so any grant overflows.
			grant(t, raw, scope, 1)
			requireStreamFailed(t, raw, 1)
			open(t, raw, scope, 3) // the connection still serves new streams
		}},
		{"grant for a retired stream is discarded", func(t *testing.T, raw *rawCarriage, scope brokerwire.Scope) {
			open(t, raw, scope, 1)
			grant(t, raw, scope, 1)
			requireStreamFailed(t, raw, 1)
			grant(t, raw, scope, 1) // stream 1 is retired now
			open(t, raw, scope, 2)
		}},
		{"grant for a never-opened stream fails the connection", func(t *testing.T, raw *rawCarriage, scope brokerwire.Scope) {
			open(t, raw, scope, 1)
			grant(t, raw, scope, 9)
			require.Error(t, raw.awaitReadError(t, 5*time.Second))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := startEndpoint(t, Config{})
			raw := rawDial(t, e, brokerwire.DefaultCeilings())
			tc.run(t, raw, raw.register(t))
		})
	}
}
