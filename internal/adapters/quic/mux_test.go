// Private daemonmux QUIC carriage seam coverage (P3.2e).
//
// These tests exercise the mux-specific raw QUIC carriage over a real loopback
// QUIC connection: quic.DialMuxContext pins the ephemeral certificate and sends
// the single bounded token/nonce auth record, quic.Server.AcceptMux consumes it
// exactly once, and both hand daemonmux a raw bounded transport over the one
// bidirectional stream. The daemonmux-level parity tests live in
// internal/adapters/daemonmux/quic_carriage_test.go.
package quic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// muxTestDeadline bounds every real loopback QUIC mux carriage wait.
const muxTestDeadline = 10 * time.Second

// muxTestServer mints one real loopback bootstrap endpoint and returns it with
// its readiness record.
func muxTestServer(t *testing.T) (*Server, Readiness) {
	t.Helper()
	server, readiness, err := NewServer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	return server, readiness
}

// newMuxBootstrapServer mints one bootstrap server with an explicit QUIC
// config, so a test can make a stream-contract violation admissible at the QUIC
// layer and observe the lifetime guard itself.
func newMuxBootstrapServer(t *testing.T, config Config) (*Server, Readiness) {
	t.Helper()
	cert, fingerprint, err := GenerateEphemeralCert()
	require.NoError(t, err)
	listener, err := ListenConfig("127.0.0.1:0", cert, config, bootstrapMaxPending)
	require.NoError(t, err)
	token := make([]byte, bootstrapTokenBytes)
	_, err = rand.Read(token)
	require.NoError(t, err)
	nonce := make([]byte, bootstrapNonceBytes)
	_, err = rand.Read(nonce)
	require.NoError(t, err)
	tokenVerifier := sha256.Sum256(token)
	nonceVerifier := sha256.Sum256(nonce)
	expires := time.Now().Add(bootstrapExpiry)
	server := &Server{
		listener:      listener,
		tokenVerifier: tokenVerifier[:],
		nonceVerifier: nonceVerifier[:],
		expires:       expires,
		fingerprint:   fingerprint,
	}
	t.Cleanup(func() { _ = server.Close() })
	return server, Readiness{
		Version:     bootstrapSchemaVersion,
		Port:        listenerPort(listener),
		Token:       base64.StdEncoding.EncodeToString(token),
		Nonce:       base64.StdEncoding.EncodeToString(nonce),
		Fingerprint: hex.EncodeToString(fingerprint),
		ExpiresAt:   expires.Unix(),
	}
}

// muxAcceptResult is one AcceptMux outcome delivered from a worker goroutine.
type muxAcceptResult struct {
	carriage wire.BoundedTransport
	err      error
}

// acceptMuxAsync accepts one carriage from server and delivers the outcome.
func acceptMuxAsync(server *Server, ctx context.Context) <-chan muxAcceptResult {
	out := make(chan muxAcceptResult, 1)
	go func() {
		carriage, err := server.AcceptMux(ctx)
		out <- muxAcceptResult{carriage: carriage, err: err}
	}()
	return out
}

// awaitMuxAccept waits for one successful authenticated carriage.
func awaitMuxAccept(t *testing.T, out <-chan muxAcceptResult) wire.BoundedTransport {
	t.Helper()
	select {
	case result := <-out:
		require.NoError(t, result.err)
		require.NotNil(t, result.carriage)
		t.Cleanup(func() { _ = result.carriage.Close() })
		return result.carriage
	case <-time.After(muxTestDeadline):
		t.Fatal("AcceptMux did not return")
		return nil
	}
}

// muxTestPair establishes one authenticated mux carriage over a real loopback
// QUIC connection and returns its broker and daemon ends.
func muxTestPair(t *testing.T) (wire.BoundedTransport, wire.BoundedTransport) {
	t.Helper()
	server, readiness := muxTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), muxTestDeadline)
	t.Cleanup(cancel)
	accepted := acceptMuxAsync(server, ctx)
	client, err := DialMuxContext(ctx, dialTestAddr(t, readiness), readiness, Config{}, muxTestDeadline)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client, awaitMuxAccept(t, accepted)
}

// awaitBoundedReadError proves a carriage closed its bounded read: the refused
// or lost carriage must return an error instead of yielding data.
func awaitBoundedReadError(t *testing.T, carriage wire.BoundedTransport) error {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		_, err := carriage.RecvBounded(bootstrapMaxAuthRecord)
		result <- err
	}()
	select {
	case err := <-result:
		require.Error(t, err)
		return err
	case <-time.After(muxTestDeadline):
		t.Fatal("a closed mux carriage did not fail its bounded read")
		return nil
	}
}

// TestMuxDialAcceptHandsOutOneAuthenticatedBoundedCarriage proves the seam
// hands daemonmux a bounded carriage that carries traffic both ways over
// exactly one client-initiated bidirectional QUIC stream.
func TestMuxDialAcceptHandsOutOneAuthenticatedBoundedCarriage(t *testing.T) {
	client, daemon := muxTestPair(t)

	payload := []byte("mux carriage stays exact")
	require.NoError(t, client.Send(wire.Envelope{Payload: payload}))
	got, err := daemon.RecvBounded(uint64(len(payload)))
	require.NoError(t, err)
	require.Equal(t, payload, got.Payload)

	reply := []byte("mux reply stays exact")
	require.NoError(t, daemon.Send(wire.Envelope{Payload: reply}))
	echo, err := client.RecvBounded(uint64(len(reply)))
	require.NoError(t, err)
	require.Equal(t, reply, echo.Payload)

	// One physical connection carries one bidirectional stream: the first
	// client-initiated stream, multiplexed by daemonmux, never a native stream
	// per logical attachment.
	brokerStream := client.(*Transport)
	daemonStream := daemon.(*Transport)
	require.Equal(t, brokerStream.stream.StreamID(), daemonStream.stream.StreamID())
	require.Zero(t, uint64(brokerStream.stream.StreamID()), "the carriage is the first client-initiated bidirectional stream")
}

// TestMuxCarriageRecvBoundedNeverWidensTheFramingCeiling proves the raw
// bounded transport daemonmux consumes really enforces the caller's ceiling
// before allocating or reading a body.
func TestMuxCarriageRecvBoundedNeverWidensTheFramingCeiling(t *testing.T) {
	client, daemon := muxTestPair(t)

	require.NoError(t, client.Send(wire.Envelope{Payload: []byte("larger than the caller ceiling")}))
	_, err := daemon.RecvBounded(4)
	require.ErrorIs(t, err, errTooLarge)
}

// TestMuxDialRejectsBadPinWithoutConsumingTheCredential proves an unpinned
// peer is refused before authentication and that no bootstrap secret reaches
// the error, while the one-time credential survives for the legitimate peer.
func TestMuxDialRejectsBadPinWithoutConsumingTheCredential(t *testing.T) {
	server, readiness := muxTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), muxTestDeadline)
	defer cancel()

	wrong := readiness
	wrong.Fingerprint = hex.EncodeToString(make([]byte, sha256.Size))
	carriage, err := DialMuxContext(ctx, dialTestAddr(t, readiness), wrong, Config{}, 5*time.Second)
	require.Error(t, err)
	require.Nil(t, carriage)
	require.NotContains(t, err.Error(), readiness.Token)
	require.NotContains(t, err.Error(), readiness.Nonce)
	require.NotContains(t, err.Error(), readiness.Fingerprint)

	server.consumedMu.Lock()
	require.False(t, server.consumed, "a refused pin must not consume the one-time credential")
	server.consumedMu.Unlock()

	// No carriage was admitted: an accept bound to the test deadline expires
	// without handing anything out.
	refusedCtx, stopRefused := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer stopRefused()
	result := <-acceptMuxAsync(server, refusedCtx)
	require.Nil(t, result.carriage)
	require.Error(t, result.err)

	// The legitimate pin still authenticates exactly one carriage.
	accepted := acceptMuxAsync(server, ctx)
	good, err := DialMuxContext(ctx, dialTestAddr(t, readiness), readiness, Config{}, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = good.Close() })
	daemon := awaitMuxAccept(t, accepted)
	payload := []byte("the legitimate carriage admits")
	require.NoError(t, good.Send(wire.Envelope{Payload: payload}))
	got, err := daemon.RecvBounded(uint64(len(payload)))
	require.NoError(t, err)
	require.Equal(t, payload, got.Payload)
}

// TestMuxFailedAuthClosesOnlyThatTransport proves a refused token or nonce
// closes its own carriage, hands nothing out, and leaves the one-time
// credential available for the legitimate peer.
func TestMuxFailedAuthClosesOnlyThatTransport(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Readiness)
	}{
		{
			name: "wrong token",
			mutate: func(r *Readiness) {
				r.Token = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0xA5}, bootstrapTokenBytes))
			},
		},
		{
			name: "wrong nonce",
			mutate: func(r *Readiness) {
				r.Nonce = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x5A}, bootstrapNonceBytes))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, readiness := muxTestServer(t)
			ctx, cancel := context.WithTimeout(context.Background(), muxTestDeadline)
			defer cancel()

			accepted := acceptMuxAsync(server, ctx)
			bad := readiness
			tt.mutate(&bad)
			refused, err := DialMuxContext(ctx, dialTestAddr(t, readiness), bad, Config{}, 5*time.Second)
			require.NoError(t, err, "auth is one-way; the peer learns the refusal from the closed carriage")
			t.Cleanup(func() { _ = refused.Close() })
			awaitBoundedReadError(t, refused)

			// The credential was not consumed by the failed attempt.
			good, err := DialMuxContext(ctx, dialTestAddr(t, readiness), readiness, Config{}, 5*time.Second)
			require.NoError(t, err)
			t.Cleanup(func() { _ = good.Close() })
			daemon := awaitMuxAccept(t, accepted)
			payload := []byte("the legitimate carriage admits")
			require.NoError(t, good.Send(wire.Envelope{Payload: payload}))
			got, err := daemon.RecvBounded(uint64(len(payload)))
			require.NoError(t, err)
			require.Equal(t, payload, got.Payload)
		})
	}
}

// TestMuxReplayedReadinessYieldsNoSecondCarriage proves the one-time credential
// admits exactly one physical carriage: a replay fails closed and never
// disturbs the carriage the legitimate peer already owns.
func TestMuxReplayedReadinessYieldsNoSecondCarriage(t *testing.T) {
	server, readiness := muxTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), muxTestDeadline)
	defer cancel()

	accepted := acceptMuxAsync(server, ctx)
	client, err := DialMuxContext(ctx, dialTestAddr(t, readiness), readiness, Config{}, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	daemon := awaitMuxAccept(t, accepted)

	replayCtx, stopReplay := context.WithCancel(context.Background())
	defer stopReplay()
	replayedAccept := acceptMuxAsync(server, replayCtx)
	replayed, err := DialMuxContext(ctx, dialTestAddr(t, readiness), readiness, Config{}, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replayed.Close() })
	// The server refuses and closes the replayed carriage; only then does the
	// still-waiting accept end.
	awaitBoundedReadError(t, replayed)
	stopReplay()

	result := <-replayedAccept
	require.Nil(t, result.carriage, "a replayed credential must admit no second carriage")
	require.Error(t, result.err)

	// The first carriage is untouched by the replay.
	payload := []byte("the original carriage survives a replay")
	require.NoError(t, client.Send(wire.Envelope{Payload: payload}))
	got, err := daemon.RecvBounded(uint64(len(payload)))
	require.NoError(t, err)
	require.Equal(t, payload, got.Payload)
}

// TestMuxAcceptMuxRefusesExpiredBootstrap proves an expired credential admits
// no carriage even though the listener is still bound.
func TestMuxAcceptMuxRefusesExpiredBootstrap(t *testing.T) {
	server, _ := newBootstrapTestServer(t, bootstrapMaxPending, -time.Minute)
	defer func() { _ = server.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), muxTestDeadline)
	defer cancel()

	carriage, err := server.AcceptMux(ctx)
	require.Nil(t, carriage)
	require.ErrorIs(t, err, errBootstrapExpired)
}

// TestMuxCarriageRejectsExtraQuicStream proves the one-stream guard stays in
// force for the mux carriage lifetime: a second native QUIC stream never becomes
// a logical daemonmux attachment, it fails the whole physical carriage. The
// listener admits two streams at the QUIC layer so the guard, not flow control,
// is what enforces the contract.
func TestMuxCarriageRejectsExtraQuicStream(t *testing.T) {
	server, readiness := newMuxBootstrapServer(t, Config{
		HandshakeIdleTimeout: bootstrapAuthDeadline,
		MaxIncomingStreams:   2,
	})
	ctx, cancel := context.WithTimeout(context.Background(), muxTestDeadline)
	defer cancel()

	accepted := acceptMuxAsync(server, ctx)
	client, err := DialMuxContext(ctx, dialTestAddr(t, readiness), readiness, Config{}, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	daemon := awaitMuxAccept(t, accepted)

	payload := []byte("the single stream stays exact")
	require.NoError(t, client.Send(wire.Envelope{Payload: payload}))
	got, err := daemon.RecvBounded(uint64(len(payload)))
	require.NoError(t, err)
	require.Equal(t, payload, got.Payload)

	extra, err := client.(*Transport).conn.OpenStreamSync(ctx)
	require.NoError(t, err, "the second stream is admissible under the explicit limit")
	defer func() { _ = extra.Close() }()
	_, err = extra.Write([]byte("extra stream violation"))
	require.NoError(t, err)

	awaitBoundedReadError(t, daemon)
	require.Eventually(t, func() bool {
		return client.(*Transport).LinkState() == ports.LinkStateOffline &&
			daemon.(*Transport).LinkState() == ports.LinkStateOffline
	}, muxTestDeadline, 10*time.Millisecond, "the extra stream must fail the whole carriage on both ends")
}

// TestMuxNilServerFailsClosed proves a nil bootstrap server hands out no
// carriage.
func TestMuxNilServerFailsClosed(t *testing.T) {
	var server *Server
	carriage, err := server.AcceptMux(context.Background())
	require.ErrorIs(t, err, ErrMuxCarriage)
	require.Nil(t, carriage)
}

// TestMuxSetupCancellationAndCarriageClose proves clean cancellation and
// teardown: a canceled setup context hands out no carriage on either role, and
// closing one end unblocks a blocked bounded read on the other.
func TestMuxSetupCancellationAndCarriageClose(t *testing.T) {
	t.Run("canceled dial", func(t *testing.T) {
		_, readiness := muxTestServer(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		carriage, err := DialMuxContext(ctx, dialTestAddr(t, readiness), readiness, Config{}, 5*time.Second)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, carriage)
	})

	t.Run("canceled accept", func(t *testing.T) {
		server, _ := muxTestServer(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		carriage, err := server.AcceptMux(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, carriage)
	})

	t.Run("peer close unblocks the bounded read", func(t *testing.T) {
		client, daemon := muxTestPair(t)
		result := make(chan error, 1)
		go func() {
			_, err := daemon.RecvBounded(bootstrapMaxAuthRecord)
			result <- err
		}()
		require.NoError(t, client.Close())
		select {
		case err := <-result:
			require.Error(t, err)
		case <-time.After(muxTestDeadline):
			t.Fatal("peer close did not unblock the bounded read")
		}
	})
}

// TestMuxCarriageErrorsCarryNoSecret proves the sealed-secret invariant for the
// seam's error surface: no error text contains the token or nonce, whether the
// refusal is observed by the dial or by the refused carriage's own read.
func TestMuxCarriageErrorsCarryNoSecret(t *testing.T) {
	server, readiness := muxTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), muxTestDeadline)
	defer cancel()

	wrongPin := readiness
	wrongPin.Fingerprint = hex.EncodeToString(bytes.Repeat([]byte{0x11}, sha256.Size))
	carriage, err := DialMuxContext(ctx, dialTestAddr(t, readiness), wrongPin, Config{}, 2*time.Second)
	require.Nil(t, carriage)
	require.Error(t, err)
	require.NotContains(t, err.Error(), readiness.Token)
	require.NotContains(t, err.Error(), readiness.Nonce)
	require.NotContains(t, err.Error(), readiness.Fingerprint)

	// A refused credential is observed by the carriage's own closed read, whose
	// error must stay equally free of authorization material.
	acceptCtx, stopAccept := context.WithCancel(context.Background())
	defer stopAccept()
	accepted := acceptMuxAsync(server, acceptCtx)
	wrongToken := readiness
	wrongToken.Token = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x22}, bootstrapTokenBytes))
	refused, err := DialMuxContext(ctx, dialTestAddr(t, readiness), wrongToken, Config{}, 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = refused.Close() })
	readErr := awaitBoundedReadError(t, refused)
	require.NotContains(t, readErr.Error(), readiness.Token)
	require.NotContains(t, readErr.Error(), readiness.Nonce)
	stopAccept()

	result := <-accepted
	require.Nil(t, result.carriage, "a refused credential admits no carriage")
	require.Error(t, result.err)
}
