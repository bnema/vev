package quic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// dialTestAddr composes the loopback address for the test server's
// published port, mirroring the dialer's target-host resolution.
func dialTestAddr(t *testing.T, readiness Readiness) string {
	t.Helper()
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(readiness.Port))
}

// decodeBootstrapFingerprint decodes the readiness pin for raw
// endpoint dials that bypass the authenticated Dial helper.
func decodeBootstrapFingerprint(t *testing.T, readiness Readiness) ([]byte, error) {
	t.Helper()
	return hex.DecodeString(readiness.Fingerprint)
}

func TestBootstrapRoundTrip(t *testing.T) {
	server, readiness, err := NewServer()
	require.NoError(t, err)
	defer func() { _ = server.Close() }()

	raw, err := EncodeReadiness(readiness)
	require.NoError(t, err)
	parsed, err := ParseReadiness(append(append([]byte(nil), raw...), '\n'))
	require.NoError(t, err)
	require.Equal(t, readiness.Token, parsed.Token)

	accepted := make(chan wire.Transport, 1)
	acceptErr := make(chan error, 1)
	go func() {
		transport, err := server.Accept(context.Background())
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- transport
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := Dial(ctx, dialTestAddr(t, readiness), readiness, Config{}, 10*time.Second)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	select {
	case transport := <-accepted:
		defer func() { _ = transport.Close() }()
		payload := []byte("bootstrap session stays exact")
		require.NoError(t, client.Send(wire.Envelope{Payload: payload}))
		got, err := transport.Recv()
		require.NoError(t, err)
		require.Equal(t, payload, got.Payload)
	case err := <-acceptErr:
		t.Fatalf("bootstrap accept: %v", err)
	case <-ctx.Done():
		t.Fatal("bootstrap accept timed out")
	}
}

func TestBootstrapTokenSingleUse(t *testing.T) {
	server, readiness, err := NewServer()
	require.NoError(t, err)
	defer func() { _ = server.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	first, err := Dial(ctx, dialTestAddr(t, readiness), readiness, Config{}, 10*time.Second)
	require.NoError(t, err)
	defer func() { _ = first.Close() }()

	accepted := make(chan wire.Transport, 1)
	go func() {
		transport, err := server.Accept(context.Background())
		if err == nil {
			accepted <- transport
		}
	}()
	select {
	case transport := <-accepted:
		_ = transport.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("first bootstrap accept timed out")
	}

	// A second dial reuses the consumed token: auth must fail.
	second, err := Dial(ctx, dialTestAddr(t, readiness), readiness, Config{}, 10*time.Second)
	if err == nil {
		_ = second.Close()
	}
	secondAccept := make(chan wire.Transport, 1)
	go func() {
		transport, err := server.Accept(context.Background())
		if err == nil {
			secondAccept <- transport
		}
	}()
	select {
	case transport := <-secondAccept:
		_ = transport.Close()
		t.Fatal("consumed bootstrap token was accepted twice")
	case <-time.After(5 * time.Second):
	}
}

func TestBootstrapWrongTokenFails(t *testing.T) {
	server, readiness, err := NewServer()
	require.NoError(t, err)
	defer func() { _ = server.Close() }()

	// Wrong but well-formed token: decoding succeeds, auth must fail.
	wrong := make([]byte, bootstrapTokenBytes)
	for i := range wrong {
		wrong[i] = 0xA5
	}
	readiness.Token = base64.StdEncoding.EncodeToString(wrong)
	accepted := make(chan wire.Transport, 1)
	go func() {
		transport, err := server.Accept(context.Background())
		if err == nil {
			accepted <- transport
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := Dial(ctx, dialTestAddr(t, readiness), readiness, Config{}, 5*time.Second)
	if err == nil {
		_ = client.Close()
	}
	select {
	case transport := <-accepted:
		_ = transport.Close()
		t.Fatal("wrong bootstrap token was accepted")
	case <-time.After(5 * time.Second):
	}
	_ = server
}

func TestBootstrapMalformedReadiness(t *testing.T) {
	server, readiness, err := NewServer()
	require.NoError(t, err)
	defer func() { _ = server.Close() }()
	raw, err := EncodeReadiness(readiness)
	require.NoError(t, err)

	mutate := func(apply func(*Readiness)) []byte {
		cloned := readiness
		apply(&cloned)
		mutated, err := json.Marshal(cloned)
		require.NoError(t, err)
		return mutated
	}

	for _, line := range [][]byte{
		nil,
		{},
		[]byte("not json"),
		[]byte("{}"),
		make([]byte, bootstrapMaxReadiness+1),
		// Wrong schema version.
		mutate(func(r *Readiness) { r.Version = bootstrapSchemaVersion + 1 }),
		// Non-port address values.
		mutate(func(r *Readiness) { r.Port = 0 }),
		mutate(func(r *Readiness) { r.Port = 65536 }),
		// Malformed token/nonce encodings.
		mutate(func(r *Readiness) { r.Token = "00" }),
		mutate(func(r *Readiness) { r.Nonce = "not-base64!!" }),
		// Expiry in the past and implausibly far in the future.
		mutate(func(r *Readiness) { r.ExpiresAt = time.Now().Add(-time.Second).Unix() }),
		mutate(func(r *Readiness) { r.ExpiresAt = time.Now().Add(time.Hour).Unix() }),
	} {
		_, err := ParseReadiness(line)
		require.Error(t, err)
	}

	// The minted record itself parses.
	_, err = ParseReadiness(append(append([]byte(nil), raw...), '\n'))
	require.NoError(t, err)
}

// TestBootstrapConcurrentTokenUse proves the token is consumed exactly
// once: two simultaneous valid dials admit at most one session.
func TestBootstrapConcurrentTokenUse(t *testing.T) {
	server, readiness, err := NewServer()
	require.NoError(t, err)
	defer func() { _ = server.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	acceptCtx, stopAccept := context.WithCancel(ctx)
	defer stopAccept()
	addr := dialTestAddr(t, readiness)
	const racers = 2
	accepted := make(chan wire.Transport, racers)
	var wg sync.WaitGroup
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			transport, err := server.Accept(acceptCtx)
			if err == nil {
				accepted <- transport
			}
		}()
	}
	dialed := make(chan wire.Transport, racers)
	var dialWg sync.WaitGroup
	for range racers {
		dialWg.Add(1)
		go func() {
			defer dialWg.Done()
			client, err := Dial(ctx, addr, readiness, Config{}, 10*time.Second)
			if err == nil {
				dialed <- client
			}
		}()
	}
	// The first admitted session stops the other Accept: a consumed
	// token can never admit again, so waiting for expiry is pointless.
	first := <-accepted
	stopAccept()
	wg.Wait()
	close(accepted)
	count := 1
	_ = first.Close()
	for transport := range accepted {
		count++
		_ = transport.Close()
	}
	require.Equal(t, 1, count, "exactly one concurrent token use must succeed")
	dialWg.Wait()
	close(dialed)
	for client := range dialed {
		_ = client.Close()
	}
}

// TestBootstrapOversizedAuthRecordFails proves an unauthenticated peer
// cannot force a large pre-auth allocation: the bounded auth record is
// rejected before JSON parsing. The dial bypasses Dial's auth send and
// emits the oversized record directly on the single stream.
func TestBootstrapOversizedAuthRecordFails(t *testing.T) {
	server, readiness, err := NewServer()
	require.NoError(t, err)
	defer func() { _ = server.Close() }()

	accepted := make(chan wire.Transport, 1)
	go func() {
		transport, err := server.Accept(context.Background())
		if err == nil {
			accepted <- transport
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fingerprint, err := decodeBootstrapFingerprint(t, readiness)
	require.NoError(t, err)
	raw, err := DialConfig(dialTestAddr(t, readiness), "vev-bootstrap", fingerprint, Config{}, 5*time.Second).Dial(ctx)
	require.NoError(t, err)
	defer func() { _ = raw.Close() }()
	over := bytes.Repeat([]byte("x"), bootstrapMaxAuthRecord+1)
	require.NoError(t, raw.Send(wire.Envelope{Payload: over}))
	// The server must close the failed auth without admitting a session.
	select {
	case transport := <-accepted:
		_ = transport.Close()
		t.Fatal("oversized auth record was accepted")
	case <-time.After(5 * time.Second):
	}
}

// newBootstrapTestServer builds a Server with known token material and an
// explicit admission bound so tests can observe admission holding and
// expiry internals without waiting on wall-clock bootstrap windows.
func newBootstrapTestServer(t *testing.T, maxPending int, expiry time.Duration) (*Server, Readiness) {
	t.Helper()
	cert, fingerprint, err := GenerateEphemeralCert()
	require.NoError(t, err)
	listener, err := ListenConfig("127.0.0.1:0", cert, Config{HandshakeIdleTimeout: bootstrapAuthDeadline}, maxPending)
	require.NoError(t, err)
	token := make([]byte, bootstrapTokenBytes)
	_, err = rand.Read(token)
	require.NoError(t, err)
	nonce := make([]byte, bootstrapNonceBytes)
	_, err = rand.Read(nonce)
	require.NoError(t, err)
	tokenVerifier := sha256.Sum256(token)
	nonceVerifier := sha256.Sum256(nonce)
	expires := time.Now().Add(expiry)
	server := &Server{
		listener:      listener,
		tokenVerifier: tokenVerifier[:],
		nonceVerifier: nonceVerifier[:],
		expires:       expires,
	}
	return server, Readiness{
		Version:     bootstrapSchemaVersion,
		Port:        listenerPort(listener),
		Token:       base64.StdEncoding.EncodeToString(token),
		Nonce:       base64.StdEncoding.EncodeToString(nonce),
		Fingerprint: hex.EncodeToString(fingerprint),
		ExpiresAt:   expires.Unix(),
	}
}

// TestBootstrapListenerIsRemotelyReachable proves the production listener
// binds the wildcard rather than loopback and serves both address families,
// so the remote SSH peer that resolved the target host can dial it directly.
func TestBootstrapListenerIsRemotelyReachable(t *testing.T) {
	dialWildcard := func(t *testing.T, host string) {
		t.Helper()
		server, readiness, err := NewServer()
		require.NoError(t, err)
		defer func() { _ = server.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		accepted := make(chan wire.Transport, 1)
		acceptErr := make(chan error, 1)
		go func() {
			transport, err := server.Accept(ctx)
			if err != nil {
				acceptErr <- err
				return
			}
			accepted <- transport
		}()
		addr := net.JoinHostPort(host, strconv.Itoa(readiness.Port))
		client, err := Dial(ctx, addr, readiness, Config{}, 5*time.Second)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()
		select {
		case transport := <-accepted:
			_ = transport.Close()
		case err := <-acceptErr:
			t.Fatalf("accept over %s: %v", host, err)
		case <-time.After(10 * time.Second):
			t.Fatalf("accept over %s timed out", host)
		}
	}

	server, _, err := NewServer()
	require.NoError(t, err)
	defer func() { _ = server.Close() }()
	host, _, err := net.SplitHostPort(server.Addr())
	require.NoError(t, err)
	ip := net.ParseIP(host)
	require.NotNil(t, ip, "listener address %q has no IP host", server.Addr())
	require.True(t, ip.IsUnspecified(), "bootstrap listener must bind the wildcard, got %s", server.Addr())
	require.False(t, ip.IsLoopback())

	t.Run("ipv4", func(t *testing.T) { dialWildcard(t, "127.0.0.1") })
	t.Run("ipv6", func(t *testing.T) {
		if ip.To4() != nil {
			t.Skip("listener bound IPv4-only")
		}
		probe, err := net.Listen("tcp", "[::1]:0")
		if err != nil {
			t.Skip("IPv6 loopback unavailable")
		}
		_ = probe.Close()
		dialWildcard(t, "::1")
	})
}

// TestBootstrapAdmissionHeldThroughAuthentication proves an unauthenticated
// connection keeps its admission slot: the bound must count peers parked in
// authentication, not only connections whose stream contract completed.
func TestBootstrapAdmissionHeldThroughAuthentication(t *testing.T) {
	server, readiness := newBootstrapTestServer(t, 1, time.Minute)
	defer func() { _ = server.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		_, _ = server.Accept(ctx)
	}()
	fingerprint, err := decodeBootstrapFingerprint(t, readiness)
	require.NoError(t, err)
	// The raw dial sends only the dial probe: the connection stays
	// unauthenticated and must hold the single admission slot.
	client, err := DialConfig(dialTestAddr(t, readiness), "vev-bootstrap", fingerprint, Config{}, 5*time.Second).Dial(ctx)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	require.Eventually(t, func() bool {
		server.listener.pendingMu.Lock()
		defer server.listener.pendingMu.Unlock()
		return server.listener.pending == 1
	}, 3*time.Second, 10*time.Millisecond, "unauthenticated connection must hold its admission slot")
	cancel()
	<-acceptDone
}

// TestBootstrapOversizedAuthHeaderRejectedBeforePayload proves the 512-byte
// auth framing limit is enforced on the length prefix: a peer advertising a
// huge frame and never sending its body is rejected promptly instead of
// forcing a large pre-auth allocation and stalling until the auth deadline.
func TestBootstrapOversizedAuthHeaderRejectedBeforePayload(t *testing.T) {
	server, readiness, err := NewServer()
	require.NoError(t, err)
	defer func() { _ = server.Close() }()
	accepted := make(chan wire.Transport, 1)
	go func() {
		transport, err := server.Accept(context.Background())
		if err == nil {
			accepted <- transport
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fingerprint, err := decodeBootstrapFingerprint(t, readiness)
	require.NoError(t, err)
	raw, err := DialConfig(dialTestAddr(t, readiness), "vev-bootstrap", fingerprint, Config{}, 5*time.Second).Dial(ctx)
	require.NoError(t, err)
	defer func() { _ = raw.Close() }()
	// Advertise a 1 MiB auth frame and never send its body: only the
	// four-byte length prefix exists on the wire.
	_, err = raw.(*Transport).stream.Write(binary.BigEndian.AppendUint32(nil, 1<<20))
	require.NoError(t, err)
	start := time.Now()
	_, err = raw.Recv()
	require.Error(t, err)
	require.Less(t, time.Since(start), 2*time.Second,
		"oversized auth header must be rejected before the payload is read")
	select {
	case transport := <-accepted:
		_ = transport.Close()
		t.Fatal("oversized auth header admitted a session")
	default:
	}
}

// TestBootstrapConsumeIsExpiryGuardedAndAtomic proves the verifier
// comparison, the locked expiry recheck, and one-time token consumption.
func TestBootstrapConsumeIsExpiryGuardedAndAtomic(t *testing.T) {
	server, readiness := newBootstrapTestServer(t, 1, time.Minute)
	defer func() { _ = server.Close() }()
	token, err := base64.StdEncoding.DecodeString(readiness.Token)
	require.NoError(t, err)
	nonce, err := base64.StdEncoding.DecodeString(readiness.Nonce)
	require.NoError(t, err)
	record := authRecord{Token: token, Nonce: nonce}

	wrong := append([]byte(nil), token...)
	wrong[0] ^= 0xFF
	require.ErrorIs(t, server.consume(authRecord{Token: wrong, Nonce: nonce}), errBootstrapAuth)
	require.False(t, server.consumed)

	require.NoError(t, server.consume(record))
	require.True(t, server.consumed)
	require.ErrorIs(t, server.consume(record), errBootstrapConsumed)

	// Expiry is rechecked under the consumption lock: a valid record that
	// raced the credential lifetime can never authenticate after expiry.
	server.consumed = false
	server.expires = time.Now().Add(-time.Millisecond)
	require.ErrorIs(t, server.consume(record), errBootstrapExpired)
	require.False(t, server.consumed)
}

// TestBootstrapServerStoresVerifierOnly proves the server keeps a digest of
// the credential rather than the reusable secret itself.
func TestBootstrapServerStoresVerifierOnly(t *testing.T) {
	server, readiness, err := NewServer()
	require.NoError(t, err)
	defer func() { _ = server.Close() }()
	token, err := base64.StdEncoding.DecodeString(readiness.Token)
	require.NoError(t, err)
	tokenSum := sha256.Sum256(token)
	require.Equal(t, tokenSum[:], server.tokenVerifier)
	require.NotEqual(t, token, server.tokenVerifier)
	nonce, err := base64.StdEncoding.DecodeString(readiness.Nonce)
	require.NoError(t, err)
	nonceSum := sha256.Sum256(nonce)
	require.Equal(t, nonceSum[:], server.nonceVerifier)
}

// blockingSendTransport is a wire.Transport whose Send blocks until Close,
// reading the payload in a loop so a caller that erased the record before
// joining the send goroutine would race the reader.
type blockingSendTransport struct {
	closed   chan struct{}
	sendDone chan struct{}
}

func newBlockingSendTransport() *blockingSendTransport {
	return &blockingSendTransport{closed: make(chan struct{}), sendDone: make(chan struct{})}
}

func (b *blockingSendTransport) Send(envelope wire.Envelope) error {
	defer close(b.sendDone)
	for {
		select {
		case <-b.closed:
			return io.EOF
		default:
		}
		var seen byte
		for _, value := range envelope.Payload {
			seen ^= value
		}
		_ = seen
		time.Sleep(50 * time.Microsecond)
	}
}

func (b *blockingSendTransport) Recv() (wire.Envelope, error) { return wire.Envelope{}, io.EOF }

func (b *blockingSendTransport) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

// TestSendAuthRecordJoinsBlockedSendBeforeErase proves Dial's timeout path
// closes the transport and joins the blocked send goroutine before returning:
// the caller's deferred record erase can never race the sender. The join is
// asserted directly and the payload is erased afterwards, so a missing join is
// both a test failure and a race-detector report.
func TestSendAuthRecordJoinsBlockedSendBeforeErase(t *testing.T) {
	transport := newBlockingSendTransport()
	record := []byte("one-time-bootstrap-token-and-nonce")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := sendAuthRecord(ctx, transport, record)
	require.ErrorIs(t, err, errBootstrapAuth)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-transport.sendDone:
	default:
		t.Fatal("sendAuthRecord returned while the send goroutine was still running")
	}
	zeroBytes(record)
}
