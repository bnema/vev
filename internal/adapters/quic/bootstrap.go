package quic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
)

// Bootstrap normative bounds (P6.2): ephemeral listener/certificate,
// 32-byte token, nonce, ≤4 KiB readiness, ≤15-second expiry, exact
// SHA-256 pin, one bounded auth record, atomic one-time consumption,
// ≤4 unauthenticated peers, 3-second auth deadline, exactly one
// bidirectional stream, private IPC only after auth, deterministic
// process cleanup.
const (
	bootstrapTokenBytes   = 32
	bootstrapNonceBytes   = 16
	bootstrapMaxReadiness = 4 << 10
	bootstrapExpiry       = 15 * time.Second
	bootstrapAuthDeadline = 3 * time.Second
	bootstrapMaxPending   = 4
	// bootstrapSchemaVersion versions the readiness record shape
	// independently of the session protocol epoch.
	bootstrapSchemaVersion = 1
	// bootstrapMaxAuthRecord bounds the single auth envelope payload
	// before JSON parsing: token + nonce in base64 plus record
	// framing cannot legitimately approach this size.
	bootstrapMaxAuthRecord = 512
)

// Readiness is the ≤4 KiB JSON record the bootstrap server prints on
// stdout. The client parses exactly one line: schema version, port,
// token, nonce, fingerprint, and expiry bind the QUIC dial that
// follows. The port is host-independent: the client composes it with
// the SSH target host, never trusting a remote loopback address. The
// token travels base64; no secret beyond it leaves the SSH channel,
// and the token is consumed atomically on first use.
type Readiness struct {
	Version     int    `json:"version"`
	Port        int    `json:"port"`
	Token       string `json:"token"`
	Nonce       string `json:"nonce"`
	Fingerprint string `json:"fingerprint"`
	ExpiresAt   int64  `json:"expires_at"`
}

var (
	errBootstrapExpired   = errors.New("quic: bootstrap readiness expired")
	errBootstrapConsumed  = errors.New("quic: bootstrap token already consumed")
	errBootstrapMalformed = errors.New("quic: malformed bootstrap readiness")
	errBootstrapAuth      = errors.New("quic: bootstrap authentication failed")
	errBootstrapNoStream  = errors.New("quic: bootstrap peer opened no stream")
)

// Server is the ephemeral bootstrap endpoint: one QUIC listener with a
// fresh certificate, one atomic token, and deterministic cleanup. The
// token is consumed exactly once by the first authenticated stream;
// every other attempt fails closed.
type Server struct {
	listener *Listener
	// Only verifiers are stored: the raw token and nonce buffers are
	// erased as soon as the readiness record is encoded, so a process
	// memory disclosure never yields the reusable credential itself.
	tokenVerifier []byte
	nonceVerifier []byte
	expires       time.Time
	fingerprint   []byte

	consumedMu sync.Mutex
	consumed   bool

	closeOnce sync.Once
	closeErr  error
}

// NewServer mints an ephemeral certificate, binds a wildcard QUIC
// listener, and returns the server plus its readiness record. The
// caller prints exactly one readiness line on stdout. Only the port
// is published: the client resolves the SSH target host itself.
//
// The production bootstrap listener is intentionally reachable from
// the remote SSH peer, not only over a local tunnel: it binds the
// wildcard address so both IPv4 and IPv6 (dual-stack where the OS
// supports it) are served. Generic ListenConfig callers retain the
// safe loopback default.
func NewServer() (*Server, Readiness, error) {
	cert, fingerprint, err := GenerateEphemeralCert()
	if err != nil {
		return nil, Readiness{}, err
	}
	token := make([]byte, bootstrapTokenBytes)
	if _, err := rand.Read(token); err != nil {
		return nil, Readiness{}, err
	}
	defer zeroBytes(token)
	nonce := make([]byte, bootstrapNonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return nil, Readiness{}, err
	}
	defer zeroBytes(nonce)
	listener, err := ListenConfig(":0", cert, Config{HandshakeIdleTimeout: bootstrapAuthDeadline}, bootstrapMaxPending)
	if err != nil {
		return nil, Readiness{}, err
	}
	port := listenerPort(listener)
	if port <= 0 {
		_ = listener.Close()
		return nil, Readiness{}, errBootstrapMalformed
	}
	expires := time.Now().Add(bootstrapExpiry)
	server := &Server{
		listener:      listener,
		tokenVerifier: verifier(token),
		nonceVerifier: verifier(nonce),
		expires:       expires,
		fingerprint:   fingerprint,
	}
	return server, Readiness{
		Version:     bootstrapSchemaVersion,
		Port:        port,
		Token:       base64.StdEncoding.EncodeToString(token),
		Nonce:       base64.StdEncoding.EncodeToString(nonce),
		Fingerprint: hex.EncodeToString(fingerprint),
		ExpiresAt:   expires.Unix(),
	}, nil
}

// listenerPort extracts the bound port in host-independent form.
func listenerPort(listener *Listener) int {
	_, port, err := net.SplitHostPort(listener.Addr())
	if err != nil {
		return 0
	}
	parsed, err := strconv.Atoi(port)
	if err != nil {
		return 0
	}
	return parsed
}

// EncodeReadiness marshals one readiness line (no trailing newline;
// the caller adds exactly one).
func EncodeReadiness(readiness Readiness) ([]byte, error) {
	raw, err := json.Marshal(readiness)
	if err != nil {
		return nil, err
	}
	if len(raw) > bootstrapMaxReadiness {
		return nil, errBootstrapMalformed
	}
	return raw, nil
}

// ParseReadiness parses and validates one readiness line: schema
// version, size, JSON shape, token/nonce/port/fingerprint presence,
// and expiry. The remote server is authoritative for expiry during auth;
// client-side parsing rejects only implausibly long-lived credentials and
// allows bounded host-clock skew. Secret values never appear in the error.
func ParseReadiness(line []byte) (Readiness, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || len(line) > bootstrapMaxReadiness {
		return Readiness{}, errBootstrapMalformed
	}
	var readiness Readiness
	if err := json.Unmarshal(line, &readiness); err != nil {
		return Readiness{}, errBootstrapMalformed
	}
	if readiness.Version != bootstrapSchemaVersion {
		return Readiness{}, errBootstrapMalformed
	}
	if readiness.Port <= 0 || readiness.Port > 65535 {
		return Readiness{}, errBootstrapMalformed
	}
	token, err := base64.StdEncoding.DecodeString(readiness.Token)
	if err != nil || len(token) != bootstrapTokenBytes {
		return Readiness{}, errBootstrapMalformed
	}
	zeroBytes(token)
	nonce, err := base64.StdEncoding.DecodeString(readiness.Nonce)
	if err != nil || len(nonce) != bootstrapNonceBytes {
		return Readiness{}, errBootstrapMalformed
	}
	zeroBytes(nonce)
	fingerprint, err := hex.DecodeString(readiness.Fingerprint)
	if err != nil || len(fingerprint) != sha256.Size {
		return Readiness{}, errBootstrapMalformed
	}
	now := time.Now()
	expires := time.Unix(readiness.ExpiresAt, 0)
	const bootstrapClockSkew = 5 * time.Minute
	if !expires.After(now.Add(-bootstrapClockSkew)) || expires.After(now.Add(bootstrapExpiry+bootstrapClockSkew)) {
		return Readiness{}, errBootstrapExpired
	}
	return readiness, nil
}

// verifier is the server-side material stored for a secret: the raw
// token/nonce buffers are erased immediately after this digest exists.
func verifier(secret []byte) []byte {
	sum := sha256.Sum256(secret)
	return sum[:]
}

// authRecord is the single bounded auth record: token plus nonce, each
// exactly once. The server compares in constant time and consumes the
// token atomically.
type authRecord struct {
	Token []byte `json:"token"`
	Nonce []byte `json:"nonce"`
}

// Accept waits for one authenticated stream: the peer must send exactly
// one auth record (token + nonce) within the auth deadline, consumed
// atomically. The returned transport owns the single bidirectional
// stream; unauthenticated attempts fail closed without IPC access.
// Accept stops at token expiry or context cancellation: an expired
// bootstrap exits instead of admitting peers past the advertised
// credential lifetime. The blocking listener wait is bounded by the
// same conditions, so a parked Accept cannot outlive the server.
func (s *Server) Accept(ctx context.Context) (wire.Transport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The wait for a connection is itself bounded by the credential
	// lifetime: an expired bootstrap exits instead of admitting a peer.
	waitCtx, cancelWait := context.WithDeadline(ctx, s.expires)
	defer cancelWait()
	for {
		admittedConn, err := s.listener.admitOne(waitCtx, bootstrapAuthDeadline)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return nil, ctx.Err()
			case !time.Now().Before(s.expires):
				return nil, errBootstrapExpired
			default:
				return nil, err
			}
		}
		err = s.authenticate(waitCtx, admittedConn)
		admittedConn.release()
		if err == nil {
			return admittedConn.transport, nil
		}
		_ = admittedConn.transport.Close()
		if errors.Is(err, errBootstrapExpired) || !time.Now().Before(s.expires) {
			return nil, errBootstrapExpired
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Authentication failed: keep listening for the legitimate
		// peer until the credential expires.
	}
}

// authenticate reads the single bounded auth record and consumes the
// token atomically. The deadline held by the admitted connection covers
// stream open and this read; the record length prefix is bound before any
// payload allocation; and expiry is rechecked under the consumption lock
// so a record racing the credential lifetime cannot authenticate late.
func (s *Server) authenticate(ctx context.Context, admittedConn admitted) error {
	if !time.Now().Before(s.expires) {
		return errBootstrapExpired
	}
	deadline := admittedConn.deadline
	if s.expires.Before(deadline) {
		deadline = s.expires
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	type authResult struct {
		payload []byte
		err     error
	}
	results := make(chan authResult, 1)
	go func() {
		// recvBounded rejects an over-limit length prefix before
		// allocating, so an unauthenticated peer cannot force a large
		// pre-auth allocation by advertising a huge frame.
		envelope, err := admittedConn.transport.recvBounded(bootstrapMaxAuthRecord)
		results <- authResult{payload: envelope.Payload, err: err}
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", errBootstrapAuth, ctx.Err())
	case result := <-results:
		if result.err != nil {
			return errBootstrapAuth
		}
		defer zeroBytes(result.payload)
		var record authRecord
		if err := json.Unmarshal(result.payload, &record); err != nil {
			return errBootstrapAuth
		}
		// The decoded secret buffers are erased as soon as the
		// comparison finishes.
		defer zeroBytes(record.Token)
		defer zeroBytes(record.Nonce)
		return s.consume(record)
	}
}

// consume atomically checks expiry and consumes the one-time token.
// Expiry is rechecked while holding the consumption lock so a record that
// raced the credential lifetime can never authenticate after expiry, and
// the token is consumed exactly once across concurrent attempts.
func (s *Server) consume(record authRecord) error {
	s.consumedMu.Lock()
	defer s.consumedMu.Unlock()
	if !time.Now().Before(s.expires) {
		return errBootstrapExpired
	}
	if s.consumed {
		return errBootstrapConsumed
	}
	tokenSum := sha256.Sum256(record.Token)
	nonceSum := sha256.Sum256(record.Nonce)
	if subtle.ConstantTimeCompare(tokenSum[:], s.tokenVerifier) != 1 ||
		subtle.ConstantTimeCompare(nonceSum[:], s.nonceVerifier) != 1 {
		return errBootstrapAuth
	}
	s.consumed = true
	return nil
}

// Close shuts down the listener and fails pending accepts. Established
// transports are unaffected. Token material is erased: the credential
// dies with the bootstrap process in every case.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.listener.Close()
		s.consumedMu.Lock()
		defer s.consumedMu.Unlock()
		zeroBytes(s.tokenVerifier)
		zeroBytes(s.nonceVerifier)
		s.tokenVerifier = nil
		s.nonceVerifier = nil
	})
	return s.closeErr
}

// Addr returns the bound address for the readiness record.
func (s *Server) Addr() string { return s.listener.Addr() }

// Dial dials a bootstrap server: pin the fingerprint, connect QUIC to
// the caller-resolved address, and send the single auth record. The
// address is composed by the caller from the SSH target host plus the
// readiness port: the readiness record never carries a dialable
// address. The token travels only inside the encrypted stream after
// the pinned handshake.
func Dial(ctx context.Context, addr string, readiness Readiness, config Config, timeout time.Duration) (wire.Transport, error) {
	if addr == "" {
		return nil, errBootstrapMalformed
	}
	token, err := base64.StdEncoding.DecodeString(readiness.Token)
	if err != nil || len(token) != bootstrapTokenBytes {
		return nil, errBootstrapMalformed
	}
	defer zeroBytes(token)
	nonce, err := base64.StdEncoding.DecodeString(readiness.Nonce)
	if err != nil || len(nonce) != bootstrapNonceBytes {
		return nil, errBootstrapMalformed
	}
	defer zeroBytes(nonce)
	fingerprint, err := hex.DecodeString(readiness.Fingerprint)
	if err != nil || len(fingerprint) != sha256.Size {
		return nil, errBootstrapMalformed
	}
	dialer := DialConfig(addr, "vev-bootstrap", fingerprint, config, timeout)
	transport, err := dialer.Dial(ctx)
	if err != nil {
		return nil, err
	}
	record, err := json.Marshal(authRecord{Token: token, Nonce: nonce})
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	// The framed copy of the secret is erased once the send attempt ends.
	defer zeroBytes(record)
	authCtx, cancel := context.WithTimeout(ctx, bootstrapAuthDeadline)
	defer cancel()
	if err := sendAuthRecord(authCtx, transport, record); err != nil {
		_ = transport.Close()
		return nil, err
	}
	return transport, nil
}

// sendAuthRecord sends the framed auth record within ctx. On timeout it
// closes the transport and joins the blocked send goroutine before returning,
// so the caller may erase record after return without racing the sender.
// The join relies on Close unblocking an in-flight Send, which wire.Transport
// implementations used here guarantee.
func sendAuthRecord(ctx context.Context, transport wire.Transport, record []byte) error {
	sent := make(chan error, 1)
	go func() {
		sent <- transport.Send(wire.Envelope{Payload: record})
	}()
	select {
	case <-ctx.Done():
		_ = transport.Close()
		<-sent
		return fmt.Errorf("%w: %w", errBootstrapAuth, ctx.Err())
	case err := <-sent:
		return err
	}
}

// ReadReadiness reads exactly one readiness line (≤4 KiB) from r with a
// bounded wait. Stderr is never mixed in: the caller wires stdout only.
func ReadReadiness(ctx context.Context, r io.Reader) (Readiness, error) {
	type readResult struct {
		line []byte
		err  error
	}
	results := make(chan readResult, 1)
	go func() {
		var buf bytes.Buffer
		tmp := make([]byte, 512)
		for buf.Len() <= bootstrapMaxReadiness {
			n, err := r.Read(tmp)
			if n > 0 {
				buf.Write(tmp[:n])
				if index := bytes.IndexByte(buf.Bytes(), '\n'); index >= 0 {
					results <- readResult{line: buf.Bytes()[:index]}
					return
				}
			}
			if err != nil {
				results <- readResult{err: err}
				return
			}
		}
		results <- readResult{err: errBootstrapMalformed}
	}()
	select {
	case <-ctx.Done():
		return Readiness{}, fmt.Errorf("%w: %w", errBootstrapMalformed, ctx.Err())
	case result := <-results:
		if result.err != nil {
			return Readiness{}, errBootstrapMalformed
		}
		return ParseReadiness(result.line)
	}
}
