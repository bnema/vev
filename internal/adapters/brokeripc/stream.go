package brokeripc

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/ports"
)

// Logical stream bridging (P3.3).
//
// One brokerwire logical stream carries the typed session protocol between a
// client and the daemon behind the broker. The broker terminates both hops: the
// client's session connection is built by sessionwire over the stream's private
// bounded byte pipe, and the admitted broker core hands back its own typed
// connection to the daemon. Two relay goroutines carry typed messages between
// them, so no session byte is ever reinterpreted here.
//
// Identity and cancellation stay per stream: a stream the core loses, a stream
// whose local consumer stalled, and a stream the peer closed each settle alone,
// sending at most one typed StreamClosed and releasing only that stream's
// resources. The connection and every sibling stream stay up.

// serverStream is the broker-side half of one logical stream.
type serverStream struct {
	session  *serverSession
	id       ports.BrokerStreamID
	core     ports.BrokerLogicalConnection
	pipe     *streamPipe
	carriage *carriage
	wire     ports.ServerConnection

	mu     sync.Mutex
	closed bool
	done   chan struct{}
}

// newServerStream builds one bridged stream over the admitted core connection.
func newServerStream(s *serverSession, id ports.BrokerStreamID, core ports.BrokerLogicalConnection) (*serverStream, error) {
	if s == nil || core == nil || id == 0 {
		return nil, ErrConfig
	}
	st := &serverStream{session: s, id: id, core: core, done: make(chan struct{})}
	st.pipe = newStreamPipe(s.cfg.StreamInboundChunks, s.cfg.StreamInboundBytes, st.sendChunk)
	st.carriage = newCarriage(st.pipe)
	st.wire = sessionwire.NewServerConnection(st.carriage)
	if st.wire == nil {
		return nil, ErrConfig
	}
	return st, nil
}

// sendChunk splits one outbound byte slice under the negotiated stream chunk
// ceiling and writes each frame in order.
func (st *serverStream) sendChunk(data []byte) error {
	ceiling := int(st.session.ceilings.StreamChunkLimit)
	if ceiling <= 0 {
		return ErrConfig
	}
	for len(data) > 0 {
		n := min(len(data), ceiling)
		if err := st.session.send(brokerwire.ServerStreamData{
			Epoch: st.session.epoch, Connection: st.session.scope.Connection, Stream: st.id, Data: data[:n],
		}); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// deliver accepts one inbound chunk from the connection reader.
func (st *serverStream) deliver(chunk []byte) error { return st.pipe.deliver(chunk) }

// start launches the three per-stream workers. Every one is joined by the
// session's WaitGroup before shutdown returns.
func (st *serverStream) start() {
	st.session.wg.Add(3)
	go st.relayClient()
	go st.relayServer()
	go st.watchCore()
}

// relayClient carries client-to-daemon messages from the wire session
// connection to the admitted core connection.
func (st *serverStream) relayClient() {
	defer st.session.wg.Done()
	for {
		message, err := st.wire.ReceiveClient()
		if err != nil {
			st.fail(cancelOrCause(st.session.ctx, streamFailure(err)))
			return
		}
		if err := st.core.SendClient(message); err != nil {
			st.fail(err)
			return
		}
	}
}

// relayServer carries daemon-to-client messages from the admitted core
// connection to the wire session connection.
func (st *serverStream) relayServer() {
	defer st.session.wg.Done()
	for {
		message, err := st.core.ReceiveServer()
		if err != nil {
			st.fail(coreFailure(st.core, err))
			return
		}
		if err := st.wire.SendServer(message); err != nil {
			st.fail(streamFailure(err))
			return
		}
	}
}

// watchCore publishes the core connection's terminal outcome even when no read
// is in flight.
func (st *serverStream) watchCore() {
	defer st.session.wg.Done()
	select {
	case <-st.core.Done():
		st.fail(coreFailure(st.core, nil))
	case <-st.done:
	}
}

// fail settles the stream once and reports it to the session, which retires its
// wire identity and sends at most one typed StreamClosed.
func (st *serverStream) fail(err error) {
	if st.close(err) {
		st.session.settleStream(st, err)
	}
}

// close releases the stream exactly once: the pipe unblocks a stalled reader,
// the wire session connection releases the carriage, and the core connection is
// closed. It reports whether this call was the one that settled the stream.
func (st *serverStream) close(cause error) bool {
	if st == nil {
		return false
	}
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return false
	}
	st.closed = true
	st.mu.Unlock()
	st.pipe.closeWith(cause)
	_ = st.wire.Close()
	_ = st.core.Close()
	close(st.done)
	return true
}

// streamFailure classifies one wire-side stream error: an orderly carriage end
// is a peer close, not a transport failure.
func streamFailure(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return ErrStreamGone
	}
	return err
}

// coreFailure classifies one core connection outcome. The core's typed terminal
// error wins; an orderly end is a peer close.
func coreFailure(core ports.BrokerLogicalConnection, err error) error {
	if err == nil && core != nil {
		err = core.Err()
	}
	if err == nil {
		return ErrStreamGone
	}
	var failure ports.BrokerError
	if errors.As(err, &failure) {
		return err
	}
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return ErrStreamGone
	}
	return err
}

// orderlyStream reports whether one stream outcome closes the stream without a
// failure detail.
func orderlyStream(err error) bool {
	return err == nil || errors.Is(err, ErrStreamGone)
}

// startStream admits one inbound open on the wire tracker and starts its bridge
// once the broker core established the stream.
func (s *serverSession) startStream(m brokerwire.OpenStream) {
	if err := s.conn.OpenStream(m.Stream); err != nil {
		if sendErr := s.refuseStream(m.Stream, admissionError(err)); sendErr != nil {
			s.abort(sendErr)
		}
		return
	}
	request := ports.BrokerOpenStreamRequest{
		Epoch: m.Epoch, Purpose: m.Purpose, Local: m.Local,
		Connection: m.Connection, Stream: m.Stream,
		Endpoint: m.Endpoint, Registration: m.Registration,
		Target: m.Target, Env: m.Env, Policy: m.Policy,
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		core, err := s.core.OpenStream(s.ctx, request)
		if err != nil {
			_, _ = s.conn.CloseStream(m.Stream)
			if sendErr := s.refuseStream(m.Stream, err); sendErr != nil {
				s.abort(sendErr)
			}
			return
		}
		if core == nil {
			_, _ = s.conn.CloseStream(m.Stream)
			if sendErr := s.refuseStream(m.Stream, errors.Join(ports.BrokerAdmissionInvalid, errors.New("brokeripc: core returned a nil logical connection"))); sendErr != nil {
				s.abort(sendErr)
			}
			return
		}
		stream, err := newServerStream(s, m.Stream, core)
		if err != nil {
			_ = core.Close()
			_, _ = s.conn.CloseStream(m.Stream)
			if sendErr := s.refuseStream(m.Stream, err); sendErr != nil {
				s.abort(sendErr)
			}
			return
		}
		if !s.registerStream(stream) {
			stream.close(ErrSessionClosed)
			return
		}
		if err := s.conn.StreamOpened(m.Stream); err != nil {
			s.settleStream(stream, errors.Join(ErrProtocol, err))
			return
		}
		if err := s.send(brokerwire.StreamOpened{Epoch: s.epoch, Connection: s.scope.Connection, Stream: m.Stream}); err != nil {
			s.settleStream(stream, transportFailure(err))
			return
		}
		stream.start()
	}()
}

// registerStream installs one bridge, or reports false when the session is
// closing.
func (s *serverSession) registerStream(st *serverStream) bool {
	s.streamsMu.Lock()
	defer s.streamsMu.Unlock()
	if s.ctx.Err() != nil {
		return false
	}
	s.streams[st.id] = st
	return true
}

// unregisterStream removes one bridge, if present.
func (s *serverSession) unregisterStream(id ports.BrokerStreamID) {
	s.streamsMu.Lock()
	delete(s.streams, id)
	s.streamsMu.Unlock()
}

// lookupStream returns the bridge for one stream identity.
func (s *serverSession) lookupStream(id ports.BrokerStreamID) *serverStream {
	s.streamsMu.Lock()
	defer s.streamsMu.Unlock()
	return s.streams[id]
}

// settleStream retires one stream's wire identity and reports its outcome to the
// peer. A session that is already shutting down sends nothing: the carriage is
// closing and the peer is gone.
func (s *serverSession) settleStream(st *serverStream, err error) {
	if st == nil {
		return
	}
	st.close(err)
	s.unregisterStream(st.id)
	_, _ = s.conn.CloseStream(st.id)
	// Retire the broker-side stream reservation as well, so a stream that
	// settled for any reason is released at the core and not only in its
	// carriage.
	_ = s.core.CloseStream(s.scope.Connection, st.id)
	if s.ctx.Err() != nil {
		return
	}
	detail := brokerwire.ErrorDetail{}
	if !orderlyStream(err) {
		detail = errorDetail(err)
	}
	if sendErr := s.send(brokerwire.StreamClosed{
		Epoch: s.epoch, Connection: s.scope.Connection, Stream: st.id,
		Error: detail, HasError: detail != (brokerwire.ErrorDetail{}),
	}); sendErr != nil {
		s.abort(sendErr)
	}
}

// closeStreams settles every bridged stream during shutdown, releasing each
// core connection and session carriage exactly once.
func (s *serverSession) closeStreams() {
	s.streamsMu.Lock()
	streams := make([]*serverStream, 0, len(s.streams))
	for _, st := range s.streams {
		streams = append(streams, st)
	}
	s.streams = make(map[ports.BrokerStreamID]*serverStream)
	s.streamsMu.Unlock()
	for _, st := range streams {
		st.close(ErrSessionClosed)
	}
}

// closeStreamByPeer retires one stream the peer closed. No StreamClosed is sent
// back: the peer already knows.
func (s *serverSession) closeStreamByPeer(id ports.BrokerStreamID) {
	st := s.lookupStream(id)
	if _, err := s.conn.CloseStream(id); err != nil && !errors.Is(err, brokerwire.ErrFutureStream) {
		// A close for a stream that was never allocated is a peer protocol
		// violation; a late close for a retired stream is simply discarded.
		if errors.Is(err, brokerwire.ErrConnectionState) || errors.Is(err, brokerwire.ErrScopeMismatch) {
			s.abort(errors.Join(ErrProtocol, err))
			return
		}
	}
	if st == nil {
		return
	}
	s.unregisterStream(id)
	st.close(ErrStreamGone)
	// Retire the broker-side stream reservation as well: the peer's close ends
	// the client-scoped stream, not merely its carriage.
	_ = s.core.CloseStream(s.scope.Connection, id)
}

// deliverStreamData routes one inbound client chunk to its bridge. Backpressure
// settles only that stream; a chunk for an identity the peer never opened is a
// protocol violation.
func (s *serverSession) deliverStreamData(m brokerwire.ClientStreamData) error {
	disposition, err := s.conn.StreamData(m.Stream)
	if err != nil {
		// Stream data for a stream that was never allocated, or before it was
		// confirmed open, is a peer protocol violation.
		return errors.Join(ErrProtocol, err)
	}
	if disposition == brokerwire.StreamDiscarded {
		return nil
	}
	st := s.lookupStream(m.Stream)
	if st == nil {
		// The bridge was settled between the tracker check and the lookup (a
		// stream-local failure won the race); the chunk is discarded with it.
		return nil
	}
	switch err := st.deliver(m.Data); {
	case err == nil:
		return nil
	case errors.Is(err, ErrStreamBackpressure):
		s.settleStream(st, ErrStreamBackpressure)
		return nil
	default:
		// The stream already reached its terminal outcome; its data is
		// discarded.
		return nil
	}
}

// clientStream is the client-side half of one logical stream: the sessionwire
// client connection over the stream's bounded pipe, plus the terminal outcome a
// ports.BrokerLogicalConnection exposes independently of reads.
type clientStream struct {
	ports.ClientConnection
	client   *client
	id       ports.BrokerStreamID
	pipe     *streamPipe
	carriage *carriage

	opened chan error
	mu     sync.Mutex
	closed bool
	err    error
	done   chan struct{}
}

// newClientStream builds one client-side stream over the broker connection.
func newClientStream(c *client, id ports.BrokerStreamID) (*clientStream, error) {
	if c == nil || id == 0 {
		return nil, ErrConfig
	}
	st := &clientStream{
		client: c,
		id:     id,
		opened: make(chan error, 1),
		done:   make(chan struct{}),
	}
	st.pipe = newStreamPipe(c.cfg.StreamInboundChunks, c.cfg.StreamInboundBytes, st.sendChunk)
	st.carriage = newCarriage(st.pipe)
	st.ClientConnection = sessionwire.NewClientConnection(st.carriage)
	if st.ClientConnection == nil {
		return nil, ErrConfig
	}
	return st, nil
}

// sendChunk splits one outbound client byte slice under the negotiated stream
// chunk ceiling.
func (st *clientStream) sendChunk(data []byte) error {
	ceiling := int(st.client.ceilings.StreamChunkLimit)
	if ceiling <= 0 {
		return ErrConfig
	}
	for len(data) > 0 {
		n := min(len(data), ceiling)
		if err := st.client.send(brokerwire.ClientStreamData{
			Epoch: st.client.scope.Epoch, Connection: st.client.scope.Connection, Stream: st.id, Data: data[:n],
		}); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// deliver accepts one inbound server chunk.
func (st *clientStream) deliver(chunk []byte) error { return st.pipe.deliver(chunk) }

// Done closes when the stream reached its terminal outcome.
func (st *clientStream) Done() <-chan struct{} { return st.done }

// Err reports the terminal cause; nil is an orderly close.
func (st *clientStream) Err() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.err
}

// Close ends the stream: it sends one CloseStream frame best-effort, releases
// the carriage, and resolves any pending open. It is idempotent and
// concurrent-safe.
func (st *clientStream) Close() error {
	if st == nil {
		return nil
	}
	st.client.requestStreamClose(st.id)
	st.terminate(nil)
	return nil
}

// terminate settles the stream exactly once, publishes its terminal cause (nil
// for an orderly close), and resolves a pending open.
func (st *clientStream) terminate(cause error) bool {
	if st == nil {
		return false
	}
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return false
	}
	st.closed = true
	st.err = cause
	st.mu.Unlock()
	st.pipe.closeWith(cause)
	_ = st.carriage.Close()
	st.client.unregisterStream(st.id)
	// Retire the identity on the connection's tracker as well, so a frame that
	// arrives after local retirement is classified as discarded.
	_, _ = st.client.conn.CloseStream(st.id)
	select {
	case st.opened <- cause:
	default:
	}
	close(st.done)
	return true
}

// fail settles the stream and tells the broker to retire it, so a stream-local
// failure never leaves the peer half-open.
func (st *clientStream) fail(err error) {
	if st.terminate(err) {
		st.client.requestStreamClose(st.id)
	}
}
