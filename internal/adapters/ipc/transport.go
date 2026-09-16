package ipc

import (
	"errors"
	"net"
	"sync"

	"github.com/bnema/vev/internal/adapters/streamframe"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// ErrZeroLengthFrame is returned by Recv when an envelope's length field is
// zero. Kept as an alias so existing callers keep compiling; new code
// prefers streamframe.ErrZeroLength.
var ErrZeroLengthFrame = streamframe.ErrZeroLength

// ErrFrameTooLarge is returned by Recv when an envelope's length field
// exceeds the ceiling, and by Send when a payload is too large to encode.
var ErrFrameTooLarge = streamframe.ErrTooLarge

// ErrBackpressure means the bounded IPC egress queue is full. Callers using
// SendAsync must treat it as a failed render emission rather than spawning an
// unbounded queue of their own.
var ErrBackpressure = streamframe.ErrBackpressure

var errClosed = streamframe.ErrClosed

// unixTransport implements wire.Transport over a net.Conn (in practice an
// AF_UNIX SOCK_STREAM connection, but any net.Conn works — this also lets
// tests exercise it over net.Pipe).
//
// Framing, queueing, and close behavior live in streamframe; this adapter
// retains lifecycle ownership (conn close releases blocked Recv), peer
// checks, and runtime observation.
type Option func(*unixTransport)

// WithRuntimeObserver enables process-local transport marks. It accepts no
// clock: timestamp ownership belongs to the configured observer.
func WithRuntimeObserver(observer ports.SerializedRuntimeObserver) Option {
	return func(t *unixTransport) { t.observer = observer }
}

type unixTransport struct {
	conn     net.Conn
	observer ports.SerializedRuntimeObserver
	framer   *streamframe.Framer

	operationMu     sync.Mutex
	operationCount  int
	observedClosing bool
	operationsDone  chan struct{}

	closeOnce sync.Once
	closeErr  error
}

// NewTransport wraps conn as a wire.Transport speaking vev's length-framed
// envelope protocol.
func NewTransport(conn net.Conn, opts ...Option) wire.Transport {
	t := &unixTransport{conn: conn}
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	t.framer = streamframe.NewFramer(conn, conn, conn.Close)
	return t
}

var _ wire.BoundedTransport = (*unixTransport)(nil)

// Send queues the envelope for the sole writer and waits for its wire attempt.
func (t *unixTransport) Send(envelope wire.Envelope) error {
	end := t.beginOperation(ports.RuntimeAdapterSendStart, uint64(len(envelope.Payload)))
	err := t.framer.Send(envelope.Payload)
	end(err == nil)
	return err
}

// SendAsync accepts the envelope for ordered background transmission without
// waiting for a socket write. The queue is bounded and ownership includes a
// payload copy.
func (t *unixTransport) SendAsync(envelope wire.Envelope) error {
	end := t.beginOperation(ports.RuntimeAdapterSendStart, uint64(len(envelope.Payload)))
	err := t.framer.SendAsync(envelope.Payload)
	end(err == nil)
	return err
}

// Recv reads one complete envelope. It blocks until a full envelope
// arrives, the connection is closed (io.EOF), or an error occurs.
func (t *unixTransport) Recv() (wire.Envelope, error) {
	end := t.beginOperation(ports.RuntimeAdapterReceiveStart, 0)
	payload, err := t.framer.Recv()
	if err != nil {
		end(false)
		return wire.Envelope{}, mapFramerError(err)
	}
	end(true)
	return wire.Envelope{Payload: payload}, nil
}

// RecvBounded reads one complete envelope bounded to limit before allocation,
// sharing the framer's validation with Recv: an over-limit length prefix is
// refused without reading its body. It is the bounded raw-carriage capability
// a preamble-negotiated multiplexer consumes.
func (t *unixTransport) RecvBounded(limit uint64) (wire.Envelope, error) {
	end := t.beginOperation(ports.RuntimeAdapterReceiveStart, 0)
	payload, err := t.framer.RecvBounded(limit)
	if err != nil {
		end(false)
		return wire.Envelope{}, mapFramerError(err)
	}
	end(true)
	return wire.Envelope{Payload: payload}, nil
}

func mapFramerError(err error) error {
	switch {
	case errors.Is(err, streamframe.ErrZeroLength):
		return ErrZeroLengthFrame
	case errors.Is(err, streamframe.ErrTooLarge):
		return ErrFrameTooLarge
	case errors.Is(err, streamframe.ErrClosed):
		return errClosed
	default:
		return err
	}
}

func (t *unixTransport) beginOperation(start ports.RuntimeMarkKind, bytes uint64) func(bool) {
	if t.observer == nil {
		return func(bool) {}
	}
	if !t.beginObservedOperation() {
		return func(bool) {}
	}
	correlation := ports.NewRuntimeCorrelation()
	t.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("ipc", correlation, start, bytes, true))
	end := ports.RuntimeAdapterSendEnd
	if start == ports.RuntimeAdapterReceiveStart {
		end = ports.RuntimeAdapterReceiveEnd
	}
	return func(valid bool) {
		defer t.finishObservedOperation()
		t.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("ipc", correlation, end, bytes, valid))
	}
}

func (t *unixTransport) beginObservedOperation() bool {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	if t.observedClosing {
		return false
	}
	if t.operationCount == 0 {
		t.operationsDone = make(chan struct{})
	}
	t.operationCount++
	return true
}

func (t *unixTransport) finishObservedOperation() {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	t.operationCount--
	if t.operationCount == 0 {
		close(t.operationsDone)
	}
}

func (t *unixTransport) beginShutdown() <-chan struct{} {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	t.observedClosing = true
	if t.operationCount == 0 {
		return nil
	}
	return t.operationsDone
}

// Close interrupts Recv (via conn.Close, which releases the framer's
// blocked read) and the writer, fails queued envelopes, and waits until
// the sole writer and all observed operations have stopped.
func (t *unixTransport) Close() error {
	t.closeOnce.Do(func() {
		operationsDone := t.beginShutdown()
		t.closeErr = t.framer.Close()
		if operationsDone != nil {
			<-operationsDone
		}
	})
	return t.closeErr
}
