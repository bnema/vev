package ipc

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/bnema/vev/internal/ports"
)

// maxFrameLen is the largest permitted frame length (the length field
// covers the type byte plus payload, not including the 4-byte length
// prefix itself).
const maxFrameLen = 16 << 20 // 16 MiB

// frameHeaderLen is the size, in bytes, of the length prefix that precedes
// every frame on the wire.
const frameHeaderLen = 4

// ErrZeroLengthFrame is returned by Recv when a frame's length field is
// zero. A valid frame always carries at least its type byte.
var ErrZeroLengthFrame = errors.New("ipc: zero-length frame")

// ErrFrameTooLarge is returned by Recv when a frame's length field exceeds
// maxFrameLen, and by Send when a payload is too large to encode.
var ErrFrameTooLarge = errors.New("ipc: frame exceeds maximum length")

// ErrBackpressure means the bounded IPC egress queue is full. Callers using
// SendAsync must treat it as a failed render emission rather than spawning an
// unbounded queue of their own.
var ErrBackpressure = errors.New("ipc: egress queue full")

var errClosed = errors.New("ipc: closed")

// sendQueueCapacity bounds frames accepted while a peer is not reading. It is
// deliberately small: render output can be large and must never turn a wedged
// peer into an unbounded memory commitment.
const sendQueueCapacity = 8

// unixTransport implements ports.Transport over a net.Conn (in practice an
// AF_UNIX SOCK_STREAM connection, but any net.Conn works — this also lets
// tests exercise it over net.Pipe).
//
// unixTransport has one writer goroutine which owns every conn.Write. Send
// waits for that owner's wire attempt; SendAsync only performs a bounded,
// ordered enqueue. Recv still has one caller at a time.
type Option func(*unixTransport)

// WithRuntimeObserver enables process-local transport marks. It accepts no
// clock: timestamp ownership belongs to the configured observer.
func WithRuntimeObserver(observer ports.SerializedRuntimeObserver) Option {
	return func(t *unixTransport) { t.observer = observer }
}

type unixTransport struct {
	conn     net.Conn
	observer ports.SerializedRuntimeObserver

	operationMu     sync.Mutex
	operationCount  int
	observedClosing bool
	operationsDone  chan struct{}

	// egressMu synchronizes Close with nonblocking enqueue. Once closed, no
	// request can be left behind after the writer drains the queue.
	egressMu      sync.Mutex
	egressClosing bool
	egress        chan sendRequest
	done          chan struct{}
	writerDone    chan struct{}
	closeOnce     sync.Once
	closeErr      error

	// readBuf is reused across Recv calls (grow-once strategy): it grows
	// to fit the largest frame seen so far and is never shrunk. The Frame
	// returned to the caller is always a freshly allocated, right-sized
	// copy, so callers may retain it indefinitely.
	readBuf []byte
}

// NewTransport wraps conn as a ports.Transport speaking vev's framed
// binary protocol.
func NewTransport(conn net.Conn, opts ...Option) ports.Transport {
	t := &unixTransport{
		conn:       conn,
		egress:     make(chan sendRequest, sendQueueCapacity),
		done:       make(chan struct{}),
		writerDone: make(chan struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	go t.writeLoop()
	return t
}

type sendRequest struct {
	data []byte
	done chan error // nil for SendAsync
	end  func(bool)
}

// Send queues f for the sole writer and waits for its wire attempt.
func (t *unixTransport) Send(f ports.Frame) error {
	data, err := marshalFrame(f)
	if err != nil {
		return err
	}
	end := t.beginOperation(ports.RuntimeAdapterSendStart, uint64(len(f.Payload)))
	result := make(chan error, 1)
	if err := t.enqueue(sendRequest{data: data, done: result, end: end}); err != nil {
		end(false)
		return err
	}
	select {
	case err := <-result:
		return err
	case <-t.done:
		return errClosed
	}
}

// SendAsync accepts f for ordered background transmission without waiting for
// a socket write. The queue is bounded and ownership includes a payload copy.
func (t *unixTransport) SendAsync(f ports.Frame) error {
	data, err := marshalFrame(f)
	if err != nil {
		return err
	}
	end := t.beginOperation(ports.RuntimeAdapterSendStart, uint64(len(f.Payload)))
	if err := t.enqueue(sendRequest{data: data, end: end}); err != nil {
		end(false)
		return err
	}
	return nil
}

func marshalFrame(f ports.Frame) ([]byte, error) {
	n := 1 + len(f.Payload) // type + payload
	if n > maxFrameLen {
		return nil, ErrFrameTooLarge
	}
	buf := make([]byte, frameHeaderLen+n)
	binary.BigEndian.PutUint32(buf[:frameHeaderLen], uint32(n))
	buf[frameHeaderLen] = byte(f.Type)
	copy(buf[frameHeaderLen+1:], f.Payload)
	return buf, nil
}

func (t *unixTransport) enqueue(req sendRequest) error {
	t.egressMu.Lock()
	defer t.egressMu.Unlock()
	if t.egressClosing {
		return errClosed
	}
	select {
	case t.egress <- req:
		return nil
	default:
		return ErrBackpressure
	}
}

func (t *unixTransport) writeLoop() {
	defer close(t.writerDone)
	for {
		select {
		case req := <-t.egress:
			t.completeWrite(req, writeAll(t.conn, req.data))
		case <-t.done:
			t.egressMu.Lock()
			for {
				select {
				case req := <-t.egress:
					t.completeWrite(req, errClosed)
				default:
					t.egressMu.Unlock()
					return
				}
			}
		}
	}
}

func (t *unixTransport) completeWrite(req sendRequest, err error) {
	if req.end != nil {
		req.end(err == nil)
	}
	if req.done != nil {
		req.done <- err
	}
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Recv reads one frame: a 4-byte length, then a body of that many bytes
// (type byte + payload). It blocks until a full frame arrives, the
// connection is closed (io.EOF), or an error occurs.
func (t *unixTransport) Recv() (ports.Frame, error) {
	end := t.beginOperation(ports.RuntimeAdapterReceiveStart, 0)
	var hdr [frameHeaderLen]byte
	if _, err := io.ReadFull(t.conn, hdr[:]); err != nil {
		end(false)
		return ports.Frame{}, err
	}

	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		end(false)
		return ports.Frame{}, ErrZeroLengthFrame
	}
	if n > maxFrameLen {
		end(false)
		return ports.Frame{}, ErrFrameTooLarge
	}

	if cap(t.readBuf) < int(n) {
		t.readBuf = make([]byte, n)
	} else {
		t.readBuf = t.readBuf[:n]
	}
	if _, err := io.ReadFull(t.conn, t.readBuf); err != nil {
		end(false)
		return ports.Frame{}, err
	}

	var payload []byte
	if n > 1 {
		payload = make([]byte, n-1)
		copy(payload, t.readBuf[1:])
	}
	end(true)
	return ports.Frame{Type: ports.MsgType(t.readBuf[0]), Payload: payload}, nil
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

// Close interrupts Recv and the writer's current Write, fails queued frames,
// and waits until the sole writer and all observed operations have stopped.
func (t *unixTransport) Close() error {
	t.closeOnce.Do(func() {
		operationsDone := t.beginShutdown()
		t.egressMu.Lock()
		t.egressClosing = true
		close(t.done)
		t.egressMu.Unlock()
		t.closeErr = t.conn.Close()
		<-t.writerDone
		if operationsDone != nil {
			<-operationsDone
		}
	})
	return t.closeErr
}
