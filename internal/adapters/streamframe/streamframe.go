// Package streamframe carries length-delimited Protobuf envelopes over one
// reliable ordered byte stream. It owns no application semantics: IPC, SSH
// stdio, and QUIC adapters compose it for framing and share its bounds,
// serialized writes, and close behavior.
package streamframe

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

// Framing bounds. The absolute ceiling never moves; per-connection
// negotiated ceilings only lower it.
const (
	// AbsoluteLimit caps one serialized envelope on any stream.
	AbsoluteLimit = 16 << 20
	// DefaultQueuedMessages bounds frames accepted while a peer is slow.
	DefaultQueuedMessages = 8
	// DefaultQueuedBytes bounds queued payload bytes per connection.
	DefaultQueuedBytes = 32 << 20
)

var (
	ErrClosed       = errors.New("streamframe: closed")
	ErrBackpressure = errors.New("streamframe: queue full")
	ErrTooLarge     = errors.New("streamframe: envelope exceeds maximum length")
	ErrZeroLength   = errors.New("streamframe: zero-length envelope")
)

// Option tunes a Framer.
type Option func(*Framer)

// WithMaxEnvelope sets the negotiated per-connection ceiling. Values above
// AbsoluteLimit are clamped; values below 1 disable the framer.
func WithMaxEnvelope(limit uint64) Option {
	return func(f *Framer) {
		switch {
		case limit == 0:
			f.maxEnvelope = 0
		case limit > AbsoluteLimit:
			f.maxEnvelope = AbsoluteLimit
		default:
			f.maxEnvelope = int(limit)
		}
	}
}

// WithQueueBounds sets the bounded async admission budget.
func WithQueueBounds(messages int, bytes uint64) Option {
	return func(f *Framer) {
		if messages > 0 {
			f.queueMessages = messages
		}
		if bytes > 0 {
			f.queueBytes = bytes
		}
	}
}

// Framer frames complete envelopes over r/w: 4-byte big-endian length then
// payload, no type byte. One writer is serialized internally; exactly one
// Recv caller at a time. Close is concurrent-safe and unblocks in-flight
// Send and Recv, then invokes the injected closer exactly once so blocked
// I/O on the underlying stream releases.
type Framer struct {
	r      io.Reader
	w      io.Writer
	closer func() error

	maxEnvelope   int
	queueMessages int
	queueBytes    uint64

	writeMu sync.Mutex

	// readMu serializes Recv so concurrent receivers each get one
	// complete envelope instead of racing on the reused read buffer.
	readMu sync.Mutex

	egressMu          sync.Mutex
	egressClosing     bool
	egressSenders     int
	egressSendersDone chan struct{}
	egress            chan sendRequest
	queuedBytes       uint64
	done              chan struct{}
	writerDone        chan struct{}
	closeOnce         sync.Once
	closeErr          error

	readBuf []byte
}

type sendRequest struct {
	data []byte
	done chan error // nil for async admission
	// counted is the byte-budget charge admitted for this request.
	// Synchronous sends rendezvous with the writer instead of
	// charging the async byte budget, so only async requests carry
	// a charge the write loop releases on dequeue.
	counted uint64
}

// NewFramer builds a Framer over r/w. closer releases the underlying stream
// and is invoked exactly once by Close; it may be nil. Blocked Recv calls
// observe Close promptly only when closer releases the underlying read
// (concrete adapters close their connection/stream there); otherwise Recv
// returns when that source itself errors or closes.
func NewFramer(r io.Reader, w io.Writer, closer func() error, opts ...Option) *Framer {
	f := &Framer{
		r:             r,
		w:             w,
		closer:        closer,
		maxEnvelope:   AbsoluteLimit,
		queueMessages: DefaultQueuedMessages,
		queueBytes:    DefaultQueuedBytes,
		egress:        make(chan sendRequest, DefaultQueuedMessages),
		done:          make(chan struct{}),
		writerDone:    make(chan struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(f)
		}
	}
	if cap(f.egress) < f.queueMessages {
		f.egress = make(chan sendRequest, f.queueMessages)
	}
	go f.writeLoop()
	return f
}

// Send copies payload into the bounded queue and waits for the wire attempt.
// It reports the write result, ErrClosed, or ErrTooLarge. The synchronous
// path rendezvous with the writer instead of charging the async byte
// budget, so it carries no counted charge to release on dequeue.
func (f *Framer) Send(payload []byte) error {
	data, err := f.frame(payload)
	if err != nil {
		return err
	}
	result := make(chan error, 1)
	if err := f.enqueueWait(sendRequest{data: data, done: result}); err != nil {
		return err
	}
	select {
	case err := <-result:
		return err
	case <-f.done:
		return ErrClosed
	}
}

// SendAsync admits payload for ordered background transmission without
// waiting for the write. It returns ErrBackpressure when the byte or message
// budget is exhausted; ownership includes a payload copy.
func (f *Framer) SendAsync(payload []byte) error {
	data, err := f.frame(payload)
	if err != nil {
		return err
	}
	return f.enqueueAsync(sendRequest{data: data})
}

// Recv reads one complete envelope and returns a freshly allocated copy the
// caller owns. Concurrent Recv calls are serialized; each receives one
// complete envelope. It blocks until a full envelope arrives, the framer
// closes (io.EOF), or an error occurs.
func (f *Framer) Recv() ([]byte, error) {
	f.readMu.Lock()
	defer f.readMu.Unlock()
	var header [4]byte
	if _, err := io.ReadFull(f.r, header[:]); err != nil {
		if f.isClosed() {
			return nil, io.EOF
		}
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 {
		return nil, ErrZeroLength
	}
	if f.maxEnvelope <= 0 || length > uint32(f.maxEnvelope) {
		return nil, ErrTooLarge
	}
	if cap(f.readBuf) < int(length) {
		f.readBuf = make([]byte, length)
	} else {
		f.readBuf = f.readBuf[:length]
	}
	if _, err := io.ReadFull(f.r, f.readBuf); err != nil {
		if f.isClosed() {
			return nil, io.EOF
		}
		return nil, err
	}
	return append([]byte(nil), f.readBuf...), nil
}

// Close unblocks in-flight Send and Recv, fails queued payloads, waits for
// the sole writer, then invokes the injected closer exactly once.
func (f *Framer) Close() error {
	f.closeOnce.Do(func() {
		f.egressMu.Lock()
		f.egressClosing = true
		close(f.done)
		f.egressMu.Unlock()
		if f.closer != nil {
			f.closeErr = f.closer()
		}
		<-f.writerDone
	})
	return f.closeErr
}

func (f *Framer) isClosed() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

func (f *Framer) frame(payload []byte) ([]byte, error) {
	if f.maxEnvelope <= 0 {
		return nil, ErrClosed
	}
	if len(payload) == 0 {
		return nil, ErrZeroLength
	}
	if len(payload) > f.maxEnvelope {
		return nil, ErrTooLarge
	}
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	return buf, nil
}

func (f *Framer) enqueueWait(req sendRequest) error {
	f.egressMu.Lock()
	if f.egressClosing {
		f.egressMu.Unlock()
		return ErrClosed
	}
	if f.egressSenders == 0 {
		f.egressSendersDone = make(chan struct{})
	}
	f.egressSenders++
	f.egressMu.Unlock()

	select {
	case <-f.done:
		f.finishEgressSender()
		return ErrClosed
	case f.egress <- req:
		f.finishEgressSender()
		return nil
	}
}

// withCountedCharge stamps the async byte-budget charge onto the request
// before it enters the queue: channel sends copy the struct, so the charge
// must be set prior to admission for the write loop to release it later.
func withCountedCharge(req sendRequest) sendRequest {
	req.counted = uint64(len(req.data))
	return req
}

func (f *Framer) enqueueAsync(req sendRequest) error {
	f.egressMu.Lock()
	defer f.egressMu.Unlock()
	if f.egressClosing {
		return ErrClosed
	}
	if uint64(len(req.data)) > f.queueBytes-f.queuedBytes {
		return ErrBackpressure
	}
	select {
	case f.egress <- withCountedCharge(req):
		f.queuedBytes += uint64(len(req.data))
		return nil
	default:
		return ErrBackpressure
	}
}

func (f *Framer) finishEgressSender() {
	f.egressMu.Lock()
	defer f.egressMu.Unlock()
	f.egressSenders--
	if f.egressSenders == 0 {
		close(f.egressSendersDone)
	}
}

func (f *Framer) waitForEgressSenders() {
	f.egressMu.Lock()
	done := f.egressSendersDone
	f.egressMu.Unlock()
	if done != nil {
		<-done
	}
}

func (f *Framer) writeLoop() {
	defer close(f.writerDone)
	for {
		select {
		case req := <-f.egress:
			f.egressMu.Lock()
			f.queuedBytes -= min(f.queuedBytes, req.counted)
			f.egressMu.Unlock()
			f.completeWrite(req, writeAll(f.w, req.data))
		case <-f.done:
			f.waitForEgressSenders()
			f.egressMu.Lock()
			for {
				select {
				case req := <-f.egress:
					f.completeWrite(req, ErrClosed)
				default:
					f.egressMu.Unlock()
					return
				}
			}
		}
	}
}

func (f *Framer) completeWrite(req sendRequest, err error) {
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
