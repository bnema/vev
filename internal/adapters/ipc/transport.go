package ipc

import (
	"encoding/binary"
	"errors"
	"io"
	"net"

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

// unixTransport implements ports.Transport over a net.Conn (in practice an
// AF_UNIX SOCK_STREAM connection, but any net.Conn works — this also lets
// tests exercise it over net.Pipe).
//
// unixTransport is safe for one concurrent caller of Send and one
// concurrent caller of Recv at the same time (i.e. a single reader
// goroutine and a single writer goroutine); it is not safe for concurrent
// calls to Send from multiple goroutines, nor Recv from multiple
// goroutines.
type Option func(*unixTransport)

// WithRuntimeObserver enables process-local transport marks. It accepts no
// clock: timestamp ownership belongs to the configured observer.
func WithRuntimeObserver(observer ports.RuntimeObserver) Option {
	return func(t *unixTransport) { t.observer = observer }
}

type unixTransport struct {
	conn     net.Conn
	observer ports.RuntimeObserver

	// readBuf is reused across Recv calls (grow-once strategy): it grows
	// to fit the largest frame seen so far and is never shrunk. The Frame
	// returned to the caller is always a freshly allocated, right-sized
	// copy, so callers may retain it indefinitely.
	readBuf []byte
}

// NewTransport wraps conn as a ports.Transport speaking vev's framed
// binary protocol.
func NewTransport(conn net.Conn, opts ...Option) ports.Transport {
	t := &unixTransport{conn: conn}
	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}
	return t
}

// Send writes f as a single frame: a 4-byte big-endian length (covering
// the type byte and payload), the type byte, then the payload — assembled
// into one buffer and written with a single Write call.
func (t *unixTransport) Send(f ports.Frame) error {
	end := t.beginOperation(ports.RuntimeAdapterSendStart, uint64(len(f.Payload)))
	n := 1 + len(f.Payload) // type + payload
	if n > maxFrameLen {
		end(false)
		return ErrFrameTooLarge
	}

	buf := make([]byte, frameHeaderLen+n)
	binary.BigEndian.PutUint32(buf[:frameHeaderLen], uint32(n))
	buf[frameHeaderLen] = byte(f.Type)
	copy(buf[frameHeaderLen+1:], f.Payload)

	_, err := t.conn.Write(buf)
	end(err == nil)
	return err
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
	correlation := ports.NewRuntimeCorrelation()
	t.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("ipc", correlation, start, bytes, true))
	end := ports.RuntimeAdapterSendEnd
	if start == ports.RuntimeAdapterReceiveStart {
		end = ports.RuntimeAdapterReceiveEnd
	}
	return func(valid bool) {
		t.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("ipc", correlation, end, bytes, valid))
	}
}

// Close closes the underlying connection.
func (t *unixTransport) Close() error {
	return t.conn.Close()
}
