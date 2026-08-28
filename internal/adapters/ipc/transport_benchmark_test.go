package ipc

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
)

func BenchmarkTransportSend(b *testing.B) {
	conn := discardConn{}
	tr := NewTransport(conn)
	payload := []byte("payload payload payload")
	frame := wire.Frame{Type: wire.MsgOutput, Payload: payload}

	b.ReportAllocs()
	for b.Loop() {
		if err := tr.Send(frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTransportRecvReuse(b *testing.B) {
	payload := []byte("payload payload payload")
	encoded := encodeBenchmarkFrame(wire.Frame{Type: wire.MsgOutput, Payload: payload})
	recv := &unixTransport{conn: &loopingReaderConn{data: encoded}}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := recv.Recv(); err != nil {
			b.Fatal(err)
		}
	}
}

func encodeBenchmarkFrame(f wire.Frame) []byte {
	n := 1 + len(f.Payload)
	buf := make([]byte, frameHeaderLen+n)
	binary.BigEndian.PutUint32(buf[:frameHeaderLen], uint32(n))
	buf[frameHeaderLen] = byte(f.Type)
	copy(buf[frameHeaderLen+1:], f.Payload)
	return buf
}

type loopingReaderConn struct {
	data []byte
	off  int
}

func (c *loopingReaderConn) Read(p []byte) (int, error) {
	if len(c.data) == 0 {
		return 0, io.EOF
	}
	for i := range p {
		p[i] = c.data[c.off]
		c.off = (c.off + 1) % len(c.data)
	}
	return len(p), nil
}

func (c *loopingReaderConn) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (c *loopingReaderConn) Close() error              { return nil }
func (c *loopingReaderConn) LocalAddr() net.Addr       { return nil }
func (c *loopingReaderConn) RemoteAddr() net.Addr      { return nil }
func (c *loopingReaderConn) SetDeadline(time.Time) error {
	return nil
}
func (c *loopingReaderConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *loopingReaderConn) SetWriteDeadline(time.Time) error {
	return nil
}

type discardConn struct{}

func (discardConn) Read([]byte) (int, error)         { return 0, io.ErrClosedPipe }
func (discardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (discardConn) Close() error                     { return nil }
func (discardConn) LocalAddr() net.Addr              { return nil }
func (discardConn) RemoteAddr() net.Addr             { return nil }
func (discardConn) SetDeadline(time.Time) error      { return nil }
func (discardConn) SetReadDeadline(time.Time) error  { return nil }
func (discardConn) SetWriteDeadline(time.Time) error { return nil }
