package ipc

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestTransportSendRecvBothDirections(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	client := NewTransport(c1)
	server := NewTransport(c2)

	clientToServer := ports.Frame{Type: ports.MsgHello, Payload: MarshalHello(Hello{Version: 1, Intent: IntentNew, Name: "w0"})}
	serverToClient := ports.Frame{Type: ports.MsgWelcome, Payload: MarshalWelcome(Welcome{SessionID: "s1", SessionName: "main"})}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := client.Send(clientToServer); err != nil {
			t.Errorf("client.Send() error = %v", err)
		}
	}()

	got, err := server.Recv()
	wg.Wait() // ensure the sender goroutine has fully returned before any Fatalf
	if err != nil {
		t.Fatalf("server.Recv() error = %v", err)
	}
	if !reflect.DeepEqual(got, clientToServer) {
		t.Fatalf("server.Recv() = %#v, want %#v", got, clientToServer)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.Send(serverToClient); err != nil {
			t.Errorf("server.Send() error = %v", err)
		}
	}()

	got, err = client.Recv()
	wg.Wait()
	if err != nil {
		t.Fatalf("client.Recv() error = %v", err)
	}
	if !reflect.DeepEqual(got, serverToClient) {
		t.Fatalf("client.Recv() = %#v, want %#v", got, serverToClient)
	}
}

func TestTransportEOFOnClose(t *testing.T) {
	c1, c2 := net.Pipe()
	server := NewTransport(c2)

	if err := c1.Close(); err != nil {
		t.Fatalf("c1.Close() error = %v", err)
	}

	_, err := server.Recv()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("server.Recv() error = %v, want io.EOF", err)
	}
}

func TestTransportCloseClosesUnderlyingConn(t *testing.T) {
	c1, c2 := net.Pipe()
	client := NewTransport(c1)
	defer c2.Close()

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Writing to the closed end should now fail.
	if _, err := c1.Write([]byte("x")); err == nil {
		t.Fatalf("expected write on closed conn to fail")
	}
}

func TestTransportLargeFrameNearCap(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	client := NewTransport(c1)
	server := NewTransport(c2)

	// Largest payload whose frame length (1 + len(payload)) still fits
	// exactly at maxFrameLen.
	payload := make([]byte, maxFrameLen-1)
	for i := range payload {
		payload[i] = byte(i)
	}
	want := ports.Frame{Type: ports.MsgOutput, Payload: payload}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := client.Send(want); err != nil {
			t.Errorf("client.Send() error = %v", err)
		}
	}()

	got, err := server.Recv()
	wg.Wait() // ensure the sender goroutine has fully returned before any Fatalf
	if err != nil {
		t.Fatalf("server.Recv() error = %v", err)
	}
	if got.Type != want.Type || len(got.Payload) != len(want.Payload) {
		t.Fatalf("server.Recv() = type %v len %d, want type %v len %d", got.Type, len(got.Payload), want.Type, len(want.Payload))
	}
	for i := range got.Payload {
		if got.Payload[i] != want.Payload[i] {
			t.Fatalf("payload mismatch at byte %d: got %d want %d", i, got.Payload[i], want.Payload[i])
		}
	}
}

func TestTransportManySmallFramesBackToBack(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	client := NewTransport(c1)
	server := NewTransport(c2)

	const count = 200
	frames := make([]ports.Frame, count)
	for i := range frames {
		frames[i] = ports.Frame{Type: ports.MsgPing, Payload: nil}
	}
	// Vary a couple to prove ordering and content are both preserved.
	frames[7] = ports.Frame{Type: ports.MsgInput, Payload: MarshalInput(Input{Data: []byte("hop")})}
	frames[150] = ports.Frame{Type: ports.MsgResize, Payload: MarshalResize(Resize{Size: domain.Size{Cols: 5, Rows: 6}})}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, f := range frames {
			if err := client.Send(f); err != nil {
				t.Errorf("client.Send() error = %v", err)
				return
			}
		}
	}()

	// Report mismatches with Errorf (not Fatalf) and keep draining: the
	// sender goroutine's net.Pipe writes block until read, so bailing out
	// early here would deadlock (or leak) the sender.
	for i, want := range frames {
		got, err := server.Recv()
		if err != nil {
			t.Errorf("server.Recv() frame %d error = %v", i, err)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("server.Recv() frame %d = %#v, want %#v", i, got, want)
		}
	}
	wg.Wait()
}

func TestTransportRecvOversizeFrame(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	server := NewTransport(c2)

	go func() {
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], maxFrameLen+1)
		_, _ = c1.Write(hdr[:])
	}()

	_, err := server.Recv()
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("server.Recv() error = %v, want ErrFrameTooLarge", err)
	}
}

func TestTransportRecvZeroLengthFrame(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	server := NewTransport(c2)

	go func() {
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], 0)
		_, _ = c1.Write(hdr[:])
	}()

	_, err := server.Recv()
	if !errors.Is(err, ErrZeroLengthFrame) {
		t.Fatalf("server.Recv() error = %v, want ErrZeroLengthFrame", err)
	}
}

func TestTransportRecvTruncatedHeader(t *testing.T) {
	c1, c2 := net.Pipe()
	server := NewTransport(c2)

	go func() {
		_, _ = c1.Write([]byte{0x00, 0x01}) // only 2 of 4 header bytes
		_ = c1.Close()
	}()

	_, err := server.Recv()
	if err == nil {
		t.Fatalf("server.Recv() expected error on truncated header, got nil")
	}
}

func TestTransportSendOversizePayloadRejected(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	client := NewTransport(c1)
	payload := make([]byte, maxFrameLen) // +1 byte type => exceeds max
	err := client.Send(ports.Frame{Type: ports.MsgOutput, Payload: payload})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("client.Send() error = %v, want ErrFrameTooLarge", err)
	}
}
