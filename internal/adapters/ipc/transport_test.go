package ipc

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestTransportSendRecvBothDirections(t *testing.T) {
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

	client := NewTransport(c1)
	server := NewTransport(c2)

	clientToServer := ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(ports.Hello{Version: 1, Intent: ports.IntentNew, Name: "w0"})}
	serverToClient := ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{SessionID: "s1", SessionName: "main"})}

	var wg sync.WaitGroup
	wg.Go(func() {
		if err := client.Send(clientToServer); err != nil {
			t.Errorf("client.Send() error = %v", err)
		}
	})

	got, err := server.Recv()
	wg.Wait() // ensure the sender goroutine has fully returned before any Fatalf
	if err != nil {
		t.Fatalf("server.Recv() error = %v", err)
	}
	if !reflect.DeepEqual(got, clientToServer) {
		t.Fatalf("server.Recv() = %#v, want %#v", got, clientToServer)
	}

	wg.Go(func() {
		if err := server.Send(serverToClient); err != nil {
			t.Errorf("server.Send() error = %v", err)
		}
	})

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
	defer func() { _ = c2.Close() }()

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
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

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
	wg.Go(func() {
		if err := client.Send(want); err != nil {
			t.Errorf("client.Send() error = %v", err)
		}
	})

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
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

	client := NewTransport(c1)
	server := NewTransport(c2)

	const count = 200
	frames := make([]ports.Frame, count)
	for i := range frames {
		frames[i] = ports.Frame{Type: ports.MsgPing, Payload: nil}
	}
	// Vary a couple to prove ordering and content are both preserved.
	frames[7] = ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: []byte("hop")})}
	frames[150] = ports.Frame{Type: ports.MsgResize, Payload: ports.MarshalResize(ports.Resize{Size: domain.Size{Cols: 5, Rows: 6}})}

	var wg sync.WaitGroup
	wg.Go(func() {
		for _, f := range frames {
			if err := client.Send(f); err != nil {
				t.Errorf("client.Send() error = %v", err)
				return
			}
		}
	})

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
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

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
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

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
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

	client := NewTransport(c1)
	payload := make([]byte, maxFrameLen) // +1 byte type => exceeds max
	err := client.Send(ports.Frame{Type: ports.MsgOutput, Payload: payload})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("client.Send() error = %v, want ErrFrameTooLarge", err)
	}
}

func TestTransportAsyncEgressPreservesWelcomeBeforeOutput(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()

	server := NewTransport(serverConn)
	client := NewTransport(clientConn)
	async, ok := server.(ports.AsyncTransport)
	if !ok {
		t.Fatal("IPC transport does not implement AsyncTransport")
	}

	welcome := ports.Frame{Type: ports.MsgWelcome, Payload: []byte("welcome")}
	sent := make(chan error, 1)
	go func() { sent <- server.Send(welcome) }()
	got, err := client.Recv()
	if err != nil {
		t.Fatalf("Recv welcome: %v", err)
	}
	if !reflect.DeepEqual(got, welcome) {
		t.Fatalf("welcome = %#v, want %#v", got, welcome)
	}
	if err := <-sent; err != nil {
		t.Fatalf("Send welcome: %v", err)
	}

	payload := []byte("first")
	if err := async.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: payload}); err != nil {
		t.Fatalf("SendAsync first output: %v", err)
	}
	payload[0] = 'X' // async ownership must not retain caller memory.
	if err := async.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte("second")}); err != nil {
		t.Fatalf("SendAsync second output: %v", err)
	}
	for _, want := range []ports.Frame{
		{Type: ports.MsgOutput, Payload: []byte("first")},
		{Type: ports.MsgOutput, Payload: []byte("second")},
	} {
		got, err := client.Recv()
		if err != nil {
			t.Fatalf("Recv output: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("output = %#v, want %#v", got, want)
		}
	}
}

func TestTransportAsyncEgressIsBoundedAndCloseInterruptsWorkers(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	tr := NewTransport(serverConn)
	async, ok := tr.(ports.AsyncTransport)
	if !ok {
		t.Fatal("IPC transport does not implement AsyncTransport")
	}

	// The first write blocks in net.Pipe because the peer never drains it.
	if err := async.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte("blocked")}); err != nil {
		t.Fatalf("SendAsync blocked frame: %v", err)
	}
	var backpressure error
	for range sendQueueCapacity + 2 {
		err := async.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte("queued")})
		if errors.Is(err, ErrBackpressure) {
			backpressure = err
			break
		}
		if err != nil {
			t.Fatalf("SendAsync unexpected error: %v", err)
		}
	}
	if !errors.Is(backpressure, ErrBackpressure) {
		t.Fatalf("SendAsync never reported bounded backpressure, got %v", backpressure)
	}

	recvDone := make(chan error, 1)
	go func() {
		_, err := tr.Recv()
		recvDone <- err
	}()
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-recvDone:
		if err == nil {
			t.Fatal("Recv succeeded after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt Recv")
	}
	select {
	case <-tr.(*unixTransport).writerDone:
	default:
		t.Fatal("Close returned before the IPC writer terminated")
	}
}
