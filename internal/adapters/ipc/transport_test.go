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
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func TestTransportSendRecvBothDirections(t *testing.T) {
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()

	client := NewTransport(c1)
	server := NewTransport(c2)

	clientToServer := wire.Envelope{Payload: mustEncodeClient(protocol.Hello{Version: 1, Intent: protocol.IntentNew, Name: "w0"})}
	serverToClient := wire.Envelope{Payload: mustEncodeServer(protocol.Welcome{SessionID: "s1", SessionName: "main"})}

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

	// Largest payload accepted: the canonical envelope ceiling admits a
	// payload of exactly wire.AbsoluteEnvelopeLimit, so one byte below stays near cap.
	payload := make([]byte, wire.AbsoluteEnvelopeLimit-1)
	for i := range payload {
		payload[i] = byte(i)
	}
	want := wire.Envelope{Payload: payload}

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
	if len(got.Payload) != len(want.Payload) {
		t.Fatalf("server.Recv() = len %d, want len %d", len(got.Payload), len(want.Payload))
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
	frames := make([]wire.Envelope, count)
	for i := range frames {
		frames[i] = wire.Envelope{Payload: []byte("ping")}
	}
	// Vary a couple to prove ordering and content are both preserved.
	frames[7] = wire.Envelope{Payload: mustEncodeClient(protocol.Input{Data: []byte("hop")})}
	frames[150] = wire.Envelope{Payload: mustEncodeClient(protocol.Resize{Size: domain.Size{Cols: 5, Rows: 6}})}

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
		binary.BigEndian.PutUint32(hdr[:], wire.AbsoluteEnvelopeLimit+1)
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
	payload := make([]byte, wire.AbsoluteEnvelopeLimit+1) // one byte past the canonical ceiling
	err := client.Send(wire.Envelope{Payload: payload})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("client.Send() error = %v, want ErrFrameTooLarge", err)
	}
}

func TestTransportAsyncEgressPreservesWelcomeBeforeOutput(t *testing.T) {
	const timeout = time.Second

	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()
	if err := serverConn.SetDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("SetDeadline server connection: %v", err)
	}
	if err := clientConn.SetDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("SetDeadline client connection: %v", err)
	}

	server := NewTransport(serverConn)
	defer closeTransport(t, server)
	client := NewTransport(clientConn)
	defer closeTransport(t, client)
	async, ok := server.(wire.AsyncTransport)
	if !ok {
		t.Fatal("IPC transport does not implement AsyncTransport")
	}

	welcome := wire.Envelope{Payload: []byte("welcome")}
	sent := make(chan error, 1)
	go func() { sent <- server.Send(welcome) }()
	got, err := client.Recv()
	if err != nil {
		t.Fatalf("Recv welcome: %v", err)
	}
	if !reflect.DeepEqual(got, welcome) {
		t.Fatalf("welcome = %#v, want %#v", got, welcome)
	}
	select {
	case err := <-sent:
		if err != nil {
			t.Fatalf("Send welcome: %v", err)
		}
	case <-time.After(timeout):
		t.Fatal("Send welcome did not return")
	}

	payload := []byte("first")
	if err := async.SendAsync(wire.Envelope{Payload: payload}); err != nil {
		t.Fatalf("SendAsync first output: %v", err)
	}
	payload[0] = 'X' // async ownership must not retain caller memory.
	if err := async.SendAsync(wire.Envelope{Payload: []byte("second")}); err != nil {
		t.Fatalf("SendAsync second output: %v", err)
	}
	for _, want := range []wire.Envelope{
		{Payload: []byte("first")},
		{Payload: []byte("second")},
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

func TestTransportSendWaitsForEgressCapacity(t *testing.T) {
	const timeout = time.Second

	serverConn, clientConn := net.Pipe()
	defer func() { _ = serverConn.Close() }()
	defer func() { _ = clientConn.Close() }()
	if err := clientConn.SetDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("SetDeadline client connection: %v", err)
	}

	server := NewTransport(serverConn)
	defer closeTransport(t, server)
	client := NewTransport(clientConn)
	defer closeTransport(t, client)
	async, ok := server.(wire.AsyncTransport)
	if !ok {
		t.Fatal("IPC transport does not implement AsyncTransport")
	}

	queued := fillAsyncEgress(t, async, timeout)
	want := wire.Envelope{Payload: []byte("synchronous")}
	sent := make(chan error, 1)
	go func() { sent <- server.Send(want) }()
	// Send must park behind the full egress queue; give the goroutine a
	// moment to reach the queue, then prove it has not returned.
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-sent:
		t.Fatalf("Send returned before capacity was available: %v", err)
	default:
	}

	for _, want := range append(queued, want) {
		got, err := client.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Recv = %#v, want %#v", got, want)
		}
	}
	select {
	case err := <-sent:
		if err != nil {
			t.Fatalf("Send after draining: %v", err)
		}
	case <-time.After(timeout):
		t.Fatal("Send did not return after draining")
	}
}

func TestTransportCloseInterruptsSendWaitingForEgressCapacity(t *testing.T) {
	const timeout = time.Second

	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	server := NewTransport(serverConn)
	defer closeTransport(t, server)
	async, ok := server.(wire.AsyncTransport)
	if !ok {
		t.Fatal("IPC transport does not implement AsyncTransport")
	}
	fillAsyncEgress(t, async, timeout)

	sent := make(chan error, 1)
	go func() { sent <- server.Send(wire.Envelope{Payload: []byte("synchronous")}) }()
	time.Sleep(20 * time.Millisecond)

	closeTransport(t, server)
	select {
	case err := <-sent:
		if !errors.Is(err, errClosed) {
			t.Fatalf("Send after Close = %v, want errClosed", err)
		}
	case <-time.After(timeout):
		t.Fatal("Close did not interrupt Send")
	}
}

func closeTransport(t *testing.T, transport wire.Transport) {
	t.Helper()

	closed := make(chan error, 1)
	go func() { closed <- transport.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return")
	}
}

// fillAsyncEgress admits envelopes until the bounded egress queue reports
// backpressure, returning every admitted envelope in transmission order.
func fillAsyncEgress(t *testing.T, async wire.AsyncTransport, timeout time.Duration) []wire.Envelope {
	t.Helper()

	deadline := time.Now().Add(timeout)
	queued := make([]wire.Envelope, 0, 16)
	for i := 0; ; i++ {
		envelope := wire.Envelope{Payload: []byte{byte(i)}}
		err := async.SendAsync(envelope)
		switch {
		case err == nil:
			queued = append(queued, envelope)
		case errors.Is(err, ErrBackpressure):
			if len(queued) == 0 {
				t.Fatal("SendAsync reported backpressure before admitting any envelope")
			}
			return queued
		default:
			t.Fatalf("SendAsync queued envelope: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("SendAsync never reported bounded backpressure")
		}
	}
}

func TestTransportAsyncEgressIsBoundedAndCloseInterruptsWorkers(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	tr := NewTransport(serverConn)
	defer closeTransport(t, tr)
	async, ok := tr.(wire.AsyncTransport)
	if !ok {
		t.Fatal("IPC transport does not implement AsyncTransport")
	}

	// The first write blocks in net.Pipe because the peer never drains it.
	if err := async.SendAsync(wire.Envelope{Payload: []byte("blocked")}); err != nil {
		t.Fatalf("SendAsync blocked frame: %v", err)
	}
	var backpressure error
	for i := 0; i < 64; i++ {
		err := async.SendAsync(wire.Envelope{Payload: []byte("queued")})
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
	closeTransport(t, tr)
	select {
	case err := <-recvDone:
		if err == nil {
			t.Fatal("Recv succeeded after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt Recv")
	}
	// Close waits for the sole writer to stop, so admission is closed for good.
	if err := async.SendAsync(wire.Envelope{Payload: []byte("after close")}); !errors.Is(err, errClosed) {
		t.Fatalf("SendAsync after Close = %v, want errClosed", err)
	}
}
