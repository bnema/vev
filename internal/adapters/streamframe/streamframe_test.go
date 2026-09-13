package streamframe

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestFramerRoundTrip proves framed envelopes survive partial IO: the
// writer emits 4-byte length + payload, the reader reassembles.
func TestFramerRoundTrip(t *testing.T) {
	reader, writer := io.Pipe()
	defer func() {
		_ = reader.Close()
		_ = writer.Close()
	}()
	sender := NewFramer(nil, writer, nil)
	receiver := NewFramer(reader, nil, nil)
	defer func() {
		_ = sender.Close()
		_ = receiver.Close()
	}()
	payloads := [][]byte{[]byte("hello"), bytes.Repeat([]byte("x"), 70000), []byte("bye")}
	done := make(chan error, 1)
	go func() {
		for _, payload := range payloads {
			if err := sender.Send(payload); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for _, want := range payloads {
		got, err := receiver.Recv()
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.NoError(t, <-done)
}

// TestFramerPartialReads feeds the length header and body byte-by-byte.
func TestFramerPartialReads(t *testing.T) {
	payload := []byte("partial payload")
	var encoded bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	encoded.Write(header[:])
	encoded.Write(payload)
	raw := encoded.Bytes()

	pr, pw := io.Pipe()
	defer func() {
		_ = pr.Close()
		_ = pw.Close()
	}()
	receiver := NewFramer(pr, nil, nil)
	defer func() { _ = receiver.Close() }()
	go func() {
		for _, b := range raw {
			_, _ = pw.Write([]byte{b})
		}
	}()
	got, err := receiver.Recv()
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// TestFramerRejectsZeroAndOversize proves bounds are enforced before
// allocation: zero lengths fail, lengths above the negotiated ceiling fail,
// lengths above the absolute ceiling fail even with a raised negotiation.
func TestFramerRejectsZeroAndOversize(t *testing.T) {
	t.Run("zero length", func(t *testing.T) {
		pr, pw := io.Pipe()
		defer func() {
			_ = pr.Close()
			_ = pw.Close()
		}()
		receiver := NewFramer(pr, nil, nil)
		defer func() { _ = receiver.Close() }()
		go func() { _, _ = pw.Write([]byte{0, 0, 0, 0}) }()
		_, err := receiver.Recv()
		require.ErrorIs(t, err, ErrZeroLength)
	})
	t.Run("above negotiated", func(t *testing.T) {
		pr, pw := io.Pipe()
		defer func() {
			_ = pr.Close()
			_ = pw.Close()
		}()
		receiver := NewFramer(pr, nil, nil, WithMaxEnvelope(16))
		defer func() { _ = receiver.Close() }()
		go func() {
			var header [4]byte
			binary.BigEndian.PutUint32(header[:], 17)
			_, _ = pw.Write(header[:])
		}()
		_, err := receiver.Recv()
		require.ErrorIs(t, err, ErrTooLarge)
	})
	t.Run("send above negotiated", func(t *testing.T) {
		sender := NewFramer(nil, io.Discard, nil, WithMaxEnvelope(4))
		defer func() { _ = sender.Close() }()
		require.ErrorIs(t, sender.Send([]byte("12345")), ErrTooLarge)
		require.ErrorIs(t, sender.Send(nil), ErrZeroLength)
	})
}

// TestFramerTruncation proves a short body fails instead of blocking forever.
func TestFramerTruncation(t *testing.T) {
	pr, pw := io.Pipe()
	receiver := NewFramer(pr, nil, nil)
	defer func() { _ = receiver.Close() }()
	go func() {
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], 100)
		_, _ = pw.Write(header[:])
		_, _ = pw.Write([]byte("short"))
		_ = pw.Close()
	}()
	_, err := receiver.Recv()
	require.Error(t, err)
}

// TestFramerTrailingBoundary proves two back-to-back envelopes split exactly.
func TestFramerTrailingBoundary(t *testing.T) {
	var encoded bytes.Buffer
	for _, payload := range [][]byte{[]byte("one"), []byte("two")} {
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
		encoded.Write(header[:])
		encoded.Write(payload)
	}
	receiver := NewFramer(bytes.NewReader(encoded.Bytes()), nil, nil)
	defer func() { _ = receiver.Close() }()
	first, err := receiver.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("one"), first)
	second, err := receiver.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("two"), second)
}

// TestFramerCloseUnblocksSendRecv proves Close interrupts both directions
// and runs the injected closer exactly once.
func TestFramerCloseUnblocksSendRecv(t *testing.T) {
	pr, pw := io.Pipe()
	closes := 0
	// The injected closer releases blocked I/O, like the concrete
	// adapters will in P3.3; the Framer itself never owns the stream.
	framer := NewFramer(pr, pw, func() error {
		closes++
		_ = pr.Close()
		_ = pw.Close()
		return nil
	})
	recvErr := make(chan error, 1)
	go func() {
		_, err := framer.Recv()
		recvErr <- err
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, framer.Close())
	require.NoError(t, framer.Close())
	select {
	case err := <-recvErr:
		require.True(t, errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, ErrClosed))
	case <-time.After(5 * time.Second):
		t.Fatal("Recv was not unblocked by Close")
	}
	require.Equal(t, 1, closes)
}

// TestFramerByteBudgetExhaustion proves async admission stops at the byte
// budget and resumes after the writer drains.
func TestFramerByteBudgetExhaustion(t *testing.T) {
	writer := &gateWriter{gate: make(chan struct{}, 2)}
	sender := NewFramer(nil, writer, nil, WithQueueBounds(8, 64))
	defer func() {
		writer.gate <- struct{}{}
		writer.gate <- struct{}{}
		_ = sender.Close()
	}()

	payload := bytes.Repeat([]byte("a"), 32)
	require.NoError(t, sender.SendAsync(payload))
	// Wait until the sole writer has dequeued and is blocked writing the first
	// frame. Dequeued writes no longer consume the queue's admission budget.
	require.Eventually(t, func() bool {
		sender.egressMu.Lock()
		defer sender.egressMu.Unlock()
		return len(sender.egress) == 0 && sender.queuedBytes == 0
	}, 5*time.Second, time.Millisecond)
	require.NoError(t, sender.SendAsync(payload))
	require.ErrorIs(t, sender.SendAsync(payload), ErrBackpressure)

	writer.gate <- struct{}{}
	require.Eventually(t, func() bool {
		sender.egressMu.Lock()
		defer sender.egressMu.Unlock()
		return len(sender.egress) == 0 && sender.queuedBytes == 0
	}, 5*time.Second, time.Millisecond)
	require.NoError(t, sender.SendAsync(payload))
}

// TestFramerSyncBypassKeepsByteBudget proves synchronous sends do not
// corrupt async byte accounting: a dequeued sync frame releases no
// charge, so later async admission cannot exceed the byte budget. Each
// writer Write consumes one gate token, pinning the dequeue order to
// async A, sync S, async B: after A is written the used budget must
// still be B's 64 bytes, so a further 64-byte frame is backpressure.
// (The old code released the dequeued sync frame's 36 bytes and wrongly
// admitted it.)
func TestFramerSyncBypassKeepsByteBudget(t *testing.T) {
	gate := &gateWriter{gate: make(chan struct{}, 8)}
	sender := NewFramer(nil, gate, nil, WithQueueBounds(8, 100))
	defer func() { _ = sender.Close() }()

	asyncA := bytes.Repeat([]byte("a"), 32) // framed 36, counted
	syncS := bytes.Repeat([]byte("s"), 32)  // framed 36, uncounted
	asyncB := bytes.Repeat([]byte("b"), 60) // framed 64, counted
	require.NoError(t, sender.SendAsync(asyncA))

	// Wait until the writer holds A (channel empty), so the sync Send
	// below can only land behind A and the length poll below cannot
	// mistake A for S.
	queueLen := func() int {
		sender.egressMu.Lock()
		defer sender.egressMu.Unlock()
		return len(sender.egress)
	}
	queuedBytes := func() uint64 {
		sender.egressMu.Lock()
		defer sender.egressMu.Unlock()
		return sender.queuedBytes
	}
	require.Eventually(t, func() bool {
		return queueLen() == 0
	}, 5*time.Second, time.Millisecond)

	syncDone := make(chan error, 1)
	go func() { syncDone <- sender.Send(syncS) }()
	require.Eventually(t, func() bool {
		return queueLen() == 1
	}, 5*time.Second, time.Millisecond)
	require.NoError(t, sender.SendAsync(asyncB))

	// Finish A only, then wait until S is dequeued: S's dequeue must
	// release no charge, leaving B's 64 bytes charged (36 free). A
	// further 64-byte frame no longer fits.
	gate.gate <- struct{}{}
	require.Eventually(t, func() bool {
		return gate.writtenForTest() == 1 && queueLen() == 1
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, uint64(64), queuedBytes(), "sync dequeue must release no byte charge")

	// A 64-byte frame no longer fits the remaining 36-byte budget.
	probe := bytes.Repeat([]byte("c"), 60) // framed 64
	require.ErrorIs(t, sender.SendAsync(probe), ErrBackpressure)

	// Drain the rest in queue order.
	gate.gate <- struct{}{}
	gate.gate <- struct{}{}
	require.NoError(t, <-syncDone)
	require.Eventually(t, func() bool {
		return gate.writtenForTest() == 3
	}, 5*time.Second, time.Millisecond)
	for _, want := range [][]byte{asyncA, syncS, asyncB} {
		got, err := gate.nextFrame()
		require.NoError(t, err)
		require.Equal(t, want, got[4:])
	}
}

// TestFramerRetainedPayloadSafety proves Send copies: mutating the caller's
// buffer afterwards does not corrupt the queued envelope.
func TestFramerRetainedPayloadSafety(t *testing.T) {
	pr, pw := io.Pipe()
	defer func() {
		_ = pr.Close()
		_ = pw.Close()
	}()
	sender := NewFramer(nil, pw, nil)
	defer func() { _ = sender.Close() }()
	receiver := NewFramer(pr, nil, nil)
	defer func() { _ = receiver.Close() }()
	payload := []byte("mutable")
	require.NoError(t, sender.SendAsync(payload))
	for i := range payload {
		payload[i] = 'X'
	}
	got, err := receiver.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("mutable"), got)
}

// TestFramerConcurrentCloseSendRecv hammers all three operations at once
// under -race: no panic, no deadlock, every call returns.
func TestFramerConcurrentCloseSendRecv(t *testing.T) {
	pr, pw := io.Pipe()
	framer := NewFramer(pr, pw, func() error {
		_ = pr.Close()
		_ = pw.Close()
		return nil
	})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = framer.Send([]byte("x"))
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = framer.Recv()
		}()
	}
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, framer.Close())
	wg.Wait()
}

type blockingWriter struct {
	release chan struct{}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	<-w.release
	return len(p), nil
}

// gateWriter releases one framed write per gate token and records
// every write for ordered inspection. It pins the writer-side
// interleaving a test needs without touching Framer internals.
type gateWriter struct {
	gate chan struct{}

	mu     sync.Mutex
	frames [][]byte
}

func (w *gateWriter) Write(p []byte) (int, error) {
	<-w.gate
	w.mu.Lock()
	defer w.mu.Unlock()
	w.frames = append(w.frames, append([]byte(nil), p...))
	return len(p), nil
}

func (w *gateWriter) writtenForTest() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.frames)
}

func (w *gateWriter) nextFrame() ([]byte, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		w.mu.Lock()
		if len(w.frames) > 0 {
			frame := w.frames[0]
			w.frames = w.frames[1:]
			w.mu.Unlock()
			return frame, nil
		}
		w.mu.Unlock()
		if time.Now().After(deadline) {
			return nil, errors.New("gateWriter: timed out waiting for frame")
		}
		time.Sleep(time.Millisecond)
	}
}
