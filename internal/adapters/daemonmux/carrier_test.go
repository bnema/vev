package daemonmux

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/streamframe"
	"github.com/bnema/vev/internal/protocol/wire"
)

// errFakeRawClosed is the synthetic raw-carriage error a fakeRawTransport
// returns once Close released its blocked I/O, mirroring the adapter
// prompt-close contract.
var errFakeRawClosed = errors.New("fakeRawTransport: closed")

// fakeRawTransport is one end of a scripted raw framed carriage. RecvBounded
// records every caller limit and refuses a body above it exactly like a real
// streamframe framer, so a bridge test observes the bound the bridge applied.
type fakeRawTransport struct {
	mu        sync.Mutex
	limits    []uint64
	sends     [][]byte
	inbound   chan []byte
	closedCh  chan struct{}
	closeOnce sync.Once
	closes    atomic.Int64
	closeErr  error
	// sendHold, when non-nil, blocks Send until released or the carriage
	// closes, so a test can observe a synchronous send in flight.
	sendHold chan struct{}
}

func newFakeRawTransport() *fakeRawTransport {
	return &fakeRawTransport{
		inbound:  make(chan []byte, 8),
		closedCh: make(chan struct{}),
	}
}

func (f *fakeRawTransport) Send(envelope wire.Envelope) error {
	f.mu.Lock()
	hold := f.sendHold
	f.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-f.closedCh:
			return errFakeRawClosed
		}
	}
	select {
	case <-f.closedCh:
		return errFakeRawClosed
	default:
	}
	f.mu.Lock()
	f.sends = append(f.sends, append([]byte(nil), envelope.Payload...))
	f.mu.Unlock()
	return nil
}

func (f *fakeRawTransport) RecvBounded(limit uint64) (wire.Envelope, error) {
	f.mu.Lock()
	f.limits = append(f.limits, limit)
	f.mu.Unlock()
	select {
	case payload := <-f.inbound:
		if uint64(len(payload)) > limit {
			return wire.Envelope{}, streamframe.ErrTooLarge
		}
		return wire.Envelope{Payload: payload}, nil
	case <-f.closedCh:
		return wire.Envelope{}, errFakeRawClosed
	}
}

func (f *fakeRawTransport) Close() error {
	f.closes.Add(1)
	f.closeOnce.Do(func() { close(f.closedCh) })
	return f.closeErr
}

func (f *fakeRawTransport) recordedLimits() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.limits...)
}

func (f *fakeRawTransport) recordedSends() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.sends...)
}

// carrierTestCeilings is a valid advertisement with the smallest envelope
// ceiling, so the negotiated bound stays clearly above wire.PreambleLimit.
func carrierTestCeilings() MuxCeilings {
	ceilings := DefaultMuxCeilings()
	ceilings.MaxReceiveEnvelopeBytes = MinMuxEnvelopeBytes
	return ceilings
}

// TestFramedCarrierBridgeBoundsPreambleThenNegotiated proves the bridge bounds
// every receive to wire.PreambleLimit before Negotiate and to the negotiated
// envelope ceiling afterwards, refusing an oversized frame at each phase.
func TestFramedCarrierBridgeBoundsPreambleThenNegotiated(t *testing.T) {
	raw := newFakeRawTransport()
	bridge, err := NewPreambleCarrier(raw)
	require.NoError(t, err)
	require.True(t, bridge.Preamble())
	require.Equal(t, uint64(wire.PreambleLimit), bridge.ReceiveLimit())

	small := []byte("preamble-sized request")
	raw.inbound <- small
	got, err := bridge.Receive(context.Background())
	require.NoError(t, err)
	require.Equal(t, small, got)
	require.Equal(t, []uint64{wire.PreambleLimit}, raw.recordedLimits())

	oversize := make([]byte, wire.PreambleLimit+1)
	raw.inbound <- oversize
	_, err = bridge.Receive(context.Background())
	require.ErrorIs(t, err, streamframe.ErrTooLarge)

	ceilings := carrierTestCeilings()
	require.NoError(t, bridge.Negotiate(ceilings))
	require.False(t, bridge.Preamble())
	require.Equal(t, ceilings.MaxReceiveEnvelopeBytes, bridge.ReceiveLimit())

	raw.inbound <- oversize
	got, err = bridge.Receive(context.Background())
	require.NoError(t, err)
	require.Equal(t, oversize, got)
	limits := raw.recordedLimits()
	require.Equal(t, ceilings.MaxReceiveEnvelopeBytes, limits[len(limits)-1])
}

// TestFramedCarrierBridgeNegotiateIsExplicitAndOneWay proves the state
// transition is explicit: an invalid advertisement is refused, a second
// negotiation is refused, and negotiation after Close is refused.
func TestFramedCarrierBridgeNegotiateIsExplicitAndOneWay(t *testing.T) {
	raw := newFakeRawTransport()
	bridge, err := NewPreambleCarrier(raw)
	require.NoError(t, err)

	require.ErrorIs(t, bridge.Negotiate(MuxCeilings{}), ErrInvalidCeilings)

	require.NoError(t, bridge.Negotiate(carrierTestCeilings()))
	require.ErrorIs(t, bridge.Negotiate(carrierTestCeilings()), ErrCarrierState)

	require.NoError(t, bridge.Close())
	require.ErrorIs(t, bridge.Negotiate(carrierTestCeilings()), ErrCarrierClosed)
}

// TestNewPreambleCarrierRefusesNil proves construction validates its raw
// transport.
func TestNewPreambleCarrierRefusesNil(t *testing.T) {
	_, err := NewPreambleCarrier(nil)
	require.ErrorIs(t, err, ErrCarrierState)
}

// TestFramedCarrierBridgeSendIsSynchronousAndExact proves Send returns the raw
// write result: it does not return while the write is held, records the payload
// byte for byte, and propagates the raw error.
func TestFramedCarrierBridgeSendIsSynchronousAndExact(t *testing.T) {
	raw := newFakeRawTransport()
	bridge, err := NewPreambleCarrier(raw)
	require.NoError(t, err)
	defer func() { _ = bridge.Close() }()

	raw.sendHold = make(chan struct{})
	payload := []byte("exact outbound envelope")
	sendDone := make(chan error, 1)
	go func() { sendDone <- bridge.Send(context.Background(), payload) }()

	select {
	case err := <-sendDone:
		t.Fatalf("Send returned %v while the raw write was held", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.Empty(t, raw.recordedSends())

	close(raw.sendHold)
	require.NoError(t, <-sendDone)
	require.Equal(t, [][]byte{payload}, raw.recordedSends())

	raw.closeErr = errors.New("raw close failed")
	require.ErrorIs(t, bridge.Close(), raw.closeErr)
	require.ErrorIs(t, bridge.Send(context.Background(), payload), ErrCarrierClosed)
	_, err = bridge.Receive(context.Background())
	require.ErrorIs(t, err, ErrCarrierClosed)
}

// TestFramedCarrierBridgeReceiveContextCancelReleasesRead proves a cancelled
// receive closes the raw carriage to release the blocked call, returns the
// context error, and leaves no goroutine behind.
func TestFramedCarrierBridgeReceiveContextCancelReleasesRead(t *testing.T) {
	raw := newFakeRawTransport()
	bridge, err := NewPreambleCarrier(raw)
	require.NoError(t, err)

	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	recvDone := make(chan error, 1)
	go func() {
		_, err := bridge.Receive(ctx)
		recvDone <- err
	}()
	require.Eventually(t, func() bool { return len(raw.recordedLimits()) == 1 }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-recvDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Receive did not return after context cancellation")
	}
	require.Equal(t, int64(1), raw.closes.Load(), "cancellation must release the blocked raw read")
	require.NoError(t, bridge.Close())
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+1 }, 5*time.Second, time.Millisecond,
		"the bridge must not abandon its receive worker (before=%d now=%d)", before, runtime.NumGoroutine())
}

// TestFramedCarrierBridgeCloseInterruptsBlockedReceiveAndIsConcurrentSafe
// proves Close unblocks a blocked receive, is idempotent across concurrent
// callers, closes the raw carriage exactly once, and joins its workers.
func TestFramedCarrierBridgeCloseInterruptsBlockedReceiveAndIsConcurrentSafe(t *testing.T) {
	raw := newFakeRawTransport()
	bridge, err := NewPreambleCarrier(raw)
	require.NoError(t, err)

	before := runtime.NumGoroutine()
	recvDone := make(chan error, 1)
	go func() {
		_, err := bridge.Receive(context.Background())
		recvDone <- err
	}()
	require.Eventually(t, func() bool { return len(raw.recordedLimits()) == 1 }, time.Second, time.Millisecond)

	const closers = 8
	var wg sync.WaitGroup
	closeErrs := make([]error, closers)
	for i := range closers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			closeErrs[i] = bridge.Close()
		}()
	}
	wg.Wait()
	for _, err := range closeErrs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), raw.closes.Load(), "Close must close the raw carriage exactly once")

	select {
	case err := <-recvDone:
		require.ErrorIs(t, err, ErrCarrierClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock the receive")
	}
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+1 }, 5*time.Second, time.Millisecond,
		"Close must join every bridge worker (before=%d now=%d)", before, runtime.NumGoroutine())
}

// TestFramedCarrierBridgeOverIPCFramer proves the bridge drives a real vev raw
// framed transport: the shared framing round-trips through the bridge, the
// preamble bound refuses an oversized frame before its body, and Negotiate
// raises the bound for a larger negotiated frame.
func TestFramedCarrierBridgeOverIPCFramer(t *testing.T) {
	t.Run("preamble and negotiated frames round trip", func(t *testing.T) {
		left, right := net.Pipe()
		t.Cleanup(func() {
			_ = left.Close()
			_ = right.Close()
		})
		raw, ok := ipc.NewTransport(left).(RawFramedTransport)
		require.True(t, ok, "the IPC transport must satisfy the bounded raw capability")
		bridge, err := NewPreambleCarrier(raw)
		require.NoError(t, err)
		peer := ipc.NewTransport(right)
		defer func() {
			_ = bridge.Close()
			_ = peer.Close()
		}()

		small := []byte("small preamble frame")
		go func() { _ = peer.Send(wire.Envelope{Payload: small}) }()
		got, err := bridge.Receive(context.Background())
		require.NoError(t, err)
		require.Equal(t, small, got)

		require.NoError(t, bridge.Negotiate(carrierTestCeilings()))
		large := make([]byte, wire.PreambleLimit+4096)
		for i := range large {
			large[i] = byte('a' + i%26)
		}
		go func() { _ = peer.Send(wire.Envelope{Payload: large}) }()
		got, err = bridge.Receive(context.Background())
		require.NoError(t, err)
		require.Equal(t, large, got)

		outbound := []byte("exact outbound through the real framer")
		type peerRecv struct {
			envelope wire.Envelope
			err      error
		}
		peerResult := make(chan peerRecv, 1)
		go func() {
			envelope, err := peer.Recv()
			peerResult <- peerRecv{envelope: envelope, err: err}
		}()
		require.NoError(t, bridge.Send(context.Background(), outbound))
		result := <-peerResult
		require.NoError(t, result.err)
		require.Equal(t, outbound, result.envelope.Payload)
	})

	t.Run("oversized preamble frame is refused before its body", func(t *testing.T) {
		left, right := net.Pipe()
		t.Cleanup(func() {
			_ = left.Close()
			_ = right.Close()
		})
		raw, ok := ipc.NewTransport(left).(RawFramedTransport)
		require.True(t, ok)
		bridge, err := NewPreambleCarrier(raw)
		require.NoError(t, err)
		defer func() { _ = bridge.Close() }()

		// Advertise a 1 MiB frame and never send its body: only the four-byte
		// length prefix exists on the wire.
		go func() { _, _ = right.Write(binary.BigEndian.AppendUint32(nil, 1<<20)) }()
		_, err = bridge.Receive(context.Background())
		require.ErrorIs(t, err, streamframe.ErrTooLarge)
	})
}
