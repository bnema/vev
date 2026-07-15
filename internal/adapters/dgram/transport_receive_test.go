package dgram

import (
	"bytes"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	pdgram "github.com/bnema/vev/pkg/dgram"
)

func TestReliableDuplicateAckedButNotDeliveredTwice(t *testing.T) {
	aPC, bPC := newPair()
	var droppedAck atomic.Bool
	bPC.drop = func(_ []byte, addr net.Addr) bool {
		return addr.String() == "a" && droppedAck.CompareAndSwap(false, true)
	}
	a, err := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := a.Close(); err != nil {
			t.Errorf("close a: %v", err)
		}
	}()
	defer func() {
		if err := b.Close(); err != nil {
			t.Errorf("close b: %v", err)
		}
	}()
	if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("typed")}); err != nil {
		t.Fatal(err)
	}
	got := recvWithin(t, b, time.Second)
	if got.Type != ports.MsgInput || string(got.Payload) != "typed" {
		t.Fatalf("got=%+v", got)
	}
	eventually(t, time.Second, droppedAck.Load)
	if got, ok := recvMaybe(b, 3*defaultResend); ok {
		t.Fatalf("duplicate reliable frame delivered: %+v", got)
	}
}

func TestAuthenticatedPeerChangeUpdatesPeerAndEmitsConnected(t *testing.T) {
	aPC, bPC := newPair()
	a, err := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := a.Close(); err != nil {
			t.Errorf("close a: %v", err)
		}
	}()
	evilPC := &fakePC{addr: testAddr("evil"), in: make(chan packet, 100), peers: map[string]*fakePC{}}
	aPC.peers["evil"] = evilPC
	_ = bPC
	old := a.Peer().String()
	a.setLinkState(ports.LinkStateOffline, nil)
	<-a.LinkEvents()
	aPC.in <- packet{[]byte("not authenticated"), testAddr("evil")}
	if a.Peer().String() != old {
		t.Fatalf("unauthenticated packet changed peer to %v", a.Peer())
	}
	c, _ := pdgram.NewCodec(key())
	rec := encodeData(7, true, ports.Frame{Type: ports.MsgPing})
	frags, _ := pdgram.FragmentPayload(9, rec, pdgram.DefaultMTU)
	raw, _ := pdgram.MarshalFragment(frags[0])
	aPC.in <- packet{c.Seal(2, 99, raw, nil), testAddr("evil")}
	waitPeer(t, a, "evil")
	select {
	case ev := <-a.LinkEvents():
		if ev.State != ports.LinkStateConnected {
			t.Fatalf("event state=%v, want connected", ev.State)
		}
	case <-time.After(time.Second):
		t.Fatal("missing connected event after authenticated rehome")
	}
	select {
	case pkt := <-evilPC.in:
		if pkt.addr.String() != "a" {
			t.Fatalf("ack source=%v, want a", pkt.addr)
		}
	case <-time.After(time.Second):
		t.Fatal("missing ACK to authenticated new peer")
	}
}

func TestAuthenticatedFragmentSeparatesContactProgressAndPeerEligibility(t *testing.T) {
	aPC, bPC := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	defer func() { _ = bPC.Close() }()

	a.setLinkState(ports.LinkStateDegraded, nil)
	<-a.LinkEvents()
	codec, err := pdgram.NewCodec(key())
	if err != nil {
		t.Fatal(err)
	}
	authenticated := make(chan struct{}, 8)
	malformed := make(chan struct{}, 8)
	a.mu.Lock()
	a.afterAuthenticatedPacket = func() { authenticated <- struct{}{} }
	a.afterMalformedFragment = func() { malformed <- struct{}{} }
	initialPacket := a.health.lastPacket
	initialRecord := a.health.lastRecord
	a.mu.Unlock()

	sealFragment := func(counter uint64, frag pdgram.Fragment) []byte {
		t.Helper()
		raw, marshalErr := pdgram.MarshalFragment(frag)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return codec.Seal(2, counter, raw, nil)
	}
	waitSignal := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for %s", what)
		}
	}
	assertHealth := func(wantPacket, wantRecord time.Time, wantPeer string, wantState ports.LinkState) {
		t.Helper()
		a.mu.Lock()
		gotPacket := a.health.lastPacket
		gotRecord := a.health.lastRecord
		a.mu.Unlock()
		if !gotPacket.Equal(wantPacket) || !gotRecord.Equal(wantRecord) {
			t.Fatalf("health packet/record=(%v,%v), want (%v,%v)", gotPacket, gotRecord, wantPacket, wantRecord)
		}
		if got := a.Peer().String(); got != wantPeer {
			t.Fatalf("peer=%s, want %s", got, wantPeer)
		}
		if got := a.LinkState(); got != wantState {
			t.Fatalf("state=%v, want %v", got, wantState)
		}
	}

	// Neither failed authentication nor an authenticated but invalid fragment
	// crosses the contact or roaming trust boundary.
	badAEAD := codec.Seal(2, 1, []byte("not a fragment"), nil)
	badAEAD[len(badAEAD)-1] ^= 0xff
	aPC.in <- packet{b: badAEAD, addr: testAddr("roam1")}
	aPC.in <- packet{b: codec.Seal(2, 2, []byte("not a fragment"), nil), addr: testAddr("roam1")}
	waitSignal(malformed, "malformed fragment")
	assertHealth(initialPacket, initialRecord, "b", ports.LinkStateDegraded)

	clk.advance(time.Second)
	partial := pdgram.Fragment{Seq: 100, Index: 0, Count: 2, Data: []byte("half")}
	acceptedPacket := clk.Now()
	acceptedRaw := sealFragment(10, partial)
	aPC.in <- packet{b: acceptedRaw, addr: testAddr("roam1")}
	waitSignal(authenticated, "accepted incomplete fragment")
	assertHealth(acceptedPacket, initialRecord, "roam1", ports.LinkStateDegraded)

	// A replay and an Add-time fragment inconsistency cannot refresh contact or
	// migrate the peer. Reassembly diagnostics still update on the Add error.
	aPC.in <- packet{b: acceptedRaw, addr: testAddr("roam2")}
	aPC.in <- packet{b: codec.Seal(2, 11, []byte("still not a fragment"), nil), addr: testAddr("roam2")}
	waitSignal(malformed, "replay barrier")
	assertHealth(acceptedPacket, initialRecord, "roam1", ports.LinkStateDegraded)

	aPC.in <- packet{b: sealFragment(12, pdgram.Fragment{Seq: 100, Index: 1, Count: 3, Data: []byte("mismatch")}), addr: testAddr("roam2")}
	waitSignal(malformed, "reassembly rejection")
	assertHealth(acceptedPacket, initialRecord, "roam1", ports.LinkStateDegraded)

	// Fresh reordered packets refresh contact, but only the greatest accepted
	// AEAD counter is eligible to move the roaming peer. Contact alone must not
	// re-arm socket hopping while stream progress remains stalled.
	a.mu.Lock()
	a.hoppedOffline = true
	a.mu.Unlock()
	clk.advance(time.Second)
	reorderedPacket := clk.Now()
	aPC.in <- packet{b: sealFragment(9, pdgram.Fragment{Seq: 101, Index: 0, Count: 2, Data: []byte("older")}), addr: testAddr("roam2")}
	waitSignal(authenticated, "fresh reordered fragment")
	assertHealth(reorderedPacket, initialRecord, "roam1", ports.LinkStateDegraded)
	a.mu.Lock()
	if !a.hoppedOffline {
		a.mu.Unlock()
		t.Fatal("fragment-only contact re-armed socket hopping")
	}
	a.mu.Unlock()

	clk.advance(time.Second)
	newestPacket := clk.Now()
	aPC.in <- packet{b: sealFragment(13, pdgram.Fragment{Seq: 102, Index: 0, Count: 2, Data: []byte("newer")}), addr: testAddr("roam2")}
	waitSignal(authenticated, "new greatest fragment")
	assertHealth(newestPacket, initialRecord, "roam2", ports.LinkStateDegraded)

	clk.advance(time.Second)
	completeAt := clk.Now()
	aPC.in <- packet{b: sealFragment(14, pdgram.Fragment{Seq: 103, Index: 0, Count: 1, Data: probeRecord(recPong, 999)}), addr: testAddr("roam2")}
	waitSignal(authenticated, "complete record")
	eventually(t, time.Second, func() bool { return a.LinkState() == ports.LinkStateConnected })
	assertHealth(completeAt, completeAt, "roam2", ports.LinkStateConnected)
}

func TestReliableReceiveBufferDoesNotDropAckedFutureFrame(t *testing.T) {
	tr := &Transport{maxRecvBuffer: 1, nextRecvSeq: 1, recvBuf: make(map[uint64]ports.Frame)}
	_, ack, queued := tr.enqueueReliable(2, ports.Frame{Type: ports.MsgOutput, Payload: []byte("future")})
	if !ack || !queued {
		t.Fatalf("future enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	_, ack, queued = tr.enqueueReliable(1, ports.Frame{Type: ports.MsgOutput, Payload: []byte("next")})
	if !ack || !queued {
		t.Fatalf("next enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	if _, ok := tr.recvBuf[2]; !ok {
		t.Fatal("acked future frame was dropped when next frame arrived")
	}
}

func TestDataRecordWireEncoding(t *testing.T) {
	got := encodeData(5, true, ports.Frame{Type: ports.MsgOutput, Payload: []byte("x")})
	want := []byte{recData, 0, 0, 0, 0, 0, 0, 0, 5, 1, byte(ports.MsgOutput), 0, 'x'}
	if !bytes.Equal(got, want) {
		t.Fatalf("encoded data=%v, want %v", got, want)
	}
	seq, reliable, frame, ok := decodeData(got)
	if !ok || seq != 5 || !reliable || frame.Type != ports.MsgOutput || string(frame.Payload) != "x" {
		t.Fatalf("decoded seq=%d reliable=%v frame=%+v ok=%v", seq, reliable, frame, ok)
	}
}

func TestDataRecordWireDecodeRejectsUnknownFlags(t *testing.T) {
	withTrailingGarbage := []byte{recData, 0, 0, 0, 0, 0, 0, 0, 5, 1, byte(ports.MsgOutput), 1, 'x'}
	if _, _, _, ok := decodeData(withTrailingGarbage); ok {
		t.Fatal("decodeData accepted unknown data-record flags with trailing garbage")
	}
}

func TestReliableDeliveryPreservesInputSequenceOrder(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	for i := range 100 {
		if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 100 {
		got := recvWithin(t, b, time.Second)
		if got.Type != ports.MsgInput || len(got.Payload) != 1 || got.Payload[0] != byte(i) {
			t.Fatalf("frame %d delivered out of order: %+v", i, got)
		}
	}
}

func TestAckProcessingContinuesWhenConsumerBackpressured(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	for i := range cap(b.in) + 20 {
		if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		pending := len(a.pending)
		a.mu.Unlock()
		if pending == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	a.mu.Lock()
	pending := len(a.pending)
	a.mu.Unlock()
	t.Fatalf("acks blocked behind undrained consumer; pending=%d", pending)
}

func TestReliableReceiverAcksOnlyHighestContiguousSequence(t *testing.T) {
	tr := &Transport{maxRecvBuffer: maxRecvBuffer, nextRecvSeq: 1, recvBuf: make(map[uint64]ports.Frame)}

	ackSeq, ack, queued := tr.enqueueReliable(3, ports.Frame{Type: ports.MsgOutput, Payload: []byte("third")})
	if !ack || !queued {
		t.Fatalf("out-of-order enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	if got, want := ackSeq, uint64(0); got != want {
		t.Fatalf("out-of-order ack=%d, want highest contiguous %d", got, want)
	}

	ackSeq, ack, queued = tr.enqueueReliable(1, ports.Frame{Type: ports.MsgOutput, Payload: []byte("first")})
	if !ack || !queued {
		t.Fatalf("first enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	if got, want := ackSeq, uint64(1); got != want {
		t.Fatalf("first contiguous ack=%d, want %d", got, want)
	}

	ackSeq, ack, queued = tr.enqueueReliable(2, ports.Frame{Type: ports.MsgOutput, Payload: []byte("second")})
	if !ack || !queued {
		t.Fatalf("gap fill enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	if got, want := ackSeq, uint64(3); got != want {
		t.Fatalf("filled gap ack=%d, want %d", got, want)
	}
}
