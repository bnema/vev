package dgram

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

// gatedControlPC stops the first control write after sendControl has selected
// its socket and installed its deadline. Close is separately gated so the test
// can observe whether a hop attempts to retire that socket too early.
type gatedControlPC struct {
	*fakePC
	entered      chan struct{}
	releaseWrite chan struct{}
	closeStarted chan struct{}
	releaseClose chan struct{}
	gateWrite    sync.Once
	gateClose    sync.Once
}

func (p *gatedControlPC) SetWriteDeadline(time.Time) error {
	p.gateWrite.Do(func() {
		close(p.entered)
		<-p.releaseWrite
	})
	return nil
}

func (p *gatedControlPC) Close() error {
	p.gateClose.Do(func() {
		close(p.closeStarted)
		<-p.releaseClose
	})
	return p.fakePC.Close()
}

func TestACKWriteCompletesBeforeRebindRetiresPacketConn(t *testing.T) {
	old, bPC := newPair()
	gated := &gatedControlPC{
		fakePC:       old,
		entered:      make(chan struct{}),
		releaseWrite: make(chan struct{}),
		closeStarted: make(chan struct{}),
		releaseClose: make(chan struct{}),
	}
	var releaseWrite, releaseClose sync.Once
	release := func() {
		releaseWrite.Do(func() { close(gated.releaseWrite) })
		releaseClose.Do(func() { close(gated.releaseClose) })
	}

	var fresh *fakePC
	a, err := NewTransportWithOptions(gated, bPC.addr, key(), 1, 2, Options{
		ResendAfter: time.Hour,
		Heartbeat:   time.Hour,
		RebindPacketConn: func(net.PacketConn) (net.PacketConn, error) {
			fresh = &fakePC{
				addr:  testAddr("a2"),
				in:    make(chan packet, 10),
				read:  make(chan struct{}, 10),
				peers: map[string]*fakePC{"b": bPC},
			}
			bPC.peers["a2"] = fresh
			return fresh, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewTransportWithOptions(bPC, old.addr, key(), 2, 1, Options{
		ResendAfter: time.Hour,
		Heartbeat:   time.Hour,
	})
	if err != nil {
		release()
		_ = a.Close()
		t.Fatal(err)
	}
	defer func() {
		release()
		_ = a.Close()
		_ = b.Close()
	}()

	// A decoded ACK clears this pending record, proving delivery to the peer,
	// rather than merely observing a write on the old PacketConn.
	b.mu.Lock()
	b.pending[7] = &pending{}
	processed := make(chan struct{}, 1)
	b.afterPacketProcessed = func() { processed <- struct{}{} }
	b.mu.Unlock()

	a.mu.Lock()
	a.linkState = ports.LinkStateProbing
	a.health.generation = 1
	a.mu.Unlock()
	controlDone := make(chan error, 1)
	go func() { controlDone <- a.sendControl(recAck, 7) }()
	awaitSignal(t, gated.entered, "ACK control write deadline")

	hopped := make(chan struct{})
	go func() {
		a.hopPacketConnOnce(1)
		close(hopped)
	}()

	// A hop must wait for the control write, even though this ACK does not wait
	// for data's writeMu. Before the write is released it must not retire old.
	select {
	case <-gated.closeStarted:
		t.Fatal("rebind began closing packet conn during control write")
	case <-hopped:
		t.Fatal("rebind completed during control write")
	case <-time.After(25 * time.Millisecond):
	}

	releaseWrite.Do(func() { close(gated.releaseWrite) })
	if err := awaitResult(t, controlDone, "ACK control write"); err != nil {
		t.Fatalf("ACK control write: %v", err)
	}
	awaitSignal(t, processed, "decoded ACK delivery")
	b.mu.Lock()
	_, pending := b.pending[7]
	b.mu.Unlock()
	if pending {
		t.Fatal("decoded ACK did not clear peer pending record")
	}

	awaitSignal(t, gated.closeStarted, "rebind retires old packet conn")
	releaseClose.Do(func() { close(gated.releaseClose) })
	select {
	case <-hopped:
	case <-time.After(time.Second):
		t.Fatal("rebind did not complete after control write")
	}
	if fresh == nil {
		t.Fatal("rebind did not install fresh packet conn")
	}
}
