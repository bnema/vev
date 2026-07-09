// Package dgram adapts authenticated UDP-style packet connections to ports.Transport.
package dgram

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
	pdgram "github.com/bnema/vev/pkg/dgram"
)

const (
	recData  byte = 1
	recAck   byte = 2
	recProbe byte = 3
	recPong  byte = 4

	defaultMTU                  = pdgram.DefaultMTU
	defaultResend               = 250 * time.Millisecond
	defaultMaxResendAfter       = 2 * time.Second
	defaultMaxResendPerTick     = 64
	defaultWriteTimeout         = 250 * time.Millisecond
	defaultHeartbeat            = 3 * time.Second
	defaultDegraded             = 10 * time.Second
	defaultProbe                = 20 * time.Second
	defaultOffline              = 30 * time.Second
	defaultDead                 = 60 * time.Second
	defaultMaxPending           = 1024
	defaultMaxPendingWait       = 50 * time.Millisecond
	defaultMaxRecvBuf           = 1024
	defaultOutputPaceMinDelay   = 8 * time.Millisecond
	defaultOutputPaceMinCadence = 20 * time.Millisecond
	defaultOutputPaceMaxDelay   = 250 * time.Millisecond
	defaultOutputPaceBatch      = 2
	defaultPacketPaceBudget     = 8
	dataRecordHeaderSize        = 12
	maxRecvBuffer               = defaultMaxRecvBuf
	linkEventBufferSize         = 16
	probeReplyBufferSize        = 16
)

var (
	ErrPendingFull = errors.New("dgram: pending reliable queue full")
	ErrLinkDead    = errors.New("dgram: link dead")
)

type Options struct {
	MTU              int
	ResendAfter      time.Duration
	MaxResendAfter   time.Duration
	MaxResendPerTick int
	WriteTimeout     time.Duration
	Heartbeat        time.Duration
	DegradedAfter    time.Duration
	ProbeAfter       time.Duration
	OfflineAfter     time.Duration
	DeadAfter        time.Duration
	MaxPending       int
	MaxPendingWait   time.Duration
	MaxRecvBuffer    int
	Clock            ports.Clock
	// RebindPacketConn is an optional client-side hook used to hop the local UDP
	// socket when the peer is offline. Server/proxy transports should leave it nil.
	RebindPacketConn func(net.PacketConn) (net.PacketConn, error)
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) NewTimer(d time.Duration) ports.Timer {
	return realTimer{t: time.NewTimer(d)}
}

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
func (r realTimer) Stop() bool                 { return r.t.Stop() }

func normalizeOptions(opts Options) Options {
	if opts.MTU <= 0 {
		opts.MTU = defaultMTU
	}
	if opts.ResendAfter <= 0 {
		opts.ResendAfter = defaultResend
	}
	if opts.MaxResendAfter <= 0 {
		opts.MaxResendAfter = defaultMaxResendAfter
	}
	if opts.MaxResendAfter < opts.ResendAfter {
		opts.MaxResendAfter = opts.ResendAfter
	}
	if opts.MaxResendPerTick <= 0 {
		opts.MaxResendPerTick = defaultMaxResendPerTick
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = defaultWriteTimeout
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = defaultHeartbeat
	}
	if opts.DegradedAfter <= 0 {
		opts.DegradedAfter = defaultDegraded
	}
	if opts.ProbeAfter <= 0 {
		opts.ProbeAfter = defaultProbe
	}
	if opts.OfflineAfter <= 0 {
		opts.OfflineAfter = defaultOffline
	}
	if opts.DeadAfter <= 0 {
		opts.DeadAfter = defaultDead
	}
	if opts.MaxPending <= 0 {
		opts.MaxPending = defaultMaxPending
	}
	if opts.MaxPendingWait <= 0 {
		opts.MaxPendingWait = defaultMaxPendingWait
	}
	if opts.MaxRecvBuffer <= 0 {
		opts.MaxRecvBuffer = defaultMaxRecvBuf
	}
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}
	return opts
}

type Transport struct {
	pc               net.PacketConn
	codec            *pdgram.Codec
	sendDir, recvDir uint32
	mtu              int

	mu               sync.Mutex
	peer             net.Addr
	ctr              uint64
	seq              uint64
	probeSeq         uint64
	pending          map[uint64]*pending
	closed           bool
	closeErr         error
	lastHeard        time.Time
	heartbeat        time.Duration
	resendAfter      time.Duration
	maxResendAfter   time.Duration
	maxResendPerTick int
	srtt             time.Duration
	rttvar           time.Duration
	rto              time.Duration
	writeTimeout     time.Duration
	degradedAfter    time.Duration
	probeAfter       time.Duration
	offlineAfter     time.Duration
	deadAfter        time.Duration
	maxPending       int
	maxPendingWait   time.Duration
	maxRecvBuffer    int
	clock            ports.Clock
	linkState        ports.LinkState
	linkEvents       chan ports.LinkEvent
	probeWait        map[uint64]chan struct{}
	sendWake         chan struct{}
	probeReply       chan uint64
	rebind           func(net.PacketConn) (net.PacketConn, error)
	hoppedOffline    bool
	outputQueue      []queuedSend
	outputWake       chan struct{}
	outputNext       time.Time

	recvMu sync.Mutex
	replay *pdgram.ReplayWindow
	reasm  *pdgram.Reassembler
	in     chan ports.Frame
	done   chan struct{}

	writeMu           sync.Mutex
	dataPaceMu        sync.Mutex
	dataPaceRemaining int
	dataPaceNext      time.Time
	// outboundMu orders first transmissions and paced batches. Retransmits use
	// writeMu directly because they do not establish new sequence order.
	outboundMu sync.Mutex

	deliverMu   sync.Mutex
	deliverCond *sync.Cond
	nextRecvSeq uint64
	recvBuf     map[uint64]ports.Frame
}

type pending struct {
	frame           ports.Frame
	enqueued        time.Time
	first           time.Time
	last            time.Time
	initialInFlight bool
	attempts        int
	retransmitted   bool
}

func NewTransport(pc net.PacketConn, peer net.Addr, key []byte, sendDir, recvDir uint32) (*Transport, error) {
	return NewTransportWithOptions(pc, peer, key, sendDir, recvDir, Options{})
}

func NewTransportWithOptions(pc net.PacketConn, peer net.Addr, key []byte, sendDir, recvDir uint32, opts Options) (*Transport, error) {
	c, err := pdgram.NewCodec(key)
	if err != nil {
		return nil, err
	}
	opts = normalizeOptions(opts)
	t := &Transport{
		pc:               pc,
		codec:            c,
		sendDir:          sendDir,
		recvDir:          recvDir,
		mtu:              opts.MTU,
		peer:             peer,
		pending:          make(map[uint64]*pending),
		replay:           pdgram.NewReplayWindow(),
		reasm:            pdgram.NewReassembler(),
		in:               make(chan ports.Frame, 32),
		done:             make(chan struct{}),
		lastHeard:        opts.Clock.Now(),
		heartbeat:        opts.Heartbeat,
		resendAfter:      opts.ResendAfter,
		maxResendAfter:   opts.MaxResendAfter,
		maxResendPerTick: opts.MaxResendPerTick,
		rto:              opts.ResendAfter,
		writeTimeout:     opts.WriteTimeout,
		degradedAfter:    opts.DegradedAfter,
		probeAfter:       opts.ProbeAfter,
		offlineAfter:     opts.OfflineAfter,
		deadAfter:        opts.DeadAfter,
		maxPending:       opts.MaxPending,
		maxPendingWait:   opts.MaxPendingWait,
		maxRecvBuffer:    opts.MaxRecvBuffer,
		clock:            opts.Clock,
		linkState:        ports.LinkStateConnected,
		linkEvents:       make(chan ports.LinkEvent, linkEventBufferSize),
		probeWait:        make(map[uint64]chan struct{}),
		rebind:           opts.RebindPacketConn,
		nextRecvSeq:      1,
		recvBuf:          make(map[uint64]ports.Frame),
	}
	t.sendWake = make(chan struct{})
	t.outputWake = make(chan struct{})
	t.probeReply = make(chan uint64, probeReplyBufferSize)
	t.deliverCond = sync.NewCond(&t.deliverMu)
	go t.readLoop(pc)
	go t.resendLoop()
	go t.deliveryLoop()
	go t.probeReplyLoop()
	go t.outputPaceLoop()
	return t, nil
}

func (t *Transport) Send(f ports.Frame) error { return t.send(f, false) }

func (t *Transport) SendAsync(f ports.Frame) error { return t.send(f, true) }

// SendSynchronous owns queue reservation, fragment pacing, per-write deadlines,
// and close cancellation. Callers must not layer a shorter preflight timer over it.
func (t *Transport) SendSynchronous(f ports.Frame) error { return t.Send(f) }

func (t *Transport) send(f ports.Frame, async bool) error {
	reliable := true
	if err := t.lockOutboundSlot(reliable); err != nil {
		return err
	}
	unlockOutbound := true
	defer func() {
		if unlockOutbound {
			t.outboundMu.Unlock()
		}
	}()
	// lockOutboundSlot returns with both outboundMu and mu held.

	t.seq++
	seq := t.seq
	if reliable {
		t.pending[seq] = &pending{frame: f, enqueued: t.clock.Now()}
	}
	if shouldPaceOutput(f) {
		now := t.clock.Now()
		if len(t.outputQueue) > 0 || (!t.outputNext.IsZero() && now.Before(t.outputNext)) {
			wasEmpty := len(t.outputQueue) == 0
			var done chan error
			if !async {
				done = make(chan error, 1)
			}
			t.outputQueue = append(t.outputQueue, queuedSend{seq: seq, reliable: reliable, frame: f, done: done})
			if wasEmpty {
				t.notifyOutputPacerLocked()
			}
			t.mu.Unlock()
			if done == nil {
				return nil
			}
			t.outboundMu.Unlock()
			unlockOutbound = false
			return t.waitQueuedSend(done)
		}
		t.outputNext = now.Add(t.outputPaceDelayLocked())
		t.mu.Unlock()
		t.markPendingSent(seq, reliable)
		if err := t.sendData(seq, reliable, f); err != nil {
			t.removePending(seq, reliable)
			return err
		}
		t.markPendingReady(seq, reliable)
		return nil
	}
	if len(t.outputQueue) > 0 {
		var done chan error
		if !async {
			done = make(chan error, 1)
		}
		t.outputQueue = append(t.outputQueue, queuedSend{seq: seq, reliable: reliable, frame: f, done: done})
		t.mu.Unlock()
		if done == nil {
			return nil
		}
		t.outboundMu.Unlock()
		unlockOutbound = false
		return t.waitQueuedSend(done)
	}
	t.mu.Unlock()
	t.markPendingSent(seq, reliable)
	if err := t.sendData(seq, reliable, f); err != nil {
		t.removePending(seq, reliable)
		return err
	}
	t.markPendingReady(seq, reliable)
	return nil
}

func (t *Transport) waitQueuedSend(done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-t.done:
		t.mu.Lock()
		err := t.closeErr
		t.mu.Unlock()
		if err != nil {
			return err
		}
		return errors.New("dgram: closed")
	}
}

func (t *Transport) closedError() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closeErr != nil {
		return t.closeErr
	}
	return errors.New("dgram: closed")
}

func (t *Transport) lockOutboundSlot(reliable bool) error {
	var pendingWaitStarted time.Time
	for {
		t.outboundMu.Lock()
		t.mu.Lock()
		if t.closed {
			err := t.closeErr
			t.mu.Unlock()
			t.outboundMu.Unlock()
			if err != nil {
				return err
			}
			return errors.New("dgram: closed")
		}
		if !reliable || len(t.pending) < t.maxPending {
			return nil
		}
		if t.linkState != ports.LinkStateConnected {
			t.mu.Unlock()
			t.outboundMu.Unlock()
			return ErrPendingFull
		}
		if pendingWaitStarted.IsZero() {
			pendingWaitStarted = t.clock.Now()
		}
		remaining := t.maxPendingWait - t.clock.Now().Sub(pendingWaitStarted)
		if remaining <= 0 {
			t.mu.Unlock()
			t.outboundMu.Unlock()
			return ErrPendingFull
		}
		wake := t.sendWake
		timer := t.clock.NewTimer(remaining)
		t.mu.Unlock()
		t.outboundMu.Unlock()
		select {
		case <-timer.C():
		case <-wake:
		case <-t.done:
		}
		timer.Stop()
	}
}

func (t *Transport) removePending(seq uint64, reliable bool) {
	if !reliable {
		return
	}
	t.mu.Lock()
	removed := t.removePendingLocked(seq)
	if removed {
		t.notifySendWaitersLocked()
	}
	t.mu.Unlock()
}

func (t *Transport) removeQueuedPending(queued []queuedSend, err error) {
	t.mu.Lock()
	removed := false
	for _, q := range queued {
		if q.reliable {
			removed = t.removePendingLocked(q.seq) || removed
		}
		if q.done != nil {
			q.done <- err
			close(q.done)
		}
	}
	if removed {
		t.notifySendWaitersLocked()
	}
	t.mu.Unlock()
}

func (t *Transport) removePendingLocked(seq uint64) bool {
	if _, ok := t.pending[seq]; !ok {
		return false
	}
	delete(t.pending, seq)
	return true
}

func (t *Transport) Recv() (ports.Frame, error) {
	select {
	case f := <-t.in:
		return f, nil
	case <-t.done:
		t.mu.Lock()
		err := t.closeErr
		t.mu.Unlock()
		if err != nil {
			return ports.Frame{}, err
		}
		return ports.Frame{}, errors.New("dgram: closed")
	}
}
func (t *Transport) Close() error {
	t.closeWithError(errors.New("dgram: closed"))
	t.mu.Lock()
	pc := t.pc
	t.mu.Unlock()
	return pc.Close()
}

func (t *Transport) Peer() net.Addr { t.mu.Lock(); defer t.mu.Unlock(); return t.peer }

func (t *Transport) LinkState() ports.LinkState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.linkState
}

func (t *Transport) LinkEvents() <-chan ports.LinkEvent { return t.linkEvents }

// Probe sends an authenticated datagram and waits for the peer to authenticate a
// response or for ctx to expire. It is intended for UDP bootstrap reachability checks.
func (t *Transport) Probe(ctx context.Context) error {
	id, ch, err := t.registerProbe()
	if err != nil {
		return err
	}
	defer t.unregisterProbe(id)
	if err := t.sendProbe(recProbe, id); err != nil {
		return err
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.done:
		t.mu.Lock()
		err := t.closeErr
		t.mu.Unlock()
		if err != nil {
			return err
		}
		return errors.New("dgram: closed")
	}
}

func (t *Transport) registerProbe() (uint64, chan struct{}, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		if t.closeErr != nil {
			return 0, nil, t.closeErr
		}
		return 0, nil, errors.New("dgram: closed")
	}
	t.probeSeq++
	id := t.probeSeq
	ch := make(chan struct{})
	t.probeWait[id] = ch
	return id, ch, nil
}

func (t *Transport) unregisterProbe(id uint64) {
	t.mu.Lock()
	delete(t.probeWait, id)
	t.mu.Unlock()
}

func (t *Transport) sendData(seq uint64, reliable bool, f ports.Frame) error {
	p := encodeData(seq, reliable, f)
	return t.sendPayload(p, true, seq)
}
func (t *Transport) sendAck(seq uint64) {
	var b [9]byte
	b[0] = recAck
	binary.BigEndian.PutUint64(b[1:], seq)
	_ = t.sendPayload(b[:], false, 0)
}
func (t *Transport) sendProbe(kind byte, id uint64) error {
	var b [9]byte
	b[0] = kind
	binary.BigEndian.PutUint64(b[1:], id)
	return t.sendPayload(b[:], false, 0)
}
func (t *Transport) sendPayload(p []byte, paced bool, pendingSeq uint64) error {
	if paced {
		t.dataPaceMu.Lock()
		defer t.dataPaceMu.Unlock()
	}
	frags, err := pdgram.FragmentPayload(t.nextCounter(), p, t.mtu-pdgram.HeaderSize-t.codec.Overhead())
	if err != nil {
		return err
	}
	for i, f := range frags {
		if pendingSeq != 0 && !t.pendingExists(pendingSeq) {
			return nil
		}
		if paced {
			if err := t.takeDataPaceSlot(); err != nil {
				return err
			}
		}
		raw, err := pdgram.MarshalFragment(f)
		if err != nil {
			return err
		}
		pkt := t.codec.Seal(t.sendDir, t.nextCounter(), raw, nil)
		t.mu.Lock()
		peer := t.peer
		closed := t.closed
		closeErr := t.closeErr
		t.mu.Unlock()
		if closed {
			if closeErr != nil {
				return closeErr
			}
			return errors.New("dgram: closed")
		}
		if peer == nil {
			return errors.New("dgram: no peer")
		}
		if pendingSeq != 0 && i == len(frags)-1 {
			t.markPendingFinalWrite(pendingSeq)
		}
		if err := t.writePacket(pkt, peer); err != nil {
			return err
		}
	}
	return nil
}

func (t *Transport) pendingExists(seq uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.pending[seq]
	return ok
}

func (t *Transport) takeDataPaceSlot() error {
	now := t.clock.Now()
	if t.dataPaceRemaining > 0 && now.Before(t.dataPaceNext) {
		t.dataPaceRemaining--
		return nil
	}
	if t.dataPaceNext.After(now) {
		timer := t.clock.NewTimer(t.dataPaceNext.Sub(now))
		select {
		case <-timer.C():
		case <-t.done:
			timer.Stop()
			return t.closedError()
		}
		timer.Stop()
	}
	t.mu.Lock()
	delay := t.outputPaceDelayLocked()
	t.mu.Unlock()
	t.dataPaceRemaining = defaultPacketPaceBudget - 1
	t.dataPaceNext = t.clock.Now().Add(delay)
	return nil
}

func (t *Transport) writePacket(pkt []byte, peer net.Addr) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	t.mu.Lock()
	pc := t.pc
	writeTimeout := t.writeTimeout
	t.mu.Unlock()

	if writeTimeout > 0 {
		if err := pc.SetWriteDeadline(t.clock.Now().Add(writeTimeout)); err != nil {
			return err
		}
		defer func() { _ = pc.SetWriteDeadline(time.Time{}) }()
	}
	_, err := pc.WriteTo(pkt, peer)
	return err
}

func (t *Transport) nextCounter() uint64 { t.mu.Lock(); defer t.mu.Unlock(); t.ctr++; return t.ctr }

func (t *Transport) readLoop(pc net.PacketConn) {
	buf := make([]byte, 64*1024)
	for {
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			t.mu.Lock()
			current := t.pc == pc
			closed := t.closed
			t.mu.Unlock()
			if current && !closed {
				t.closeWithError(err)
			}
			return
		}
		t.recvMu.Lock()
		_, pt, err := t.codec.Open(append([]byte(nil), buf[:n]...), t.recvDir, nil, t.replay)
		if err != nil {
			t.recvMu.Unlock()
			continue
		}
		frag, err := pdgram.UnmarshalFragment(pt)
		if err != nil {
			t.recvMu.Unlock()
			continue
		}
		payload, ok, err := t.reasm.Add(frag)
		t.recvMu.Unlock()
		if err != nil || !ok {
			continue
		}
		t.updatePeerFromAuthenticated(addr)
		t.handleRecord(payload)
	}
}

func (t *Transport) updatePeerFromAuthenticated(addr net.Addr) {
	t.mu.Lock()
	t.peer = addr
	t.lastHeard = t.clock.Now()
	t.hoppedOffline = false
	t.mu.Unlock()
	t.setLinkState(ports.LinkStateConnected, nil)
}

func (t *Transport) handleRecord(p []byte) {
	if len(p) < 1 {
		return
	}
	switch p[0] {
	case recAck:
		if len(p) != 9 {
			return
		}
		seq := binary.BigEndian.Uint64(p[1:])
		t.mu.Lock()
		acked := false
		for pendingSeq, p := range t.pending {
			if pendingSeq > seq {
				continue
			}
			if pendingSeq == seq && p != nil && !p.retransmitted {
				t.updateRTTLocked(t.clock.Now().Sub(p.first))
			}
			delete(t.pending, pendingSeq)
			acked = true
		}
		if acked {
			t.notifySendWaitersLocked()
		}
		t.mu.Unlock()
	case recProbe:
		if len(p) != 9 {
			return
		}
		select {
		case t.probeReply <- binary.BigEndian.Uint64(p[1:]):
		default:
		}
	case recPong:
		if len(p) != 9 {
			return
		}
		id := binary.BigEndian.Uint64(p[1:])
		t.mu.Lock()
		ch := t.probeWait[id]
		if ch != nil {
			delete(t.probeWait, id)
			close(ch)
		}
		t.mu.Unlock()
	case recData:
		seq, reliable, f, ok := decodeData(p)
		if !ok {
			return
		}
		if reliable {
			ackSeq, ack, queued := t.enqueueReliable(seq, f)
			if ack {
				t.sendAck(ackSeq)
			}
			if queued {
				t.signalDelivery()
			}
			return
		}
		t.deliver(f)
	}
}

func (t *Transport) enqueueReliable(seq uint64, f ports.Frame) (ackSeq uint64, ack bool, queued bool) {
	t.deliverMu.Lock()
	defer t.deliverMu.Unlock()
	if seq < t.nextRecvSeq {
		return t.highestContiguousRecvLocked(), true, false
	}
	if _, exists := t.recvBuf[seq]; exists {
		return t.highestContiguousRecvLocked(), true, false
	}
	if len(t.recvBuf) >= t.maxRecvBuffer && seq != t.nextRecvSeq {
		return 0, false, false
	}
	t.recvBuf[seq] = f
	return t.highestContiguousRecvLocked(), true, true
}

func (t *Transport) highestContiguousRecvLocked() uint64 {
	seq := t.nextRecvSeq
	for {
		if _, ok := t.recvBuf[seq]; ok {
			seq++
			continue
		}
		break
	}
	return seq - 1
}

func (t *Transport) signalDelivery() {
	t.deliverMu.Lock()
	t.deliverCond.Signal()
	t.deliverMu.Unlock()
}

func (t *Transport) deliveryLoop() {
	for {
		t.deliverMu.Lock()
		f, ok := t.recvBuf[t.nextRecvSeq]
		for !ok {
			if t.isClosed() {
				t.deliverMu.Unlock()
				return
			}
			t.deliverCond.Wait()
			f, ok = t.recvBuf[t.nextRecvSeq]
		}
		delete(t.recvBuf, t.nextRecvSeq)
		t.nextRecvSeq++
		t.deliverMu.Unlock()
		t.deliver(f)
	}
}

func (t *Transport) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func (t *Transport) deliver(f ports.Frame) {
	select {
	case t.in <- f:
	case <-t.done:
	}
}

func (t *Transport) probeReplyLoop() {
	for {
		select {
		case id := <-t.probeReply:
			_ = t.sendProbe(recPong, id)
		case <-t.done:
			return
		}
	}
}

func (t *Transport) notifySendWaitersLocked() {
	if t.sendWake == nil {
		return
	}
	close(t.sendWake)
	t.sendWake = make(chan struct{})
}

func (t *Transport) resendLoop() {
	resend := t.clock.NewTimer(t.resendAfter)
	defer resend.Stop()
	heartbeat := t.clock.NewTimer(t.heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-resend.C():
			t.resendPending()
			t.checkSilence()
			t.checkSendStall()
			resend.Reset(t.resendAfter)
		case <-heartbeat.C():
			_ = t.sendProbe(recProbe, 0)
			t.checkSilence()
			t.checkSendStall()
			heartbeat.Reset(t.heartbeat)
		case <-t.done:
			return
		}
	}
}

func (t *Transport) resendPending() {
	now := t.clock.Now()
	var resend []struct {
		seq uint64
		f   ports.Frame
	}
	t.mu.Lock()
	seqs := make([]uint64, 0, len(t.pending))
	for seq := range t.pending {
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	limit := t.maxResendPerTick
	for _, seq := range seqs {
		if limit <= 0 {
			break
		}
		p := t.pending[seq]
		if p == nil || p.last.IsZero() || p.initialInFlight || now.Sub(p.last) < t.resendDelayLocked(p) {
			continue
		}
		p.initialInFlight = true
		p.attempts++
		p.retransmitted = true
		resend = append(resend, struct {
			seq uint64
			f   ports.Frame
		}{seq, p.frame})
		limit--
	}
	t.mu.Unlock()
	for _, r := range resend {
		if err := t.sendData(r.seq, true, r.f); err == nil {
			t.markPendingReady(r.seq, true)
		} else {
			t.mu.Lock()
			if p := t.pending[r.seq]; p != nil {
				p.initialInFlight = false
			}
			t.mu.Unlock()
		}
	}
}

func (t *Transport) resendDelayLocked(p *pending) time.Duration {
	d := t.rto
	if d <= 0 {
		d = t.resendAfter
	}
	for i := 0; i < p.attempts && d < t.maxResendAfter; i++ {
		d *= 2
		if d > t.maxResendAfter {
			return t.maxResendAfter
		}
	}
	return d
}

func (t *Transport) updateRTTLocked(sample time.Duration) {
	if sample <= 0 {
		return
	}
	if t.srtt <= 0 {
		t.srtt = sample
		t.rttvar = sample / 2
	} else {
		delta := t.srtt - sample
		if delta < 0 {
			delta = -delta
		}
		t.rttvar = (3*t.rttvar + delta) / 4
		t.srtt = (7*t.srtt + sample) / 8
	}
	variation := 4 * t.rttvar
	if variation < time.Millisecond {
		variation = time.Millisecond
	}
	rto := t.srtt + variation
	if rto < t.resendAfter {
		rto = t.resendAfter
	}
	if rto > t.maxResendAfter {
		rto = t.maxResendAfter
	}
	t.rto = rto
}

func (t *Transport) hopPacketConnOnce() {
	t.mu.Lock()
	if t.rebind == nil || t.hoppedOffline || t.closed {
		t.mu.Unlock()
		return
	}
	old := t.pc
	rebind := t.rebind
	t.hoppedOffline = true
	t.mu.Unlock()
	pc, err := rebind(old)
	if err != nil {
		t.mu.Lock()
		if t.pc == old {
			t.hoppedOffline = false
		}
		t.mu.Unlock()
		return
	}
	t.mu.Lock()
	if t.closed || t.pc != old {
		t.mu.Unlock()
		_ = pc.Close()
		return
	}
	t.pc = pc
	t.mu.Unlock()
	go t.readLoop(pc)
	_ = old.Close()
}

func (t *Transport) checkSilence() {
	t.mu.Lock()
	last := t.lastHeard
	closed := t.closed
	degradedAfter := t.degradedAfter
	probeAfter := t.probeAfter
	offlineAfter := t.offlineAfter
	deadAfter := t.deadAfter
	now := t.clock.Now()
	t.mu.Unlock()
	if closed {
		return
	}
	silentFor := now.Sub(last)
	switch {
	case deadAfter > 0 && silentFor >= deadAfter:
		t.setLinkState(ports.LinkStateDead, ErrLinkDead)
		t.closeWithError(ErrLinkDead)
	case offlineAfter > 0 && silentFor >= offlineAfter:
		t.setLinkState(ports.LinkStateOffline, nil)
	case probeAfter > 0 && silentFor >= probeAfter:
		t.setLinkState(ports.LinkStateProbing, nil)
		t.hopPacketConnOnce()
	case degradedAfter > 0 && silentFor >= degradedAfter:
		t.setLinkState(ports.LinkStateDegraded, nil)
	}
}

// checkSendStall declares the link dead when the oldest unacked reliable frame
// has gone unacknowledged for deadAfter despite at least one retransmit. The
// receive-silence check in checkSilence misses one-way-dead links whose receive
// side stays fresh from peer heartbeats; this catches them at the layer that
// owns death. enqueued is set once when a frame is accepted and never refreshed,
// so acks (which delete pending entries) keep the oldest age bounded on a
// progressing link — only a frame that is never acked crosses the threshold.
func (t *Transport) checkSendStall() {
	t.mu.Lock()
	if t.closed || t.deadAfter <= 0 {
		t.mu.Unlock()
		return
	}
	deadAfter := t.deadAfter
	now := t.clock.Now()
	var oldest time.Time
	var attempts int
	found := false
	for _, p := range t.pending {
		if p == nil || p.enqueued.IsZero() {
			continue
		}
		if !found || p.enqueued.Before(oldest) {
			oldest = p.enqueued
			attempts = p.attempts
			found = true
		}
	}
	t.mu.Unlock()
	if !found || attempts < 1 || now.Sub(oldest) < deadAfter {
		return
	}
	t.setLinkState(ports.LinkStateDead, ErrLinkDead)
	t.closeWithError(ErrLinkDead)
}

func (t *Transport) setLinkState(state ports.LinkState, err error) {
	now := t.clock.Now()
	t.mu.Lock()
	if t.linkState == state {
		t.mu.Unlock()
		return
	}
	t.linkState = state
	if state == ports.LinkStateConnected {
		for _, p := range t.pending {
			if p != nil {
				p.attempts = 0
			}
		}
	}
	t.notifySendWaitersLocked()
	t.mu.Unlock()
	event := ports.LinkEvent{State: state, At: now, Err: err}
	select {
	case t.linkEvents <- event:
	default:
	}
}

func (t *Transport) closeWithError(err error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	t.closeErr = err
	for _, q := range t.outputQueue {
		if q.done != nil {
			q.done <- err
			close(q.done)
		}
	}
	t.outputQueue = nil
	t.probeWait = make(map[uint64]chan struct{})
	close(t.done)
	t.notifySendWaitersLocked()
	t.mu.Unlock()
	t.deliverMu.Lock()
	t.deliverCond.Broadcast()
	t.deliverMu.Unlock()
}

func encodeData(seq uint64, reliable bool, f ports.Frame) []byte {
	b := make([]byte, dataRecordHeaderSize+len(f.Payload))
	b[0] = recData
	binary.BigEndian.PutUint64(b[1:9], seq)
	if reliable {
		b[9] = 1
	}
	b[10] = byte(f.Type)
	copy(b[dataRecordHeaderSize:], f.Payload)
	return b
}
func decodeData(b []byte) (uint64, bool, ports.Frame, bool) {
	if len(b) < dataRecordHeaderSize || b[0] != recData {
		return 0, false, ports.Frame{}, false
	}
	if b[11] != 0 {
		return 0, false, ports.Frame{}, false
	}
	return binary.BigEndian.Uint64(b[1:9]), b[9] == 1, ports.Frame{Type: ports.MsgType(b[10]), Payload: append([]byte(nil), b[dataRecordHeaderSize:]...)}, true
}
