// Package dgram adapts authenticated UDP-style packet connections to ports.Transport.
package dgram

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sort"
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

	defaultMTU              = pdgram.DefaultMTU
	defaultResend           = 250 * time.Millisecond
	defaultMaxResendAfter   = 2 * time.Second
	defaultMaxResendPerTick = 64
	defaultWriteTimeout     = 250 * time.Millisecond
	defaultHeartbeat        = 3 * time.Second
	defaultDegraded         = 10 * time.Second
	defaultProbe            = 20 * time.Second
	defaultOffline          = 30 * time.Second
	defaultDead             = 60 * time.Second
	defaultMaxPending       = 1024
	defaultMaxRecvBuf       = 1024
	maxRecvBuffer           = defaultMaxRecvBuf
	linkEventBufferSize     = 16
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
	writeTimeout     time.Duration
	degradedAfter    time.Duration
	probeAfter       time.Duration
	offlineAfter     time.Duration
	deadAfter        time.Duration
	maxPending       int
	maxRecvBuffer    int
	clock            ports.Clock
	linkState        ports.LinkState
	linkEvents       chan ports.LinkEvent
	probeWait        map[uint64]chan struct{}
	sendCond         *sync.Cond
	rebind           func(net.PacketConn) (net.PacketConn, error)
	hoppedOffline    bool

	recvMu sync.Mutex
	replay *pdgram.ReplayWindow
	reasm  *pdgram.Reassembler
	in     chan ports.Frame
	done   chan struct{}

	writeMu sync.Mutex

	deliverMu   sync.Mutex
	deliverCond *sync.Cond
	nextRecvSeq uint64
	recvBuf     map[uint64]ports.Frame
}

type pending struct {
	frame    ports.Frame
	last     time.Time
	attempts int
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
	t := &Transport{pc: pc, codec: c, sendDir: sendDir, recvDir: recvDir, mtu: opts.MTU, peer: peer, pending: make(map[uint64]*pending), replay: pdgram.NewReplayWindow(), reasm: pdgram.NewReassembler(), in: make(chan ports.Frame, 32), done: make(chan struct{}), lastHeard: opts.Clock.Now(), heartbeat: opts.Heartbeat, resendAfter: opts.ResendAfter, maxResendAfter: opts.MaxResendAfter, maxResendPerTick: opts.MaxResendPerTick, writeTimeout: opts.WriteTimeout, degradedAfter: opts.DegradedAfter, probeAfter: opts.ProbeAfter, offlineAfter: opts.OfflineAfter, deadAfter: opts.DeadAfter, maxPending: opts.MaxPending, maxRecvBuffer: opts.MaxRecvBuffer, clock: opts.Clock, linkState: ports.LinkStateConnected, linkEvents: make(chan ports.LinkEvent, linkEventBufferSize), probeWait: make(map[uint64]chan struct{}), rebind: opts.RebindPacketConn, nextRecvSeq: 1, recvBuf: make(map[uint64]ports.Frame)}
	t.deliverCond = sync.NewCond(&t.deliverMu)
	t.sendCond = sync.NewCond(&t.mu)
	go t.readLoop(pc)
	go t.resendLoop()
	go t.deliveryLoop()
	return t, nil
}

func (t *Transport) Send(f ports.Frame) error {
	// Until full ACK-driven terminal state sync exists, every frame is reliable;
	// dropping MsgOutput can permanently desynchronize the client screen.
	reliable := true
	t.mu.Lock()
	for reliable && len(t.pending) >= t.maxPending && !t.closed {
		if t.linkState != ports.LinkStateConnected {
			t.mu.Unlock()
			return ErrPendingFull
		}
		t.sendCond.Wait()
	}
	if t.closed {
		err := t.closeErr
		t.mu.Unlock()
		if err != nil {
			return err
		}
		return errors.New("dgram: closed")
	}
	t.seq++
	seq := t.seq
	if reliable {
		t.pending[seq] = &pending{frame: f, last: t.clock.Now()}
	}
	t.mu.Unlock()
	return t.sendData(seq, reliable, f)
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
	return t.sendPayload(p)
}
func (t *Transport) sendAck(seq uint64) {
	var b [9]byte
	b[0] = recAck
	binary.BigEndian.PutUint64(b[1:], seq)
	_ = t.sendPayload(b[:])
}
func (t *Transport) sendProbe(kind byte, id uint64) error {
	var b [9]byte
	b[0] = kind
	binary.BigEndian.PutUint64(b[1:], id)
	return t.sendPayload(b[:])
}
func (t *Transport) sendPayload(p []byte) error {
	frags, err := pdgram.FragmentPayload(t.nextCounter(), p, t.mtu-pdgram.HeaderSize-t.codec.Overhead())
	if err != nil {
		return err
	}
	for _, f := range frags {
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
		if err := t.writePacket(pkt, peer); err != nil {
			return err
		}
	}
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
		if err := pc.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
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
		if _, ok := t.pending[seq]; ok {
			delete(t.pending, seq)
			if t.sendCond != nil {
				t.sendCond.Signal()
			}
		}
		t.mu.Unlock()
	case recProbe:
		if len(p) != 9 {
			return
		}
		_ = t.sendProbe(recPong, binary.BigEndian.Uint64(p[1:]))
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
			ack, queued := t.enqueueReliable(seq, f)
			if ack {
				t.sendAck(seq)
			}
			if queued {
				t.signalDelivery()
			}
			return
		}
		t.deliver(f)
	}
}

func (t *Transport) enqueueReliable(seq uint64, f ports.Frame) (ack bool, queued bool) {
	t.deliverMu.Lock()
	defer t.deliverMu.Unlock()
	if seq < t.nextRecvSeq {
		return true, false
	}
	if _, exists := t.recvBuf[seq]; exists {
		return true, false
	}
	if len(t.recvBuf) >= t.maxRecvBuffer && seq != t.nextRecvSeq {
		return false, false
	}
	t.recvBuf[seq] = f
	return true, true
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
			resend.Reset(t.resendAfter)
		case <-heartbeat.C():
			_ = t.sendProbe(recProbe, 0)
			t.checkSilence()
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
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	limit := t.maxResendPerTick
	for _, seq := range seqs {
		if limit <= 0 {
			break
		}
		p := t.pending[seq]
		if p == nil || now.Sub(p.last) < t.resendDelayLocked(p) {
			continue
		}
		p.last = now
		p.attempts++
		resend = append(resend, struct {
			seq uint64
			f   ports.Frame
		}{seq, p.frame})
		limit--
	}
	t.mu.Unlock()
	for _, r := range resend {
		_ = t.sendData(r.seq, true, r.f)
	}
}

func (t *Transport) resendDelayLocked(p *pending) time.Duration {
	d := t.resendAfter
	for i := 0; i < p.attempts && d < t.maxResendAfter; i++ {
		d *= 2
		if d > t.maxResendAfter {
			return t.maxResendAfter
		}
	}
	return d
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
		t.hopPacketConnOnce()
	case probeAfter > 0 && silentFor >= probeAfter:
		t.setLinkState(ports.LinkStateProbing, nil)
	case degradedAfter > 0 && silentFor >= degradedAfter:
		t.setLinkState(ports.LinkStateDegraded, nil)
	}
}

func (t *Transport) setLinkState(state ports.LinkState, err error) {
	now := t.clock.Now()
	t.mu.Lock()
	if t.linkState == state {
		t.mu.Unlock()
		return
	}
	t.linkState = state
	if t.sendCond != nil {
		t.sendCond.Broadcast()
	}
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
	t.probeWait = make(map[uint64]chan struct{})
	close(t.done)
	if t.sendCond != nil {
		t.sendCond.Broadcast()
	}
	t.mu.Unlock()
	t.deliverMu.Lock()
	t.deliverCond.Broadcast()
	t.deliverMu.Unlock()
}

func encodeData(seq uint64, reliable bool, f ports.Frame) []byte {
	b := make([]byte, 12+len(f.Payload))
	b[0] = recData
	binary.BigEndian.PutUint64(b[1:9], seq)
	if reliable {
		b[9] = 1
	}
	b[10] = byte(f.Type)
	b[11] = 0
	copy(b[12:], f.Payload)
	return b
}
func decodeData(b []byte) (uint64, bool, ports.Frame, bool) {
	if len(b) < 12 || b[0] != recData {
		return 0, false, ports.Frame{}, false
	}
	return binary.BigEndian.Uint64(b[1:9]), b[9] == 1, ports.Frame{Type: ports.MsgType(b[10]), Payload: append([]byte(nil), b[12:]...)}, true
}
