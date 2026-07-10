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
	probeReplyBufferSize        = controlQueueSize // legacy test burst bound
)

var (
	ErrPendingFull = errors.New("dgram: pending reliable queue full")
	ErrLinkDead    = errors.New("dgram: link dead")
	errControlFull = errors.New("dgram: control queue full")
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
	// Observe receives bounded, asynchronous, privacy-safe link health snapshots.
	Observe DiagnosticObserver
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

	mu                      sync.Mutex
	peer                    net.Addr
	ctr                     uint64
	seq                     uint64
	probeSeq                uint64
	pending                 map[uint64]*pending
	closed                  bool
	closeErr                error
	lastHeard               time.Time
	lastAuthenticatedPacket time.Time
	lastCompleteRecord      time.Time
	lastACKProgress         time.Time
	retransmits             uint64
	reassemblyInflight      int
	// afterAuthenticatedPacket is a test synchronization hook. It runs after
	// authenticated-contact state has been committed.
	afterAuthenticatedPacket func()
	// afterMalformedFragment is a test synchronization hook. It runs after an
	// authenticated datagram has been rejected as an invalid fragment.
	afterMalformedFragment func()
	observe                DiagnosticObserver
	diagnosticCh           chan Diagnostic
	heartbeat              time.Duration
	resendAfter            time.Duration
	maxResendAfter         time.Duration
	maxResendPerTick       int
	srtt                   time.Duration
	rttvar                 time.Duration
	rto                    time.Duration
	writeTimeout           time.Duration
	degradedAfter          time.Duration
	probeAfter             time.Duration
	offlineAfter           time.Duration
	deadAfter              time.Duration
	maxPending             int
	maxPendingWait         time.Duration
	maxRecvBuffer          int
	clock                  ports.Clock
	linkState              ports.LinkState
	linkEvents             chan ports.LinkEvent
	probeWait              map[uint64]chan struct{}
	sendWake               chan struct{}
	control                chan controlRecord
	ackWake                chan struct{}
	ackSend                chan uint64
	controlMu              sync.Mutex
	ackSeq                 uint64
	ackQueued              bool
	rebind                 func(net.PacketConn) (net.PacketConn, error)
	hoppedOffline          bool
	outputQueue            []queuedSend
	outputWake             chan struct{}
	outputNext             time.Time
	retransmitWork         chan []retransmitRecord
	writeDeadlines         *writeDeadlineState

	recvMu sync.Mutex
	replay *pdgram.ReplayWindow
	reasm  *pdgram.Reassembler
	in     chan ports.Frame
	done   chan struct{}

	writeMu           sync.Mutex
	dataPaceMu        sync.Mutex
	dataPaceRemaining int
	dataPaceNext      time.Time
	// beforeDataPace is a test synchronization hook. It is read under mu.
	beforeDataPace func()
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

type retransmitRecord struct {
	seq uint64
	f   ports.Frame
}

type writeDeadlineState struct {
	mu        sync.Mutex
	pc        net.PacketConn
	nextID    uint64
	deadlines map[uint64]time.Time
}

func newWriteDeadlineState(pc net.PacketConn) *writeDeadlineState {
	return &writeDeadlineState{pc: pc, deadlines: make(map[uint64]time.Time)}
}

func (s *writeDeadlineState) begin(deadline time.Time) (func(), error) {
	s.mu.Lock()
	s.nextID++
	id := s.nextID
	s.deadlines[id] = deadline
	if err := s.applyLocked(); err != nil {
		delete(s.deadlines, id)
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		delete(s.deadlines, id)
		_ = s.applyLocked()
		s.mu.Unlock()
	}, nil
}

func (s *writeDeadlineState) applyLocked() error {
	var earliest time.Time
	for _, deadline := range s.deadlines {
		if earliest.IsZero() || deadline.Before(earliest) {
			earliest = deadline
		}
	}
	return s.pc.SetWriteDeadline(earliest)
}

func (s *writeDeadlineState) expire(now time.Time) {
	s.mu.Lock()
	for id, deadline := range s.deadlines {
		if !deadline.After(now) {
			delete(s.deadlines, id)
		}
	}
	_ = s.applyLocked()
	s.mu.Unlock()
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
		pc:                      pc,
		codec:                   c,
		sendDir:                 sendDir,
		recvDir:                 recvDir,
		mtu:                     opts.MTU,
		peer:                    peer,
		pending:                 make(map[uint64]*pending),
		replay:                  pdgram.NewReplayWindow(),
		reasm:                   pdgram.NewReassembler(),
		in:                      make(chan ports.Frame, 32),
		done:                    make(chan struct{}),
		lastHeard:               opts.Clock.Now(),
		lastAuthenticatedPacket: opts.Clock.Now(),
		lastCompleteRecord:      opts.Clock.Now(),
		lastACKProgress:         opts.Clock.Now(),
		observe:                 opts.Observe,
		heartbeat:               opts.Heartbeat,
		resendAfter:             opts.ResendAfter,
		maxResendAfter:          opts.MaxResendAfter,
		maxResendPerTick:        opts.MaxResendPerTick,
		rto:                     opts.ResendAfter,
		writeTimeout:            opts.WriteTimeout,
		degradedAfter:           opts.DegradedAfter,
		probeAfter:              opts.ProbeAfter,
		offlineAfter:            opts.OfflineAfter,
		deadAfter:               opts.DeadAfter,
		maxPending:              opts.MaxPending,
		maxPendingWait:          opts.MaxPendingWait,
		maxRecvBuffer:           opts.MaxRecvBuffer,
		clock:                   opts.Clock,
		linkState:               ports.LinkStateConnected,
		linkEvents:              make(chan ports.LinkEvent, linkEventBufferSize),
		probeWait:               make(map[uint64]chan struct{}),
		rebind:                  opts.RebindPacketConn,
		nextRecvSeq:             1,
		recvBuf:                 make(map[uint64]ports.Frame),
	}
	t.sendWake = make(chan struct{})
	t.control = make(chan controlRecord, controlQueueSize)
	t.ackWake = make(chan struct{}, 1)
	t.ackSend = make(chan uint64, 1)
	t.outputWake = make(chan struct{})
	t.writeDeadlines = newWriteDeadlineState(pc)
	// One queued batch bounds retransmit work while its sole sender is paced.
	t.retransmitWork = make(chan []retransmitRecord, 1)
	if t.observe != nil {
		t.diagnosticCh = make(chan Diagnostic, diagnosticBufferSize)
		go t.diagnosticLoop()
	}
	t.deliverCond = sync.NewCond(&t.deliverMu)
	go t.readLoop(pc)
	go t.retransmitLoop()
	go t.heartbeatLoop()
	go t.healthLoop()
	go t.retransmitSenderLoop()
	go t.deliveryLoop()
	go t.controlLoop()
	go t.ackSendLoop()
	go t.outputPaceLoop()
	t.emitDiagnostic()
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
	sendResult := make(chan error, 1)
	if !t.queueControlRecord(controlRecord{kind: recProbe, id: id, result: sendResult}) {
		return errControlFull
	}
	for {
		select {
		case err := <-sendResult:
			if err != nil {
				return err
			}
			sendResult = nil
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
func (t *Transport) sendPayload(p []byte, paced bool, pendingSeq uint64) error {
	if paced {
		t.mu.Lock()
		beforeDataPace := t.beforeDataPace
		t.mu.Unlock()
		if beforeDataPace != nil {
			beforeDataPace()
		}
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
	writeDeadlines := t.writeDeadlines
	t.mu.Unlock()

	return t.writeDatagram(pc, peer, pkt, writeDeadlines, writeTimeout)
}

func (t *Transport) writeDatagram(pc net.PacketConn, peer net.Addr, pkt []byte, deadlines *writeDeadlineState, timeout time.Duration) error {
	if timeout <= 0 {
		_, err := pc.WriteTo(pkt, peer)
		return err
	}
	ownDeadline := t.clock.Now().Add(timeout)
	finish, err := deadlines.begin(ownDeadline)
	if err != nil {
		return err
	}
	defer finish()
	for {
		_, err = pc.WriteTo(pkt, peer)
		if err == nil {
			return nil
		}
		var timeoutErr interface{ Timeout() bool }
		now := t.clock.Now()
		if !errors.As(err, &timeoutErr) || !timeoutErr.Timeout() || !now.Before(ownDeadline) {
			return err
		}
		deadlines.expire(now)
	}
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
			t.mu.Lock()
			afterMalformed := t.afterMalformedFragment
			t.mu.Unlock()
			if afterMalformed != nil {
				afterMalformed()
			}
			continue
		}
		payload, complete, err := t.reasm.Add(frag)
		inflight := t.reasm.Inflight()
		t.recvMu.Unlock()
		t.recordAuthenticatedFragment(inflight)
		if err != nil || !complete {
			continue
		}
		t.recordCompleteRecord()
		t.updatePeerFromAuthenticated(addr)
		t.handleRecord(payload)
	}
}

func (t *Transport) recordAuthenticatedFragment(inflight int) {
	t.mu.Lock()
	t.lastAuthenticatedPacket = t.clock.Now()
	t.reassemblyInflight = inflight
	after := t.afterAuthenticatedPacket
	t.mu.Unlock()
	if after != nil {
		after()
	}
}

func (t *Transport) recordCompleteRecord() {
	t.mu.Lock()
	t.lastCompleteRecord = t.clock.Now()
	t.mu.Unlock()
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
			t.lastACKProgress = t.clock.Now()
			t.notifySendWaitersLocked()
		}
		t.mu.Unlock()
		if acked {
			t.emitDiagnostic()
		}
	case recProbe:
		if len(p) != 9 {
			return
		}
		t.queueControl(recPong, binary.BigEndian.Uint64(p[1:]))
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
				t.queueACK(ackSeq)
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

func (t *Transport) notifySendWaitersLocked() {
	if t.sendWake == nil {
		return
	}
	close(t.sendWake)
	t.sendWake = make(chan struct{})
}

func (t *Transport) retransmitLoop() {
	resend := t.clock.NewTimer(t.resendAfter)
	defer resend.Stop()
	for {
		select {
		case <-resend.C():
			t.queueRetransmits()
			resend.Reset(t.resendAfter)
		case <-t.done:
			return
		}
	}
}

// healthLoop owns link-health timing so bulk retransmission cannot delay state
// transitions or socket hopping.
func (t *Transport) healthLoop() {
	health := t.clock.NewTimer(t.resendAfter)
	defer health.Stop()
	for {
		select {
		case <-health.C():
			t.checkSilence()
			t.checkSendStall()
			health.Reset(t.resendAfter)
		case <-t.done:
			return
		}
	}
}

func (t *Transport) retransmitSenderLoop() {
	for {
		select {
		case batch := <-t.retransmitWork:
			for _, r := range batch {
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
		case <-t.done:
			return
		}
	}
}

func (t *Transport) heartbeatLoop() {
	heartbeat := t.clock.NewTimer(t.heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-heartbeat.C():
			t.queueControl(recProbe, 0)
			heartbeat.Reset(t.heartbeat)
		case <-t.done:
			return
		}
	}
}

func (t *Transport) queueRetransmits() {
	// Do not mark records in-flight while the one-slot sender queue is full.
	// There is one producer and the sender only consumes, so capacity cannot be
	// lost after this check.
	if len(t.retransmitWork) == cap(t.retransmitWork) {
		return
	}

	now := t.clock.Now()
	t.mu.Lock()
	resend, emitDiagnostic := t.selectRetransmitsLocked(now)
	t.mu.Unlock()

	if len(resend) > 0 {
		select {
		case t.retransmitWork <- resend:
		case <-t.done:
			return
		}
	}
	if emitDiagnostic {
		t.emitDiagnostic()
	}
}

// selectRetransmitsLocked deterministically reserves one bounded retransmit
// turn. The caller must hold t.mu and performs all pacing and writes after
// releasing it.
func (t *Transport) selectRetransmitsLocked(now time.Time) ([]retransmitRecord, bool) {
	resend := make([]retransmitRecord, 0, t.maxResendPerTick)
	seqs := make([]uint64, 0, len(t.pending))
	for seq := range t.pending {
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	limit := t.maxResendPerTick
	emitDiagnostic := false
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
		t.retransmits++
		emitDiagnostic = emitDiagnostic || t.retransmits%64 == 0
		resend = append(resend, retransmitRecord{seq: seq, f: p.frame})
		limit--
	}
	return resend, emitDiagnostic
}

// resendPending remains a synchronous test hook for retransmission selection
// unit tests. Production timer paths use queueRetransmits and the sole sender.
func (t *Transport) resendPending() {
	now := t.clock.Now()
	t.mu.Lock()
	resend, _ := t.selectRetransmitsLocked(now)
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
	t.writeDeadlines = newWriteDeadlineState(pc)
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
	t.emitDiagnostic()
	event := ports.LinkEvent{State: state, At: now, Err: err}
	select {
	case t.linkEvents <- event:
	default:
	}
}

func (t *Transport) diagnosticLoop() {
	for {
		select {
		case d := <-t.diagnosticCh:
			t.observe(d)
		case <-t.done:
			return
		}
	}
}

func (t *Transport) emitDiagnostic() {
	if t.diagnosticCh == nil {
		return
	}
	t.mu.Lock()
	now := t.clock.Now()
	lastAuthenticatedPacket := t.lastAuthenticatedPacket
	lastCompleteRecord := t.lastCompleteRecord
	lastACKProgress := t.lastACKProgress
	pendingBytes := 0
	for _, p := range t.pending {
		if p != nil {
			pendingBytes += len(p.frame.Payload)
		}
	}
	state := t.linkState
	pendingRecords := len(t.pending)
	retransmits := t.retransmits
	reassemblyInflight := t.reassemblyInflight
	t.mu.Unlock()

	d := Diagnostic{
		At:                       now,
		State:                    state,
		SinceAuthenticatedPacket: diagnosticAge(now, lastAuthenticatedPacket),
		SinceCompleteRecord:      diagnosticAge(now, lastCompleteRecord),
		SinceACKProgress:         diagnosticAge(now, lastACKProgress),
		PendingRecords:           pendingRecords,
		PendingBytes:             pendingBytes,
		Retransmits:              retransmits,
		ReassemblyInflight:       reassemblyInflight,
	}
	select {
	case t.diagnosticCh <- d:
	default:
	}
}

func diagnosticAge(now, then time.Time) time.Duration {
	if then.After(now) {
		return 0
	}
	return now.Sub(then)
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
