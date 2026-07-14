// Package dgram adapts authenticated UDP-style packet connections to ports.Transport.
package dgram

import (
	"context"
	"errors"
	"net"
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
	defaultMaxPendingWait   = 50 * time.Millisecond
	defaultMaxRecvBuf       = 1024
	dataRecordHeaderSize    = 12
	maxRecvBuffer           = defaultMaxRecvBuf
	linkEventBufferSize     = 16
	probeReplyBufferSize    = controlQueueSize // legacy test burst bound
)

var (
	ErrPendingFull = errors.New("dgram: pending reliable queue full")
	ErrLinkDead    = errors.New("dgram: link dead")
	errControlFull = errors.New("dgram: control queue full")
)

type RuntimeOption func(*Transport)

// WithRuntimeObserver enables process-local marks without changing dgram's
// behavior clock or carriage policy.
func WithRuntimeObserver(observer ports.SerializedRuntimeObserver) RuntimeOption {
	return func(t *Transport) { t.runtimeObserver = observer }
}

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

var _ ports.DatagramTransport = (*Transport)(nil)

type Transport struct {
	pc               net.PacketConn
	codec            *pdgram.Codec
	sendDir, recvDir uint32
	mtu              int

	mu             sync.Mutex
	peer           net.Addr
	ctr            uint64
	seq            uint64
	probeSeq       uint64
	pending        map[uint64]*pending
	closed         bool
	closeErr       error
	health         healthTracker
	peerCounter    uint64
	peerCounterSet bool
	// These mirrors preserve the established diagnostic snapshot fields while
	// health owns link-state decisions.
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
	// afterHealthDecision is a test synchronization hook. It runs after a health
	// snapshot is taken and before that decision is conditionally committed.
	afterHealthDecision func()
	// afterLinkStateCommit is a test synchronization hook. It runs after state
	// mutation and before ordered event publication.
	afterLinkStateCommit func(ports.LinkState)
	observe              DiagnosticObserver
	runtimeObserver      ports.SerializedRuntimeObserver
	operationMu          sync.Mutex
	operationCount       int
	closing              bool
	operationsDone       chan struct{}
	diagnosticCh         chan Diagnostic
	heartbeat            time.Duration
	resendAfter          time.Duration
	maxResendAfter       time.Duration
	maxResendPerTick     int
	srtt                 time.Duration
	rttvar               time.Duration
	rto                  time.Duration
	writeTimeout         time.Duration
	degradedAfter        time.Duration
	probeAfter           time.Duration
	offlineAfter         time.Duration
	deadAfter            time.Duration
	maxPending           int
	maxPendingWait       time.Duration
	maxRecvBuffer        int
	clock                ports.Clock
	congestion           congestionController
	linkState            ports.LinkState
	linkStateGeneration  uint64
	linkEvents           chan ports.LinkEvent
	linkEventMu          sync.Mutex
	probeWait            map[uint64]chan struct{}
	sendWake             chan struct{}
	control              chan controlRecord
	ackWake              chan struct{}
	ackSend              chan uint64
	controlMu            sync.Mutex
	ackSeq               uint64
	ackQueued            bool
	rebind               func(net.PacketConn) (net.PacketConn, error)
	hoppedOffline        bool
	hopGeneration        uint64
	outputQueue          []queuedSend
	outputWake           chan struct{}
	dataSend             chan dataSendJob
	retransmitWork       chan []retransmitRecord
	writeDeadlines       *writeDeadlineState

	recvMu sync.Mutex
	replay *pdgram.ReplayWindow
	reasm  *pdgram.Reassembler
	in     chan ports.Frame
	done   chan struct{}

	writeMu sync.Mutex
	// beforeDataPace is a test synchronization hook. It is read under mu.
	beforeDataPace func()
	// afterDataPaceWait observes a data pacer's next manual-clock deadline.
	// Test observers must be non-blocking.
	afterDataPaceWait func(time.Time)
	// afterDataJob observes completion of a data or retransmit send job. Test
	// observers must be non-blocking.
	afterDataJob func()
	// Timer hooks are test-only completion notifications. Observers must be
	// non-blocking so timer loops never depend on test scheduling.
	afterRetransmitTimer func()
	afterHealthTimer     func()
	afterHeartbeatTimer  func()
	// afterPacketProcessed is a test synchronization hook called after an
	// authenticated packet has been fully processed. It is read under mu.
	afterPacketProcessed func()
	// ACK hooks are test-only and observers must be non-blocking.
	afterACKWakeAccepted func()
	afterACKScheduled    func()
	afterACKDispatched   func()
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
	wireBytes       int
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

func (t *Transport) pendingExists(seq uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.pending[seq]
	return ok
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
			health.Reset(t.resendAfter)
			t.notifyTimerHook(func() func() { return t.afterHealthTimer })
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
			t.notifyTimerHook(func() func() { return t.afterHeartbeatTimer })
		case <-t.done:
			return
		}
	}
}

func (t *Transport) notifyTimerHook(get func() func()) {
	t.mu.Lock()
	hook := get()
	t.mu.Unlock()
	if hook != nil {
		hook()
	}
}

func (t *Transport) hopPacketConnOnce(generation uint64) {
	t.mu.Lock()
	if t.rebind == nil || t.hoppedOffline || t.closed || t.linkState != ports.LinkStateProbing || t.health.generation != generation {
		t.mu.Unlock()
		return
	}
	old := t.pc
	rebind := t.rebind
	t.hoppedOffline = true
	t.hopGeneration = generation
	t.mu.Unlock()
	pc, err := rebind(old)

	// Serialize the swap with writes. writePacket takes writeMu before mu, so
	// preserve that order here while retiring old.
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	t.mu.Lock()
	if err != nil {
		t.rollbackHopLocked(generation)
		t.mu.Unlock()
		return
	}
	if t.closed || t.pc != old || t.health.generation != generation || t.linkState != ports.LinkStateProbing {
		t.rollbackHopLocked(generation)
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

func (t *Transport) rollbackHopLocked(generation uint64) {
	if t.hoppedOffline && t.hopGeneration == generation {
		t.hoppedOffline = false
		t.hopGeneration = 0
	}
}

func (t *Transport) checkSilence() {
	runHook := true
	for {
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return
		}
		now := t.clock.Now()
		generation := t.health.generation
		state, hop, dead := t.health.decide(now, len(t.pending) > 0, t.degradedAfter, t.probeAfter, t.offlineAfter, t.deadAfter)
		afterDecision := t.afterHealthDecision
		t.mu.Unlock()

		if runHook {
			runHook = false
			if afterDecision != nil {
				afterDecision()
			}
		}

		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return
		}
		if t.health.generation != generation {
			t.mu.Unlock()
			continue
		}
		changed, stateGeneration := t.setLinkStateLocked(state)
		afterStateCommit := t.afterLinkStateCommit
		closed := false
		if dead {
			closed = t.closeWithErrorLocked(ErrLinkDead)
		}
		t.mu.Unlock()

		if changed {
			if afterStateCommit != nil {
				afterStateCommit(state)
			}
			var linkErr error
			if dead {
				linkErr = ErrLinkDead
			}
			t.publishLinkState(state, now, linkErr, stateGeneration)
		}
		if closed {
			t.broadcastClosed()
			return
		}
		if hop {
			t.hopPacketConnOnce(generation)
		}
		return
	}
}

func (t *Transport) setLinkState(state ports.LinkState, err error) {
	now := t.clock.Now()
	t.mu.Lock()
	changed, stateGeneration := t.setLinkStateLocked(state)
	afterStateCommit := t.afterLinkStateCommit
	t.mu.Unlock()
	if changed {
		if afterStateCommit != nil {
			afterStateCommit(state)
		}
		t.publishLinkState(state, now, err, stateGeneration)
	}
}

func (t *Transport) setLinkStateLocked(state ports.LinkState) (bool, uint64) {
	if t.linkState == state {
		return false, t.linkStateGeneration
	}
	t.linkState = state
	t.linkStateGeneration++
	if state == ports.LinkStateConnected {
		t.hoppedOffline = false
		t.hopGeneration = 0
		for _, p := range t.pending {
			if p != nil {
				p.attempts = 0
			}
		}
	}
	t.notifySendWaitersLocked()
	return true, t.linkStateGeneration
}

func (t *Transport) publishLinkState(state ports.LinkState, now time.Time, err error, generation uint64) {
	t.linkEventMu.Lock()
	defer t.linkEventMu.Unlock()
	t.mu.Lock()
	if t.linkState != state || t.linkStateGeneration != generation {
		t.mu.Unlock()
		return
	}
	event := ports.LinkEvent{State: state, At: now, Err: err}
	select {
	case t.linkEvents <- event:
	default:
	}
	t.mu.Unlock()
	t.emitDiagnostic()
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
	d := t.diagnosticSnapshot()
	select {
	case t.diagnosticCh <- d:
	default:
	}
}

func (t *Transport) diagnosticSnapshot() Diagnostic {
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

	return Diagnostic{
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
}

func diagnosticAge(now, then time.Time) time.Duration {
	if then.After(now) {
		return 0
	}
	return now.Sub(then)
}

// DatagramTransport marks Transport as a UDP-style datagram transport.
func (t *Transport) beginRuntimeOperation(start ports.RuntimeMarkKind, bytes uint64) func(bool) {
	if t.runtimeObserver == nil {
		return func(bool) {}
	}
	if !t.beginObservedOperation() {
		return func(bool) {}
	}
	correlation := ports.NewRuntimeCorrelation()
	t.runtimeObserver.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("dgram", correlation, start, bytes, true))
	end := ports.RuntimeAdapterSendEnd
	if start == ports.RuntimeAdapterReceiveStart {
		end = ports.RuntimeAdapterReceiveEnd
	}
	return func(valid bool) {
		defer t.finishObservedOperation()
		t.runtimeObserver.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("dgram", correlation, end, bytes, valid))
	}
}

func (t *Transport) beginObservedOperation() bool {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	if t.closing {
		return false
	}
	if t.operationCount == 0 {
		t.operationsDone = make(chan struct{})
	}
	t.operationCount++
	return true
}

func (t *Transport) finishObservedOperation() {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	t.operationCount--
	if t.operationCount == 0 {
		close(t.operationsDone)
	}
}

func (t *Transport) beginShutdown() <-chan struct{} {
	t.operationMu.Lock()
	defer t.operationMu.Unlock()
	t.closing = true
	if t.operationCount == 0 {
		return nil
	}
	return t.operationsDone
}

func (*Transport) DatagramTransport() {}
