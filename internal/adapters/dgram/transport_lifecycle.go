package dgram

import (
	"errors"
	"net"
	"sync"

	"github.com/bnema/vev/internal/ports"
	pdgram "github.com/bnema/vev/pkg/dgram"
)

func NewTransport(pc net.PacketConn, peer net.Addr, key []byte, sendDir, recvDir uint32, runtimeOpts ...RuntimeOption) (*Transport, error) {
	return NewTransportWithOptions(pc, peer, key, sendDir, recvDir, Options{}, runtimeOpts...)
}

func NewTransportWithOptions(pc net.PacketConn, peer net.Addr, key []byte, sendDir, recvDir uint32, opts Options, runtimeOpts ...RuntimeOption) (*Transport, error) {
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
		health:                  newHealthTracker(opts.Clock.Now()),
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
		congestion:              newCongestionController(opts.MTU),
		linkState:               ports.LinkStateConnected,
		linkEvents:              make(chan ports.LinkEvent, linkEventBufferSize),
		probeWait:               make(map[uint64]chan struct{}),
		rebind:                  opts.RebindPacketConn,
		nextRecvSeq:             1,
		recvBuf:                 make(map[uint64]ports.Frame),
	}
	for _, opt := range runtimeOpts {
		if opt != nil {
			opt(t)
		}
	}
	t.sendWake = make(chan struct{})
	t.control = make(chan controlRecord, controlQueueSize)
	t.ackWake = make(chan struct{}, 1)
	t.ackSend = make(chan uint64, 1)
	t.outputWake = make(chan struct{})
	t.dataSend = make(chan dataSendJob, opts.MaxPending)
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
	go t.dataSendLoop()
	go t.deliveryLoop()
	go t.controlLoop()
	go t.ackSendLoop()
	go t.outputPaceLoop()
	t.emitDiagnostic()
	return t, nil
}

func (t *Transport) Recv() (ports.Frame, error) {
	end := t.beginRuntimeOperation(ports.RuntimeAdapterReceiveStart, 0)
	select {
	case f := <-t.in:
		end(true)
		return f, nil
	case <-t.done:
		t.mu.Lock()
		err := t.closeErr
		t.mu.Unlock()
		if err != nil {
			end(false)
			return ports.Frame{}, err
		}
		err = errors.New("dgram: closed")
		end(false)
		return ports.Frame{}, err
	}
}

func (t *Transport) Close() error {
	done := t.beginShutdown()
	t.closeWithError(errors.New("dgram: closed"))
	t.mu.Lock()
	pc := t.pc
	t.mu.Unlock()
	err := pc.Close()
	if done != nil {
		<-done
	}
	return err
}

func (t *Transport) Peer() net.Addr { t.mu.Lock(); defer t.mu.Unlock(); return t.peer }

func (t *Transport) LinkState() ports.LinkState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.linkState
}

func (t *Transport) LinkEvents() <-chan ports.LinkEvent { return t.linkEvents }

func (t *Transport) closedError() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closeErr != nil {
		return t.closeErr
	}
	return errors.New("dgram: closed")
}

func (t *Transport) closeWithError(err error) {
	t.mu.Lock()
	closed := t.closeWithErrorLocked(err)
	t.mu.Unlock()
	if closed {
		t.broadcastClosed()
	}
}

func (t *Transport) closeWithErrorLocked(err error) bool {
	if t.closed {
		return false
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
	return true
}

func (t *Transport) broadcastClosed() {
	t.deliverMu.Lock()
	t.deliverCond.Broadcast()
	t.deliverMu.Unlock()
}
