package dgram

import (
	"errors"
	"net"
	"slices"
	"time"

	pdgram "github.com/bnema/vev/pkg/dgram"
)

func (t *Transport) retransmitLoop() {
	resend := t.clock.NewTimer(t.resendAfter)
	defer resend.Stop()
	for {
		select {
		case <-resend.C():
			t.queueRetransmits()
			resend.Reset(t.resendAfter)
			t.notifyTimerHook(func() func() { return t.afterRetransmitTimer })
		case <-t.done:
			return
		}
	}
}

func (t *Transport) dataSendLoop() {
	pacer := bytePacer{clk: t.clock, afterTimer: func(deadline time.Time) {
		t.mu.Lock()
		hook := t.afterDataPaceWait
		t.mu.Unlock()
		if hook != nil {
			hook(deadline)
		}
	}}
	for {
		select {
		case job := <-t.dataSend:
			t.runDataSendJob(&pacer, job, true)
		case batch := <-t.retransmitWork:
			t.runRetransmitBatch(&pacer, batch)
		case <-t.done:
			return
		}
	}
}

func (t *Transport) runDataSendJob(pacer *bytePacer, job dataSendJob, initial bool) {
	if initial {
		t.markPendingSent(job.seq, job.reliable)
	}
	_, err := t.writeDataJob(pacer, job, nil)
	if err != nil {
		if initial {
			t.removePending(job.seq, job.reliable)
		} else {
			t.markPendingReady(job.seq, job.reliable)
		}
		if !errors.Is(err, errPacerClosed) {
			t.closeWithError(err)
		}
	} else {
		t.markPendingReady(job.seq, job.reliable)
	}
	t.mu.Lock()
	afterDataJob := t.afterDataJob
	t.mu.Unlock()
	if afterDataJob != nil {
		afterDataJob()
	}
	if job.done != nil {
		if errors.Is(err, errPacerClosed) {
			err = t.closedError()
		}
		job.done <- err
		close(job.done)
	}
}

func (t *Transport) writeDataJob(pacer *bytePacer, job dataSendJob, onFirstWrite func()) (bool, error) {
	if job.reliable && !t.pendingExists(job.seq) {
		return false, nil
	}
	payload := encodeData(job.seq, job.reliable, job.frame)
	frags, err := pdgram.FragmentPayload(t.nextCounter(), payload, t.mtu-pdgram.HeaderSize-t.codec.Overhead())
	if err != nil {
		return false, err
	}
	t.mu.Lock()
	beforeDataPace := t.beforeDataPace
	t.mu.Unlock()
	if beforeDataPace != nil {
		beforeDataPace()
	}
	limits := func() (int, int) {
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.congestion.bytesPerSecond(), t.congestion.burstBytes()
	}
	emitted := false
	wireBytes := 0
	for i, frag := range frags {
		if job.reliable && !t.pendingExists(job.seq) {
			return emitted, nil
		}
		raw, err := pdgram.MarshalFragment(frag)
		if err != nil {
			return emitted, err
		}
		pkt := t.codec.Seal(t.sendDir, t.nextCounter(), raw, nil)
		wireBytes += len(pkt)
		if job.reliable {
			t.mu.Lock()
			if pending := t.pending[job.seq]; pending != nil && pending.wireBytes < wireBytes {
				pending.wireBytes = wireBytes
			}
			t.mu.Unlock()
		}
		if err := pacer.wait(t.done, len(pkt), limits); err != nil {
			return emitted, err
		}
		if job.reliable && !t.pendingExists(job.seq) {
			return emitted, nil
		}
		t.mu.Lock()
		peer := t.peer
		closed := t.closed
		closeErr := t.closeErr
		t.mu.Unlock()
		if closed {
			if closeErr != nil {
				return emitted, closeErr
			}
			return emitted, net.ErrClosed
		}
		if peer == nil {
			return emitted, errors.New("dgram: no peer")
		}
		if i == len(frags)-1 {
			t.markPendingFinalWrite(job.seq)
		}
		if err := t.writePacket(pkt, peer); err != nil {
			return emitted, err
		}
		if !emitted {
			emitted = true
			if onFirstWrite != nil {
				onFirstWrite()
			}
		}
	}
	return emitted, nil
}

func (t *Transport) runRetransmitBatch(pacer *bytePacer, batch []retransmitRecord) {
	defer func() {
		t.mu.Lock()
		hook := t.afterDataJob
		t.mu.Unlock()
		if hook != nil {
			hook()
		}
	}()
	lossApplied := false
	onFirstWrite := func() {
		if lossApplied {
			return
		}
		t.mu.Lock()
		t.congestion.onLoss()
		t.mu.Unlock()
		lossApplied = true
	}
	for _, r := range batch {
		job := dataSendJob{seq: r.seq, reliable: true, frame: r.f}
		_, err := t.writeDataJob(pacer, job, onFirstWrite)
		if err == nil {
			t.markPendingReady(r.seq, true)
			continue
		}
		t.markPendingReady(r.seq, true)
		return
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

// resendPending remains a test hook for retransmission selection. Constructed
// transports transfer its batch to the sole data sender; small unit fixtures
// without transport goroutines execute the selected writes inline.
func (t *Transport) resendPending() {
	now := t.clock.Now()
	t.mu.Lock()
	resend, _ := t.selectRetransmitsLocked(now)
	t.mu.Unlock()
	if t.retransmitWork != nil {
		select {
		case t.retransmitWork <- resend:
		case <-t.done:
		}
		return
	}
	pacer := bytePacer{clk: t.clock}
	t.runRetransmitBatch(&pacer, resend)
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
	t.congestion.onRTT(t.srtt)
}
