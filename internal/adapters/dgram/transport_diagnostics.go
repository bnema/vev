package dgram

import (
	"time"

	"github.com/bnema/vev/internal/ports"
)

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
