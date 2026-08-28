package ports

import (
	"errors"
	"sync/atomic"
)

// RuntimeMarkSchema is the version of the process-local performance trace.
const RuntimeMarkSchema uint16 = 1

// RuntimeMarkKind identifies a process-local runtime boundary. It deliberately
// contains no protocol payload, cells, or rendering policy.
type RuntimeMarkKind string

const (
	RuntimeInputInjected        RuntimeMarkKind = "input_injected"
	RuntimeResizeRequested      RuntimeMarkKind = "resize_requested"
	RuntimeResizeCommitted      RuntimeMarkKind = "resize_committed"
	RuntimeCaptureStart         RuntimeMarkKind = "capture_start"
	RuntimeCaptureEnd           RuntimeMarkKind = "capture_end"
	RuntimeComposeStart         RuntimeMarkKind = "compose_start"
	RuntimeComposeEnd           RuntimeMarkKind = "compose_end"
	RuntimeDiffStart            RuntimeMarkKind = "diff_start"
	RuntimeDiffEnd              RuntimeMarkKind = "diff_end"
	RuntimeQueueEnqueued        RuntimeMarkKind = "queue_enqueued"
	RuntimeQueueDequeued        RuntimeMarkKind = "queue_dequeued"
	RuntimeACKBlockedStart      RuntimeMarkKind = "ack_blocked_start"
	RuntimeACKBlockedEnd        RuntimeMarkKind = "ack_blocked_end"
	RuntimeEmitStart            RuntimeMarkKind = "emit_start"
	RuntimeEmitEnd              RuntimeMarkKind = "emit_end"
	RuntimeAdapterSendStart     RuntimeMarkKind = "adapter_send_start"
	RuntimeAdapterSendEnd       RuntimeMarkKind = "adapter_send_end"
	RuntimeAdapterReceiveStart  RuntimeMarkKind = "adapter_receive_start"
	RuntimeAdapterReceiveEnd    RuntimeMarkKind = "adapter_receive_end"
	RuntimeTerminalFlushed      RuntimeMarkKind = "terminal_flushed"
	RuntimeScreenSnapshot       RuntimeMarkKind = "screen_snapshot"
	RuntimeScreenDelta          RuntimeMarkKind = "screen_delta"
	RuntimeScreenResetRequested RuntimeMarkKind = "screen_reset_requested"
	RuntimeTransportDiagnostic  RuntimeMarkKind = "transport_diagnostic"
)

// RuntimeMark is a process-local observation record. Tick is intentionally
// left zero by producers: the concrete process observer owns timestamping.
type RuntimeMark struct {
	Schema      uint16
	ProcessID   string
	Component   string
	Scenario    string
	Run         uint64
	Sequence    uint64
	RequestID   uint64
	Epoch       uint64
	Kind        RuntimeMarkKind
	Tick        int64
	Bytes       uint64
	Fragments   uint64
	Retransmits uint64
	Pending     uint64
	AckRTTNanos int64
	Valid       bool
}

// RuntimeObserver receives process-local marks. Implementations must not let
// a producer provide a timestamp in another clock domain.
type RuntimeObserver interface{ ObserveRuntime(RuntimeMark) }

// SerializedRuntimeObserver is the only observer contract accepted by runtime
// transport and use-case hot paths. ObserveRuntime must return without waiting
// for sink I/O. Flush and Close wait until every accepted mark has reached the
// concrete observer; Close is idempotent and rejects later marks.
type SerializedRuntimeObserver interface {
	RuntimeObserver
	Flush()
	Close()
}

// RuntimeMarkValid reports whether a mark has all correlation fields required
// by the JSONL schema. Tick is checked separately by the timestamp owner.
func RuntimeMarkValid(mark RuntimeMark) bool {
	if mark.Schema != RuntimeMarkSchema || mark.Component == "" || mark.Scenario == "" || mark.Run == 0 || mark.Sequence == 0 || mark.RequestID == 0 || mark.Epoch == 0 {
		return false
	}
	switch mark.Kind {
	case RuntimeInputInjected, RuntimeResizeRequested, RuntimeResizeCommitted,
		RuntimeCaptureStart, RuntimeCaptureEnd, RuntimeComposeStart, RuntimeComposeEnd,
		RuntimeDiffStart, RuntimeDiffEnd, RuntimeQueueEnqueued, RuntimeQueueDequeued,
		RuntimeACKBlockedStart, RuntimeACKBlockedEnd, RuntimeEmitStart, RuntimeEmitEnd,
		RuntimeAdapterSendStart, RuntimeAdapterSendEnd, RuntimeAdapterReceiveStart,
		RuntimeAdapterReceiveEnd, RuntimeTerminalFlushed, RuntimeScreenSnapshot,
		RuntimeScreenDelta, RuntimeScreenResetRequested, RuntimeTransportDiagnostic:
		return true
	default:
		return false
	}
}

// RuntimeCorrelation identifies one attempted operation. It is deliberately
// copied to both endpoints of a span; process ID remains owned by the sink.
type RuntimeCorrelation struct {
	Scenario                        string
	Run, Sequence, RequestID, Epoch uint64
}

// RuntimeCorrelationInputs are supplied once by process orchestration. They
// are deliberately data-only: consumer APIs continue to receive only their
// one RuntimeObserver argument and never a clock or tick.
type RuntimeCorrelationInputs struct {
	Scenario string
	Run      uint64
}

// NewRuntimeCorrelationObserver binds every mark from one OS process to its
// harness manifest scenario/run. Operation IDs remain producer-owned when
// supplied; missing IDs are assigned from the process-wide allocator.
func NewRuntimeCorrelationObserver(observer RuntimeObserver, inputs RuntimeCorrelationInputs) (RuntimeObserver, error) {
	if observer == nil || inputs.Scenario == "" || inputs.Run == 0 {
		return nil, errors.New("invalid runtime correlation inputs")
	}
	return &runtimeCorrelationObserver{observer: observer, inputs: inputs}, nil
}

type runtimeCorrelationObserver struct {
	observer RuntimeObserver
	inputs   RuntimeCorrelationInputs
}

func (o *runtimeCorrelationObserver) ObserveRuntime(mark RuntimeMark) {
	if o == nil || o.observer == nil {
		return
	}
	mark.Scenario, mark.Run = o.inputs.Scenario, o.inputs.Run
	if mark.Sequence == 0 {
		mark.Sequence = runtimeMarkSequence.Add(1)
	}
	if mark.RequestID == 0 {
		mark.RequestID = mark.Sequence
	}
	if mark.Epoch == 0 {
		mark.Epoch = mark.RequestID
	}
	o.observer.ObserveRuntime(mark)
}

var runtimeMarkSequence atomic.Uint64

// NewRuntimeCorrelation makes a unique, process-local operation identity.
// Components with a domain request/epoch should use those values directly via
// NewRuntimeMarkWithCorrelation instead of inventing a second identity.
func NewRuntimeCorrelation() RuntimeCorrelation {
	sequence := runtimeMarkSequence.Add(1)
	return RuntimeCorrelation{Scenario: "runtime", Run: 1, Sequence: sequence, RequestID: sequence, Epoch: sequence}
}

// NewRuntimeMarkWithCorrelation returns a timestamp-free mark carrying the
// supplied operation identity. Start/end callers must reuse correlation.
func NewRuntimeMarkWithCorrelation(component string, correlation RuntimeCorrelation, kind RuntimeMarkKind, bytes uint64, valid bool) RuntimeMark {
	return RuntimeMark{Schema: RuntimeMarkSchema, Component: component, Scenario: correlation.Scenario, Run: correlation.Run, Sequence: correlation.Sequence, RequestID: correlation.RequestID, Epoch: correlation.Epoch, Kind: kind, Bytes: bytes, Valid: valid}
}

// NewRuntimeMark is retained for one-shot marks. Unlike the old helper it
// never returns constant correlation IDs.
func NewRuntimeMark(component string, kind RuntimeMarkKind, bytes uint64, valid bool) RuntimeMark {
	return NewRuntimeMarkWithCorrelation(component, NewRuntimeCorrelation(), kind, bytes, valid)
}
