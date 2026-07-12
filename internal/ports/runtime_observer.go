package ports

// RuntimeMarkSchema is the version of the process-local performance trace.
const RuntimeMarkSchema uint16 = 1

// RuntimeMarkKind identifies a process-local runtime boundary. It deliberately
// contains no protocol payload, cells, or rendering policy.
type RuntimeMarkKind string

const (
	RuntimeInputInjected       RuntimeMarkKind = "input_injected"
	RuntimeResizeRequested     RuntimeMarkKind = "resize_requested"
	RuntimeResizeCommitted     RuntimeMarkKind = "resize_committed"
	RuntimeCaptureStart        RuntimeMarkKind = "capture_start"
	RuntimeCaptureEnd          RuntimeMarkKind = "capture_end"
	RuntimeComposeStart        RuntimeMarkKind = "compose_start"
	RuntimeComposeEnd          RuntimeMarkKind = "compose_end"
	RuntimeDiffStart           RuntimeMarkKind = "diff_start"
	RuntimeDiffEnd             RuntimeMarkKind = "diff_end"
	RuntimeQueueEnqueued       RuntimeMarkKind = "queue_enqueued"
	RuntimeQueueDequeued       RuntimeMarkKind = "queue_dequeued"
	RuntimeACKBlockedStart     RuntimeMarkKind = "ack_blocked_start"
	RuntimeACKBlockedEnd       RuntimeMarkKind = "ack_blocked_end"
	RuntimeEmitStart           RuntimeMarkKind = "emit_start"
	RuntimeEmitEnd             RuntimeMarkKind = "emit_end"
	RuntimeAdapterSendStart    RuntimeMarkKind = "adapter_send_start"
	RuntimeAdapterSendEnd      RuntimeMarkKind = "adapter_send_end"
	RuntimeAdapterReceiveStart RuntimeMarkKind = "adapter_receive_start"
	RuntimeAdapterReceiveEnd   RuntimeMarkKind = "adapter_receive_end"
	RuntimeTerminalFlushed     RuntimeMarkKind = "terminal_flushed"
	RuntimeTransportDiagnostic RuntimeMarkKind = "transport_diagnostic"
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
		RuntimeAdapterReceiveEnd, RuntimeTerminalFlushed, RuntimeTransportDiagnostic:
		return true
	default:
		return false
	}
}

// NewRuntimeMark returns a timestamp-free mark for lightweight opt-in hooks.
// Callers that need a span pass the same correlation fields to both endpoints.
func NewRuntimeMark(component string, kind RuntimeMarkKind, bytes uint64, valid bool) RuntimeMark {
	return RuntimeMark{Schema: RuntimeMarkSchema, Component: component, Scenario: "runtime", Run: 1, Sequence: 1, RequestID: 1, Epoch: 1, Kind: kind, Bytes: bytes, Valid: valid}
}
