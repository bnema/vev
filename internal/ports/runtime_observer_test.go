package ports

import (
	"reflect"
	"testing"
)

func TestRuntimeObserverContract(t *testing.T) {
	wantFields := []struct {
		name string
		typ  reflect.Type
	}{
		{"Schema", reflect.TypeFor[uint16]()},
		{"ProcessID", reflect.TypeFor[string]()},
		{"Component", reflect.TypeFor[string]()},
		{"Scenario", reflect.TypeFor[string]()},
		{"Run", reflect.TypeFor[uint64]()},
		{"Sequence", reflect.TypeFor[uint64]()},
		{"RequestID", reflect.TypeFor[uint64]()},
		{"Epoch", reflect.TypeFor[uint64]()},
		{"Kind", reflect.TypeFor[RuntimeMarkKind]()},
		{"Tick", reflect.TypeFor[int64]()},
		{"Bytes", reflect.TypeFor[uint64]()},
		{"Fragments", reflect.TypeFor[uint64]()},
		{"Retransmits", reflect.TypeFor[uint64]()},
		{"Pending", reflect.TypeFor[uint64]()},
		{"AckRTTNanos", reflect.TypeFor[int64]()},
		{"Valid", reflect.TypeFor[bool]()},
	}

	typ := reflect.TypeFor[RuntimeMark]()
	if typ.NumField() != len(wantFields) {
		t.Fatalf("RuntimeMark has %d fields, want exactly %d", typ.NumField(), len(wantFields))
	}
	for i, want := range wantFields {
		field := typ.Field(i)
		if field.Name != want.name || field.Type != want.typ {
			t.Fatalf("RuntimeMark field %d = %s %v, want %s %v", i, field.Name, field.Type, want.name, want.typ)
		}
	}

	var _ RuntimeObserver = runtimeObserverFunc(func(RuntimeMark) {})
}

func TestRuntimeObserverRequiredSpanKinds(t *testing.T) {
	pairs := []struct {
		start RuntimeMarkKind
		end   RuntimeMarkKind
	}{
		{RuntimeDiffStart, RuntimeDiffEnd},
		{RuntimeQueueEnqueued, RuntimeQueueDequeued},
		{RuntimeACKBlockedStart, RuntimeACKBlockedEnd},
		{RuntimeAdapterSendStart, RuntimeAdapterSendEnd},
		{RuntimeAdapterReceiveStart, RuntimeAdapterReceiveEnd},
	}
	for _, pair := range pairs {
		if pair.start == "" || pair.end == "" || pair.start == pair.end {
			t.Fatalf("invalid required span mapping %#v", pair)
		}
	}
}

func TestRuntimeMarkHasNoRenderingPayloadOrPolicyFields(t *testing.T) {
	typ := reflect.TypeFor[RuntimeMark]()
	for _, forbidden := range []string{"Cell", "Cells", "Payload", "Frame", "Renderer", "Policy", "Transport"} {
		if _, ok := typ.FieldByName(forbidden); ok {
			t.Fatalf("RuntimeMark must not carry %s", forbidden)
		}
	}
}

type runtimeObserverFunc func(RuntimeMark)

func (f runtimeObserverFunc) ObserveRuntime(mark RuntimeMark) { f(mark) }
