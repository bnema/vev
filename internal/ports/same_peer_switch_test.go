package ports

import (
	"bytes"
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func samePeerSwitchTarget() ExactSessionTarget {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 9
	return ExactSessionTarget{LifecycleID: lifecycle, SessionName: "work"}
}

func TestSamePeerSwitchWireStrict(t *testing.T) {
	request := SamePeerSwitchRequest{RequestID: 7, Target: samePeerSwitchTarget(), PreferredTabID: "tab-2"}
	payload, err := MarshalSamePeerSwitchRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte{0, 0, 0, 0, 0, 0, 0, 7, 9}, make([]byte, 15)...)
	want = append(want, 0, 4, 'w', 'o', 'r', 'k', 0, 5, 't', 'a', 'b', '-', '2')
	if !bytes.Equal(payload, want) {
		t.Fatalf("request payload = %x, want %x", payload, want)
	}
	decoded, err := UnmarshalSamePeerSwitchRequest(payload)
	if err != nil || decoded != request {
		t.Fatalf("request = %+v, error = %v, want %+v", decoded, err, request)
	}
	assertAllPrefixesFail(t, payload, UnmarshalSamePeerSwitchRequest)
	assertTrailingGarbageFails(t, payload, UnmarshalSamePeerSwitchRequest)

	failure := SamePeerSwitchFailure{RequestID: 7, Code: SamePeerSwitchStaleTarget}
	failurePayload, err := MarshalSamePeerSwitchFailure(failure)
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{0, 0, 0, 0, 0, 0, 0, 7, 1}; !bytes.Equal(failurePayload, want) {
		t.Fatalf("failure payload = %x, want %x", failurePayload, want)
	}
	decodedFailure, err := UnmarshalSamePeerSwitchFailure(failurePayload)
	if err != nil || decodedFailure != failure {
		t.Fatalf("failure = %+v, error = %v, want %+v", decodedFailure, err, failure)
	}
	assertAllPrefixesFail(t, failurePayload, UnmarshalSamePeerSwitchFailure)
	assertTrailingGarbageFails(t, failurePayload, UnmarshalSamePeerSwitchFailure)
}

func TestSamePeerSwitchWireRejectsInvalidValues(t *testing.T) {
	valid := SamePeerSwitchRequest{RequestID: 1, Target: samePeerSwitchTarget()}
	for _, request := range []SamePeerSwitchRequest{
		{},
		{RequestID: 1},
		{RequestID: 1, Target: samePeerSwitchTarget(), PreferredTabID: "bad tab"},
	} {
		if payload, err := MarshalSamePeerSwitchRequest(request); err == nil || payload != nil {
			t.Fatalf("MarshalSamePeerSwitchRequest(%+v) = %x, %v; want error", request, payload, err)
		}
	}
	payload, err := MarshalSamePeerSwitchRequest(valid)
	if err != nil {
		t.Fatal(err)
	}
	payload[7] = 0
	if _, err := UnmarshalSamePeerSwitchRequest(payload); err == nil {
		t.Fatal("UnmarshalSamePeerSwitchRequest accepted an invalid request ID")
	}
	if _, err := UnmarshalSamePeerSwitchFailure([]byte{0, 0, 0, 0, 0, 0, 0, 1, 99}); err == nil {
		t.Fatal("UnmarshalSamePeerSwitchFailure accepted an unknown code")
	}
}
