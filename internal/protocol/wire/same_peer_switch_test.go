package wire

import (
	"bytes"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func samePeerSwitchTarget() protocol.ExactSessionTarget {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 9
	return protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "work"}
}

func TestSamePeerSwitchWireStrict(t *testing.T) {
	request := protocol.SamePeerSwitchRequest{RequestID: 7, Target: samePeerSwitchTarget(), PreferredTabID: "tab-2"}
	requestWire := append([]byte{0, 0, 0, 0, 0, 0, 0, 7, 9}, make([]byte, 15)...)
	requestWire = append(requestWire, 0, 4, 'w', 'o', 'r', 'k', 0, 5, 't', 'a', 'b', '-', '2')
	failure := protocol.SamePeerSwitchFailure{RequestID: 7, Code: protocol.SamePeerSwitchStaleTarget}

	for _, tt := range []struct {
		name    string
		want    []byte
		marshal func() ([]byte, error)
		decode  func([]byte) (any, error)
		assert  func(*testing.T, any)
	}{
		{
			name:    "request",
			want:    requestWire,
			marshal: func() ([]byte, error) { return MarshalSamePeerSwitchRequest(request) },
			decode:  func(payload []byte) (any, error) { return UnmarshalSamePeerSwitchRequest(payload) },
			assert: func(t *testing.T, decoded any) {
				t.Helper()
				got, ok := decoded.(protocol.SamePeerSwitchRequest)
				if !ok {
					t.Fatalf("decoded type = %T, want protocol.SamePeerSwitchRequest", decoded)
				}
				if got != request {
					t.Fatalf("request = %+v, want %+v", got, request)
				}
			},
		},
		{
			name:    "failure",
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 7, 1},
			marshal: func() ([]byte, error) { return MarshalSamePeerSwitchFailure(failure) },
			decode:  func(payload []byte) (any, error) { return UnmarshalSamePeerSwitchFailure(payload) },
			assert: func(t *testing.T, decoded any) {
				t.Helper()
				got, ok := decoded.(protocol.SamePeerSwitchFailure)
				if !ok {
					t.Fatalf("decoded type = %T, want protocol.SamePeerSwitchFailure", decoded)
				}
				if got != failure {
					t.Fatalf("failure = %+v, want %+v", got, failure)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := tt.marshal()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(payload, tt.want) {
				t.Fatalf("payload = %x, want %x", payload, tt.want)
			}
			decoded, err := tt.decode(payload)
			if err != nil {
				t.Fatal(err)
			}
			tt.assert(t, decoded)
			assertAllPrefixesFail(t, payload, tt.decode)
			assertTrailingGarbageFails(t, payload, tt.decode)
		})
	}
}

func TestSamePeerSwitchWireRejectsInvalidValues(t *testing.T) {
	valid := protocol.SamePeerSwitchRequest{RequestID: 1, Target: samePeerSwitchTarget()}
	for _, request := range []protocol.SamePeerSwitchRequest{
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
