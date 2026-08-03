// Package replaytest owns the protocol-neutral transport replay fixture and
// assertions shared by transport adapter tests.
package replaytest

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/bnema/vev/internal/ports"
)

var transcript = []ports.Frame{
	{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{Base: 0, New: 1, Data: []byte("\x1b[2J\x1b[Hone\r\ntwo")})},
	{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{Base: 1, New: 2, Echo: 7, Data: []byte("\x1b[2;1HTWO")})},
}

// Transcript returns a deep copy of the canonical replay transcript so an
// adapter cannot mutate the fixture observed by another adapter.
func Transcript() []ports.Frame {
	return cloneFrames(transcript)
}

// Exchange carries already-composed frames through one transport adapter.
type Exchange func(t *testing.T, frames []ports.Frame) []ports.Frame

// Run verifies that an adapter preserves the canonical transcript exactly.
func Run(t *testing.T, exchange Exchange) {
	t.Helper()
	want := Transcript()
	got := exchange(t, cloneFrames(want))
	if len(got) != len(want) {
		t.Fatalf("replayed frame count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Type != want[i].Type {
			t.Errorf("frame %d type = %d, want %d", i, got[i].Type, want[i].Type)
		}
		if !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Errorf("frame %d payload = %x, want byte-for-byte %x", i, got[i].Payload, want[i].Payload)
			continue
		}
		gotOutput, err := ports.UnmarshalOutput(got[i].Payload)
		if err != nil {
			t.Errorf("frame %d output decode: %v", i, err)
			continue
		}
		wantOutput, err := ports.UnmarshalOutput(want[i].Payload)
		if err != nil {
			t.Fatalf("canonical frame %d output decode: %v", i, err)
		}
		if !reflect.DeepEqual(gotOutput, wantOutput) {
			t.Errorf("frame %d output = %#v, want %#v", i, gotOutput, wantOutput)
		}
	}
}

func cloneFrames(frames []ports.Frame) []ports.Frame {
	cloned := make([]ports.Frame, len(frames))
	for i, frame := range frames {
		cloned[i] = ports.Frame{Type: frame.Type, Payload: append([]byte(nil), frame.Payload...)}
	}
	return cloned
}
