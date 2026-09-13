// Package replaytest owns the protocol-neutral transport replay fixture and
// assertions shared by transport adapter tests. The transcript is a typed
// Output conversation carried as serialized Protobuf envelopes; adapters
// prove they preserve it byte-for-byte. The transcript stays value-only:
// envelopes are composed from generated wire types here, never through a
// concrete adapter.
package replaytest

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func mustEncodeOutput(m protocol.Output) []byte {
	m.Context = &protocol.ViewContext{
		Publication: m.New,
		Route:       protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "replay"}},
		TabID:       "tab-1", FocusedPaneID: "pane-1",
	}
	if err := protocol.ValidateOutput(m); err != nil {
		panic(err)
	}
	envelope := &wire.ServerEnvelope{Payload: &wire.ServerEnvelope_Output{Output: &wire.Output{
		Epoch:        m.Epoch,
		Base:         m.Base,
		NewState:     m.New,
		Echo:         m.Echo,
		ViewRevision: m.ViewRevision,
		Cols:         uint32(m.Size.Cols),
		Rows:         uint32(m.Size.Rows),
		Full:         m.Full,
		Context: &wire.ViewContext{Publication: m.Context.Publication, Route: &wire.CommittedRouteIdentity{
			Target: &wire.ExactTarget{
				LifecycleId: &wire.LifecycleID{Value: append([]byte(nil), m.Context.Route.Target.LifecycleID[:]...)},
				SessionName: m.Context.Route.Target.SessionName,
			},
			Ephemeral: m.Context.Route.Ephemeral,
		}, TabId: string(m.Context.TabID), FocusedPaneId: string(m.Context.FocusedPaneID)},
		Encoding:           0,
		UncompressedLength: uint64(len(m.Data)),
		Data:               append([]byte(nil), m.Data...),
	}}}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(envelope)
	if err != nil {
		panic(err)
	}
	return raw
}

var transcript = []wire.Envelope{
	{Payload: mustEncodeOutput(protocol.Output{Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 80, Rows: 24}, Full: true, Data: []byte("\x1b[2J\x1b[Hone\r\ntwo")})},
	{Payload: mustEncodeOutput(protocol.Output{Epoch: 1, Base: 1, New: 2, Echo: 7, Size: domain.Size{Cols: 80, Rows: 24}, Data: []byte("\x1b[2;1HTWO")})},
}

// Transcript returns a deep copy of the canonical replay transcript so an
// adapter cannot mutate the fixture observed by another adapter.
func Transcript() []wire.Envelope {
	return cloneEnvelopes(transcript)
}

// Exchange carries already-composed envelopes through one transport adapter.
type Exchange func(t *testing.T, envelopes []wire.Envelope) []wire.Envelope

// Run verifies that an adapter preserves the canonical transcript exactly.
func Run(t *testing.T, exchange Exchange) {
	t.Helper()
	want := Transcript()
	got := exchange(t, cloneEnvelopes(want))
	if len(got) != len(want) {
		t.Fatalf("replayed envelope count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Errorf("envelope %d payload differs in %d bytes, want byte-for-byte equality", i, len(want[i].Payload))
			continue
		}
		gotOutput, err := decodeOutput(got[i].Payload)
		if err != nil {
			t.Errorf("envelope %d output decode: %v", i, err)
			continue
		}
		wantOutput, err := decodeOutput(want[i].Payload)
		if err != nil {
			t.Fatalf("canonical envelope %d output decode: %v", i, err)
		}
		if !reflect.DeepEqual(gotOutput, wantOutput) {
			t.Errorf("envelope %d output = %#v, want %#v", i, gotOutput, wantOutput)
		}
	}
}

func decodeOutput(payload []byte) (protocol.Output, error) {
	envelope := &wire.ServerEnvelope{}
	if err := wire.ScanEnvelope(envelope, payload); err != nil {
		return protocol.Output{}, err
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, envelope)); err != nil {
		return protocol.Output{}, err
	}
	output, ok := envelope.Payload.(*wire.ServerEnvelope_Output)
	if !ok || output.Output == nil {
		return protocol.Output{}, &protocol.DecodeFailure{Category: protocol.DecodeWrongDirection, Err: errors.New("replaytest: expected Output envelope")}
	}
	decoded := protocol.Output{
		Epoch: output.Output.GetEpoch(), Base: output.Output.GetBase(), New: output.Output.GetNewState(),
		Echo: output.Output.GetEcho(), ViewRevision: output.Output.GetViewRevision(),
		Size: domain.Size{Cols: int(output.Output.GetCols()), Rows: int(output.Output.GetRows())},
		Full: output.Output.GetFull(), Data: append([]byte(nil), output.Output.GetData()...),
	}
	if context := output.Output.GetContext(); context != nil {
		route, err := committedRouteFromWire(context.GetRoute())
		if err != nil {
			return protocol.Output{}, fmt.Errorf("replaytest: invalid route: %w", err)
		}
		decoded.Context = &protocol.ViewContext{
			Publication:   context.GetPublication(),
			Route:         route,
			TabID:         domain.TabStableID(context.GetTabId()),
			FocusedPaneID: domain.PaneStableID(context.GetFocusedPaneId()),
		}
	}
	if err := protocol.ValidateOutput(decoded); err != nil {
		return protocol.Output{}, fmt.Errorf("replaytest: invalid output: %w", err)
	}
	return decoded, nil
}

func cloneEnvelopes(envelopes []wire.Envelope) []wire.Envelope {
	cloned := make([]wire.Envelope, len(envelopes))
	for i, envelope := range envelopes {
		cloned[i] = wire.Envelope{Payload: append([]byte(nil), envelope.Payload...)}
	}
	return cloned
}
