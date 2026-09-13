package wire

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// Baseline Protobuf envelope benchmarks frozen at the P3.3 cutover. They
// exercise representative control, Hello, Output, and image envelopes so
// AC8 can compare equivalent QUIC-carried microbenchmarks after P6. The
// recorded V0 legacy-codec medians live in docs/protocol.md; these
// benchmarks measure the same conversations through the new envelopes.

func baselineHello() protocol.Hello {
	return protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach,
		ClientID:          [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Name:              "work",
		Size:              domain.Size{Cols: 120, Rows: 40},
		TermEnv:           "xterm-256color",
		Cwd:               "/tmp/project",
		TrueColor:         true,
		MaxOutputInFlight: 8,
	}
}

func baselineViewContext() *protocol.ViewContext {
	return &protocol.ViewContext{
		Publication: 1,
		Route:       protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}},
		TabID:       "tab-1", FocusedPaneID: "pane-1",
	}
}

func BenchmarkHelloEnvelope(b *testing.B) {
	hello := baselineHello()
	converted := &Hello{
		Version:           uint32(hello.Version),
		Intent:            uint32(hello.Intent),
		ClientId:          append([]byte(nil), hello.ClientID[:]...),
		Name:              hello.Name,
		Cols:              uint32(hello.Size.Cols),
		Rows:              uint32(hello.Size.Rows),
		TermEnv:           hello.TermEnv,
		Cwd:               hello.Cwd,
		TrueColor:         hello.TrueColor,
		MaxOutputInFlight: uint32(hello.MaxOutputInFlight),
	}
	b.ReportAllocs()
	for range b.N {
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&ClientEnvelope{
			Payload: &ClientEnvelope_Hello{Hello: converted},
		})
		if payload == nil || err != nil {
			b.Fatalf("marshal hello: %v", err)
		}
	}
}

func BenchmarkHelloEnvelopeUnmarshal(b *testing.B) {
	converted := &Hello{Version: uint32(protocol.Version), Intent: uint32(protocol.IntentAttach)}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&ClientEnvelope{
		Payload: &ClientEnvelope_Hello{Hello: converted},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for range b.N {
		decoded := &ClientEnvelope{}
		if err := ScanEnvelope(decoded, payload); err != nil {
			b.Fatal(err)
		}
		if uerr := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, decoded)); uerr != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInputEnvelopeRoundTrip(b *testing.B) {
	data := []byte("ls -la\r")
	converted := &Input{InputSeq: 1, ActionId: 2, Data: data}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for range b.N {
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&ClientEnvelope{
			Payload: &ClientEnvelope_Input{Input: converted},
		})
		if err != nil {
			b.Fatal(err)
		}
		decoded := &ClientEnvelope{}
		if err := ScanEnvelope(decoded, payload); err != nil {
			b.Fatal(err)
		}
		if uerr := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, decoded)); uerr != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkImageEnvelopeRoundTrip(b *testing.B) {
	data := bytes.Repeat([]byte{0x89, 0x50, 0x4e, 0x47}, 4096)
	converted := &ImagePush{InputSeq: 1, Mime: "image/png", Data: data}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&ClientEnvelope{
		Payload: &ClientEnvelope_ImagePush{ImagePush: converted},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for range b.N {
		decoded := &ClientEnvelope{}
		if err := ScanEnvelope(decoded, payload); err != nil {
			b.Fatal(err)
		}
		if uerr := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, decoded)); uerr != nil {
			b.Fatal(err)
		}
	}
}

func baselineOutputData(compressible bool) []byte {
	if compressible {
		return bytes.Repeat([]byte("\x1b[38;2;120;180;240mstyled viewport row\x1b[0m\r\n"), 128)
	}
	return bytes.Repeat([]byte("0123456789abcdef"), 512)
}

func baselineOutputEnvelope(compressible bool) []byte {
	context := baselineViewContext()
	converted := &Output{
		Epoch: 1, NewState: 1, Cols: 120, Rows: 40, Full: true,
		Context: &ViewContext{
			Publication:   context.Publication,
			Route:         &CommittedRouteIdentity{Target: &ExactTarget{SessionName: "work"}},
			TabId:         string(context.TabID),
			FocusedPaneId: string(context.FocusedPaneID),
		},
		Encoding:           0,
		UncompressedLength: uint64(len(baselineOutputData(compressible))),
		Data:               baselineOutputData(compressible),
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&ServerEnvelope{
		Payload: &ServerEnvelope_Output{Output: converted},
	})
	if err != nil {
		panic(err)
	}
	return payload
}

func BenchmarkOutputCompressibleEnvelopeRoundTrip(b *testing.B) {
	encoded := baselineOutputEnvelope(true)
	b.ReportAllocs()
	b.SetBytes(int64(len(baselineOutputData(true))))
	for range b.N {
		decoded := &ServerEnvelope{}
		if err := ScanEnvelope(decoded, encoded); err != nil {
			b.Fatal(err)
		}
		if uerr := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(encoded, decoded)); uerr != nil {
			b.Fatal(uerr)
		}
	}
}

func BenchmarkOutputIncompressibleEnvelopeRoundTrip(b *testing.B) {
	encoded := baselineOutputEnvelope(false)
	b.ReportAllocs()
	b.SetBytes(int64(len(baselineOutputData(false))))
	for range b.N {
		decoded := &ServerEnvelope{}
		if err := ScanEnvelope(decoded, encoded); err != nil {
			b.Fatal(err)
		}
		if uerr := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(encoded, decoded)); uerr != nil {
			b.Fatal(uerr)
		}
	}
}
