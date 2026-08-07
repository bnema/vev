package main

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const (
	probeTestCols = 32
	probeTestRows = 2
)

type probeTestTransport struct {
	sent []ports.Frame
}

func (t *probeTestTransport) Send(frame ports.Frame) error {
	t.sent = append(t.sent, frame)
	return nil
}

func (*probeTestTransport) Recv() (ports.Frame, error) { return ports.Frame{}, errors.New("not used") }
func (*probeTestTransport) Close() error               { return nil }

func probeTestOutput(t *testing.T, output ports.Output) ports.Frame {
	t.Helper()
	payload, err := ports.MarshalOutput(output)
	if err != nil {
		t.Fatalf("marshal output: %v", err)
	}
	return ports.Frame{Type: ports.MsgOutput, Payload: payload}
}

func TestVisualProbePersistsFullAndIncrementalOutput(t *testing.T) {
	size := domain.Size{Cols: probeTestCols, Rows: probeTestRows}
	probe := newVisualProbe(size)
	for _, tt := range []struct {
		name   string
		output ports.Output
		want   string
		state  uint64
	}{
		{
			name:   "full",
			output: ports.Output{Epoch: 1, New: 1, Full: true, ViewRevision: 4, Size: size, Data: []byte("full")},
			want:   "full",
			state:  1,
		},
		{
			name:   "incremental",
			output: ports.Output{Epoch: 1, Base: 1, New: 2, ViewRevision: 4, Size: size, Data: []byte(" incremental")},
			want:   "full incremental",
			state:  2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result := probe.apply(tt.output)
			if !result.Accepted || !result.StateBearing {
				t.Fatalf("result = %+v, want accepted state-bearing output", result)
			}
			if result.Ack.Epoch != tt.output.Epoch || result.Ack.State != tt.state {
				t.Fatalf("ack = %+v, want epoch=%d state=%d", result.Ack, tt.output.Epoch, tt.state)
			}
			if !strings.Contains(probe.text(), tt.want) {
				t.Fatalf("screen = %q, want %q", probe.text(), tt.want)
			}
			if probe.state.state != tt.state {
				t.Fatalf("state = %d, want %d", probe.state.state, tt.state)
			}
		})
	}
	if got := len(probe.checkpoints); got != 2 {
		t.Fatalf("checkpoints = %d, want 2", got)
	}
	if got := len(probe.events); got != 2 {
		t.Fatalf("events = %d, want 2", got)
	}
}

func TestVisualProbeRejectsOutputWithoutMutationOrAck(t *testing.T) {
	size := domain.Size{Cols: probeTestCols, Rows: probeTestRows}
	for _, tt := range []struct {
		name   string
		output ports.Output
	}{
		{
			name:   "base gap",
			output: ports.Output{Epoch: 1, Base: 0, New: 3, Full: true, ViewRevision: 4, Size: size, Data: []byte("rejected")},
		},
		{
			name:   "revision gap",
			output: ports.Output{Epoch: 1, Base: 1, New: 2, ViewRevision: 5, Size: size, Data: []byte("rejected")},
		},
		{
			name:   "stale epoch",
			output: ports.Output{Epoch: 0, Base: 1, New: 2, ViewRevision: 4, Size: size, Data: []byte("rejected")},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			probe := newVisualProbe(size)
			first := ports.Output{Epoch: 1, New: 1, Full: true, ViewRevision: 4, Size: size, Data: []byte("keep")}
			if result := probe.apply(first); !result.Accepted {
				t.Fatal("seed output was rejected")
			}
			beforeText, beforeState, beforeCheckpoints := probe.text(), probe.state, len(probe.checkpoints)
			result := probe.apply(tt.output)
			if result.Accepted || result.StateBearing || result.Ack != (ports.Ack{}) {
				t.Fatalf("result = %+v, want rejected output without ACK", result)
			}
			if got := probe.text(); got != beforeText {
				t.Fatalf("screen changed from %q to %q", beforeText, got)
			}
			if probe.state != beforeState {
				t.Fatalf("state = %+v, want unchanged %+v", probe.state, beforeState)
			}
			if len(probe.checkpoints) != beforeCheckpoints {
				t.Fatalf("checkpoints = %d, want %d", len(probe.checkpoints), beforeCheckpoints)
			}
		})
	}
}

func TestVisualProbeSideEffectsApplyWithoutAck(t *testing.T) {
	size := domain.Size{Cols: probeTestCols, Rows: probeTestRows}
	for _, tt := range []struct {
		name   string
		seed   *ports.Output
		effect ports.Output
	}{
		{
			name:   "before state-bearing output",
			effect: ports.Output{Epoch: 1, Size: size, Data: []byte("side effect")},
		},
		{
			name:   "after state-bearing output",
			seed:   &ports.Output{Epoch: 1, New: 1, Full: true, ViewRevision: 4, Size: size, Data: []byte("seed")},
			effect: ports.Output{Epoch: 1, ViewRevision: 4, Size: size, Data: []byte(" side effect")},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			probe := newVisualProbe(size)
			if tt.seed != nil && !probe.apply(*tt.seed).Accepted {
				t.Fatal("seed output was rejected")
			}
			beforeState, beforeCheckpoints := probe.state, len(probe.checkpoints)
			result := probe.apply(tt.effect)
			if !result.Accepted || result.StateBearing || result.Ack != (ports.Ack{}) {
				t.Fatalf("result = %+v, want accepted side effect without ACK", result)
			}
			if !strings.Contains(probe.text(), string(tt.effect.Data)) {
				t.Fatalf("screen = %q, want side effect %q", probe.text(), tt.effect.Data)
			}
			if probe.state != beforeState {
				t.Fatalf("state = %+v, want unchanged %+v", probe.state, beforeState)
			}
			if len(probe.checkpoints) != beforeCheckpoints+1 {
				t.Fatalf("checkpoints = %d, want %d", len(probe.checkpoints), beforeCheckpoints+1)
			}
		})
	}
}

func TestProcessOutputFrameACKsOnlyAcceptedStateBearingFrames(t *testing.T) {
	size := domain.Size{Cols: probeTestCols, Rows: probeTestRows}
	for _, tt := range []struct {
		name      string
		output    ports.Output
		wantAck   bool
		wantReset bool
	}{
		{
			name:    "accepted full",
			output:  ports.Output{Epoch: 1, New: 1, Full: true, ViewRevision: 4, Size: size, Data: []byte("full")},
			wantAck: true,
		},
		{
			name:   "accepted side effect",
			output: ports.Output{Epoch: 1, Size: size, Data: []byte("side effect")},
		},
		{
			name:      "rejected state",
			output:    ports.Output{Epoch: 1, New: 3, Full: true, ViewRevision: 4, Size: size, Data: []byte("gap")},
			wantReset: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			transport := &probeTestTransport{}
			tr := &harnessTransport{Transport: transport, probe: newVisualProbe(size)}
			if tt.name == "rejected state" {
				seed := probeTestOutput(t, ports.Output{Epoch: 1, New: 1, Full: true, ViewRevision: 4, Size: size, Data: []byte("seed")})
				if err := processOutputFrame(tr, seed); err != nil {
					t.Fatal(err)
				}
				transport.sent = nil
			}
			if err := processOutputFrame(tr, probeTestOutput(t, tt.output)); err != nil {
				t.Fatal(err)
			}
			var ackCount, resetCount int
			for _, frame := range transport.sent {
				switch frame.Type {
				case ports.MsgAck:
					ackCount++
				case ports.MsgOutputResetRequest:
					resetCount++
				}
			}
			if (ackCount > 0) != tt.wantAck {
				t.Fatalf("ACK count = %d, want ACK = %t", ackCount, tt.wantAck)
			}
			if (resetCount > 0) != tt.wantReset {
				t.Fatalf("reset count = %d, want reset = %t", resetCount, tt.wantReset)
			}
		})
	}
}

func TestCaptureUnifiedPickerRowsRequiresLocalAndRemoteEntries(t *testing.T) {
	probe := newVisualProbe(domain.Size{Cols: 48, Rows: 4})
	probe.screen.Write([]byte("local-picker\npicker@remote"))

	rows := capturePickerRows(probe)
	if err := assertUnifiedPickerRows(rows, "local-picker", "picker"); err != nil {
		t.Fatalf("assert unified picker rows: %v; rows = %#v", err, rows)
	}
}

func TestVisualProbeRecordsUnexpectedAttachTarget(t *testing.T) {
	probe := newVisualProbe(domain.Size{Cols: probeTestCols, Rows: probeTestRows})
	probe.recordIncoming(ports.Frame{Type: ports.MsgAttachTarget})

	if probe.unexpectedHandoffs != 1 {
		t.Fatalf("unexpected handoffs = %d, want 1", probe.unexpectedHandoffs)
	}
	if len(probe.events) != 1 || probe.events[0].Kind != probeEventUnexpectedHandoff {
		t.Fatalf("events = %#v, want one unexpected handoff event", probe.events)
	}
}

func TestAssertNoRemoteChromeIgnoresLocalBarsButRejectsContentLeak(t *testing.T) {
	for _, test := range []struct {
		name    string
		content string
		wantErr bool
	}{
		{name: "local bars", content: "picker at remote\nremote content\nlocal bottom", wantErr: false},
		{name: "remote status in content", content: "local top\npicker at remote\nlocal bottom", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := newVisualProbe(domain.Size{Cols: 48, Rows: 3})
			probe.screen.Write([]byte(test.content))
			err := assertNoRemoteChrome(probe, "picker")
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, want error = %t", err, test.wantErr)
			}
		})
	}
}

func TestHarnessArtifactIsBoundedAndContainsOnlyMetadata(t *testing.T) {
	dir := t.TempDir()
	artifact := newHarnessArtifact(dir)
	probe := newVisualProbe(domain.Size{Cols: probeTestCols, Rows: probeTestRows})
	probe.configure("local-picker", nil)
	artifact.registerProbe(probe)
	secret := "not-for-artifact"
	if result := probe.apply(ports.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: probeTestCols, Rows: probeTestRows}, Data: []byte(secret)}); !result.Accepted {
		t.Fatal("probe output was rejected")
	}
	if err := artifact.write(true); err != nil {
		t.Fatal(err)
	}
	path := dir + "/remote-picker-harness.json"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > maxArtifactBytes {
		t.Fatalf("artifact bytes = %d, want <= %d", len(data), maxArtifactBytes)
	}
	if strings.Contains(string(data), secret) {
		t.Fatal("artifact contained raw output")
	}
	var document artifactDocument
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("artifact JSON: %v", err)
	}
	if !document.Passed || len(document.Probes) != 1 || len(document.Probes[0].Checkpoints) != 1 {
		t.Fatalf("artifact document = %+v, want one passed probe checkpoint", document)
	}
}
