package client

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func TestOutputApplyStateTransitions(t *testing.T) {
	full := protocol.Output{Epoch: 1, Base: 0, New: 1, Full: true, ViewRevision: 2, Size: domain.Size{Cols: 80, Rows: 24}}
	tests := []struct {
		name     string
		state    outputApplyState
		output   protocol.Output
		accepted bool
		want     outputApplyState
	}{
		{name: "first full", output: full, accepted: true, want: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}},
		{name: "first gap", output: protocol.Output{Epoch: 1, Base: 1, New: 2, ViewRevision: 2}, accepted: false},
		{name: "increment", state: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}, output: protocol.Output{Epoch: 1, Base: 1, New: 2, ViewRevision: 2}, accepted: true, want: outputApplyState{epoch: 1, state: 2, viewRevision: 2, initialized: true}},
		{name: "base gap", state: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}, output: protocol.Output{Epoch: 1, Base: 0, New: 2, Full: true, ViewRevision: 2}, accepted: false},
		{name: "revision gap", state: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}, output: protocol.Output{Epoch: 1, Base: 1, New: 2, ViewRevision: 3}, accepted: false},
		{name: "handoff side effect crosses newer epoch replay gate", state: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}, output: protocol.Output{Epoch: 2, ViewRevision: 3}, accepted: true, want: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}},
		{name: "handoff side effect crosses stale epoch replay gate", state: outputApplyState{epoch: 2, state: 4, viewRevision: 5, initialized: true}, output: protocol.Output{Epoch: 1, ViewRevision: 2}, accepted: true, want: outputApplyState{epoch: 2, state: 4, viewRevision: 5, initialized: true}},
		{name: "side effect cannot invent state", state: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}, output: protocol.Output{Epoch: 2, Base: 1}, accepted: false},
		{name: "side effect cannot claim full replay", state: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}, output: protocol.Output{Epoch: 2, Full: true}, accepted: false},
		{name: "new epoch reset", state: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}, output: protocol.Output{Epoch: 2, Base: 0, New: 1, Full: true, ViewRevision: 3}, accepted: true, want: outputApplyState{epoch: 2, state: 1, viewRevision: 3, initialized: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.state.initialized {
				tt.state.context = testOutputView(1)
			}
			if tt.output.New != 0 {
				context := testOutputView(2)
				tt.output.Context = &context
				tt.want.context = context
			} else {
				tt.want.context = tt.state.context
			}
			got, accepted := tt.state.next(tt.output)
			if accepted != tt.accepted {
				t.Fatalf("accepted = %v, want %v", accepted, tt.accepted)
			}
			if accepted && got != tt.want {
				t.Fatalf("state = %+v, want %+v", got, tt.want)
			}
		})
	}
}
