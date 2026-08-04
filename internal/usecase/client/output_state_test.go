package client

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestOutputApplyStateTransitions(t *testing.T) {
	full := ports.Output{Epoch: 1, Base: 0, New: 1, Full: true, ViewRevision: 2, Size: domain.Size{Cols: 80, Rows: 24}}
	tests := []struct {
		name     string
		state    outputApplyState
		output   ports.Output
		accepted bool
		want     outputApplyState
	}{
		{name: "first full", output: full, accepted: true, want: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}},
		{name: "first gap", output: ports.Output{Epoch: 1, Base: 1, New: 2, ViewRevision: 2}, accepted: false},
		{name: "increment", state: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}, output: ports.Output{Epoch: 1, Base: 1, New: 2, ViewRevision: 2}, accepted: true, want: outputApplyState{epoch: 1, state: 2, viewRevision: 2, initialized: true}},
		{name: "base gap", state: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}, output: ports.Output{Epoch: 1, Base: 0, New: 2, Full: true, ViewRevision: 2}, accepted: false},
		{name: "revision gap", state: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}, output: ports.Output{Epoch: 1, Base: 1, New: 2, ViewRevision: 3}, accepted: false},
		{name: "new epoch reset", state: outputApplyState{epoch: 1, state: 1, viewRevision: 2, initialized: true}, output: ports.Output{Epoch: 2, Base: 0, New: 1, Full: true, ViewRevision: 3}, accepted: true, want: outputApplyState{epoch: 2, state: 1, viewRevision: 3, initialized: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
