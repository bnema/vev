package client

import "github.com/bnema/vev/internal/ports"

// outputApplyState is the client-side dependency chain for one attachment.
// State-bearing frames are accepted only when their epoch, base, and view
// revision continue the last frame; a new epoch must carry a full reset.
type outputApplyState struct {
	epoch        uint64
	state        uint64
	viewRevision uint64
	initialized  bool
}

func (s outputApplyState) next(output ports.Output) (outputApplyState, bool) {
	if output.Epoch == 0 {
		return outputApplyState{}, false
	}
	// State-independent terminal side effects are ordered on the same transport
	// as ordinary output but deliberately do not advance or acknowledge the
	// replay chain. They must still reach the terminal while a reset request is
	// gating state-bearing frames; handoff cleanup is the final such frame on
	// the old attachment and no later replay can make it up.
	if output.New == 0 {
		if output.Base != 0 || output.Full {
			return outputApplyState{}, false
		}
		return s, true
	}
	if !s.initialized {
		if output.Base != 0 || !output.Full {
			return outputApplyState{}, false
		}
		return outputApplyState{epoch: output.Epoch, state: output.New, viewRevision: output.ViewRevision, initialized: true}, true
	}
	if output.Epoch < s.epoch {
		return outputApplyState{}, false
	}
	if output.Epoch == s.epoch {
		if output.ViewRevision != s.viewRevision {
			return outputApplyState{}, false
		}
		if output.Full || output.Base != s.state || output.New != output.Base+1 {
			return outputApplyState{}, false
		}
		return outputApplyState{epoch: s.epoch, state: output.New, viewRevision: s.viewRevision, initialized: true}, true
	}
	if !output.Full || output.Base != 0 {
		return outputApplyState{}, false
	}
	return outputApplyState{epoch: output.Epoch, state: output.New, viewRevision: output.ViewRevision, initialized: true}, true
}
