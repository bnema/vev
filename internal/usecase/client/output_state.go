package client

import (
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// outputApplyState is the client-side dependency chain for one attachment.
// Cells and captured semantic context are admitted together and committed only
// after the corresponding terminal transaction succeeds.
type outputApplyState struct {
	epoch        uint64
	state        uint64
	viewRevision uint64
	initialized  bool
	context      protocol.ViewContext
}

func (s outputApplyState) next(output protocol.Output) (outputApplyState, bool) {
	if output.Epoch == 0 {
		return outputApplyState{}, false
	}
	// Ordered side effects cross replay reset gates, but cannot advance or
	// overwrite committed state. Handoff cleanup must not invent a destination.
	if output.New == 0 {
		if output.Base != 0 || output.Full || output.Context != nil {
			return outputApplyState{}, false
		}
		return s, true
	}
	if output.Context == nil || output.Context.Validate() != nil || output.Context.Publication <= s.context.Publication {
		return outputApplyState{}, false
	}
	if !s.initialized {
		if output.Base != 0 || !output.Full {
			return outputApplyState{}, false
		}
	} else {
		if output.Epoch < s.epoch {
			return outputApplyState{}, false
		}
		if output.Epoch == s.epoch {
			if output.ViewRevision != s.viewRevision || output.Context.Route.Target.LifecycleID != s.context.Route.Target.LifecycleID {
				return outputApplyState{}, false
			}
			if output.Full || output.Base != s.state || output.New != output.Base+1 {
				return outputApplyState{}, false
			}
		} else if !output.Full || output.Base != 0 {
			return outputApplyState{}, false
		}
	}
	return outputApplyState{
		epoch: output.Epoch, state: output.New, viewRevision: output.ViewRevision,
		initialized: true, context: *output.Context,
	}, true
}

// nextView returns accepted and reset-needed separately: old updates are
// harmless, while missing/future dependencies use the existing coalesced reset
// request rather than retaining an unbounded metadata reorder queue.
func (s outputApplyState) nextView(update protocol.UIViewUpdate) (next outputApplyState, accepted, reset bool) {
	if update.Context.Validate() != nil || update.Epoch == 0 || update.State == 0 || !s.initialized {
		return s, false, true
	}
	if update.Epoch < s.epoch || update.Epoch == s.epoch && update.State < s.state || update.Context.Publication <= s.context.Publication {
		return s, false, false
	}
	if update.Epoch != s.epoch || update.State != s.state || update.Context.Route.Target.LifecycleID != s.context.Route.Target.LifecycleID {
		return s, false, true
	}
	s.context = update.Context
	return s, true, false
}

func (s outputApplyState) uiContext(identity ports.UIContext, status ports.UIPresentationStatus) ports.UIContext {
	return ports.UIContext{
		AttachmentHandle: identity.AttachmentHandle, Generation: identity.Generation,
		Route: s.context.Route, TabID: s.context.TabID, FocusedPaneID: s.context.FocusedPaneID,
		OutputEpoch: s.epoch, OutputState: s.state, ViewRevision: s.viewRevision,
		ViewPublication: s.context.Publication, Status: status,
	}
}
