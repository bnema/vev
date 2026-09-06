package client

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func testOutputView(publication uint64) protocol.ViewContext {
	return protocol.ViewContext{
		Publication: publication,
		Route:       protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}},
		TabID:       "tab-1", FocusedPaneID: "pane-1",
	}
}

func TestOutputApplyCapturedViewContext(t *testing.T) {
	context := testOutputView(1)
	initial, accepted := (outputApplyState{}).next(protocol.Output{Epoch: 2, New: 1, Full: true, ViewRevision: 3, Context: &context})
	require.True(t, accepted)
	require.Equal(t, context, initial.context)
	context.FocusedPaneID = "changed after acceptance"
	require.Equal(t, testOutputView(1), initial.context, "accepted metadata must not alias received pointers")
	for _, scenario := range []string{"delta", "missing context", "stale publication", "invalid context", "different session", "contextual side effect", "plain side effect"} {
		t.Run(scenario, func(t *testing.T) {
			context := testOutputView(2)
			output := protocol.Output{Epoch: 2, Base: 1, New: 2, ViewRevision: 3, Context: &context}
			switch scenario {
			case "missing context":
				output.Context = nil
			case "different session":
				context.Route.Target.LifecycleID = domain.SessionLifecycleID{2}
			case "stale publication":
				context.Publication = 1
			case "invalid context":
				context.FocusedPaneID = ""
			case "contextual side effect":
				output.Base, output.New = 0, 0
			case "plain side effect":
				output.Base, output.New, output.Epoch = 0, 0, 9
				output.Context = nil
			}
			next, accepted := initial.next(output)
			require.Equal(t, scenario == "delta" || scenario == "plain side effect", accepted)
			if scenario == "delta" {
				require.Equal(t, context, next.context)
			} else if scenario == "plain side effect" {
				require.Equal(t, initial, next, "query/cleanup bytes cannot overwrite committed view identity")
			}
		})
	}
}

func TestOutputApplyMetadataOnlyUpdates(t *testing.T) {
	initial := outputApplyState{epoch: 2, state: 4, viewRevision: 3, initialized: true, context: testOutputView(7)}
	for _, test := range []struct {
		name                      string
		initialized               bool
		epoch, state, publication uint64
		accepted, reset           bool
	}{
		{"same output new focus", true, 2, 4, 8, true, false},
		{"before initialization", false, 2, 4, 8, false, true},
		{"duplicate", true, 2, 4, 7, false, false},
		{"stale publication", true, 2, 4, 6, false, false},
		{"stale epoch", true, 1, 4, 8, false, false},
		{"stale output", true, 2, 3, 8, false, false},
		{"future epoch", true, 3, 4, 8, false, true},
		{"future output", true, 2, 5, 8, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := initial
			state.initialized = test.initialized
			view := testOutputView(test.publication)
			view.FocusedPaneID = "pane-2"
			next, accepted, reset := state.nextView(protocol.UIViewUpdate{Epoch: test.epoch, State: test.state, Context: view})
			require.Equal(t, test.accepted, accepted)
			require.Equal(t, test.reset, reset)
			if accepted {
				require.Equal(t, initial.epoch, next.epoch)
				require.Equal(t, initial.state, next.state)
				require.Equal(t, initial.viewRevision, next.viewRevision)
				require.Equal(t, view, next.context)
			}
		})
	}
}

func TestOutputUIContextUsesOnlyCapturedIdentity(t *testing.T) {
	state := outputApplyState{epoch: 2, state: 4, viewRevision: 3, initialized: true, context: testOutputView(7)}
	got := state.uiContext(ports.UIContext{AttachmentHandle: "public", Generation: 5}, ports.UIStatusReconnecting)
	require.Equal(t, ports.UIContext{
		AttachmentHandle: "public", Generation: 5, Route: state.context.Route,
		TabID: state.context.TabID, FocusedPaneID: state.context.FocusedPaneID,
		OutputEpoch: 2, OutputState: 4, ViewRevision: 3, ViewPublication: 7, Status: ports.UIStatusReconnecting,
	}, got)
}
