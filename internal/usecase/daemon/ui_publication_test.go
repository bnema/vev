package daemon

import (
	"errors"
	"testing"

	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func publicationTestContext() protocol.ViewContext {
	return protocol.ViewContext{
		Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}},
		TabID: "tab-1", FocusedPaneID: "pane-1",
	}
}

func TestPreparedOutputSemanticPublication(t *testing.T) {
	for _, scenario := range []string{"success", "send failure", "stale", "exhausted"} {
		t.Run(scenario, func(t *testing.T) {
			stream := newOutputStateStream()
			frame := renderer.NewFrame(3, 1)
			prepared, err := stream.prepare(frame, nil, true)
			require.NoError(t, err)
			context := publicationTestContext()
			prepared.context = &context
			if scenario == "stale" {
				stream.rebase()
			}
			if scenario == "exhausted" {
				stream.viewPublication = ^uint64(0)
			}
			before := stream.viewPublication
			called := false
			sendErr := errors.New("send failed")
			err = prepared.send(prepared.data, 9, func(output protocol.Output) error {
				called = true
				require.NotNil(t, output.Context)
				require.Equal(t, before+1, output.Context.Publication)
				require.Equal(t, context.Route, output.Context.Route)
				require.Equal(t, uint64(9), output.Echo)
				_, marshalErr := wire.MarshalOutput(output)
				require.NoError(t, marshalErr)
				if scenario == "send failure" {
					return sendErr
				}
				return nil
			})
			switch scenario {
			case "success":
				require.NoError(t, err)
				require.True(t, called)
				require.True(t, prepared.sent)
				require.Equal(t, before+1, stream.viewPublication)
				sideEffect, err := stream.sideEffect([]byte("query"), 9)
				require.NoError(t, err)
				require.Nil(t, sideEffect.Context)
				require.Equal(t, context.Route, stream.lastViewContext.Route)
				boundary := prepared.boundary
				require.Equal(t, protocol.UIReceipt{Epoch: stream.epoch, State: stream.next, ViewPublication: stream.viewPublication, Outcome: protocol.UIReceiptProcessed}, boundary)
				stream.rebase()
				require.Equal(t, boundary, prepared.boundary, "later view rebases cannot change an accepted receipt boundary")
			case "send failure":
				require.ErrorIs(t, err, sendErr)
				require.True(t, called)
				require.False(t, prepared.sent)
				require.Equal(t, before, stream.viewPublication)
				require.Zero(t, prepared.boundary)
			case "stale", "exhausted":
				if scenario == "exhausted" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.False(t, called)
				require.False(t, prepared.sent)
				require.Equal(t, before, stream.viewPublication)
			}
		})
	}
}

func TestPreparedOutputNoBytePublication(t *testing.T) {
	for _, scenario := range []string{"unchanged", "fence", "focus", "failure", "stale"} {
		t.Run(scenario, func(t *testing.T) {
			stream := newOutputStateStream()
			frame := renderer.NewFrame(3, 1)
			context := publicationTestContext()
			first, err := stream.prepare(frame, nil, true)
			require.NoError(t, err)
			first.context = &context
			require.NoError(t, first.send(first.data, 0, func(protocol.Output) error { return nil }))
			prepared, err := stream.prepare(frame, nil, false)
			require.NoError(t, err)
			require.Empty(t, prepared.data)
			prepared.context = &context
			if scenario == "focus" {
				context.FocusedPaneID = "pane-2"
			}
			if scenario == "stale" {
				stream.rebase()
			}
			epoch, state, outstanding := stream.epoch, stream.next, stream.outstanding()
			called := false
			sendErr := errors.New("metadata send failed")
			err = prepared.publishNoBytes(scenario == "fence" || scenario == "failure" || scenario == "stale", func(update protocol.UIViewUpdate) error {
				called = true
				require.Equal(t, epoch, update.Epoch)
				require.Equal(t, state, update.State)
				require.Equal(t, uint64(2), update.Context.Publication)
				require.Equal(t, context.FocusedPaneID, update.Context.FocusedPaneID)
				_, marshalErr := wire.MarshalUIViewUpdate(update)
				require.NoError(t, marshalErr)
				if scenario == "failure" {
					return sendErr
				}
				return nil
			})
			if scenario == "failure" {
				require.ErrorIs(t, err, sendErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, scenario != "unchanged" && scenario != "stale", called)
			require.Equal(t, scenario != "failure" && scenario != "stale", prepared.sent)
			wantPublication := uint64(1)
			if scenario == "focus" || scenario == "fence" {
				wantPublication = 2
			}
			require.Equal(t, wantPublication, stream.viewPublication)
			require.Equal(t, epoch, stream.epoch)
			require.Equal(t, state, stream.next)
			require.Equal(t, outstanding, stream.outstanding())
			if scenario == "focus" || scenario == "fence" {
				boundary := prepared.boundary
				require.Equal(t, protocol.UIReceipt{Epoch: epoch, State: state, ViewPublication: wantPublication, Outcome: protocol.UIReceiptProcessed}, boundary)
				stream.rebase()
				require.Equal(t, boundary, prepared.boundary)
			} else {
				require.Zero(t, prepared.boundary, "only an accepted publication supplies a receipt boundary")
			}
		})
	}
}

func TestCapturedViewContextUsesCapturedFocus(t *testing.T) {
	for _, floating := range []bool{false, true} {
		t.Run(map[bool]string{false: "tiled", true: "floating"}[floating], func(t *testing.T) {
			context := publicationTestContext()
			state := capturedRenderState{
				route: context.Route,
				view:  attachmentView{tabID: context.TabID, paneID: "not-the-rendered-focus"},
				panes: []capturedPaneRenderState{
					{stableID: "other-pane"},
					{stableID: context.FocusedPaneID, focused: true},
				},
				floating: capturedFloatingRenderState{visible: floating, focused: floating, pane: capturedPaneRenderState{stableID: "floating-pane"}},
			}
			got := state.viewContext()
			if floating {
				context.FocusedPaneID = "floating-pane"
			}
			require.Equal(t, context, got)
			state.route.Target.SessionName = "renamed"
			state.panes[1].stableID = "replacement"
			require.Equal(t, context, got, "published values do not alias capture storage")
		})
	}
}
