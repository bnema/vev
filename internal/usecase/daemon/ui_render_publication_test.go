package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestRenderPublicationUsesCapturedIdentityAndRevision(t *testing.T) {
	for _, scenario := range []string{"unchanged", "rename after capture", "view replaced after capture"} {
		t.Run(scenario, func(t *testing.T) {
			d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
			ac.sendMu.Lock()
			state, ok := captureRenderState(sess, ac, renderCaptureRequest{reset: true})
			if !ok {
				ac.sendMu.Unlock()
				t.Fatal("capture rejected")
			}
			want := state.viewContext()
			want.Publication = 1
			require.NoError(t, want.Validate())
			composed := composeFrame(*state, ac.pipelineCache)
			switch scenario {
			case "rename after capture":
				sess.mu.Lock()
				sess.name = "renamed"
				sess.mu.Unlock()
			case "view replaced after capture":
				view := ac.viewSnapshot()
				view.revision++
				ac.publishView(view)
			}
			require.True(t, d.emitFrame(sess, ac, state, composed)) // releases sendMu
			if scenario == "view replaced after capture" {
				require.Empty(t, drainAllFrames(sends))
				require.Zero(t, ac.output.viewPublication)
				require.Zero(t, ac.output.next)
				return
			}
			output, err := wire.UnmarshalOutput(awaitFrame(t, sends, wire.MsgOutput).Payload)
			require.NoError(t, err)
			require.Equal(t, &want, output.Context)
			require.Equal(t, state.view.revision, output.ViewRevision)
			require.Equal(t, want, ac.output.lastViewContext)
			require.Equal(t, domain.PaneStableID(sess.tabs[0].focusedPane().stableID), output.Context.FocusedPaneID)
		})
	}
}

func TestRenderPublishesContextWithoutTerminalBytes(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	paint := func(reset bool) {
		t.Helper()
		ac.sendMu.Lock()
		state, ok := captureRenderState(sess, ac, renderCaptureRequest{reset: reset})
		if !ok {
			ac.sendMu.Unlock()
			t.Fatal("capture rejected")
		}
		composed := composeFrame(*state, ac.pipelineCache)
		require.True(t, d.emitFrame(sess, ac, state, composed))
	}
	paint(true)
	initial, err := wire.UnmarshalOutput(awaitFrame(t, sends, wire.MsgOutput).Payload)
	require.NoError(t, err)
	require.NotNil(t, initial.Context)
	beforeOutstanding := ac.output.outstanding()
	sess.mu.Lock()
	sess.name = "renamed"
	sess.mu.Unlock()
	// The test's bars contain no session name, so only semantic context changes.
	paint(false)
	update, err := wire.UnmarshalUIViewUpdate(awaitFrame(t, sends, wire.MsgUIViewUpdate).Payload)
	require.NoError(t, err)
	require.Equal(t, initial.Epoch, update.Epoch)
	require.Equal(t, initial.New, update.State)
	require.Equal(t, initial.Context.Publication+1, update.Context.Publication)
	require.Equal(t, "renamed", update.Context.Route.Target.SessionName)
	require.Equal(t, initial.Context.Route.Target.LifecycleID, update.Context.Route.Target.LifecycleID)
	position, err := wire.UnmarshalRoutePosition(awaitFrame(t, sends, wire.MsgRoutePosition).Payload)
	require.NoError(t, err)
	require.Equal(t, update.Context.Route.Target, position.Target)
	require.Equal(t, beforeOutstanding, ac.output.outstanding(), "metadata does not create an ACK obligation")
	require.Empty(t, drainAllFrames(sends))
	paint(false)
	require.Empty(t, drainAllFrames(sends), "unchanged view without a fence sends nothing")
	require.Equal(t, update.Context.Publication, ac.output.viewPublication)
	query, err := ac.output.sideEffect([]byte("query"), 0)
	require.NoError(t, err)
	require.Nil(t, query.Context)
	require.Equal(t, update.Context, ac.output.lastViewContext)
}
