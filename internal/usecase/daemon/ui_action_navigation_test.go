package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestUIActionNavigationUsesAdmittedCause(t *testing.T) {
	for _, actionID := range []uint64{0, 17} {
		for _, message := range []protocol.ServerMessage{
			protocol.AttachTarget{CauseActionID: 999, Session: "destination", Intent: protocol.IntentAttach},
			protocol.NavigationDirective{CauseActionID: 999, Action: protocol.NavigationBack},
			protocol.RouteNavigationAction{CauseActionID: 999, SnapshotGeneration: 1, Key: 2, Generation: 3},
			protocol.RouteCreateSessionAction{CauseActionID: 999, RequestID: 1, SnapshotGeneration: 2, Key: 3, Generation: 4, SessionName: "new"},
		} {
			t.Run(testNavigationName(message, actionID), func(t *testing.T) {
				_, sess, ac, sends := newManualSessionWithPTYs(t, nil)
				effect, ok := ac.beginAttachmentEffect(captureAttachmentCapability(sess, ac, ac.transport()))
				require.True(t, ok)
				t.Cleanup(effect.End)
				effect.uiActionID = actionID
				require.NoError(t, effect.sendControl(message))
				frame := awaitTestValue(t, sends, "navigation was not sent")
				require.Equal(t, actionID, testNavigationCause(t, frame))
				original, err := testServerFrame(message)
				require.NoError(t, err)
				require.Equal(t, uint64(999), testNavigationCause(t, original), "sending must not mutate reusable navigation templates")
			})
		}
	}
}

func testNavigationName(message protocol.ServerMessage, actionID uint64) string {
	name := "human/"
	if actionID != 0 {
		name = "automation/"
	}
	switch message.(type) {
	case protocol.AttachTarget:
		return name + "attach"
	case protocol.NavigationDirective:
		return name + "directive"
	case protocol.RouteNavigationAction:
		return name + "recent"
	default:
		return name + "create"
	}
}

func testNavigationCause(t *testing.T, frame wire.Frame) uint64 {
	t.Helper()
	switch frame.Type {
	case wire.MsgAttachTarget:
		message, err := wire.UnmarshalAttachTarget(frame.Payload)
		require.NoError(t, err)
		return message.CauseActionID
	case wire.MsgNavigationAction:
		message, err := wire.UnmarshalNavigationDirective(frame.Payload)
		require.NoError(t, err)
		return message.CauseActionID
	case wire.MsgNavigateRecentRoute:
		message, err := wire.UnmarshalRouteNavigationAction(frame.Payload)
		require.NoError(t, err)
		return message.CauseActionID
	case wire.MsgRouteCreateSession:
		message, err := wire.UnmarshalRouteCreateSessionAction(frame.Payload)
		require.NoError(t, err)
		return message.CauseActionID
	default:
		t.Fatalf("unexpected navigation frame type %d", frame.Type)
		return 0
	}
}

func TestUIActionDelayedKeyRetainsOriginalCause(t *testing.T) {
	for _, revoked := range []bool{false, true} {
		t.Run(map[bool]string{false: "later action", true: "revoked lease"}[revoked], func(t *testing.T) {
			d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
			rc := d.attachCoordinator(sess, nil, ac, true)
			t.Cleanup(func() { rc.beginSessionTeardown().finish(); rc.waitForTimerWorkers() })
			capability := captureAttachmentCapability(sess, ac, ac.transport())
			effect, ok := ac.beginAttachmentEffect(capability)
			require.True(t, ok)
			effect.uiActionID = 7
			handler := daemonKeyHandler{d: d, ac: ac, effect: effect}
			effect.End()
			later, ok := ac.beginAttachmentEffect(capability)
			require.True(t, ok)
			later.uiActionID = 99
			later.End()
			if revoked {
				rc.mu.Lock()
				rc.rebindAttachmentWithReadinessLocked(ac, true)
				rc.mu.Unlock()
			}
			current, delayed, owned := handler.acquireAttachmentEffect()
			if revoked {
				require.Nil(t, current)
				require.Nil(t, delayed)
				require.Empty(t, sends)
				return
			}
			require.Same(t, sess, current)
			require.True(t, owned)
			t.Cleanup(delayed.End)
			require.Equal(t, uint64(7), delayed.uiActionID)
			require.NoError(t, d.sendNavigationActionForAttachment(delayed, protocol.NavigationBack))
			require.Equal(t, uint64(7), testNavigationCause(t, awaitTestValue(t, sends, "delayed navigation was not sent")))
		})
	}
}

func TestUIActionInputPaletteSubmissionCarriesItsOwnCause(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	rc := d.attachCoordinator(sess, nil, ac, true)
	t.Cleanup(func() { rc.beginSessionTeardown().finish(); rc.waitForTimerWorkers() })
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{
		Generation: 9, Active: protocol.RouteRef{Key: 1, Generation: 1},
		ActiveEntry: testRouteEntry(1, 1, sess.name, 1, protocol.RouteKindLocal),
		Entries:     []protocol.RecentRouteEntry{{Key: 8, Generation: 4, Target: testRouteTarget("logs", 8), Name: "logs", HostLabel: "edge", Kind: protocol.RouteKindRemote}},
	})
	for _, input := range []protocol.Input{
		{ActionID: 40, Data: []byte("\x1b ")},
		{ActionID: 41, Data: []byte("logs@edge\r")},
	} {
		require.False(t, d.handleAttachmentClientMessage(captureAttachmentCapability(sess, ac, ac.transport()), input))
	}
	frame := awaitFrame(t, sends, wire.MsgNavigateRecentRoute)
	require.Equal(t, uint64(41), testNavigationCause(t, frame), "submission owns navigation, not the earlier palette-opening action")
}
