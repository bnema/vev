package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestUIActionNavigationUsesAdmittedCause(t *testing.T) {
	for _, actionID := range []uint64{0, 17} {
		for _, message := range []protocol.ServerMessage{
			protocol.AttachTarget{CauseActionID: 999, Session: "destination", Intent: protocol.IntentAttach},
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
				require.Equal(t, actionID, testNavigationCause(t, frame.Payload))
				original, err := testServerEnvelope(message)
				require.NoError(t, err)
				require.Equal(t, uint64(999), testNavigationCause(t, original.Payload), "sending must not mutate reusable navigation templates")
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
	case protocol.RouteNavigationAction:
		return name + "recent"
	default:
		return name + "create"
	}
}

func testNavigationCause(t *testing.T, payload []byte) uint64 {
	t.Helper()
	message, err := sessionwire.DecodeServerEnvelope(payload)
	require.NoError(t, err)
	switch message := message.(type) {
	case protocol.AttachTarget:
		return message.CauseActionID
	case protocol.RouteNavigationAction:
		return message.CauseActionID
	case protocol.RouteCreateSessionAction:
		return message.CauseActionID
	default:
		t.Fatalf("unexpected navigation message %T", message)
		return 0
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
	frame := awaitFrame(t, sends, "RouteNavigationAction")
	require.Equal(t, uint64(41), testNavigationCause(t, frame.Payload), "submission owns navigation, not the earlier palette-opening action")
}
