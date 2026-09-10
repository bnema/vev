package daemon

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// TestParkedRouteSuppressesQueuedClipboardForward documents the
// desktop-effect boundary for P2.2: an application-originated OSC 52
// clipboard queued while the route is parked must not reach the parked
// client, and resuming must produce an authoritative full paint rather
// than replaying the suppressed effect.
func TestParkedRouteSuppressesQueuedClipboardForward(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)

	leaseID := armAndPrepareParkedRoute(t, d, source, ac, sends)
	require.True(t, ac.parkedRouteOutput.Load(), "prepare must park output before the clipboard arrives")

	d.forwardClipboardAsync(clipboardOwnerLease(t, source), base64.StdEncoding.EncodeToString([]byte("parked-clipboard")))
	select {
	case frame := <-sends:
		t.Fatalf("parked clipboard forward emitted frame type %d", frame.Type)
	case <-time.After(100 * time.Millisecond):
	}

	resume := protocol.ParkedRouteRequest{RequestID: 2, LeaseID: leaseID, Action: protocol.ParkedRouteResume}
	d.handleAttachmentClientFrame(source.captureAttachmentCapability(ac, ac.transport()), wire.Frame{Type: wire.MsgParkedRouteRequest, Payload: wire.MarshalParkedRouteRequest(resume)})
	_ = awaitFrame(t, sends, wire.MsgParkedRouteResponse)
	outputFrame := awaitFrame(t, sends, wire.MsgOutput)
	output, err := wire.UnmarshalOutput(outputFrame.Payload)
	require.NoError(t, err)
	require.True(t, output.Full, "resume must produce an authoritative full paint")
	require.NotContains(t, string(output.Data), "parked-clipboard", "suppressed clipboard must not replay inside the resume paint")
	require.NotContains(t, string(output.Data), "\x1b]52;", "suppressed clipboard must not replay as OSC 52 inside the resume paint")
	position, err := wire.UnmarshalRoutePosition(awaitFrame(t, sends, wire.MsgRoutePosition).Payload)
	require.NoError(t, err)
	require.Equal(t, source.incarnation, position.Target.LifecycleID)
	select {
	case frame := <-sends:
		t.Fatalf("parked clipboard replayed after resume as frame type %d", frame.Type)
	default:
	}
}
