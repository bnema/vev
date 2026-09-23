package brokeripc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// previewTarget builds one valid live preview target on the canonical local
// authority: it still requires a non-empty route identity for the local daemon.
func previewTarget() domain.RemoteSessionTarget {
	return domain.RemoteSessionTarget{
		Endpoint:      "local",
		DisplayOrigin: "local",
		LifecycleID:   domain.SessionLifecycleID{0x11},
		SessionName:   "work",
		LiveTabID:     "tab-alpha",
	}
}

// previewRequest builds one valid generation-N preview request for a client:
// the local daemon route plus the exact session/tab target and bounded
// viewport sent to it. The broker allocates the observation stream itself, so
// the request names none. The endpoint epoch is assigned at accept, so the
// test takes it explicitly.
func previewRequest(t *testing.T, epoch ports.BrokerEpoch, client ports.BrokerService, generation ports.BrokerPreviewGeneration) ports.BrokerPreviewRequest {
	t.Helper()
	request := ports.BrokerPreviewRequest{
		Epoch:      epoch,
		Connection: client.ConnectionID(),
		Generation: generation,
		Route:      ports.BrokerPreviewRoute{Local: true, Policy: openRequest(1).Policy},
		Preview:    protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: previewTarget(), Width: 80, Height: 24},
	}
	require.NoError(t, request.Validate())
	return request
}

// awaitPreviewChanged waits for one preview Changed wake under an explicit
// bound, so a subscription that never publishes is a bounded test failure.
func awaitPreviewChanged(t *testing.T, sub ports.BrokerPreviewSubscription) ports.BrokerPreviewPublication {
	t.Helper()
	select {
	case <-sub.Changed():
		return sub.Latest()
	case <-time.After(5 * time.Second):
		t.Fatal("the preview subscription did not publish within the bound")
		return ports.BrokerPreviewPublication{}
	}
}

// serverPreviewGeneration returns the server session's current preview
// generation: it observes the same replacement bookkeeping startPreview
// maintains, so a test can prove which generation the server holds current.
func serverPreviewGeneration(session ports.BrokerCoreService) ports.BrokerPreviewGeneration {
	server, ok := session.(*serverSession)
	if !ok {
		return 0
	}
	server.subMu.Lock()
	defer server.subMu.Unlock()
	return server.previewGeneration
}

// serverPreviewOf returns the server session's current preview subscription.
func serverPreviewOf(session ports.BrokerCoreService) ports.BrokerPreviewSubscription {
	server, ok := session.(*serverSession)
	if !ok {
		return nil
	}
	server.subMu.Lock()
	defer server.subMu.Unlock()
	return server.preview
}

// TestPreviewReplacementClosesPrevious proves GO-001/GO-003 on the wire: a
// second SubscribePreview with a newer generation replaces the first on both
// ends. The client retires the first subscription, and the server keeps only
// the newest generation as its current preview.
func TestPreviewReplacementClosesPrevious(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, session := e.pair()

	_, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 1))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return serverPreviewOf(session) != nil }, 5*time.Second, 10*time.Millisecond, "the first generation must register server-side")

	second, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 2))
	require.NoError(t, err)
	require.NotNil(t, second)

	require.Eventually(t, func() bool { return serverPreviewGeneration(session) == 2 }, 5*time.Second, 10*time.Millisecond, "the second generation must replace the first server-side")
}

// TestPreviewCancelGenerationFencesStaleCancel proves the CancelPreview
// generation match: cancelling a superseded generation leaves the current
// preview registered, while cancelling the current generation clears it.
func TestPreviewCancelGenerationFencesStaleCancel(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, session := e.pair()

	first, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 1))
	require.NoError(t, err)
	second, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 2))
	require.NoError(t, err)

	require.Eventually(t, func() bool { return serverPreviewGeneration(session) == 2 }, 5*time.Second, 10*time.Millisecond, "the second generation must register server-side")

	// A stale cancel for the superseded first generation must not disturb the
	// current second generation.
	first.Close()
	require.Never(t, func() bool { return serverPreviewOf(session) == nil }, 200*time.Millisecond, 10*time.Millisecond, "a stale cancel must not clear the current preview")

	second.Close()
	require.Eventually(t, func() bool { return serverPreviewOf(session) == nil }, 5*time.Second, 10*time.Millisecond, "cancelling the current generation must clear the preview")
}

// TestPreviewCloseClearsCurrent proves GO-004: closing the client connection
// retires its current preview subscription instead of relying on transport
// EOF for server teardown.
func TestPreviewCloseClearsCurrent(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, session := e.pair()

	sub, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 1))
	require.NoError(t, err)
	_ = sub
	require.Eventually(t, func() bool { return serverPreviewOf(session) != nil }, 5*time.Second, 10*time.Millisecond, "the preview must register server-side")

	require.NoError(t, client.Close())
	require.Eventually(t, func() bool { return serverPreviewOf(session) == nil }, 5*time.Second, 10*time.Millisecond, "closing the client must retire its preview server-side")
}

// TestPreviewShutdownClearsServerState proves the server shutdown path stops
// the replaced forwarder bookkeeping: a session that held a preview releases
// it when the session shuts down.
func TestPreviewShutdownClearsServerState(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, session := e.pair()

	_, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 1))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return serverPreviewOf(session) != nil }, 5*time.Second, 10*time.Millisecond, "the preview must register server-side")

	require.NoError(t, session.Close())
	require.Eventually(t, func() bool { return serverPreviewOf(session) == nil }, 5*time.Second, 10*time.Millisecond, "session shutdown must release the preview")
}
