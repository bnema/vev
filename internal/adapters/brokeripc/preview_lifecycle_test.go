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

// awaitPreview waits for the core to hand out its next preview subscription,
// under a single bounded failure guard.
func awaitPreview(t *testing.T, core *coreState) *previewState {
	t.Helper()
	select {
	case state := <-core.previews:
		return state
	case <-time.After(5 * time.Second):
		t.Fatal("the preview subscription never reached the core")
		return nil
	}
}

// awaitPreviewClosed waits for the server to retire one preview subscription.
func awaitPreviewClosed(t *testing.T, state *previewState, why string) {
	t.Helper()
	select {
	case <-state.closed:
	case <-time.After(5 * time.Second):
		t.Fatal(why)
	}
}

// TestPreviewReplacementClosesPrevious proves GO-001/GO-003 on the wire: a
// second SubscribePreview with a newer generation replaces the first on both
// ends. The client retires the first subscription, and the server keeps only
// the newest generation as its current preview.
//
// real I/O: the broker IPC listener runs on a real AF_UNIX socket.
func TestPreviewReplacementClosesPrevious(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, session := e.pair()
	core := e.authority.last()

	_, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 1))
	require.NoError(t, err)
	first := awaitPreview(t, core)

	second, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 2))
	require.NoError(t, err)
	require.NotNil(t, second)

	awaitPreviewClosed(t, first, "the second generation must replace the first server-side")
	require.Equal(t, ports.BrokerPreviewGeneration(2), serverPreviewGeneration(session))
	require.NotNil(t, serverPreviewOf(session))
}

// TestPreviewCancelGenerationFencesStaleCancel proves the CancelPreview
// generation match: cancelling a superseded generation leaves the current
// preview registered, while cancelling the current generation clears it.
//
// real I/O: the broker IPC listener runs on a real AF_UNIX socket.
func TestPreviewCancelGenerationFencesStaleCancel(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, session := e.pair()
	core := e.authority.last()

	first, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 1))
	require.NoError(t, err)
	firstState := awaitPreview(t, core)
	second, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 2))
	require.NoError(t, err)
	secondState := awaitPreview(t, core)

	awaitPreviewClosed(t, firstState, "the second generation must replace the first server-side")
	require.Equal(t, ports.BrokerPreviewGeneration(2), serverPreviewGeneration(session))

	// A stale cancel for the superseded first generation must not disturb the
	// current second generation. Frames on one connection are processed in
	// order, so the membership round trip below proves any cancel was handled.
	first.Close()
	requireConnectionDelegates(t, client)
	require.NotNil(t, serverPreviewOf(session), "a stale cancel must not clear the current preview")
	select {
	case <-secondState.closed:
		t.Fatal("a stale cancel must not close the current preview")
	default:
	}

	second.Close()
	awaitPreviewClosed(t, secondState, "cancelling the current generation must clear the preview")
	require.Nil(t, serverPreviewOf(session))
}

// TestPreviewCloseClearsCurrent proves GO-004: closing the client connection
// retires its current preview subscription instead of relying on transport
// EOF for server teardown.
//
// real I/O: the broker IPC listener runs on a real AF_UNIX socket.
func TestPreviewCloseClearsCurrent(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, session := e.pair()
	core := e.authority.last()

	_, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 1))
	require.NoError(t, err)
	state := awaitPreview(t, core)
	// Frames on one connection are handled in order, so the round trip proves
	// StartPreview finished registering the subscription.
	requireConnectionDelegates(t, client)
	require.NotNil(t, serverPreviewOf(session), "the preview must register server-side")

	require.NoError(t, client.Close())
	awaitPreviewClosed(t, state, "closing the client must retire its preview server-side")
	require.Nil(t, serverPreviewOf(session))
}

// TestPreviewShutdownClearsServerState proves the server shutdown path stops
// the replaced forwarder bookkeeping: a session that held a preview releases
// it when the session shuts down.
//
// real I/O: the broker IPC listener runs on a real AF_UNIX socket.
func TestPreviewShutdownClearsServerState(t *testing.T) {
	e := startEndpoint(t, Config{})
	client, session := e.pair()
	core := e.authority.last()

	_, err := client.SubscribePreview(previewRequest(t, e.epoch, client, 1))
	require.NoError(t, err)
	state := awaitPreview(t, core)
	// Frames on one connection are handled in order, so the round trip proves
	// StartPreview finished registering the subscription.
	requireConnectionDelegates(t, client)
	require.NotNil(t, serverPreviewOf(session), "the preview must register server-side")

	require.NoError(t, session.Close())
	awaitPreviewClosed(t, state, "session shutdown must release the preview")
	require.Nil(t, serverPreviewOf(session))
}
