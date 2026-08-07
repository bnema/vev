package daemon

import (
	"context"
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestSendRemoteViewCommandAllowsOnlyRelativeTabCommands(t *testing.T) {
	for _, slug := range []string{"next-tab", "previous-tab"} {
		t.Run(slug, func(t *testing.T) {
			d, _, link, transport := newRemoteMetadataLinkFixture(t)
			link.commands = NewCommandRequestTracker()

			done := make(chan error, 1)
			go func() {
				done <- d.sendRemoteViewCommand(context.Background(), link, link.generation, slug)
			}()

			frame := awaitFrame(t, transport.sent, ports.MsgCommand)
			request, err := ports.UnmarshalCommandRequest(frame.Payload)
			require.NoError(t, err)
			require.Equal(t, ports.ProtocolVersion, request.Version)
			require.NotZero(t, request.RequestID)
			require.True(t, request.Attached)
			require.False(t, request.Self)
			require.Equal(t, slug, request.Slug)
			require.Empty(t, request.Args)
			require.Empty(t, request.TargetSession)
			require.Empty(t, request.TargetTab)
			require.Empty(t, request.TargetPane)

			resultPayload := ports.MarshalCommandResult(ports.CommandResult{RequestID: request.RequestID, OK: true})
			require.NoError(t, d.handleRemoteLinkFrame(link, ports.Frame{Type: ports.MsgCommandResult, Payload: resultPayload}))
			require.NoError(t, awaitTestValue(t, done, "relative remote tab command did not complete"))
			require.Zero(t, link.commands.PendingCount())
		})
	}
}

func TestSendRemoteViewCommandReportsRemoteFailureAndCancellation(t *testing.T) {
	t.Run("remote failure", func(t *testing.T) {
		d, _, link, transport := newRemoteMetadataLinkFixture(t)
		link.commands = NewCommandRequestTracker()
		done := make(chan error, 1)
		go func() {
			done <- d.sendRemoteViewCommand(context.Background(), link, link.generation, "next-tab")
		}()
		frame := awaitFrame(t, transport.sent, ports.MsgCommand)
		request, err := ports.UnmarshalCommandRequest(frame.Payload)
		require.NoError(t, err)
		resultPayload := ports.MarshalCommandResult(ports.CommandResult{RequestID: request.RequestID, Code: 12, Text: "rejected"})
		require.NoError(t, d.handleRemoteLinkFrame(link, ports.Frame{Type: ports.MsgCommandResult, Payload: resultPayload}))
		require.ErrorContains(t, awaitTestValue(t, done, "failed remote tab command did not complete"), "command \"next-tab\" failed (12): rejected")
		require.Zero(t, link.commands.PendingCount())
	})

	t.Run("canceled context", func(t *testing.T) {
		d, _, link, transport := newRemoteMetadataLinkFixture(t)
		link.commands = NewCommandRequestTracker()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- d.sendRemoteViewCommand(ctx, link, link.generation, "previous-tab")
		}()
		awaitFrame(t, transport.sent, ports.MsgCommand)
		cancel()
		require.ErrorIs(t, awaitTestValue(t, done, "canceled remote tab command did not complete"), context.Canceled)
		require.Zero(t, link.commands.PendingCount())
	})
}

func TestSendRemoteViewCommandRejectsOperationsOutsideRelativeTabAllowlist(t *testing.T) {
	for _, slug := range []string{
		"new-session",
		"focus-pane-left",
		"resize-pane",
		"move-tab",
	} {
		t.Run(slug, func(t *testing.T) {
			d, _, link, transport := newRemoteMetadataLinkFixture(t)
			link.commands = NewCommandRequestTracker()

			err := d.sendRemoteViewCommand(context.Background(), link, link.generation, slug)
			require.ErrorContains(t, err, "is not allowed")
			require.Zero(t, link.commands.PendingCount())
			select {
			case frame := <-transport.sent:
				t.Fatalf("unallowlisted remote operation sent frame type %d", frame.Type)
			default:
			}
		})
	}
}
