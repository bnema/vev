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
