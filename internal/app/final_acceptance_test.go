//go:build linux

package app

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/daemon"
	"github.com/stretchr/testify/require"
)

func sendAcceptanceInput(t *testing.T, tr ports.ClientConnection, data string) {
	t.Helper()
	require.NoError(t, tr.SendClient(protocol.Input{Data: []byte(data)}))
}

func awaitAcceptanceFrame(t *testing.T, p *typedPump, predicate func(protocol.ServerMessage) bool) protocol.ServerMessage {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case message, ok := <-p.ch:
			require.True(t, ok, "connection closed before message")
			if predicate(message) {
				return message
			}
		case <-deadline.C:
			t.Fatal("timed out waiting for acceptance message")
		}
	}
}

func awaitAcceptanceCommand(t *testing.T, p *typedPump, requestID uint64) protocol.CommandResult {
	t.Helper()
	message := awaitAcceptanceFrame(t, p, func(message protocol.ServerMessage) bool {
		result, ok := message.(protocol.CommandResult)
		return ok && result.RequestID == requestID
	})
	result, ok := message.(protocol.CommandResult)
	require.True(t, ok, "expected command result, got %T", message)
	return result
}

func awaitAcceptanceOutput(t *testing.T, p *typedPump, predicate func(protocol.Output) bool) protocol.Output {
	t.Helper()
	message := awaitAcceptanceFrame(t, p, func(message protocol.ServerMessage) bool {
		output, ok := message.(protocol.Output)
		return ok && predicate(output)
	})
	output, ok := message.(protocol.Output)
	require.True(t, ok, "expected output, got %T", message)
	return output
}

func awaitAcceptanceCommandResult(t *testing.T, tr ports.ClientConnection, p *typedPump, requestID uint64, slug string) protocol.CommandResult {
	t.Helper()
	require.NoError(t, tr.SendClient(protocol.CommandRequest{
		Version: protocol.Version, RequestID: requestID, Attached: true, Slug: slug,
	}))
	return awaitAcceptanceCommand(t, p, requestID)
}

func TestAcceptanceTwoLocalAttachmentsKeepViewsOverTransports(t *testing.T) {
	size := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/cat", nil))
	first, firstPump := attach(t, dir, protocol.IntentNew, "shared", size)
	second, secondPump := attach(t, dir, protocol.IntentAttach, "shared", size)
	t.Cleanup(func() { _ = first.Close() })
	t.Cleanup(func() { _ = second.Close() })

	sendAcceptanceInput(t, first, "SHARED_INPUT\n")
	awaitText(t, firstPump, size, "SHARED_INPUT")
	awaitText(t, secondPump, size, "SHARED_INPUT")

	// A tab opened by the first attachment is shared session topology, but its
	// selected window is not. The second attachment stays on the original tab.
	sendAcceptanceInput(t, first, "\x1b ")
	awaitText(t, firstPump, size, "Commands")
	sendAcceptanceInput(t, first, "CNT\r")
	awaitText(t, firstPump, size, " 1 (cat)  2 (cat) ")
	assertNoTextAfterInput(t, secondPump, size, " 1 (cat)  2 (cat) ")

	sendAcceptanceInput(t, first, "\x1b ")
	awaitText(t, firstPump, size, "Commands")
	sendAcceptanceInput(t, first, "SPR\r")
	sendAcceptanceInput(t, first, "FIRST_PANE_INPUT\n")
	awaitText(t, firstPump, size, "FIRST_PANE_INPUT")
	assertNoTextAfterInput(t, secondPump, size, "FIRST_PANE_INPUT")
	sendAcceptanceInput(t, second, "SECOND_TAB_INPUT\n")
	awaitText(t, secondPump, size, "SECOND_TAB_INPUT")

	// Overlays are attachment-owned: a palette on one wire and copy mode on
	// the other must not repaint the peer's viewport.
	sendAcceptanceInput(t, first, "\x1b ")
	awaitText(t, firstPump, size, "Commands")
	assertNoTextAfterInput(t, secondPump, size, "Commands")
	sendAcceptanceInput(t, first, "\x1b")

	sendAcceptanceInput(t, second, "\x1b ")
	awaitText(t, secondPump, size, "Commands")
	sendAcceptanceInput(t, second, "VIS\r")
	awaitText(t, secondPump, size, "[SCROLL]")
	assertNoTextAfterInput(t, firstPump, size, "[SCROLL]")

	// The resize keeps its attachment-local output stream while recalculating
	// shared PTY/content geometry from the latest valid attachment claim; the
	// peer stream remains independent.
	require.NoError(t, first.SendClient(protocol.Resize{Size: domain.Size{Cols: 100, Rows: 30}}))
	resized := awaitAcceptanceOutput(t, firstPump, func(output protocol.Output) bool { return output.Size == (domain.Size{Cols: 100, Rows: 30}) })
	require.Equal(t, domain.Size{Cols: 100, Rows: 30}, resized.Size)
	assertNoTextAfterInput(t, secondPump, size, "FIRST_PANE_INPUT")

	require.NoError(t, first.SendClient(protocol.OutputResetRequest{}))
	reset := awaitAcceptanceOutput(t, firstPump, func(output protocol.Output) bool { return output.Base == 0 && output.Full })
	require.True(t, reset.Full)
	assertNoTextAfterInput(t, secondPump, size, "FIRST_PANE_INPUT")

	require.NoError(t, first.SendClient(protocol.Detach{}))
	awaitAcceptanceFrame(t, firstPump, func(message protocol.ServerMessage) bool {
		_, ok := message.(protocol.Detached)
		return ok
	})

	sendAcceptanceInput(t, second, "q")
	sendAcceptanceInput(t, second, "PEER_SURVIVES\n")
	awaitText(t, secondPump, size, "PEER_SURVIVES")
}

func TestAcceptanceAttachedCommandUsesItsConnectionOnly(t *testing.T) {
	size := domain.Size{Cols: 80, Rows: 24}
	dir, _ := startDaemon(t, daemon.WithShell("/bin/cat", nil))
	first, firstPump := attach(t, dir, protocol.IntentNew, "commands", size)
	second, secondPump := attach(t, dir, protocol.IntentAttach, "commands", size)
	t.Cleanup(func() { _ = first.Close() })
	t.Cleanup(func() { _ = second.Close() })

	firstResult := awaitAcceptanceCommandResult(t, first, firstPump, 101, "new-tab")
	secondResult := awaitAcceptanceCommandResult(t, second, secondPump, 202, "new-tab")
	require.True(t, firstResult.OK, firstResult.Text)
	require.True(t, secondResult.OK, secondResult.Text)
	require.Empty(t, firstResult.Output)
	require.Empty(t, secondResult.Output)
	require.NotEqual(t, firstResult.RequestID, secondResult.RequestID)

	// Drain any asynchronous output before proving that a reset on one real
	// IPC transport does not create a reset frame on the other.
	for {
		select {
		case <-secondPump.ch:
		default:
			goto drained
		}
	}

drained:
	require.NoError(t, first.SendClient(protocol.OutputResetRequest{}))
	awaitAcceptanceOutput(t, firstPump, func(output protocol.Output) bool { return output.Full && output.Base == 0 })
	assertNoTextAfterInput(t, secondPump, size, "Commands")
}
