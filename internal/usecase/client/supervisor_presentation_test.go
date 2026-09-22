package client

import (
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

// TestSupervisorPublishesConnectingBeforeGrantAndPickerAfterDrain pins the
// supervisor's publication ownership: it publishes Connecting before the
// attachment foreground is granted (so a capture during the attempt sees
// Connecting, never a stale attachment context), and Picker only after the
// attachment run drained, revoked, and closed its stream. Attached is never
// published by the supervisor: it belongs to the admitted foreground.
func TestSupervisorPublishesConnectingBeforeGrantAndPickerAfterDrain(t *testing.T) {
	picker := newAttachTestPicker()
	var ui *UI
	harness := startAttachHarnessConfig(t, picker, func(cfg *SupervisorConfig) {
		ui = NewUI(cfg.Terminal.(ports.UIState), cfg.Clock)
		cfg.UI = ui
	})
	stream := newSessionTestStream()
	harness.service.setOpenStream(func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
		return stream, nil
	})

	picker.commit(sessionTestRequest(true))
	awaitHello(t, stream)

	// The Connecting presentation is published by the supervisor before the
	// foreground commits any frame, so the composition's recorded publication
	// order starts with an unattached Connecting context.
	terminal := harness.terminal
	require.Eventually(t, func() bool {
		for _, publication := range terminal.publicationList() {
			if publication.Status == ports.UIStatusConnecting {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond, "the supervisor must publish Connecting before the grant writes")
	publications := terminal.publicationList()
	require.Zero(t, publications[0].Generation, "Connecting never carries an actionable generation")
	require.Equal(t, ui.Handle(), publications[0].AttachmentHandle)
	require.Zero(t, publications[0].OutputState, "Connecting never carries a committed output boundary")

	deliverReadyStream(t, stream)
	awaitAttachedState(t, harness.sup)
	attached := terminal.publicationList()
	last := attached[len(attached)-1]
	require.Equal(t, ports.UIStatusAttached, last.Status)
	require.NotZero(t, last.Generation, "the committed attachment publishes its real action generation")
	require.Equal(t, uint64(1), last.OutputState, "the attached publication carries the committed frame boundary")

	// A broker loss settles the attachment; the supervisor returns to Picker
	// only after the run drained and revoked.
	harness.service.lose(errTestPresentationLoss)
	awaitPickerState(t, harness.sup)
	require.Eventually(t, func() bool {
		list := terminal.publicationList()
		return list[len(list)-1].Status == ports.UIStatusPicker
	}, 5*time.Second, time.Millisecond, "the supervisor publishes Picker after the drain")
	returned := terminal.publicationList()
	last = returned[len(returned)-1]
	require.Equal(t, ports.UIStatusPicker, last.Status)
	require.Zero(t, last.Generation, "the returned Picker presentation carries no actionable generation")
	require.Zero(t, last.OutputState, "the returned Picker presentation carries no session metadata")
	ui.mu.Lock()
	require.Nil(t, ui.input, "the UI binding is revoked before Picker is presented")
	require.Nil(t, ui.foreground, "the UI foreground is retired before Picker is presented")
	ui.mu.Unlock()
}

var errTestPresentationLoss = errPresentationLossForTest()

func errPresentationLossForTest() error {
	return &ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "attachment lost"}
}
