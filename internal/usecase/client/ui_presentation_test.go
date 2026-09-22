package client

import (
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

// newPresentationTestUI builds one UI service over a real VT terminal with no
// foreground bound, exactly as a composition does before any attachment.
func newPresentationTestUI(t *testing.T) (*UI, *uiterm.Terminal) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 8, Rows: 2}}, "")
	require.NoError(t, err)
	t.Cleanup(terminal.Close)
	return NewUI(terminal, systemClock{}), terminal
}

// TestUIPublishPresentationUnattachedShape pins the published context of the two
// unattached presentations: the run's stable handle and the status alone. Picker
// and Connecting carry no session identity, no committed output boundary, and no
// actionable generation, and publishing them never advances the internal action
// generation counter, so no caller can ever fence an action against an invented
// generation.
func TestUIPublishPresentationUnattachedShape(t *testing.T) {
	ui, _ := newPresentationTestUI(t)
	for _, status := range []ports.UIPresentationStatus{ports.UIStatusPicker, ports.UIStatusConnecting} {
		t.Run(string(status), func(t *testing.T) {
			require.NoError(t, ui.PublishPresentation(status))
			snapshot, err := ui.Capture(ui.Handle())
			require.NoError(t, err)
			require.Equal(t, ports.UIContext{AttachmentHandle: ui.Handle(), Status: status}, snapshot.Context)
			ui.mu.Lock()
			generation := ui.generation
			ui.mu.Unlock()
			require.Zero(t, generation, "an unattached publication never allocates an action generation")
		})
	}

	// Attached is never publishable through the composition seam: only the
	// admitted attachment foreground may publish it.
	require.Error(t, ui.PublishPresentation(ports.UIStatusAttached))
}

// TestUIActionsRefusedOutsideAttached pins that keys and text are attachment
// actions: they are refused with unavailable while the published presentation is
// Picker or Connecting, even when a generation-shaped request is supplied.
func TestUIActionsRefusedOutsideAttached(t *testing.T) {
	ui, _ := newPresentationTestUI(t)
	for _, status := range []ports.UIPresentationStatus{ports.UIStatusPicker, ports.UIStatusConnecting} {
		t.Run(string(status), func(t *testing.T) {
			require.NoError(t, ui.PublishPresentation(status))
			for _, request := range []ports.UIActionRequest{
				{Attachment: ui.Handle(), Generation: 1, Keys: []string{"Escape"}},
				{Attachment: ui.Handle(), Generation: 1, Text: "hello"},
			} {
				_, err := ui.Action(t.Context(), request)
				var uiErr *ports.UIError
				require.ErrorAs(t, err, &uiErr)
				require.Equal(t, ports.UIErrUnavailable, uiErr.Code)
			}
		})
	}
}

// TestUICaptureAndWaitWorkInEveryPresentation pins that capture and wait are
// available in all three presentations: an unattached presentation is a state a
// caller may observe and wait for, and a wait may name it as its predicate.
func TestUICaptureAndWaitWorkInEveryPresentation(t *testing.T) {
	ui, _ := newPresentationTestUI(t)
	picker := ports.UIStatusPicker
	require.NoError(t, ui.PublishPresentation(picker))
	require.NotEmpty(t, presentationCapture(t, ui).Context.AttachmentHandle)
	matched, err := ui.Wait(t.Context(), ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &picker}})
	require.NoError(t, err)
	require.Equal(t, picker, matched.Context.Status)

	connecting := ports.UIStatusConnecting
	require.NoError(t, ui.PublishPresentation(connecting))
	require.NotEmpty(t, presentationCapture(t, ui).Context.AttachmentHandle)
	matched, err = ui.Wait(t.Context(), ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &connecting}})
	require.NoError(t, err)
	require.Equal(t, connecting, matched.Context.Status)
	require.Zero(t, matched.Context.Generation)

	attached := ports.UIStatusAttached
	require.NoError(t, ui.state.(ports.UIOutputTransaction).PublishContext(ports.UIContext{
		AttachmentHandle: ui.Handle(), Generation: 7, Status: attached, OutputEpoch: 1, OutputState: 1, ViewPublication: 1,
	}))
	require.NotEmpty(t, presentationCapture(t, ui).Context.AttachmentHandle)
	matched, err = ui.Wait(t.Context(), ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached}})
	require.NoError(t, err)
	require.Equal(t, attached, matched.Context.Status)
	require.Equal(t, uint64(7), matched.Context.Generation)
}

func presentationCapture(t *testing.T, ui *UI) ports.UISnapshot {
	t.Helper()
	snapshot, err := ui.Capture(ui.Handle())
	require.NoError(t, err)
	return snapshot
}

// TestUIWaitWakesOnPresentationOnlyPublication pins that a wait predicate is
// woken by a presentation-only publication, not merely by daemon frames: the UI
// observation signal covers committed contexts.
func TestUIWaitWakesOnPresentationOnlyPublication(t *testing.T) {
	ui, _ := newPresentationTestUI(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); ui.Observe(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("the observation worker did not stop")
		}
	})

	connecting := ports.UIStatusConnecting
	result := make(chan ports.UIWaitResult, 1)
	go func() {
		matched, err := ui.Wait(t.Context(), ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &connecting}})
		if err == nil {
			result <- matched
		}
	}()
	require.Eventually(t, func() bool {
		ui.mu.Lock()
		waits := ui.waits
		ui.mu.Unlock()
		return waits == 1
	}, time.Second, time.Millisecond, "the wait was never admitted")
	require.NoError(t, ui.PublishPresentation(connecting))
	select {
	case matched := <-result:
		require.Equal(t, connecting, matched.Context.Status)
	case <-time.After(time.Second):
		t.Fatal("a presentation-only publication did not wake the wait")
	}
}
