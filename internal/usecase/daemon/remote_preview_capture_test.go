package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

func TestCaptureRemotePreviewUsesBottomRowsForShorterPreview(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = remoteLifecycleForTest()
	pane := sess.tabs[0].focusedPane()
	pane.mu.Lock()
	pane.screen = vt.NewScreen(2, 3)
	pane.screen.Frame.Set(0, 0, renderer.Cell{Rune: 'a'})
	pane.screen.Frame.Set(0, 1, renderer.Cell{Rune: 'b'})
	pane.screen.Frame.Set(0, 2, renderer.Cell{Rune: 'c'})
	pane.mu.Unlock()

	target := domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: sess.incarnation,
		SessionName: "work", LiveTabID: "tab-1",
	}
	preview, err := d.captureRemotePreview(ports.RemotePreviewRequest{
		Version: ports.RemotePreviewSchemaVersion, Target: target, Width: 1, Height: 2,
	})
	require.NoError(t, err)
	require.Equal(t, []rune{'b', 'c'}, []rune{preview.Cells[0].Rune, preview.Cells[1].Rune})
}

func TestCaptureRemotePreviewCompactsBlankTailToKeepShortScreenContentVisible(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = remoteLifecycleForTest()
	pane := sess.tabs[0].focusedPane()
	pane.mu.Lock()
	pane.screen = vt.NewScreen(2, 4)
	pane.screen.Frame.Set(0, 0, renderer.Cell{Rune: 'v'})
	pane.mu.Unlock()

	target := domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: sess.incarnation,
		SessionName: "work", LiveTabID: "tab-1",
	}
	preview, err := d.captureRemotePreview(ports.RemotePreviewRequest{
		Version: ports.RemotePreviewSchemaVersion, Target: target, Width: 2, Height: 2,
	})
	require.NoError(t, err)
	require.Equal(t, uint16(1), preview.Height)
	require.Equal(t, []rune{'v', ' '}, []rune{preview.Cells[0].Rune, preview.Cells[1].Rune})
}

func TestCaptureRemotePreviewIgnoresBoundarySplitWideRuneWhenCompacting(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = remoteLifecycleForTest()
	pane := sess.tabs[0].focusedPane()
	pane.mu.Lock()
	pane.screen = vt.NewScreen(3, 3)
	pane.screen.Frame.Set(0, 1, renderer.Cell{Rune: 'a'})
	pane.screen.Frame.Set(1, 2, renderer.Cell{Rune: '界'})
	pane.screen.Frame.Set(2, 2, renderer.Cell{Continuation: true})
	pane.mu.Unlock()

	target := domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: sess.incarnation,
		SessionName: "work", LiveTabID: "tab-1",
	}
	preview, err := d.captureRemotePreview(ports.RemotePreviewRequest{
		Version: ports.RemotePreviewSchemaVersion, Target: target, Width: 2, Height: 2,
	})
	require.NoError(t, err)
	require.Equal(t, uint16(1), preview.Height)
	require.Equal(t, []rune{'a', ' '}, []rune{preview.Cells[0].Rune, preview.Cells[1].Rune})
}

func TestCaptureRemotePreviewPreservesStyledBlankTail(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = remoteLifecycleForTest()
	pane := sess.tabs[0].focusedPane()
	style := renderer.DefaultStyle()
	style.Background = 4
	pane.mu.Lock()
	pane.screen = vt.NewScreen(2, 4)
	pane.screen.Frame.Set(0, 2, renderer.Cell{Rune: 'a'})
	pane.screen.Frame.Set(0, 3, renderer.Cell{Rune: ' ', Style: style})
	pane.mu.Unlock()

	target := domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: sess.incarnation,
		SessionName: "work", LiveTabID: "tab-1",
	}
	preview, err := d.captureRemotePreview(ports.RemotePreviewRequest{
		Version: ports.RemotePreviewSchemaVersion, Target: target, Width: 2, Height: 2,
	})
	require.NoError(t, err)
	require.Equal(t, uint16(2), preview.Height)
	require.Equal(t, rune('a'), preview.Cells[0].Rune)
	require.Equal(t, rune(' '), preview.Cells[2].Rune)
	require.True(t, preview.Cells[2].Style.Equal(style))
}

func TestStaticRemotePickerPreviewUsesOnlyItsTextRow(t *testing.T) {
	preview := staticRemotePickerPreview(8, 4, "loading")

	require.Equal(t, 8, preview.Width)
	require.Equal(t, 1, preview.Height)
	require.Equal(t, []rune{'l', 'o', 'a', 'd', 'i', 'n', 'g', ' '}, []rune{
		preview.Rows[0][0].Rune, preview.Rows[0][1].Rune, preview.Rows[0][2].Rune, preview.Rows[0][3].Rune,
		preview.Rows[0][4].Rune, preview.Rows[0][5].Rune, preview.Rows[0][6].Rune, preview.Rows[0][7].Rune,
	})
}

func TestCaptureRemotePreviewDoesNotSplitWideRuneAtCropBoundary(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "tab-1", "pane-1")
	sess.ephemeral = false
	sess.incarnation = remoteLifecycleForTest()
	pane := sess.tabs[0].focusedPane()
	pane.mu.Lock()
	pane.screen = vt.NewScreen(3, 1)
	style := renderer.DefaultStyle()
	style.Bold = true
	pane.screen.Frame.Set(1, 0, renderer.Cell{Rune: '界', Style: style})
	pane.screen.Frame.Set(2, 0, renderer.Cell{Continuation: true, Style: style})
	pane.mu.Unlock()

	target := domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: sess.incarnation,
		SessionName: "work", LiveTabID: "tab-1",
	}
	preview, err := d.captureRemotePreview(ports.RemotePreviewRequest{
		Version: ports.RemotePreviewSchemaVersion, Target: target, Width: 2, Height: 1,
	})
	require.NoError(t, err)
	require.Equal(t, uint16(2), preview.Width)
	require.Equal(t, rune(' '), preview.Cells[1].Rune, "a crop cannot expose a dangling wide rune")
	require.False(t, preview.Cells[1].Continuation)
	require.True(t, preview.Cells[1].Style.Equal(style), "boundary replacement preserves the cell style")
	require.NoError(t, ports.ValidateRemotePreview(preview))
}
