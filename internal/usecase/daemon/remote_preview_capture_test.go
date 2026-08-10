package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
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

func TestCaptureRemotePreviewCompactsBlankTail(t *testing.T) {
	type screenCell struct {
		x, y int
		cell renderer.Cell
	}
	type previewCell struct {
		index int
		rune  rune
		style *renderer.Style
	}
	styledBlank := renderer.DefaultStyle()
	styledBlank.Background = 4
	tests := []struct {
		name                      string
		screenWidth, screenHeight int
		cells                     []screenCell
		width, height             uint16
		wantHeight                uint16
		wantCells                 []previewCell
	}{
		{
			name:         "keeps short screen content visible",
			screenWidth:  2,
			screenHeight: 4,
			cells:        []screenCell{{x: 0, y: 0, cell: renderer.Cell{Rune: 'v'}}},
			width:        2,
			height:       2,
			wantHeight:   1,
			wantCells:    []previewCell{{index: 0, rune: 'v'}, {index: 1, rune: ' '}},
		},
		{
			name:         "ignores boundary split wide rune",
			screenWidth:  3,
			screenHeight: 3,
			cells: []screenCell{
				{x: 0, y: 1, cell: renderer.Cell{Rune: 'a'}},
				{x: 1, y: 2, cell: renderer.Cell{Rune: '界'}},
				{x: 2, y: 2, cell: renderer.Cell{Continuation: true}},
			},
			width:      2,
			height:     2,
			wantHeight: 1,
			wantCells:  []previewCell{{index: 0, rune: 'a'}, {index: 1, rune: ' '}},
		},
		{
			name:         "preserves styled blank tail",
			screenWidth:  2,
			screenHeight: 4,
			cells: []screenCell{
				{x: 0, y: 2, cell: renderer.Cell{Rune: 'a'}},
				{x: 0, y: 3, cell: renderer.Cell{Rune: ' ', Style: styledBlank}},
			},
			width:      2,
			height:     2,
			wantHeight: 2,
			wantCells:  []previewCell{{index: 0, rune: 'a'}, {index: 2, rune: ' ', style: &styledBlank}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			sess := addControlSession(d, "work", "tab-1", "pane-1")
			sess.ephemeral = false
			sess.incarnation = remoteLifecycleForTest()
			pane := sess.tabs[0].focusedPane()
			pane.mu.Lock()
			pane.screen = vt.NewScreen(test.screenWidth, test.screenHeight)
			for _, cell := range test.cells {
				pane.screen.Frame.Set(cell.x, cell.y, cell.cell)
			}
			pane.mu.Unlock()

			target := domain.RemoteSessionTarget{
				Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: sess.incarnation,
				SessionName: "work", LiveTabID: "tab-1",
			}
			preview, err := d.captureRemotePreview(ports.RemotePreviewRequest{
				Version: ports.RemotePreviewSchemaVersion, Target: target, Width: test.width, Height: test.height,
			})
			require.NoError(t, err)
			require.Equal(t, test.wantHeight, preview.Height)
			for _, want := range test.wantCells {
				require.Equal(t, want.rune, preview.Cells[want.index].Rune)
				if want.style != nil {
					require.True(t, preview.Cells[want.index].Style.Equal(*want.style))
				}
			}
		})
	}
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
