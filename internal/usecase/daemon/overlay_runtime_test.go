package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	promptui "github.com/bnema/vev/internal/usecase/prompt"
	"github.com/bnema/vev/internal/usecase/visualsearch"
	"github.com/bnema/vev/pkg/renderer"
)

func TestOverlayRuntimeActive(t *testing.T) {
	ac := &attachedClient{}
	ac.initOverlays()
	require.False(t, ac.overlays.Active())

	ac.overlays.promptMu.Lock()
	ac.overlays.prompt = promptui.New(" Test ", "")
	ac.overlays.promptMu.Unlock()
	require.True(t, ac.overlays.Active())
}

func TestOverlayRuntimeHandleInputPrecedence(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, d *Daemon, sess *session, ac *attachedClient)
		input []byte
		check func(t *testing.T, ac *attachedClient)
	}{
		{
			name: "prompt before palette picker copy",
			setup: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				t.Helper()
				d.enterCopyMode(sess, ac)
				d.enterPicker(sess, ac)
				d.enterPalette(sess, ac)
				d.enterPrompt(sess, ac, "Rename", "", func(string) error { return nil })
			},
			input: []byte("x"),
			check: func(t *testing.T, ac *attachedClient) {
				t.Helper()
				require.Equal(t, "x", ac.overlays.prompt.Value())
				require.Equal(t, "", ac.overlays.palette.Query())
				require.True(t, ac.overlays.pickerActive())
				require.True(t, ac.overlays.copyActive())
			},
		},
		{
			name: "palette before picker copy",
			setup: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				t.Helper()
				d.enterCopyMode(sess, ac)
				d.enterPicker(sess, ac)
				d.enterPalette(sess, ac)
			},
			input: []byte("x"),
			check: func(t *testing.T, ac *attachedClient) {
				t.Helper()
				require.Equal(t, "x", ac.overlays.palette.Query())
				require.True(t, ac.overlays.pickerActive())
				require.True(t, ac.overlays.copyActive())
			},
		},
		{
			name: "picker before copy",
			setup: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				t.Helper()
				d.enterCopyMode(sess, ac)
				d.enterPicker(sess, ac)
			},
			input: []byte("j"),
			check: func(t *testing.T, ac *attachedClient) {
				t.Helper()
				target, ok := ac.overlays.picker.Selected()
				require.True(t, ok)
				require.Equal(t, 1, target.TabIndex)
				require.True(t, ac.overlays.copyActive())
			},
		},
		{
			name: "copy",
			setup: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				t.Helper()
				d.enterCopyMode(sess, ac)
			},
			input: []byte("q"),
			check: func(t *testing.T, ac *attachedClient) {
				t.Helper()
				require.False(t, ac.overlays.copyActive())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p1, releasePTY1 := newBlockingPTY(t)
			p2, releasePTY2 := newBlockingPTY(t)
			d, sess, ac, _ := newManualSessionWithPTYs(t, p1, p2)
			defer releasePTY1()
			defer releasePTY2()
			tc.setup(t, d, sess, ac)

			require.True(t, ac.overlays.HandleInput(d, tc.input))

			tc.check(t, ac)
		})
	}
}

func TestOverlayRuntimeSnapshotDoesNotAliasCopySearchState(t *testing.T) {
	ac := &attachedClient{}
	ac.initOverlays()
	snap := scopy.Snapshot{Rows: [][]renderer.Cell{testRow("alpha"), testRow("beta alpha")}, Width: 16, Height: 2}
	mode := scopy.NewMode(snap)
	require.True(t, mode.Search(snap, "alpha"))
	search := visualsearch.New(snap)
	for _, r := range "alpha" {
		search.Insert(r)
	}

	ac.overlays.copyMu.Lock()
	ac.overlays.copyMode = mode
	ac.overlays.copySearch = search
	ac.overlays.copyMu.Unlock()

	renderSnap := ac.overlays.SnapshotForRender()
	defer renderSnap.Unlock()

	ac.overlays.copyMu.Lock()
	ac.overlays.copyMode.Searches[0].Row = 99
	ac.overlays.copySearch.Insert('z')
	ac.overlays.copyMu.Unlock()

	require.Equal(t, 0, renderSnap.copyMode.Searches[0].Row)
	require.Equal(t, "alpha", renderSnap.copySearchModel.Query())
}

func TestOverlayRuntimeHandleInputInactive(t *testing.T) {
	ac := &attachedClient{}
	ac.initOverlays()

	require.False(t, ac.overlays.HandleInput(&Daemon{}, []byte("x")))
}

func TestOverlayRuntimeSnapshotCarriesCopyPaneAndClearReleasesIt(t *testing.T) {
	ac := &attachedClient{}
	ac.initOverlays()
	p := newPane("floating", nil, domain.Size{Cols: 4, Rows: 2})
	snap := scopy.Snapshot{Rows: [][]renderer.Cell{testRow("row")}, Width: 4, Height: 2}

	ac.overlays.copyMu.Lock()
	ac.overlays.copyMode = scopy.NewMode(snap)
	ac.overlays.copyPane = p
	ac.overlays.copyMu.Unlock()

	renderSnap := ac.overlays.SnapshotForRender()
	require.Same(t, p, renderSnap.copyPane)
	renderSnap.Unlock()

	ac.overlays.copyMu.Lock()
	ac.overlays.clearCopyModeLocked()
	require.Nil(t, ac.overlays.copyPane)
	require.Nil(t, ac.overlays.copyMode)
	ac.overlays.copyMu.Unlock()
}
