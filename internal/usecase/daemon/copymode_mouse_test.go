package daemon

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	scopy "github.com/bnema/vev/internal/usecase/copy"
)

type manualMouseClock struct{ now time.Time }

func (c *manualMouseClock) Now() time.Time                   { return c.now }
func (c *manualMouseClock) Advance(d time.Duration)          { c.now = c.now.Add(d) }
func (*manualMouseClock) NewTimer(time.Duration) ports.Timer { return stubTimer{} }

func mouseCopyHarness(t *testing.T, rows ...string) (*Daemon, *session, *attachedClient, *manualMouseClock) {
	t.Helper()
	p, release := newBlockingPTY(t)
	t.Cleanup(release)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	clock := &manualMouseClock{now: time.Unix(1, 0)}
	d.clock = clock
	pane := testAttachmentTab(sess).focusedPane()
	for i, row := range rows {
		writeTestRow(pane.screen, i, row)
	}
	d.enterCopyMode(sess, ac)
	return d, sess, ac, clock
}

func copyMouseInput(d *Daemon, sess *session, ac *attachedClient, raw string) {
	d.handleInput(sess, ac, []byte(raw))
}

func TestCopyDoubleClick(t *testing.T) {
	t.Run("same normalized position within interval selects word", func(t *testing.T) {
		d, sess, ac, clock := mouseCopyHarness(t, "a界 beta")
		pane := testAttachmentTab(sess).focusedPane()
		d.exitCopyMode(ac)
		pane.mu.Lock()
		require.True(t, pane.screen.Cell(2, 0).Continuation)
		pane.mu.Unlock()
		d.enterCopyMode(sess, ac)

		// SGR column 3 is the continuation cell of the wide glyph at column 2.
		copyMouseInput(d, sess, ac, "\x1b[<0;3;2M\x1b[<0;3;2m")
		clock.Advance(copyDoubleClickInterval)
		copyMouseInput(d, sess, ac, "\x1b[<0;3;2M")

		ac.overlays.copyMu.Lock()
		selection := ac.overlays.copyMode.Selection()
		pointer := ac.overlays.copyPointer
		ac.overlays.copyMu.Unlock()
		require.True(t, selection.Enabled)
		require.Equal(t, scopy.Word, selection.Granularity)
		require.Equal(t, scopy.Pos{Row: 0, Col: 1}, pointer.press)
	})

	for _, tc := range []struct {
		name    string
		advance time.Duration
		second  string
	}{
		{name: "expired", advance: copyDoubleClickInterval + time.Nanosecond, second: "\x1b[<0;1;2M"},
		{name: "different position", second: "\x1b[<0;2;2M"},
	} {
		t.Run(tc.name+" stays passive", func(t *testing.T) {
			d, sess, ac, clock := mouseCopyHarness(t, "alpha")
			copyMouseInput(d, sess, ac, "\x1b[<0;1;2M\x1b[<0;1;2m")
			clock.Advance(tc.advance)
			copyMouseInput(d, sess, ac, tc.second)
			ac.overlays.copyMu.Lock()
			selection := ac.overlays.copyMode.Selection()
			ac.overlays.copyMu.Unlock()
			require.False(t, selection.Enabled)
		})
	}

	t.Run("different pane is not a double click", func(t *testing.T) {
		d := newTestDaemon(t, nil, &manualMouseClock{now: time.Unix(1, 0)})
		rt := newOverlayRuntime(&attachedClient{})
		p1 := newPane("one", nil, domain.Size{Cols: 4, Rows: 1})
		p2 := newPane("two", nil, domain.Size{Cols: 4, Rows: 1})
		rt.copyClick = copyClickCandidate{valid: true, pane: p1, pos: scopy.Pos{Row: 0, Col: 1}, at: d.clock.Now()}
		require.False(t, d.isCopyDoubleClickLocked(rt, p2, scopy.Pos{Row: 0, Col: 1}, d.clock.Now()))
	})

	for _, text := range []string{"alpha beta", "alpha/beta"} {
		t.Run("separator is passive "+text, func(t *testing.T) {
			d, sess, ac, clock := mouseCopyHarness(t, text)
			if text == "alpha/beta" {
				cfg := domain.Defaults()
				cfg.Copy.WordSeparators = "/"
				d.ApplyConfig(cfg)
				d.exitCopyMode(ac)
				d.enterCopyMode(sess, ac)
			}
			copyMouseInput(d, sess, ac, "\x1b[<0;6;2M\x1b[<0;6;2m")
			clock.Advance(time.Millisecond)
			copyMouseInput(d, sess, ac, "\x1b[<0;6;2M")
			ac.overlays.copyMu.Lock()
			selection := ac.overlays.copyMode.Selection()
			ac.overlays.copyMu.Unlock()
			require.False(t, selection.Enabled)
		})
	}

	t.Run("normal screen double click publishes word drag", func(t *testing.T) {
		d, sess, ac, clock := mouseCopyHarness(t, "alpha beta")
		d.exitCopyMode(ac)
		copyMouseInput(d, sess, ac, "\x1b[<0;1;2M\x1b[<0;1;2m")
		clock.Advance(time.Millisecond)
		copyMouseInput(d, sess, ac, "\x1b[<0;1;2M\x1b[<32;7;2M")
		ac.overlays.copyMu.Lock()
		selection := ac.overlays.copyMode.Selection()
		text := ac.overlays.copyMode.SelectedText()
		pointer := ac.overlays.copyPointer
		ac.overlays.copyMu.Unlock()
		require.Equal(t, scopy.Word, selection.Granularity)
		require.True(t, pointer.wordDrag)
		require.Equal(t, "alpha beta", text)
	})

	t.Run("first click drag invalidates candidate", func(t *testing.T) {
		d, sess, ac, clock := mouseCopyHarness(t, "alpha")
		copyMouseInput(d, sess, ac, "\x1b[<0;1;2M\x1b[<32;2;2M\x1b[<0;2;2m")
		clock.Advance(time.Millisecond)
		copyMouseInput(d, sess, ac, "\x1b[<0;1;2M")
		ac.overlays.copyMu.Lock()
		selection := ac.overlays.copyMode.Selection()
		ac.overlays.copyMu.Unlock()
		require.NotEqual(t, scopy.Word, selection.Granularity)
	})
}

func TestCopyWordDragKeepsWordAnchorOnFirstMotion(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		pressCol, motionCol    int
		wantAnchor, wantActive scopy.Pos
	}{
		{
			name:       "forward from inside first word",
			pressCol:   3,
			motionCol:  8,
			wantAnchor: scopy.Pos{Row: 0},
			wantActive: scopy.Pos{Row: 0, Col: 9},
		},
		{
			name:       "reverse from inside second word",
			pressCol:   8,
			motionCol:  3,
			wantAnchor: scopy.Pos{Row: 0, Col: 6},
			wantActive: scopy.Pos{Row: 0, Col: 4},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, clock := mouseCopyHarness(t, "alpha beta")
			copyMouseInput(d, sess, ac, "\x1b[<0;"+strconv.Itoa(tc.pressCol)+";2M\x1b[<0;"+strconv.Itoa(tc.pressCol)+";2m")
			clock.Advance(time.Millisecond)
			copyMouseInput(d, sess, ac, "\x1b[<0;"+strconv.Itoa(tc.pressCol)+";2M\x1b[<32;"+strconv.Itoa(tc.motionCol)+";2M")

			ac.overlays.copyMu.Lock()
			selection := ac.overlays.copyMode.Selection()
			text := ac.overlays.copyMode.SelectedText()
			pointer := ac.overlays.copyPointer
			ac.overlays.copyMu.Unlock()
			require.Equal(t, scopy.Word, selection.Granularity)
			require.Equal(t, tc.wantAnchor, selection.Anchor)
			require.Equal(t, tc.wantActive, selection.Active)
			require.Equal(t, "alpha beta", text)
			require.True(t, pointer.dragging)
			require.True(t, pointer.wordDrag)
		})
	}
}

func TestCopyCharacterDragStartsAtPress(t *testing.T) {
	d, sess, ac, _ := mouseCopyHarness(t, "alpha beta")
	copyMouseInput(d, sess, ac, "\x1b[<0;3;2M\x1b[<32;8;2M")

	ac.overlays.copyMu.Lock()
	selection := ac.overlays.copyMode.Selection()
	pointer := ac.overlays.copyPointer
	ac.overlays.copyMu.Unlock()
	require.Equal(t, scopy.Character, selection.Granularity)
	require.Equal(t, scopy.Pos{Row: 0, Col: 2}, selection.Anchor)
	require.Equal(t, scopy.Pos{Row: 0, Col: 7}, selection.Active)
	require.True(t, pointer.dragging)
	require.False(t, pointer.wordDrag)
}

func TestCopySearchReleaseInvalidatesPointerBeforeNextDrag(t *testing.T) {
	d, sess, ac, _ := mouseCopyHarness(t, "alpha bravo")

	copyMouseInput(d, sess, ac, "\x1b[<0;1;2M")
	ac.overlays.copyMu.Lock()
	require.True(t, ac.overlays.copyPointer.valid)
	epoch := ac.overlays.copyPointerEpoch
	ac.overlays.copyMu.Unlock()

	copyMouseInput(d, sess, ac, "/")
	require.True(t, ac.overlays.copySearchActive())
	copyMouseInput(d, sess, ac, "\x1b[<0;1;2m")

	ac.overlays.copyMu.Lock()
	require.False(t, ac.overlays.copyPointer.valid, "release must clear the pointer while search owns mouse input")
	require.Greater(t, ac.overlays.copyPointerEpoch, epoch, "release must advance the pointer epoch")
	ac.overlays.copyMu.Unlock()

	copyMouseInput(d, sess, ac, "\x03")
	require.False(t, ac.overlays.copySearchActive())
	copyMouseInput(d, sess, ac, "\x1b[<0;7;2M\x1b[<32;9;2M")

	ac.overlays.copyMu.Lock()
	selection := ac.overlays.copyMode.Selection()
	ac.overlays.copyMu.Unlock()
	require.True(t, selection.Enabled)
	require.Equal(t, scopy.Pos{Row: 0, Col: 6}, selection.Anchor)
	require.Equal(t, scopy.Pos{Row: 0, Col: 8}, selection.Active)
}

func TestNormalScreenPassiveDoubleClickSetsMappedCursor(t *testing.T) {
	for _, tc := range []struct {
		name  string
		text  string
		setup func(*Daemon)
	}{
		{name: "whitespace", text: "alpha beta"},
		{name: "configured separator", text: "alpha/beta", setup: func(d *Daemon) {
			cfg := domain.Defaults()
			cfg.Copy.WordSeparators = "/"
			d.ApplyConfig(cfg)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, clock := mouseCopyHarness(t, tc.text)
			d.exitCopyMode(ac)
			if tc.setup != nil {
				tc.setup(d)
			}

			copyMouseInput(d, sess, ac, "\x1b[<0;6;2M\x1b[<0;6;2m")
			clock.Advance(time.Millisecond)
			copyMouseInput(d, sess, ac, "\x1b[<0;6;2M")

			ac.overlays.copyMu.Lock()
			mode := ac.overlays.copyMode
			selection := mode.Selection()
			cursor := mode.Cursor()
			ac.overlays.copyMu.Unlock()
			require.False(t, selection.Enabled)
			require.Equal(t, scopy.Pos{Row: 0, Col: 5}, cursor)
		})
	}
}

func TestCopyWordDragAndOSC52(t *testing.T) {
	d, sess, ac, clock := mouseCopyHarness(t, "alpha beta", "gamma delta")
	copyMouseInput(d, sess, ac, "\x1b[<0;1;2M\x1b[<0;1;2m")
	clock.Advance(time.Millisecond)
	copyMouseInput(d, sess, ac, "\x1b[<0;1;2M\x1b[<32;2;3M\x1b[<0;2;3m")

	ac.overlays.copyMu.Lock()
	mode := ac.overlays.copyMode
	selection := mode.Selection()
	text := mode.SelectedText()
	ac.overlays.copyMu.Unlock()
	want := "alpha beta\ngamma"
	require.Equal(t, scopy.Word, selection.Granularity)
	require.Equal(t, want, text)
	require.Equal(t, scopy.OSC52(want)[0], scopy.OSC52(text)[0])

	copyMouseInput(d, sess, ac, "y")
	require.False(t, ac.overlays.copyActive())
}

func seedCopyInteractionLocked(rt *overlayRuntime, p *pane, doc *scopy.Document) {
	rt.beginCopyPointerLocked(copyPointerState{pane: p, document: doc})
	rt.copyClick = copyClickCandidate{valid: true, pane: p}
}

func TestCopyPointerResetClearsClick(t *testing.T) {
	p := newPane("pane", nil, domain.Size{Cols: 4, Rows: 1})
	doc := scopy.NewDocument(scopy.NewSnapshotFromRows([][]renderer.Cell{testRow("word")}, 4, 1), domain.DefaultWordSeparators)
	for _, tc := range []struct {
		name        string
		reset       func(*overlayRuntime)
		holdsCopyMu bool
	}{
		{name: "clear mode", reset: func(rt *overlayRuntime) { rt.clearCopyModeLocked() }, holdsCopyMu: true},
		{name: "replace publication", reset: func(rt *overlayRuntime) { rt.invalidateCopyPointerLocked(true) }, holdsCopyMu: true},
		{name: "pane close", reset: func(rt *overlayRuntime) { rt.clearCopyModeForPane(p) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := newOverlayRuntime(&attachedClient{})
			rt.copyMu.Lock()
			rt.copyPane, rt.copyDocument, rt.copyMode = p, doc, scopy.NewMode(doc)
			seedCopyInteractionLocked(rt, p, doc)
			if !tc.holdsCopyMu {
				rt.copyMu.Unlock()
				tc.reset(rt)
				rt.copyMu.Lock()
			} else {
				tc.reset(rt)
			}
			require.False(t, rt.copyPointer.valid)
			require.False(t, rt.copyClick.valid)
			rt.copyMu.Unlock()
		})
	}
}

func TestCopyWordSelectionYanksExactOSC52(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	clock := &manualMouseClock{now: time.Unix(1, 0)}
	d.clock = clock
	writeTestRow(testAttachmentTab(sess).focusedPane().screen, 0, "alpha beta")
	d.enterCopyMode(sess, ac)
	mustOutputData(t, sends)

	copyMouseInput(d, sess, ac, "\x1b[<0;1;2M\x1b[<0;1;2m")
	clock.Advance(time.Millisecond)
	copyMouseInput(d, sess, ac, "\x1b[<0;1;2M")
	mustOutputData(t, sends)
	copyMouseInput(d, sess, ac, "y")
	// The selection repaint can have been queued before the synchronous OSC 52
	// output; consume it, then assert the wire payload itself byte-for-byte.
	mustOutputData(t, sends)
	require.Equal(t, scopy.OSC52("alpha")[0], mustOutputData(t, sends))
}

func TestCopyPointerAndClickResetOnCopyExitAndReplacement(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*Daemon, *session, *attachedClient)
	}{
		{name: "q", run: func(d *Daemon, _ *session, ac *attachedClient) { d.handleCopyInput(ac, []byte("q")) }},
		{name: "escape", run: func(d *Daemon, _ *session, ac *attachedClient) { d.handleCopyInput(ac, []byte("\x1bx")) }},
		{name: "yank", run: func(d *Daemon, _ *session, ac *attachedClient) { d.handleCopyInput(ac, []byte("y")) }},
		{name: "replacement publication", run: func(d *Daemon, sess *session, ac *attachedClient) {
			p := testAttachmentTab(sess).focusedPane()
			p.mu.Lock()
			doc := scopy.NewDocument(scopy.NewSnapshot(p.history, p.screen, p.screen.LineBounds(), nil), domain.DefaultWordSeparators)
			p.mu.Unlock()
			require.True(t, d.publishCopyMode(sess, ac, testAttachmentTab(sess), p, doc, nil, nil))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, _ := mouseCopyHarness(t, "word")
			ac.overlays.copyMu.Lock()
			seedCopyInteractionLocked(ac.overlays, ac.overlays.copyPane, ac.overlays.copyDocument)
			ac.overlays.copyMu.Unlock()
			tc.run(d, sess, ac)
			ac.overlays.copyMu.Lock()
			require.False(t, ac.overlays.copyPointer.valid)
			require.False(t, ac.overlays.copyClick.valid)
			ac.overlays.copyMu.Unlock()
		})
	}
}
