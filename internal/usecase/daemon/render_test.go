package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
)

func TestPaletteBackdropDimsSimultaneousCopyMode(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	theme := backdropTheme()
	ac.setThemeForTest(theme)
	client := vt.NewScreen(80, 25)
	pane := sess.tabs[0].focusedPane()
	pane.screen.Write([]byte("\x1b[38;2;180;90;30mX"))

	d.enterCopyMode(sess, ac)
	mustApplyOutput(t, client, awaitFrame(t, sends, wire.MsgOutput))
	undimmed := client.Cell(0, 1)
	require.Equal(t, 'X', undimmed.Rune, "fixture must address copy-mode pane content")
	copyBar := client.RowCells(client.Rows() - 1)
	require.Contains(t, rowText(copyBar), "[SCROLL]", "fixture must capture the copy status bar")

	d.enterPalette(sess, ac)
	mustApplyOutput(t, client, awaitFrame(t, sends, wire.MsgOutput))
	dimmed := client.Cell(0, 1)
	require.Equal(t, undimmed.Rune, dimmed.Rune, "palette backdrop must preserve copy content")
	require.Equal(t, themeui.NewDimmer(theme).Dim(undimmed.Style).Canonical(), dimmed.Style.Canonical(), "palette backdrop must dim the composed copy frame")
	for x, cell := range copyBar {
		cell.Style = themeui.NewDimmer(theme).Dim(cell.Style).Canonical()
		require.Equal(t, cell, client.Cell(x, client.Rows()-1), "copy status bar must be part of the complete backdrop")
	}
	paletteVisible := false
	for y := range client.Rows() {
		paletteVisible = paletteVisible || strings.Contains(rowText(client.RowCells(y)), "Commands")
	}
	require.True(t, paletteVisible, "palette must remain composed above copy mode")
}

func TestFirstPaintRetainedFloatingPaneEmitsOneReset(t *testing.T) {
	for _, tc := range []struct {
		name       string
		clientSize domain.Size
	}{
		{name: "matching outer size", clientSize: domain.Size{Cols: 80, Rows: 25}},
		{name: "different outer size", clientSize: domain.Size{Cols: 100, Rows: 40}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pty, releasePTY := newBlockingPTY(t)
			defer releasePTY()
			d, sess, ac, sends := newManualSessionWithPTYs(t, pty)
			ac.sendMu.Lock()
			ac.size = tc.clientSize
			ac.sendMu.Unlock()

			// A retained visible popup still needs activation warmup. When the
			// outer terminal also changes, its completed transaction must cover
			// that popup rather than emitting a second reset-producing frame.
			floating := newPane(layout.PaneID("floating"), nil, domain.Size{Cols: 20, Rows: 8})
			installTestFloating(testAttachmentTab(sess), floating, true)

			d.firstPaint(sess, ac)

			frame := awaitFrame(t, sends, wire.MsgOutput)
			output, err := wire.UnmarshalOutput(frame.Payload)
			require.NoError(t, err)
			require.Zero(t, output.Base, "first paint must be a mandatory reset")
			select {
			case extra := <-sends:
				t.Fatalf("first paint emitted duplicate frame after floating activation resize: %#v", extra)
			default:
			}
		})
	}
}

func TestPaletteBackdropDimsSimultaneousPicker(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	ac.setThemeForTest(backdropTheme())
	client := vt.NewScreen(80, 25)
	pane := sess.tabs[0].focusedPane()
	pane.screen.Write([]byte("X"))

	d.enterPicker(sess, ac)
	mustApplyOutput(t, client, awaitFrame(t, sends, wire.MsgOutput))
	pickerTitle := client.Cell(31, 2)
	require.Equal(t, 'S', pickerTitle.Rune, "fixture must address the picker title")
	undimmedPane := client.Cell(0, 1)

	d.enterPalette(sess, ac)
	mustApplyOutput(t, client, awaitFrame(t, sends, wire.MsgOutput))
	dimmedPickerTitle := pickerTitle
	dimmedPickerTitle.Style = themeui.NewDimmer(backdropTheme()).Dim(pickerTitle.Style).Canonical()
	require.Equal(t, dimmedPickerTitle, client.Cell(31, 2), "the lower-priority picker must be part of the palette backdrop")
	require.Equal(t, themeui.NewDimmer(backdropTheme()).Dim(undimmedPane.Style).Canonical(), client.Cell(0, 1).Style.Canonical(), "pane content outside overlays must use the theme dim style")
}

func TestPaletteBackdropProductionRenderAndDismissal(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	ac.setThemeForTest(backdropTheme())
	client := vt.NewScreen(80, 25)
	pane := sess.tabs[0].focusedPane()
	pane.screen.Write([]byte("X"))
	d.paint(sess, ac, true, nil)
	mustApplyOutput(t, client, awaitFrame(t, sends, wire.MsgOutput))
	undimmed := client.Cell(0, 1)
	topBar := client.Cell(0, 0)
	bottomBar := client.Cell(0, 24)

	d.handleInput(sess, ac, []byte("\x1b "))
	mustApplyOutput(t, client, awaitFrame(t, sends, wire.MsgOutput))
	dimmed := client.Cell(0, 1)
	require.Equal(t, 'X', dimmed.Rune)
	require.Equal(t, themeui.NewDimmer(backdropTheme()).Dim(undimmed.Style).Canonical(), dimmed.Style.Canonical(), "open palette must use the theme dim style")
	dimmedTopBar := topBar
	dimmedTopBar.Style = themeui.NewDimmer(backdropTheme()).Dim(topBar.Style).Canonical()
	dimmedBottomBar := bottomBar
	dimmedBottomBar.Style = themeui.NewDimmer(backdropTheme()).Dim(bottomBar.Style).Canonical()
	require.Equal(t, dimmedTopBar, client.Cell(0, 0), "top chrome is part of the complete backdrop")
	require.Equal(t, dimmedBottomBar, client.Cell(0, 24), "bottom chrome is part of the complete backdrop")

	d.handleInput(sess, ac, []byte("\x1b"))
	mustApplyOutput(t, client, awaitFrame(t, sends, wire.MsgOutput))
	require.Equal(t, undimmed, client.Cell(0, 1), "full redraw restores pane rune and style")
	require.Equal(t, topBar, client.Cell(0, 0))
	require.Equal(t, bottomBar, client.Cell(0, 24))
}

// --- test doubles -----------------------------------------------------------

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

// channelPTY drives ptyReader through generated PTY mocks. A processed token is
// published only when ptyReader asks for its next read, which proves the
// preceding chunk completed its parser and coordinator path without sleeping.
type channelPTYStep struct {
	data []byte
	err  error
}

func newChannelPTY(t *testing.T) (*portsmocks.MockPTY, chan<- channelPTYStep, <-chan struct{}) {
	t.Helper()
	pty := portsmocks.NewMockPTY(t)
	steps := make(chan channelPTYStep)
	processed := make(chan struct{}, 8)
	read := false
	pty.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if read {
			processed <- struct{}{}
		}
		read = true
		step := <-steps
		return copy(buf, step.data), step.err
	}).Maybe()
	pty.EXPECT().Close().Return(nil).Maybe()
	return pty, steps, processed
}

func awaitPTYReadProcessed(t *testing.T, processed <-chan struct{}) {
	t.Helper()
	<-processed
}

func awaitOutputFrameWithoutSleep(t *testing.T, sends <-chan wire.Frame) wire.Frame {
	t.Helper()
	frame := <-sends
	require.Equal(t, wire.MsgOutput, frame.Type)
	return frame
}

func drainCoordinatorTimers(clock *coordinatorMockClock) []*coordinatorMockTimer {
	var timers []*coordinatorMockTimer
	for {
		select {
		case timer := <-clock.timers:
			timers = append(timers, timer)
		default:
			return timers
		}
	}
}

func fireCoordinatorTimer(t *testing.T, rc *renderCoordinator, timers []*coordinatorMockTimer, duration time.Duration) {
	t.Helper()
	for _, timer := range timers {
		if timer.duration != duration {
			continue
		}
		var done <-chan struct{}
		rc.mu.Lock()
		if rc.normalLane.token.timer == timer.mock {
			done = rc.normalLane.token.done
		}
		rc.mu.Unlock()
		timer.ch <- time.Time{}
		if done != nil {
			<-done
		}
		return
	}
	t.Fatalf("coordinator did not arm %s timer", duration)
}

func TestPTYReaderLogsClosureWithoutNotification(t *testing.T) {
	pty := portsmocks.NewMockPTY(t)
	readErr := errors.New("pty master closed")
	pty.EXPECT().Read(mock.Anything).Return(0, readErr).Once()
	d, sess, ac, _ := newManualSessionWithPTYs(t, pty)
	var logs bytes.Buffer
	d.log = slog.New(slog.NewTextHandler(&logs, nil))
	pane := sess.tabs[0].focusedPane()
	pane.onExit = func() {}

	d.sessWg.Add(1)
	d.ptyReader(sess, sess.tabs[0], pane)

	require.Contains(t, logs.String(), "level=INFO")
	require.Contains(t, logs.String(), "msg=\"pane pty closed\"")
	require.Contains(t, logs.String(), "err=\"pty master closed\"")
	require.Contains(t, logs.String(), "session=work")
	require.Empty(t, d.notices.history(), "PTY closure is diagnostic only")
	toasts, _ := visibleToasts(ac)
	require.Empty(t, toasts, "PTY closure must not notify the user")
}

type concurrentExitPTY struct {
	started chan struct{}
	release chan struct{}
}

func newConcurrentExitPTY() *concurrentExitPTY {
	return &concurrentExitPTY{started: make(chan struct{}), release: make(chan struct{})}
}

func (p *concurrentExitPTY) Read([]byte) (int, error) {
	close(p.started)
	<-p.release
	return 0, io.EOF
}
func (*concurrentExitPTY) Write(b []byte) (int, error)  { return len(b), nil }
func (*concurrentExitPTY) Close() error                 { return nil }
func (*concurrentExitPTY) Resize(domain.Geometry) error { return nil }
func (*concurrentExitPTY) Pid() int                     { return 0 }
func (*concurrentExitPTY) ForegroundPgid() (int, error) {
	return 0, nil
}

func TestPTYReaderReadsSessionNameUnderLockDuringConcurrentRename(t *testing.T) {
	// Repeated synchronized exits make the reader's closure log contend with a
	// rename. Running this under -race proves the name snapshot uses sess.mu.
	for range 100 {
		pty := newConcurrentExitPTY()
		d, sess, _, _ := newManualSessionWithPTYs(t, pty)
		pane := sess.tabs[0].focusedPane()
		pane.onExit = func() {}

		d.sessWg.Add(1)
		go d.ptyReader(sess, sess.tabs[0], pane)
		<-pty.started

		start := make(chan struct{})
		renameErr := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			close(pty.release)
		}()
		go func() {
			defer wg.Done()
			<-start
			renameErr <- d.renameSession(sess, "renamed")
		}()
		close(start)
		wg.Wait()
		require.NoError(t, <-renameErr)
		d.sessWg.Wait()
	}
}

func TestPTYReaderSyncVisibilityTransitions(t *testing.T) {
	t.Run("inactive synchronized pane activates only after complete urgent frame", func(t *testing.T) {
		inactivePTY, inactiveSteps, inactiveProcessed := newChannelPTY(t)
		activePTY, _, _ := newChannelPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, activePTY, inactivePTY)
		clock := newCoordinatorMockClock(t, 8)
		d.clock = clock.clock
		rc := d.attachCoordinator(sess, nil, ac, true)

		d.sessWg.Add(1)
		go d.ptyReader(sess, sess.tabs[1], sess.tabs[1].focusedPane())
		inactiveSteps <- channelPTYStep{data: []byte("\x1b[?2026hpartial")}
		awaitPTYReadProcessed(t, inactiveProcessed)

		sess.mu.Lock()
		selectTestAttachmentTabLocked(sess, 1)
		sess.mu.Unlock()
		d.invalidateRender(sess, ac, true, "sync activation")
		fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), urgentRenderDeadline)
		requireNoCoordinatorOutputFrame(t, sends)

		inactiveSteps <- channelPTYStep{data: []byte(" complete\x1b[?2026l")}
		awaitPTYReadProcessed(t, inactiveProcessed)
		fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), urgentRenderDeadline)
		frame := awaitOutputFrameWithoutSleep(t, sends)
		output, err := wire.UnmarshalOutput(frame.Payload)
		require.NoError(t, err)
		require.Contains(t, string(output.Data), "partial complete")
		requireNoCoordinatorOutputFrame(t, sends)

		inactiveSteps <- channelPTYStep{err: io.EOF}
		d.sessWg.Wait()
	})

	t.Run("hidden synchronized pane cannot stall newly active output or flush late", func(t *testing.T) {
		oldPTY, oldSteps, oldProcessed := newChannelPTY(t)
		newPTY, newSteps, newProcessed := newChannelPTY(t)
		parkedPTY, _, _ := newChannelPTY(t)
		// Keep one unopened tab so reader EOF cleanup cannot tear down the
		// session and introduce unrelated detach-notification timers.
		d, sess, ac, sends := newManualSessionWithPTYs(t, oldPTY, newPTY, parkedPTY)
		clock := newCoordinatorMockClock(t, 8)
		d.clock = clock.clock
		rc := d.attachCoordinator(sess, nil, ac, true)

		d.sessWg.Add(2)
		go d.ptyReader(sess, sess.tabs[0], sess.tabs[0].focusedPane())
		go d.ptyReader(sess, sess.tabs[1], sess.tabs[1].focusedPane())
		oldSteps <- channelPTYStep{data: []byte("\x1b[?2026hold partial")}
		awaitPTYReadProcessed(t, oldProcessed)

		sess.mu.Lock()
		selectTestAttachmentTabLocked(sess, 1)
		sess.mu.Unlock()
		newSteps <- channelPTYStep{data: []byte("newly active")}
		awaitPTYReadProcessed(t, newProcessed)
		fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), minOutputRenderDeadline)
		frame := awaitOutputFrameWithoutSleep(t, sends)
		output, err := wire.UnmarshalOutput(frame.Payload)
		require.NoError(t, err)
		require.Contains(t, string(output.Data), "newly active")
		require.NotContains(t, string(output.Data), "old partial")

		oldSteps <- channelPTYStep{data: []byte("\x1b[?2026l")}
		awaitPTYReadProcessed(t, oldProcessed)
		requireNoCoordinatorOutputFrame(t, sends)

		oldSteps <- channelPTYStep{err: io.EOF}
		newSteps <- channelPTYStep{err: io.EOF}
		d.sessWg.Wait()
	})
}

func TestPTYReaderRepublishesSynchronizedCompletionAfterAttachmentLifecycle(t *testing.T) {
	t.Run("detached target completes cross-session preview without later PTY output", func(t *testing.T) {
		pty, steps, processed := newChannelPTY(t)
		d, target, targetClient, _ := newManualSessionWithPTYs(t, pty)
		clock := newCoordinatorMockClock(t, 8)
		d.clock = clock.clock
		rc := d.attachCoordinator(target, nil, targetClient, true)

		// A viewer in a different session is previewing the headless target.
		viewer := &attachedClient{}
		viewer.initOverlays()
		viewer.overlays.pickerMu.Lock()
		viewer.overlays.pickerPreview = target.tabs[0]
		viewer.overlays.pickerMu.Unlock()
		d.sessions["viewer"] = &session{sessionCore: sessionCore{id: "viewer", attachments: map[*attachedClient]struct{}{viewer: {}}}}
		previews := make(chan renderWake, 2)
		rc.subscribePreviewFor(viewer, 1, func(w renderWake) { previews <- w })

		d.sessWg.Add(1)
		go d.ptyReader(target, target.tabs[0], target.tabs[0].focusedPane())
		steps <- channelPTYStep{data: []byte("\x1b[?2026hpending")}
		awaitPTYReadProcessed(t, processed)
		drainCoordinatorTimers(clock) // detach must clear the pending output work.

		target.mu.Lock()
		clearAttachmentsForTestLocked(target)
		target.mu.Unlock()
		rc.noteDetach(targetClient)
		steps <- channelPTYStep{data: []byte(" complete\x1b[?2026l")}
		awaitPTYReadProcessed(t, processed)

		fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), urgentRenderDeadline)
		wake := <-previews
		require.True(t, wake.urgent)
		require.Equal(t, 1, wake.coalesced, "completion starts a fresh headless preview batch after detach")
		select {
		case duplicate := <-previews:
			t.Fatalf("sync completion must publish exactly one preview wake: %#v", duplicate)
		default:
		}

		steps <- channelPTYStep{err: io.EOF}
		d.sessWg.Wait()
	})

	t.Run("remaining attachment receives shared output after peer detach", func(t *testing.T) {
		pty, steps, processed := newChannelPTY(t)
		d, target, detached, _ := newManualSessionWithPTYs(t, pty)
		clock := newCoordinatorMockClock(t, 8)
		d.clock = clock.clock
		rc := d.attachCoordinator(target, nil, detached, true)

		secondTransport, secondSends := newCapturingTransport(t)
		remaining := &attachedClient{tr: secondTransport, output: newOutputStateStream(), size: detached.size}
		remaining.initOverlays()
		remaining.setSession(target)
		target.mu.Lock()
		require.True(t, target.registerAttachmentLocked(remaining))
		target.mu.Unlock()
		rc.attach(remaining)

		d.sessWg.Add(1)
		go d.ptyReader(target, target.tabs[0], target.tabs[0].focusedPane())
		steps <- channelPTYStep{data: []byte("before detach")}
		awaitPTYReadProcessed(t, processed)
		timers := drainCoordinatorTimers(clock)

		// Detaching one attachment must not discard the session-shared pending
		// invalidation or stop the coordinator from painting the remaining one.
		target.mu.Lock()
		require.True(t, target.unregisterAttachmentLocked(detached))
		target.mu.Unlock()
		rc.noteDetach(detached)
		fireCoordinatorTimer(t, rc, timers, minOutputRenderDeadline)
		require.NotEmpty(t, secondSends, "the remaining attachment received no shared output after its peer detached")
		before := len(secondSends)

		steps <- channelPTYStep{data: []byte("after detach")}
		awaitPTYReadProcessed(t, processed)
		fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), minOutputRenderDeadline)
		require.Greater(t, len(secondSends), before, "shared output stopped after the peer detached")

		steps <- channelPTYStep{err: io.EOF}
		d.sessWg.Wait()
		d.waitNotifies()
	})

	t.Run("headless pane without a preview stays quiet on completion", func(t *testing.T) {
		pty, steps, processed := newChannelPTY(t)
		d, target, client, _ := newManualSessionWithPTYs(t, pty)
		clock := newCoordinatorMockClock(t, 8)
		d.clock = clock.clock
		rc := d.attachCoordinator(target, nil, client, true)
		wakes := make(chan renderWake, 1)
		rc.opts.wake = func(w renderWake) { wakes <- w }
		target.mu.Lock()
		clearAttachmentsForTestLocked(target)
		target.mu.Unlock()
		rc.noteDetach(client)

		d.sessWg.Add(1)
		go d.ptyReader(target, target.tabs[0], target.tabs[0].focusedPane())
		steps <- channelPTYStep{data: []byte("\x1b[?2026hpending")}
		awaitPTYReadProcessed(t, processed)
		drainCoordinatorTimers(clock)
		steps <- channelPTYStep{data: []byte(" complete\x1b[?2026l")}
		awaitPTYReadProcessed(t, processed)
		require.Empty(t, drainCoordinatorTimers(clock))
		requireNoWake(t, wakes)

		steps <- channelPTYStep{err: io.EOF}
		d.sessWg.Wait()
	})
}

// S2 keeps damage pending until the owning render capture consumes it. In
// particular, a pane becoming invisible must not let a PTY reader erase data
// that a later attachment or picker preview needs to render.
func TestPaneRenderableActiveAttachmentDoesNotScanPickerPreviews(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	tb := testAttachmentTab(sess)
	pane := tb.focusedPane()

	// Holding daemon ownership makes a picker scan block. An active attached
	// tab must decide renderability from its session state without that scan.
	d.mu.Lock()
	defer d.mu.Unlock()
	done := make(chan bool, 1)
	go func() { done <- d.paneRenderable(sess, tb, pane) }()
	require.True(t, awaitTestValue(t, done, "active attached renderability scanned picker previews"))
}

func TestNonRenderablePaneDamageRemainsPendingForCapture(t *testing.T) {
	newFixture := func(t *testing.T) (*Daemon, *session, *tab, *pane, chan wire.Frame) {
		t.Helper()
		p, release := newBlockingPTY(t)
		t.Cleanup(release)
		d, sess, _, sends := newManualSessionWithPTYs(t, p)
		tb := sess.tabs[0]
		return d, sess, tb, tb.focusedPane(), sends
	}

	t.Run("headless, inactive, collapsed, and hidden panes retain damage without output", func(t *testing.T) {
		for _, tt := range []struct {
			name  string
			setup func(*Daemon, *session, *tab, *pane)
		}{
			{name: "headless", setup: func(_ *Daemon, sess *session, _ *tab, _ *pane) { clearAttachmentsForTest(sess) }},
			{name: "inactive tab", setup: func(_ *Daemon, sess *session, tb *tab, _ *pane) {
				other := newTab(nil, domain.Size{Cols: 80, Rows: 23})
				sess.tabs = append([]*tab{other}, sess.tabs...)
				selectTestAttachmentTab(sess, 0)
				require.NotSame(t, other, tb)
			}},
			{name: "collapsed", setup: func(_ *Daemon, _ *session, tb *tab, p *pane) {
				// The expanded leaf is deliberately absent from panes: p is the
				// inactive stack leaf whose content is not composited.
				tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf("visible"), layout.NewLeaf(p.id)}, Expanded: "visible"}, Focus: "visible"}
			}},
			{name: "hidden floating", setup: func(_ *Daemon, _ *session, tb *tab, p *pane) {
				tb.floating = floatingSlot{state: floatingHidden, pane: p, generation: 1}
				delete(tb.panes, p.id)
			}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				d, sess, tb, p, sends := newFixture(t)
				tt.setup(d, sess, tb, p)
				p.screen.Write([]byte("damage"))
				_ = d.paneRenderable(sess, tb, p)
				require.NotEmpty(t, p.screen.Damage(), "only render capture may consume VT damage")
				select {
				case frame := <-sends:
					t.Fatalf("non-renderable output must not compose or send: %#v", frame)
				default:
				}
			})
		}
	})

	t.Run("retains active and picker-preview pane damage", func(t *testing.T) {
		d, sess, tb, p, _ := newFixture(t)
		p.screen.Write([]byte("active"))
		_ = d.paneRenderable(sess, tb, p)
		require.NotEmpty(t, p.screen.Damage(), "active pane damage belongs to coordinator composition")
		p.screen.ClearDamage()

		clearAttachmentsForTest(sess)
		viewer := &attachedClient{}
		viewer.initOverlays()
		viewer.overlays.pickerMu.Lock()
		viewer.overlays.pickerPreview = tb
		viewer.overlays.pickerMu.Unlock()
		d.sessions["viewer"] = &session{sessionCore: sessionCore{id: "viewer", attachments: map[*attachedClient]struct{}{viewer: {}}}}
		p.screen.Write([]byte("preview"))
		_ = d.paneRenderable(sess, tb, p)
		require.NotEmpty(t, p.screen.Damage(), "picker preview damage must remain for coordinator composition")
	})
}

func TestAltXClosesFinalTabAndDetaches(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte("CLT\r"))

	require.Equal(t, 0, sessionCount(d))
	f := awaitFrame(t, sends, wire.MsgDetached)
	det, err := wire.UnmarshalDetached(f.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.ReasonSessionKilled, det.Reason)
}

func TestPTYEOFClosesActiveNonFinalTabAndRepaintsRemaining(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d, sess, _, sends := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY2()
	selectTestAttachmentTab(sess, 0)
	sess.tabs[1].focusedPane().screen.Write([]byte("remaining"))

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.tabs[0], sess.tabs[0].focusedPane())
	releasePTY1()

	require.Eventually(t, func() bool { return tabCount(sess) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, sessionCount(d))
	require.Equal(t, 0, testAttachmentTabIndex(sess))
	f := awaitFrame(t, sends, wire.MsgOutput)
	out, err := wire.UnmarshalOutput(f.Payload)
	require.NoError(t, err)
	data := string(out.Data)
	require.Contains(t, data, "remaining")
	require.Contains(t, data, "work")
	require.Contains(t, data, ";7m")

	d.sessWg.Wait()
}

func TestPTYEOFClosesInactiveNonFinalTabAndRepaintsStatus(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d, sess, _, sends := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	selectTestAttachmentTab(sess, 0)
	sess.tabs[0].focusedPane().screen.Write([]byte("active"))

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.tabs[1], sess.tabs[1].focusedPane())
	releasePTY2()

	require.Eventually(t, func() bool { return tabCount(sess) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, sessionCount(d))
	require.Equal(t, 0, testAttachmentTabIndex(sess))
	f := awaitFrame(t, sends, wire.MsgOutput)
	out, err := wire.UnmarshalOutput(f.Payload)
	require.NoError(t, err)
	data := string(out.Data)
	require.Contains(t, data, "active")
	require.Contains(t, data, "work")
	require.NotContains(t, data, "  2 ")

	d.sessWg.Wait()
}

func TestPTYEOFFinalTabKillsSessionAndDetaches(t *testing.T) {
	d, sess, _, sends, releases := newManualTabSession(t, 1)

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.tabs[0], sess.tabs[0].focusedPane())
	releases[0]()

	require.Eventually(t, func() bool { return sessionCount(d) == 0 }, 2*time.Second, 5*time.Millisecond)
	f := awaitFrame(t, sends, wire.MsgDetached)
	det, err := wire.UnmarshalDetached(f.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.ReasonSessionKilled, det.Reason)

	d.sessWg.Wait()
}

// --- resize ordering --------------------------------------------------------

func TestResizePreservesLiveContentAndEvictsScrollback(t *testing.T) {
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Resize(domain.Geometry{Size: domain.Size{Cols: 4, Rows: 2}}).Return(nil).Once()
	p.EXPECT().Resize(domain.Geometry{Size: domain.Size{Cols: 6, Rows: 4}}).Return(nil).Once()

	win := newTab(p, domain.Size{Cols: 4, Rows: 4})
	for y, text := range []string{"0000", "1111", "2222", "3333"} {
		writeTestRow(win.focusedPane().screen, y, text)
	}
	win.focusedPane().screen.Row = 3

	tr := newMockServerConnection(t)
	tr.EXPECT().Close().Return(nil).Maybe()
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()

	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	sess := &session{sessionCore: sessionCore{id: "s", name: "s", attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{win}}
	ac.setSession(sess)

	// Client rows are one more than the equivalent case in a single-bar
	// layout: tabSize reserves 2 chrome rows (top + bottom bar) here, not 1,
	// so a client height of 4 (not 3) is what yields the same 2-row tab.
	d.resize(sess, ac, domain.Size{Cols: 4, Rows: 4})
	require.Equal(t, "2222", frameRowString(captureTestFrame(win.focusedPane().screen), 0))
	require.Equal(t, "3333", frameRowString(captureTestFrame(win.focusedPane().screen), 1))
	require.Equal(t, 2, win.focusedPane().history.Len())
	require.Equal(t, "0000", cellsString(win.focusedPane().history.View().Row(0)))
	require.Equal(t, "1111", cellsString(win.focusedPane().history.View().Row(1)))

	d.resize(sess, ac, domain.Size{Cols: 6, Rows: 6})
	require.Equal(t, "2222  ", frameRowString(captureTestFrame(win.focusedPane().screen), 0))
	require.Equal(t, "3333  ", frameRowString(captureTestFrame(win.focusedPane().screen), 1))
	require.Equal(t, 2, win.focusedPane().history.Len())
}

func frameRowString(f renderer.Frame, y int) string {
	return cellsString(f.Row(y))
}

func cellsString(row []renderer.Cell) string {
	runes := make([]rune, len(row))
	for i, c := range row {
		runes[i] = c.Rune
	}
	return string(runes)
}

func TestTabSizeReservesTopAndBottomChromeRows(t *testing.T) {
	require.Equal(t, domain.Size{Cols: 80, Rows: 22}, tabSize(domain.Size{Cols: 80, Rows: 24}))
	require.Equal(t, domain.Size{Cols: 80, Rows: 1}, tabSize(domain.Size{Cols: 80, Rows: 2}))
}

func TestOffsetDamageShiftsScreenDamageBelowTopBar(t *testing.T) {
	damage := offsetDamage([]renderer.Damage{
		{Kind: renderer.DamageText, X: 2, Y: 3, Width: 4, Height: 1},
		renderer.FullRedraw(),
	})
	require.Equal(t, []renderer.Damage{
		{Kind: renderer.DamageText, X: 2, Y: 4, Width: 4, Height: 1},
		renderer.FullRedraw(),
	}, damage)
}

func TestTranslateDamageShiftsXYAndPreservesFullRedraw(t *testing.T) {
	damage := translateDamage([]renderer.Damage{
		{Kind: renderer.DamageText, X: 2, Y: 3, Width: 4, Height: 1},
		renderer.FullRedraw(),
	}, 5, 7)
	require.Equal(t, []renderer.Damage{
		{Kind: renderer.DamageText, X: 7, Y: 10, Width: 4, Height: 1},
		renderer.FullRedraw(),
	}, damage)
}

func TestTranslatePaneDamagePreservesFullWidthScrollFastPathOnly(t *testing.T) {
	tests := []struct {
		name    string
		content domain.Rect
		area    domain.Rect
		in      renderer.Damage
		want    []renderer.Damage
	}{
		{
			name:    "half width scroll becomes pane text damage",
			content: domain.Rect{X: 21, Y: 0, Width: 20, Height: 4},
			area:    domain.Rect{Width: 41, Height: 4},
			in:      renderer.Damage{Kind: renderer.DamageScrollUp, X: 0, Y: 0, Width: 20, Height: 4, Count: 1},
			want:    []renderer.Damage{{Kind: renderer.DamageText, X: 21, Y: 0, Width: 20, Height: 4}},
		},
		{
			name:    "full width scroll keeps translated scroll damage",
			content: domain.Rect{X: 0, Y: 5, Width: 80, Height: 10},
			area:    domain.Rect{Width: 80, Height: 24},
			in:      renderer.Damage{Kind: renderer.DamageScrollUp, X: 0, Y: 0, Width: 80, Height: 10, Count: 1},
			want:    []renderer.Damage{{Kind: renderer.DamageScrollUp, X: 0, Y: 5, Width: 80, Height: 10, Count: 1}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, translatePaneDamage(tt.in, tt.content, tt.area))
		})
	}
}

func TestOverlayPaintInvalidationShowsAndRestoresBaseFrame(t *testing.T) {
	tests := []struct {
		name          string
		open          func(*Daemon, *session, *attachedClient)
		close         func(*Daemon, *session, *attachedClient)
		visible       string
		notVisible    string
		wantRestored  string
		prepareScreen func(*session)
	}{
		{
			name: "prompt",
			open: func(d *Daemon, sess *session, ac *attachedClient) {
				d.enterPrompt(sess, ac, " Rename session ", "work", func(string) error { return nil })
			},
			close:      func(d *Daemon, _ *session, ac *attachedClient) { d.handlePromptInput(ac, []byte("\x1b")) },
			visible:    "Rename session",
			notVisible: "Rename session",
		},
		{
			name:       "palette",
			open:       func(d *Daemon, sess *session, ac *attachedClient) { d.enterPalette(sess, ac) },
			close:      func(d *Daemon, _ *session, ac *attachedClient) { d.handlePaletteInput(ac, []byte("\x1b")) },
			visible:    "Commands",
			notVisible: "Commands",
		},
		{
			name:       "picker",
			open:       func(d *Daemon, sess *session, ac *attachedClient) { d.enterPicker(sess, ac) },
			close:      func(d *Daemon, sess *session, ac *attachedClient) { d.closePicker(ac); d.paint(sess, ac, true, nil) },
			visible:    "Sessions",
			notVisible: "Sessions",
		},
		{
			name:  "copy mode",
			open:  func(d *Daemon, sess *session, ac *attachedClient) { d.enterCopyMode(sess, ac) },
			close: func(d *Daemon, _ *session, ac *attachedClient) { d.handleCopyInput(ac, []byte("q")) },
			prepareScreen: func(sess *session) {
				installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
			},
			visible:      "[SCROLL]",
			notVisible:   "[SCROLL]",
			wantRestored: "live",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, release := newBlockingPTY(t)
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)
			defer release()
			if tt.prepareScreen != nil {
				tt.prepareScreen(sess)
			}
			sess.tabs[0].focusedPane().screen.Write([]byte("live"))
			if tt.wantRestored == "" {
				tt.wantRestored = "live"
			}

			tt.open(d, sess, ac)
			shown := string(mustOutputData(t, sends))
			require.Contains(t, shown, tt.visible)

			tt.close(d, sess, ac)
			restored := string(mustOutputData(t, sends))
			require.Contains(t, restored, tt.wantRestored)
			require.NotContains(t, restored, tt.notVisible)
		})
	}
}

func TestOverlayPaintBypassesComposedCache(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	pane := sess.tabs[0].focusedPane()
	pane.screen.Write([]byte("live"))

	d.paint(sess, ac, true, nil)
	_ = mustOutputData(t, sends)
	require.True(t, ac.pipelineCache.valid)
	require.NotContains(t, frameText(ac.pipelineCache.frame), "Rename session")

	d.enterPrompt(sess, ac, " Rename session ", "work", func(string) error { return nil })
	shown := string(mustOutputData(t, sends))
	require.Contains(t, shown, "Rename session")
	require.False(t, ac.pipelineCache.valid, "overlay paint must not store the modal-mutated frame in the composed cache")
	require.NotContains(t, frameText(ac.pipelineCache.frame), "Rename session")

	d.closePrompt(ac)
	d.paint(sess, ac, false, nil)
	restored := string(mustOutputData(t, sends))
	require.NotContains(t, restored, "Rename session")
	require.True(t, ac.pipelineCache.valid)
	cached := frameText(ac.pipelineCache.frame)
	require.Contains(t, cached, "live")
	require.NotContains(t, cached, "Rename session")
}

func frameText(frame renderer.Frame) string {
	var b strings.Builder
	for y := 0; y < frame.Height; y++ {
		b.WriteString(rowText(frame.Row(y)))
		b.WriteByte('\n')
	}
	return b.String()
}

func TestPaintEarlyReturnReleasesOverlayRenderLocks(t *testing.T) {
	tests := []struct {
		name string
		open func(*Daemon, *session, *attachedClient)
		try  func(*overlayRuntime) bool
		give func(*overlayRuntime)
	}{
		{
			name: "prompt",
			open: func(d *Daemon, sess *session, ac *attachedClient) {
				d.enterPrompt(sess, ac, " Rename session ", "work", func(string) error { return nil })
			},
			try:  func(rt *overlayRuntime) bool { return rt.promptMu.TryLock() },
			give: func(rt *overlayRuntime) { rt.promptMu.Unlock() },
		},
		{
			name: "palette",
			open: func(d *Daemon, sess *session, ac *attachedClient) {
				d.enterPalette(sess, ac)
			},
			try:  func(rt *overlayRuntime) bool { return rt.paletteMu.TryLock() },
			give: func(rt *overlayRuntime) { rt.paletteMu.Unlock() },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, release := newBlockingPTY(t)
			d, sess, ac, _ := newManualSessionWithPTYs(t, p)
			defer release()
			sess.tabs[0].tree.Focus = layout.PaneID("missing")

			tt.open(d, sess, ac)

			require.True(t, tt.try(ac.overlays), "overlay render lock should be released after paint returns early")
			tt.give(ac.overlays)
		})
	}
}

func TestResizeOrdersPTYBeforeScreen(t *testing.T) {
	newSize := domain.Size{Cols: 100, Rows: 30}

	p := portsmocks.NewMockPTY(t)
	var screenWidthAtResize int
	win := newTab(newScriptPTY(nil), domain.Size{Cols: 80, Rows: 24})
	p.EXPECT().Resize(domain.Geometry{Size: domain.Size{Cols: 100, Rows: 28}}).RunAndReturn(func(domain.Geometry) error {
		// The screen must not yet be resized when the PTY is: proves order.
		screenWidthAtResize = win.focusedPane().screen.Columns()
		return nil
	}).Once()
	win.focusedPane().pty = p

	var gotOutput atomic.Bool
	tr := newMockServerConnection(t)
	tr.EXPECT().Close().Return(nil).Maybe()
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f wire.Frame) error {
		if f.Type == wire.MsgOutput {
			gotOutput.Store(true)
		}
		return nil
	}).Maybe()

	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	sess := &session{sessionCore: sessionCore{id: "s", name: "s", incarnation: newTestLifecycle(t), attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{win}}
	ac.setSession(sess)

	d.resize(sess, ac, newSize)
	// This test verifies resize ordering, not the idle fallback. Consume the
	// pending resize paint deterministically under the non-firing stub clock.
	d.paint(sess, ac, false, nil)

	require.Equal(t, 80, screenWidthAtResize, "pty.Resize must run before screen.Resize")
	require.Equal(t, 100, win.focusedPane().screen.Columns(), "screen resized after pty")
	require.Equal(t, 28, win.focusedPane().screen.Rows(), "screen reserves top and bottom chrome rows")
	require.True(t, gotOutput.Load(), "resize forces a full redraw output")
}

// --- reader EOF -> registry-empty shutdown ----------------------------------

func TestSendErrorKeepsEphemeralHeadless(t *testing.T) {
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Close().Return(nil).Maybe()

	tr := newMockServerConnection(t)
	tr.EXPECT().Send(mock.Anything).Return(io.ErrClosedPipe).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()

	d := newTestDaemon(t, nil, stubClock{})
	win := newTab(p, domain.Size{Cols: 20, Rows: 5})
	sctx, cancel := context.WithCancel(context.Background())
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	sess := &session{sessionCore: sessionCore{id: "e", name: "0", ephemeral: true, attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{win}, ctx: sctx, cancel: cancel}
	ac.setSession(sess)
	d.sessions[sess.id] = sess

	win.mu.Lock()
	win.focusedPane().screen.Write([]byte("x"))
	win.mu.Unlock()

	d.paint(sess, ac, true, nil)

	require.Equal(t, 1, sessionCount(d), "ephemeral session survives failed client send")
	sess.mu.Lock()
	require.Empty(t, sess.snapshotAttachmentsLocked())
	sess.mu.Unlock()

	_ = d.killSession(sess, protocol.ReasonServerShutdown, false)
	cancel()
	d.waitNotifies()
}

func TestPTYKittyAnonymousGraphicsDoesNotWriteResponseToPTY(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := portsmocks.NewMockPTY(t)
	chunks := [][]byte{[]byte("\x1b_Ga=T,f=32,s=1,v=1,C=1;AQIDBA\x1b\\")}
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if len(chunks) == 0 {
			return 0, io.EOF
		}
		n := copy(buf, chunks[0])
		chunks = chunks[1:]
		return n, nil
	})
	p.EXPECT().Close().Return(nil).Maybe()

	sctx, cancel := context.WithCancel(context.Background())
	win := newTestTabWithContext(p, sctx, cancel)
	sess := &session{sessionCore: sessionCore{id: "anonymous", name: "anonymous"}, tabs: []*tab{win}, ctx: sctx, cancel: cancel}
	d.sessions[sess.id] = sess
	d.sessWg.Add(1)

	d.ptyReader(sess, win, win.focusedPane())
}

func TestPTYKittyIcatDetectionGetsResponsesWrittenBackToPTY(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := portsmocks.NewMockPTY(t)
	chunks := [][]byte{[]byte("\x1b_Ga=q,f=24,s=1,v=1,S=3,i=1;MTIz\x1b\\\x1b_Ga=q,f=24,t=t,s=1,v=1,S=47,i=2;L2Rldi9zaG0va2l0dHktdHR5LWdyYXBoaWNzLXByb3RvY29sLTMzMTU3NTkxNjc\x1b\\\x1b_Ga=q,f=24,t=s,s=1,v=1,S=18,i=3;aWNhdC1aQlJCWFdNQ0lIQ0ZD\x1b\\\x1b[c")}
	writes := make(chan []byte, 1)
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if len(chunks) == 0 {
			return 0, io.EOF
		}
		n := copy(buf, chunks[0])
		chunks = chunks[1:]
		return n, nil
	})
	p.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		writes <- append([]byte(nil), b...)
		return len(b), nil
	}).Once()
	p.EXPECT().Close().Return(nil).Maybe()

	tr, sends := newCapturingTransport(t)
	sctx, cancel := context.WithCancel(context.Background())
	win := newTestTabWithContext(p, sctx, cancel)
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	sess := &session{sessionCore: sessionCore{id: "query", name: "query", attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{win}, ctx: sctx, cancel: cancel}
	ac.setSession(sess)
	d.sessions[sess.id] = sess
	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	select {
	case got := <-writes:
		require.Equal(t, []byte("\x1b_Gi=1;OK\x1b\\\x1b_Gi=2;ENOTSUP\x1b\\\x1b_Gi=3;ENOTSUP\x1b\\\x1b[?62;22c"), got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for PTY response write")
	}
	select {
	case f := <-sends:
		require.NotEqual(t, wire.MsgOutput, f.Type)
	default:
	}
}

func TestPTYKittyAnimationFallbackKeepsProtocolErrorsOutOfShell(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := portsmocks.NewMockPTY(t)
	const imageNumber = "622840581"
	chunks := [][]byte{[]byte(
		"\x1b_Ga=T,q=1,f=24,s=1,v=1,I=" + imageNumber + ";AAAA\x1b\\" +
			"\x1b_Ga=f,q=1,f=24,s=1,v=1,I=" + imageNumber + ";AAAA\x1b\\" +
			"\x1b_Ga=a,q=1,s=3,I=" + imageNumber + ";\x1b\\",
	)}
	writes := make(chan []byte, 1)
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if len(chunks) == 0 {
			return 0, io.EOF
		}
		n := copy(buf, chunks[0])
		chunks = chunks[1:]
		return n, nil
	})
	p.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		writes <- append([]byte(nil), b...)
		return len(b), nil
	}).Maybe()
	p.EXPECT().Close().Return(nil).Maybe()

	sctx, cancel := context.WithCancel(context.Background())
	win := newTestTabWithContext(p, sctx, cancel)
	sess := &session{sessionCore: sessionCore{id: "animation", name: "animation"}, tabs: []*tab{win}, ctx: sctx, cancel: cancel}
	publishTiledPaneOwners(sess, win)
	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	select {
	case got := <-writes:
		t.Fatalf("unsupported animation response leaked into shell input: %q", got)
	default:
	}
	snapshot := win.focusedPane().screen.GraphicsSnapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, uint64(1), snapshot.Usage().Placements, "the supported first frame remains as a static fallback")
}

func TestCaptureCursorInputsHidesFocusedCursorForNoticesOverlay(t *testing.T) {
	_, sess, _, _ := newManualSessionWithPTYs(t, nil)
	p := sess.tabs[0].focusedPane()

	p.mu.Lock()
	cursor := captureCursorInputsLocked(p, domain.Rect{Width: 80, Height: 23}, capturedOverlayRenderState{noticesOverlayActive: true})
	p.mu.Unlock()

	require.True(t, cursor.hiddenByOverlay)
}

func TestCursorTailVisibleHideAndMoveOnly(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	win := sess.tabs[0]
	win.focusedPane().screen.Write([]byte("A"))

	d.paint(sess, ac, true, nil)
	data := mustOutputData(t, sends)
	require.Contains(t, string(data), "\x1b[1 q")
	require.Contains(t, string(data), "\x1b[?25h")

	win.focusedPane().screen.Write([]byte("\x1b[2;3H"))
	d.paint(sess, ac, false, nil)
	data = mustOutputData(t, sends)
	require.Contains(t, string(data), "\x1b[3;3H")
	require.Contains(t, string(data), "\x1b[?25h")

	win.focusedPane().screen.Write([]byte("\x1b[?25l"))
	d.paint(sess, ac, false, nil)
	data = mustOutputData(t, sends)
	require.Contains(t, string(data), "\x1b[?25l")
}

func TestPaintAlignsFloatingCursorWithCommittedGeometry(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	contentArea := domain.Rect{Y: 1, Width: 80, Height: 23}
	cfg := d.currentFloatingConfig()
	committed := calculateContentFloatingGeometry(domain.Size{Cols: contentArea.Width, Rows: contentArea.Height}, cfg)
	floating := newPane("floating", nil, rectSize(committed.Inner))
	writeTestRow(floating.screen, 0, "F")
	floating.popupGeometry = committed
	installTestFloating(testAttachmentTab(sess), floating, true)

	d.paint(sess, ac, true, nil)
	data := mustOutputData(t, sends)
	want := committed.translate(contentArea.X, contentArea.Y)
	require.Contains(t, string(data), cursorCSI(want.Inner.Y+1, 1))
	require.Contains(t, string(data), "F")
	require.Equal(t, want.Inner.Y, ac.output.lastCursor.row)
	require.Equal(t, want.Inner.X, ac.output.lastCursor.col)
	require.Contains(t, string(data), cursorCSI(want.Inner.Y+1, want.Inner.X+1))
}

func TestCursorTailUsesFocusedPanePlacement(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	win := sess.tabs[0]
	win.size = domain.Size{Cols: 80, Rows: 23}
	left := win.focusedPane()
	right := newPane("pane-2", nil, domain.Size{Cols: 39, Rows: 23})
	win.panes[right.id] = right
	win.tree = layout.NewTree(left.id)
	require.NoError(t, win.tree.Split(left.id, layout.Right, true, right.id, domain.Rect{Width: 80, Height: 23}))
	win.tree.Focus = right.id
	right.screen.Write([]byte("\x1b[2;3H"))
	placements, ok := layout.Solve(win.tree.Root, domain.Rect{Width: 80, Height: 23})
	require.True(t, ok)
	rightContent := placementContent(placements, right.id)

	d.paint(sess, ac, true, nil)
	data := mustOutputData(t, sends)
	want := cursorCSI(rightContent.Y+right.screen.CursorRow()+2, rightContent.X+right.screen.CursorCol()+1)
	require.Contains(t, string(data), want)
}

func TestCursorTailUsesExpandedStackContentPlacement(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	win := sess.tabs[0]
	win.size = domain.Size{Cols: 80, Rows: 23}
	one := win.focusedPane()
	two := newPane("pane-2", nil, domain.Size{Cols: 80, Rows: 20})
	three := newPane("pane-3", nil, domain.Size{Cols: 80, Rows: 20})
	win.panes[two.id] = two
	win.panes[three.id] = three
	win.tree = &layout.Tree{
		Root: &layout.Node{Kind: layout.Stack, Children: []*layout.Node{
			layout.NewLeaf(one.id),
			layout.NewLeaf(two.id),
			layout.NewLeaf(three.id),
		}, Expanded: two.id},
		Focus: two.id,
	}
	two.screen.Write([]byte("\x1b[2;3H"))
	placements, ok := layout.Solve(win.tree.Root, domain.Rect{Width: 80, Height: 23})
	require.True(t, ok)
	twoContent := placementContent(placements, two.id)
	require.Greater(t, twoContent.Y, 0, "stack title bars should offset content")

	d.paint(sess, ac, true, nil)
	data := mustOutputData(t, sends)
	want := cursorCSI(twoContent.Y+two.screen.CursorRow()+2, twoContent.X+two.screen.CursorCol()+1)
	require.Contains(t, string(data), want)
}

func cursorCSI(row, col int) string {
	return "\x1b[" + strconv.Itoa(row) + ";" + strconv.Itoa(col) + "H"
}

func TestComposeCopyClientFrameOverlaysBaseAtTarget(t *testing.T) {
	base := renderer.NewFrame(20, 8)
	for y := range base.Height {
		for x := range base.Width {
			base.Set(x, y, renderer.Cell{Rune: '#', Style: renderer.DefaultStyle()})
		}
	}
	p := newPane("floating", nil, domain.Size{Cols: 18, Rows: 2})
	p.screen.Write([]byte("ab\r\ncd"))
	document := scopy.NewSnapshot(p.history, p.screen, p.screen.LineBounds(), nil)
	mode := scopy.NewMode(scopy.NewDocument(document, domain.DefaultWordSeparators))
	target := domain.Rect{X: 2, Y: 3, Width: 18, Height: 2}

	frame, damage := composeCopyClientFrame(mode, target, base, resolveStyles(nil))

	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
	require.Equal(t, "##ab                ", rowText(frame.Row(3)))
	require.Equal(t, "##cd                ", rowText(frame.Row(4)))
	require.Equal(t, strings.Repeat("#", 20), rowText(frame.Row(2)))
	require.Equal(t, strings.Repeat("#", 20), rowText(frame.Row(5)))
	require.Equal(t, strings.Repeat("#", 20), rowText(frame.Row(6)))
	status := rowText(frame.Row(7))
	require.Contains(t, status, "[SCROLL]")
	require.NotContains(t, status, "#")
	require.Equal(t, renderer.BlankCell(), frame.At(19, 7), "status filler must be blank cells, not zero cells")
}

func TestPaintComposesCopyBodyAboveFloating(t *testing.T) {
	normal, releaseNormal := newBlockingPTY(t)
	floatingPTY, releaseFloating := newBlockingPTY(t)
	defer releaseNormal()
	defer releaseFloating()
	d, sess, ac, sends := newManualSessionWithPTYs(t, normal)
	fp := newPane("floating", floatingPTY, domain.Size{Cols: 20, Rows: 3})
	appendHistoryRow(t, fp.history, testRow("flt-old"))
	fp.screen.Write([]byte("flt-live"))
	installTestFloating(testAttachmentTab(sess), fp, true)

	d.enterCopyMode(sess, ac)
	data := mustOutputData(t, sends)
	require.Contains(t, string(data), "[SCROLL]")
	require.Contains(t, string(data), "\x1b[?25l")

	d.handleInput(sess, ac, []byte("g"))
	data = mustOutputData(t, sends)
	require.Contains(t, string(data), "lt-old", "copy viewport of the captured floating pane must compose above the popup")
}
