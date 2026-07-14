package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

func TestPaletteBackdropDimsSimultaneousCopyMode(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	theme := backdropTheme()
	ac.setTheme(theme)
	client := vt.NewScreen(80, 25)
	pane := sess.tabs[0].focusedPane()
	pane.screen.Write([]byte("\x1b[38;2;180;90;30mX"))

	d.enterCopyMode(sess, ac)
	mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	undimmed := client.Frame.At(0, 1)
	require.Equal(t, 'X', undimmed.Rune, "fixture must address copy-mode pane content")
	copyBar := append([]renderer.Cell(nil), client.Frame.Row(client.Frame.Height-1)...)
	require.Contains(t, rowText(copyBar), "[SCROLL]", "fixture must capture the copy status bar")

	d.enterPalette(sess, ac)
	mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	dimmed := client.Frame.At(0, 1)
	require.Equal(t, undimmed.Rune, dimmed.Rune, "palette backdrop must preserve copy content")
	require.Equal(t, themeui.DimStyle(undimmed.Style, theme), dimmed.Style, "palette backdrop must dim the composed copy frame")
	require.Equal(t, copyBar, client.Frame.Row(client.Frame.Height-1), "copy status bar must remain crisp")
	paletteVisible := false
	for y := range client.Frame.Height {
		paletteVisible = paletteVisible || strings.Contains(rowText(client.Frame.Row(y)), "Commands")
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
			installTestFloating(sess.activeTab(), floating, true)

			d.firstPaint(sess, ac, tc.clientSize)

			frame := awaitFrame(t, sends, ports.MsgOutput)
			output, err := ports.UnmarshalOutput(frame.Payload)
			require.NoError(t, err)
			require.Zero(t, output.BaseStateNum, "first paint must be a mandatory reset")
			select {
			case extra := <-sends:
				t.Fatalf("first paint emitted duplicate frame after floating activation resize: %#v", extra)
			default:
			}
		})
	}
}

func TestPaletteBackdropKeepsSimultaneousPickerCrisp(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	ac.setTheme(backdropTheme())
	client := vt.NewScreen(80, 25)
	pane := sess.tabs[0].focusedPane()
	pane.screen.Write([]byte("X"))

	d.enterPicker(sess, ac)
	mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	pickerTitle := client.Frame.At(36, 2)
	require.Equal(t, 'S', pickerTitle.Rune, "fixture must address the picker title")
	undimmedPane := client.Frame.At(0, 1)

	d.enterPalette(sess, ac)
	mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	require.Equal(t, pickerTitle, client.Frame.At(36, 2), "picker composed with palette must remain crisp")
	require.Equal(t, themeui.DimStyle(undimmedPane.Style, backdropTheme()), client.Frame.At(0, 1).Style, "pane content outside overlays must use the theme dim style")
}

func TestPaletteBackdropProductionRenderAndDismissal(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	ac.setTheme(backdropTheme())
	client := vt.NewScreen(80, 25)
	pane := sess.tabs[0].focusedPane()
	pane.screen.Write([]byte("X"))
	d.paint(sess, ac, true, nil)
	mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	undimmed := client.Frame.At(0, 1)
	topBar := client.Frame.At(0, 0)
	bottomBar := client.Frame.At(0, 24)

	d.handleInput(sess, ac, []byte("\x1b "))
	mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	dimmed := client.Frame.At(0, 1)
	require.Equal(t, 'X', dimmed.Rune)
	require.Equal(t, themeui.DimStyle(undimmed.Style, backdropTheme()), dimmed.Style, "open palette must use the theme dim style")
	require.Equal(t, topBar, client.Frame.At(0, 0), "top chrome remains crisp")
	require.Equal(t, bottomBar, client.Frame.At(0, 24), "bottom chrome remains crisp")

	d.handleInput(sess, ac, []byte("\x1b"))
	mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	require.Equal(t, undimmed, client.Frame.At(0, 1), "full redraw restores pane rune and style")
	require.Equal(t, topBar, client.Frame.At(0, 0))
	require.Equal(t, bottomBar, client.Frame.At(0, 24))
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

func awaitOutputFrameWithoutSleep(t *testing.T, sends <-chan ports.Frame) ports.Frame {
	t.Helper()
	frame := <-sends
	require.Equal(t, ports.MsgOutput, frame.Type)
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
		sess.active = 1
		sess.mu.Unlock()
		d.invalidateRender(sess, ac, true, "sync activation")
		fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), urgentRenderDeadline)
		requireNoCoordinatorOutputFrame(t, sends)

		inactiveSteps <- channelPTYStep{data: []byte(" complete\x1b[?2026l")}
		awaitPTYReadProcessed(t, inactiveProcessed)
		fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), urgentRenderDeadline)
		frame := awaitOutputFrameWithoutSleep(t, sends)
		output, err := ports.UnmarshalOutput(frame.Payload)
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
		sess.active = 1
		sess.mu.Unlock()
		newSteps <- channelPTYStep{data: []byte("newly active")}
		awaitPTYReadProcessed(t, newProcessed)
		fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), minOutputRenderDeadline)
		frame := awaitOutputFrameWithoutSleep(t, sends)
		output, err := ports.UnmarshalOutput(frame.Payload)
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

func TestPTYReaderRestoresPrimaryScreenImmediatelyAfterDEC1049Exit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		window        uint64
		exitBeforeACK bool
		exitSuffix    string
		overlay       bool
	}{
		{name: "datagram output window (1): exit waits for ACK", window: 1},
		{name: "datagram output window (1): exit and prompt arrive before ACK", window: 1, exitBeforeACK: true, exitSuffix: "PROMPT> "},
		{name: "local default output window (8): exit waits for ACK", window: maxUnackedOutputStates},
		{name: "local default output window (8): exit and prompt arrive before ACK", window: maxUnackedOutputStates, exitBeforeACK: true, exitSuffix: "PROMPT> "},
		{name: "local default output window (8): palette overlay with bars", window: maxUnackedOutputStates, overlay: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pty, steps, processed := newChannelPTY(t)
			d, sess, ac, sends := newManualSessionWithPTYs(t, pty)
			// Datagram clients use a one-frame window; local clients retain the
			// default eight-frame window. Both ACK orderings stay deterministic.
			ac.output.maxOutstanding = tc.window
			clock := newCoordinatorMockClock(t, 8)
			d.clock = clock.clock
			rc := d.attachCoordinator(sess, nil, ac, true)
			client := vt.NewScreen(80, 25)
			pane := sess.tabs[0].focusedPane()

			d.sessWg.Add(1)
			go d.ptyReader(sess, sess.tabs[0], pane)
			t.Cleanup(func() {
				steps <- channelPTYStep{err: io.EOF}
				d.sessWg.Wait()
				d.waitNotifies()
			})

			replay := func(data []byte, deadline time.Duration) ports.Output {
				t.Helper()
				steps <- channelPTYStep{data: data}
				awaitPTYReadProcessed(t, processed)
				fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), deadline)
				return mustApplyOutput(t, client, awaitOutputFrameWithoutSleep(t, sends))
			}
			acknowledge := func(out ports.Output) {
				ac.ackOutputState(out.NewStateNum)
				rc.notifyAck()
			}

			if tc.overlay {
				d.enterPalette(sess, ac)
				fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), urgentRenderDeadline)
				acknowledge(mustApplyOutput(t, client, awaitOutputFrameWithoutSleep(t, sends)))
			}
			acknowledge(replay([]byte("PRIMARY"), minOutputRenderDeadline))
			primary := client.Frame.Clone()

			// The reported PTY stream is deliberately split into enter, body, and
			// exit reads. MATRIX distinguishes alternate cells in the replayed
			// terminal model from all primary, chrome, and overlay cells.
			acknowledge(replay([]byte("\x1b[?1049h"), minOutputRenderDeadline))
			alternate := replay([]byte(strings.Repeat("MATRIX", 300)), minOutputRenderDeadline)
			pane.mu.Lock()
			require.Contains(t, strings.Join(frameRows(pane.screen.Frame), "\n"), "MATRIX", "fixture must make alternate cells visible to the production renderer")
			pane.mu.Unlock()

			exitData := []byte("\x1b[?1049l" + tc.exitSuffix)
			var exit ports.Output
			if tc.exitBeforeACK {
				steps <- channelPTYStep{data: exitData}
				awaitPTYReadProcessed(t, processed)
				fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), minOutputRenderDeadline)
				if tc.window == 1 {
					requireNoCoordinatorOutputFrame(t, sends)
					acknowledge(alternate)
					exit = mustApplyOutput(t, client, awaitOutputFrameWithoutSleep(t, sends))
				} else {
					exit = mustApplyOutput(t, client, awaitOutputFrameWithoutSleep(t, sends))
					require.Equal(t, alternate.NewStateNum, exit.BaseStateNum,
						"local exit must be rendered before the alternate-frame ACK")
					acknowledge(alternate)
				}
			} else {
				acknowledge(alternate)
				steps <- channelPTYStep{data: exitData}
				awaitPTYReadProcessed(t, processed)
				fireCoordinatorTimer(t, rc, drainCoordinatorTimers(clock), minOutputRenderDeadline)
				exit = mustApplyOutput(t, client, awaitOutputFrameWithoutSleep(t, sends))
			}
			require.NotEmpty(t, exit.Data, "the first post-exit output must actively remove alternate cells")
			rendered := strings.Join(frameRows(client.Frame), "\n")
			require.NotContains(t, rendered, "MATRIX", "the first post-exit frame must not retain alternate cells")
			if tc.exitSuffix == "" {
				require.Equal(t, frameRows(primary), frameRows(client.Frame), "the first post-exit output must restore the primary screen")
			} else {
				require.Contains(t, rendered, tc.exitSuffix, "bytes immediately after DEC 1049 exit must appear in the first post-exit frame")
			}
		})
	}
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
		d.sessions["viewer"] = &session{id: "viewer", client: viewer}
		previews := make(chan renderWake, 2)
		rc.subscribePreviewFor(viewer, 1, func(w renderWake) { previews <- w })

		d.sessWg.Add(1)
		go d.ptyReader(target, target.tabs[0], target.tabs[0].focusedPane())
		steps <- channelPTYStep{data: []byte("\x1b[?2026hpending")}
		awaitPTYReadProcessed(t, processed)
		drainCoordinatorTimers(clock) // detach must clear the pending output work.

		target.mu.Lock()
		target.client = nil
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

	t.Run("replacement receives one complete reset frame without later PTY output", func(t *testing.T) {
		pty, steps, processed := newChannelPTY(t)
		d, target, oldClient, _ := newManualSessionWithPTYs(t, pty)
		clock := newCoordinatorMockClock(t, 8)
		d.clock = clock.clock
		rc := d.attachCoordinator(target, nil, oldClient, true)

		wakes := make(chan renderWake, 2)
		rc.opts.wake = func(w renderWake) { wakes <- w }
		d.sessWg.Add(1)
		go d.ptyReader(target, target.tabs[0], target.tabs[0].focusedPane())
		steps <- channelPTYStep{data: []byte("\x1b[?2026hpending")}
		awaitPTYReadProcessed(t, processed)
		drainCoordinatorTimers(clock)

		replacement := &attachedClient{output: newOutputStateStream()}
		rc.noteReplace(oldClient, replacement, true)
		target.mu.Lock()
		target.client = replacement
		target.mu.Unlock()
		// The replacement's initial full paint remains gated by the batch. Its
		// reset must survive completion and coalesce with the completion wake.
		rc.invalidateForAttachment(replacement, renderInvalidation{class: invalidateUrgent, reset: true, producer: "replacement first paint"})
		replacementTimers := drainCoordinatorTimers(clock)
		steps <- channelPTYStep{data: []byte(" complete\x1b[?2026l")}
		awaitPTYReadProcessed(t, processed)
		requireNoWake(t, wakes)

		fireCoordinatorTimer(t, rc, replacementTimers, urgentRenderDeadline)
		wake := <-wakes
		require.True(t, wake.urgent)
		require.True(t, wake.reset, "the replacement's cleared batch must repaint a complete frame")
		require.Equal(t, 2, wake.coalesced, "completion coalesces only with the replacement's fresh reset batch")
		require.Same(t, replacement, wake.lease.attachment)
		select {
		case duplicate := <-wakes:
			t.Fatalf("sync completion must publish exactly one replacement wake: %#v", duplicate)
		default:
		}

		steps <- channelPTYStep{err: io.EOF}
		d.sessWg.Wait()
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
		target.client = nil
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
	tb := sess.activeTab()
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
	newFixture := func(t *testing.T) (*Daemon, *session, *tab, *pane, chan ports.Frame) {
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
			{name: "headless", setup: func(_ *Daemon, sess *session, _ *tab, _ *pane) { sess.client = nil }},
			{name: "inactive tab", setup: func(_ *Daemon, sess *session, tb *tab, _ *pane) {
				other := newTab(nil, domain.Size{Cols: 80, Rows: 23})
				sess.tabs = append([]*tab{other}, sess.tabs...)
				sess.active = 0
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

		sess.client = nil
		viewer := &attachedClient{}
		viewer.initOverlays()
		viewer.overlays.pickerMu.Lock()
		viewer.overlays.pickerPreview = tb
		viewer.overlays.pickerMu.Unlock()
		d.sessions["viewer"] = &session{id: "viewer", client: viewer}
		p.screen.Write([]byte("preview"))
		_ = d.paneRenderable(sess, tb, p)
		require.NotEmpty(t, p.screen.Damage(), "picker preview damage must remain for coordinator composition")
	})
}

func TestPTYWriteErrorIsLogged(t *testing.T) {
	var logs bytes.Buffer
	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(&logs, nil)))
	errBoom := errors.New("boom")
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Write([]byte("input")).Return(0, errBoom).Once()
	win := newTab(p, domain.Size{Cols: 80, Rows: 23})
	sess := &session{id: "manual", name: "work", tabs: []*tab{win}}
	ac := &attachedClient{}
	ac.initOverlays()
	ac.setSession(sess)

	daemonKeyHandler{d: d, ac: ac}.Forward([]byte("input"))

	got := logs.String()
	if !strings.Contains(got, "pty write failed") || !strings.Contains(got, "boom") || !strings.Contains(got, "work") {
		t.Fatalf("log output %q does not contain PTY write failure details", got)
	}
}

func TestAltXClosesFinalTabAndDetaches(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte("CLT\r"))

	require.Equal(t, 0, sessionCount(d))
	f := awaitFrame(t, sends, ports.MsgDetached)
	det, err := ports.UnmarshalDetached(f.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonSessionKilled, det.Reason)
}

func TestPTYEOFClosesActiveNonFinalTabAndRepaintsRemaining(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d, sess, _, sends := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY2()
	sess.active = 0
	sess.tabs[1].focusedPane().screen.Write([]byte("remaining"))

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.tabs[0], sess.tabs[0].focusedPane())
	releasePTY1()

	require.Eventually(t, func() bool { return tabCount(sess) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, sessionCount(d))
	require.Equal(t, 0, activeTabIndex(sess))
	f := awaitFrame(t, sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(f.Payload)
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
	sess.active = 0
	sess.tabs[0].focusedPane().screen.Write([]byte("active"))

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.tabs[1], sess.tabs[1].focusedPane())
	releasePTY2()

	require.Eventually(t, func() bool { return tabCount(sess) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, sessionCount(d))
	require.Equal(t, 0, activeTabIndex(sess))
	f := awaitFrame(t, sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(f.Payload)
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
	f := awaitFrame(t, sends, ports.MsgDetached)
	det, err := ports.UnmarshalDetached(f.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonSessionKilled, det.Reason)

	d.sessWg.Wait()
}

// --- resize ordering --------------------------------------------------------

func TestResizePreservesLiveContentAndEvictsScrollback(t *testing.T) {
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Resize(domain.Size{Cols: 4, Rows: 2}).Return(nil).Once()
	p.EXPECT().Resize(domain.Size{Cols: 6, Rows: 4}).Return(nil).Once()

	win := newTab(p, domain.Size{Cols: 4, Rows: 4})
	for y, text := range []string{"0000", "1111", "2222", "3333"} {
		copy(win.focusedPane().screen.Frame.Row(y), testRow(text))
	}
	win.focusedPane().screen.Row = 3

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Close().Return(nil).Maybe()
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()

	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	sess := &session{id: "s", name: "s", tabs: []*tab{win}, client: ac}
	ac.setSession(sess)

	// Client rows are one more than the equivalent case in a single-bar
	// layout: tabSize reserves 2 chrome rows (top + bottom bar) here, not 1,
	// so a client height of 4 (not 3) is what yields the same 2-row tab.
	d.resize(sess, ac, domain.Size{Cols: 4, Rows: 4})
	require.Equal(t, "2222", frameRowString(win.focusedPane().screen.Frame, 0))
	require.Equal(t, "3333", frameRowString(win.focusedPane().screen.Frame, 1))
	require.Equal(t, 2, win.focusedPane().history.Len())
	require.Equal(t, "0000", cellsString(win.focusedPane().history.View().Row(0)))
	require.Equal(t, "1111", cellsString(win.focusedPane().history.View().Row(1)))

	d.resize(sess, ac, domain.Size{Cols: 6, Rows: 6})
	require.Equal(t, "2222  ", frameRowString(win.focusedPane().screen.Frame, 0))
	require.Equal(t, "3333  ", frameRowString(win.focusedPane().screen.Frame, 1))
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

func TestComposeTabFrameTwoPaneSplitBlitsDividersDimsAndTranslatesDamage(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 41, Rows: 4})
	left := win.focusedPane()
	left.screen.ClearDamage()
	left.screen.Write([]byte("L"))
	left.screen.ClearDamage()

	right := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 4})
	right.screen.Write([]byte("R"))
	right.screen.ClearDamage()
	right.screen.Write([]byte("x"))

	win.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf(left.id), layout.NewLeaf(right.id)}}
	win.tree.Focus = right.id
	win.panes[right.id] = right

	theme := themeui.Theme{Known: true, TrueColor: true, HasFG: true, HasBG: true, Foreground: renderer.RGB{R: 200, G: 200, B: 200}, Background: renderer.RGB{R: 10, G: 10, B: 10}}
	frame, damage := composeTabFrame(win, domain.Rect{Width: 41, Height: 4}, theme)

	require.Equal(t, 'L', frame.At(0, 0).Rune)
	require.Equal(t, '│', frame.At(20, 0).Rune)
	require.Equal(t, 'R', frame.At(21, 0).Rune)
	require.True(t, frame.At(0, 0).Style.HasForegroundRGB, "unfocused left pane should be dimmed during blit")
	require.False(t, left.screen.Frame.At(0, 0).Style.HasForegroundRGB, "dimming must not mutate vt.Screen")
	require.Contains(t, damage, renderer.Damage{Kind: renderer.DamageText, X: 22, Y: 0, Width: 1, Height: 1, Count: 1})
}

func TestComposeClientFrameCacheSkipsUndamagedPaneBlits(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 41, Rows: 4})
	left := win.focusedPane()
	right := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 4})
	win.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf(left.id), layout.NewLeaf(right.id)}}
	win.tree.Focus = right.id
	win.panes[right.id] = right
	left.screen.Write([]byte("L"))
	right.screen.Write([]byte("R"))
	var bars barCache
	var composed composedFrameCache
	sess := &session{id: "s", name: "work", tabs: []*tab{win}}
	composeClientFrameWithLayoutCached(barState{status: sess.statusSegments(true)}, win, true, solveTabLayoutLocked(win), &bars, &composed)
	left.screen.ClearDamage()
	right.screen.ClearDamage()
	left.screen.Frame.Set(0, 0, renderer.Cell{Rune: 'Z'})
	right.screen.Write([]byte("x"))

	frame, damage := composeClientFrameWithLayoutCached(barState{status: sess.statusSegments(true)}, win, false, solveTabLayoutLocked(win), &bars, &composed)

	require.Equal(t, 'L', frame.At(0, 1).Rune, "undamaged left pane should remain cached, not re-blitted")
	require.Equal(t, 'x', frame.At(22, 1).Rune)
	require.Contains(t, damage, renderer.Damage{Kind: renderer.DamageText, X: 22, Y: 1, Width: 1, Height: 1, Count: 1})
}

func TestPaintComposeConsumesOnlyDamageIncludedInFrame(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 20, Rows: 4})
	p := win.focusedPane()
	p.screen.Write([]byte("old"))
	var bars barCache
	var composed composedFrameCache
	state := barState{status: (&session{id: "s", name: "work", tabs: []*tab{win}}).statusSegments(true)}

	_, damage := composeClientFrameWithLayoutCachedConsumeDamage(state, win, false, solveTabLayoutLocked(win), &bars, &composed)
	require.NotEmpty(t, damage)
	require.Empty(t, p.screen.Damage(), "paint compose should consume damage it included in the frame")

	p.screen.Write([]byte("new"))
	require.NotEmpty(t, p.screen.Damage(), "damage produced after paint compose must remain for a later render")

	_, _ = composeClientFrameWithLayoutCached(state, win, false, solveTabLayoutLocked(win), &bars, &composed)
	require.NotEmpty(t, p.screen.Damage(), "standalone compose helpers should remain non-consuming")
}

func TestPaintComposeClearsCollapsedPaneDamage(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 20, Rows: 5})
	p1 := win.focusedPane()
	p2 := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 3})
	win.tree.Root = &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(p1.id), layout.NewLeaf(p2.id)}, Expanded: p2.id}
	win.tree.Focus = p2.id
	win.panes[p2.id] = p2
	p1.screen.Write([]byte("hidden"))
	p2.screen.Write([]byte("shown"))
	var bars barCache
	var composed composedFrameCache
	state := barState{status: (&session{id: "s", name: "work", tabs: []*tab{win}}).statusSegments(true)}

	_, _ = composeClientFrameWithLayoutCachedConsumeDamage(state, win, false, solveTabLayoutLocked(win), &bars, &composed)

	require.Empty(t, p1.screen.Damage(), "collapsed pane damage should not accumulate while paint renders stack title bars")
	require.Empty(t, p2.screen.Damage())
}

func TestComposeClientFrameCacheFocusChangeReblitsDimmedPanes(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 41, Rows: 4})
	left := win.focusedPane()
	right := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 4})
	win.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf(left.id), layout.NewLeaf(right.id)}}
	win.panes[right.id] = right
	left.screen.Write([]byte("L"))
	right.screen.Write([]byte("R"))
	theme := themeui.Theme{Known: true, TrueColor: true, HasFG: true, HasBG: true, Foreground: renderer.RGB{R: 200, G: 200, B: 200}, Background: renderer.RGB{R: 10, G: 10, B: 10}}
	var bars barCache
	var composed composedFrameCache
	state := barState{status: (&session{id: "s", name: "work", tabs: []*tab{win}}).statusSegments(true), theme: theme}
	win.tree.Focus = left.id
	composeClientFrameWithLayoutCached(state, win, true, solveTabLayoutLocked(win), &bars, &composed)
	left.screen.ClearDamage()
	right.screen.ClearDamage()
	leftWasFocused := composed.frame.At(0, 1).Style
	rightWasDimmed := composed.frame.At(21, 1).Style

	win.tree.Focus = right.id
	frame, damage := composeClientFrameWithLayoutCached(state, win, false, solveTabLayoutLocked(win), &bars, &composed)

	require.Equal(t, rightWasDimmed, frame.At(0, 1).Style, "previously focused pane should be re-blitted dimmed")
	require.Equal(t, leftWasFocused, frame.At(21, 1).Style, "newly focused pane should be re-blitted undimmed")
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
}

func TestComposeClientFrameCacheLayoutChangeClearsStaleDividers(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 41, Rows: 5})
	left := win.focusedPane()
	right := newPane("pane-2", nil, domain.Size{Cols: 41, Rows: 5})
	win.panes[right.id] = right
	left.screen.Write([]byte("L"))
	right.screen.Write([]byte("R"))
	var bars barCache
	var composed composedFrameCache
	state := barState{status: (&session{id: "s", name: "work", tabs: []*tab{win}}).statusSegments(true)}
	win.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf(left.id), layout.NewLeaf(right.id)}}
	composeClientFrameWithLayoutCached(state, win, true, solveTabLayoutLocked(win), &bars, &composed)
	require.Equal(t, '│', composed.frame.At(20, 1).Rune)
	left.screen.ClearDamage()
	right.screen.ClearDamage()

	win.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Vertical, Children: []*layout.Node{layout.NewLeaf(left.id), layout.NewLeaf(right.id)}}
	frame, damage := composeClientFrameWithLayoutCached(state, win, false, solveTabLayoutLocked(win), &bars, &composed)

	require.NotEqual(t, '│', frame.At(20, 1).Rune, "old vertical divider must be cleared when layout changes")
	require.Equal(t, '─', frame.At(20, 3).Rune)
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
}

func TestComposeTabFrameCachedTitleBarsDoNotAllocate(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 20, Rows: 5})
	p := win.focusedPane()
	p.title.processName = "shell"
	win.tree.Root = &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(p.id)}, Expanded: p.id}
	layoutSnap := solveTabLayoutLocked(win)
	frame := renderer.NewFrame(20, 5)
	titleGenerations := map[layout.PaneID]uint64{p.id: p.title.generation}
	p.screen.ClearDamage()

	composeTabFrameIntoWithLayoutOptions(win, frame, domain.Rect{Width: 20, Height: 5}, themeui.Theme{}, layoutSnap, true, false, titleGenerations)
	allocs := testing.AllocsPerRun(100, func() {
		composeTabFrameIntoWithLayoutOptions(win, frame, domain.Rect{Width: 20, Height: 5}, themeui.Theme{}, layoutSnap, true, false, titleGenerations)
	})
	require.Zero(t, allocs, "valid cached title-bar renders must not allocate")
}

func TestComposeTabFrameStackUsesShellFallback(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 20, Rows: 5})
	p := win.focusedPane()
	p.title.displayFallback = "fish"
	win.tree.Root = &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(p.id)}, Expanded: p.id}

	frame, _ := composeTabFrame(win, domain.Rect{Width: 20, Height: 5}, themeui.Theme{})

	require.Equal(t, "fish", rowText(frame.Row(0))[:4])
}

func TestComposeTabFrameStackDrawsTitleBarsAndDimsCollapsed(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 20, Rows: 5})
	p1 := win.focusedPane()
	p1.title.processName = "one"
	p1.screen.ClearDamage()
	p2 := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 3})
	p2.title.processName = "two"
	p2.screen.Write([]byte("T"))
	p2.screen.ClearDamage()

	win.tree.Root = &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(p1.id), layout.NewLeaf(p2.id)}, Expanded: p2.id}
	win.tree.Focus = p2.id
	win.panes[p2.id] = p2

	theme := themeui.Theme{Known: true, TrueColor: true, HasFG: true, HasBG: true, Foreground: renderer.RGB{R: 200, G: 200, B: 200}, Background: renderer.RGB{R: 10, G: 10, B: 10}}
	frame, _ := composeTabFrame(win, domain.Rect{Width: 20, Height: 5}, theme)

	require.Equal(t, "one", rowText(frame.Row(0))[:3])
	require.Equal(t, "two", rowText(frame.Row(1))[:3])
	require.Equal(t, 'T', frame.At(0, 2).Rune)
	require.True(t, frame.At(0, 0).Style.HasForegroundRGB, "collapsed title bar should use dimmed chrome")
	require.True(t, frame.At(0, 1).Style.Inverse || frame.At(0, 1).Style.HasBackgroundRGB, "focused title bar should use accent chrome")
}

func TestComposeClientFrameCacheRefreshesStackTitleBars(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 20, Rows: 5})
	p1 := win.focusedPane()
	p1.title.processName = "one"
	p2 := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 3})
	p2.title.processName = "two"
	p2.screen.Write([]byte("T"))
	win.tree.Root = &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(p1.id), layout.NewLeaf(p2.id)}, Expanded: p2.id}
	win.tree.Focus = p2.id
	win.panes[p2.id] = p2
	var bars barCache
	var composed composedFrameCache
	state := barState{status: (&session{id: "s", name: "work", tabs: []*tab{win}}).statusSegments(true)}
	composeClientFrameWithLayoutCached(state, win, true, solveTabLayoutLocked(win), &bars, &composed)
	p1.screen.ClearDamage()
	p2.screen.ClearDamage()

	p1.title.processName = "renamed"
	p1.title.generation++
	frame, damage := composeClientFrameWithLayoutCached(state, win, false, solveTabLayoutLocked(win), &bars, &composed)

	require.Equal(t, "renamed", rowText(frame.Row(1))[:7])
	require.NotEqual(t, []renderer.Damage{renderer.FullRedraw()}, damage, "title-only changes should not force a full layout reset")
	require.Contains(t, damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 1, Width: 20, Height: 1})

	_, damage = composeClientFrameWithLayoutCached(state, win, false, solveTabLayoutLocked(win), &bars, &composed)
	require.NotContains(t, damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 1, Width: 20, Height: 1}, "unchanged title must not redraw its row")
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

func TestComposeClientFrameBarCacheSkipsUnchangedBars(t *testing.T) {
	sess, win := newBarCacheTestSession()
	var cache barCache

	_, damage := composeClientFrame(sess, win, true, "", &cache)
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
	win.focusedPane().screen.ClearDamage()
	win.focusedPane().screen.Write([]byte("x"))

	_, damage = composeClientFrame(sess, win, false, "", &cache)

	require.NotEmpty(t, damage)
	for _, d := range damage {
		require.NotEqual(t, 0, d.Y, "unchanged top bar should not be damaged")
		require.NotEqual(t, win.focusedPane().screen.Frame.Height+1, d.Y, "unchanged bottom bar should not be damaged")
	}
}

func TestComposeClientFrameBarCacheDamagesChangedBottomOnly(t *testing.T) {
	sess, win := newBarCacheTestSession()
	var cache barCache
	composeClientFrame(sess, win, true, "", &cache)
	win.focusedPane().screen.ClearDamage()

	sess.name = "renamed"
	_, damage := composeClientFrame(sess, win, false, "", &cache)

	require.Equal(t, []renderer.Damage{{Kind: renderer.DamageText, X: 0, Y: win.focusedPane().screen.Frame.Height + 1, Width: win.focusedPane().screen.Frame.Width, Height: 1}}, damage)
}

func TestComposeClientFrameFullRedrawPrimesBarCache(t *testing.T) {
	sess, win := newBarCacheTestSession()
	var cache barCache
	cache.top = []renderer.Cell{renderer.BlankCell()}
	cache.bottom = []renderer.Cell{renderer.BlankCell()}

	_, damage := composeClientFrame(sess, win, true, "", &cache)
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
	win.focusedPane().screen.ClearDamage()

	_, damage = composeClientFrame(sess, win, false, "", &cache)
	require.Empty(t, damage)
}

func newBarCacheTestSession() (*session, *tab) {
	win := newTab(newScriptPTY(nil), domain.Size{Cols: 20, Rows: 3})
	sess := &session{id: "s", name: "work", tabs: []*tab{win}}
	return sess, win
}

func TestResizeOrdersPTYBeforeScreen(t *testing.T) {
	newSize := domain.Size{Cols: 100, Rows: 30}

	p := portsmocks.NewMockPTY(t)
	var screenWidthAtResize int
	win := newTab(newScriptPTY(nil), domain.Size{Cols: 80, Rows: 24})
	p.EXPECT().Resize(domain.Size{Cols: 100, Rows: 28}).RunAndReturn(func(sz domain.Size) error {
		// The screen must not yet be resized when the PTY is: proves order.
		screenWidthAtResize = win.focusedPane().screen.Frame.Width
		return nil
	}).Once()
	win.focusedPane().pty = p

	var gotOutput atomic.Bool
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Close().Return(nil).Maybe()
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		if f.Type == ports.MsgOutput {
			gotOutput.Store(true)
		}
		return nil
	}).Maybe()

	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	sess := &session{id: "s", name: "s", tabs: []*tab{win}, client: ac}
	ac.setSession(sess)

	d.resize(sess, ac, newSize)
	// This test verifies resize ordering, not the idle fallback. Consume the
	// pending resize paint deterministically under the non-firing stub clock.
	d.paint(sess, ac, false, nil)

	require.Equal(t, 80, screenWidthAtResize, "pty.Resize must run before screen.Resize")
	require.Equal(t, 100, win.focusedPane().screen.Frame.Width, "screen resized after pty")
	require.Equal(t, 28, win.focusedPane().screen.Frame.Height, "screen reserves top and bottom chrome rows")
	require.True(t, gotOutput.Load(), "resize forces a full redraw output")
}

// --- reader EOF -> registry-empty shutdown ----------------------------------

func TestSendErrorKeepsEphemeralHeadless(t *testing.T) {
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Close().Return(nil).Maybe()

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(io.ErrClosedPipe).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()

	d := newTestDaemon(t, nil, stubClock{})
	win := newTab(p, domain.Size{Cols: 20, Rows: 5})
	sctx, cancel := context.WithCancel(context.Background())
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	sess := &session{id: "e", name: "0", ephemeral: true, tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)
	d.sessions[sess.id] = sess

	win.mu.Lock()
	win.focusedPane().screen.Write([]byte("x"))
	win.mu.Unlock()

	d.paint(sess, ac, true, nil)

	require.Equal(t, 1, sessionCount(d), "ephemeral session survives failed client send")
	sess.mu.Lock()
	require.Nil(t, sess.client)
	sess.mu.Unlock()

	_ = d.killSession(sess, ports.ReasonServerShutdown, false)
	cancel()
	d.waitNotifies()
}

func TestPTYQueryGetsResponseWrittenBackToPTY(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := portsmocks.NewMockPTY(t)
	chunks := [][]byte{[]byte("\x1b[6n")}
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
	sess := &session{id: "query", name: "query", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)
	d.sessions[sess.id] = sess
	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	select {
	case got := <-writes:
		require.Equal(t, []byte("\x1b[1;1R"), got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for PTY response write")
	}
	select {
	case f := <-sends:
		require.NotEqual(t, ports.MsgOutput, f.Type)
	default:
	}
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
	floating.screen.Frame.Set(0, 0, renderer.Cell{Rune: 'F'})
	floating.popupGeometry = committed
	installTestFloating(sess.activeTab(), floating, true)

	d.paint(sess, ac, true, nil)
	data := mustOutputData(t, sends)
	want := committed.translate(contentArea.X, contentArea.Y)
	require.Contains(t, string(data), cursorCSI(want.Inner.Y+1, 1))
	require.Contains(t, string(data), "F")
	require.Equal(t, want.Inner.Y, ac.lastCursor.row)
	require.Equal(t, want.Inner.X, ac.lastCursor.col)
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
	document := scopy.NewSnapshot(p.history, p.screen.Frame)
	mode := scopy.NewMode(document)
	target := domain.Rect{X: 2, Y: 3, Width: 18, Height: 2}

	frame, damage := composeCopyClientFrame(mode, &document, target, base, barState{})

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

func TestCopyTargetRectLocked(t *testing.T) {
	tb := newTab(nil, domain.Size{Cols: 80, Rows: 23})
	cfg := domain.Defaults().Floating
	contentArea := domain.Rect{Y: 1, Width: 80, Height: 23}
	fp := newPane("floating", nil, domain.Size{Cols: 10, Rows: 4})
	stray := newPane("stray", nil, domain.Size{Cols: 10, Rows: 4})
	tb.mu.Lock()
	defer tb.mu.Unlock()
	layoutSnap := solveTabLayoutLocked(tb)
	main := tb.focusedPane()
	fp.popupGeometry = calculateContentFloatingGeometry(domain.Size{Cols: contentArea.Width, Rows: contentArea.Height}, cfg)
	floatingFrameGeometry := fp.popupGeometry.translate(contentArea.X, contentArea.Y)

	cases := []struct {
		name        string
		pane        *pane
		floating    *pane
		hasFloating bool
		want        domain.Rect
	}{
		{name: "floating source targets committed frame-absolute popup inner", pane: fp, floating: fp, hasFloating: true, want: calculateContentFloatingGeometry(domain.Size{Cols: contentArea.Width, Rows: contentArea.Height}, cfg).translate(contentArea.X, contentArea.Y).Inner},
		{name: "normal source targets solved placement", pane: main, want: domain.Rect{X: 0, Y: 1, Width: 80, Height: 23}},
		{name: "unplaced source falls back to pane frame", pane: stray, want: domain.Rect{X: 0, Y: 1, Width: 10, Height: 4}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := copyTargetRectLocked(layoutSnap, contentArea, tc.pane, tc.floating, tc.hasFloating, floatingFrameGeometry)
			require.Equal(t, tc.want, got)
			if tc.hasFloating {
				require.NotEqual(t, fp.popupGeometry.Inner, got, "content-relative geometry must not target the client frame directly")
			}
		})
	}
}

func TestPaintComposesCopyBodyAboveFloating(t *testing.T) {
	normal, releaseNormal := newBlockingPTY(t)
	floatingPTY, releaseFloating := newBlockingPTY(t)
	defer releaseNormal()
	defer releaseFloating()
	d, sess, ac, sends := newManualSessionWithPTYs(t, normal)
	fp := newPane("floating", floatingPTY, domain.Size{Cols: 20, Rows: 3})
	fp.history.Append(testRow("flt-old"))
	fp.screen.Write([]byte("flt-live"))
	installTestFloating(sess.activeTab(), fp, true)

	d.enterCopyMode(sess, ac)
	data := mustOutputData(t, sends)
	require.Contains(t, string(data), "[SCROLL]")
	require.Contains(t, string(data), "\x1b[?25l")

	d.handleInput(sess, ac, []byte("g"))
	data = mustOutputData(t, sends)
	require.Contains(t, string(data), "lt-old", "copy viewport of the captured floating pane must compose above the popup")
}
