package daemon

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/ui"
)

func TestPaletteOpenTypeEnterRunAndEscClose(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p1, p2)
	defer release1()
	defer release2()

	d.handleInput(sess, ac, []byte("\x1b "))
	require.True(t, ac.overlays.paletteActive())
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "Commands")

	d.handleInput(sess, ac, []byte("NXT\r"))
	require.False(t, ac.overlays.paletteActive())
	require.Equal(t, 1, activeTabIndex(sess))
	awaitFrame(t, sends, ports.MsgOutput)

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("\x1b"))
	require.False(t, ac.overlays.paletteActive())
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPaletteCNSPromptsForSessionNameThenCreatesAndSwitches(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p1)
	defer release1()
	defer release2()
	ptys := portsmocks.NewMockPTYFactory(t)
	ptys.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(p2, nil).Once()
	d.ptys = ptys

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("CNS\r"))
	promptFrame := awaitFrame(t, sends, ports.MsgOutput)
	promptOutput, err := ports.UnmarshalOutput(promptFrame.Payload)
	require.NoError(t, err)
	require.False(t, ac.overlays.paletteActive())
	require.True(t, ac.overlays.promptActive())
	require.Contains(t, string(promptOutput.Data), "Create session")

	d.handleInput(sess, ac, []byte("scratch\r"))
	// The submit first paints the newly attached session while the prompt is
	// still open, then handlePromptInput closes the prompt and repaints the
	// client's current session. The final frame must be for the new session.
	awaitFrame(t, sends, ports.MsgOutput)
	finalRepaint := awaitFrame(t, sends, ports.MsgOutput)
	finalOutput, err := ports.UnmarshalOutput(finalRepaint.Payload)
	require.NoError(t, err)
	require.False(t, ac.overlays.promptActive())
	require.Equal(t, 2, sessionCount(d))
	require.Nil(t, sess.client)
	newSess := ac.currentSession()
	require.NotNil(t, newSess)
	require.NotSame(t, sess, newSess)
	require.Equal(t, "scratch", newSess.name)
	require.False(t, newSess.ephemeral)
	require.Same(t, ac, newSess.client)
	require.Contains(t, string(finalOutput.Data), "scratch")
	require.NotContains(t, string(finalOutput.Data), "Create session")
}

func TestPaletteReopensWithSuccessfulCommandFirst(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("NXT\r"))
	require.False(t, ac.overlays.paletteActive())
	awaitFrame(t, sends, ports.MsgOutput)

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	cmd, ok := ac.overlays.palette.Selected()
	require.True(t, ok)
	require.Equal(t, "NXT", cmd.Code)
}

func TestPaletteRecentCommandsNewestFirstThenRegistryOrder(t *testing.T) {
	d := &Daemon{}
	d.recordPaletteUse("SSP")
	d.recordPaletteUse("NXT")
	// STALE is not a registered command code; it must be dropped from output.
	d.recordPaletteUse("STALE")
	d.recordPaletteUse("SSP")

	commands := d.paletteCommands()
	codes := make([]string, len(commands))
	for i, cmd := range commands {
		codes[i] = cmd.Code
	}
	require.Equal(t, []string{"SSP", "NXT", "CNT", "CNS", "CLT", "SPR", "SPL", "SPU", "SPD", "STP", "TST", "CLP", "FPL", "FPR", "FPU", "FPD", "PVT", "VIS", "RNS", "DET"}, codes)
}

func TestPaletteRecencyCanBeUpdatedConcurrently(t *testing.T) {
	d := &Daemon{}
	codes := []string{"CNT", "CNS", "CLT", "SPR", "SPL", "SPU", "SPD", "STP", "TST", "CLP", "FPL", "FPR", "FPU", "FPD", "NXT", "PVT", "SSP", "VIS", "RNS", "DET"}

	var wg sync.WaitGroup
	for range 50 {
		for _, code := range codes {
			wg.Add(1)
			go func(code string) {
				defer wg.Done()
				d.recordPaletteUse(code)
				_ = d.paletteCommands()
			}(code)
		}
	}
	wg.Wait()

	commands := d.paletteCommands()
	require.Len(t, commands, len(codes))
	seen := map[string]bool{}
	for _, cmd := range commands {
		require.False(t, seen[cmd.Code], "duplicate command %s", cmd.Code)
		seen[cmd.Code] = true
	}
}

func TestPaletteCommandNoopRepaintsAfterClose(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1b "))
	paletteFrame := awaitFrame(t, sends, ports.MsgOutput)
	paletteOutput, err := ports.UnmarshalOutput(paletteFrame.Payload)
	require.NoError(t, err)
	require.Contains(t, string(paletteOutput.Data), "Commands")

	d.handleInput(sess, ac, []byte("NXT\r"))
	require.False(t, ac.overlays.paletteActive())
	repaint := awaitFrame(t, sends, ports.MsgOutput)
	repaintOutput, err := ports.UnmarshalOutput(repaint.Payload)
	require.NoError(t, err)
	require.NotContains(t, string(repaintOutput.Data), "Commands")
}

func TestPaletteCreateTabErrorRepaintsAfterClose(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()
	ptys := portsmocks.NewMockPTYFactory(t)
	ptys.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("open failed"))
	d.ptys = ptys

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("CNT\r"))
	require.False(t, ac.overlays.paletteActive())
	repaint := awaitFrame(t, sends, ports.MsgOutput)
	repaintOutput, err := ports.UnmarshalOutput(repaint.Payload)
	require.NoError(t, err)
	require.NotContains(t, string(repaintOutput.Data), "Commands")
	require.Len(t, sess.tabs, 1)
}

func TestPaletteEnterNoMatchKeepsOpenAndEscapeSplit(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("zzzz\r"))
	require.True(t, ac.overlays.paletteActive())
	awaitFrame(t, sends, ports.MsgOutput)

	d.handlePaletteInput(ac, []byte{0x1b, '['})
	require.True(t, ac.overlays.paletteActive())
	require.Equal(t, []byte{0x1b, '['}, ac.overlays.palettePending)
	d.handlePaletteInput(ac, []byte{'A'})
	require.True(t, ac.overlays.paletteActive())
	require.Empty(t, ac.overlays.palettePending)
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPaletteCtrlNAndCtrlPNavigate(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)

	d.handlePaletteInput(ac, []byte{0x0e})
	awaitFrame(t, sends, ports.MsgOutput)
	cmd, ok := ac.overlays.palette.Selected()
	require.True(t, ok)
	require.Equal(t, "CNS", cmd.Code)

	d.handlePaletteInput(ac, []byte{0x10})
	awaitFrame(t, sends, ports.MsgOutput)
	cmd, ok = ac.overlays.palette.Selected()
	require.True(t, ok)
	require.Equal(t, "CNT", cmd.Code)
}

func TestPaletteModalGeometry(t *testing.T) {
	require.Equal(t, 100, paletteModal.WidthPct)
	require.Equal(t, 11, paletteModal.FixedHeight)
	require.Equal(t, ui.AnchorBottom, paletteModal.Anchor)
	require.Equal(t, 1, paletteModal.BottomMargin)
	require.Equal(t, 32, paletteModal.MinWidth)

	bounds := paletteModal.Bounds(domain.Size{Cols: 120, Rows: 40})
	require.Equal(t, domain.Rect{X: 0, Y: 28, Width: 120, Height: 11}, bounds)
}

func TestPaletteUTF8PendingCompletesFilter(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handlePaletteInput(ac, []byte{0xc3})
	require.Equal(t, []byte{0xc3}, ac.overlays.palettePending)
	d.handlePaletteInput(ac, []byte{0xa9})
	require.Empty(t, ac.overlays.palettePending)
	require.Equal(t, "é", ac.overlays.palette.Query())
}

func TestPaletteRenderAndInputCanRunConcurrently(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	d.enterPalette(sess, ac)

	drainDone := make(chan struct{})
	drainStopped := make(chan struct{})
	go func() {
		defer close(drainStopped)
		for {
			select {
			case <-sends:
			case <-drainDone:
				return
			}
		}
	}()
	defer func() {
		close(drainDone)
		<-drainStopped
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			d.paint(sess, ac, true)
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			d.handlePaletteInput(ac, []byte("abc\x7f"))
		}
	}()
	wg.Wait()
}

func TestPaletteExecMethods(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	p3, release3 := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p1, p2, p3)
	defer release1()
	defer release2()
	defer release3()
	exec := paletteExec{d: d, sess: sess, ac: ac}

	require.NoError(t, exec.NextTab())
	require.Equal(t, 1, activeTabIndex(sess))
	require.NoError(t, exec.PrevTab())
	require.Equal(t, 0, activeTabIndex(sess))
	sess.ephemeral = true
	require.NoError(t, exec.RenameSession())
	require.True(t, ac.overlays.promptActive())
	d.closePrompt(ac)
	require.True(t, sess.ephemeral)
	require.NoError(t, exec.OpenSessionPicker())
	require.True(t, ac.overlays.pickerActive())
	d.closePicker(ac)
	require.NoError(t, exec.EnterVisualMode())
	require.True(t, ac.overlays.copyActive())
	d.exitCopyMode(ac)
	require.NoError(t, exec.SplitRight())
	require.NoError(t, exec.SplitLeft())
	require.NoError(t, exec.SplitUp())
	require.NoError(t, exec.SplitDown())
	require.NoError(t, exec.StackPane())
	require.NoError(t, exec.ToggleStack())
	require.NoError(t, exec.ClosePane())
	require.NoError(t, exec.FocusPaneLeft())
	require.NoError(t, exec.FocusPaneRight())
	require.NoError(t, exec.FocusPaneUp())
	require.NoError(t, exec.FocusPaneDown())
	require.NoError(t, exec.CloseTab())
	require.Len(t, sess.tabs, 2)
	// Drain paints generated by methods above.
	for len(sends) > 0 {
		<-sends
	}
}
