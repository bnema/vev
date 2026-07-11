package daemon

import (
	"context"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestPromptModalGeometry(t *testing.T) {
	base := domain.Size{Cols: 100, Rows: 40}

	require.Equal(t, domain.Rect{X: 0, Y: 35, Width: 100, Height: 4}, promptModalFor(" Prompt ").Bounds(base))
}

func TestEnterPromptRendersTitleAndPrefill(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()

	d.enterPrompt(sess, ac, " Rename session ", "0", func(string) error { return nil })
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "Rename session")
	require.Contains(t, string(msg.Data), "> 0")
	require.True(t, ac.overlays.promptActive())
}

func TestPromptSubmitRenamesAndPromotesSession(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	sess.name = "0"
	sess.ephemeral = true

	d.enterPrompt(sess, ac, " Rename session ", sess.name, func(name string) error { return d.renameSession(sess, name) })
	awaitFrame(t, sends, ports.MsgOutput)
	d.handlePromptInput(ac, []byte("work\r"))
	repaint := awaitFrame(t, sends, ports.MsgOutput)

	require.False(t, ac.overlays.promptActive())
	require.Equal(t, "0work", sess.name)
	require.False(t, sess.ephemeral)
	out, err := ports.UnmarshalOutput(repaint.Payload)
	require.NoError(t, err)
	require.Contains(t, string(out.Data), "0work")
	require.NotContains(t, string(out.Data), "0work*")
}

func TestPromptSubmitErrorKeepsPromptOpen(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p1)
	defer release1()
	defer release2()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.sessions["other"] = &session{id: "other", name: "taken", ctx: ctx, cancel: cancel, tabs: []*tab{newTestTabWithContext(p2, ctx, cancel)}}

	d.enterPrompt(sess, ac, " Rename session ", "", func(name string) error { return d.renameSession(sess, name) })
	awaitFrame(t, sends, ports.MsgOutput)
	d.handlePromptInput(ac, []byte("taken\r"))
	repaint := awaitFrame(t, sends, ports.MsgOutput)

	require.True(t, ac.overlays.promptActive())
	require.NotEqual(t, "taken", sess.name)
	out, err := ports.UnmarshalOutput(repaint.Payload)
	require.NoError(t, err)
	require.Contains(t, string(out.Data), "name already in use")
}

func TestPromptEscapeCancelsWithoutRename(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	oldName := sess.name

	d.enterPrompt(sess, ac, " Rename session ", oldName, func(name string) error { return d.renameSession(sess, name) })
	awaitFrame(t, sends, ports.MsgOutput)
	d.handlePromptInput(ac, []byte("new\x1b"))
	awaitFrame(t, sends, ports.MsgOutput)

	require.False(t, ac.overlays.promptActive())
	require.Equal(t, oldName, sess.name)
}

func TestPaletteRNSOpensRenamePrompt(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	sess.name = "0"
	sess.ephemeral = true

	d.enterPalette(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handlePaletteInput(ac, []byte("RNS\r"))
	out := awaitFrame(t, sends, ports.MsgOutput)

	require.False(t, ac.overlays.paletteActive())
	require.True(t, ac.overlays.promptActive())
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "Rename session")
	require.Contains(t, string(msg.Data), "> 0")
}
