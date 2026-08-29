package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

type failingPTYFactory struct{ err error }

func (f failingPTYFactory) Open(context.Context, string, []string, []string, string, domain.Geometry) (ports.PTY, error) {
	return nil, f.err
}

func TestPromptModalGeometry(t *testing.T) {
	base := domain.Size{Cols: 100, Rows: 40}

	require.Equal(t, domain.Rect{X: 0, Y: 35, Width: 100, Height: 4}, promptModalFor(" Prompt ").Resolve(base).Bounds)
}

func TestEnterPromptRendersTitleAndPrefill(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()

	d.enterPrompt(sess, ac, " Rename session ", "0", func(string) error { return nil })
	out := awaitFrame(t, sends, wire.MsgOutput)
	msg, err := wire.UnmarshalOutput(out.Payload)
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
	awaitFrame(t, sends, wire.MsgOutput)
	d.handlePromptInput(ac, []byte("work\r"))
	repaint := awaitFrame(t, sends, wire.MsgOutput)

	require.False(t, ac.overlays.promptActive())
	require.Equal(t, "0work", sess.name)
	require.False(t, sess.ephemeral)
	out, err := wire.UnmarshalOutput(repaint.Payload)
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
	d.sessions["other"] = &session{sessionCore: sessionCore{id: "other", name: "taken"}, ctx: ctx, cancel: cancel, tabs: []*tab{newTestTabWithContext(p2, ctx, cancel)}}

	d.enterPrompt(sess, ac, " Rename session ", "", func(name string) error { return d.renameSession(sess, name) })
	awaitFrame(t, sends, wire.MsgOutput)
	d.handlePromptInput(ac, []byte("taken\r"))
	repaint := awaitFrame(t, sends, wire.MsgOutput)

	require.True(t, ac.overlays.promptActive())
	require.NotEqual(t, "taken", sess.name)
	out, err := wire.UnmarshalOutput(repaint.Payload)
	require.NoError(t, err)
	require.Contains(t, string(out.Data), "name already in use")
}

func TestTransitionPromptFailuresStayVisible(t *testing.T) {
	tests := []struct {
		name    string
		submit  error
		wantErr string
	}{
		{name: "destination unavailable", submit: errCreateDestinationUnavailable, wantErr: "that destination is no longer available"},
		{name: "transition failed", submit: errors.New("transition failed"), wantErr: "transition failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, release := newBlockingPTY(t)
			defer release()
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)

			d.enterTransitionPrompt(sess, ac, " Create session ", "", func(string, *attachmentEffect) error {
				return tt.submit
			})
			awaitFrame(t, sends, wire.MsgOutput)
			d.handlePromptInput(ac, []byte("example\r"))
			repaint := awaitFrame(t, sends, wire.MsgOutput)

			require.True(t, ac.overlays.promptActive())
			out, err := wire.UnmarshalOutput(repaint.Payload)
			require.NoError(t, err)
			require.Contains(t, string(out.Data), tt.wantErr)
		})
	}
}

func TestPromptSubmitSessionSpawnFailureReportsOneSafeNotice(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	cause := errors.New("fork/exec /bin/sh: permission denied")
	d.ptys = failingPTYFactory{err: cause}

	// Submit through the actual prompt input boundary rather than calling its
	// callback directly.
	d.enterPrompt(sess, ac, " Create session ", "", func(name string) error {
		return d.createSessionAndSwitch(sess, ac, name)
	})
	awaitFrame(t, sends, wire.MsgOutput)
	d.handlePromptInput(ac, []byte("new\r"))
	repaint := awaitFrame(t, sends, wire.MsgOutput)
	output, err := wire.UnmarshalOutput(repaint.Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "couldn't create session: shell failed to start")

	history := d.notices.history()
	require.Len(t, history, 1, "the prompt boundary must report the failure once")
	notice := history[0]
	require.Equal(t, domain.NoticeSessionSpawn, notice.Code)
	require.Equal(t, "couldn't create session: shell failed to start", notice.Message)
	require.NotContains(t, notice.Message, cause.Error())
	require.Contains(t, notice.Details, cause.Error())
	require.True(t, ac.overlays.promptActive(), "failed submission stays inline")
}

func TestPromptSubmitValidationErrorStaysInlineOnly(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr error
		setup   func(*Daemon)
	}{
		{name: "invalid name", input: []byte("invalid name\r"), wantErr: domain.ErrInvalidSessionName},
		{name: "required name", input: []byte("\r"), wantErr: errSessionNameRequired},
		{
			name:    "name in use",
			input:   []byte("taken\r"),
			wantErr: errSessionNameInUse,
			setup: func(d *Daemon) {
				d.sessions["taken"] = &session{sessionCore: sessionCore{id: "taken", name: "taken"}}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, release := newBlockingPTY(t)
			defer release()
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)
			if tt.setup != nil {
				tt.setup(d)
			}

			d.enterPrompt(sess, ac, " Create session ", "", func(name string) error {
				return d.createSessionAndSwitch(sess, ac, name)
			})
			awaitFrame(t, sends, wire.MsgOutput)
			d.handlePromptInput(ac, tt.input)
			repaint := awaitFrame(t, sends, wire.MsgOutput)
			output, err := wire.UnmarshalOutput(repaint.Payload)
			require.NoError(t, err)
			require.Contains(t, string(output.Data), tt.wantErr.Error())

			require.True(t, ac.overlays.promptActive())
			require.Empty(t, d.notices.history())
		})
	}
}

func TestPromptEscapeCancelsWithoutRename(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer release()
	oldName := sess.name

	d.enterPrompt(sess, ac, " Rename session ", oldName, func(name string) error { return d.renameSession(sess, name) })
	awaitFrame(t, sends, wire.MsgOutput)
	d.handlePromptInput(ac, []byte("new\x1b"))
	awaitFrame(t, sends, wire.MsgOutput)

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
	awaitFrame(t, sends, wire.MsgOutput)
	d.handlePaletteInput(ac, []byte("RNS\r"))
	out := awaitFrame(t, sends, wire.MsgOutput)

	require.False(t, ac.overlays.paletteActive())
	require.True(t, ac.overlays.promptActive())
	msg, err := wire.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "Rename session")
	require.Contains(t, string(msg.Data), "> 0")
}
