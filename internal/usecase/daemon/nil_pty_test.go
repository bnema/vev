package daemon

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestNilPTYLifecyclePathsDoNotPanic(t *testing.T) {
	d := New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{sessionCore: sessionCore{id: "s", name: "s"}, ctx: ctx, cancel: cancel}
	d.sessions[sess.id] = sess
	first := newTab(nil, domain.Size{Cols: 41, Rows: 10})
	second := newTab(nil, domain.Size{Cols: 41, Rows: 10})
	sess.tabs = []*tab{first, second}

	first.mu.Lock()
	first.size = domain.Size{Cols: 20, Rows: 5}
	first.bumpLayoutGenerationLocked()
	first.mu.Unlock()
	require.NotPanics(t, func() {
		sess.geometry.applyTabLayout(d, sess, first)
	})
	require.Equal(t, 20, first.focusedPane().screen.Frame.Width)
	require.Equal(t, 5, first.focusedPane().screen.Frame.Height)

	first.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-2"}
	first.panes["pane-2"] = newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 10})
	require.NotPanics(t, func() {
		require.NoError(t, d.closePane(sess, first, "pane-2", nil, false))
	})
	require.NotContains(t, first.panes, layout.PaneID("pane-2"))

	require.NotPanics(t, func() { require.NoError(t, d.closeTab(sess, second, false)) })
	require.Len(t, sess.tabs, 1)
}

func TestKillSessionIgnoresNilPanePTYs(t *testing.T) {
	d := New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{sessionCore: sessionCore{id: "s", name: "s", ephemeral: true}, ctx: ctx, cancel: cancel, tabs: []*tab{newTab(nil, domain.Size{Cols: 10, Rows: 3})}}
	d.sessions[sess.id] = sess

	require.NotPanics(t, func() {
		require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, true))
	})
	require.NotContains(t, d.sessions, sess.id)
}

func TestNilPTYInputPathsDoNotPanic(t *testing.T) {
	d := New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tb := newTab(nil, domain.Size{Cols: 10, Rows: 3})
	sess := &session{sessionCore: sessionCore{id: "s", name: "s"}, ctx: ctx, cancel: cancel, tabs: []*tab{tb}}
	ac := &attachedClient{}
	ac.initOverlays()
	ac.setSession(sess)

	require.NotPanics(t, func() { d.writeToPane(sess, tb.focusedPane(), []byte("x")) })
	require.NotPanics(t, func() { daemonKeyHandler{d: d, ac: ac}.Forward([]byte("x")) })
}
