package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

var (
	_ domain.TabStableID  = moveSourceLocator{}.TabID
	_ domain.PaneStableID = moveSourceLocator{}.PaneID
)

// --- test doubles -----------------------------------------------------------

type refusingSnapshotDeleteRepository struct {
	noOpSnapshotRepository
	err error
}

func (refusingSnapshotDeleteRepository) Publish(context.Context, ports.SnapshotPublication) error {
	return nil
}
func (s refusingSnapshotDeleteRepository) DeleteIncarnation(context.Context, domain.IncarnationID) error {
	return s.err
}

func newTestTabWithContext(p ports.PTY, ctx context.Context, cancel context.CancelFunc) *tab {
	tb := newTab(p, domain.Size{Cols: 80, Rows: 23})
	tb.ctx, tb.cancel = ctx, cancel
	for _, pane := range tb.panes {
		pane.ctx, pane.cancel = ctx, cancel
	}
	return tb
}

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestAltTForwardsToPTY(t *testing.T) {
	writes := make(chan []byte, 2)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()

	d.handleInput(sess, ac, []byte("\x1bt"))

	require.False(t, ac.overlays.pickerActive())
	require.Equal(t, []byte("\x1bt"), <-writes)
}

func TestPickerViewsAddsBellSuffixForAttention(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	ctx, cancel := context.WithCancel(d.serveCtx)
	defer cancel()
	current := &session{sessionCore: sessionCore{id: "s1", name: "alpha"}, ctx: ctx, cancel: cancel, tabs: []*tab{{}, {}}}
	ringing := &session{sessionCore: sessionCore{id: "s2", name: "beta"}, ctx: ctx, cancel: cancel, tabs: []*tab{{name: "shell"}, {name: "logs"}}}
	ringing.mu.Lock()
	ringing.tabs[1].attention = true
	ringing.tabs[1].attentionAt = time.Unix(10, 0)
	ringing.mu.Unlock()
	d.sessions[current.id] = current
	d.sessions[ringing.id] = ringing

	views, currentSelection := d.pickerViews(current, nil)

	require.Equal(t, picker.SourceFilter{Session: current.id}, currentSelection)
	require.Len(t, views, 2)
	require.Equal(t, "alpha", views[0].Name)
	require.Equal(t, []picker.TabEntry{{Name: "1"}, {Name: "2"}}, views[0].Tabs)
	require.Equal(t, "beta ", views[1].Name)
	require.Equal(t, []picker.TabEntry{{Name: "shell"}, {Name: "logs", Attention: true}}, views[1].Tabs)

	model := picker.New(views, picker.SelectionConfig{Mode: picker.SelectNavigationTab, Current: picker.SourceFilter{Session: current.id, TabID: views[0].Tabs[0].TabID}})
	model.Down()
	model.Down()
	target, ok := model.Selected()
	require.True(t, ok)
	require.Equal(t, "beta", target.Name)
	require.NotNil(t, target.ExpectedCreatedAt)
}

func TestPickerViewsIncludesEphemeralSessions(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	current := &session{sessionCore: sessionCore{id: "current", name: "current"}, tabs: []*tab{{}}}
	ephemeral := &session{sessionCore: sessionCore{id: "ephemeral", name: "1", ephemeral: true}, tabs: []*tab{{}}}
	d.sessions[current.id] = current
	d.sessions[ephemeral.id] = ephemeral

	views, _ := d.pickerViews(current, nil)

	require.Len(t, views, 2)
	var ephemeralView picker.SessionView
	for _, view := range views {
		if view.ID == ephemeral.id {
			ephemeralView = view
			break
		}
	}
	require.Equal(t, ephemeral.id, ephemeralView.ID)
	require.Equal(t, "1", ephemeralView.Name)
}

func TestPickerViewsOrdersByMRUWithEphemeralInterleaved(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	old := &session{sessionCore: sessionCore{id: "old", name: "alpha"}, tabs: []*tab{{}}}
	old.mruAt.Store(1)
	eph := &session{sessionCore: sessionCore{id: "eph", name: "1", ephemeral: true}, tabs: []*tab{{}}}
	eph.mruAt.Store(3)
	recent := &session{sessionCore: sessionCore{id: "recent", name: "zeta"}, tabs: []*tab{{}}}
	recent.mruAt.Store(2)
	d.sessions[old.id] = old
	d.sessions[eph.id] = eph
	d.sessions[recent.id] = recent
	d.stopped["halted-old"] = stoppedSession{name: "halted-old", createdAt: 10, lastUsedSeq: 1}
	d.stopped["halted-new"] = stoppedSession{name: "halted-new", createdAt: 11, lastUsedSeq: 2}

	views, _ := d.pickerViews(recent, nil)

	names := make([]string, 0, len(views))
	for _, v := range views {
		names = append(names, v.Name)
	}
	// Live MRU-desc (ephemeral "1" is most recent), then stopped block MRU-desc.
	require.Equal(t, []string{"1", "zeta", "alpha", "halted-new", "halted-old"}, names)
	require.True(t, views[3].Stopped)
	require.True(t, views[4].Stopped)
}

func TestPickerViewsMRUTieBreaksAlphabetically(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	b := &session{sessionCore: sessionCore{id: "b", name: "bravo"}, tabs: []*tab{{}}}
	a := &session{sessionCore: sessionCore{id: "a", name: "alpha"}, tabs: []*tab{{}}}
	// Equal mruAt (both zero) must yield a stable alphabetical order.
	d.sessions[b.id] = b
	d.sessions[a.id] = a

	views, _ := d.pickerViews(a, nil)

	require.Equal(t, "alpha", views[0].Name)
	require.Equal(t, "bravo", views[1].Name)
}

func TestPickerViewsGroupedModePutsNamedBeforeEphemeral(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.pickerSort.Store(uint32(pickerSortGrouped))
	eph := &session{sessionCore: sessionCore{id: "eph", name: "1", ephemeral: true}, tabs: []*tab{{}}}
	eph.mruAt.Store(5)
	named := &session{sessionCore: sessionCore{id: "named", name: "work"}, tabs: []*tab{{}}}
	named.mruAt.Store(2)
	named2 := &session{sessionCore: sessionCore{id: "named2", name: "notes"}, tabs: []*tab{{}}}
	named2.mruAt.Store(3)
	d.sessions[eph.id] = eph
	d.sessions[named.id] = named
	d.sessions[named2.id] = named2
	d.stopped["halted"] = stoppedSession{name: "halted", createdAt: 9, lastUsedSeq: 9}

	views, _ := d.pickerViews(named, nil)

	names := make([]string, 0, len(views))
	for _, v := range views {
		names = append(names, v.Name)
	}
	// Named (MRU-desc) → ephemeral (MRU-desc) → stopped, even though the
	// ephemeral session has the highest mruAt.
	require.Equal(t, []string{"notes", "work", "1", "halted"}, names)
}

func TestPickerSortToggleFlipsModeAndKeepsSelection(t *testing.T) {
	// Rows in recent mode are: ephemeral "1" (header + tab), "work" (header +
	// two tabs), then the stopped block. Navigation starts on "work"'s active
	// tab, so "j" walks down from there.
	cases := []struct {
		name        string
		navigate    []byte
		wantStopped bool
	}{
		{name: "live tab selection", navigate: []byte("j")},
		{name: "stopped session selection", navigate: []byte("jj"), wantStopped: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, sends, releases := newManualTabSession(t, 2)
			defer releaseAll(releases)
			sess.mu.Lock()
			sess.registerAttachmentLocked(ac)
			secondTabID := domain.TabStableID(sess.tabs[1].stableID)
			sess.mu.Unlock()
			sess.mruAt.Store(1)
			// A second live session, ephemeral and more recently used: recent mode
			// lists it first, grouped mode moves it below the named session.
			eph := &session{sessionCore: sessionCore{id: "eph", name: "1", ephemeral: true}, tabs: []*tab{{}}}
			eph.mruAt.Store(5)
			d.mu.Lock()
			d.sessions[eph.id] = eph
			d.stopped["halted"] = stoppedSession{name: "halted", createdAt: 9, lastUsedSeq: 9}
			d.mu.Unlock()

			d.enterPicker(sess, ac)
			awaitFrame(t, sends, ports.MsgOutput)
			// Move off the attached session's active tab: a navigate rebuild snaps
			// back to it unless the toggle preserves the selection.
			d.handleInput(sess, ac, tc.navigate)
			awaitFrame(t, sends, ports.MsgOutput)
			ac.overlays.pickerMu.Lock()
			before, ok := ac.overlays.picker.Selected()
			ac.overlays.pickerMu.Unlock()
			require.True(t, ok)
			require.Equal(t, tc.wantStopped, before.Stopped)
			if tc.wantStopped {
				require.Equal(t, "halted", before.Name)
			} else {
				require.Equal(t, secondTabID, before.TabID)
			}

			d.handleInput(sess, ac, []byte("s"))
			awaitFrame(t, sends, ports.MsgOutput)

			require.Equal(t, uint32(pickerSortGrouped), d.pickerSort.Load())
			require.True(t, ac.overlays.pickerActive())
			ac.overlays.pickerMu.Lock()
			selected, ok := ac.overlays.picker.Selected()
			title := ac.overlays.pickerTitle
			ac.overlays.pickerMu.Unlock()
			require.True(t, ok)
			require.Equal(t, before, selected)
			require.Equal(t, " Sessions · grouped ", title)

			d.handleInput(sess, ac, []byte("s"))
			awaitFrame(t, sends, ports.MsgOutput)

			require.Equal(t, uint32(pickerSortRecent), d.pickerSort.Load())
			require.True(t, ac.overlays.pickerActive())
			ac.overlays.pickerMu.Lock()
			selected, ok = ac.overlays.picker.Selected()
			title = ac.overlays.pickerTitle
			ac.overlays.pickerMu.Unlock()
			require.True(t, ok)
			require.Equal(t, before, selected)
			require.Equal(t, " Sessions · recent ", title)
		})
	}
}

func TestPickerTitleReflectsSortMode(t *testing.T) {
	tests := []struct {
		name string
		mode pickerSortMode
		want string
	}{
		{name: "recent", mode: pickerSortRecent, want: " Sessions · recent "},
		{name: "grouped", mode: pickerSortGrouped, want: " Sessions · grouped "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, pickerTitle(tc.mode))
		})
	}
}

func TestPickerViewsCarryNamedLifecycleIdentity(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	ctx, cancel := context.WithCancel(d.serveCtx)
	defer cancel()
	active := &session{sessionCore: sessionCore{id: "active", name: "active", createdAt: 23}, ctx: ctx, cancel: cancel, tabs: []*tab{{}}}
	d.sessions[active.id] = active
	d.stopped["stopped"] = stoppedSession{name: "stopped", createdAt: 24}

	views, _ := d.pickerViews(active, nil)

	require.Len(t, views, 2)
	require.NotNil(t, views[0].ExpectedCreatedAt)
	require.Equal(t, int64(23), *views[0].ExpectedCreatedAt)
	require.NotNil(t, views[1].ExpectedCreatedAt)
	require.Equal(t, int64(24), *views[1].ExpectedCreatedAt)
}

func TestPickerCanonicalViewsLeaveMoveFilteringToModel(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	source := &session{sessionCore: sessionCore{id: "source", name: "source", incarnation: domain.IncarnationID{1}},
		tabs: []*tab{
			{stableID: "source-tab", panes: map[layout.PaneID]*pane{}},
			{stableID: "sibling-tab", panes: map[layout.PaneID]*pane{}},
		},
	}
	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", incarnation: domain.IncarnationID{2}, ephemeral: true}, tabs: []*tab{{stableID: "destination-tab", panes: map[layout.PaneID]*pane{}}}}
	d.sessions[source.id] = source
	d.sessions[destination.id] = destination
	d.stopped["stopped"] = stoppedSession{name: "stopped", createdAt: 9, incarnation: domain.IncarnationID{9}}
	sourceLocator := moveSourceLocator{
		Session: moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name},
		TabID:   "source-tab",
		PaneID:  "source-pane",
	}

	views, current := d.pickerViews(source, nil)
	require.Len(t, views, 3, "canonical capture includes active and stopped lifecycles")
	require.Equal(t, picker.SourceFilter{Session: source.id, Incarnation: source.incarnation, TabID: "source-tab"}, current)
	var sourceView picker.SessionView
	for _, view := range views {
		if view.ID == source.id {
			sourceView = view
		}
	}
	require.Equal(t, []picker.TabEntry{{TabID: "source-tab", Name: "1"}, {TabID: "sibling-tab", Name: "2"}}, sourceView.Tabs,
		"daemon capture does not remove the move source")

	paneModel := d.newPickerModel(source, nil, pickerMovePane, sourceLocator, picker.SourceFilter{
		Session: source.id, Incarnation: source.incarnation, TabID: "sibling-tab",
	})
	paneTarget, ok := paneModel.Selected()
	require.True(t, ok)
	require.Equal(t, domain.TabStableID("sibling-tab"), paneTarget.TabID)

	tabModel := d.newPickerModel(source, nil, pickerMoveTab, sourceLocator, picker.SourceFilter{})
	tabTarget, ok := tabModel.Selected()
	require.True(t, ok)
	require.Equal(t, destination.id, tabTarget.Session)
	require.Equal(t, destination.incarnation, tabTarget.Incarnation)
	require.Equal(t, destination.name, tabTarget.Name)
	require.Equal(t, -1, tabTarget.TabIndex)
}

func TestPickerMoveWithoutDestinationDoesNotPublishOverlay(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 1)
	defer releases[0]()
	sess.mu.Lock()
	sess.incarnation = domain.IncarnationID{1}
	tb := sess.tabs[0]
	sess.mu.Unlock()
	tb.mu.Lock()
	tabID := tb.stableID
	paneID := tb.focusedPane().stableID
	tb.mu.Unlock()
	source := moveSourceLocator{
		Session: moveSessionLocator{ID: sess.id, Incarnation: sess.incarnation, Name: sess.name},
		TabID:   domain.TabStableID(tabID), PaneID: domain.PaneStableID(paneID),
	}

	for _, intent := range []pickerIntent{pickerMovePane, pickerMoveTab} {
		err := d.enterPickerForIntent(sess, ac, intent, source)

		require.ErrorIs(t, err, errNoMoveDestination)
		require.False(t, ac.overlays.pickerActive())
		ac.overlays.pickerMu.Lock()
		require.Nil(t, ac.overlays.pickerPreview)
		require.Nil(t, ac.overlays.pickerPreviewSession)
		require.Zero(t, ac.overlays.pickerPreviewGeneration, "failed entry must not publish or retire a picker generation")
		ac.overlays.pickerMu.Unlock()
	}
}

func TestPickerMovePaneStoresSourceAndCleansPreviewSubscription(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	defer release1()
	defer release2()
	d, sourceSession, ac, _ := newManualSessionWithPTYs(t, p1)
	sourceSession.id, sourceSession.name, sourceSession.incarnation = "source", "z-source", domain.IncarnationID{1}
	delete(d.sessions, domain.SessionID("manual"))
	d.sessions[sourceSession.id] = sourceSession
	targetTab := newTestTabWithContext(p2, sourceSession.ctx, sourceSession.cancel)
	targetTab.stableID = "destination-tab"
	targetSession := &session{sessionCore: sessionCore{id: "destination", name: "a-destination", incarnation: domain.IncarnationID{2}, ephemeral: true}, ctx: sourceSession.ctx, cancel: func() {}, tabs: []*tab{targetTab}}
	d.sessions[targetSession.id] = targetSession
	sourceTab := sourceSession.tabs[0]
	sourceTab.mu.Lock()
	sourceTab.stableID = "source-tab"
	sourcePaneID := sourceTab.focusedPane().stableID
	sourceTab.mu.Unlock()
	source := moveSourceLocator{
		Session: moveSessionLocator{ID: sourceSession.id, Incarnation: sourceSession.incarnation, Name: sourceSession.name},
		TabID:   "source-tab", PaneID: domain.PaneStableID(sourcePaneID),
	}

	require.NoError(t, d.enterPickerForIntent(sourceSession, ac, pickerMovePane, source))
	ac.overlays.pickerMu.Lock()
	storedIntent, storedSource := ac.overlays.pickerIntent, ac.overlays.pickerSource
	selected, ok := ac.overlays.picker.Selected()
	generation := ac.overlays.pickerPreviewGeneration
	require.Same(t, targetTab, ac.overlays.pickerPreview)
	require.Same(t, targetSession, ac.overlays.pickerPreviewSession)
	ac.overlays.pickerMu.Unlock()
	require.Equal(t, pickerMovePane, storedIntent)
	require.Equal(t, source, storedSource)
	require.True(t, ok)
	require.Equal(t, targetSession.id, selected.Session)
	require.Equal(t, targetSession.incarnation, selected.Incarnation)
	require.Equal(t, targetSession.name, selected.Name)
	require.Equal(t, domain.TabStableID(targetTab.stableID), selected.TabID)
	require.Equal(t, 0, selected.TabIndex)
	require.Nil(t, selected.ExpectedCreatedAt)
	rc := targetSession.renderCoordinator()
	require.NotNil(t, rc)
	rc.mu.Lock()
	subscription, subscribed := rc.previewWakes[ac]
	rc.mu.Unlock()
	require.True(t, subscribed)
	require.Equal(t, generation, subscription.generation)

	d.enterPicker(sourceSession, ac)

	rc.mu.Lock()
	_, subscribed = rc.previewWakes[ac]
	rc.mu.Unlock()
	require.False(t, subscribed, "picker replacement must retire the prior cross-session preview subscription")
	ac.overlays.pickerMu.Lock()
	require.NotNil(t, ac.overlays.picker)
	require.Equal(t, pickerNavigate, ac.overlays.pickerIntent)
	require.Equal(t, moveSourceLocator{}, ac.overlays.pickerSource)
	ac.overlays.pickerMu.Unlock()

	d.closePicker(ac)
	ac.overlays.pickerMu.Lock()
	require.Nil(t, ac.overlays.picker)
	require.Nil(t, ac.overlays.pickerPreview)
	ac.overlays.pickerMu.Unlock()
}

func TestPickerMoveTabPreviewsDestinationActiveTabWithoutActivatingIt(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	p3, release3 := newBlockingPTY(t)
	defer release1()
	defer release2()
	defer release3()
	d, sourceSession, ac, _ := newManualSessionWithPTYs(t, p1)
	sourceSession.id, sourceSession.name, sourceSession.incarnation = "source", "source", domain.IncarnationID{1}
	delete(d.sessions, domain.SessionID("manual"))
	d.sessions[sourceSession.id] = sourceSession
	target := &session{sessionCore: sessionCore{id: "destination", name: "destination", incarnation: domain.IncarnationID{2}, ephemeral: true},
		ctx: sourceSession.ctx, cancel: func() {},
		tabs: []*tab{
			newTestTabWithContext(p2, sourceSession.ctx, sourceSession.cancel),
			newTestTabWithContext(p3, sourceSession.ctx, sourceSession.cancel),
		},
	}
	target.tabs[0].stableID, target.tabs[1].stableID = "first", "active"
	d.sessions[target.id] = target
	source := moveSourceLocator{
		Session: moveSessionLocator{ID: sourceSession.id, Incarnation: sourceSession.incarnation, Name: sourceSession.name},
		TabID:   domain.TabStableID(sourceSession.tabs[0].stableID),
	}

	require.NoError(t, d.enterPickerForIntent(sourceSession, ac, pickerMoveTab, source))

	ac.overlays.pickerMu.Lock()
	selected, ok := ac.overlays.picker.Selected()
	require.Same(t, target.tabs[0], ac.overlays.pickerPreview)
	ac.overlays.pickerMu.Unlock()
	require.True(t, ok)
	require.Equal(t, target.id, selected.Session)
	require.Equal(t, target.incarnation, selected.Incarnation)
	require.Equal(t, target.name, selected.Name)
	require.Equal(t, -1, selected.TabIndex)
	require.Nil(t, selected.ExpectedCreatedAt)
	require.Equal(t, 0, testAttachmentTabIndex(target), "preview must not mutate destination view")
	d.closePicker(ac)
}

func TestPickerMoveRefreshPreservesSelectedDestination(t *testing.T) {
	t.Run("pane destination stable tab", func(t *testing.T) {
		d, sourceSession, ac, _, sourceReleases := newManualTabSession(t, 1)
		defer releaseAll(sourceReleases)
		sourceSession.id, sourceSession.name, sourceSession.incarnation = "source", "source", domain.IncarnationID{1}
		sourceSession.tabs[0].stableID = "source-tab"
		delete(d.sessions, domain.SessionID("manual"))
		d.sessions[sourceSession.id] = sourceSession

		firstPTY, releaseFirst := newBlockingPTY(t)
		defer releaseFirst()
		secondPTY, releaseSecond := newBlockingPTY(t)
		defer releaseSecond()
		firstTab := newTestTabWithContext(firstPTY, sourceSession.ctx, sourceSession.cancel)
		secondTab := newTestTabWithContext(secondPTY, sourceSession.ctx, sourceSession.cancel)
		firstTab.stableID, secondTab.stableID = "first", "selected"
		destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", incarnation: domain.IncarnationID{2}, ephemeral: true}, ctx: sourceSession.ctx, cancel: func() {}, tabs: []*tab{firstTab, secondTab}}
		d.sessions[destination.id] = destination
		source := moveSourceLocator{
			Session: moveSessionLocator{ID: sourceSession.id, Incarnation: sourceSession.incarnation, Name: sourceSession.name},
			TabID:   domain.TabStableID(sourceSession.tabs[0].stableID),
		}

		require.NoError(t, d.enterPickerForIntent(sourceSession, ac, pickerMovePane, source))
		ac.overlays.pickerMu.Lock()
		ac.overlays.picker.Down()
		before, ok := ac.overlays.picker.Selected()
		ac.overlays.pickerMu.Unlock()
		require.True(t, ok)
		require.Equal(t, domain.TabStableID("selected"), before.TabID)

		destination.mu.Lock()
		destination.tabs[0], destination.tabs[1] = destination.tabs[1], destination.tabs[0]
		destination.mu.Unlock()
		d.refreshPickerOpts(ac, pickerRefreshOptions{nearestRow: -1})

		ac.overlays.pickerMu.Lock()
		after, ok := ac.overlays.picker.Selected()
		ac.overlays.pickerMu.Unlock()
		require.True(t, ok)
		require.Equal(t, before.Session, after.Session)
		require.Equal(t, before.Incarnation, after.Incarnation)
		require.Equal(t, before.TabID, after.TabID)
		require.Equal(t, 0, after.TabIndex, "mutable index follows the preserved stable tab")
		d.closePicker(ac)
	})

	t.Run("tab destination session", func(t *testing.T) {
		d, sourceSession, ac, _, sourceReleases := newManualTabSession(t, 1)
		defer releaseAll(sourceReleases)
		sourceSession.id, sourceSession.name, sourceSession.incarnation = "source", "source", domain.IncarnationID{1}
		sourceSession.tabs[0].stableID = "source-tab"
		delete(d.sessions, domain.SessionID("manual"))
		d.sessions[sourceSession.id] = sourceSession

		releases := make([]func(), 0, 2)
		for i, destination := range []struct {
			id   domain.SessionID
			name string
		}{{id: "first", name: "0-first"}, {id: "selected", name: "1-selected"}} {
			pty, release := newBlockingPTY(t)
			releases = append(releases, release)
			tb := newTestTabWithContext(pty, sourceSession.ctx, sourceSession.cancel)
			tb.stableID = string(destination.id) + "-tab"
			d.sessions[destination.id] = &session{sessionCore: sessionCore{id: destination.id, name: destination.name, incarnation: domain.IncarnationID{byte(i + 2)}, ephemeral: true}, ctx: sourceSession.ctx, cancel: func() {}, tabs: []*tab{tb}}
		}
		defer releaseAll(releases)
		source := moveSourceLocator{
			Session: moveSessionLocator{ID: sourceSession.id, Incarnation: sourceSession.incarnation, Name: sourceSession.name},
			TabID:   domain.TabStableID(sourceSession.tabs[0].stableID),
		}

		require.NoError(t, d.enterPickerForIntent(sourceSession, ac, pickerMoveTab, source))
		ac.overlays.pickerMu.Lock()
		ac.overlays.picker.Down()
		before, ok := ac.overlays.picker.Selected()
		ac.overlays.pickerMu.Unlock()
		require.True(t, ok)
		require.Equal(t, domain.SessionID("selected"), before.Session)

		d.refreshPickerOpts(ac, pickerRefreshOptions{nearestRow: -1})

		ac.overlays.pickerMu.Lock()
		after, ok := ac.overlays.picker.Selected()
		ac.overlays.pickerMu.Unlock()
		require.True(t, ok)
		require.Equal(t, before, after)
		d.closePicker(ac)
	})
}

func TestPickerRejectsRecreatedEphemeralTargetFromStaleSelection(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, current, ac, _ := newManualSessionWithPTYs(t, p)
	ctx, cancel := context.WithCancel(d.serveCtx)
	defer cancel()
	original := &session{sessionCore: sessionCore{id: "ephemeral-1", name: "2", ephemeral: true}, ctx: ctx, cancel: cancel, tabs: []*tab{{}}}
	d.sessions[original.id] = original

	views, _ := d.pickerViews(current, nil)
	var originalView picker.SessionView
	for _, view := range views {
		if view.ID == original.id {
			originalView = view
			break
		}
	}
	model := picker.New([]picker.SessionView{originalView}, picker.SelectionConfig{Mode: picker.SelectNavigationTab, Current: picker.SourceFilter{Session: original.id, TabID: originalView.Tabs[0].TabID}})
	target, ok := model.Selected()
	require.True(t, ok)

	delete(d.sessions, original.id)
	replacement := &session{sessionCore: sessionCore{id: "ephemeral-2", name: original.name, ephemeral: true}, ctx: ctx, cancel: cancel, tabs: []*tab{{}}}
	d.sessions[replacement.id] = replacement

	require.Error(t, d.switchToTarget(current, ac, target))
	require.Same(t, current, ac.currentSession())
}

func TestPickerViewsComposesFocusedPaneTitleWithAttentionSuffix(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	d.shell = "/bin/sh"
	sess.tabs[1].name = "logs"

	pane0 := sess.tabs[0].focusedPane()
	pane0.mu.Lock()
	pane0.title.processName = "vim"
	pane0.title.processNameValid = true
	pane0.mu.Unlock()

	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.mu.Unlock()

	views, _ := d.pickerViews(sess, nil)

	require.Len(t, views, 1)
	require.Equal(t, []picker.TabEntry{
		{TabID: domain.TabStableID(sess.tabs[0].stableID), Name: "1", Detail: " (vim)"},
		{TabID: domain.TabStableID(sess.tabs[1].stableID), Name: "logs", Detail: " (sh)", Attention: true},
	}, views[0].Tabs)
}

func TestPickerViewsOmitsTerminalTitleWhenTabsConfigDisabled(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	d.shell = "/bin/sh"
	sess.tabs[1].name = "logs"

	pane0 := sess.tabs[0].focusedPane()
	pane0.mu.Lock()
	pane0.title.processName = "vim"
	pane0.title.processNameValid = true
	pane0.title.terminalTitle = "server.go — vev"
	pane0.mu.Unlock()

	d.ApplyConfig(domain.Config{Tabs: domain.TabsConfig{TerminalTitle: false}})

	views, _ := d.pickerViews(sess, nil)

	require.Len(t, views, 1)
	require.Equal(t, []picker.TabEntry{
		{TabID: domain.TabStableID(sess.tabs[0].stableID), Name: "1", Detail: " (vim)"},
		{TabID: domain.TabStableID(sess.tabs[1].stableID), Name: "logs", Detail: " (sh)"},
	}, views[0].Tabs)
}

func TestPickerWaitsForRestoringTargetBeforeSwitching(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, from, ac, _ := newManualSessionWithPTYs(t, p)
	record := durableRecoveryRecord(0)
	record.Name = "restoring"
	state, done := initialSessionState(record)
	d.stopped[record.Name] = stoppedSession{
		name:        record.Name,
		createdAt:   record.CreatedAt,
		incarnation: record.IncarnationID,
		record:      record,
		state:       state,
		restoreDone: done,
	}
	factory := &recoveryCountingPTYFactory{}
	d.ptys = factory

	result := make(chan error, 1)
	go func() {
		result <- d.switchToTarget(from, ac, picker.Target{Name: record.Name, Stopped: true})
	}()

	select {
	case err := <-result:
		t.Fatalf("picker switch returned before target restoration completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	require.Zero(t, factory.calls.Load())
	require.Same(t, from, ac.currentSession())

	ctx, cancel := context.WithCancel(d.serveCtx)
	defer cancel()
	target := &session{sessionCore: sessionCore{id: "restored", name: record.Name, createdAt: record.CreatedAt}, ctx: ctx, cancel: cancel, tabs: []*tab{newTab(nil, domain.Size{Cols: 80, Rows: 23})}}
	d.mu.Lock()
	delete(d.stopped, record.Name)
	d.sessions[target.id] = target
	closeRuntimeRestoreDoneLocked(done)
	d.mu.Unlock()

	require.NoError(t, <-result)
	require.Same(t, target, ac.currentSession())
}

func TestRestoreCancellationTransitionsBeforePickerCompletion(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, from, ac, _ := newManualSessionWithPTYs(t, p)
	record := durableRecoveryRecord(0)
	record.Name = "restoring"
	repository := &cancellationRecoveryRepository{started: make(chan struct{})}
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})
	coordinator := recoveryusecase.NewCoordinator(catalogue, repository, nil)
	WithCatalogue(catalogue, []domain.CatalogueRecord{record})(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(coordinator)(d)
	restoreCtx, cancelRestore := context.WithCancel(context.Background())
	restored := make(chan struct{})
	go func() {
		d.restoreCatalogue(restoreCtx, mustDurableRecords(t, catalogue))
		close(restored)
	}()
	<-repository.started

	result := make(chan error, 1)
	go func() {
		result <- d.switchToTarget(from, ac, picker.Target{Name: record.Name, Stopped: true})
	}()
	cancelRestore()

	require.Error(t, <-result)
	<-restored
	require.Same(t, from, ac.currentSession())
	d.mu.Lock()
	entry := d.stopped[record.Name]
	d.mu.Unlock()
	require.Equal(t, ports.SessionBroken, entry.state)
	require.Equal(t, "restore interrupted", entry.record.DegradedReason)
	select {
	case <-entry.restoreDone:
	default:
		t.Fatal("restore completion was not signaled")
	}
}

func TestPickerRejectsCatalogueTargetsWithoutFreshRuntime(t *testing.T) {
	tests := []struct {
		name         string
		broken       bool
		runtimeState ports.SessionState
	}{
		{name: "broken", broken: true, runtimeState: ports.SessionBroken},
		{name: "healthy without restored runtime", runtimeState: ports.SessionStopped},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, release := newBlockingPTY(t)
			defer release()
			d, from, ac, _ := newManualSessionWithPTYs(t, p)
			store, storeState := newMockStore(t)
			record := durableRecoveryRecord(0)
			record.Name = "unsafe"
			if tt.broken {
				record.DegradedReason = "checkpoint validation failed"
			}
			catalogue := persist.New(store)
			WithCatalogue(catalogue, []domain.CatalogueRecord{record})(d)
			d.stopped[record.Name] = stoppedSession{
				name:        record.Name,
				createdAt:   record.CreatedAt,
				incarnation: record.IncarnationID,
				record:      record,
				state:       tt.runtimeState,
			}
			factory := &recoveryCountingPTYFactory{}
			d.ptys = factory

			err := d.switchToTarget(from, ac, picker.Target{Name: record.Name, Stopped: true})

			var userError *domain.UserError
			require.ErrorAs(t, err, &userError)
			require.Equal(t, domain.NoticeSessionUnavailable, userError.Code)
			var protocolError *protoErr
			require.ErrorAs(t, err, &protocolError)
			require.Equal(t, ports.ErrInternal, protocolError.code)
			require.Zero(t, factory.calls.Load(), "unsafe target must not open a PTY")
			require.Same(t, from, ac.currentSession())

			storeState.mu.Lock()
			sets := storeState.sets
			storeState.mu.Unlock()
			require.Zero(t, sets, "unsafe target must not mutate the catalogue")
			d.mu.Lock()
			entry := d.stopped[record.Name]
			d.mu.Unlock()
			require.Equal(t, record, entry.record)
			require.Equal(t, tt.runtimeState, entry.state)
		})
	}
}

func TestPickerResumesStoppedSessionWithPersistedTabNames(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	p3, release3 := newBlockingPTY(t)
	defer release1()
	defer release2()
	defer release3()
	d, from, ac, sends := newManualSessionWithPTYs(t, p1)
	d.ptys = newFactorySeq(t, p2, p3)
	d.stopped["work"] = stoppedSession{name: "work", cwd: "/tmp/work", createdAt: 7, tabNames: []string{"shell", "logs"}, record: domain.CatalogueRecord{Name: "work"}, state: ports.SessionStopped}

	d.resumeStoppedAndSwitch(from, ac, picker.Target{Name: "work", Stopped: true})
	awaitFrame(t, sends, ports.MsgOutput)

	target := ac.currentSession()
	require.NotNil(t, target)
	require.Equal(t, "work", target.name)
	require.Len(t, target.tabs, 2)
	require.Equal(t, "shell", target.tabs[0].name)
	require.Equal(t, "logs", target.tabs[1].name)
}

func TestPickerSameSessionNavigationSwitchAndEscClose(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	sess.mu.Lock()
	sess.registerAttachmentLocked(ac)
	sess.mu.Unlock()
	d.ptys = newBlockingOpenFactory(t, d)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	d.enterPicker(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("j"))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("\r"))

	require.Equal(t, 1, testAttachmentTabIndex(sess))
	requireFloatingInitialized(t, testAttachmentTab(sess))
	awaitFrame(t, sends, ports.MsgOutput)
	d.enterPicker(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	require.True(t, ac.overlays.pickerActive())
	timer.ch <- time.Now()
	require.Eventually(t, func() bool { return !ac.overlays.pickerActive() }, time.Second, 5*time.Millisecond)
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPickerSplitArrowNavigatesWithoutExiting(t *testing.T) {
	cases := []struct {
		name       string
		input      [][]byte
		wantActive int
	}{
		{name: "escape then down arrow", input: [][]byte{[]byte("\x1b"), []byte("[B")}, wantActive: 1},
		{name: "escape then up arrow", input: [][]byte{[]byte("j"), []byte("\x1b"), []byte("[A")}, wantActive: 0},
		{name: "split down arrow", input: [][]byte{[]byte("\x1b["), []byte("B")}, wantActive: 1},
		{name: "split SS3 down arrow", input: [][]byte{[]byte("\x1bO"), []byte("B")}, wantActive: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, sends, releases := newManualTabSession(t, 2)
			defer func() {
				for _, release := range releases {
					release()
				}
			}()

			d.enterPicker(sess, ac)
			awaitFrame(t, sends, ports.MsgOutput)
			for _, input := range tc.input {
				d.handleInput(sess, ac, input)
			}
			require.True(t, ac.overlays.pickerActive())
			d.handleInput(sess, ac, []byte("\r"))
			require.Equal(t, tc.wantActive, testAttachmentTabIndex(sess))
		})
	}
}

func TestPickerLoneEscapeExitsAfterDelay(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.enterPicker(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	require.True(t, ac.overlays.pickerActive())
	timer.ch <- time.Now()
	require.Eventually(t, func() bool { return !ac.overlays.pickerActive() }, time.Second, 5*time.Millisecond)
}

func TestBackSessionFirstResetDoesNotReuseSamePaneIDCapture(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d, source, ac, sends := newManualSessionWithPTYs(t, p1)
	source.id, source.name = "source", "source"
	delete(d.sessions, domain.SessionID("manual"))
	d.sessions[source.id] = source
	target := &session{sessionCore: sessionCore{id: "target", name: "target"}, ctx: source.ctx, cancel: func() {},
		tabs: []*tab{newTab(p2, domain.Size{Cols: 80, Rows: 22})},
	}
	d.sessions[target.id] = target

	sourcePane := source.tabs[0].focusedPane()
	targetPane := target.tabs[0].focusedPane()
	require.Equal(t, layout.PaneID("pane-1"), sourcePane.id, "source uses reusable pane-1")
	require.Equal(t, sourcePane.id, targetPane.id, "target deliberately reuses pane-1")
	sourcePane.screen.Write([]byte("SOURCE"))
	client := vt.NewScreen(80, 25)
	d.firstPaint(source, ac, ac.size)
	sourceOutput := mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	ac.ackOutputState(sourceOutput.New)

	targetPane.screen.Write([]byte("TARGET"))
	targetPane.screen.ClearDamage() // TARGET is already rendered and has no pending VT damage.
	require.Empty(t, targetPane.screen.Damage(), "target deliberately has no pending VT damage")
	ac.previousSession.Set(target)

	// Exercise the user-facing previous-session route, which delegates to the
	// real switchToTarget hand-off and immediately emits its required reset.
	d.backSession(source, ac)
	firstReset := awaitFrame(t, sends, ports.MsgOutput)
	out := mustApplyOutput(t, client, firstReset)
	require.Zero(t, out.Base, "the first target frame must be the reset, not an eventual repair")
	frame := strings.Join(frameRows(client.Frame), "\n")
	require.NotContains(t, frame, "SOURCE", "first target reset must not reuse source capture")
	require.Contains(t, frame, "TARGET", "first target reset must immediately show clean target VT state")
}

func TestPickerStalePaintAfterSessionSwitchSendsNoFrame(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d := newTestDaemon(t, nil, stubClock{})
	tr1, sends1 := newCapturingTransport(t)
	tr2, _ := newCapturingTransport(t)
	ac1 := &attachedClient{tr: tr1, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac1.initOverlays()
	ac2 := &attachedClient{tr: tr2, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac2.initOverlays()
	sctx1, cancel1 := context.WithCancel(d.serveCtx)
	sctx2, cancel2 := context.WithCancel(d.serveCtx)
	defer cancel1()
	defer cancel2()
	sess1 := &session{sessionCore: sessionCore{id: "s1", name: "alpha", ephemeral: true, attachments: map[*attachedClient]struct{}{ac1: {}}}, ctx: sctx1, cancel: cancel1, tabs: []*tab{newTestTabWithContext(p1, sctx1, cancel1)}}
	sess2 := &session{sessionCore: sessionCore{id: "s2", name: "beta", attachments: map[*attachedClient]struct{}{ac2: {}}}, ctx: sctx2, cancel: cancel2, tabs: []*tab{newTestTabWithContext(p2, sctx2, cancel2)}}
	ac1.setSession(sess1)
	ac2.setSession(sess2)
	d.sessions[sess1.id] = sess1
	d.sessions[sess2.id] = sess2

	d.firstPaint(sess1, ac1, ac1.size)
	awaitFrame(t, sends1, ports.MsgOutput)
	d.stealClientForTarget(sess1, ac1, sess2, picker.Target{Session: sess2.id})
	d.firstPaint(sess2, ac1, ac1.size)
	awaitFrame(t, sends1, ports.MsgOutput)
	require.Same(t, sess2, ac1.currentSession())
	for len(sends1) > 0 {
		<-sends1
	}
	oldPane := sess1.tabs[0].focusedPane()
	oldPane.screen.Write([]byte("stale-damage"))
	require.NotEmpty(t, oldPane.screen.Damage())

	d.paint(sess1, ac1, false, nil)

	require.Zero(t, len(sends1), "stale paint from old session sent a frame")
	require.NotEmpty(t, oldPane.screen.Damage(), "stale paint from old session consumed damage")
}

func TestPickerSessionSwitchPublishesBeforeInFlightPaintSendCompletes(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d := newTestDaemon(t, nil, stubClock{})
	enteredSend := make(chan struct{})
	releaseSend := make(chan struct{})
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(ports.Frame) error {
		close(enteredSend)
		<-releaseSend
		return nil
	}).Once()
	tr.EXPECT().Close().Return(nil).Maybe()
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	handoffAtSendMu := make(chan struct{})
	ac.renderStages = renderStageHooks{
		handoffRebase: func() { close(handoffAtSendMu) },
	}
	sctx1, cancel1 := context.WithCancel(d.serveCtx)
	sctx2, cancel2 := context.WithCancel(d.serveCtx)
	defer cancel1()
	defer cancel2()
	sess1 := &session{sessionCore: sessionCore{id: "s1", name: "alpha", attachments: map[*attachedClient]struct{}{ac: {}}}, ctx: sctx1, cancel: cancel1, tabs: []*tab{newTestTabWithContext(p1, sctx1, cancel1)}}
	sess2 := &session{sessionCore: sessionCore{id: "s2", name: "beta"}, ctx: sctx2, cancel: cancel2, tabs: []*tab{newTestTabWithContext(p2, sctx2, cancel2)}}
	ac.setSession(sess1)
	d.sessions[sess1.id] = sess1
	d.sessions[sess2.id] = sess2

	sess1.tabs[0].focusedPane().screen.Write([]byte("paint while switching"))
	paintDone := make(chan struct{})
	go func() {
		d.paint(sess1, ac, false, nil)
		close(paintDone)
	}()
	<-enteredSend

	switchDone := make(chan attachmentTransitionResult, 1)
	switchErr := make(chan error, 1)
	go func() {
		result, err := d.transitionAttachment(attachmentTransitionRequest{
			source: sess1,
			target: sess2,
			next:   ac,

			expectedTransport: ac.transportSnapshot(),
			ready:             true,
		})
		switchDone <- result
		switchErr <- err
	}()

	var result attachmentTransitionResult
	select {
	case result = <-switchDone:
		require.NoError(t, <-switchErr)
	case <-time.After(2 * time.Second):
		t.Fatal("session switch waited for in-flight transport send")
	}
	require.True(t, result.published.attachmentCurrent())
	select {
	case <-handoffAtSendMu:
		t.Fatal("output rebase ran during architecture publication")
	default:
	}

	close(releaseSend)
	awaitTestCompletion(t, paintDone, "paint did not finish after Send was released")
	for _, cleanup := range result.cleanups {
		cleanup.finish()
	}
}

func TestPickerPreviewGenerationRejectsStaleSubscriptionReplacement(t *testing.T) {
	rc := newRenderCoordinator(renderCoordinatorOptions{})
	viewer := &attachedClient{}
	newer := func(renderWake) {}

	require.True(t, rc.subscribePreviewFor(viewer, 2, newer))
	require.False(t, rc.subscribePreviewFor(viewer, 1, func(renderWake) {}))
	rc.teardownPreviewFor(viewer, 1)

	rc.mu.Lock()
	subscription, ok := rc.previewWakes[viewer]
	rc.mu.Unlock()
	require.True(t, ok)
	require.Equal(t, uint64(2), subscription.generation)
	require.NotNil(t, subscription.fn)
}

func TestPickerPreviewPostSubscribeRevalidationKeepsNewerSubscription(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	viewer := &attachedClient{}
	viewer.initOverlays()
	target := &session{}
	tab := &tab{}
	rc := newRenderCoordinator(renderCoordinatorOptions{})

	viewer.overlays.pickerMu.Lock()
	viewer.overlays.pickerPreviewGeneration = 2
	viewer.overlays.pickerPreviewSession = target
	viewer.overlays.pickerPreview = tab
	viewer.overlays.pickerMu.Unlock()
	require.True(t, rc.subscribePreviewFor(viewer, 2, func(renderWake) {}))

	// This is the exact post-subscribe check from registerPreviewForSelection.
	// It represents a generation-1 subscriber resuming after generation 2 won.
	d.revalidatePreviewSubscription(viewer, rc, target, tab, 1)

	rc.mu.Lock()
	subscription, ok := rc.previewWakes[viewer]
	rc.mu.Unlock()
	require.True(t, ok)
	require.Equal(t, uint64(2), subscription.generation)
}

func TestPickerDestroyedTabCleanupDoesNotClearNewerGeneration(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	viewer := &attachedClient{}
	viewer.initOverlays()
	newerSession := &session{}
	newerTab := &tab{}
	rc := newRenderCoordinator(renderCoordinatorOptions{})

	viewer.overlays.pickerMu.Lock()
	viewer.overlays.pickerPreviewGeneration = 2
	viewer.overlays.pickerPreviewSession = newerSession
	viewer.overlays.pickerPreview = newerTab
	viewer.overlays.pickerMu.Unlock()
	require.True(t, rc.subscribePreviewFor(viewer, 2, func(renderWake) {}))

	require.False(t, d.clearPreviewGeneration(viewer, 1))
	require.True(t, pickerPreviewCurrent(viewer, newerSession, newerTab, 2))
	rc.mu.Lock()
	_, subscribed := rc.previewWakes[viewer]
	rc.mu.Unlock()
	require.True(t, subscribed)
}

func TestCaptureOverlayLayersPreservesPickerSemanticSurfacesAcrossFallbacks(t *testing.T) {
	palette := [16]renderer.RGB{}
	palette[2] = renderer.RGB{R: 10, G: 230, B: 120}
	palette[10] = palette[2]
	accentTheme := themeui.Theme{
		Foreground: renderer.RGB{R: 230, G: 230, B: 230}, Background: renderer.RGB{R: 8, G: 9, B: 10},
		HasFG: true, HasBG: true, Known: true, TrueColor: true, UsePalette: true,
		Palette: palette, PaletteKnown: 1<<2 | 1<<10,
	}
	indexedTheme := accentTheme
	indexedTheme.TrueColor = false
	neutralTheme := accentTheme
	neutralTheme.PaletteKnown = 0
	neutralTheme.SchemeKnown = false

	defaults := domain.Defaults()
	paletteOff := defaults
	paletteOff.ThemePalette = false
	forcedDark := defaults
	forcedDark.Theme = domain.ThemeDark
	forcedLight := defaults
	forcedLight.Theme = domain.ThemeLight
	tests := []struct {
		name    string
		raw     themeui.Theme
		config  domain.Config
		indexed bool
	}{
		{name: "truecolor accent", raw: accentTheme, config: defaults},
		{name: "indexed only", raw: indexedTheme, config: defaults, indexed: true},
		{name: "palette off", raw: accentTheme, config: paletteOff},
		{name: "forced dark", raw: accentTheme, config: forcedDark},
		{name: "forced light", raw: accentTheme, config: forcedLight},
		{name: "neutral fallback", raw: neutralTheme, config: defaults},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			d.ApplyConfig(tt.config)
			applied := d.resolveAppliedTheme(tt.raw)
			state := capturedRenderState{
				theme:  applied.Raw,
				styles: applied.Resolved.Styles,
				layout: capturedTabLayout{area: domain.Rect{Width: 100, Height: 38}},
			}
			snap := &overlayRenderSnapshot{
				pickerActive: true,
				pickerModel: picker.New([]picker.SessionView{
					{ID: "selected", Name: "selected", Tabs: []picker.TabEntry{{Name: "one"}}, Active: 0},
					{ID: "inactive", Name: "inactive", Tabs: []picker.TabEntry{{Name: "two", Detail: " (detail)"}}, Active: 0},
				}, picker.SelectionConfig{Mode: picker.SelectNavigationTab}),
			}

			captureOverlayLayers(&state, snap, domain.PaletteConfig{})

			require.False(t, state.styles.PickerDescription.HasBackgroundRGB)
			require.False(t, state.styles.PickerSeparator.HasBackgroundRGB)
			if tt.indexed {
				require.Equal(t, 2, state.styles.PickerDescription.Foreground)
				require.Equal(t, 2, state.styles.PickerSeparator.Foreground)
			}

			inner := state.overlays.picker.inner
			layout := picker.ChooseLayout(domain.Size{Cols: inner.Width, Rows: inner.Height})
			require.Equal(t, picker.LayoutHorizontal, layout.Mode)
			require.True(t, inner.At(5, 3).Style.Equal(state.styles.PickerDescription), "description text keeps a muted foreground on the terminal background")
			require.True(t, inner.At(layout.List.Width-1, 3).Style.Equal(state.styles.PickerBase), "inactive row filler keeps the terminal background")
			for y := layout.Separator.Y; y < layout.Separator.Y+layout.Separator.Height; y++ {
				for x := layout.Separator.X; x < layout.Separator.X+layout.Separator.Width; x++ {
					cell := inner.At(x, y)
					require.Equal(t, '│', cell.Rune)
					require.True(t, cell.Style.Equal(state.styles.PickerSeparator), "separator keeps a foreground-only style on the terminal background")
				}
			}
		})
	}
}

func TestCaptureOverlayLayersResizeRecomposesPickerWithoutStalePreview(t *testing.T) {
	model := picker.New([]picker.SessionView{{ID: "s", Name: "session", Tabs: []picker.TabEntry{{Name: "tab"}}, Active: 0}}, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
	state := capturedRenderState{theme: themeui.BuiltinDark, styles: themeui.Resolve(themeui.BuiltinDark, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}).Styles}
	cases := []struct {
		name       string
		size       domain.Size
		marker     rune
		wantMode   picker.LayoutMode
		wantSep    rune
		wantMarker bool
	}{
		{name: "horizontal", size: domain.Size{Cols: 100, Rows: 40}, marker: '◆', wantMode: picker.LayoutHorizontal, wantSep: '│', wantMarker: true},
		{name: "stacked", size: domain.Size{Cols: 40, Rows: 20}, marker: '◇', wantMode: picker.LayoutStacked, wantSep: '─', wantMarker: true},
		{name: "list only", size: domain.Size{Cols: 24, Rows: 11}, marker: '●', wantMode: picker.LayoutListOnly},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			state.layout.area = domain.Rect{Width: tt.size.Cols, Height: tt.size.Rows - 2}
			snap := &overlayRenderSnapshot{pickerActive: true, pickerModel: model, previewTab: nil}
			state.preview = picker.Preview{Width: 1, Height: 1, Rows: [][]renderer.Cell{{{Rune: tt.marker, Style: renderer.DefaultStyle()}}}}

			captureOverlayLayers(&state, snap, domain.PaletteConfig{})

			inner := state.overlays.picker.inner
			layout := picker.ChooseLayout(domain.Size{Cols: inner.Width, Rows: inner.Height})
			require.Equal(t, tt.wantMode, layout.Mode)
			got := strings.Join(frameRows(inner), "\n")
			if tt.wantMarker {
				require.Contains(t, got, string(tt.marker))
			} else {
				require.NotContains(t, got, "◆")
				require.NotContains(t, got, "◇")
				require.NotContains(t, got, "●")
			}
			if tt.wantSep == 0 {
				require.NotContains(t, got, "│")
				require.NotContains(t, got, "─")
				return
			}
			if tt.wantSep == '│' {
				require.NotContains(t, got, "─")
			} else {
				require.NotContains(t, got, "│")
			}
			for y := layout.Separator.Y; y < layout.Separator.Y+layout.Separator.Height; y++ {
				for x := layout.Separator.X; x < layout.Separator.X+layout.Separator.Width; x++ {
					cell := inner.At(x, y)
					require.Equal(t, tt.wantSep, cell.Rune)
					require.True(t, cell.Style.Equal(state.styles.PickerSeparator), "separator must use the cached PickerSeparator role")
				}
			}
		})
	}
}

func TestPickerPreviewContainsOnlyVisibleFrameRowsWithLargeScrollback(t *testing.T) {
	withScrollback := newPickerPreviewTabWithHistoryRows(t, 10_000)
	require.Equal(t, 10_000, withScrollback.focusedPane().screen.History().Len())

	preview := snapshotPickerPreview(withScrollback)
	require.Equal(t, 10, preview.Width)
	require.Equal(t, 3, preview.Height)
	require.Len(t, preview.Rows, preview.Height)
	cells := 0
	for _, row := range preview.Rows {
		require.Len(t, row, preview.Width)
		cells += len(row)
	}
	require.Equal(t, preview.Width*preview.Height, cells)
	got := strings.Join(func() []string {
		rows := make([]string, len(preview.Rows))
		for i, row := range preview.Rows {
			rows[i] = rowText(row)
		}
		return rows
	}(), "\n")
	require.Contains(t, got, "NOW")
	require.NotContains(t, got, "history-only-marker")
}

func newPickerPreviewTabWithHistoryRows(t testing.TB, historyRows int) *tab {
	t.Helper()
	tb := newTab(nil, domain.Size{Cols: 10, Rows: 3})
	p := tb.focusedPane()
	for range historyRows {
		appendHistoryRow(t, p.screen.History(), testRow("history-only-marker"))
	}
	p.screen.Write([]byte("NOW"))
	return tb
}

func TestPickerPreviewSinglePaneSnapshotsFocusedPane(t *testing.T) {
	tb := newTab(nil, domain.Size{Cols: 10, Rows: 3})
	p := tb.focusedPane()
	p.screen.Write([]byte("focused"))

	preview := snapshotPickerPreview(tb)

	require.Equal(t, 10, preview.Width)
	require.Equal(t, 3, preview.Height)
	require.Equal(t, 'f', preview.Rows[0][0].Rune)
	require.Equal(t, 'o', preview.Rows[0][1].Rune)
}

func TestPickerPreviewMultiPaneComposesTabFrame(t *testing.T) {
	tb := newTab(nil, domain.Size{Cols: 41, Rows: 5})
	left := tb.focusedPane()
	left.title.processName = "one"
	left.screen.Write([]byte("L"))
	rightTop := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 3})
	rightTop.title.processName = "two"
	rightBottom := newPane("pane-3", nil, domain.Size{Cols: 20, Rows: 2})
	rightBottom.title.processName = "three"
	rightBottom.screen.Write([]byte("R"))

	tb.mu.Lock()
	tb.panes[rightTop.id] = rightTop
	tb.panes[rightBottom.id] = rightBottom
	tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
		layout.NewLeaf(left.id),
		{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(rightTop.id), layout.NewLeaf(rightBottom.id)}, Expanded: rightBottom.id},
	}}
	tb.tree.Focus = rightBottom.id
	tb.mu.Unlock()

	preview := snapshotPickerPreview(tb)

	require.Equal(t, 41, preview.Width)
	require.Equal(t, 5, preview.Height)
	require.Equal(t, 'L', preview.Rows[0][0].Rune, "left pane content should remain visible")
	require.Equal(t, '│', preview.Rows[0][20].Rune, "split divider should be included")
	require.Equal(t, "two", rowText(preview.Rows[0][21:24]), "collapsed stack title bar should be included")
	require.Equal(t, 'R', preview.Rows[1][21].Rune, "expanded stack member draws no title bar; its content starts where the title bar used to be")
}

func TestPickerModalGeometry(t *testing.T) {
	base := domain.Size{Cols: 100, Rows: 40}

	require.Equal(t, domain.Rect{X: 10, Y: 4, Width: 80, Height: 32}, pickerModal.Resolve(base).Bounds)
	require.Equal(t, domain.AnchorCenter, pickerModal.Anchor)
}

func TestPickerResizeRecomposesModal(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer releasePTY()

	d.enterPicker(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.resize(sess, ac, domain.Size{Cols: 100, Rows: 30})
	// Picker recomposition is the relevant event here; do not depend on a real
	// resize-idle timer in this synchronous rendering test.
	d.paint(sess, ac, false, nil)

	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "┌")
	require.Contains(t, string(msg.Data), "Sessions")
}

func TestResumeStoppedAndSwitchInheritsTerminalEnv(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p1, release1 := newBlockingPTY(t)
	defer release1()
	p2, release2 := newBlockingPTY(t)
	defer release2()
	var opens [][]string
	f := portsmocks.NewMockPTYFactory(t)
	normalSize := domain.Size{Cols: sz.Cols, Rows: sz.Rows - 2}
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, normalSize).RunAndReturn(
		func(_ context.Context, _ string, _ []string, env []string, _ string, _ domain.Size) (ports.PTY, error) {
			opens = append(opens, append([]string(nil), env...))
			if len(opens) == 1 {
				return p1, nil
			}
			return p2, nil
		},
	).Twice()
	floating := newQuietPTY()
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(got domain.Size) bool {
		return got != normalSize && got.Valid()
	})).Return(floating, nil).Once()
	d := newTestDaemon(t, f, stubClock{})
	d.stopped["old"] = stoppedSession{name: "old", cwd: t.TempDir(), createdAt: 1}
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	sess, ac, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "work", Size: sz, TrueColor: true}, tr)
	require.NoError(t, err)
	d.resumeStoppedAndSwitch(sess, ac, picker.Target{Name: "old", Stopped: true})
	got := ac.currentSession()
	require.NotNil(t, got)
	got.mu.Lock()
	require.True(t, got.terminal.TrueColor)
	got.mu.Unlock()
	require.Len(t, opens, 2)
	require.Contains(t, opens[1], "TERM=xterm-direct")
	require.Contains(t, opens[1], "COLORTERM=truecolor")
	require.Contains(t, opens[1], "TERM_PROGRAM=vev")
	require.Eventually(t, func() bool {
		tb := testAttachmentTab(got)
		if tb == nil {
			return false
		}
		tb.mu.Lock()
		defer tb.mu.Unlock()
		return tb.floating.pane != nil && tb.floating.pane.pty == floating
	}, time.Second, 5*time.Millisecond)
	_ = d.killSession(got, ports.ReasonSessionKilled, false)
	release1()
	release2()
	d.sessWg.Wait()
	select {
	case <-floating.done:
	default:
		t.Fatal("floating prewarm PTY was not closed")
	}
}

// TestPickerEnterOnStoppedSessionRestoreFailureSurfacesNoticeAndStaysPut drives
// Enter on a stopped picker entry whose restore spawn fails (mock PTY Open
// error). The failure must reach the user as a NoticeSessionUnavailable
// notice and toast, and the client must remain attached to its origin
// session rather than being left in limbo.
func TestPickerEnterOnStoppedSessionRestoreFailureSurfacesNoticeAndStaysPut(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, from, ac, sends := newManualSessionWithPTYs(t, p)
	d.stopped["stopped"] = stoppedSession{name: "stopped", cwd: "/tmp/stopped", createdAt: 7}
	cause := errors.New("open failed")
	ptys := portsmocks.NewMockPTYFactory(t)
	ptys.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, cause).Once()
	d.ptys = ptys

	d.enterPicker(from, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(from, ac, []byte("j"))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(from, ac, []byte("\r"))
	awaitFrame(t, sends, ports.MsgOutput)

	history := d.notices.history()
	require.Len(t, history, 1, "failed stopped-session restore must record exactly one notice")
	require.Equal(t, domain.NoticeSessionUnavailable, history[0].Code)
	require.Equal(t, domain.NoticeError, history[0].Severity)

	toasts := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.NoticeSessionUnavailable, toasts[0].Code)

	require.Same(t, from, ac.currentSession(), "a failed restore must leave the client on its origin session")
}

// TestPickerNavigationRefreshAfterDeleteSelectsReplacingRow pins the post-kill
// contract: the cursor stays on the row index the killed session occupied, so
// it lands on whatever session took its place — clamped to the new last
// selectable row when the bottom session was killed. Session names drive the
// picker's alphabetical tie-break (every session here shares mruAt 0), which is
// what puts the victim in the middle or at the end of the list.
func TestPickerNavigationRefreshAfterDeleteSelectsReplacingRow(t *testing.T) {
	tests := []struct {
		name         string
		currentName  string
		otherName    string
		targetName   string
		nav          []byte // keys walked from the attached session's active tab onto the victim
		wantSession  domain.SessionID
		wantTabID    domain.TabStableID
		wantTabIndex int
	}{
		{
			// Rows: [a-other, other-tab, m-target, target-tab, z-current, current-first, current-active].
			// Killing target-tab (row 3) promotes z-current's header to row 2, so
			// row 3 becomes current-first — not the attached active tab.
			name:        "middle session killed selects the row taking its place",
			currentName: "z-current", otherName: "a-other", targetName: "m-target",
			nav:         []byte("kk"),
			wantSession: "current", wantTabID: "current-first", wantTabIndex: 0,
		},
		{
			// Rows: [a-current, current-first, current-active, m-other, other-tab, z-target, target-tab].
			// Killing the bottom row (6) clamps to the new last selectable row.
			name:        "last session killed selects the new last row",
			currentName: "a-current", otherName: "m-other", targetName: "z-target",
			nav:         []byte("jj"),
			wantSession: "other", wantTabID: "other-tab", wantTabIndex: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, current, ac, _, currentReleases := newManualTabSession(t, 2)
			defer releaseAll(currentReleases)
			current.id, current.name = "current", tt.currentName
			current.tabs[0].stableID, current.tabs[1].stableID = "current-first", "current-active"
			selectTestAttachmentTab(current, 1)
			delete(d.sessions, domain.SessionID("manual"))
			d.sessions[current.id] = current

			otherPTY, releaseOther := newBlockingPTY(t)
			defer releaseOther()
			otherTab := newTestTabWithContext(otherPTY, current.ctx, current.cancel)
			otherTab.stableID = "other-tab"
			other := &session{sessionCore: sessionCore{id: "other", name: tt.otherName, ephemeral: true}, ctx: current.ctx, cancel: func() {}, tabs: []*tab{otherTab}}
			d.sessions[other.id] = other

			targetPTY, releaseTarget := newBlockingPTY(t)
			defer releaseTarget()
			targetTab := newTestTabWithContext(targetPTY, current.ctx, current.cancel)
			targetTab.stableID = "target-tab"
			target := &session{sessionCore: sessionCore{id: "target", name: tt.targetName, ephemeral: true}, ctx: current.ctx, cancel: func() {}, tabs: []*tab{targetTab}}
			d.sessions[target.id] = target

			d.enterPicker(current, ac)
			for _, key := range tt.nav {
				d.handlePickerInput(ac, []byte{key})
			}
			ac.overlays.pickerMu.Lock()
			selected, ok := ac.overlays.picker.Selected()
			ac.overlays.pickerMu.Unlock()
			require.True(t, ok)
			require.Equal(t, target.id, selected.Session, "test must delete the selected non-attached session")

			d.handlePickerInput(ac, []byte("x"))

			d.mu.Lock()
			_, targetStillLive := d.sessions[target.id]
			d.mu.Unlock()
			require.False(t, targetStillLive, "ordinary picker deletion must remove the selected session")

			ac.overlays.pickerMu.Lock()
			selected, ok = ac.overlays.picker.Selected()
			ac.overlays.pickerMu.Unlock()
			require.True(t, ok)
			require.Equal(t, tt.wantSession, selected.Session)
			require.Equal(t, tt.wantTabID, selected.TabID)
			require.Equal(t, tt.wantTabIndex, selected.TabIndex)
		})
	}
}

func TestPickerAttachmentEffectDeleteRemovesSelectedSessionAndRefreshes(t *testing.T) {
	d, current, ac, _, currentReleases := newManualTabSession(t, 1)
	defer releaseAll(currentReleases)
	current.id, current.name = "current", "z-current"
	delete(d.sessions, domain.SessionID("manual"))
	d.sessions[current.id] = current

	targetPTY, releaseTarget := newBlockingPTY(t)
	defer releaseTarget()
	targetTab := newTestTabWithContext(targetPTY, current.ctx, current.cancel)
	target := &session{sessionCore: sessionCore{id: "target", name: "a-target", ephemeral: true}, ctx: current.ctx, cancel: func() {}, tabs: []*tab{targetTab}}
	d.sessions[target.id] = target
	current.mu.Lock()
	current.registerAttachmentLocked(ac)
	current.mu.Unlock()
	require.NotNil(t, d.attachCoordinator(current, ac, true))

	d.enterPicker(current, ac)
	d.handlePickerInput(ac, []byte("k"))
	ac.overlays.pickerMu.Lock()
	selected, ok := ac.overlays.picker.Selected()
	ac.overlays.pickerMu.Unlock()
	require.True(t, ok)
	require.Equal(t, target.id, selected.Session)

	token := current.attachmentToken(ac, ac.transport())
	token.lease = current.renderCoordinator().attachmentLease(ac)
	ac.publishAttachmentCapability(token)
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	d.handlePickerInput(ac, []byte("x"), effect)

	d.mu.Lock()
	_, targetStillLive := d.sessions[target.id]
	d.mu.Unlock()
	require.False(t, targetStillLive, "effect-backed picker deletion must remove the selected session")
	require.True(t, ac.overlays.pickerActive(), "successful deletion refreshes rather than closes the picker")
}

func TestPickerKillActiveSessionSnapshotDeleteRefusalReportsOnceAndKeepsPicker(t *testing.T) {
	d, from, ac, sends, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)
	cause := errors.New("snapshot delete refused")
	WithSnapshotRepository(refusingSnapshotDeleteRepository{err: cause})(d)
	target := mustLocalSession(t, d.sessions[domain.SessionID("recent")])
	store, _ := newMockStore(t)
	WithStore(t, store)(d)
	target.mu.Lock()
	if target.incarnation == (domain.IncarnationID{}) {
		target.incarnation = domain.IncarnationID{1}
	}
	record := target.persistRecordLocked(1)
	target.mu.Unlock()
	require.NoError(t, d.catalogue.Create(record))

	d.enterPicker(from, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	// MRU-desc order is from(current), recent, older; "j" moves onto "recent".
	d.handleInput(from, ac, []byte("j"))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(from, ac, []byte("x"))

	history := d.notices.history()
	require.Len(t, history, 1, "the refused purge must be reported exactly once")
	require.Equal(t, domain.NoticeInternal, history[0].Code)
	require.Contains(t, history[0].Details, cause.Error())
	require.Len(t, awaitToastCount(t, ac, 1), 1, "the refusal must produce one visible notice")

	require.Same(t, from, ac.currentSession(), "purging another session must preserve the attachment")
	require.True(t, ac.overlays.pickerActive(), "x refreshes the picker instead of closing it")
	d.mu.Lock()
	_, stillActive := d.sessions[target.id]
	d.mu.Unlock()
	require.False(t, stillActive, "the purge keeps its existing removal semantics despite cleanup failure")
}

// TestPickerKillStoppedSessionPersistDeleteFailureSurfacesNoticeAndKeepsEntry
// drives 'x' on a stopped picker entry whose persisted-record delete fails
// (mock store error). The failure must reach the user as a
// NoticePersistDelete notice and toast, and the entry must remain listed
// after the picker refreshes rather than silently vanishing from the daemon's
// bookkeeping while still present on disk.
func TestPickerKillStoppedSessionPersistDeleteFailureSurfacesNoticeAndKeepsEntry(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, from, ac, sends := newManualSessionWithPTYs(t, p)
	otherPTY, releaseOther := newBlockingPTY(t)
	defer releaseOther()
	otherTab := newTestTabWithContext(otherPTY, from.ctx, from.cancel)
	otherTab.stableID = "other-tab"
	other := &session{sessionCore: sessionCore{id: "other", name: "a-other", ephemeral: true}, ctx: from.ctx, cancel: func() {}, tabs: []*tab{otherTab}}
	d.sessions[other.id] = other
	cause := errors.New("delete failed")
	store, state := newMockStore(t)
	WithStore(t, store)(d)
	record := domain.CatalogueRecord{Name: "stopped", IncarnationID: domain.IncarnationID{1}, Cwd: "/tmp/stopped", CreatedAt: 7}
	require.NoError(t, d.catalogue.Create(record))
	state.mu.Lock()
	state.deleteErr = func(string) error { return cause }
	state.mu.Unlock()
	d.stopped["stopped"] = stoppedSession{name: "stopped", cwd: "/tmp/stopped", createdAt: 7, incarnation: record.IncarnationID, record: record}

	d.enterPicker(from, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(from, ac, []byte("j"))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(from, ac, []byte("x"))
	awaitFrame(t, sends, ports.MsgOutput)

	history := d.notices.history()
	require.NotEmpty(t, history, "failed persisted-record delete must record a notice")
	require.Equal(t, domain.NoticePersistDelete, history[0].Code)
	require.Equal(t, domain.NoticeError, history[0].Severity)

	toasts := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.NoticePersistDelete, toasts[0].Code)

	d.mu.Lock()
	stopped, retained := d.stopped["stopped"]
	d.mu.Unlock()
	require.True(t, retained, "failed deletion must retain the reserved name")
	require.True(t, stopped.purging, "an uncertain deletion stays fenced from restore")

	from.mu.Lock()
	activeTabID := testAttachmentTabLocked(from).stableID
	from.mu.Unlock()
	ac.overlays.pickerMu.Lock()
	selected, selectedOK := ac.overlays.picker.Selected()
	ac.overlays.pickerMu.Unlock()
	require.True(t, selectedOK)
	require.Equal(t, from.id, selected.Session, "failed x refresh resets navigation to the attached session")
	require.Equal(t, domain.TabStableID(activeTabID), selected.TabID, "failed x refresh follows the attached session's stable active tab")
}
