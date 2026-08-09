package daemon

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/palette"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
)

func testRecentRouteSnapshot() ports.RecentRouteSnapshot {
	return ports.RecentRouteSnapshot{
		Generation: 1,
		Active:     ports.RouteRef{Key: 1, Generation: 1},
		Entries: []ports.RecentRouteEntry{
			{Key: 2, Generation: 1, Name: "recent", Kind: ports.RouteKindLocal},
			{Key: 3, Generation: 1, Name: "older", Kind: ports.RouteKindLocal},
		},
	}
}

func beginRecentRoutePaletteEffect(t *testing.T, d *Daemon, sess *session, ac *attachedClient) attachmentConnectionToken {
	t.Helper()
	transport := ac.transport()
	rc := d.attachCoordinator(sess, nil, ac, true)
	token := sess.attachmentToken(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.publishAttachmentCapability(token)
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	t.Cleanup(effect.End)
	token.effect = effect
	return token
}

func TestCaptureOverlayLayersPreservesPaletteDescriptionSurfaceAcrossFallbacks(t *testing.T) {
	paletteColors := [16]renderer.RGB{}
	paletteColors[2] = renderer.RGB{R: 10, G: 230, B: 120}
	paletteColors[10] = paletteColors[2]
	accentTheme := themeui.Theme{
		Foreground: renderer.RGB{R: 230, G: 230, B: 230}, Background: renderer.RGB{R: 8, G: 9, B: 10},
		HasFG: true, HasBG: true, Known: true, TrueColor: true, UsePalette: true,
		Palette: paletteColors, PaletteKnown: 1<<2 | 1<<10,
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
			model := palette.New(palette.CommandResults([]command.Command{
				{Code: "ONE", Name: "One", Desc: "first description"},
				{Code: "TWO", Name: "Two", Desc: "second description"},
			}))
			model.Down()
			state := capturedRenderState{
				theme:  applied.Raw,
				styles: applied.Resolved.Styles,
				layout: capturedTabLayout{area: domain.Rect{Width: 100, Height: 38}},
			}
			snap := &overlayRenderSnapshot{paletteActive: true, paletteModel: model}

			captureOverlayLayers(&state, snap, domain.PaletteConfig{})

			inactive := state.overlays.palette.inner.At(4, 1).Style
			require.Equal(t, state.styles.PickerDescription.Foreground, inactive.Foreground)
			require.Equal(t, state.styles.PickerDescription.HasForegroundRGB, inactive.HasForegroundRGB)
			require.Equal(t, state.styles.PickerDescription.ForegroundRGB, inactive.ForegroundRGB)
			require.False(t, inactive.HasBackgroundRGB)
			if tt.indexed {
				require.Equal(t, 2, inactive.Foreground)
			}
			require.True(t, state.overlays.palette.inner.At(31, 1).Style.Equal(state.styles.PickerBase), "inactive row filler keeps the terminal background")

			selected := state.overlays.palette.inner.At(4, 2).Style
			require.True(t, selected.Equal(state.styles.PickerSelectionMuted))
		})
	}
}

func TestPaletteOpenTypeEnterRunAndEscClose(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p1, p2)
	sess.mu.Lock()
	sess.registerAttachmentLocked(ac)
	sess.mu.Unlock()
	d.ptys = newBlockingOpenFactory(t, d)
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
	require.Equal(t, 1, testAttachmentTabIndex(sess))
	requireFloatingInitialized(t, testAttachmentTab(sess))
	awaitFrame(t, sends, ports.MsgOutput)

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("\x1b"))
	require.False(t, ac.overlays.paletteActive())
	awaitFrame(t, sends, ports.MsgOutput)
}

// TestPaletteCommandFailureSurfacesAsNotice drives a palette command whose Run
// target fails and asserts the failure reaches the user as a notice instead of
// only a log line. The new-tab spawn failure is wrapped as a domain.UserError
// with NoticeTabSpawn (session.go createTab), so it surfaces under that code
// rather than the NoticeInternal catch-all.
func TestPaletteCommandFailureSurfacesAsNotice(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	ptys := portsmocks.NewMockPTYFactory(t)
	ptys.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("open failed")).Once()
	d.ptys = ptys

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("CNT\r"))
	awaitFrame(t, sends, ports.MsgOutput)

	history := d.notices.history()
	require.NotEmpty(t, history, "failed palette command must record a notice")
	require.Equal(t, domain.NoticeTabSpawn, history[0].Code)
}

func TestPaletteCommandNoticeErrorPreservesTypedErrorsAndMapsMoveFailures(t *testing.T) {
	typedTabSpawn := domain.UserErr(domain.NoticeTabSpawn, "couldn't open tab", errors.New("open failed"))
	tests := []struct {
		name, slug, message string
		err                 error
		code                domain.NoticeCode
		severity            domain.NoticeSeverity
		wantSame            bool
	}{
		{name: "CNT typed command error", slug: "new-tab", err: typedTabSpawn, code: domain.NoticeTabSpawn, severity: domain.NoticeError, message: "couldn't open tab", wantSame: true},
		{name: "MPN no destination", slug: "move-pane", err: errNoMoveDestination, code: domain.NoticeSessionUnavailable, severity: domain.NoticeWarn, message: "No destination available."},
		{name: "MTB warming floating", slug: "move-tab", err: errMoveFloatingWarming, code: domain.NoticeSessionUnavailable, severity: domain.NoticeWarn, message: "Wait for the floating pane to finish opening."},
		{name: "MPN final pane with floating slot", slug: "move-pane", err: errMoveFinalSourceFloating, code: domain.NoticeLayoutTooSmall, severity: domain.NoticeWarn, message: "Close the floating pane or move the whole tab."},
		{name: "MPN destination too small", slug: "move-pane", err: errMoveTooSmall, code: domain.NoticeLayoutTooSmall, severity: domain.NoticeWarn, message: "Not enough space in destination tab."},
		{name: "MTB stale destination", slug: "move-tab", err: errMoveStaleTarget, code: domain.NoticeSessionUnavailable, severity: domain.NoticeWarn, message: "Destination is no longer available."},
		{name: "MPN generic invalid", slug: "move-pane", err: errMovePaneInvalid, code: domain.NoticeInternal, severity: domain.NoticeError, message: "Move failed."},
		{name: "MPN typed application error", slug: "move-pane", err: typedTabSpawn, code: domain.NoticeTabSpawn, severity: domain.NoticeError, message: "couldn't open tab", wantSame: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, ok := command.BySlug(tt.slug)
			require.True(t, ok)
			got := paletteCommandNoticeError(cmd, tt.err)
			if tt.wantSame {
				require.Same(t, tt.err, got)
			}
			var userErr *domain.UserError
			require.ErrorAs(t, got, &userErr)
			require.Equal(t, tt.code, userErr.Code)
			require.Equal(t, tt.severity, userErr.Severity)
			require.Equal(t, tt.message, userErr.Msg)
		})
	}
}

func TestPaletteCommandNoticeErrorUsesCommandScope(t *testing.T) {
	t.Run("future cross-session command inherits move rejection presentation", func(t *testing.T) {
		cmd := command.Command{Slug: "future-transfer", Scope: command.CommandScopeCrossSession}
		got := paletteCommandNoticeError(cmd, errMoveStaleTarget)
		var userErr *domain.UserError
		require.ErrorAs(t, got, &userErr)
		require.Equal(t, domain.NoticeSessionUnavailable, userErr.Code)
		require.Equal(t, "Destination is no longer available.", userErr.Msg)
	})

	t.Run("move-like slug without metadata is unchanged", func(t *testing.T) {
		err := errors.New("ordinary failure")
		got := paletteCommandNoticeError(command.Command{Slug: "move-pane"}, err)
		require.Same(t, err, got)
	})
}

func TestPaletteEntryPublishesEligibleNamedSessionResults(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, current, ac, _ := newManualSessionWithPTYs(t, p)
	d.sessions["active"] = &session{sessionCore: sessionCore{id: "active", name: "active", createdAt: 10}}
	d.sessions["ephemeral"] = &session{sessionCore: sessionCore{id: "ephemeral", name: "ephemeral", ephemeral: true, createdAt: 11}}
	d.stopped["stopped"] = stoppedSession{name: "stopped", createdAt: 12}
	d.stopped["purging"] = stoppedSession{name: "purging", createdAt: 13, purging: true}

	d.enterPalette(current, ac)
	ac.overlays.paletteMu.Lock()
	defer ac.overlays.paletteMu.Unlock()

	got := map[string]palette.ResultKind{}
	for _, match := range ac.overlays.palette.Matches() {
		if name, ok := match.Result.SessionName(); ok {
			got[name] = match.Result.Kind()
		}
	}
	require.Equal(t, map[string]palette.ResultKind{
		"active":  palette.ResultKindActiveSession,
		"stopped": palette.ResultKindStoppedSession,
	}, got)
}

func TestPaletteSessionFailureFeedbackClearsOnQueryChange(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, current, ac, _ := newManualSessionWithPTYs(t, p)
	target := &session{sessionCore: sessionCore{id: "active", name: "active", createdAt: 10}}
	d.sessions[target.id] = target

	d.enterPalette(current, ac)
	d.handlePaletteInput(ac, []byte("active"))
	ac.overlays.paletteMu.Lock()
	selected, ok := ac.overlays.palette.Selected()
	ac.overlays.paletteMu.Unlock()
	_, isSession := selected.SessionName()
	require.True(t, ok)
	require.True(t, isSession)
	d.mu.Lock()
	delete(d.sessions, target.core().id)
	d.mu.Unlock()

	d.handlePaletteInput(ac, []byte("\r"))
	ac.overlays.paletteMu.Lock()
	require.Equal(t, "requested session is unavailable", ac.overlays.paletteFeedback)
	ac.overlays.paletteMu.Unlock()
	d.handlePaletteInput(ac, []byte("x"))
	ac.overlays.paletteMu.Lock()
	require.Empty(t, ac.overlays.paletteFeedback)
	ac.overlays.paletteMu.Unlock()
}

func TestPaletteEnterFreezesActiveSessionSelectionAgainstTrailingInput(t *testing.T) {
	d, current, ac, sends, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)
	target := mustLocalSession(t, d.sessions[domain.SessionID("recent")])
	target.createdAt = 42

	d.handleInput(current, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(current, ac, []byte("recent\rx"))

	require.Same(t, target, ac.currentSession(), "Enter must retain the selected session despite trailing frame bytes")
	require.False(t, ac.overlays.paletteActive(), "trailing input must not prevent the captured selection from closing the palette")
	awaitFrame(t, sends, ports.MsgOutput)
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPaletteSelectedActiveSessionSwitchesWithoutRecordingCommandRecency(t *testing.T) {
	d, current, ac, sends, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)
	target := mustLocalSession(t, d.sessions[domain.SessionID("recent")])
	target.createdAt = 42

	d.handleInput(current, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(current, ac, []byte("recent\r"))

	require.Same(t, target, ac.currentSession())
	require.Contains(t, target.snapshotAttachments(), ac, "canonical handoff reuses the attached client")
	require.False(t, ac.overlays.paletteActive())
	require.Empty(t, d.paletteRecent, "session selections are not command recency")
	awaitFrame(t, sends, ports.MsgOutput)
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPaletteStoppedSessionResumeFailureKeepsPaletteAndSourceAttachment(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, current, ac, sends := newManualSessionWithPTYs(t, p)
	stopped := stoppedSession{name: "stopped", cwd: "/tmp", createdAt: 42, lastUsedSeq: 7, tabNames: []string{"shell", "logs"}}
	d.stopped[stopped.name] = stopped
	ptys := portsmocks.NewMockPTYFactory(t)
	ptys.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("open failed")).Once()
	d.ptys = ptys

	d.handleInput(current, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(current, ac, []byte("stopped\r"))

	require.Same(t, current, ac.currentSession(), "failed resume must retain the source attachment")
	current.mu.Lock()
	require.Contains(t, current.snapshotAttachmentsLocked(), ac)
	current.mu.Unlock()
	require.True(t, ac.overlays.paletteActive())
	ac.overlays.paletteMu.Lock()
	require.Equal(t, "requested session is unavailable", ac.overlays.paletteFeedback)
	ac.overlays.paletteMu.Unlock()
	require.Equal(t, stopped, d.stopped[stopped.name], "failed resume must retain stopped lifecycle metadata")
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPaletteSelectedStoppedSessionResumesAndSwitches(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	defer release1()
	defer release2()
	d, current, ac, sends := newManualSessionWithPTYs(t, p1)
	d.ptys = newFactory(t, p2)
	d.stopped["stopped"] = stoppedSession{name: "stopped", cwd: "/tmp", createdAt: 42}

	d.handleInput(current, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	generation := ac.connectionGeneration.Load()
	d.handleInput(current, ac, []byte("stopped\r"))

	resumed := ac.currentSession()
	require.Equal(t, "stopped", resumed.name)
	require.Equal(t, int64(42), resumed.createdAt)
	require.Equal(t, true, resumed.attachmentRegistered(ac))
	require.Greater(t, ac.connectionGeneration.Load(), generation, "stopped-session handoff must publish through the attachment transition")
	require.False(t, ac.overlays.paletteActive())
	firstPaint := awaitFrame(t, sends, ports.MsgOutput)
	firstOutput, err := ports.UnmarshalOutput(firstPaint.Payload)
	require.NoError(t, err)
	require.Zero(t, firstOutput.Base, "stopped-session first paint must reset moving output state")
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPaletteJRSActivatesOnlyExactContextualHint(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{Generation: 1, Active: ports.RouteRef{Key: 1, Generation: 1}})

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("JRS"))
	awaitFrame(t, sends, ports.MsgOutput)

	ac.overlays.paletteMu.Lock()
	require.Equal(t, command.ContextHintRecentSessions, ac.overlays.paletteHints.Kind)
	require.Equal(t, "no recent sessions", ac.overlays.paletteHints.Feedback)
	ac.overlays.paletteMu.Unlock()

	d.handleInput(sess, ac, []byte("\b\b\bRNS"))
	awaitFrame(t, sends, ports.MsgOutput)
	ac.overlays.paletteMu.Lock()
	require.Equal(t, command.ContextHintNone, ac.overlays.paletteHints.Kind)
	ac.overlays.paletteMu.Unlock()
}

func TestPaletteJRSUsesEffectiveOverrideOnly(t *testing.T) {
	d, current, ac, sends, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)
	ac.setRouteSnapshot(testRecentRouteSnapshot())
	token := beginRecentRoutePaletteEffect(t, d, current, ac)
	d.ApplyConfig(domain.Config{Codes: map[string]string{"jump-recent-session": "RJS"}})

	d.handleInputForAttachment(token, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInputForAttachment(token, []byte("RJS"))
	awaitFrame(t, sends, ports.MsgOutput)
	ac.overlays.paletteMu.Lock()
	require.Equal(t, command.ContextHintRecentSessions, ac.overlays.paletteHints.Kind)
	ac.overlays.paletteMu.Unlock()
	d.handleInputForAttachment(token, []byte(" 1\r"))

	actionFrame := awaitFrame(t, sends, ports.MsgNavigateRecentRoute)
	action, err := ports.UnmarshalRouteNavigationAction(actionFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.RouteNavigationAction{SnapshotGeneration: 1, Key: 2, Generation: 1}, action)
	require.Same(t, current, ac.currentSession())
	require.False(t, ac.overlays.paletteActive())

	d.handleInputForAttachment(token, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInputForAttachment(token, []byte("JRS"))
	awaitFrame(t, sends, ports.MsgOutput)
	ac.overlays.paletteMu.Lock()
	require.Equal(t, command.ContextHintNone, ac.overlays.paletteHints.Kind)
	ac.overlays.paletteMu.Unlock()
	require.Same(t, current, ac.currentSession())
	d.handleInputForAttachment(token, []byte("\r"))
	require.True(t, ac.overlays.paletteActive(), "literal JRS must not execute an overridden jump command")
}

func TestPaletteResultsDeduplicateRoutesLocalToCurrentRemoteDaemon(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	current := addControlSession(d, "hi", "tab-1", "pane-1")
	represented := addControlSession(d, "remote-manual", "tab-2", "pane-2")
	represented.ephemeral = false
	snapshot := ports.RecentRouteSnapshot{
		Generation: 2,
		Active:     ports.RouteRef{Key: 1, Generation: 1},
		Entries: []ports.RecentRouteEntry{{
			Key: 2, Generation: 1, Name: "remote-manual", HostLabel: "remote", Kind: ports.RouteKindRemote,
		}},
	}

	for _, test := range []struct {
		name          string
		currentOrigin string
		want          []string
	}{
		{name: "remote daemon", currentOrigin: "remote", want: []string{"Switch to session remote-manual"}},
		{name: "home daemon keeps distinct remote", want: []string{"Switch to session remote-manual", "Switch to session remote-manual@remote"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			results := d.paletteResults(current, nil, snapshot, test.currentOrigin)
			var matching []string
			for _, result := range results {
				if result.DisplayText() == "Switch to session remote-manual" || result.DisplayText() == "Switch to session remote-manual@remote" {
					matching = append(matching, result.DisplayText())
				}
			}
			require.Equal(t, test.want, matching)
		})
	}
}

func TestPaletteFuzzyRemoteRecentRouteSendsExactNavigationAction(t *testing.T) {
	d, current, ac, sends, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{
		Generation: 9,
		Active:     ports.RouteRef{Key: 1, Generation: 1},
		Entries: []ports.RecentRouteEntry{{
			Key: 8, Generation: 4, Name: "logs", HostLabel: "edge", Kind: ports.RouteKindRemote,
		}},
	})
	token := beginRecentRoutePaletteEffect(t, d, current, ac)

	d.handleInputForAttachment(token, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInputForAttachment(token, []byte("logs@edge\r"))

	actionFrame := awaitFrame(t, sends, ports.MsgNavigateRecentRoute)
	action, err := ports.UnmarshalRouteNavigationAction(actionFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.RouteNavigationAction{SnapshotGeneration: 9, Key: 8, Generation: 4}, action)
	require.False(t, ac.overlays.paletteActive())
}

func TestPaletteFuzzySelectedStaticCommandExecutes(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer releases[0]()
	defer releases[1]()

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("next\r"))

	require.False(t, ac.overlays.paletteActive())
	require.Equal(t, 1, testAttachmentTabIndex(sess))
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPaletteJRSUsesCapturedRankAfterMRUChanges(t *testing.T) {
	d, current, ac, sends, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)
	ac.setRouteSnapshot(testRecentRouteSnapshot())
	token := beginRecentRoutePaletteEffect(t, d, current, ac)
	d.handleInputForAttachment(token, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	// Reordering live MRU after opening must not shift rank 1 from its capture.
	d.sessions[domain.SessionID("older")].core().mruAt.Store(100)
	d.handleInputForAttachment(token, []byte("JRS 1\r"))

	actionFrame := awaitFrame(t, sends, ports.MsgNavigateRecentRoute)
	action, err := ports.UnmarshalRouteNavigationAction(actionFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.RouteNavigationAction{SnapshotGeneration: 1, Key: 2, Generation: 1}, action)
	require.Same(t, current, ac.currentSession())
	require.False(t, ac.overlays.paletteActive())
}

func TestPaletteDeniedPostHandoffAttachmentEffectClosesAndInvalidates(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	invalidations := installPaletteInvalidationObserver(sess)

	d.enterPalette(sess, ac)
	d.handlePaletteInput(ac, []byte("BSK"))
	// Make the attachment token detached while retaining currentSession. The
	// no-op BSK command succeeds, but its post-execution beginAttachmentEffect is
	// deterministically denied.
	sess.mu.Lock()
	clearAttachmentsForTestLocked(sess)
	sess.mu.Unlock()

	d.handlePaletteInput(ac, []byte("\r"))

	require.False(t, ac.overlays.paletteActive(), "denied cleanup admission must still close the executed palette")
	invalidation := awaitTestValue(t, invalidations, "denied palette cleanup did not invalidate rendering")
	require.True(t, invalidation.reset)
}

func TestPaletteJRSDoesNotRevalidateTargetInDaemon(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{
		Generation: 7,
		Active:     ports.RouteRef{Key: 1, Generation: 1},
		Entries:    []ports.RecentRouteEntry{{Key: 9, Generation: 4, Name: "captured", Kind: ports.RouteKindLocal}},
	})

	validated := make(chan struct{})
	releaseHandoff := make(chan struct{})
	d.beforeRecentSessionHandoff = func() {
		close(validated)
		<-releaseHandoff
	}
	token := beginRecentRoutePaletteEffect(t, d, sess, ac)
	d.handleInputForAttachment(token, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	inputHandled := make(chan struct{})
	go func() {
		d.handleInputForAttachment(token, []byte("JRS 1\r"))
		close(inputHandled)
	}()
	awaitTestCompletion(t, validated, "JRS did not publish the captured route action")
	// The daemon must not consult or revalidate the target lifecycle. The
	// client owns exact-target validation after it receives this action.
	close(releaseHandoff)
	awaitTestCompletion(t, inputHandled, "JRS route-action input did not complete")

	actionFrame := awaitFrame(t, sends, ports.MsgNavigateRecentRoute)
	action, err := ports.UnmarshalRouteNavigationAction(actionFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.RouteNavigationAction{SnapshotGeneration: 7, Key: 9, Generation: 4}, action)
	require.False(t, ac.overlays.paletteActive())
}

func TestPaletteJRSOutOfRangeKeepsPaletteOpenWithoutClamping(t *testing.T) {
	d, current, ac, sends, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)
	ac.setRouteSnapshot(testRecentRouteSnapshot())
	token := beginRecentRoutePaletteEffect(t, d, current, ac)

	d.handleInputForAttachment(token, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInputForAttachment(token, []byte("JRS 3\r"))

	require.Same(t, current, ac.currentSession())
	require.True(t, ac.overlays.paletteActive())
	ac.overlays.paletteMu.Lock()
	require.Equal(t, "JRS 3", ac.overlays.palette.Query())
	require.Equal(t, "requested recent session is unavailable", ac.overlays.paletteFeedback)
	ac.overlays.paletteMu.Unlock()
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPaletteJRSMalformedRankFeedback(t *testing.T) {
	d, current, ac, sends, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)

	d.handleInput(current, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(current, ac, []byte("JRS 0\r"))

	require.Same(t, current, ac.currentSession())
	require.True(t, ac.overlays.paletteActive())
	ac.overlays.paletteMu.Lock()
	require.Equal(t, "JRS 0", ac.overlays.palette.Query())
	require.Equal(t, "rank must be one positive decimal", ac.overlays.paletteFeedback)
	ac.overlays.paletteMu.Unlock()
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPaletteFailureDoesNotOverwriteChangedQueryInSameGeneration(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)

	d.enterPalette(sess, ac)
	ac.overlays.paletteMu.Lock()
	generation := ac.overlays.paletteGeneration
	ac.overlays.palette.Insert('x')
	ac.overlays.paletteMu.Unlock()
	ac.paletteFailure(generation, "", "stale failure")

	ac.overlays.paletteMu.Lock()
	require.Empty(t, ac.overlays.paletteFeedback)
	ac.overlays.paletteMu.Unlock()
}

func TestPaletteFailureDoesNotOverwriteNewerInteraction(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)

	d.enterPalette(sess, ac)
	ac.overlays.paletteMu.Lock()
	staleGeneration := ac.overlays.paletteGeneration
	ac.overlays.paletteMu.Unlock()
	d.enterPalette(sess, ac)
	ac.paletteFailure(staleGeneration, "JRS 1", "stale failure")

	ac.overlays.paletteMu.Lock()
	require.Empty(t, ac.overlays.paletteFeedback)
	ac.overlays.paletteMu.Unlock()
	awaitFrame(t, sends, ports.MsgOutput)
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPaletteFLTExecutesFloatingToggle(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	tb := testAttachmentTab(sess)
	installTestFloating(tb, newPane("floating", nil, domain.Size{Cols: 20, Rows: 5}), false)

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("FLT\r"))

	require.False(t, ac.overlays.paletteActive())
	tb.mu.Lock()
	require.Equal(t, floatingVisible, tb.floating.state)
	tb.mu.Unlock()
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPaletteCNSPromptsForSessionNameThenCreatesAndSwitches(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p1)
	defer release1()
	defer release2()
	d.ptys = newFactorySeq(t, p2)

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("CNS\r"))
	promptFrame := awaitFrame(t, sends, ports.MsgOutput)
	promptOutput, err := ports.UnmarshalOutput(promptFrame.Payload)
	require.NoError(t, err)
	require.False(t, ac.overlays.paletteActive())
	require.True(t, ac.overlays.promptActive())
	require.Contains(t, string(promptOutput.Data), "Create session")

	generation := ac.connectionGeneration.Load()
	d.handleInput(sess, ac, []byte("scratch\r"))
	// The submit first paints the newly attached session while the prompt is
	// still open, then handlePromptInput closes the prompt and repaints the
	// client's current session. The final frame must be for the new session.
	firstPaint := awaitFrame(t, sends, ports.MsgOutput)
	firstOutput, err := ports.UnmarshalOutput(firstPaint.Payload)
	require.NoError(t, err)
	require.Zero(t, firstOutput.Base, "new-session first paint must reset moving output state")
	finalRepaint := awaitFrame(t, sends, ports.MsgOutput)
	finalOutput, err := ports.UnmarshalOutput(finalRepaint.Payload)
	require.NoError(t, err)
	require.False(t, ac.overlays.promptActive())
	require.Equal(t, 2, sessionCount(d))
	require.Empty(t, sess.snapshotAttachments())
	newSess := ac.currentSession()
	require.NotNil(t, newSess)
	require.NotSame(t, sess, newSess)
	require.Equal(t, "scratch", newSess.name)
	require.False(t, newSess.ephemeral)
	require.Contains(t, newSess.snapshotAttachments(), ac)
	require.Equal(t, true, newSess.attachmentRegistered(ac))
	require.Greater(t, ac.connectionGeneration.Load(), generation, "new-session handoff must publish through the attachment transition")
	require.Contains(t, string(finalOutput.Data), "scratch")
	require.NotContains(t, string(finalOutput.Data), "Create session")
}

func TestPaletteReopensWithSuccessfulCommandFirst(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	sess.mu.Lock()
	sess.registerAttachmentLocked(ac)
	sess.mu.Unlock()
	defer releases[0]()
	defer releases[1]()

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("NXT\r"))
	require.False(t, ac.overlays.paletteActive())
	awaitFrame(t, sends, ports.MsgOutput)

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	result, ok := ac.overlays.palette.Selected()
	require.True(t, ok)
	cmd, ok := result.Command()
	require.True(t, ok)
	require.Equal(t, "NXT", cmd.Code)
}

func TestPaletteCodeOverridesRejectAPIOnlyCommands(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})

	overrides, warnings := d.buildCodeOverrides(map[string]string{
		"new-tab": "TAB",
		"toast":   "API",
	})

	require.Equal(t, map[string]string{"new-tab": "TAB"}, overrides)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0].Msg, `unknown command code slug "toast"`)
}

func TestCommandByEffectiveCodeExcludesAPIOnlyCommands(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	overrides := map[string]string{
		"new-tab": "TAB",
		"toast":   "API",
	}
	d.codeOverrides.Store(&overrides)

	cmd, ok := d.commandByEffectiveCode("tab")
	require.True(t, ok)
	require.Equal(t, "new-tab", cmd.Slug)
	require.Equal(t, "TAB", cmd.Code)

	_, ok = d.commandByEffectiveCode("API")
	require.False(t, ok)
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
	require.Equal(t, []string{"SSP", "NXT", "CNT", "CNS", "CLT", "SPR", "SPL", "SPU", "SPD", "CEL", "CER", "STP", "TST", "FLT", "CLP", "MPN", "MTB", "FPL", "FPR", "FPU", "FPD", "RSZ", "GPW", "SPW", "GPH", "SPH", "EQP", "PVT", "BSK", "JRS", "NTC", "YLN", "VIS", "RNS", "RNT", "DET"}, codes)
}

func TestPaletteRecencyCanBeUpdatedConcurrently(t *testing.T) {
	d := &Daemon{}
	codes := []string{"CNT", "CNS", "CLT", "SPR", "SPL", "SPU", "SPD", "STP", "TST", "FLT", "CLP", "MPN", "MTB", "FPL", "FPR", "FPU", "FPD", "RSZ", "GPW", "SPW", "GPH", "SPH", "EQP", "NXT", "PVT", "BSK", "SSP", "VIS", "RNS", "RNT", "DET"}

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
	require.Len(t, commands, len(command.PaletteRegistry()))
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
	ptys.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("open failed"))
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
	result, ok := ac.overlays.palette.Selected()
	require.True(t, ok)
	cmd, ok := result.Command()
	require.True(t, ok)
	require.Equal(t, "CNS", cmd.Code)

	d.handlePaletteInput(ac, []byte{0x10})
	awaitFrame(t, sends, ports.MsgOutput)
	result, ok = ac.overlays.palette.Selected()
	require.True(t, ok)
	cmd, ok = result.Command()
	require.True(t, ok)
	require.Equal(t, "CNT", cmd.Code)
}

func TestPaletteModalGeometry(t *testing.T) {
	type testCase struct {
		name string
		size domain.Size
		cfg  domain.PaletteConfig
		want domain.Rect
	}

	auto := domain.PaletteConfig{}
	tests := []testCase{
		{name: "auto 79 column drawer", size: domain.Size{Cols: 79, Rows: 40}, cfg: auto, want: domain.Rect{X: 0, Y: 28, Width: 79, Height: 11}},
		{name: "auto 80 column shelf", size: domain.Size{Cols: 80, Rows: 40}, cfg: auto, want: domain.Rect{X: 0, Y: 28, Width: 80, Height: 11}},
		{name: "auto 95 column shelf", size: domain.Size{Cols: 95, Rows: 40}, cfg: auto, want: domain.Rect{X: 0, Y: 28, Width: 95, Height: 11}},
		{name: "auto 96 column rail", size: domain.Size{Cols: 96, Rows: 40}, cfg: auto, want: domain.Rect{X: 31, Y: 28, Width: 64, Height: 11}},
		{name: "auto 120 column rail", size: domain.Size{Cols: 120, Rows: 40}, cfg: auto, want: domain.Rect{X: 55, Y: 28, Width: 64, Height: 11}},
		{name: "auto tiny terminal clamps", size: domain.Size{Cols: 20, Rows: 6}, cfg: auto, want: domain.Rect{X: 0, Y: 3, Width: 20, Height: 2}},
	}
	for _, anchor := range []domain.Anchor{domain.AnchorTopLeft, domain.AnchorTop, domain.AnchorTopRight, domain.AnchorLeft, domain.AnchorCenter, domain.AnchorRight, domain.AnchorBottomLeft, domain.AnchorBottom, domain.AnchorBottomRight} {
		wantX := map[domain.Anchor]int{domain.AnchorTopLeft: 1, domain.AnchorTop: 28, domain.AnchorTopRight: 55, domain.AnchorLeft: 1, domain.AnchorCenter: 28, domain.AnchorRight: 55, domain.AnchorBottomLeft: 1, domain.AnchorBottom: 28, domain.AnchorBottomRight: 55}[anchor]
		wantY := map[domain.Anchor]int{domain.AnchorTopLeft: 1, domain.AnchorTop: 1, domain.AnchorTopRight: 1, domain.AnchorLeft: 14, domain.AnchorCenter: 14, domain.AnchorRight: 14, domain.AnchorBottomLeft: 28, domain.AnchorBottom: 28, domain.AnchorBottomRight: 28}[anchor]
		tests = append(tests, testCase{name: anchor.String(), size: domain.Size{Cols: 120, Rows: 40}, cfg: domain.PaletteConfig{Anchor: anchor, AnchorSet: true}, want: domain.Rect{X: wantX, Y: wantY, Width: 64, Height: 11}})
	}
	for _, anchor := range []domain.Anchor{domain.AnchorLeft, domain.AnchorCenter, domain.AnchorRight} {
		tests = append(tests, testCase{name: "narrow " + anchor.String(), size: domain.Size{Cols: 95, Rows: 40}, cfg: domain.PaletteConfig{Anchor: anchor, AnchorSet: true}, want: domain.Rect{X: 0, Y: 14, Width: 95, Height: 11}})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modal := paletteModalFor(tt.size, tt.cfg)
			presentation := modal.Resolve(tt.size)
			require.Equal(t, tt.want, presentation.Bounds)
			if tt.size.Cols < ui.ResponsiveDrawerBreakpoint {
				require.Equal(t, ui.PresentationDrawer, presentation.Mode)
				require.Equal(t, ui.BorderTop, presentation.Borders)
			} else {
				require.Equal(t, ui.PresentationFloating, presentation.Mode)
				require.Equal(t, ui.BorderAll, presentation.Borders)
			}
			require.Equal(t, " Commands ", modal.Title)
			require.Equal(t, 11, modal.FixedHeight)
			require.Equal(t, ui.Margins{Top: 1, Right: 1, Bottom: 1, Left: 1}, modal.Margins)
		})
	}
}

func TestComposePaletteClientFrameUsesResponsivePresentation(t *testing.T) {
	model := palette.New(nil)
	for _, tt := range []struct {
		name      string
		width     int
		topLeft   rune
		innerLeft rune
	}{
		{name: "79 column drawer", width: 79, topLeft: '─', innerLeft: '>'},
		{name: "80 column shelf", width: 80, topLeft: '┌', innerLeft: '│'},
	} {
		t.Run(tt.name, func(t *testing.T) {
			frame, _ := composePaletteClientFrame(model, renderer.NewFrame(tt.width, 40), domain.PaletteConfig{}, "")

			require.Equal(t, tt.topLeft, frame.At(0, 28).Rune)
			require.Equal(t, tt.innerLeft, frame.At(0, 29).Rune)
		})
	}
}

func TestComposePaletteClientFrameUsesConfiguredPosition(t *testing.T) {
	model := palette.New(nil)
	base := renderer.NewFrame(120, 40)
	for _, tt := range []struct {
		name string
		cfg  domain.PaletteConfig
		at   domain.Rect
	}{
		{name: "top left", cfg: domain.PaletteConfig{Anchor: domain.AnchorTopLeft, AnchorSet: true}, at: domain.Rect{X: 1, Y: 1}},
		{name: "bottom right", cfg: domain.PaletteConfig{Anchor: domain.AnchorBottomRight, AnchorSet: true}, at: domain.Rect{X: 55, Y: 28}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			frame, _ := composePaletteClientFrame(model, base, tt.cfg, "")
			require.Equal(t, '┌', frame.At(tt.at.X, tt.at.Y).Rune)
		})
	}
}

func TestComposePaletteClientFrameUsesSelectedDescriptionTheme(t *testing.T) {
	model := palette.New(palette.CommandResults([]command.Command{{Code: "ONE", Desc: "first description"}}))
	size := domain.Size{Cols: 120, Rows: 40}
	base := renderer.NewFrame(size.Cols, size.Rows)
	styles := themeui.Resolve(themeui.BuiltinDark, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}).Styles

	frame, _ := composePaletteClientFrame(model, base, domain.PaletteConfig{}, "", styles)
	inner := paletteModalFor(size, domain.PaletteConfig{}).Resolve(size).Inner

	require.True(t, frame.At(inner.X+4, inner.Y+1).Style.Equal(styles.PickerSelectionMuted))
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
			d.paint(sess, ac, true, nil)
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

func TestPaletteTabCompletesSelectedCommandWithoutForwardingToPTY(t *testing.T) {
	tests := []struct {
		name                       string
		config                     map[string]string
		recent                     string
		query                      string
		want                       string
		wantCompletionInvalidation bool
		setupInvalidations         int
	}{
		{name: "changed completion invalidates once", query: "NX", want: "NXT", wantCompletionInvalidation: true},
		{name: "exact argument command is a no-op", query: "JRS 1", want: "JRS 1"},
		{name: "empty query starts at registry first", want: "CNT", wantCompletionInvalidation: true},
		{name: "empty query starts at daemon MRU", recent: "NXT", want: "NXT", wantCompletionInvalidation: true},
		{name: "effective configured override completes", config: map[string]string{"new-tab": "NEW"}, want: "NEW", wantCompletionInvalidation: true, setupInvalidations: 1},
		{name: "stale pre-override MRU is ignored", config: map[string]string{"new-tab": "NEW"}, recent: "CNT", want: "NEW", wantCompletionInvalidation: true, setupInvalidations: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writes := make(chan []byte, 1)
			p, release := newBlockingPTYWithWrites(t, writes)
			defer release()
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)
			invs := installPaletteInvalidationObserver(sess)
			if tt.recent != "" {
				d.recordPaletteUse(tt.recent)
			}
			if tt.config != nil {
				d.ApplyConfig(domain.Config{Codes: tt.config})
				for range tt.setupInvalidations {
					awaitInvalidation(t, invs)
				}
				requireNoInvalidation(t, invs)
			}

			d.handleInput(sess, ac, []byte("\x1b "))
			awaitInvalidation(t, invs)
			requireNoInvalidation(t, invs)
			if tt.query != "" {
				d.handleInput(sess, ac, []byte(tt.query))
				awaitInvalidation(t, invs)
				requireNoInvalidation(t, invs)
			}

			d.handleInput(sess, ac, []byte("\t"))
			ac.overlays.paletteMu.Lock()
			got := ac.overlays.palette.Query()
			ac.overlays.paletteMu.Unlock()
			require.Equal(t, tt.want, got)
			if tt.wantCompletionInvalidation {
				awaitInvalidation(t, invs)
			}
			requireNoInvalidation(t, invs)
			requireNoOutputFrame(t, sends)
			requireNoPTYWrite(t, writes)
		})
	}
}

func TestPaletteBatchedTabCompletionPreservesRepaintInvalidation(t *testing.T) {
	writes := make(chan []byte, 1)
	p, release := newBlockingPTYWithWrites(t, writes)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	invs := installPaletteInvalidationObserver(sess)

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitInvalidation(t, invs)
	requireNoInvalidation(t, invs)

	d.handleInput(sess, ac, []byte("NX\t\t"))
	ac.overlays.paletteMu.Lock()
	got := ac.overlays.palette.Query()
	ac.overlays.paletteMu.Unlock()
	require.Equal(t, "NXT", got)
	awaitInvalidation(t, invs)
	requireNoInvalidation(t, invs)
	requireNoOutputFrame(t, sends)
	requireNoPTYWrite(t, writes)
}

func installPaletteInvalidationObserver(sess *session) chan renderInvalidation {
	invs := make(chan renderInvalidation, 8)
	sess.installRenderCoordinator(newRenderCoordinator(renderCoordinatorOptions{
		clock: nil,
		wake:  nil,
		onInvalidate: func(inv renderInvalidation) {
			invs <- inv
		},
	}))
	return invs
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
	require.Equal(t, 1, testAttachmentTabIndex(sess))
	require.NoError(t, exec.PrevTab())
	require.Equal(t, 0, testAttachmentTabIndex(sess))
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
	// A single pane remains: every direction is a benign no-neighbor move,
	// not an error the palette should ever surface (Task 10).
	require.ErrorIs(t, exec.FocusPaneLeft(), errNoNeighbor)
	require.ErrorIs(t, exec.FocusPaneRight(), errNoNeighbor)
	require.ErrorIs(t, exec.FocusPaneUp(), errNoNeighbor)
	require.ErrorIs(t, exec.FocusPaneDown(), errNoNeighbor)
	require.NoError(t, exec.CloseTab())
	require.Len(t, sess.tabs, 2)
	// Drain paints generated by methods above.
	for len(sends) > 0 {
		<-sends
	}
}
