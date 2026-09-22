package client

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/usecase/ui"
)

// Picker parity with main (Plan 003 B1-B4, C1). These port the behaviors the
// deleted daemon picker tests pinned on main (picker_interaction_test.go,
// remote_picker_test.go, picker_lines.go) onto the client-owned catalogue.

func pickerTestTab(id, name, detail string, attention bool) catalogue.RemoteCatalogTab {
	return catalogue.RemoteCatalogTab{ID: id, Name: name, Detail: detail, Attention: attention}
}

func pickerTestTabbed(name string, seed byte, state catalogue.RemoteCatalogSessionState, lastUsed uint64, active string, tabs ...catalogue.RemoteCatalogTab) catalogue.RemoteCatalogSession {
	session := pickerTestSession(name, seed, state)
	session.LastUsedSeq = lastUsed
	session.ActiveTabID = active
	for i := range tabs {
		tabs[i].Index = uint16(i)
	}
	session.Tabs = tabs
	return session
}

func pickerLinesOfKind(lines []protocol.PickerLine, kind protocol.PickerLineKind) []protocol.PickerLine {
	out := make([]protocol.PickerLine, 0)
	for _, line := range lines {
		if line.Kind == kind {
			out = append(out, line)
		}
	}
	return out
}

// pickerTabKey returns the key of the tab row labelled label that follows the
// session header labelled session.
func pickerTabKey(t *testing.T, lines []protocol.PickerLine, session, label string) string {
	t.Helper()
	inSession := false
	for _, line := range lines {
		switch line.Kind {
		case protocol.PickerLineSession, protocol.PickerLineHost, protocol.PickerLineSection:
			inSession = line.Kind == protocol.PickerLineSession && line.Label == session
		case protocol.PickerLineTab:
			if inSession && line.Label == label {
				return line.Key
			}
		}
	}
	t.Fatalf("no tab %q under session %q", label, session)
	return ""
}

// TestPickerCatalogueTabRows ports main's picker_lines.go tab rows: a session
// with tabs is a non-focusable header naming its tabs; each tab row is a
// destination with its pane-title detail and its own bell, and the header
// rings when any tab does.
func TestPickerCatalogueTabRows(t *testing.T) {
	now := time.Unix(1000, 0)
	navigateKill := protocol.PickerCanNavigate | protocol.PickerCanKill
	tests := []struct {
		name          string
		daemon        ports.BrokerDaemonObservation
		session       string
		wantHeaderBel bool
		wantTabs      []protocol.PickerLine // Label, Detail, Attention, Actions, Stopped
	}{
		{
			name: "live local session",
			daemon: pickerTestLocalObservation(now, pickerTestTabbed("work", 1, catalogue_Up, 3, "t2",
				pickerTestTab("t1", "shell", "(zsh)", false),
				pickerTestTab("t2", "build", "(make)", true),
				pickerTestTab("t3", "", "", false),
			)),
			session:       "work",
			wantHeaderBel: true,
			wantTabs: []protocol.PickerLine{
				{Label: "shell", Detail: "(zsh)", Actions: navigateKill},
				{Label: "build", Detail: "(make)", Attention: true, Actions: navigateKill},
				{Label: "3", Actions: navigateKill},
			},
		},
		{
			name: "stopped local session keeps its tabs",
			daemon: pickerTestLocalObservation(now, pickerTestTabbed("old", 2, catalogue_Down, 1, "",
				pickerTestTab("t1", "notes", "", false),
			)),
			session: "old",
			wantTabs: []protocol.PickerLine{
				{Label: "notes", Actions: navigateKill, Stopped: true},
			},
		},
		{
			name: "remote tabs navigate but never kill",
			daemon: pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestTabbed("remote-a", 3, catalogue_Up, 0, "r1",
				pickerTestTab("r1", "logs", "(tail)", false),
				pickerTestTab("", "anon", "", false),
			)),
			session: "remote-a",
			wantTabs: []protocol.PickerLine{
				{Label: "logs", Detail: "(tail)", Actions: protocol.PickerCanNavigate},
				// A live tab without a stable ID cannot be targeted exactly.
				{Label: "anon"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalogue, _ := pickerTestCatalogue(t)
			require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{tt.daemon}}))
			for _, lines := range [][]protocol.PickerLine{catalogue.Lines(), catalogue.RecentLines()} {
				header, ok := pickerLineByLabel(lines, tt.session)
				require.True(t, ok)
				require.Equal(t, protocol.PickerLineSession, header.Kind)
				require.False(t, header.Focusable, "a tabbed session header is not a destination")
				require.Zero(t, header.Actions)
				require.Equal(t, tt.wantHeaderBel, header.Attention, "the header rings when any tab does")

				tabs := pickerLinesOfKind(lines, protocol.PickerLineTab)
				require.Len(t, tabs, len(tt.wantTabs))
				for i, want := range tt.wantTabs {
					got := tabs[i]
					require.Equal(t, want.Label, got.Label)
					require.Equal(t, want.Detail, got.Detail)
					require.Equal(t, want.Attention, got.Attention)
					require.Equal(t, want.Actions, got.Actions, "tab %q actions", got.Label)
					require.Equal(t, want.Stopped, got.Stopped)
					require.True(t, got.Focusable, "every tab row is reachable")
					require.NotEqual(t, header.Key, got.Key)
				}
			}
		})
	}
}

// TestPickerCatalogueResolvesTabTargets pins exact tab targets: live and
// stopped-local tabs prefer their stable tab, a stopped remote tab restores
// through its exact selector (stable ID, else ordinal), and a tab that left
// the session is gone.
func TestPickerCatalogueResolvesTabTargets(t *testing.T) {
	now := time.Unix(1000, 0)
	lifecycle := pickerTestLifecycle(3)
	tests := []struct {
		name    string
		daemon  ports.BrokerDaemonObservation
		session string
		tab     string
		after   *ports.BrokerDaemonObservation
		want    attachmentTab
		wantErr pickerCatalogueErrorCode
	}{
		{
			name:    "live local tab",
			daemon:  pickerTestLocalObservation(now, pickerTestTabbed("work", 3, catalogue_Up, 1, "t1", pickerTestTab("t1", "shell", "", false), pickerTestTab("t2", "build", "", false))),
			session: "work", tab: "build",
			want: attachmentTab{preferred: "t2"},
		},
		{
			name:    "stopped local tab",
			daemon:  pickerTestLocalObservation(now, pickerTestTabbed("work", 3, catalogue_Down, 1, "", pickerTestTab("t1", "shell", "", false), pickerTestTab("t2", "build", "", false))),
			session: "work", tab: "build",
			want: attachmentTab{preferred: "t2"},
		},
		{
			name:    "live remote tab",
			daemon:  pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestTabbed("remote-a", 3, catalogue_Up, 0, "r1", pickerTestTab("r1", "logs", "", false), pickerTestTab("r2", "edit", "", false))),
			session: "remote-a", tab: "edit",
			want: attachmentTab{preferred: "r2"},
		},
		{
			name:    "stopped remote tab by stable id",
			daemon:  pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestTabbed("remote-a", 3, catalogue_Down, 0, "", pickerTestTab("r1", "logs", "", false), pickerTestTab("r2", "edit", "", false))),
			session: "remote-a", tab: "edit",
			want: attachmentTab{stopped: &protocol.SessionAttachTarget{LifecycleID: lifecycle, SessionName: "remote-a", TabID: "r2", TabIndex: protocol.NoTabIndex, Stopped: true}},
		},
		{
			name:    "stopped remote tab by ordinal",
			daemon:  pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestTabbed("remote-a", 3, catalogue_Down, 0, "", pickerTestTab("", "logs", "", false), pickerTestTab("", "edit", "", false))),
			session: "remote-a", tab: "edit",
			want: attachmentTab{stopped: &protocol.SessionAttachTarget{LifecycleID: lifecycle, SessionName: "remote-a", TabIndex: 1, TabRawName: "edit", TabExpectedCount: 2, Stopped: true}},
		},
		{
			name:    "tab closed after projection",
			daemon:  pickerTestLocalObservation(now, pickerTestTabbed("work", 3, catalogue_Up, 1, "t1", pickerTestTab("t1", "shell", "", false), pickerTestTab("t2", "build", "", false))),
			session: "work", tab: "build",
			after: func() *ports.BrokerDaemonObservation {
				o := pickerTestLocalObservation(now, pickerTestTabbed("work", 3, catalogue_Up, 1, "t1", pickerTestTab("t1", "shell", "", false)))
				return &o
			}(),
			wantErr: pickerCatalogueGone,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalogue, _ := pickerTestCatalogue(t)
			require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{tt.daemon}}))
			key := pickerTabKey(t, catalogue.Lines(), tt.session, tt.tab)
			if tt.after != nil {
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 2, Daemons: []ports.BrokerDaemonObservation{*tt.after}}))
			}
			request, tab, err := catalogue.ResolveTarget(key, pickerTestBase())
			if tt.wantErr != 0 {
				require.True(t, pickerCatalogueErrorIs(err, tt.wantErr), "want %v, got %v", tt.wantErr, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, ports.BrokerAdmissionExact, request.Admission)
			require.Equal(t, lifecycle, request.Target.LifecycleID)
			require.Equal(t, tt.want, tab)
		})
	}
}

// TestPickerCatalogueRecentAndGroupedProjections ports main's
// TestPickerSnapshotPublishesDistinctRecentAndGroupedProjections plus the MRU
// order of picker.go: Recent is flat (live by recency, then remote, then
// stopped by recency); Grouped is sectioned per daemon; both carry the same
// destinations and actions.
func TestPickerCatalogueRecentAndGroupedProjections(t *testing.T) {
	now := time.Unix(1000, 0)
	local := pickerTestLocalObservation(now,
		pickerTestTabbed("alpha", 1, catalogue_Up, 5, "", pickerTestTab("a1", "sh", "", false)),
		pickerTestTabbed("beta", 2, catalogue_Up, 9, "", pickerTestTab("b1", "sh", "", false)),
		pickerTestTabbed("gamma", 3, catalogue_Down, 7, ""),
		pickerTestTabbed("delta", 4, catalogue_Down, 8, ""),
	)
	remote := pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestTabbed("remote-a", 5, catalogue_Up, 99, "", pickerTestTab("r1", "sh", "", false)))
	catalogue, _ := pickerTestCatalogue(t)
	require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{local, remote}}))
	recent, grouped := catalogue.projections()

	tests := []struct {
		name         string
		lines        []protocol.PickerLine
		wantSessions []string
		wantSections []string
	}{
		{name: "recent", lines: recent.Lines, wantSessions: []string{"beta", "alpha", "remote-a", "delta", "gamma"}, wantSections: []string{}},
		{name: "grouped", lines: grouped.Lines, wantSessions: []string{"beta", "alpha", "delta", "gamma", "remote-a"}, wantSections: []string{"local", "user@arch"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantSessions, pickerSessionLabels(tt.lines))
			require.Equal(t, tt.wantSections, pickerSectionLabels(tt.lines))
		})
	}
	actions := func(lines []protocol.PickerLine) map[string]protocol.PickerLineActions {
		out := make(map[string]protocol.PickerLineActions)
		for _, line := range lines {
			if line.Key != "" {
				out[line.Key] = line.Actions
			}
		}
		return out
	}
	require.Equal(t, actions(recent.Lines), actions(grouped.Lines), "projections disagree on destination actions")
	require.NotEqual(t, recent.Lines, grouped.Lines, "both projections carry the same sort input")
	remoteRecent, ok := pickerLineByLabel(recent.Lines, "remote-a")
	require.True(t, ok)
	require.Contains(t, remoteRecent.Detail, "@user@arch", "a flat remote row names its origin")
	require.NoError(t, protocol.ValidatePickerSnapshot(protocol.PickerSnapshot{InteractionID: 1, SourceID: pickerCatalogueSourceID, SourceRevision: 1, Status: protocol.PickerSourceOK, Recent: recent, Grouped: grouped}))
}

// TestPickerCatalogueCursorStartsOnCurrent ports main's pickerSelectionMatches
// and default cursor: the attached tab, else the attached session's active
// tab, else the most recent session's active tab.
func TestPickerCatalogueCursorStartsOnCurrent(t *testing.T) {
	now := time.Unix(1000, 0)
	local := pickerTestLocalObservation(now,
		pickerTestTabbed("alpha", 1, catalogue_Up, 5, "a2", pickerTestTab("a1", "one", "", false), pickerTestTab("a2", "two", "", false)),
		pickerTestTabbed("beta", 2, catalogue_Up, 9, "b2", pickerTestTab("b1", "one", "", false), pickerTestTab("b2", "two", "", false)),
	)
	remote := pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestTabbed("remote-a", 5, catalogue_Up, 0, "r1", pickerTestTab("r1", "one", "", false), pickerTestTab("r2", "two", "", false)))
	tests := []struct {
		name        string
		current     pickerCurrent
		wantSession string
		wantTab     string
	}{
		{name: "no attachment: most recent active tab", wantSession: "beta", wantTab: "two"},
		{name: "attached tab", current: pickerCurrent{known: true, local: true, lifecycle: pickerTestLifecycle(1), tab: "a1"}, wantSession: "alpha", wantTab: "one"},
		{name: "attached session, unknown tab", current: pickerCurrent{known: true, local: true, lifecycle: pickerTestLifecycle(1)}, wantSession: "alpha", wantTab: "two"},
		{name: "attached remote tab", current: pickerCurrent{known: true, endpoint: "user@arch", lifecycle: pickerTestLifecycle(5), tab: "r2"}, wantSession: "remote-a", wantTab: "two"},
		{name: "other endpoint never matches", current: pickerCurrent{known: true, endpoint: "user@other", lifecycle: pickerTestLifecycle(5), tab: "r2"}, wantSession: "beta", wantTab: "two"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalogue, _ := pickerTestCatalogue(t)
			catalogue.SetCurrent(tt.current)
			require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{local, remote}}))
			recent, grouped := catalogue.projections()
			for _, projection := range []protocol.PickerProjection{recent, grouped} {
				want := pickerTabKey(t, projection.Lines, tt.wantSession, tt.wantTab)
				require.Equal(t, want, projection.Cursor.Key)
				require.Equal(t, want, projection.Lines[projection.Cursor.Index].Key)
			}
		})
	}
}

// TestPickerControllerOverlayOpensOnCurrentTab pins that SetCurrent restarts
// the presentation on the attached tab and drops a leftover search.
func TestPickerControllerOverlayOpensOnCurrentTab(t *testing.T) {
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{pickerTestLocalObservation(clock.Now(),
		pickerTestTabbed("alpha", 1, catalogue_Up, 1, "a1", pickerTestTab("a1", "one", "", false), pickerTestTab("a2", "two", "", false)),
		pickerTestTabbed("beta", 2, catalogue_Up, 9, "b1", pickerTestTab("b1", "one", "", false)),
	)}})
	require.True(t, controller.ConsumeTerminalRead([]byte("/on")))
	require.True(t, controller.modelSearchActive(t))

	controller.SetCurrent(pickerCurrent{known: true, local: true, lifecycle: pickerTestLifecycle(1), tab: "a2"})
	require.False(t, controller.modelSearchActive(t), "a fresh overlay starts without the previous search")
	require.Equal(t, pickerTabKey(t, controller.Catalogue().RecentLines(), "alpha", "two"), mustCursorKey(t, controller))
}

// TestPickerControllerSearchesTabNamesAndPaneTitles pins that tab rows are
// searchable by their tab name and their pane-title detail.
func TestPickerControllerSearchesTabNamesAndPaneTitles(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "tab name", query: "deploy", want: "deploy"},
		{name: "pane title", query: "htop", want: "monitor"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, clock := pickerTestController(t)
			controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{pickerTestLocalObservation(clock.Now(),
				pickerTestTabbed("work", 1, catalogue_Up, 1, "w1",
					pickerTestTab("w1", "shell", "(zsh)", false),
					pickerTestTab("w2", "deploy", "(ansible)", false),
					pickerTestTab("w3", "monitor", "(htop)", false),
				),
			)}})
			require.True(t, controller.ConsumeTerminalRead([]byte("/"+tt.query)))
			require.Equal(t, pickerTabKey(t, controller.Catalogue().RecentLines(), "work", tt.want), mustCursorKey(t, controller))
		})
	}
}

// TestPickerControllerSortToggleSwitchesProjection pins that `s` switches
// between the distinct Recent and Grouped projections.
func TestPickerControllerSortToggleSwitchesProjection(t *testing.T) {
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
		pickerTestLocalObservation(clock.Now(), pickerTestTabbed("alpha", 1, catalogue_Up, 5, ""), pickerTestTabbed("gamma", 3, catalogue_Down, 1, "")),
		pickerTestRemoteObservation("user@arch", 1, 1, clock.Now(), pickerTestTabbed("remote-a", 5, catalogue_Up, 0, "")),
	}})
	order := func() []string {
		frame := string(controller.Render(domain.Size{Cols: 100, Rows: 30}))
		positions := map[string]int{}
		for _, label := range []string{"alpha", "gamma", "remote-a"} {
			positions[label] = strings.Index(frame, label)
			require.GreaterOrEqual(t, positions[label], 0, "%q is rendered", label)
		}
		labels := []string{"alpha", "gamma", "remote-a"}
		for i := range labels {
			for j := i + 1; j < len(labels); j++ {
				if positions[labels[j]] < positions[labels[i]] {
					labels[i], labels[j] = labels[j], labels[i]
				}
			}
		}
		return labels
	}
	require.Equal(t, []string{"alpha", "remote-a", "gamma"}, order(), "recent: live, remote, stopped")
	require.True(t, controller.ConsumeTerminalRead([]byte("s")))
	require.Equal(t, []string{"alpha", "gamma", "remote-a"}, order(), "grouped: local section, then remote")
	require.True(t, controller.ConsumeTerminalRead([]byte("s")))
	require.Equal(t, []string{"alpha", "remote-a", "gamma"}, order())
}

// TestPickerControllerRendersBellsAndRepaintsOnPublication ports main's
// TestPickerSnapshotCarriesAttentionBellsToTheClient and
// TestAttentionPulseRepublishesTheOpenPickerSource: a bell raised by a later
// publication reaches the rendered picker.
func TestPickerControllerRendersBellsAndRepaintsOnPublication(t *testing.T) {
	controller, clock := pickerTestController(t)
	quiet := pickerTestLocalObservation(clock.Now(), pickerTestTabbed("work", 1, catalogue_Up, 1, "t1", pickerTestTab("t1", "shell", "", false)))
	controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{quiet}})
	bell := string(ui.AttentionGlyph)
	require.NotContains(t, string(controller.Render(domain.Size{Cols: 100, Rows: 30})), bell)

	ringing := pickerTestLocalObservation(clock.Now(), pickerTestTabbed("work", 1, catalogue_Up, 1, "t1", pickerTestTab("t1", "shell", "", true)))
	controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 2, Daemons: []ports.BrokerDaemonObservation{ringing}})
	controller.invalidatePresentation()
	frame := string(controller.Render(domain.Size{Cols: 100, Rows: 30}))
	require.Equal(t, 2, strings.Count(frame, bell), "the header and its tab row both ring")
}

// TestPickerCatalogueResolveKill pins which rows `x` may destroy: local live
// and stopped sessions (by exact lifecycle and name), never a remote row, and
// never a lifecycle that left or was replaced.
func TestPickerCatalogueResolveKill(t *testing.T) {
	now := time.Unix(1000, 0)
	tests := []struct {
		name        string
		daemon      ports.BrokerDaemonObservation
		label       string
		after       *ports.BrokerDaemonObservation
		wantStopped bool
		wantErr     pickerCatalogueErrorCode
	}{
		{name: "live local", daemon: pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)), label: "alpha"},
		{name: "stopped local deletes history", daemon: pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Down)), label: "alpha", wantStopped: true},
		{name: "broken local deletes history", daemon: pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Broken)), label: "alpha", wantStopped: true},
		{name: "remote is refused", daemon: pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("alpha", 1, catalogue_Up)), label: "alpha", wantErr: pickerCatalogueUnavailable},
		{
			name: "gone", daemon: pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)), label: "alpha",
			after:   func() *ports.BrokerDaemonObservation { o := pickerTestLocalObservation(now); return &o }(),
			wantErr: pickerCatalogueGone,
		},
		{
			name: "replaced by the same name", daemon: pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up)), label: "alpha",
			after: func() *ports.BrokerDaemonObservation {
				o := pickerTestLocalObservation(now, pickerTestSession("alpha", 2, catalogue_Up))
				return &o
			}(),
			wantErr: pickerCatalogueReplaced,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalogue, _ := pickerTestCatalogue(t)
			require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{tt.daemon}}))
			line, ok := pickerLineByLabel(catalogue.Lines(), tt.label)
			require.True(t, ok)
			if tt.after != nil {
				require.True(t, catalogue.Apply(ports.BrokerSnapshot{Epoch: 3, Revision: 2, Daemons: []ports.BrokerDaemonObservation{*tt.after}}))
			}
			target, err := catalogue.ResolveKill(line.Key)
			if tt.wantErr != 0 {
				require.True(t, pickerCatalogueErrorIs(err, tt.wantErr), "want %v, got %v", tt.wantErr, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, BrokerOperationRoute{Epoch: 3, Destination: ports.BrokerEndpointFence{Local: true}}, target.route)
			require.NoError(t, target.route.Validate())
			require.Equal(t, tt.label, target.name)
			require.Equal(t, tt.wantStopped, target.stopped)
		})
	}
}

// TestPickerControllerKillKeyWakesSupervisor pins that `x` wakes the
// supervisor with the killed row's key, only on a row that authorises it, and
// types into an active search instead.
func TestPickerControllerKillKeyWakesSupervisor(t *testing.T) {
	tests := []struct {
		name     string
		daemon   func(time.Time) ports.BrokerDaemonObservation
		input    string
		wantKill bool
		wantKey  bool
	}{
		{name: "local row", daemon: func(now time.Time) ports.BrokerDaemonObservation {
			return pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up))
		}, input: "x", wantKill: true, wantKey: true},
		{name: "remote row authorises no kill", daemon: func(now time.Time) ports.BrokerDaemonObservation {
			return pickerTestRemoteObservation("user@arch", 1, 1, now, pickerTestSession("alpha", 1, catalogue_Up))
		}, input: "x", wantKill: true},
		{name: "search types x", daemon: func(now time.Time) ports.BrokerDaemonObservation {
			return pickerTestLocalObservation(now, pickerTestSession("alpha", 1, catalogue_Up))
		}, input: "/x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, clock := pickerTestController(t)
			controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{tt.daemon(clock.Now())}})
			require.True(t, controller.ConsumeTerminalRead([]byte(tt.input)))
			if tt.wantKill {
				select {
				case <-controller.OpsReady():
				default:
					t.Fatal("a kill decision must wake the supervisor")
				}
			}
			op, key := controller.TakeOp()
			require.Equal(t, tt.wantKill, op.kill)
			if tt.wantKey {
				require.Equal(t, pickerRowKeyByLabel(t, controller, "alpha"), key)
			} else {
				require.Empty(t, key)
			}
		})
	}
}

// TestPickerControllerHostFailureToastOncePerEpisode ports main's
// TestRemoteFailureNoticeEmittedOncePerFailureEpisode and
// TestRemoteFailureNoticesKeepEndpointsDistinct.
func TestPickerControllerHostFailureToastOncePerEpisode(t *testing.T) {
	controller, clock := pickerTestController(t)
	failing := func(endpoint string, seed byte, episode uint64, kind domain.RemoteFailureKind) ports.BrokerDaemonObservation {
		o := pickerTestRemoteObservation(endpoint, seed, seed, clock.Now())
		o.Availability = domain.RemoteAvailabilityUnreachable
		o.ConsecutiveFailures = 1
		o.FailureEpisode = episode
		o.LastFailure = domain.RemoteFailure{Kind: kind}
		return o
	}
	revision := ports.BrokerRevision(0)
	apply := func(daemons ...ports.BrokerDaemonObservation) []ui.ActiveToast {
		revision++
		controller.mu.Lock()
		controller.notices.Clear()
		controller.mu.Unlock()
		controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: revision, Daemons: daemons})
		controller.mu.Lock()
		defer controller.mu.Unlock()
		return controller.notices.Active(clock.Now())
	}

	toasts := apply(failing("user@arch", 1, 1, domain.RemoteFailureTrust))
	require.Len(t, toasts, 1)
	require.Contains(t, toasts[0].Message, "SSH host verification failed")
	require.Contains(t, toasts[0].Message, "user@arch")
	require.Empty(t, apply(failing("user@arch", 1, 1, domain.RemoteFailureTrust)), "the same episode toasts once")
	require.Len(t, apply(failing("user@arch", 1, 2, domain.RemoteFailureTrust)), 1, "a new episode toasts again")

	recovered := pickerTestRemoteObservation("user@arch", 1, 1, clock.Now().Add(time.Second))
	recovered.FailureEpisode = 2
	require.Empty(t, apply(recovered))
	require.Len(t, apply(failing("user@arch", 1, 3, domain.RemoteFailureTimeout)), 1, "an outage after recovery toasts again")

	toasts = apply(failing("user@arch", 1, 3, domain.RemoteFailureTimeout), failing("user@mule", 2, 1, domain.RemoteFailureAuthentication))
	require.Len(t, toasts, 1, "only the newly failing endpoint toasts")
	require.Contains(t, toasts[0].Message, "user@mule")
}

// TestPickerControllerRefusesFailingRemoteInstantly pins B4: a commit on a
// remote host the broker observed failing refuses at once with main's notice
// instead of dialing, while a stale-but-reachable host stays attemptable.
func TestPickerControllerRefusesFailingRemoteInstantly(t *testing.T) {
	tests := []struct {
		name         string
		availability domain.RemoteAvailability
		wantNotice   string
	}{
		{name: "unreachable", availability: domain.RemoteAvailabilityUnreachable, wantNotice: "Remote session unavailable: remote-a@user@arch — host unreachable"},
		{name: "authentication", availability: domain.RemoteAvailabilityAuthFailed, wantNotice: "Remote session unavailable: remote-a@user@arch — authentication failed"},
		{name: "reachable", availability: domain.RemoteAvailabilityReachable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, clock := pickerTestController(t)
			o := pickerTestRemoteObservation("user@arch", 1, 1, clock.Now().Add(-time.Hour), pickerTestSession("remote-a", 3, catalogue_Up))
			o.Availability = tt.availability
			controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{o}})
			controller.mu.Lock()
			controller.notices.Clear()
			controller.mu.Unlock()

			_, _, err := controller.ResolveKeyTarget(pickerRowKeyByLabel(t, controller, "remote-a"), pickerTestBase())
			if tt.wantNotice == "" {
				require.NoError(t, err)
				return
			}
			require.True(t, pickerCatalogueErrorIs(err, pickerCatalogueUnavailable))
			controller.mu.Lock()
			active := controller.notices.Active(clock.Now())
			controller.mu.Unlock()
			require.Len(t, active, 1)
			require.Equal(t, tt.wantNotice, active[0].Message)
		})
	}
}

// TestSessionAttachmentHelloCarriesTab pins that a committed tab reaches the
// daemon: a stable tab is preferred, a stopped remote tab is restored through
// its exact selector, and a session-level commit carries neither.
func TestSessionAttachmentHelloCarriesTab(t *testing.T) {
	stopped := protocol.SessionAttachTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "alpha", TabIndex: 1, TabRawName: "edit", TabExpectedCount: 2, Stopped: true}
	tests := []struct {
		name          string
		local         bool
		tab           attachmentTab
		wantPreferred domain.TabStableID
		wantTarget    *protocol.SessionAttachTarget
	}{
		{name: "session level", local: true},
		{name: "local tab", local: true, tab: attachmentTab{preferred: "t_two"}, wantPreferred: "t_two"},
		{name: "remote live tab", tab: attachmentTab{preferred: "t_two"}, wantPreferred: "t_two"},
		{name: "remote stopped tab", tab: attachmentTab{stopped: &stopped}, wantTarget: &stopped},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := sessionTestRequest(tt.local)
			provenance := SessionEnvironmentRemote
			if tt.local {
				provenance = SessionEnvironmentLocalPicker
			}
			worker, err := newSessionAttachmentWorker(sessionAttachmentConfig{Request: request, Geometry: domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, SessionEnvironment: SessionEnvironment{Provenance: provenance}, Tab: tt.tab})
			require.NoError(t, err)
			hello := worker.hello(newSessionTestStream())
			require.Equal(t, tt.wantPreferred, hello.PreferredTabID)
			require.Equal(t, tt.wantTarget, hello.SessionTarget)
			require.NoError(t, protocol.ValidateHello(hello))
		})
	}
}
