package daemon

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

type moveObservabilityFixture struct {
	daemon         *Daemon
	source         *session
	sourceTab      *tab
	sourcePane     *pane
	destination    *session
	destinationTab *tab
	logs           *bytes.Buffer
}

func newMoveObservabilityFixture(t *testing.T) moveObservabilityFixture {
	t.Helper()
	logs := &bytes.Buffer{}
	d, source, _, _ := newManualSessionWithPTYs(t, newQuietPTY())
	d.log = slog.New(slog.NewJSONHandler(logs, nil))

	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	sourcePane := sourceTab.focusedPane()
	sourcePane.stableID = "source-pane"

	destinationCtx, destinationCancel := context.WithCancel(d.serveCtx)
	t.Cleanup(destinationCancel)
	destinationTab := newTabWithStableID("destination-tab", "destination-pane", newQuietPTY(), domain.Size{Cols: 80, Rows: 23})
	destinationTab.ctx, destinationTab.cancel = context.WithCancel(destinationCtx)
	destination := &session{
		sessionCore: sessionCore{id: "destination", name: "destination", incarnation: domain.IncarnationID{9}},
		ctx:         destinationCtx,
		cancel:      destinationCancel,
		tabs:        []*tab{destinationTab},
	}
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()

	return moveObservabilityFixture{
		daemon: d, source: source, sourceTab: sourceTab, sourcePane: sourcePane,
		destination: destination, destinationTab: destinationTab, logs: logs,
	}
}

func TestMoveLogsCommitAndSourceAttachmentTermination(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		move      func(moveObservabilityFixture) error
		assertLog func(*testing.T, string)
	}{
		{
			name:      "tab",
			operation: "tab",
			move: func(f moveObservabilityFixture) error {
				return f.daemon.moveTab(moveTabRequest{
					Source:      moveSessionLocator{ID: f.source.id, Incarnation: f.source.incarnation},
					SourceTabID: domain.TabStableID(f.sourceTab.stableID),
					Destination: moveSessionLocator{ID: f.destination.id, Incarnation: f.destination.incarnation},
				})
			},
			assertLog: func(t *testing.T, output string) {
				t.Helper()
				require.Contains(t, output, `"msg":"tab move requested"`)
			},
		},
		{
			name:      "pane",
			operation: "pane",
			move: func(f moveObservabilityFixture) error {
				return f.daemon.movePane(movePaneRequest{
					Source:           moveSessionLocator{ID: f.source.id, Incarnation: f.source.incarnation},
					SourceTabID:      domain.TabStableID(f.sourceTab.stableID),
					SourcePaneID:     domain.PaneStableID(f.sourcePane.stableID),
					Destination:      moveSessionLocator{ID: f.destination.id, Incarnation: f.destination.incarnation},
					DestinationTabID: domain.TabStableID(f.destinationTab.stableID),
				})
			},
			assertLog: func(t *testing.T, output string) {
				t.Helper()
				require.Contains(t, output, `"msg":"pane move requested"`)
				require.Contains(t, output, `"source_pane_id":"source-pane","destination_tab_id":"destination-tab"`)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newMoveObservabilityFixture(t)
			require.NoError(t, tt.move(fixture))
			fixture.daemon.waitNotifies()

			output := fixture.logs.String()
			tt.assertLog(t, output)
			require.Contains(t, output, `"msg":"move committed"`)
			require.Contains(t, output, `"operation":"`+tt.operation+`"`)
			require.Contains(t, output, `"source_retired":true`)
			require.Contains(t, output, `"msg":"move detaching source attachments"`)
			require.Contains(t, output, `"reason":"session-killed"`)
			require.Contains(t, output, `"msg":"client detached"`)
			require.Contains(t, output, `"reason":1`)
		})
	}
}

func TestMoveLogsNormalizedRejection(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		move      func(*Daemon) error
		fields    []string
	}{
		{
			name:      "tab",
			operation: "tab",
			move: func(d *Daemon) error {
				return d.moveTab(moveTabRequest{
					Source:      moveSessionLocator{ID: "missing", Incarnation: domain.IncarnationID{1}},
					SourceTabID: "tab",
					Destination: moveSessionLocator{ID: "also-missing", Incarnation: domain.IncarnationID{2}},
				})
			},
			fields: []string{
				`"source_session_id":"missing"`,
				`"destination_session_id":"also-missing"`,
			},
		},
		{
			name:      "pane",
			operation: "pane",
			move: func(d *Daemon) error {
				return d.movePane(movePaneRequest{
					Source:           moveSessionLocator{ID: "missing", Incarnation: domain.IncarnationID{1}},
					SourceTabID:      "source-tab",
					SourcePaneID:     "source-pane",
					Destination:      moveSessionLocator{ID: "also-missing", Incarnation: domain.IncarnationID{2}},
					DestinationTabID: "destination-tab",
				})
			},
			fields: []string{
				`"source_pane_id":"source-pane"`,
				`"destination_tab_id":"destination-tab"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			d := New(nil, stubClock{}, slog.New(slog.NewJSONHandler(&logs, nil)))

			require.ErrorIs(t, tt.move(d), errMovePaneInvalid)
			output := logs.String()
			require.Contains(t, output, `"msg":"`+tt.operation+` move requested"`)
			require.Contains(t, output, `"msg":"`+tt.operation+` move rejected"`)
			for _, field := range tt.fields {
				require.Contains(t, output, field)
			}
		})
	}
}
