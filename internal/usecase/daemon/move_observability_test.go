package daemon

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestMoveTabLogsCommitAndSourceAttachmentTermination(t *testing.T) {
	var logs bytes.Buffer
	d, source, _, _ := newManualSessionWithPTYs(t, newQuietPTY())
	d.log = slog.New(slog.NewJSONHandler(&logs, nil))

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

	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	require.NoError(t, d.moveTab(moveTabRequest{
		Source:      moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
		SourceTabID: domain.TabStableID(sourceTab.stableID),
		Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
	}))
	d.waitNotifies()

	output := logs.String()
	require.Contains(t, output, `"msg":"tab move requested"`)
	require.Contains(t, output, `"msg":"move committed"`)
	require.Contains(t, output, `"operation":"tab"`)
	require.Contains(t, output, `"source_retired":true`)
	require.Contains(t, output, `"msg":"move detaching source attachments"`)
	require.Contains(t, output, `"reason":"session-killed"`)
	require.Contains(t, output, `"msg":"client detached"`)
	require.Contains(t, output, `"reason":1`)
}

func TestMoveTabLogsNormalizedRejection(t *testing.T) {
	var logs bytes.Buffer
	d := New(nil, stubClock{}, slog.New(slog.NewJSONHandler(&logs, nil)))

	err := d.moveTab(moveTabRequest{
		Source:      moveSessionLocator{ID: "missing", Incarnation: domain.IncarnationID{1}},
		SourceTabID: "tab",
		Destination: moveSessionLocator{ID: "also-missing", Incarnation: domain.IncarnationID{2}},
	})

	require.ErrorIs(t, err, errMovePaneInvalid)
	output := logs.String()
	require.Contains(t, output, `"msg":"tab move requested"`)
	require.Contains(t, output, `"msg":"tab move rejected"`)
	require.Contains(t, output, `"source_session_id":"missing"`)
	require.Contains(t, output, `"destination_session_id":"also-missing"`)
}
