package daemon

import (
	"context"
	"strings"
	"testing"
	"time"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/require"
)

func TestHistoryPolicyReloadIncludesHiddenFloatingPanes(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "history", "tab", "pane")
	tb := sess.tabs[0]
	primary := tb.focusedPane()
	floating := newPane("floating", nil, domain.Size{Cols: 80, Rows: 24})
	installTestFloating(tb, floating, false)
	panes := []*pane{primary, floating}
	views := make([]vt.HistoryView, len(panes))
	for i, p := range panes {
		for _, text := range []string{"a", "b", "c"} {
			appendHistoryRow(t, p.history, testRow(text))
		}
		views[i] = p.history.View()
	}
	cfg := domain.Defaults()
	cfg.Scrollback = domain.ScrollbackConfig{Megabytes: 1, Lines: 2}
	d.ApplyConfig(cfg)
	for i, p := range panes {
		require.Equal(t, 2, p.history.Len())
		require.Equal(t, uint64(1_000_000), p.history.ByteCap())
		require.Equal(t, "b", cellsString(p.history.View().Row(0)))
		require.Equal(t, 3, views[i].Len(), "reload must preserve captured documents")
	}
	cfg.Scrollback.Megabytes = 0
	d.ApplyConfig(cfg)
	for _, p := range panes {
		require.Zero(t, p.history.Len())
		require.Zero(t, p.history.Cap())
	}
	cfg.Scrollback = domain.ScrollbackConfig{Megabytes: 1}
	d.ApplyConfig(cfg)
	created := newPaneWithStableIDAndTitle("new", "stable-new", nil, domain.Size{Cols: 80, Rows: 24}, "shell", d.currentHistoryConfig())
	require.Zero(t, created.history.Cap())
	require.Equal(t, uint64(1_000_000), created.history.ByteCap())
}

func TestHistoryCompressionSkipsBusyPanesAndRemainsBounded(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "history", "tab", "pane")
	p := sess.tabs[0].focusedPane()
	installTestHistory(p, vt.HistoryConfig{MaxRows: 64, MaxBytes: 1 << 20, ChunkRows: 16})
	for range 48 {
		appendHistoryRow(t, p.history, testRow(strings.Repeat("a", 80)))
	}
	before := p.history.View()
	p.mu.Lock()
	d.compressIdleHistories() // TryLock must not wait for the reader's lock.
	p.mu.Unlock()
	require.Zero(t, p.history.CompressionStats().ColdPages)
	for range 4 {
		d.compressIdleHistories()
	}
	require.Equal(t, 2, p.history.CompressionStats().ColdPages)
	require.Equal(t, 'a', before.Cell(0, 0).Rune)
	require.Equal(t, 48, p.history.Len())
}

func TestHistoryMaintenanceStopsItsTimerOnCancellation(t *testing.T) {
	clock := portsmocks.NewMockClock(t)
	timer := portsmocks.NewMockTimer(t)
	clock.EXPECT().NewTimer(historyMaintenanceInterval).Return(timer).Once()
	timer.EXPECT().C().Return(make(<-chan time.Time)).Once()
	timer.EXPECT().Stop().Return(true).Once()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	d := &Daemon{clock: clock}
	d.historyMaintenance(ctx)
}
