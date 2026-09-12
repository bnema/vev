package daemon

import (
	"context"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// newTestTabWithContext builds one tab wired to a caller-owned context, so a
// test can cancel the tab and its panes together.
func newTestTabWithContext(p ports.PTY, ctx context.Context, cancel context.CancelFunc) *tab {
	tb := newTab(p, domain.Size{Cols: 80, Rows: 23})
	tb.ctx, tb.cancel = ctx, cancel
	for _, pane := range tb.panes {
		pane.ctx, pane.cancel = ctx, cancel
	}
	return tb
}
