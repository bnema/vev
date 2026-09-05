package daemon

import (
	"context"
	"time"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
)

func historyConfigFor(policy domain.ScrollbackConfig) vt.HistoryConfig {
	if policy.Megabytes == 0 {
		return vt.HistoryConfig{}
	}
	return vt.HistoryConfig{MaxBytes: policy.Megabytes * 1_000_000, MaxRows: policy.Lines}
}

func (d *Daemon) currentHistoryConfig() vt.HistoryConfig {
	policy := d.scrollbackConfig.Load()
	if policy == nil {
		return historyConfigFor(domain.DefaultScrollbackConfig())
	}
	return historyConfigFor(*policy)
}

// historyPanes gathers references without holding a session/tab lock while
// touching history. Include hidden floating panes; deduplicate a pane that moves
// between tabs while the separate session snapshots are being collected.
func (d *Daemon) historyPanes() []*pane {
	var panes []*pane
	seen := make(map[*pane]struct{})
	for _, sess := range d.sessionsSnapshot() {
		sess.mu.Lock()
		tabs := append([]*tab(nil), sess.tabs...)
		sess.mu.Unlock()
		for _, tb := range tabs {
			if tb == nil {
				continue
			}
			tb.mu.Lock()
			current := tb.panesSnapshot()
			if tb.floating.pane != nil {
				current = append(current, tb.floating.pane)
			}
			tb.mu.Unlock()
			for _, p := range current {
				if p == nil {
					continue
				}
				if _, exists := seen[p]; exists {
					continue
				}
				seen[p] = struct{}{}
				panes = append(panes, p)
			}
		}
	}
	return panes
}

func (d *Daemon) applyHistoryLimitsToPane(p *pane) {
	p.mu.Lock()
	var err error
	if p.history != nil {
		// Load inside the pane lock so an older concurrent reload cannot install
		// a stale policy after a newer reload has already published its value.
		err = p.history.SetLimits(d.currentHistoryConfig())
	}
	p.mu.Unlock()
	if err != nil {
		d.log.Warn("cannot apply scrollback limits", "err", err)
	}
}

func (d *Daemon) applyHistoryLimits() {
	for _, p := range d.historyPanes() {
		d.applyHistoryLimitsToPane(p)
	}
}

// Keep background compression infrequent and bounded; it must not compete
// with interactive output when many panes retain history.
const historyMaintenanceInterval = 5 * time.Second

// historyMaintenance is application scheduling, not a VT-owned worker. It
// visits one history page per pane per interval and skips busy/resizing panes.
func (d *Daemon) historyMaintenance(ctx context.Context) {
	timer := d.clock.NewTimer(historyMaintenanceInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C():
			d.compressIdleHistories()
			timer.Reset(historyMaintenanceInterval)
		}
	}
}

func (d *Daemon) compressIdleHistories() {
	for _, p := range d.historyPanes() {
		if !p.mu.TryLock() {
			continue
		}
		var err error
		if p.history != nil && !p.resizeApplying {
			_, err = p.history.CompressIdle(1)
		}
		p.mu.Unlock()
		if err != nil {
			d.log.Warn("cannot compress idle scrollback", "err", err)
		}
	}
}
