package daemon

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/bnema/vev/internal/usecase/layout"
)

const paneTitleCacheTTL = time.Second

func (d *Daemon) refreshPaneTitle(sess *session, id layout.PaneID) string {
	return d.refreshPaneTitleCached(sess, id, false)
}

func (d *Daemon) refreshPaneTitleOnFocus(sess *session, id layout.PaneID) string {
	return d.refreshPaneTitleCached(sess, id, true)
}

func (d *Daemon) refreshPaneTitleCached(sess *session, id layout.PaneID, force bool) string {
	fallback := filepath.Base(d.shell)
	if fallback == "." || fallback == string(filepath.Separator) || fallback == "" {
		fallback = d.shell
	}
	if sess == nil {
		return fallback
	}
	tb := sess.activeTab()
	if tb == nil {
		return fallback
	}
	tb.mu.Lock()
	p := tb.panes[id]
	tb.mu.Unlock()
	if p == nil {
		return fallback
	}
	now := d.clock.Now()
	p.mu.Lock()
	if !force && p.titleValid && now.Sub(p.titleAt) < paneTitleCacheTTL {
		title := p.title
		p.mu.Unlock()
		if title == "" {
			return fallback
		}
		return title
	}
	p.mu.Unlock()
	title := fallback
	if d.procComm != nil && p.pty != nil {
		if pgid, err := p.pty.ForegroundPgid(); err == nil && pgid > 0 {
			if comm, err := d.procComm(pgid); err == nil && strings.TrimSpace(comm) != "" {
				title = strings.TrimSpace(comm)
			}
		}
	}
	p.mu.Lock()
	p.title = title
	p.titleAt = now
	p.titleValid = true
	p.mu.Unlock()
	return title
}
