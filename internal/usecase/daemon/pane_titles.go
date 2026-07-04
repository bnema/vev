package daemon

import (
	"path/filepath"
	"strings"

	"github.com/bnema/vev/internal/usecase/layout"
)

func (d *Daemon) refreshPaneTitle(sess *session, id layout.PaneID) string {
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
	title := fallback
	if d.procComm != nil && p.pty != nil {
		if pgid, err := foregroundPgid(p.pty); err == nil && pgid > 0 {
			if comm, err := d.procComm(pgid); err == nil && strings.TrimSpace(comm) != "" {
				title = strings.TrimSpace(comm)
			}
		}
	}
	p.mu.Lock()
	p.title = title
	p.mu.Unlock()
	return title
}

func foregroundPgid(pty interface{ ForegroundPgid() (int, error) }) (pgid int, err error) {
	defer func() {
		if recover() != nil {
			pgid = 0
		}
	}()
	return pty.ForegroundPgid()
}
