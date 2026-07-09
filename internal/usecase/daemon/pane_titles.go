package daemon

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/bnema/vev/internal/usecase/layout"
)

const paneTitleCacheTTL = time.Second

// paneTitleState is guarded by pane.mu. generation changes whenever the
// displayed title can change, allowing cached composition to damage only a
// title row rather than the entire stacked layout.
type paneTitleState struct {
	processName      string
	processNameAt    time.Time
	processNameValid bool
	terminalTitle    string
	generation       uint64
}

func formatPaneTitle(processName, terminalTitle, fallback string) string {
	if processName != "" && terminalTitle != "" {
		return processName + ": " + terminalTitle
	}
	if processName != "" {
		return processName
	}
	if terminalTitle != "" {
		return terminalTitle
	}
	return fallback
}

func (p *pane) formattedTitleLocked(fallback string) string {
	return formatPaneTitle(p.title.processName, p.title.terminalTitle, fallback)
}

// refreshTerminalTitleLocked synchronizes the title retained by the VT parser.
// It must be called while p.mu is held, including by ptyReader around Write.
func (p *pane) refreshTerminalTitleLocked() {
	title := p.screen.TerminalTitle()
	if p.title.terminalTitle != title {
		p.title.terminalTitle = title
		p.title.generation++
	}
}

func (d *Daemon) paneTitleFallback() string {
	fallback := filepath.Base(d.shell)
	if fallback == "." || fallback == string(filepath.Separator) || fallback == "" {
		return d.shell
	}
	return fallback
}

func (d *Daemon) refreshPaneTitle(sess *session, id layout.PaneID) string {
	return d.refreshPaneTitleCached(sess, id, false)
}

func (d *Daemon) refreshPaneTitleOnFocus(sess *session, id layout.PaneID) string {
	return d.refreshPaneTitleCached(sess, id, true)
}

// refreshPaneTitleCached retains the ID lookup used by normal layout panes.
func (d *Daemon) refreshPaneTitleCached(sess *session, id layout.PaneID, force bool) string {
	fallback := d.paneTitleFallback()
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
	return d.refreshPaneDisplayTitle(sess, p, force)
}

// refreshPaneDisplayTitle refreshes only the process-name portion of p's
// display title. It deliberately releases pane.mu before querying the PTY so
// callers that already render under pane locking cannot recurse on that lock.
func (d *Daemon) refreshPaneDisplayTitle(_ *session, p *pane, force bool) string {
	fallback := d.paneTitleFallback()
	if p == nil {
		return fallback
	}
	now := d.clock.Now()
	p.mu.Lock()
	if !force && p.title.processNameValid && now.Sub(p.title.processNameAt) < paneTitleCacheTTL {
		title := p.formattedTitleLocked(fallback)
		p.mu.Unlock()
		return title
	}
	p.mu.Unlock()

	processName := fallback
	if d.procComm != nil && p.pty != nil {
		if pgid, err := p.pty.ForegroundPgid(); err == nil && pgid > 0 {
			if comm, err := d.procComm(pgid); err == nil && strings.TrimSpace(comm) != "" {
				processName = strings.TrimSpace(comm)
			}
		}
	}

	p.mu.Lock()
	if p.title.processName != processName {
		p.title.processName = processName
		p.title.generation++
	}
	p.title.processNameAt = now
	p.title.processNameValid = true
	title := p.formattedTitleLocked(fallback)
	p.mu.Unlock()
	return title
}
