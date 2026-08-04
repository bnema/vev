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
	displayFallback  string
	generation       uint64
}

func formatPaneTitle(processName, terminalTitle, fallback string) string {
	if processName != "" && terminalTitle != "" {
		prefix := processName + ": "
		if strings.HasPrefix(terminalTitle, prefix) {
			return terminalTitle
		}
		return prefix + terminalTitle
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

func (p *pane) displayTitleLocked() string {
	return p.formattedTitleLocked(p.title.displayFallback)
}

// setDisplayFallback updates the fallback owned by a pane. It must not damage
// cached composition unless that fallback changes the displayed title.
func (p *pane) setDisplayFallback(fallback string) {
	p.mu.Lock()
	if p.title.displayFallback == fallback {
		p.mu.Unlock()
		return
	}
	oldTitle := p.displayTitleLocked()
	p.title.displayFallback = fallback
	if oldTitle != p.displayTitleLocked() {
		p.title.generation++
	}
	p.mu.Unlock()
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

// composeTabTitle renders "name (paneTitle)". An empty paneTitle, or one equal
// to the tab name, collapses to just the name.
func composeTabTitle(tabName, paneTitle string) string {
	return tabName + tabTitleDetail(tabName, paneTitle)
}

// tabTitleDetail returns the parenthesized " (paneTitle)" suffix for
// tabName/paneTitle, or "" when paneTitle is empty or equals tabName. This is
// the single place the name/detail composition rule lives; composeTabTitle
// and the picker's TabEntry.Detail both build on it.
func tabTitleDetail(tabName, paneTitle string) string {
	if paneTitle == "" || paneTitle == tabName {
		return ""
	}
	return " (" + paneTitle + ")"
}

// focusedPaneTitle returns the focused pane's display title for tab labels.
// Caller must hold the owning session.mu (tb.mu then pane.mu obey lock order).
// When includeTerminalTitle is false the OSC title is omitted; the process
// name (or fallback) still shows.
func (tb *tab) focusedPaneTitle(includeTerminalTitle bool) string {
	tb.mu.Lock()
	p := tb.focusedPane()
	tb.mu.Unlock()
	if p == nil {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if includeTerminalTitle {
		return p.displayTitleLocked()
	}
	return formatPaneTitle(p.title.processName, "", p.title.displayFallback)
}

// refreshSessionFocusedTitles opportunistically refreshes the process-name half
// of every tab's focused pane. Snapshot panes under locks, then refresh with no
// lock held (refreshPaneDisplayTitle re-enters p.mu itself). TTL-throttled.
func (d *Daemon) refreshSessionFocusedTitles(sess *session) {
	if sess == nil {
		return
	}
	fallback := d.paneTitleFallback()
	sess.mu.Lock()
	panes := make([]*pane, 0, len(sess.tabs))
	for _, tb := range sess.tabs {
		tb.mu.Lock()
		if p := tb.focusedPane(); p != nil {
			panes = append(panes, p)
		}
		tb.mu.Unlock()
	}
	sess.mu.Unlock()
	for _, p := range panes {
		p.setDisplayFallback(fallback)
		d.refreshPaneDisplayTitle(sess, p, false)
	}
}

func (d *Daemon) paneTitleFallback() string {
	fallback := filepath.Base(d.shell)
	if fallback == "." || fallback == string(filepath.Separator) || fallback == "" {
		return d.shell
	}
	return fallback
}

func (d *Daemon) refreshPaneTitle(sess *session, id layout.PaneID, owningTab ...*tab) string {
	return d.refreshPaneTitleCached(sess, id, false, owningTab...)
}

func (d *Daemon) refreshPaneTitleOnFocus(sess *session, id layout.PaneID, owningTab ...*tab) string {
	return d.refreshPaneTitleCached(sess, id, true, owningTab...)
}

// refreshPaneTitleCached resolves the pane in its owning tab. A caller with a
// current tab supplies it explicitly; direct callers refresh every matching
// tab because layout pane IDs are local to a tab and may repeat.
func (d *Daemon) refreshPaneTitleCached(sess *session, id layout.PaneID, force bool, owningTab ...*tab) string {
	fallback := d.paneTitleFallback()
	if sess == nil {
		return fallback
	}
	tabs := owningTab
	if len(tabs) == 0 || tabs[0] == nil {
		sess.mu.Lock()
		tabs = append([]*tab(nil), sess.tabs...)
		sess.mu.Unlock()
	}
	title := fallback
	for _, tb := range tabs {
		if tb == nil {
			continue
		}
		tb.mu.Lock()
		p := tb.panes[id]
		tb.mu.Unlock()
		if p == nil {
			continue
		}
		p.setDisplayFallback(fallback)
		title = d.refreshPaneDisplayTitle(sess, p, force)
	}
	return title
}

// refreshPaneDisplayTitle refreshes only the process-name portion of p's
// display title. It deliberately releases pane.mu before querying the PTY so
// callers that already render under pane locking cannot recurse on that lock.
func (d *Daemon) refreshPaneDisplayTitle(_ *session, p *pane, force bool) string {
	if p == nil {
		return d.paneTitleFallback()
	}
	now := d.clock.Now()
	p.mu.Lock()
	if !force && p.title.processNameValid && now.Sub(p.title.processNameAt) < paneTitleCacheTTL {
		title := p.displayTitleLocked()
		p.mu.Unlock()
		return title
	}
	p.mu.Unlock()

	// A failed lookup means no process name was discovered. Keep that state
	// distinct from the display fallback: an OSC title must then stand alone,
	// and future floating panes can supply their command fallback at format time.
	processName := ""
	if d.procComm != nil && p.pty != nil {
		if pgid, err := p.pty.ForegroundPgid(); err == nil && pgid > 0 {
			if comm, err := d.procComm(pgid); err == nil && strings.TrimSpace(comm) != "" {
				processName = strings.TrimSpace(comm)
			}
		}
	}

	p.mu.Lock()
	oldTitle := p.displayTitleLocked()
	p.title.processName = processName
	p.title.processNameAt = now
	p.title.processNameValid = true
	title := p.displayTitleLocked()
	if oldTitle != title {
		p.title.generation++
	}
	p.mu.Unlock()
	return title
}
