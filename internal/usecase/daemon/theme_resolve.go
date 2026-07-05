package daemon

import "github.com/bnema/vev/internal/usecase/theme"

func (d *Daemon) applyHostTheme(sess *session, ac *attachedClient, t theme.Theme) bool {
	sess.mu.Lock()
	if ac != nil {
		if sess.client != ac {
			sess.mu.Unlock()
			return false
		}
	} else if sess.client != nil {
		sess.mu.Unlock()
		return false
	}
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()

	if ac != nil {
		ac.setTheme(t)
	}
	for _, tb := range tabs {
		tb.mu.Lock()
		panes := tb.panesSnapshot()
		tb.mu.Unlock()
		for _, p := range panes {
			p.mu.Lock()
			applyPaneThemeLocked(p, t, ac == nil)
			p.mu.Unlock()
		}
	}
	return true
}

func applyPaneThemeLocked(p *pane, t theme.Theme, clearUnknownScheme bool) {
	p.screen.SetDefaultColors(t.Foreground, t.Background, t.HasFG && t.HasBG)
	if t.SchemeKnown {
		p.screen.SetColorScheme(t.Light)
	} else if clearUnknownScheme {
		p.screen.ClearColorScheme()
	}
}
