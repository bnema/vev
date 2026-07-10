package daemon

import "github.com/bnema/vev/internal/usecase/theme"

func (d *Daemon) applyHostTheme(sess *session, ac *attachedClient, t theme.Theme, clearUnknownScheme bool) bool {
	if ac != nil {
		ac.sendMu.Lock()
		defer ac.sendMu.Unlock()
	}
	sess.themeMu.Lock()
	defer sess.themeMu.Unlock()
	return d.applyHostThemeLocked(sess, ac, t, clearUnknownScheme)
}

func (d *Daemon) applyHostThemeLocked(sess *session, ac *attachedClient, t theme.Theme, clearUnknownScheme bool) bool {
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
		ac.composed.invalidate()
	}
	for _, tb := range tabs {
		tb.mu.Lock()
		panes := tb.panesSnapshot()
		if floating := tb.floating.pane; floating != nil {
			panes = append(panes, floating)
		}
		tb.mu.Unlock()
		for _, p := range panes {
			p.mu.Lock()
			applyPaneThemeLocked(p, t, clearUnknownScheme)
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
