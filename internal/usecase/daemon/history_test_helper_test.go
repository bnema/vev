package daemon

import vt "github.com/bnema/vev-vt"

func newTestHistory(rows int) *vt.History { return vt.NewHistory(vt.HistoryConfig{MaxRows: rows}) }

func installTestHistory(p *pane, config vt.HistoryConfig) {
	p.screen = vt.NewScreenWithHistory(p.screen.Frame.Width, p.screen.Frame.Height, config)
	p.history = p.screen.History()
}
