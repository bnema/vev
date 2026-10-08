package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	themeui "github.com/bnema/vev/internal/usecase/theme"
)

// ptyReader runs one pane reader for tests that register it with sessWg.Add
// first. Production launches readPanePTY through sessWg.Go.
func (d *Daemon) ptyReader(sess *session, tb *tab, p *pane) {
	defer d.sessWg.Done()
	if p != nil && p.ownerSnapshot() == nil && sess != nil && tb != nil {
		publishPaneOwner(p, sess, tb, 0)
	}
	d.readPanePTY(p)
}

// processPTYData feeds bytes through the pane's VT parsing path.
func (d *Daemon) processPTYData(_ *session, _ *tab, p *pane, data []byte, bufferDuringApply bool) {
	d.processPanePTYData(p, data, bufferDuringApply)
}

// setThemeForTest publishes a complete applied snapshot for tests that do not
// exercise daemon configuration.
func (ac *attachedClient) setThemeForTest(t themeui.Theme) {
	ac.setAppliedTheme(appliedTheme{Raw: t, Resolved: themeui.Resolve(t, domain.ThemeAccent{Mode: domain.ThemeAccentAuto})})
}

// sideEffect builds output without advancing the state stream, under the view
// lock.
func (s *attachmentOutput) sideEffect(data []byte, echoAck uint64) (protocol.Output, error) {
	s.lockView()
	defer s.unlockView()
	return s.sideEffectLocked(data, echoAck)
}

// outstanding reports state frames sent but not yet acknowledged.
func (s *attachmentOutput) outstanding() uint64 {
	if s == nil {
		return 0
	}
	s.lockView()
	defer s.unlockView()
	return s.next - s.acked
}
