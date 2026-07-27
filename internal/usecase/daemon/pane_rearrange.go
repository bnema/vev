package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

var errDaemonActionNoChange = errors.New("daemon action made no change")

// errPaneRearrangeNoop names the pane-rearrange outcome for package-level
// callers while adapters normalize the shared daemon no-change sentinel.
var errPaneRearrangeNoop = errDaemonActionNoChange

func (d *Daemon) consumeOrExpelPane(target daemonActionTarget, direction layout.Direction) error {
	changed, err := d.mutateTargetLayoutChanged(target, true, func(candidate *layout.Tree, area domain.Rect) (bool, error) {
		return candidate.ConsumeOrExpelPane(target.pane.id, direction, area)
	})
	if err != nil {
		return paneRearrangeUserError(err)
	}
	if !changed {
		return errDaemonActionNoChange
	}
	return nil
}

func paneRearrangeUserError(err error) error {
	switch {
	case errors.Is(err, layout.ErrUnsupportedColumnLayout):
		return domain.UserWarn(domain.NoticeLayoutTooSmall, "pane cannot be rearranged in this layout", err)
	case errors.Is(err, layout.ErrTooSmall):
		return domain.UserWarn(domain.NoticeLayoutTooSmall, "pane cannot be rearranged at this size", err)
	default:
		return err
	}
}
