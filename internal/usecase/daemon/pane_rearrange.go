package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

var errDaemonActionNoChange = errors.New("daemon action made no change")

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
		return domain.UserWarn(domain.NoticeLayoutTooSmall, "pane rearrangement requires a column layout", err)
	case errors.Is(err, layout.ErrTooSmall):
		return domain.UserWarn(domain.NoticeLayoutTooSmall, "not enough space to rearrange pane", err)
	default:
		return err
	}
}
