package daemon

import (
	"context"
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/picker"
)

// remotePickerSelection is one generation-fenced picker activation. It keeps
// the local attachment capability until the remote link has completed its
// validated proxied handshake; no remote endpoint is ever published to the
// thin client from this path.
type remotePickerSelection struct {
	model      *picker.Model
	generation uint64
	token      attachmentConnectionToken
	target     domain.RemoteSessionTarget
	ctx        context.Context
	cancel     context.CancelFunc
}

func (selection *remotePickerSelection) current() bool {
	if selection == nil || selection.token.ac == nil || selection.token.ac.overlays == nil {
		return false
	}
	rt := selection.token.ac.overlays
	rt.pickerMu.Lock()
	current := rt.pickerRemoteSelection == selection &&
		rt.picker == selection.model &&
		rt.pickerGeneration == selection.generation
	rt.pickerMu.Unlock()
	return current
}

func (d *Daemon) startRemotePickerSelection(token attachmentConnectionToken, target picker.Target, guard sessionHandoffGuard, action string) error {
	if d == nil || token.ac == nil || token.localSession() == nil || token.ac.overlays == nil || !guard.closePicker || !token.attachmentEffectCurrent() {
		return errAttachmentTransition
	}
	if target.RemoteTarget == nil {
		return errAttachmentTransition
	}
	remoteTarget := *target.RemoteTarget
	if remoteTarget.Validate() != nil || target.Stopped != remoteTarget.Stopped || !d.remotePickerTargetReadyTarget(remoteTarget) {
		return errAttachmentTransition
	}
	if target.RemoteKey != nil && (target.Session != target.RemoteKey.ID() || target.RemoteKey.Host != remoteTarget.Endpoint || target.RemoteKey.Name != remoteTarget.SessionName || target.RemoteKey.LifecycleID != remoteTarget.LifecycleID) {
		return errAttachmentTransition
	}

	ctx, cancel := context.WithCancel(context.Background())
	selection := &remotePickerSelection{
		token:  token,
		target: remoteTarget,
		ctx:    ctx,
		cancel: cancel,
	}
	rt := token.ac.overlays
	rt.pickerMu.Lock()
	if rt.picker == nil || rt.pickerRemoteSelection != nil {
		rt.pickerMu.Unlock()
		cancel()
		return errAttachmentTransition
	}
	selection.model = rt.picker
	selection.generation = rt.pickerGeneration
	rt.pickerRemoteSelection = selection
	rt.pickerTitle = " Sessions · connecting (Esc cancels) "
	rt.pickerPending = nil
	rt.pickerESC.stop()
	rt.pickerMu.Unlock()

	if token.effect != nil {
		token.effect.bindActionEnd(d, action)
	}
	d.invalidateRender(token.localSession(), token.ac, true, "remote_picker_selection.go")
	go d.completeRemotePickerSelection(selection)
	return nil
}

func (d *Daemon) completeRemotePickerSelection(selection *remotePickerSelection) {
	if selection == nil || selection.token.ac == nil {
		return
	}
	defer selection.cancel()
	view, err := d.openRemoteView(selection.ctx, selection.target, selection.token.ac.sizeSnapshot())
	if err != nil {
		d.failRemotePickerSelection(selection, err)
		return
	}
	if err := d.activateRemoteViewTab(selection.ctx, view, selection.target); err != nil {
		d.parkRemoteViewWarm(view)
		d.failRemotePickerSelection(selection, err)
		return
	}
	if !selection.current() {
		d.parkRemoteViewWarm(view)
		return
	}

	published, err := d.transitionToRemoteViewForPicker(selection.token, view, selection)
	if err != nil {
		d.parkRemoteViewWarm(view)
		d.failRemotePickerSelection(selection, err)
		return
	}
	// The transition freezes the initiating attachment before its final picker
	// generation check. Closing this exact picker afterwards cannot hand a
	// canceled or replacement picker to the remote owner.
	d.closePickerIfCurrent(selection.token.ac, selection.model, selection.generation)
	d.firstPaintForTransition(published)
}

func (d *Daemon) failRemotePickerSelection(selection *remotePickerSelection, err error) {
	if selection == nil || selection.token.ac == nil || selection.token.ac.overlays == nil {
		return
	}
	rt := selection.token.ac.overlays
	rt.pickerMu.Lock()
	current := rt.pickerRemoteSelection == selection &&
		rt.picker == selection.model &&
		rt.pickerGeneration == selection.generation
	if current {
		rt.pickerRemoteSelection = nil
		rt.pickerTitle = pickerTitle(pickerSortMode(d.pickerSort.Load()))
	}
	rt.pickerMu.Unlock()
	if !current {
		return
	}
	selection.cancel()
	if !errors.Is(err, context.Canceled) {
		d.log.Warn("remote picker selection failed", "endpoint", selection.target.Endpoint, "session", selection.target.SessionName, "err", err)
		if selection.token.current() {
			d.notify(selection.token.localSession(), domain.NoticeWarn, domain.NoticeSessionUnavailable, "couldn't connect to remote session", nil)
		}
	}
	if source := selection.token.localSession(); source != nil && selection.token.current() {
		d.invalidateRender(source, selection.token.ac, true, "remote_picker_selection.go")
	}
}
