// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/picker"
)

func (d *Daemon) movePickerSourceError(source moveSourceLocator) error {
	if d == nil || source.Session.ID == "" || source.TabID == "" {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Source session is no longer available.", errMovePaneInvalid)
	}
	sess := d.sessionByID(source.Session.ID)
	if sess == nil || sess.incarnation != source.Session.Incarnation {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Source session is no longer available.", errMovePaneInvalid)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if source.Client != nil && sess.client != source.Client {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Source client is no longer active.", errMovePaneInvalid)
	}
	tb := findMoveTabLocked(sess, source.TabID)
	if tb == nil {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Tab no longer exists.", errMovePaneInvalid)
	}
	if source.PaneID == "" {
		return nil
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if paneByStableIDLocked(tb, string(source.PaneID)) == nil {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Pane no longer exists.", errMovePaneInvalid)
	}
	return nil
}

type pickerIntent uint8

const (
	pickerNavigate pickerIntent = iota
	pickerMovePane
	pickerMoveTab
)

type moveSessionLocator struct {
	ID          domain.SessionID
	Incarnation domain.IncarnationID
	Name        string
}

type moveSourceLocator struct {
	Session moveSessionLocator
	TabID   domain.TabStableID
	PaneID  domain.PaneStableID
	Client  *attachedClient
}

// enterPickerForIntent returns errNoMoveDestination only for move intents.
func (d *Daemon) enterPickerForIntent(sess *session, ac *attachedClient, intent pickerIntent, source moveSourceLocator) error {
	model := d.newPickerModel(sess, intent, source, picker.SourceFilter{})
	if intent != pickerNavigate {
		if _, ok := model.Selected(); !ok {
			return errNoMoveDestination
		}
	}

	d.publishPicker(sess, ac, model, intent, source)
	return nil
}

func pickerSelectionMode(intent pickerIntent) picker.SelectionMode {
	switch intent {
	case pickerMovePane:
		return picker.SelectMovePaneTab
	case pickerMoveTab:
		return picker.SelectMoveTabSession
	default:
		return picker.SelectNavigationTab
	}
}

func (d *Daemon) commitMovePickerSelection(intent pickerIntent, source moveSourceLocator, target picker.Target) error {
	destination := moveSessionLocator{ID: target.Session, Incarnation: target.Incarnation, Name: target.Name}
	switch intent {
	case pickerMovePane:
		if target.TabID == "" {
			return errMovePaneInvalid
		}
		return d.movePane(movePaneRequest{
			Source:           source.Session,
			SourceTabID:      source.TabID,
			SourcePaneID:     source.PaneID,
			Destination:      destination,
			DestinationTabID: target.TabID,
		})
	case pickerMoveTab:
		return d.moveTab(moveTabRequest{
			Source:      source.Session,
			SourceTabID: source.TabID,
			Destination: destination,
		})
	default:
		return errMovePaneInvalid
	}
}

func (d *Daemon) previewTarget(target picker.Target, intent pickerIntent) (*session, *tab) {
	d.mu.Lock()
	sess := d.sessions[target.Session]
	if sess == nil {
		d.mu.Unlock()
		return nil, nil
	}
	sess.mu.Lock()
	d.mu.Unlock()
	defer sess.mu.Unlock()
	if sess.incarnation != target.Incarnation || !targetMatchesLifecycle(target, sess.name, sess.createdAt) {
		return nil, nil
	}
	if intent == pickerMoveTab {
		if sess.active < 0 || sess.active >= len(sess.tabs) {
			return nil, nil
		}
		return sess, sess.tabs[sess.active]
	}
	if target.TabID != "" {
		for _, tb := range sess.tabs {
			if domain.TabStableID(tb.stableID) == target.TabID {
				return sess, tb
			}
		}
		return nil, nil
	}
	if target.TabIndex < 0 || target.TabIndex >= len(sess.tabs) {
		return nil, nil
	}
	return sess, sess.tabs[target.TabIndex]
}

type pickerRefreshOptions struct {
	// preserveSelection keeps the currently selected target selected across the
	// rebuild even for navigate intent (used by the sort toggle).
	preserveSelection bool
	// nearestRow, when >= 0, selects the row occupying this index after the
	// rebuild (used after deletes). -1 disables it.
	nearestRow int
}

func (d *Daemon) refreshPicker(ac *attachedClient) {
	d.refreshPickerOpts(ac, pickerRefreshOptions{nearestRow: -1})
}

func (d *Daemon) refreshPickerOpts(ac *attachedClient, opts pickerRefreshOptions) {
	sess := ac.currentSession()
	if sess == nil {
		return
	}
	rt := ac.overlays
	rt.pickerMu.Lock()
	if rt.picker == nil {
		rt.pickerMu.Unlock()
		return
	}
	intent, source := rt.pickerIntent, rt.pickerSource
	current := picker.SourceFilter{}
	if intent != pickerNavigate || opts.preserveSelection {
		selected, _ := rt.picker.Selected()
		current = picker.SourceFilter{Session: selected.Session, Incarnation: selected.Incarnation, TabID: selected.TabID}
	}
	rt.pickerMu.Unlock()
	model := d.newPickerModel(sess, intent, source, current)
	if intent != pickerNavigate {
		if _, ok := model.Selected(); !ok {
			d.closePicker(ac)
			return
		}
	}
	rt.pickerMu.Lock()
	updated := rt.picker != nil && rt.pickerIntent == intent && rt.pickerSource == source
	if updated {
		rt.picker = model
		rt.pickerTitle = pickerTitle(pickerSortMode(d.pickerSort.Load()))
	}
	rt.pickerMu.Unlock()
	if updated {
		d.registerPreviewForSelection(ac)
	}
}
