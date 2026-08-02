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
	sess, ok := localSession(d.sessionByID(source.Session.ID))
	if !ok || sess.incarnation != source.Session.Incarnation {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Source session is no longer available.", errMovePaneInvalid)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if source.Client != nil && !attachmentRegisteredLocked(sess, source.Client) {
		return domain.UserWarn(domain.NoticeSessionUnavailable, "Source attachment is no longer active.", errMovePaneInvalid)
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
	if intent != pickerNavigate && !sess.capabilities().yieldsMoves() {
		return errSessionCannotYieldMoves
	}
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
			Client:           source.Client,
			Source:           source.Session,
			SourceTabID:      source.TabID,
			SourcePaneID:     source.PaneID,
			Destination:      destination,
			DestinationTabID: target.TabID,
		})
	case pickerMoveTab:
		return d.moveTab(moveTabRequest{
			Client:      source.Client,
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
	sess, ok := localSession(d.sessions[target.Session])
	if !ok {
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
		if len(sess.tabs) == 0 {
			return nil, nil
		}
		return sess, sess.tabs[0]
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
	observedModel, observedGeneration := rt.picker, rt.pickerGeneration
	current := picker.SourceFilter{}
	if intent != pickerNavigate || opts.preserveSelection {
		cursor, _ := rt.picker.Cursor()
		current = picker.SourceFilter{
			Session: cursor.Session, Incarnation: cursor.Incarnation, TabID: cursor.TabID, RemoteKey: cursor.RemoteKey,
		}
	}
	rt.pickerMu.Unlock()
	model := d.newPickerModel(sess, intent, source, current)
	if rt.afterPickerRefreshBuild != nil {
		rt.afterPickerRefreshBuild(model)
	}
	if intent != pickerNavigate {
		if _, ok := model.Selected(); !ok {
			d.closePickerIfCurrent(ac, observedModel, observedGeneration)
			return
		}
	}
	// Navigate-only: move intents keep the selection-or-close logic above
	// authoritative, so a stale row index can never override it.
	if intent == pickerNavigate && opts.nearestRow >= 0 {
		model.SelectNearestRow(opts.nearestRow)
	}
	rt.pickerMu.Lock()
	updated := rt.picker == observedModel && rt.pickerGeneration == observedGeneration && rt.pickerIntent == intent && rt.pickerSource == source
	if updated {
		rt.picker = model
		rt.pickerTitle = pickerTitle(pickerSortMode(d.pickerSort.Load()))
	}
	rt.pickerMu.Unlock()
	if updated {
		d.registerPreviewForSelection(ac)
	}
}
