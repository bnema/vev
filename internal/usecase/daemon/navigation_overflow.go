package daemon

import (
	"sort"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
)

type overflowKind uint8

const (
	overflowNone overflowKind = iota
	overflowTabs
	overflowSessions
)

type overflowStep struct {
	kind  overflowKind
	delta int
}

// resolveOverflow is the pure escalation policy for directional navigation.
// The caller supplies its position on the selected axis, so disabled axes and
// either hard wall all resolve to the zero step.
func resolveOverflow(dir layout.Direction, cfg domain.NavConfig, position, count int) overflowStep {
	var step overflowStep
	switch dir {
	case layout.Left:
		if cfg.OverflowTabs {
			step = overflowStep{kind: overflowTabs, delta: -1}
		}
	case layout.Right:
		if cfg.OverflowTabs {
			step = overflowStep{kind: overflowTabs, delta: 1}
		}
	case layout.Up:
		if cfg.OverflowSessions {
			step = overflowStep{kind: overflowSessions, delta: -1}
		}
	case layout.Down:
		if cfg.OverflowSessions {
			step = overflowStep{kind: overflowSessions, delta: 1}
		}
	}
	if step.kind == overflowNone || count < 2 || position < 0 || position >= count || position+step.delta < 0 || position+step.delta >= count {
		return overflowStep{}
	}
	return step
}

// overflowSourceEligible validates that directional overflow still originates
// from the active tiled tab. The session-to-tab lock order matches tab overflow
// commit and leaves no lock held across a session handoff.
func overflowSourceEligible(sess *session, ac *attachedClient, expectedSource *tab) bool {
	if sess == nil || expectedSource == nil {
		return false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if ac == nil || !attachmentRegisteredLocked(sess, ac) || domain.TabStableID(expectedSource.stableID) != ac.viewSnapshot().tabID {
		return false
	}
	expectedSource.mu.Lock()
	defer expectedSource.mu.Unlock()
	return expectedSource.floating.state != floatingVisible
}

type sessionOverflowSnapshot struct {
	session *session
	id      domain.SessionID
	name    string
}

// prepareSessionOverflow snapshots only the live registry entries, then sorts
// immutable names exactly like the picker. No daemon or session lock survives
// the snapshot, sorting, or the later switchToTarget call.
func (d *Daemon) prepareSessionOverflow(current *session, dir layout.Direction, cfg domain.NavConfig) (picker.Target, bool) {
	if current == nil || !cfg.OverflowSessions || (dir != layout.Up && dir != layout.Down) {
		return picker.Target{}, false
	}
	current.mu.Lock()
	currentID := current.id
	current.mu.Unlock()

	d.mu.Lock()
	live := sessionsSnapshot(d.sessions)
	d.mu.Unlock()

	snapshots := make([]sessionOverflowSnapshot, 0, len(live))
	for _, sess := range live {
		sess.mu.Lock()
		snapshot := sessionOverflowSnapshot{session: sess, id: sess.id, name: sess.name}
		sess.mu.Unlock()
		snapshots = append(snapshots, snapshot)
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].name < snapshots[j].name })

	position := -1
	for i, snapshot := range snapshots {
		if snapshot.session == current && snapshot.id == currentID {
			position = i
			break
		}
	}
	step := resolveOverflow(dir, cfg, position, len(snapshots))
	if step.kind != overflowSessions {
		return picker.Target{}, false
	}
	target := snapshots[position+step.delta]
	return picker.Target{Session: target.id, TabIndex: -1}, true
}

// commitSessionOverflow carries its source constraint into the handoff so tab
// identity and floating visibility are revalidated while source ownership is
// transferred under the daemon -> routing -> session -> tab lock order.
func (d *Daemon) commitSessionOverflow(from *session, ac *attachedClient, expectedSource *tab, target picker.Target) error {
	if from == nil || ac == nil || expectedSource == nil {
		return errNoNeighbor
	}
	return d.switchToTargetGuarded(from, ac, target, sessionHandoffGuard{expectedSource: expectedSource})
}

type tabOverflowCandidate struct {
	source         *tab
	target         *tab
	sourceIndex    int
	delta          int
	direction      layout.Direction
	span           domain.Rect
	entry          layout.PaneID
	entryPane      *pane
	targetOldFocus layout.PaneID
}

func (d *Daemon) prepareTabOverflowForAttachment(sess *session, ac *attachedClient, expectedSource *tab, dir layout.Direction, span domain.Rect, delta int) (tabOverflowCandidate, bool) {
	if sess == nil || expectedSource == nil || (delta != -1 && delta != 1) {
		return tabOverflowCandidate{}, false
	}
	sess.mu.Lock()
	if ac != nil && (!attachmentRegisteredLocked(sess, ac) || domain.TabStableID(expectedSource.stableID) != ac.viewSnapshot().tabID) {
		sess.mu.Unlock()
		return tabOverflowCandidate{}, false
	}
	sourceIndex := -1
	for i, tb := range sess.tabs {
		if tb == expectedSource {
			sourceIndex = i
			break
		}
	}
	if sourceIndex < 0 {
		sess.mu.Unlock()
		return tabOverflowCandidate{}, false
	}
	targetIndex := sourceIndex + delta
	if targetIndex < 0 || targetIndex >= len(sess.tabs) {
		sess.mu.Unlock()
		return tabOverflowCandidate{}, false
	}
	source := sess.tabs[sourceIndex]
	target := sess.tabs[targetIndex]
	sess.mu.Unlock()

	source.mu.Lock()
	floatingVisible := source.floating.state == floatingVisible
	source.mu.Unlock()
	if floatingVisible {
		return tabOverflowCandidate{}, false
	}

	target.mu.Lock()
	if target.tree == nil {
		target.mu.Unlock()
		return tabOverflowCandidate{}, false
	}
	area := domain.Rect{Width: target.size.Cols, Height: target.size.Rows}
	entry, err := target.tree.EntryPane(dir, span, area)
	entryPane := target.panes[entry]
	oldFocus := target.tree.Focus
	valid := err == nil && entryPane != nil && layout.ContainsLeaf(target.tree.Root, entry)
	target.mu.Unlock()
	if !valid {
		return tabOverflowCandidate{}, false
	}
	return tabOverflowCandidate{
		source:         source,
		target:         target,
		sourceIndex:    sourceIndex,
		delta:          delta,
		direction:      dir,
		span:           span,
		entry:          entry,
		entryPane:      entryPane,
		targetOldFocus: oldFocus,
	}, true
}

func (d *Daemon) commitTabOverflowForAttachment(sess *session, ac *attachedClient, candidate tabOverflowCandidate) bool {
	if sess == nil || candidate.source == nil || candidate.target == nil {
		return false
	}
	sess.mu.Lock()
	targetIndex := candidate.sourceIndex + candidate.delta
	if (ac != nil && (!attachmentRegisteredLocked(sess, ac) || domain.TabStableID(candidate.source.stableID) != ac.viewSnapshot().tabID)) || candidate.sourceIndex < 0 || candidate.sourceIndex >= len(sess.tabs) || targetIndex < 0 || targetIndex >= len(sess.tabs) ||
		sess.tabs[candidate.sourceIndex] != candidate.source || sess.tabs[targetIndex] != candidate.target {
		sess.mu.Unlock()
		return false
	}

	candidate.source.mu.Lock()
	if candidate.source.floating.state == floatingVisible {
		candidate.source.mu.Unlock()
		sess.mu.Unlock()
		return false
	}
	candidate.target.mu.Lock()
	if candidate.target.tree == nil {
		candidate.target.mu.Unlock()
		candidate.source.mu.Unlock()
		sess.mu.Unlock()
		return false
	}
	area := domain.Rect{Width: candidate.target.size.Cols, Height: candidate.target.size.Rows}
	entry, err := candidate.target.tree.EntryPane(candidate.direction, candidate.span, area)
	if err != nil || entry != candidate.entry || candidate.target.panes[entry] != candidate.entryPane || !layout.ContainsLeaf(candidate.target.tree.Root, entry) {
		candidate.target.mu.Unlock()
		candidate.source.mu.Unlock()
		sess.mu.Unlock()
		return false
	}
	targetTree := candidate.target.tree.Clone()
	if err := targetTree.FocusEnter(candidate.direction, candidate.span, area); err != nil {
		candidate.target.mu.Unlock()
		candidate.source.mu.Unlock()
		sess.mu.Unlock()
		return false
	}
	candidate.target.tree = targetTree
	candidate.target.bumpLayoutGenerationLocked()
	candidate.target.mu.Unlock()
	candidate.source.mu.Unlock()
	sess.mu.Unlock()
	sess.selectAttachmentTab(ac, domain.TabStableID(candidate.target.stableID))
	d.applyTabLayout(sess, candidate.target)
	return true
}
