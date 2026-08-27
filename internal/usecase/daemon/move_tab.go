package daemon

import (
	"context"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

// moveTabTopology is the private whole-Tab topology Implementation consumed by
// the shared moveTransaction Module.
type moveTabTopology struct {
	req         moveTabRequest
	admission   moveTabAdmission
	fencedPanes []*pane
}

type moveTabAdmission struct {
	tab                *tab
	sourceIndex        int
	layoutGeneration   uint64
	floatingState      floatingState
	floatingPane       *pane
	floatingGeneration uint64
	panes              []*pane
	destinationActive  *tab
	destinationSize    domain.Size
	finalSource        bool
}

type moveTabCommit struct {
	req         moveTabRequest
	source      *session
	destination *session
	admission   moveTabAdmission
}

func (d *Daemon) moveTab(req moveTabRequest) error {
	return d.executeMove(&moveTabTopology{req: req})
}

func (p *moveTabTopology) transactionRequest() moveTransactionRequest {
	req := p.req
	return moveTransactionRequest{
		operation:            "tab",
		attachment:           req.Attachment,
		attachmentCapability: req.AttachmentCapability,
		source:               req.Source,
		destination:          req.Destination,
		logAttrs: []any{
			"source_session_id", req.Source.ID,
			"source_tab_id", req.SourceTabID,
			"destination_session_id", req.Destination.ID,
		},
	}
}

func (p *moveTabTopology) validRequest() bool {
	return p != nil && p.req.SourceTabID != "" && p.req.Source.ID != p.req.Destination.ID
}

// admitLocked runs with daemon and ordered Session locks held. It snapshots
// whole-Tab policy without publishing topology or Pane ownership.
func (p *moveTabTopology) admitLocked(_ *Daemon, source, destination *session) error {
	if source == destination {
		return errMovePaneInvalid
	}
	moved := findMoveTabLocked(source, p.req.SourceTabID)
	if moved == nil || len(destination.tabs) == 0 {
		return errMoveStaleTarget
	}
	destinationActive := destination.tabs[0]
	destinationActive.mu.Lock()
	destinationSize := destinationActive.size
	destinationActive.mu.Unlock()

	moved.mu.Lock()
	defer moved.mu.Unlock()
	if !moved.floatingTransferableLocked() {
		return errMoveFloatingWarming
	}
	if moved.tree == nil || moved.tree.Root == nil {
		return errMovePaneInvalid
	}
	if _, ok := layout.Solve(moved.tree.Root, domain.Rect{Width: destinationSize.Cols, Height: destinationSize.Rows}); !ok {
		return errMoveTooSmall
	}
	panes := make([]*pane, 0, len(moved.panes)+1)
	for _, pane := range moved.panes {
		panes = append(panes, pane)
	}
	if moved.floating.pane != nil && (moved.floating.state == floatingHidden || moved.floating.state == floatingVisible) {
		panes = append(panes, moved.floating.pane)
	}
	p.admission = moveTabAdmission{
		tab: moved, sourceIndex: indexMoveTabLocked(source, moved),
		layoutGeneration: moved.layoutGeneration, floatingState: moved.floating.state,
		floatingPane: moved.floating.pane, floatingGeneration: moved.floating.generation,
		panes: panes, destinationActive: destinationActive, destinationSize: destinationSize,
		finalSource: len(source.tabs) == 1,
	}
	return nil
}

func (p *moveTabTopology) afterAdmission(d *Daemon) {
	if d.afterMoveTabSourceSnapshot != nil {
		d.afterMoveTabSourceSnapshot()
	}
}

func (p *moveTabTopology) willRetireSource() bool {
	return p != nil && p.admission.finalSource
}

func (p *moveTabTopology) resizeFences(source, destination *session) *moveResizeFences {
	fences := newMoveTabResizeFences(source, destination, p.admission.tab)
	p.fencedPanes = fences.panes
	return fences
}

func (p *moveTabTopology) publishLocked(d *Daemon, source, destination *session) (moveTopologyPublication, error) {
	commit := moveTabCommit{req: p.req, source: source, destination: destination, admission: p.admission}
	return commit.publishTopologyLocked(d, p.fencedPanes)
}

// publishTopologyLocked performs every whole-Tab validation before changing
// membership, Tab context, or Pane owners. The shared transaction owns common
// Session retirement and persistence capture after this method succeeds.
func (c *moveTabCommit) publishTopologyLocked(d *Daemon, fencedPanes []*pane) (moveTopologyPublication, error) {
	moved := c.admission.tab
	if indexMoveTabLocked(c.source, moved) != c.admission.sourceIndex || findMoveTabLocked(c.destination, c.req.SourceTabID) != nil ||
		(len(c.source.tabs) == 1) != c.admission.finalSource {
		return moveTopologyPublication{}, errMoveStaleTarget
	}
	if len(c.destination.tabs) == 0 || c.destination.tabs[0] != c.admission.destinationActive {
		return moveTopologyPublication{}, errMoveStaleTarget
	}
	c.admission.destinationActive.mu.Lock()
	destinationSizeCurrent := c.admission.destinationActive.size == c.admission.destinationSize
	c.admission.destinationActive.mu.Unlock()
	if !destinationSizeCurrent {
		return moveTopologyPublication{}, errMoveStaleTarget
	}

	moved.mu.Lock()
	defer moved.mu.Unlock()
	if moved.layoutGeneration != c.admission.layoutGeneration || !moved.floatingTransferableLocked() ||
		moved.floating.state != c.admission.floatingState || moved.floating.pane != c.admission.floatingPane ||
		moved.floating.generation != c.admission.floatingGeneration || !sameMoveTabPanesLocked(moved, c.admission.panes, fencedPanes) {
		return moveTopologyPublication{}, errMoveStaleTarget
	}
	for _, pane := range fencedPanes {
		pane.mu.Lock()
		defer pane.mu.Unlock()
		owner := pane.ownerSnapshot()
		if owner == nil || owner.session != c.source || owner.tab != moved {
			return moveTopologyPublication{}, errMoveStaleTarget
		}
	}

	// Everything above is fallible. Membership, context, and Pane owner writes
	// below form one in-memory publication and cannot become a Move rejection.
	idx := c.admission.sourceIndex
	if len(c.source.tabs) > 1 {
		c.source.prepareAttachmentViewsForRemovedTabLocked(moved, idx)
	}
	c.source.tabs = append(c.source.tabs[:idx], c.source.tabs[idx+1:]...)
	c.destination.tabs = append(c.destination.tabs, moved)
	moved.size = c.admission.destinationSize
	moved.bumpLayoutGenerationLocked()
	oldTabCancel := moved.cancel
	parent := c.destination.ctx
	if parent == nil {
		parent = d.serveCtx
	}
	moved.ctx, moved.cancel = context.WithCancel(parent)

	type ownerChange struct {
		pane     *pane
		oldOwner paneEffectLease
		newOwner paneEffectLease
	}
	ownerChanges := make([]ownerChange, 0, len(fencedPanes))
	for _, pane := range fencedPanes {
		floatingGeneration := uint64(0)
		if pane == moved.floating.pane {
			floatingGeneration = moved.floating.generation
		}
		oldOwner, newOwner := pane.publishOwnerLocked(c.destination, moved, floatingGeneration)
		ownerChanges = append(ownerChanges, ownerChange{pane: pane, oldOwner: oldOwner, newOwner: newOwner})
	}
	if d.beforeMoveTabCommit != nil {
		d.beforeMoveTabCommit()
	}
	var syncCleanup syncTimerCleanup
	for _, change := range ownerChanges {
		syncCleanup.append(d.migratePaneSyncOwnerLocked(change.pane, change.oldOwner, change.newOwner))
	}

	return moveTopologyPublication{
		sourceTab:        moved,
		destinationTab:   moved,
		movedPane:        firstMovePane(c.admission.panes),
		movedPanes:       c.admission.panes,
		syncCleanup:      syncCleanup,
		oldTabCancel:     oldTabCancel,
		sourceTabRemoved: true,
		sourceEmpty:      len(c.source.tabs) == 0,
	}, nil
}

func sameMoveTabPanesLocked(tb *tab, admitted, fenced []*pane) bool {
	current := make(map[*pane]struct{}, len(tb.panes)+1)
	for _, pane := range tb.panes {
		current[pane] = struct{}{}
	}
	if tb.floating.pane != nil && (tb.floating.state == floatingHidden || tb.floating.state == floatingVisible) {
		current[tb.floating.pane] = struct{}{}
	}
	if len(current) != len(admitted) || len(current) != len(fenced) {
		return false
	}
	for _, pane := range admitted {
		if _, ok := current[pane]; !ok {
			return false
		}
	}
	for _, pane := range fenced {
		if _, ok := current[pane]; !ok {
			return false
		}
	}
	return true
}
