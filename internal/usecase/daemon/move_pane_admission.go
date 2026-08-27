package daemon

import "github.com/bnema/vev/internal/usecase/layout"

// movePaneTopology is the private pane topology Implementation consumed by the
// shared moveTransaction Module.
type movePaneTopology struct {
	req       movePaneRequest
	admission movePaneAdmission
}

// movePaneAdmission is an immutable snapshot of the exact live topology and
// generations a move may later publish. Common Session and Attachment authority
// is snapshotted by moveTransaction.
type movePaneAdmission struct {
	source                *session
	destination           *session
	sourceTab             *tab
	destinationTab        *tab
	movedPane             *pane
	sourceGeneration      uint64
	destinationGeneration uint64
	finalSourceTab        bool
}

func (p *movePaneTopology) transactionRequest() moveTransactionRequest {
	req := p.req
	return moveTransactionRequest{
		operation:            "pane",
		attachment:           req.Attachment,
		attachmentCapability: req.AttachmentCapability,
		source:               req.Source,
		destination:          req.Destination,
		logAttrs: []any{
			"source_session_id", req.Source.ID,
			"source_tab_id", req.SourceTabID,
			"source_pane_id", req.SourcePaneID,
			"destination_session_id", req.Destination.ID,
			"destination_tab_id", req.DestinationTabID,
		},
	}
}

func (p *movePaneTopology) validRequest() bool {
	return p != nil && p.req.SourceTabID != "" && p.req.SourcePaneID != "" && p.req.DestinationTabID != ""
}

// admitLocked runs with daemon and ordered Session locks held. It acquires only
// the private ordered Tab locks and performs no publication.
func (p *movePaneTopology) admitLocked(_ *Daemon, source, destination *session) error {
	req := p.req
	sourceTab := findMoveTabLocked(source, req.SourceTabID)
	destinationTab := findMoveTabLocked(destination, req.DestinationTabID)
	if sourceTab == nil || destinationTab == nil {
		return errMoveStaleTarget
	}
	if sourceTab == destinationTab {
		return errMovePaneInvalid
	}
	unlockTabs := lockMoveTabs(sourceTab, destinationTab)
	defer unlockTabs()

	var movedPane *pane
	for _, candidate := range sourceTab.panes {
		if candidate != nil && candidate.stableID == string(req.SourcePaneID) {
			movedPane = candidate
			break
		}
	}
	if movedPane == nil {
		return errMoveStaleTarget
	}

	p.admission = movePaneAdmission{
		source: source, destination: destination, sourceTab: sourceTab, destinationTab: destinationTab,
		movedPane:        movedPane,
		sourceGeneration: sourceTab.layoutGeneration, destinationGeneration: destinationTab.layoutGeneration,
		finalSourceTab: len(source.tabs) == 1 && source.tabs[0] == sourceTab &&
			sourceTab.tree != nil && sourceTab.tree.Root != nil && len(layout.LeafIDs(sourceTab.tree.Root)) == 1,
	}
	return nil
}

func (p *movePaneTopology) afterAdmission(d *Daemon) {
	if d.afterMovePaneSourceSnapshot != nil {
		d.afterMovePaneSourceSnapshot()
	}
}

func (p *movePaneTopology) willRetireSource() bool {
	return p != nil && p.admission.finalSourceTab && p.admission.source != p.admission.destination
}

func (p *movePaneTopology) resizeFences(source, destination *session) *moveResizeFences {
	admission := p.admission
	return newMovePaneResizeFences(source, destination, admission.sourceTab, admission.destinationTab, admission.movedPane)
}

func (p *movePaneTopology) publishLocked(d *Daemon, source, destination *session) (moveTopologyPublication, error) {
	admission := p.admission
	commit := movePaneCommit{
		req:                   p.req,
		source:                source,
		destination:           destination,
		sourceTab:             admission.sourceTab,
		destinationTab:        admission.destinationTab,
		movedPane:             admission.movedPane,
		sourceGeneration:      admission.sourceGeneration,
		destinationGeneration: admission.destinationGeneration,
	}
	return commit.publishTopologyLocked(d)
}
