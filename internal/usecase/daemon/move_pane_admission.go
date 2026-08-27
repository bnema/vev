package daemon

import "github.com/bnema/vev/internal/usecase/layout"

// movePaneAdmission is an immutable snapshot of the exact live objects and
// generations a move may later commit. Resize fences are acquired only after
// this snapshot; the locked commit revalidates every field before publication.
type movePaneAdmission struct {
	source                *session
	destination           *session
	sourceTab             *tab
	destinationTab        *tab
	movedPane             *pane
	sourceAttachments     []*attachedClient
	sourceTransports      map[*attachedClient]transportSnapshot
	sourceGeneration      uint64
	destinationGeneration uint64
	finalSourceTab        bool
}

func (d *Daemon) snapshotMovePaneAdmission(req movePaneRequest, source, destination *session) (*movePaneAdmission, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing || d.sessions[source.id] != source || d.sessions[destination.id] != destination {
		return nil, errMoveStaleTarget
	}
	unlockSessions := lockAttachmentSessions(source, destination)
	defer unlockSessions()
	if !moveSessionLocatorCurrentLocked(source, req.Source) || !moveSessionLocatorCurrentLocked(destination, req.Destination) {
		return nil, errMoveStaleTarget
	}
	if req.Attachment != nil && !attachmentRegisteredLocked(source, req.Attachment) {
		return nil, errMoveStaleTarget
	}
	if req.AttachmentCapability.ac != nil && (req.AttachmentCapability.ac != req.Attachment || !req.AttachmentCapability.currentInSessionLocked(source)) {
		return nil, errMoveStaleTarget
	}

	sourceTab := findMoveTabLocked(source, req.SourceTabID)
	destinationTab := findMoveTabLocked(destination, req.DestinationTabID)
	if sourceTab == nil || destinationTab == nil {
		return nil, errMoveStaleTarget
	}
	if sourceTab == destinationTab {
		return nil, errMovePaneInvalid
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
		return nil, errMoveStaleTarget
	}

	attachments := source.snapshotAttachmentsLocked()
	transports := make(map[*attachedClient]transportSnapshot, len(attachments))
	for _, ac := range attachments {
		transports[ac] = ac.transportSnapshot()
	}
	return &movePaneAdmission{
		source: source, destination: destination, sourceTab: sourceTab, destinationTab: destinationTab,
		movedPane: movedPane, sourceAttachments: attachments, sourceTransports: transports,
		sourceGeneration: sourceTab.layoutGeneration, destinationGeneration: destinationTab.layoutGeneration,
		finalSourceTab: len(source.tabs) == 1 && source.tabs[0] == sourceTab &&
			sourceTab.tree != nil && sourceTab.tree.Root != nil && len(layout.LeafIDs(sourceTab.tree.Root)) == 1,
	}, nil
}
