package daemon

import (
	"github.com/bnema/vev/internal/usecase/layout"
)

// movePaneAdmission is an immutable snapshot of the exact live objects and
// generations a move may later commit. Resize fences are acquired only after
// this snapshot; the locked commit revalidates every field before publication.
type movePaneAdmission struct {
	source                *session
	destination           *session
	sourceTab             *tab
	destinationTab        *tab
	movedPane             *pane
	sourceClient          *attachedClient
	destinationClient     *attachedClient
	sourceSnatched        int
	sourceGeneration      uint64
	destinationGeneration uint64
	finalSourceTab        bool
	sourceTabWasActive    bool
}

func (d *Daemon) snapshotMovePaneAdmission(req movePaneRequest, source, destination *session) (*movePaneAdmission, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing || d.sessions[source.id] != source || d.sessions[destination.id] != destination {
		return nil, errMovePaneInvalid
	}
	unlockSessions := lockAttachmentSessions(source, destination)
	defer unlockSessions()
	if !moveSessionLocatorCurrentLocked(source, req.Source) || !moveSessionLocatorCurrentLocked(destination, req.Destination) {
		return nil, errMovePaneInvalid
	}

	sourceTab := findMoveTabLocked(source, req.SourceTabID)
	destinationTab := findMoveTabLocked(destination, req.DestinationTabID)
	if sourceTab == nil || destinationTab == nil || sourceTab == destinationTab {
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
		return nil, errMovePaneInvalid
	}

	return &movePaneAdmission{
		source:                source,
		destination:           destination,
		sourceTab:             sourceTab,
		destinationTab:        destinationTab,
		movedPane:             movedPane,
		sourceClient:          source.client,
		destinationClient:     destination.client,
		sourceSnatched:        len(source.snatched),
		sourceGeneration:      sourceTab.layoutGeneration,
		destinationGeneration: destinationTab.layoutGeneration,
		finalSourceTab: len(source.tabs) == 1 && source.tabs[0] == sourceTab &&
			sourceTab.tree != nil && sourceTab.tree.Root != nil && len(layout.LeafIDs(sourceTab.tree.Root)) == 1,
		sourceTabWasActive: source.active >= 0 && source.active < len(source.tabs) && source.tabs[source.active] == sourceTab,
	}, nil
}
