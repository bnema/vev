package daemon

import (
	"errors"
	"fmt"
	"math"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

var (
	errMovePaneFloatingSibling = errors.New("cannot remove a source tab with a floating pane")
	errMovePaneIDExhausted     = errors.New("destination pane IDs exhausted")
	errMovePaneInvalid         = errors.New("move pane request is no longer valid")
)

// movePaneCandidate contains only unpublished topology and identity changes.
// The caller must revalidate the live tabs before committing these values.
type movePaneCandidate struct {
	pane *pane

	sourceID        layout.PaneID
	destinationID   layout.PaneID
	removeSourceTab bool

	sourceTree            *layout.Tree
	sourcePlacements      []layout.Placement
	destinationTree       *layout.Tree
	destinationPlacements []layout.Placement
	destinationNextPaneID int
	sourceGeneration      uint64
	destinationGeneration uint64
}

// prepareMovePaneCandidate builds both affected layouts without publishing any
// tab or pane state. The caller is responsible for holding and later
// revalidating the source and destination tab state.
func prepareMovePaneCandidate(source, destination *tab, moved *pane) (*movePaneCandidate, error) {
	if source == nil || destination == nil || source == destination || moved == nil ||
		source.tree == nil || source.tree.Root == nil || source.panes[moved.id] != moved ||
		!layout.ContainsLeaf(source.tree.Root, moved.id) || destination.tree == nil || destination.tree.Root == nil ||
		destination.panes[destination.tree.Focus] == nil || !layout.ContainsLeaf(destination.tree.Root, destination.tree.Focus) {
		return nil, layout.ErrNotFound
	}

	candidate := &movePaneCandidate{
		pane:                  moved,
		sourceID:              moved.id,
		sourceGeneration:      source.layoutGeneration,
		destinationGeneration: destination.layoutGeneration,
	}

	sourceLeaves := layout.LeafIDs(source.tree.Root)
	if len(sourceLeaves) == 1 {
		if source.floating.state != floatingUninitialized {
			return nil, errMovePaneFloatingSibling
		}
		candidate.removeSourceTab = true
	} else {
		candidate.sourceTree = source.tree.Clone()
		if err := candidate.sourceTree.Close(moved.id); err != nil {
			return nil, err
		}
		var ok bool
		candidate.sourcePlacements, ok = layout.Solve(candidate.sourceTree.Root, tabArea(source))
		if !ok || candidate.sourceTree.Focus == "" || !layout.ContainsLeaf(candidate.sourceTree.Root, candidate.sourceTree.Focus) {
			return nil, layout.ErrTooSmall
		}
	}

	var err error
	candidate.destinationID, candidate.destinationNextPaneID, err = allocateMovePaneID(destination, moved.id)
	if err != nil {
		return nil, err
	}
	candidate.destinationTree = destination.tree.Clone()
	area := tabArea(destination)
	if err := candidate.destinationTree.Split(destination.tree.Focus, layout.Right, true, candidate.destinationID, area); err != nil {
		return nil, err
	}
	var ok bool
	candidate.destinationPlacements, ok = layout.Solve(candidate.destinationTree.Root, area)
	if !ok {
		return nil, layout.ErrTooSmall
	}
	return candidate, nil
}

func allocateMovePaneID(destination *tab, sourceID layout.PaneID) (layout.PaneID, int, error) {
	next := max(destination.nextPaneID, 1)
	for {
		id := layout.PaneID(fmt.Sprintf("pane-%d", next))
		_, mapped := destination.panes[id]
		available := id != sourceID && !mapped && !layout.ContainsLeaf(destination.tree.Root, id)
		if next == math.MaxInt {
			return "", 0, errMovePaneIDExhausted
		}
		if available {
			return id, next + 1, nil
		}
		next++
	}
}

func tabArea(tb *tab) domain.Rect {
	return domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
}
