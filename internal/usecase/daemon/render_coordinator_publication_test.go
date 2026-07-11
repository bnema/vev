package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoordinatorRejectsStaleAttachmentInvalidationAfterReplacement(t *testing.T) {
	var invalidations []renderInvalidation
	rc := newRenderCoordinator(renderCoordinatorOptions{
		onInvalidate: func(inv renderInvalidation) { invalidations = append(invalidations, inv) },
	})
	old := &attachedClient{}
	replacement := &attachedClient{}
	rc.attach(old)
	rc.noteReplace(old, replacement)

	// This models a PR #71 resize callback that passed its sendMu generation
	// check immediately before a replacement was published.
	rc.invalidateForAttachment(old, renderInvalidation{producer: "stale resize"})
	require.Empty(t, invalidations)

	// Pane/session producers carry no attachment and remain coordinator-owned.
	rc.invalidateForAttachment(nil, renderInvalidation{producer: "pane"})
	require.Len(t, invalidations, 1)
	require.Equal(t, "pane", invalidations[0].producer)
}
