package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/picker"
)

func TestPickerTransitionRejectsInactiveRemoteLink(t *testing.T) {
	d, source, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	view := &remoteView{key: remoteViewKey{
		endpoint: "remote.example", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote",
	}}
	link := &remoteLink{view: view, generation: 1, transport: newRemoteLinkTestTransport()}
	view.link, view.linkGeneration = link, 1
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	model := picker.New(nil, picker.SelectionConfig{})
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = model
	ac.overlays.pickerGeneration++
	selection := &remotePickerSelection{model: model, generation: ac.overlays.pickerGeneration, token: source.attachmentToken(ac, ac.transport())}
	ac.overlays.pickerRemoteSelection = selection
	ac.overlays.pickerMu.Unlock()

	_, err := d.transitionToRemoteViewForPicker(selection.token, view, selection)
	require.ErrorIs(t, err, errAttachmentTransition)
	require.Same(t, source, ac.currentAttachmentSession())
	require.True(t, source.attachmentRegistered(ac))
	require.False(t, view.attachmentRegistered(ac))
}
