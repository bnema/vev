package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAttachmentOwnerTokenRejectsTypedNilRemoteView(t *testing.T) {
	var view *remoteView
	ac := &attachedClient{tr: &closeTrackingTransport{}}

	token := attachmentOwnerToken(view, ac, ac.transport())

	require.Nil(t, token.owner)
	require.Nil(t, token.ac)
}

func TestTypedNilRemoteOwnerCannotEnterBackNavigation(t *testing.T) {
	var view *remoteView
	ac := &attachedClient{}

	require.NotPanics(t, func() {
		require.NoError(t, (&Daemon{}).backSessionForAttachment(attachmentConnectionToken{owner: view, ac: ac}))
	})
}

func TestRecordPreviousOwnerIgnoresTypedNilRemoteView(t *testing.T) {
	var view *remoteView
	ac := &attachedClient{}

	ac.recordPreviousOwner(view)

	require.Nil(t, ac.previousOwner.Get())
}
