package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// admitPickerEffectForTest admits and returns one attachment effect for a
// session/attachment pair. It is the local-only helper the picker and preview
// tests share; the effect is ended during test cleanup so a test may admit
// further effects on the same attachment.
func admitPickerEffectForTest(t *testing.T, sess *session, ac *attachedClient) *attachmentEffect {
	t.Helper()
	effect, admitted := ac.beginAttachmentEffect(captureAttachmentCapability(sess, ac, ac.transport()))
	require.True(t, admitted)
	t.Cleanup(effect.End)
	return effect
}
