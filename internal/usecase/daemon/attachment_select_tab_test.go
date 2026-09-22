package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// TestSelectTabMovesOnlyThisAttachmentView proves the picker's in-place tab
// switch: an exact tab selects it on the requesting attachment, a peer keeps
// its own view, and an unknown or malformed tab leaves the view untouched.
func TestSelectTabMovesOnlyThisAttachmentView(t *testing.T) {
	tests := []struct {
		name    string
		request func(second domain.TabStableID) protocol.SelectTab
		moves   bool
	}{
		{name: "exact tab", request: func(second domain.TabStableID) protocol.SelectTab { return protocol.SelectTab{TabID: second} }, moves: true},
		{name: "unknown tab", request: func(domain.TabStableID) protocol.SelectTab { return protocol.SelectTab{TabID: "t_missing"} }},
		{name: "malformed tab", request: func(domain.TabStableID) protocol.SelectTab { return protocol.SelectTab{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _, releases := newManualTabSession(t, 2)
			for _, release := range releases {
				defer release()
			}
			peer := addMultiplexTestAttachment(t, sess, domain.Geometry{Size: domain.Size{Cols: 90, Rows: 30}})
			rc := d.attachCoordinator(sess, nil, ac, true)
			d.attachCoordinator(sess, nil, peer, true)
			token := sess.captureAttachmentCapability(ac, ac.transport())
			token.lease = rc.attachmentLease(ac)
			ac.installTestAttachmentCapability(token)

			first := domain.TabStableID(sess.tabs[0].stableID)
			second := domain.TabStableID(sess.tabs[1].stableID)
			require.True(t, sess.selectAttachmentTab(ac, first))
			require.True(t, sess.selectAttachmentTab(peer, first))
			before := ac.viewSnapshot()

			require.False(t, d.handleAttachmentClientMessage(token, tt.request(second)))

			if tt.moves {
				require.Equal(t, second, ac.viewSnapshot().tabID)
			} else {
				require.Equal(t, before, ac.viewSnapshot(), "a refused selection never repairs the view")
			}
			require.Equal(t, first, peer.viewSnapshot().tabID, "the peer attachment keeps its own view")
		})
	}
}
