package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestAttachmentTransitionPublishesCommittedRouteIdentity(t *testing.T) {
	d, source, ac, _ := newManualSessionWithPTYs(t, nil)
	transport := &closeTrackingTransport{}
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(source, nil, ac, true)
	token := source.attachmentToken(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.publishAttachmentCapability(token)
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{Generation: 1})

	target := &session{
		sessionCore: sessionCore{
			id:          "target",
			name:        "target",
			incarnation: domain.IncarnationID{2},
		},
		ctx:    source.ctx,
		cancel: func() {},
		tabs:   []*tab{newTab(nil, domain.Size{Cols: 80, Rows: 23})},
	}
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()

	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source:            source,
		target:            target,
		next:              ac,
		expectedTransport: transportSnapshot{transport: transport, incarnation: ac.transportSnapshot().incarnation},
		ready:             true,
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(transition)

	var identity ports.CommittedRouteIdentity
	found := false
	for _, frame := range transport.Sends() {
		if frame.Type != ports.MsgCommittedRouteIdentity {
			continue
		}
		var decodeErr error
		identity, decodeErr = ports.UnmarshalCommittedRouteIdentity(frame.Payload)
		require.NoError(t, decodeErr)
		found = true
	}
	require.True(t, found, "attachment transitions must publish their committed route identity")
	require.Equal(t, "target", identity.Target.SessionName)
	require.Equal(t, domain.IncarnationID{2}, identity.Target.LifecycleID)
}
