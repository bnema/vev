package daemon

import (
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type committedIdentityErrorTransport struct {
	closeTrackingTransport
}

func (t *committedIdentityErrorTransport) Send(frame ports.Frame) error {
	if frame.Type == ports.MsgCommittedRouteIdentity {
		return errors.New("committed identity send failed")
	}
	return t.closeTrackingTransport.Send(frame)
}

func TestAttachmentTransitionPublishesCommittedRouteIdentity(t *testing.T) {
	d, source, ac, _ := newManualSessionWithPTYs(t, nil)
	transport := &closeTrackingTransport{}
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(source, nil, ac, true)
	token := source.captureAttachmentCapability(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)
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

func TestAttachmentTransitionTearsDownAfterCommittedIdentitySendFailure(t *testing.T) {
	d, source, ac, _ := newManualSessionWithPTYs(t, nil)
	transport := &committedIdentityErrorTransport{}
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(source, nil, ac, true)
	token := source.captureAttachmentCapability(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)
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

	_, err := d.transitionAttachment(attachmentTransitionRequest{
		source:            source,
		target:            target,
		next:              ac,
		expectedTransport: transportSnapshot{transport: transport, incarnation: ac.transportSnapshot().incarnation},
		ready:             true,
	})
	require.ErrorContains(t, err, "committed identity send failed")
	require.Empty(t, target.snapshotAttachments())
	require.Empty(t, source.snapshotAttachments())
	require.True(t, transport.Closed())
}
