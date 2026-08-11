package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/stretchr/testify/require"
)

func TestNavigationHandoffDoesNotMutateAfterInitiatorIncarnationChanges(t *testing.T) {
	tests := []struct {
		name   string
		rebind func(*testing.T, *Daemon, *session, *attachedClient)
	}{
		{
			name: "role generation and lease replaced on same attachment",
			rebind: func(t *testing.T, d *Daemon, source *session, ac *attachedClient) {
				result, err := d.transitionAttachment(attachmentTransitionRequest{
					source: source, target: source, next: ac,

					expectedTransport: ac.transportSnapshot(), ready: true,
				})
				require.NoError(t, err)
				d.deferAttachmentTransitionCleanups(result)
			},
		},
		{
			name: "transport incarnation rebound on same attachment",
			rebind: func(t *testing.T, _ *Daemon, source *session, ac *attachedClient) {
				ac.replaceTransport(&closeTrackingTransport{})
				current := source.attachmentToken(ac, ac.transport())
				current.lease = source.renderCoordinator().attachmentLease(ac)
				ac.publishAttachmentCapability(current)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, source, ac, _ := newManualSessionWithPTYs(t, nil)
			target := &session{sessionCore: sessionCore{id: "target", name: "target", incarnation: domain.SessionLifecycleID{1}}, ctx: source.ctx, cancel: func() {}, tabs: []*tab{
				newTab(nil, domain.Size{Cols: 80, Rows: 23}),
				newTab(nil, domain.Size{Cols: 80, Rows: 23}),
			}}
			d.mu.Lock()
			d.sessions[target.id] = target
			d.mu.Unlock()

			rc := d.attachCoordinator(source, nil, ac, true)
			token := source.attachmentToken(ac, ac.transport())
			token.lease = rc.attachmentLease(ac)
			ac.publishAttachmentCapability(token)
			effect, admitted := ac.beginAttachmentEffect(token)
			require.True(t, admitted)

			ended := make(chan struct{})
			release := make(chan struct{})
			d.afterActionAttachmentEffectEnded = func(action string) {
				if action == "detach" {
					close(ended)
					<-release
				}
			}
			done := make(chan error, 1)
			go func() {
				done <- d.switchToTargetForAttachment(effect.connectionToken(), picker.Target{Session: target.id, TabIndex: 1}, sessionHandoffGuard{}, "test-handoff")
			}()
			awaitTestCompletion(t, ended, "test handoff did not release its role ticket")
			tt.rebind(t, d, source, ac)
			close(release)
			require.NoError(t, awaitTestValue(t, done, "test handoff did not finish"))

			require.Same(t, source, ac.currentSession())
			target.mu.Lock()
			require.Empty(t, target.snapshotAttachmentsLocked())
			require.Zero(t, testAttachmentTabIndexLocked(target))
			target.mu.Unlock()
		})
	}
}
