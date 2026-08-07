package daemon

import (
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/stretchr/testify/require"
)

func TestNavigationHandoffsDropReplacedInitiatorWithoutMutation(t *testing.T) {
	tests := []struct {
		name   string
		action string
		run    func(*Daemon, *session, *session, *attachedClient, *attachmentEffectTicket) error
		check  func(*testing.T, *Daemon, *session, *session)
	}{
		{
			name: "picker selection", action: "picker-select",
			run: func(d *Daemon, source, target *session, ac *attachedClient, effect *attachmentEffectTicket) error {
				d.enterPicker(source, ac)
				return d.switchToTargetForAttachment(effect.connectionToken(), picker.Target{Session: target.id, TabIndex: 1}, sessionHandoffGuard{}, "picker-select")
			},
		},
		{
			name: "palette session", action: "palette-session",
			run: func(d *Daemon, _ *session, target *session, _ *attachedClient, effect *attachmentEffectTicket) error {
				return d.switchToTargetForAttachment(effect.connectionToken(), picker.Target{Session: target.id, TabIndex: 1}, sessionHandoffGuard{}, "palette-session")
			},
		},
		{
			name: "palette recent session", action: "palette-recent-session",
			run: func(d *Daemon, source, target *session, ac *attachedClient, effect *attachmentEffectTicket) error {
				return paletteExec{d: d, sess: source, ac: ac, recent: []recentSession{{id: target.id}}, effect: effect}.JumpRecentSession(1)
			},
		},
		{
			name: "session overflow", action: "overflow-session",
			run: func(d *Daemon, source, target *session, ac *attachedClient, effect *attachmentEffectTicket) error {
				source.mu.Lock()
				source.name = "alpha"
				source.mu.Unlock()
				target.mu.Lock()
				target.name = "charlie"
				target.mu.Unlock()
				d.ApplyConfig(domain.Config{Nav: domain.NavConfig{OverflowSessions: true}})
				return d.focusDir(source, ac, layout.Down, effect)
			},
		},
		{
			name: "stopped session resume", action: "stopped-session",
			run: func(d *Daemon, _ *session, target *session, _ *attachedClient, effect *attachmentEffectTicket) error {
				d.mu.Lock()
				delete(d.sessions, target.id)
				d.stopped["stopped"] = stoppedSession{name: "stopped", cwd: "/tmp", createdAt: 9, state: ports.SessionDown}
				d.mu.Unlock()
				expectedCreatedAt := int64(9)
				return d.switchToTargetForAttachment(effect.connectionToken(), picker.Target{Name: "stopped", Stopped: true, ExpectedCreatedAt: &expectedCreatedAt}, sessionHandoffGuard{}, "stopped-session")
			},
			check: func(t *testing.T, d *Daemon, _, _ *session) {
				d.mu.Lock()
				_, stopped := d.stopped["stopped"]
				resumed := d.findByNameLocked("stopped")
				d.mu.Unlock()
				require.True(t, stopped)
				require.Nil(t, resumed)
			},
		},
		{
			name: "transition prompt creation", action: "create-session",
			run: func(d *Daemon, _ *session, _ *session, _ *attachedClient, effect *attachmentEffectTicket) error {
				return d.createSessionAndSwitchForAttachment(effect.connectionToken(), "created")
			},
			check: func(t *testing.T, d *Daemon, _, _ *session) {
				d.mu.Lock()
				created := d.findByNameLocked("created")
				d.mu.Unlock()
				require.Nil(t, created, "stale prompt submission created a target session")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, source, old, _, releases := newManualTabSession(t, 2)
			defer releaseAll(releases)
			d.ptys = newFactorySeq(t)
			target := &session{sessionCore: sessionCore{id: "target", name: "target"}, ctx: source.ctx, cancel: func() {}, tabs: []*tab{
				newTab(nil, domain.Size{Cols: 80, Rows: 23}),
				newTab(nil, domain.Size{Cols: 80, Rows: 23}),
			}}
			d.mu.Lock()
			d.sessions[target.id] = target
			d.mu.Unlock()

			rc := d.attachCoordinator(source, nil, old, true)
			token := source.attachmentToken(old, old.transport())
			token.lease = rc.attachmentLease(old)
			old.publishAttachmentCapability(token)
			effect, admitted := old.beginAttachmentEffect(token)
			require.True(t, admitted)

			admissionEnded := make(chan struct{})
			releaseAction := make(chan struct{})
			var admissionEndedOnce sync.Once
			d.afterActionAttachmentEffectEnded = func(action string) {
				if action == tt.action {
					admissionEndedOnce.Do(func() { close(admissionEnded) })
					<-releaseAction
				}
			}
			actionDone := make(chan error, 1)
			go func() { actionDone <- tt.run(d, source, target, old, effect) }()
			select {
			case <-admissionEnded:
			case <-time.After(time.Second):
				t.Fatal("handoff did not release its role ticket")
			}

			replacement, err := d.transitionAttachment(attachmentTransitionRequest{
				source: source, target: source, next: old,

				expectedTransport: old.transportSnapshot(), ready: true,
			})
			require.NoError(t, err)
			close(releaseAction)
			require.Error(t, <-actionDone)

			require.Same(t, source, old.currentSession())
			source.mu.Lock()
			require.Contains(t, source.snapshotAttachmentsLocked(), old)
			source.mu.Unlock()
			target.mu.Lock()
			require.Empty(t, target.snapshotAttachmentsLocked(), "stale handoff attached to the target")
			require.Zero(t, testAttachmentTabIndexLocked(target), "stale handoff changed target tab focus")
			target.mu.Unlock()
			if tt.check != nil {
				tt.check(t, d, source, target)
			}
			d.deferAttachmentTransitionCleanups(replacement)
		})
	}
}

func TestStoppedSessionHandoffDoesNotResumeAfterInitiatorReplacement(t *testing.T) {
	d, source, old, _ := newManualSessionWithPTYs(t, nil)
	d.mu.Lock()
	d.stopped["stopped"] = stoppedSession{name: "stopped", cwd: "/tmp", createdAt: 7, state: ports.SessionDown}
	d.mu.Unlock()

	rc := d.attachCoordinator(source, nil, old, true)
	token := source.attachmentToken(old, old.transport())
	token.lease = rc.attachmentLease(old)
	old.publishAttachmentCapability(token)
	effect, admitted := old.beginAttachmentEffect(token)
	require.True(t, admitted)

	ended := make(chan struct{})
	release := make(chan struct{})
	var endedOnce sync.Once
	d.afterActionAttachmentEffectEnded = func(action string) {
		if action == "picker-stopped" {
			endedOnce.Do(func() { close(ended) })
			<-release
		}
	}
	expectedCreatedAt := int64(7)
	done := make(chan error, 1)
	go func() {
		done <- d.switchToTargetForAttachment(effect.connectionToken(), picker.Target{Name: "stopped", Stopped: true, ExpectedCreatedAt: &expectedCreatedAt}, sessionHandoffGuard{}, "picker-stopped")
	}()
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("stopped-session handoff did not release its role ticket")
	}

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	replacement, err := d.transitionAttachment(attachmentTransitionRequest{
		target: source, next: next, expectedTransport: next.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	close(release)
	require.Error(t, <-done)

	d.mu.Lock()
	_, stillStopped := d.stopped["stopped"]
	resumed := d.findByNameLocked("stopped")
	d.mu.Unlock()
	require.True(t, stillStopped)
	require.Nil(t, resumed)
	d.deferAttachmentTransitionCleanups(replacement)
}
