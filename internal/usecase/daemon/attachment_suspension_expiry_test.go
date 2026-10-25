package daemon

import (
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

const testSuspendedExpiry = 7 * time.Second

// newSuspendedExpiryFixture creates one manual session, suspends its sole
// attachment, and returns the armed fake-clock safety timer plus a transport
// whose Close call can be observed.
func newSuspendedExpiryFixture(t *testing.T) (*Daemon, *session, *attachedClient, *testTimer, *connLog) {
	t.Helper()
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY(t))
	WithSuspendedSafetyExpiry(testSuspendedExpiry)(d)
	d.attachCoordinator(sess, nil, ac, true)
	tr, trLog := newTrackedConnection(t)
	ac.replaceTransport(tr)
	ac.resumeCapable, ac.resumeToken = true, 123
	token := sess.captureAttachmentCapability(ac, tr)
	ac.installTestAttachmentCapability(token)
	clock, clockTimers := newSignalClock(t, 64)
	d.clock = clock
	require.NoError(t, d.suspendAttachment(token, protocol.SuspendAttachment{RequestID: 1}))
	timer := awaitTimerWithDuration(t, clockTimers, testSuspendedExpiry)
	return d, sess, ac, timer, trLog
}

// awaitTimerWithDuration drains unrelated watchdog timers (the suspension ACK
// has its own bounded-send deadline) until the requested one is armed. It must
// run inside a synctest bubble.
func awaitTimerWithDuration(t *testing.T, timers <-chan *testTimer, want time.Duration) *testTimer {
	t.Helper()
	for range 8 {
		timer := requireReceived(t, timers, "expected timer was never armed")
		if timer.duration == want {
			return timer
		}
	}
	t.Fatalf("no timer with duration %s was armed", want)
	return nil
}

func TestSuspendedAttachmentSafetyExpiryEvictsAfterTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, timer, tr := newSuspendedExpiryFixture(t)

		evicted := make(chan struct{})
		var once sync.Once
		d.afterClientGoneDetach = func() { once.Do(func() { close(evicted) }) }

		// Suspended-only presence is not observable, but membership and transport
		// ownership are retained until expiry.
		require.False(t, sess.snapshotView(viewOptions{}).attached, "suspended-only session must not be observably attached")
		require.True(t, slices.Contains(sess.snapshotAttachments(), ac), "suspension retains session membership")
		d.mu.Lock()
		require.NotNil(t, d.suspended[ac], "suspension arms the daemon-owned safety expiry")
		d.mu.Unlock()

		timer.fire()
		requireReceived(t, evicted, "safety expiry never evicted the suspended attachment")
		requireReceived(t, tr.Done(), "expiry never closed the retained transport")

		require.Nil(t, ac.currentAttachmentSession())
		sess.mu.Lock()
		_, registered := sess.attachments[ac]
		sess.mu.Unlock()
		require.False(t, registered, "expiry releases session membership")
		require.True(t, tr.Closed(), "expiry closes the retained transport")
		d.mu.Lock()
		_, retained := d.suspended[ac]
		parked := d.resume.parked[123]
		d.mu.Unlock()
		require.False(t, retained)
		require.Nil(t, parked, "expiry must not park a resume credential")
		require.False(t, ac.parked)
		require.False(t, sess.snapshotView(viewOptions{}).attached)
		require.Same(t, sess, firstSession(d), "expiry leaves the session to headless lifetime policy")
		requireAttachmentCleanup(t, d)
	})
}

func TestSuspendedAttachmentExpiryLeavesEphemeralSessionRegistered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, timer, _ := newSuspendedExpiryFixture(t)
		updateRuntimeConfig(d, func(rc *runtimeConfig) { rc.ephemeral = domain.EphemeralConfig{CloseOnExit: false} })
		sess.mu.Lock()
		sess.ephemeral = true
		sess.mu.Unlock()
		evicted := make(chan struct{})
		var once sync.Once
		d.afterClientGoneDetach = func() { once.Do(func() { close(evicted) }) }
		timer.fire()
		requireReceived(t, evicted, "safety expiry never evicted the ephemeral attachment")
		require.Same(t, sess, firstSession(d), "with close-on-exit off, ephemeral sessions follow headless lifetime policy")
		require.Nil(t, ac.currentAttachmentSession())
		requireAttachmentCleanup(t, d)
	})
}

func TestSuspendedAttachmentExpiryCancelledByActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _, tr := newSuspendedExpiryFixture(t)
		require.NoError(t, d.activateAttachment(ac, ac.transportSnapshot(), protocol.ActivateAttachment{
			RequestID: 2,
			Target:    protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name},
			Size:      ac.sizeSnapshot(),
		}))
		d.mu.Lock()
		_, retained := d.suspended[ac]
		d.mu.Unlock()
		require.False(t, retained, "activation cancels the safety expiry")
		require.Equal(t, attachmentActive, ac.attachmentActivity())
		require.True(t, sess.snapshotView(viewOptions{}).attached)
		require.False(t, tr.Closed())
		requireAttachmentCleanup(t, d)
	})
}

func TestSuspendedAttachmentExpiryIsRenameSafe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, timer, _ := newSuspendedExpiryFixture(t)
		// A rename keeps the daemon incarnation; expiry must not require the current
		// name to equal the retained suspension target.
		sess.mu.Lock()
		sess.name = "renamed"
		sess.mu.Unlock()
		evicted := make(chan struct{})
		var once sync.Once
		d.afterClientGoneDetach = func() { once.Do(func() { close(evicted) }) }
		timer.fire()
		requireReceived(t, evicted, "rename must not postpone terminal eviction")
		require.Nil(t, ac.currentAttachmentSession())
		requireAttachmentCleanup(t, d)
	})
}

func TestSuspendedAttachmentExpiryLosesToDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, timer, _ := newSuspendedExpiryFixture(t)
		var detaches atomic.Int64
		d.afterClientGoneDetach = func() { detaches.Add(1) }
		// Run the competing disconnect while the expiry watcher is paused before it
		// freezes the gate.
		d.beforeSuspendedExpiryFreeze = func(a *attachedClient) {
			d.clientGone(sess, a, a.transport(), false)
		}
		timer.fire()
		requireAttachmentCleanup(t, d)
		require.Nil(t, ac.currentAttachmentSession())
		require.Equal(t, int64(1), detaches.Load(), "the losing expiry must not evict twice")
		d.mu.Lock()
		_, retained := d.suspended[ac]
		d.mu.Unlock()
		require.False(t, retained)
	})
}

func TestSuspendedAttachmentExpiryLosesToActivation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, timer, _ := newSuspendedExpiryFixture(t)
		var detaches atomic.Int64
		d.afterClientGoneDetach = func() { detaches.Add(1) }
		activationErr := make(chan error, 1)
		d.beforeSuspendedExpiryFreeze = func(a *attachedClient) {
			activationErr <- d.activateAttachment(a, a.transportSnapshot(), protocol.ActivateAttachment{
				RequestID: 2,
				Target:    protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name},
				Size:      a.sizeSnapshot(),
			})
		}
		timer.fire()
		require.NoError(t, requireReceived(t, activationErr, "activation never ran"))
		requireAttachmentCleanup(t, d)
		require.Equal(t, attachmentActive, ac.attachmentActivity())
		require.True(t, sess.attachmentRegistered(ac))
		require.Equal(t, int64(0), detaches.Load(), "the losing expiry must not evict an activated attachment")
	})
}

func TestSuspendedAttachmentExpiryRequiresExactRecord(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(t *testing.T, d *Daemon, ac *attachedClient, record *suspendedAttachmentRetention) *suspendedAttachmentRetention
	}{
		{
			name: "stale generation",
			prepare: func(t *testing.T, d *Daemon, ac *attachedClient, record *suspendedAttachmentRetention) *suspendedAttachmentRetention {
				stale := &suspendedAttachmentRetention{
					ac: record.ac, sess: record.sess, transport: record.transport,
					generation: record.generation + 1, timer: record.timer, done: record.done,
				}
				d.mu.Lock()
				d.suspended[ac] = stale
				d.mu.Unlock()
				return stale
			},
		},
		{
			name: "stale transport incarnation",
			prepare: func(t *testing.T, d *Daemon, ac *attachedClient, record *suspendedAttachmentRetention) *suspendedAttachmentRetention {
				stale := &suspendedAttachmentRetention{
					ac: record.ac, sess: record.sess, generation: record.generation,
					transport: transportSnapshot{transport: record.transport.transport, incarnation: record.transport.incarnation + 1},
					timer:     record.timer, done: record.done,
				}
				d.mu.Lock()
				d.suspended[ac] = stale
				d.mu.Unlock()
				return stale
			},
		},
		{
			name: "foreign record identity",
			prepare: func(t *testing.T, d *Daemon, ac *attachedClient, record *suspendedAttachmentRetention) *suspendedAttachmentRetention {
				return &suspendedAttachmentRetention{
					ac: record.ac, sess: record.sess, generation: record.generation,
					transport: record.transport, timer: record.timer, done: record.done,
				}
			},
		},
		{
			name: "activity no longer suspended",
			prepare: func(t *testing.T, d *Daemon, ac *attachedClient, record *suspendedAttachmentRetention) *suspendedAttachmentRetention {
				ac.lifecycle.mu.Lock()
				ac.lifecycle.activity = attachmentActive
				ac.lifecycle.mu.Unlock()
				return record
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, sess, ac, _, _ := newSuspendedExpiryFixture(t)
				d.mu.Lock()
				record := d.suspended[ac]
				d.mu.Unlock()
				require.NotNil(t, record)
				expiring := tt.prepare(t, d, ac, record)
				d.expireSuspendedAttachment(expiring)
				require.Same(t, sess, ac.currentAttachmentSession(), "a stale record must not detach the live suspension")
				require.True(t, sess.attachmentRegistered(ac))
				d.mu.Lock()
				live := d.suspended[ac]
				d.mu.Unlock()
				require.NotNil(t, live, "the live safety expiry must survive a stale expiry")
				// Release the live record's watcher without triggering eviction.
				d.clearSuspendedExpiry(ac)
				requireAttachmentCleanup(t, d)
			})
		})
	}
}

func TestSuspendedAttachmentExpiryArmsOnlyAfterAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY(t))
		WithSuspendedSafetyExpiry(testSuspendedExpiry)(d)
		d.attachCoordinator(sess, nil, ac, true)
		entered := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		var block atomic.Bool
		tr, _ := newTrackedConnection(t, func(c *mockServerConnection, log *connLog) {
			c.EXPECT().Send(mock.Anything).RunAndReturn(func(f wire.Envelope) error {
				if block.Load() {
					once.Do(func() { close(entered) })
					<-release
				}
				log.record(f)
				return nil
			}).Maybe()
		})
		block.Store(true)
		ac.replaceTransport(tr)
		token := sess.captureAttachmentCapability(ac, tr)
		ac.installTestAttachmentCapability(token)

		done := make(chan error, 1)
		go func() { done <- d.suspendAttachment(token, protocol.SuspendAttachment{RequestID: 1}) }()
		synctest.Wait()
		requireReceived(t, entered, "suspension never attempted the ACK")
		d.mu.Lock()
		armed := d.suspended[ac] != nil
		d.mu.Unlock()
		require.False(t, armed, "the safety expiry must not be armed before the ACK is delivered")

		close(release)
		require.NoError(t, requireReceived(t, done, "suspension never completed"))
		d.mu.Lock()
		armed = d.suspended[ac] != nil
		d.mu.Unlock()
		require.True(t, armed, "the safety expiry must be armed once the ACK is delivered")
		d.clearSuspendedExpiry(ac)
		requireAttachmentCleanup(t, d)
	})
}

func TestSuspendedAttachmentExpiryAckFailureArmsNothing(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY(t))
	WithSuspendedSafetyExpiry(testSuspendedExpiry)(d)
	d.attachCoordinator(sess, nil, ac, true)
	tr, _ := newTrackedConnection(t, func(c *mockServerConnection, _ *connLog) {
		c.EXPECT().Send(mock.Anything).Return(errors.New("suspension ack failed")).Maybe()
	})
	ac.replaceTransport(tr)
	token := sess.captureAttachmentCapability(ac, tr)
	ac.installTestAttachmentCapability(token)

	require.Error(t, d.suspendAttachment(token, protocol.SuspendAttachment{RequestID: 1}))
	d.mu.Lock()
	_, armed := d.suspended[ac]
	d.mu.Unlock()
	require.False(t, armed, "a failed ACK must not arm a safety expiry")
}

func TestSuspendedAttachmentExpiryRearmsOnResuspension(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _, _ := newSuspendedExpiryFixture(t)
		d.mu.Lock()
		first := d.suspended[ac]
		d.mu.Unlock()
		require.NotNil(t, first)
		require.NoError(t, d.activateAttachment(ac, ac.transportSnapshot(), protocol.ActivateAttachment{
			RequestID: 2,
			Target:    protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name},
			Size:      ac.sizeSnapshot(),
		}))
		token := sess.captureAttachmentCapability(ac, ac.transport())
		ac.installTestAttachmentCapability(token)
		require.NoError(t, d.suspendAttachment(token, protocol.SuspendAttachment{RequestID: 3}))
		d.mu.Lock()
		second := d.suspended[ac]
		d.mu.Unlock()
		require.NotNil(t, second)
		require.NotSame(t, first, second, "re-suspension replaces the retained safety expiry")
		synctest.Wait()
		select {
		case <-first.done:
		default:
			t.Fatal("the superseded safety expiry was not released")
		}
		d.clearSuspendedExpiry(ac)
		requireAttachmentCleanup(t, d)
	})
}

func TestSuspendedAttachmentShutdownJoinsWatcher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _, _ := newSuspendedExpiryFixture(t)
		require.False(t, d.shutdownAll(protocol.ReasonServerShutdown))
		requireAttachmentCleanup(t, d)
		d.mu.Lock()
		require.Empty(t, d.suspended)
		d.mu.Unlock()
		require.Nil(t, ac.currentAttachmentSession())
		sess.mu.Lock()
		_, registered := sess.attachments[ac]
		sess.mu.Unlock()
		require.False(t, registered)
	})
}

func TestWithSuspendedSafetyExpiryOption(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	configured := New(nil, newStubClock(t), log, WithSuspendedSafetyExpiry(37*time.Second))
	require.Equal(t, 37*time.Second, configured.suspendedSafetyExpiry)
	unchanged := New(nil, newStubClock(t), log, WithSuspendedSafetyExpiry(0))
	require.Equal(t, defaultSuspendedSafetyExpiry, unchanged.suspendedSafetyExpiry)
}

func TestSessionViewAttachedRequiresInteractivePresence(t *testing.T) {
	active := &attachedClient{}
	activating := &attachedClient{}
	activating.lifecycle.activity = attachmentActivating
	suspended := &attachedClient{}
	suspended.lifecycle.activity = attachmentSuspended

	tests := []struct {
		name        string
		attachments []*attachedClient
		want        bool
	}{
		{name: "active only", attachments: []*attachedClient{active}, want: true},
		{name: "activating only", attachments: []*attachedClient{activating}, want: true},
		{name: "suspended only", attachments: []*attachedClient{suspended}, want: false},
		{name: "mixed active and suspended", attachments: []*attachedClient{suspended, active}, want: true},
		{name: "mixed activating and suspended", attachments: []*attachedClient{suspended, activating}, want: true},
		{name: "no attachments", attachments: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &session{sessionCore: sessionCore{attachments: map[*attachedClient]struct{}{}}}
			for _, ac := range tt.attachments {
				s.attachments[ac] = struct{}{}
			}
			require.Equal(t, tt.want, s.snapshotView(viewOptions{}).attached)
			s.mu.Lock()
			require.Equal(t, tt.want, sessionInteractivelyAttachedLocked(s))
			s.mu.Unlock()
			// Membership is retained for every registered participant, suspended
			// entries included, and remains activatable.
			require.Len(t, s.attachments, len(tt.attachments))
		})
	}
}

func TestHandleListAttachedExcludesSuspendedOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY(t))
		d.attachCoordinator(sess, nil, ac, true)
		require.True(t, listedAttached(t, d, sess.name), "an active attachment is observably attached")

		token := sess.captureAttachmentCapability(ac, ac.transport())
		ac.installTestAttachmentCapability(token)
		require.NoError(t, d.suspendAttachment(token, protocol.SuspendAttachment{RequestID: 1}))
		require.False(t, listedAttached(t, d, sess.name), "a suspended-only session is not attached in the listing")
		require.True(t, slices.Contains(sess.snapshotAttachments(), ac), "suspension retains membership for activation")

		require.NoError(t, d.activateAttachment(ac, ac.transportSnapshot(), protocol.ActivateAttachment{
			RequestID: 2,
			Target:    protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name},
			Size:      ac.sizeSnapshot(),
		}))
		require.True(t, listedAttached(t, d, sess.name), "activation restores observably attached presence")
	})
}

func listedAttached(t *testing.T, d *Daemon, name string) bool {
	t.Helper()
	for _, info := range listSessions(t, d).Sessions {
		if info.Name == name {
			require.Equal(t, protocol.SessionUp, info.State)
			return info.Attached
		}
	}
	t.Fatalf("session %q missing from the listing", name)
	return false
}
