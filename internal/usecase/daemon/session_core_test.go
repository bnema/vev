package daemon

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func clearAttachmentsForTest(sess *session) {
	for _, ac := range sess.snapshotAttachments() {
		sess.unregisterAttachment(ac)
	}
}

func clearAttachmentsForTestLocked(sess *session) {
	for _, ac := range sess.snapshotAttachmentsLocked() {
		sess.unregisterAttachmentLocked(ac)
	}
}

func TestSessionCorePreservesLocalIdentityAndPromotedMutex(t *testing.T) {
	sess := &session{}
	sess.id = domain.SessionID("local")
	sess.name = "work"
	sess.tabs = []*tab{{name: "shell"}}
	ac := &attachedClient{}

	core := sess.core()
	require.Same(t, &sess.sessionCore, core)
	require.Equal(t, sessionCapabilities{}, core.caps, "zero-value local sessions must retain all local capabilities")

	core.mu.Lock()
	require.False(t, sess.mu.TryLock(), "sessionCore.mu must be the mutex promoted as session.mu")
	core.registerAttachmentLocked(ac)
	require.Equal(t, true, sess.attachmentRegisteredLocked(ac))
	sess.tabs = append(sess.tabs, &tab{name: "logs"})
	require.Len(t, sess.tabs, 2)
	core.mu.Unlock()

	require.True(t, sess.mu.TryLock())
	sess.mu.Unlock()
}

//testpolicy:realtime a goroutine blocked on sync.Mutex is not durably blocked, so no bubble can settle the negative check below
func TestSessionSnapshotViewUsesPromotedMutexForLocalTabAndRoleState(t *testing.T) {
	sess := &session{
		sessionCore: sessionCore{id: domain.SessionID("local"), name: "work"},
		tabs:        []*tab{{stableID: "tab-shell", name: "shell"}},
	}
	ac := &attachedClient{}

	// Mutate local-only tab state and shared attachment role state under the
	// promoted core mutex. snapshotView must observe the same mutex before it
	// can publish either field.
	sess.core().mu.Lock()
	sess.tabs = append(sess.tabs, &tab{stableID: "tab-logs", name: "logs"})
	sess.registerAttachmentLocked(ac)
	sess.activateAttachmentViewLocked(ac, 1)

	preCall := make(chan struct{})
	allowCall := make(chan struct{})
	snapshotted := make(chan sessionView, 1)
	go func() {
		// The barrier places the goroutine immediately before snapshotView, so
		// the lock is definitely held before the call can begin.
		close(preCall)
		<-allowCall
		snapshotted <- sess.snapshotView(viewOptions{})
	}()
	select {
	case <-preCall:
	case <-time.After(testWaitTimeout):
		t.Fatal("snapshot goroutine did not reach the pre-call barrier")
	}
	close(allowCall)

	// Give the released call a bounded opportunity to acquire the mutex; it
	// must remain blocked until the owner releases it below.
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-snapshotted:
		t.Fatal("snapshotView completed while sessionCore.mu guarded local session state")
	case <-timer.C:
	}
	require.Equal(t, true, sess.attachmentRegisteredLocked(ac))
	sess.core().mu.Unlock()

	var view sessionView
	select {
	case view = <-snapshotted:
	case <-time.After(testWaitTimeout):
		t.Fatal("snapshotView did not complete after sessionCore.mu was released")
	}
	require.True(t, view.attached)
	require.Equal(t, 2, view.tabCount)
	require.Equal(t, 0, view.defaultTab)
	require.Equal(t, 1, testAttachmentTabIndex(sess))
}

func TestLocalCreateThenKillRemovesLiveRegistryEntry(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactory(t, pty), newStubClock(t))

	sess, err := createSessionForTest(d, "0", true, "/tmp", defaultSize, terminalEnv{}, nil)
	require.NoError(t, err)
	d.mu.Lock()
	registered := d.sessions[sess.id]
	d.mu.Unlock()
	require.Same(t, sess, registered)

	require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, false))
	d.mu.Lock()
	_, live := d.sessions[sess.id]
	d.mu.Unlock()
	require.False(t, live, "killed local session must not remain in the live registry")
	d.sessWg.Wait()
}

func TestAttachmentSessionRegistryUsesExactIdentity(t *testing.T) {
	d := &Daemon{sessions: make(map[domain.SessionID]*session)}
	first := &session{}
	first.id = domain.SessionID("same")
	second := &session{}
	second.id = first.id

	require.True(t, d.registerSessionLocked(first))
	require.False(t, d.registerSessionLocked(first), "duplicate registration must be rejected")
	require.False(t, d.registerSessionLocked(second), "a different pointer with the same ID must be rejected")
	require.False(t, d.unregisterSessionLocked(second), "unregister must not remove a replacement pointer")
	require.Same(t, first, d.sessions[first.id])
	require.True(t, d.unregisterSessionLocked(first))
	require.NotContains(t, d.sessions, first.id)
	require.False(t, d.unregisterSessionLocked(first))
}

func TestSessionCoreLockOrderUsesImmutableIDs(t *testing.T) {
	first := &session{}
	first.id = domain.SessionID("a")
	second := &session{}
	second.id = domain.SessionID("z")

	unlock := lockAttachmentSessions(second, first)
	require.False(t, first.core().mu.TryLock())
	require.False(t, second.core().mu.TryLock())
	unlock()

	require.True(t, first.core().mu.TryLock())
	first.core().mu.Unlock()
	require.True(t, second.core().mu.TryLock())
	second.core().mu.Unlock()
}

func mustLocalSession(t *testing.T, entry *session) *session {
	t.Helper()
	require.NotNil(t, entry)
	return entry
}

// requireQueuedFrame is the bubble form of awaitFrame: after quiescence it
// consumes queued envelopes until one decodes to want, failing if none does.
func requireQueuedFrame(t *testing.T, ch <-chan wire.Envelope, want string) wire.Envelope {
	t.Helper()
	synctest.Wait()
	for {
		select {
		case f := <-ch:
			if envelopeMessageName(t, f.Payload) == want {
				return f
			}
		default:
			t.Fatalf("envelope %s was not sent", want)
			return wire.Envelope{}
		}
	}
}

//testpolicy:realtime mutex-blocked goroutines are invisible to synctest
// receiveBounded is for tests that cannot run in a synctest bubble (goroutines
// blocked on sync.Mutex never become durably blocked): one bounded failure
// guard, no polling.
func receiveBounded[T any](t *testing.T, ch <-chan T, failure string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(testWaitTimeout):
		t.Fatal(failure)
		var zero T
		return zero
	}
}
