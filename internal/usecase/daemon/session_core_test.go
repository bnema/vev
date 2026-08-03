package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
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
	require.Same(t, sess, mustLocalSession(t, attachmentSession(sess)))
	require.False(t, sess.isProxy())
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
	awaitTestCompletion(t, preCall, "snapshot goroutine did not reach the pre-call barrier")
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

	view := awaitTestValue(t, snapshotted, "snapshotView did not complete after sessionCore.mu was released")
	require.True(t, view.attached)
	require.Equal(t, 2, view.tabCount)
	require.Equal(t, 0, view.defaultTab)
	require.Equal(t, 1, testAttachmentTabIndex(sess))
}

func TestLocalCreateThenKillRemovesLiveRegistryEntry(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	sess, err := createSessionForTest(d, "0", true, "/tmp", defaultSize, terminalEnv{}, nil)
	require.NoError(t, err)
	d.mu.Lock()
	registered := d.sessions[sess.id]
	d.mu.Unlock()
	require.Same(t, sess, registered)

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
	d.mu.Lock()
	_, live := d.sessions[sess.id]
	d.mu.Unlock()
	require.False(t, live, "killed local session must not remain in the live registry")
	d.sessWg.Wait()
}

func TestAttachmentSessionRegistryUsesExactIdentity(t *testing.T) {
	d := &Daemon{sessions: make(map[domain.SessionID]attachmentSession)}
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

func TestInitialMetadataSkipsInvalidSnapshot(t *testing.T) {
	d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	hello := ports.Hello{
		Version: ports.ProtocolVersion,
		Intent:  ports.IntentNew,
		Name:    "remote-work",
		Size:    defaultSize,
	}
	tr := newWelcomeBlockingTransport(t)
	done := make(chan struct{})
	go func() {
		d.handleHello(tr.tr, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)})
		close(done)
	}()

	awaitTestCompletion(t, tr.welcomeEntered, "handshake did not send Welcome")
	sess := firstSession(d)
	require.NotNil(t, sess)
	sess.mu.Lock()
	invalidTabIndex := len(sess.tabs)
	sess.mu.Unlock()
	selectTestAttachmentTab(sess, invalidTabIndex)
	// finish (not just release) also closes recvDone, so runConnLoop's Recv
	// fails immediately after the first paint and the handshake goroutine
	// returns instead of blocking on further input.
	tr.finish()
	awaitTestCompletion(t, done, "invalid metadata snapshot did not end the handshake")

	welcome := <-tr.sends
	require.Equal(t, ports.MsgWelcome, welcome.Type)
	select {
	case frame := <-tr.sends:
		t.Fatalf("invalid metadata snapshot was sent as frame type %d", frame.Type)
	default:
	}

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
	d.sessWg.Wait()
}

func mustLocalSession(t *testing.T, entry attachmentSession) *session {
	t.Helper()
	sess, ok := localSession(entry)
	require.True(t, ok)
	return sess
}
