package daemon

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

func TestActiveLinkLossParksActiveAndDetachesCoordinator(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	transport := &closeTrackingTransport{}
	sess, active, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), transport)
	require.NoError(t, err)
	token := active.resumeToken
	rc := sess.renderCoordinator()
	require.NotNil(t, rc.attachmentLease(active))

	d.clientGone(sess, active, transport, false)

	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.Equal(t, attachmentActive, parked.role)
	require.Nil(t, sess.client)
	require.Nil(t, rc.attachmentLease(active), "active loss should detach coordinator ownership")
}

func TestSnatchedLinkLossParksWithoutDisturbingActiveOwner(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTransport := &closeTrackingTransport{}
	sess, old, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	oldToken := old.resumeToken

	activeTransport := &closeTrackingTransport{}
	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), activeTransport)
	require.NoError(t, err)
	d.attachmentCleanupWg.Wait()
	lease := sess.renderCoordinator().attachmentLease(active)
	require.NotNil(t, lease)

	d.runConnLoop(old)

	d.mu.Lock()
	parked := d.parked[oldToken]
	d.mu.Unlock()
	require.NotNil(t, parked, "resumable snatched attachment should survive link loss")
	require.Equal(t, attachmentSnatched, parked.role)
	require.Same(t, active, sess.client)
	require.Same(t, lease, sess.renderCoordinator().attachmentLease(active))
	require.False(t, activeTransport.Closed())
}

type sendErrorTransport struct {
	closeTrackingTransport
}

func (*sendErrorTransport) Send(ports.Frame) error { return errors.New("send failed") }

func TestSnatchedPanelSendFailureParksResumableAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	failed := &sendErrorTransport{}
	sess, waiting, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), failed)
	require.NoError(t, err)
	token := waiting.resumeToken

	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), &closeTrackingTransport{})
	require.NoError(t, err)
	d.attachmentCleanupWg.Wait()

	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked, "failed snatched panel should park a resumable attachment")
	require.Equal(t, attachmentSnatched, parked.role)
	require.Nil(t, waiting.transport(), "parking should revoke the exact failed transport")
	require.True(t, failed.Closed())
	require.Same(t, active, sess.client)
}

func TestStaleSnatchedTransportCannotParkFreshIncarnation(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTransport := &closeTrackingTransport{}
	sess, waiting, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), &closeTrackingTransport{})
	require.NoError(t, err)
	d.attachmentCleanupWg.Wait()
	stale := sess.attachmentToken(waiting, oldTransport)
	fresh := &closeTrackingTransport{}
	waiting.replaceTransport(fresh)

	require.False(t, d.parkOrDropSnatchedAttachment(stale))
	d.mu.Lock()
	_, parked := d.parked[waiting.resumeToken]
	d.mu.Unlock()
	require.False(t, parked)
	require.Equal(t, attachmentSnatched, sess.attachmentRole(waiting))
	require.Same(t, fresh, waiting.transport())
	require.False(t, fresh.Closed())
	require.Same(t, active, sess.client)
}

func TestSnatchedParkExpiryDoesNotAffectActiveOwner(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	waitingTransport := &closeTrackingTransport{}
	sess, waiting, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), waitingTransport)
	require.NoError(t, err)
	token := waiting.resumeToken
	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), &closeTrackingTransport{})
	require.NoError(t, err)
	d.attachmentCleanupWg.Wait()
	lease := sess.renderCoordinator().attachmentLease(active)

	d.runConnLoop(waiting)
	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked)

	d.expireParked(token, parked)

	d.mu.Lock()
	_, retained := d.parked[token]
	d.mu.Unlock()
	require.False(t, retained)
	require.Same(t, active, sess.client)
	require.Same(t, lease, sess.renderCoordinator().attachmentLease(active))
}

func TestNonzeroResumeTokenOverridesOrdinaryRoutingFallbacks(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	work, active, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), &closeTrackingTransport{})
	require.NoError(t, err)
	const unknownToken = uint64(0xdecafbad)
	for _, tc := range []struct {
		name   string
		intent uint8
		target string
	}{
		{name: "attach existing", intent: ports.IntentAttach, target: "work"},
		{name: "create unique", intent: ports.IntentNew, target: "fallback"},
		{name: "create ephemeral", intent: ports.IntentEphemeral},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := helloResumeCapable(tc.intent, tc.target, unknownToken)
			routedSession, routedClient, routeErr := d.route(h, &closeTrackingTransport{})
			require.Error(t, routeErr)
			require.Nil(t, routedSession)
			require.Nil(t, routedClient)
			var protocolErr *protoErr
			require.ErrorAs(t, routeErr, &protocolErr)
			require.Equal(t, ports.ErrNoSuchSession, protocolErr.code)
			require.Same(t, active, work.client)
			d.mu.Lock()
			require.Len(t, d.sessions, 1)
			require.Nil(t, d.findByNameLocked("fallback"))
			d.mu.Unlock()
		})
	}

	_, replacement, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), &closeTrackingTransport{})
	require.NoError(t, err, "a zero token keeps ordinary attach semantics")
	require.NotSame(t, active, replacement)
	require.Same(t, replacement, work.client)
	require.Equal(t, attachmentSnatched, work.attachmentRole(active))
}

func TestHandleHelloExpiredSnatchedResumeFailsClosedAfterSessionEnds(t *testing.T) {
	workPTY, releaseWork := newBlockingPTY(t)
	keeperPTY, releaseKeeper := newBlockingPTY(t)
	fallbackPTY, releaseFallback := newBlockingPTY(t)
	defer releaseWork()
	defer releaseKeeper()
	defer releaseFallback()
	d := newTestDaemon(t, newFactorySeq(t, workPTY, keeperPTY, fallbackPTY), stubClock{})

	oldTransport := &closeTrackingTransport{}
	work, old, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	oldToken := old.resumeToken

	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), &closeTrackingTransport{})
	require.NoError(t, err)
	d.attachmentCleanupWg.Wait()
	require.Equal(t, attachmentSnatched, work.attachmentRole(old))
	require.Same(t, active, work.client)

	d.runConnLoop(old)
	d.mu.Lock()
	parked := d.parked[oldToken]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.Equal(t, attachmentSnatched, parked.role)

	keeper, _, err := d.route(helloResumeCapable(ports.IntentNew, "keeper", 0), &closeTrackingTransport{})
	require.NoError(t, err)
	require.NoError(t, d.killSession(work, ports.ReasonSessionKilled, false))

	d.mu.Lock()
	_, tokenRetained := d.parked[oldToken]
	stoppedWork, lifecycleRetained := d.stopped["work"]
	d.mu.Unlock()
	require.False(t, tokenRetained, "terminal lifecycle teardown must purge the parked token")
	require.True(t, lifecycleRetained, "the stopped lifecycle makes attach fallback observable")

	reconnect := &closeTrackingTransport{}
	d.handleHello(reconnect, ports.Frame{
		Type:    ports.MsgHello,
		Payload: ports.MarshalHello(helloResumeCapable(ports.IntentResume, "work", oldToken)),
	})

	frames := reconnect.Sends()
	require.Len(t, frames, 1)
	require.Equal(t, ports.MsgError, frames[0].Type)
	wireErr, err := ports.UnmarshalErrorMsg(frames[0].Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ErrNoSuchSession, wireErr.Code)
	require.True(t, reconnect.Closed())
	d.mu.Lock()
	require.Same(t, keeper, d.findByNameLocked("keeper"))
	require.Nil(t, d.findByNameLocked("work"), "expired resume must not recreate the stopped session")
	require.Equal(t, stoppedWork, d.stopped["work"])
	require.Len(t, d.sessions, 1)
	d.mu.Unlock()
	require.Nil(t, work.client, "expired resume must not reclaim the ended lifecycle")
	require.Nil(t, active.currentSession())
}

func TestSnatchedResumeHandshakeSendsWelcomeAndPanelWithoutReplacingOwner(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTransport := &closeTrackingTransport{}
	sess, old, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	token := old.resumeToken
	d.clientGone(sess, old, oldTransport, false)

	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), &closeTrackingTransport{})
	require.NoError(t, err)

	resumedTransport := &closeTrackingTransport{}
	hello := helloResumeCapable(ports.IntentResume, "work", token)
	d.handleHello(resumedTransport, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)})

	frames := resumedTransport.Sends()
	require.GreaterOrEqual(t, len(frames), 2)
	require.Equal(t, ports.MsgWelcome, frames[0].Type)
	welcome, err := ports.UnmarshalWelcome(frames[0].Payload)
	require.NoError(t, err)
	newToken := welcome.ResumeToken
	require.NotZero(t, newToken)
	require.NotEqual(t, token, newToken, "successful snatched resume rotates the credential")
	require.Equal(t, newToken, old.resumeToken, "the Welcome publishes the one rotated credential")
	require.Equal(t, ports.MsgOutput, frames[1].Type)
	output, err := ports.UnmarshalOutput(frames[1].Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "Session snatched")
	d.mu.Lock()
	_, oldTokenParked := d.parked[token]
	parked := d.parked[newToken]
	d.mu.Unlock()
	require.False(t, oldTokenParked, "the consumed credential cannot be reused")
	require.NotNil(t, parked)
	require.Equal(t, attachmentSnatched, parked.role, "the rotated credential remains bound to the snatched role")
	require.Same(t, active, sess.client)

	replay := &closeTrackingTransport{}
	d.handleHello(replay, ports.Frame{
		Type:    ports.MsgHello,
		Payload: ports.MarshalHello(helloResumeCapable(ports.IntentResume, "work", token)),
	})
	replayFrames := replay.Sends()
	require.Len(t, replayFrames, 1)
	require.Equal(t, ports.MsgError, replayFrames[0].Type)
	replayErr, err := ports.UnmarshalErrorMsg(replayFrames[0].Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ErrNoSuchSession, replayErr.Code)
	require.True(t, replay.Closed())
	require.Same(t, active, sess.client, "replaying the old credential must not replace active ownership")
	d.mu.Lock()
	require.Same(t, parked, d.parked[newToken])
	d.mu.Unlock()
}

func TestResumeWelcomeRechecksRoleAfterReplacement(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTransport := &closeTrackingTransport{}
	sess, resumed, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	token := resumed.resumeToken
	d.clientGone(sess, resumed, oldTransport, false)

	blocked := newWelcomeBlockingTransport(t)
	handshakeDone := make(chan struct{})
	go func() {
		hello := helloResumeCapable(ports.IntentResume, "work", token)
		d.handleHello(blocked.tr, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)})
		close(handshakeDone)
	}()
	awaitTestCompletion(t, blocked.welcomeEntered, "resume did not start Welcome")
	welcome := awaitFrame(t, blocked.sends, ports.MsgWelcome)
	require.Equal(t, ports.MsgWelcome, welcome.Type)
	requireNoCoordinatorOutputFrame(t, blocked.sends)
	resumedLease := sess.renderCoordinator().attachmentLease(resumed)
	require.NotNil(t, resumedLease)
	require.False(t, resumedLease.ready, "blocked Welcome must keep the resumed lease unready")

	resumeGateFrozen := make(chan struct{})
	var frozenOnce sync.Once
	d.afterRoleEffectGateFrozen = func(_ string, ac *attachedClient) {
		if ac == resumed {
			frozenOnce.Do(func() { close(resumeGateFrozen) })
		}
	}
	type attachResult struct {
		ac  *attachedClient
		err error
	}
	replacementResult := make(chan attachResult, 1)
	go func() {
		_, replacement, routeErr := d.route(
			helloResumeCapable(ports.IntentAttach, "work", 0),
			&closeTrackingTransport{},
		)
		replacementResult <- attachResult{ac: replacement, err: routeErr}
	}()
	awaitTestCompletion(t, resumeGateFrozen, "replacement did not freeze the resumed Welcome role")

	blocked.release()
	replacement := awaitTestValue(t, replacementResult, "replacement did not finish after Welcome")
	require.NoError(t, replacement.err)
	d.attachmentCleanupWg.Wait()
	panelFrame := awaitFrame(t, blocked.sends, ports.MsgOutput)
	panel, err := ports.UnmarshalOutput(panelFrame.Payload)
	require.NoError(t, err)
	require.Zero(t, panel.BaseStateNum, "post-Welcome snatched panel must reset output state")
	require.Contains(t, string(panel.Data), "Session snatched")
	requireNoCoordinatorOutputFrame(t, blocked.sends)

	require.Same(t, replacement.ac, sess.client)
	require.Equal(t, attachmentSnatched, sess.attachmentRole(resumed))
	rc := sess.renderCoordinator()
	rc.mu.Lock()
	require.False(t, resumedLease.ready, "replacement during Welcome must not ready the stale active lease")
	rc.mu.Unlock()
	require.Nil(t, rc.attachmentLease(resumed))
	require.NotNil(t, rc.attachmentLease(replacement.ac))

	blocked.finish()
	awaitTestCompletion(t, handshakeDone, "resumed snatched connection did not continue after replacement")
}

func TestSnatchedResumePreservesOwnerCapabilityAndCanReclaim(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTransport := &closeTrackingTransport{}
	sess, waiting, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	token := waiting.resumeToken
	waiting.setClientTheme(themeui.Theme{
		Foreground: renderer.RGB{R: 90, G: 10, B: 10},
		Background: renderer.RGB{R: 20, G: 1, B: 1},
		HasFG:      true, HasBG: true, Known: true,
	})
	d.clientGone(sess, waiting, oldTransport, false)

	activeTransport := &closeTrackingTransport{}
	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), activeTransport)
	require.NoError(t, err)
	rc := sess.renderCoordinator()
	activeGeneration := active.roleGeneration.Load()
	activeLease := rc.attachmentLease(active)
	require.NotNil(t, activeLease)
	activeTheme := themeui.Theme{
		Foreground: renderer.RGB{R: 10, G: 80, B: 20},
		Background: renderer.RGB{R: 1, G: 15, B: 3},
		HasFG:      true, HasBG: true, Known: true,
	}
	active.setClientTheme(activeTheme)
	require.True(t, d.applyHostTheme(sess, active, activeTheme, false))
	assertSessionDefaultColors(t, sess, activeTheme.Foreground, activeTheme.Background)

	resumedTransport := &closeTrackingTransport{}
	resumedSess, resumed, ok, err := d.resumeParked(
		helloResumeCapable(ports.IntentResume, "work", token),
		resumedTransport,
		domain.Size{Cols: 80, Rows: 24},
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, sess, resumedSess)
	require.Same(t, waiting, resumed)
	require.Same(t, active, sess.client)
	require.Equal(t, activeGeneration, active.roleGeneration.Load())
	require.Same(t, activeLease, rc.attachmentLease(active))
	require.Nil(t, rc.attachmentLease(waiting), "snatched resume must not acquire a render lease")
	require.Equal(t, attachmentSnatched, sess.attachmentRole(waiting))
	assertSessionDefaultColors(t, sess, activeTheme.Foreground, activeTheme.Background)

	resumedGeneration := waiting.roleGeneration.Load()
	require.False(t, d.handleSnatchedClientFrame(sess.attachmentToken(waiting, resumedTransport), ports.Frame{
		Type:    ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{Data: []byte{'r'}}),
	}))
	d.attachmentCleanupWg.Wait()

	require.Same(t, waiting, sess.client)
	require.Equal(t, attachmentActive, sess.attachmentRole(waiting))
	require.Greater(t, waiting.roleGeneration.Load(), resumedGeneration)
	require.NotNil(t, rc.attachmentLease(waiting))
	require.Equal(t, attachmentSnatched, sess.attachmentRole(active))
}

func TestOrdinaryAttachDemotesParkedActiveAndAllowsSnatchedResume(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	oldTransport := &closeTrackingTransport{}
	sess, old, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), oldTransport)
	require.NoError(t, err)
	token := old.resumeToken
	d.clientGone(sess, old, oldTransport, false)

	activeTransport := &closeTrackingTransport{}
	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), activeTransport)
	require.NoError(t, err)

	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked, "ordinary attach must preserve the predecessor's resume token")
	require.Equal(t, attachmentSnatched, parked.role)
	require.Equal(t, token, old.resumeToken)

	resumedTransport := &closeTrackingTransport{}
	resumedSess, resumed, ok, err := d.resumeParked(
		helloResumeCapable(ports.IntentResume, "work", token),
		resumedTransport,
		domain.Size{Cols: 80, Rows: 24},
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, sess, resumedSess)
	require.Same(t, old, resumed)
	require.Equal(t, attachmentSnatched, sess.attachmentRole(old))
	require.Same(t, active, sess.client)
}
