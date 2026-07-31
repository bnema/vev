package daemon

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

func TestSessionAttachmentRole(t *testing.T) {
	active := &attachedClient{}
	waiting := &attachedClient{}
	unknown := &attachedClient{}
	sess := &session{sessionCore: sessionCore{client: active, snatched: map[*attachedClient]struct{}{waiting: {}}}}

	require.Equal(t, attachmentActive, sess.attachmentRole(active))
	require.Equal(t, attachmentSnatched, sess.attachmentRole(waiting))
	require.Equal(t, attachmentDetached, sess.attachmentRole(unknown))
}

func TestTransitionAttachmentReplacesActiveMembership(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := &session{sessionCore: sessionCore{id: domain.SessionID("work")}}
	oldTransport := &closeTrackingTransport{}
	old := &attachedClient{tr: oldTransport}
	old.setSession(sess)
	sess.client = old
	d.sessions[sess.id] = sess

	newTransport := &closeTrackingTransport{}
	next := &attachedClient{tr: newTransport}
	expected := next.transportSnapshot()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              next,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: expected,
	})
	require.NoError(t, err)

	sess.mu.Lock()
	require.Same(t, next, sess.client)
	require.Contains(t, sess.snatched, old)
	_, nextSnatched := sess.snatched[next]
	require.False(t, nextSnatched)
	sess.mu.Unlock()
	require.Equal(t, attachmentActive, sess.attachmentRole(next))
	require.Equal(t, attachmentSnatched, sess.attachmentRole(old))
	require.True(t, result.published.current())
	require.True(t, result.displaced.current())
	require.Equal(t, uint64(1), result.published.generation)
	require.Equal(t, uint64(1), result.displaced.generation)
	require.Same(t, sess, next.currentSession())
	require.Same(t, sess, old.currentSession())
}

func TestDeferredAttachmentCleanupKeepsReplacementOpenAndSendsResetPanel(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := &session{sessionCore: sessionCore{id: domain.SessionID("work")}}
	oldTransport := &closeTrackingTransport{}
	old := &attachedClient{
		tr:     oldTransport,
		output: newOutputStateStream(),
		size:   domain.Size{Cols: 80, Rows: 24},
	}
	old.initOverlays()
	old.setThemeForTest(themeui.Theme{})
	old.setSession(sess)
	sess.client = old
	d.sessions[sess.id] = sess

	next := &attachedClient{tr: &closeTrackingTransport{}}
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              next,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: next.transportSnapshot(),
	})
	require.NoError(t, err)

	d.deferAttachmentTransitionCleanups(result)
	d.attachmentCleanupWg.Wait()

	require.False(t, oldTransport.Closed(), "snatched transport must stay open")
	frames := oldTransport.Sends()
	require.Len(t, frames, 1)
	require.Equal(t, ports.MsgOutput, frames[0].Type)
	output, err := ports.UnmarshalOutput(frames[0].Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "Session snatched")
}

func TestActiveFrameRevalidatesExactRoleBeforeEveryEffect(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*attachedClient, *session, *transactionalResizePTY)
		frame  func() ports.Frame
		assert func(*testing.T, *Daemon, *session, *attachedClient, *attachedClient, *transactionalResizePTY, *datagramTestTransport)
	}{
		{
			name:  "forwarded input",
			frame: func() ports.Frame { return frameInput([]byte("stale")) },
			assert: func(t *testing.T, _ *Daemon, _ *session, _ *attachedClient, _ *attachedClient, pty *transactionalResizePTY, _ *datagramTestTransport) {
				require.Empty(t, pty.writes(), "stale input reached the PTY")
			},
		},
		{
			name:  "key action",
			frame: func() ports.Frame { return frameInput([]byte("\x1b ")) },
			assert: func(t *testing.T, _ *Daemon, _ *session, old, next *attachedClient, _ *transactionalResizePTY, _ *datagramTestTransport) {
				require.False(t, old.overlays.paletteActive(), "stale key action mutated the displaced overlay")
				require.False(t, next.overlays.paletteActive(), "stale key action mutated the active overlay")
			},
		},
		{
			name: "mouse input",
			setup: func(_ *attachedClient, sess *session, _ *transactionalResizePTY) {
				pane := sess.activeTab().focusedPane()
				pane.mu.Lock()
				pane.screen.Write([]byte("\x1b[?1000h"))
				pane.mu.Unlock()
			},
			frame: func() ports.Frame { return frameInput([]byte("\x1b[<0;1;2M")) },
			assert: func(t *testing.T, _ *Daemon, _ *session, _ *attachedClient, _ *attachedClient, pty *transactionalResizePTY, _ *datagramTestTransport) {
				require.Empty(t, pty.writes(), "stale mouse input reached the PTY")
			},
		},
		{
			name: "image push",
			frame: func() ports.Frame {
				return ports.Frame{Type: ports.MsgImagePush, Payload: ports.MarshalImagePush(ports.ImagePush{Mime: "image/png", Data: []byte("stale-image")})}
			},
			assert: func(t *testing.T, _ *Daemon, sess *session, _ *attachedClient, _ *attachedClient, pty *transactionalResizePTY, _ *datagramTestTransport) {
				sess.mu.Lock()
				files := append([]string(nil), sess.clipFiles...)
				sess.mu.Unlock()
				require.Empty(t, files, "stale image push created a session clipboard file")
				require.Empty(t, pty.writes(), "stale image path reached the PTY")
			},
		},
		{
			name: "resize",
			frame: func() ports.Frame {
				return ports.Frame{Type: ports.MsgResize, Payload: ports.MarshalResize(ports.Resize{Size: domain.Size{Cols: 120, Rows: 40}})}
			},
			assert: func(t *testing.T, _ *Daemon, sess *session, old, _ *attachedClient, pty *transactionalResizePTY, _ *datagramTestTransport) {
				require.Empty(t, pty.requested(), "stale resize reached a session PTY")
				require.Nil(t, sess.renderCoordinator().attachmentLease(old), "stale resize rebound the coordinator to the displaced client")
			},
		},
		{
			name: "theme",
			frame: func() ports.Frame {
				return ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(ports.Theme{
					HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
					HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
				})}
			},
			assert: func(t *testing.T, _ *Daemon, _ *session, old, _ *attachedClient, _ *transactionalResizePTY, _ *datagramTestTransport) {
				require.False(t, old.getClientTheme().HasFG, "stale theme mutated the displaced client")
			},
		},
		{
			name: "client notice",
			frame: func() ports.Frame {
				return ports.Frame{Type: ports.MsgClientNotice, Payload: ports.MarshalClientNotice(ports.ClientNotice{Action: ports.ClientNoticeClipboardFallback})}
			},
			assert: func(t *testing.T, d *Daemon, _ *session, old, next *attachedClient, _ *transactionalResizePTY, _ *datagramTestTransport) {
				require.Empty(t, d.notices.history(), "stale notice mutated daemon history")
				oldToasts, _ := visibleToasts(old)
				nextToasts, _ := visibleToasts(next)
				require.Empty(t, oldToasts)
				require.Empty(t, nextToasts)
			},
		},
		{
			name: "ack",
			setup: func(old *attachedClient, _ *session, _ *transactionalResizePTY) {
				old.output.next = 2
			},
			frame: func() ports.Frame {
				return ports.Frame{Type: ports.MsgAck, Payload: ports.MarshalAck(ports.Ack{AckedStateNum: 1})}
			},
			assert: func(t *testing.T, _ *Daemon, _ *session, old, _ *attachedClient, _ *transactionalResizePTY, _ *datagramTestTransport) {
				require.Equal(t, uint64(2), old.output.outstanding(), "stale ack retired displaced output state")
			},
		},
		{
			name:  "ping",
			frame: func() ports.Frame { return ports.Frame{Type: ports.MsgPing} },
			assert: func(t *testing.T, _ *Daemon, _ *session, _ *attachedClient, _ *attachedClient, _ *transactionalResizePTY, tr *datagramTestTransport) {
				select {
				case frame := <-tr.sends:
					t.Fatalf("stale ping wrote frame type %d", frame.Type)
				default:
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pty := &transactionalResizePTY{}
			d, sess, old, _ := newManualSessionWithPTYs(t, pty)
			d.tempDir = t.TempDir()
			oldTransport := newDatagramTestTransport()
			old.replaceTransport(oldTransport)
			old.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: old}, &d.bindings)
			rc := d.attachCoordinator(sess, nil, old, true)
			token := sess.attachmentToken(old, oldTransport)
			token.lease = rc.attachmentLease(old)
			require.True(t, token.activeCurrent())
			if tt.setup != nil {
				tt.setup(old, sess, pty)
			}

			next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
			next.initOverlays()
			var transition attachmentTransitionResult
			d.afterActiveFrameDispatch = func(attachmentRoleToken) {
				d.afterActiveFrameDispatch = nil
				var err error
				transition, err = d.transitionAttachment(attachmentTransitionRequest{
					target: sess, next: next, expectedRole: attachmentDetached, targetRole: attachmentActive,
					expectedTransport: next.transportSnapshot(), ready: true,
				})
				require.NoError(t, err)
			}

			d.handleActiveClientFrame(token, tt.frame())
			for _, cleanup := range transition.cleanups {
				cleanup.finish()
			}
			tt.assert(t, d, sess, old, next, pty, oldTransport)
		})
	}
}

func TestKillSessionClosesActiveAndAllSnatchedClients(t *testing.T) {
	pty := &transactionalResizePTY{}
	d, sess, first, _ := newManualSessionWithPTYs(t, pty)
	firstTransport := newDatagramTestTransport()
	first.replaceTransport(firstTransport)

	secondTransport := newDatagramTestTransport()
	second := &attachedClient{tr: secondTransport, output: newOutputStateStream(), size: first.size}
	second.initOverlays()
	firstReplacement, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: second, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: second.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	for _, cleanup := range firstReplacement.cleanups {
		cleanup.finish()
	}

	thirdTransport := newDatagramTestTransport()
	third := &attachedClient{tr: thirdTransport, output: newOutputStateStream(), size: first.size}
	third.initOverlays()
	secondReplacement, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: third, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: third.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	for _, cleanup := range secondReplacement.cleanups {
		cleanup.finish()
	}

	otherCtx, otherCancel := context.WithCancel(d.serveCtx)
	defer otherCancel()
	otherTransport := newDatagramTestTransport()
	otherClient := &attachedClient{tr: otherTransport, output: newOutputStateStream(), size: first.size}
	otherClient.initOverlays()
	other := &session{sessionCore: sessionCore{id: "other", name: "other", client: otherClient}, ctx: otherCtx, cancel: otherCancel}
	otherClient.setSession(other)
	d.mu.Lock()
	d.sessions[other.id] = other
	d.mu.Unlock()
	d.attachCoordinator(other, nil, otherClient, true)

	roles := []*attachedClient{first, second, third}
	generations := make([]uint64, len(roles))
	for i, ac := range roles {
		generations[i] = ac.roleGeneration.Load()
	}
	var killedLoops sync.WaitGroup
	for _, ac := range roles {
		killedLoops.Go(func() { d.runConnLoop(ac) })
	}
	otherDone := make(chan struct{})
	go func() {
		d.runConnLoop(otherClient)
		close(otherDone)
	}()
	defer func() {
		_ = firstTransport.Close()
		_ = secondTransport.Close()
		_ = thirdTransport.Close()
		_ = otherTransport.Close()
		killedLoops.Wait()
		<-otherDone
	}()

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
	d.waitNotifies()
	select {
	case <-waitGroupDone(&killedLoops):
	case <-time.After(2 * time.Second):
		t.Fatal("killed active and snatched connection loops did not retire")
	}

	sess.mu.Lock()
	require.Nil(t, sess.client)
	require.Empty(t, sess.snatched)
	sess.mu.Unlock()
	for i, ac := range roles {
		require.Nil(t, ac.currentSession())
		require.Greater(t, ac.roleGeneration.Load(), generations[i], "session kill did not invalidate role generation")
	}
	for _, transport := range []*datagramTestTransport{firstTransport, secondTransport, thirdTransport} {
		detached := awaitFrame(t, transport.sends, ports.MsgDetached)
		message, err := ports.UnmarshalDetached(detached.Payload)
		require.NoError(t, err)
		require.Equal(t, ports.ReasonSessionKilled, message.Reason)
		_, recvErr := transport.Recv()
		require.ErrorIs(t, recvErr, io.EOF)
	}
	require.Same(t, otherClient, other.client)
	require.Same(t, other, otherClient.currentSession())
	require.Equal(t, attachmentActive, other.attachmentRole(otherClient))

	otherTransport.recv <- ports.Frame{Type: ports.MsgPing}
	require.Equal(t, ports.MsgPong, awaitFrame(t, otherTransport.sends, ports.MsgPong).Type)
	select {
	case <-otherDone:
		t.Fatal("killing another session retired the active client's connection")
	default:
	}
}

func TestKillSessionRetiresParkedSnatchedAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	parkedTransport := &closeTrackingTransport{}
	sess, parkedClient, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), parkedTransport)
	require.NoError(t, err)
	parkedToken := parkedClient.resumeToken
	d.clientGone(sess, parkedClient, parkedTransport, false)

	activeTransport := &closeTrackingTransport{}
	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), activeTransport)
	require.NoError(t, err)
	d.mu.Lock()
	parked := d.parked[parkedToken]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.Equal(t, attachmentSnatched, parked.role)
	parkedGeneration := parkedClient.roleGeneration.Load()

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
	d.waitNotifies()

	require.Nil(t, parkedClient.transport(), "terminal teardown must revoke the parked link ownership")
	require.Greater(t, parkedClient.roleGeneration.Load(), parkedGeneration, "terminal teardown must invalidate the parked role generation")
	require.Zero(t, parkedClient.resumeToken)
	require.False(t, parkedClient.parked)
	select {
	case <-parked.done:
	default:
		t.Fatal("terminal teardown left the parked expiry goroutine live")
	}
	require.True(t, parkedTransport.Closed())
	require.Nil(t, parkedClient.currentSession())
	require.Nil(t, active.currentSession())
	require.True(t, activeTransport.Closed())
}

func TestDaemonShutdownClosesSnatchedClients(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})

	parkedTransport := &closeTrackingTransport{}
	sess, parkedClient, err := d.route(helloResumeCapable(ports.IntentNew, "work", 0), parkedTransport)
	require.NoError(t, err)
	parkedToken := parkedClient.resumeToken
	d.clientGone(sess, parkedClient, parkedTransport, false)

	waitingTransport := &closeTrackingTransport{}
	_, waiting, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), waitingTransport)
	require.NoError(t, err)
	activeTransport := &closeTrackingTransport{}
	_, active, err := d.route(helloResumeCapable(ports.IntentAttach, "work", 0), activeTransport)
	require.NoError(t, err)
	d.attachmentCleanupWg.Wait()

	d.mu.Lock()
	parked := d.parked[parkedToken]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.Equal(t, attachmentSnatched, parked.role)
	parkedGeneration := parkedClient.roleGeneration.Load()
	require.Equal(t, attachmentSnatched, sess.attachmentRole(waiting))
	require.Equal(t, attachmentActive, sess.attachmentRole(active))

	require.False(t, d.shutdownAll(ports.ReasonServerShutdown))
	d.waitNotifies()
	d.attachmentCleanupWg.Wait()

	require.Nil(t, parkedClient.transport(), "shutdown must revoke parked attachment link ownership")
	require.Greater(t, parkedClient.roleGeneration.Load(), parkedGeneration)
	select {
	case <-parked.done:
	default:
		t.Fatal("shutdown left the parked expiry goroutine live")
	}
	for _, client := range []*attachedClient{parkedClient, waiting, active} {
		require.Nil(t, client.currentSession())
	}
	for _, transport := range []*closeTrackingTransport{parkedTransport, waitingTransport, activeTransport} {
		require.True(t, transport.Closed())
	}
	for _, transport := range []*closeTrackingTransport{waitingTransport, activeTransport} {
		frames := transport.Sends()
		require.NotEmpty(t, frames)
		detached, err := ports.UnmarshalDetached(frames[len(frames)-1].Payload)
		require.NoError(t, err)
		require.Equal(t, ports.ReasonServerShutdown, detached.Reason)
	}
}

func TestSnatchedQuitDoesNotResetActivePaneThemeOrSize(t *testing.T) {
	pty := &transactionalResizePTY{}
	d, sess, waiting, _ := newManualSessionWithPTYs(t, pty)
	waitingTransport := &closeTrackingTransport{}
	waiting.replaceTransport(waitingTransport)

	activeTransport := &closeTrackingTransport{}
	active := &attachedClient{tr: activeTransport, output: newOutputStateStream(), size: waiting.size}
	active.initOverlays()
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: active, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: active.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	for _, cleanup := range transition.cleanups {
		cleanup.finish()
	}

	activeTheme := ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 12, G: 34, B: 56},
		HasBackground: true, Background: renderer.RGB{R: 65, G: 43, B: 21},
		TrueColor: true,
	}
	d.applyTheme(sess, active, activeTheme)
	activeSize := domain.Size{Cols: 101, Rows: 37}
	d.resize(sess, active, activeSize)
	require.Equal(t, activeSize, active.size)
	assertSessionDefaultColors(t, sess, activeTheme.Foreground, activeTheme.Background)
	requestedBeforeQuit := pty.requested()

	token := sess.attachmentToken(waiting, waitingTransport)
	require.True(t, d.handleSnatchedClientFrame(token, ports.Frame{
		Type: ports.MsgInput,
		Payload: ports.MarshalInput(ports.Input{
			Data: []byte{'q'},
		}),
	}))

	require.Equal(t, attachmentDetached, sess.attachmentRole(waiting))
	require.Same(t, active, sess.client)
	require.False(t, activeTransport.Closed())
	require.True(t, waitingTransport.Closed())
	require.Equal(t, activeSize, active.size)
	require.Equal(t, requestedBeforeQuit, pty.requested(), "snatched quit must not resize active panes")
	assertSessionDefaultColors(t, sess, activeTheme.Foreground, activeTheme.Background)
}

func TestSnatchedConnectionInputCannotReachPTY(t *testing.T) {
	writes := make(chan []byte, 1)
	pty := &transactionalResizePTY{onWrite: func(data []byte) { writes <- data }}
	d, sess, old, _ := newManualSessionWithPTYs(t, pty)
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              next,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: next.transportSnapshot(),
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(result)
	d.attachmentCleanupWg.Wait()
	awaitFrame(t, oldTransport.sends, ports.MsgOutput)

	oldTransport.recv <- frameInput([]byte("restricted"))
	require.NoError(t, oldTransport.Close())
	d.runConnLoop(old)

	select {
	case got := <-writes:
		t.Fatalf("snatched input reached PTY: %q", got)
	default:
	}
}

func TestStaleActiveKeyForwardCannotReachPTYAfterSnatch(t *testing.T) {
	writes := make(chan []byte, 1)
	pty := &transactionalResizePTY{onWrite: func(data []byte) { writes <- data }}
	d, sess, old, _ := newManualSessionWithPTYs(t, pty)
	old.replaceTransport(&closeTrackingTransport{})

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	_, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              next,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: next.transportSnapshot(),
	})
	require.NoError(t, err)

	(daemonKeyHandler{d: d, ac: old}).Forward([]byte("stale"))

	select {
	case got := <-writes:
		t.Fatalf("stale active key forward reached PTY: %q", got)
	default:
	}
}

func TestSnatchedConnectionResizeUpdatesPanelWithoutResizingSession(t *testing.T) {
	pty := &transactionalResizePTY{}
	d, sess, old, _ := newManualSessionWithPTYs(t, pty)
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              next,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: next.transportSnapshot(),
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(result)
	d.attachmentCleanupWg.Wait()
	awaitFrame(t, oldTransport.sends, ports.MsgOutput)

	want := domain.Size{Cols: 100, Rows: 30}
	oldTransport.recv <- ports.Frame{Type: ports.MsgResize, Payload: ports.MarshalResize(ports.Resize{Size: want})}
	require.NoError(t, oldTransport.Close())
	d.runConnLoop(old)

	require.Equal(t, want, old.size)
	require.Empty(t, pty.requested(), "snatched resize must not resize session PTYs")
	panel := awaitFrame(t, oldTransport.sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(panel.Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "Session snatched")
}

func TestSnatchedConnectionAcknowledgesPanelAndAnswersPing(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              next,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: next.transportSnapshot(),
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(result)
	d.attachmentCleanupWg.Wait()
	panelFrame := awaitFrame(t, oldTransport.sends, ports.MsgOutput)
	panel, err := ports.UnmarshalOutput(panelFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(1), old.output.outstanding())

	oldTransport.recv <- ports.Frame{Type: ports.MsgAck, Payload: ports.MarshalAck(ports.Ack{AckedStateNum: panel.NewStateNum})}
	oldTransport.recv <- ports.Frame{Type: ports.MsgPing}
	require.NoError(t, oldTransport.Close())
	d.runConnLoop(old)

	require.Zero(t, old.output.outstanding())
	require.Equal(t, ports.MsgPong, awaitFrame(t, oldTransport.sends, ports.MsgPong).Type)
}

func TestSnatchedConnectionThemeRepaintsPanelWithoutRecoloringSession(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              next,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: next.transportSnapshot(),
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(result)
	d.attachmentCleanupWg.Wait()
	awaitFrame(t, oldTransport.sends, ports.MsgOutput)

	activeTheme := ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 10, G: 20, B: 30},
		HasBackground: true, Background: renderer.RGB{R: 40, G: 50, B: 60},
		TrueColor: true,
	}
	d.applyTheme(sess, next, activeTheme)
	snatchedTheme := ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
		TrueColor: true,
	}
	oldTransport.recv <- ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(snatchedTheme)}
	require.NoError(t, oldTransport.Close())
	d.runConnLoop(old)

	require.Equal(t, snatchedTheme.Foreground, old.getAppliedTheme().Raw.Foreground)
	assertSessionDefaultColors(t, sess, activeTheme.Foreground, activeTheme.Background)
	panel := awaitFrame(t, oldTransport.sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(panel.Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "Session snatched")
}

func TestSnatchedConnectionDetachRemovesOnlySnatchedAttachment(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := newDatagramTestTransport()
	old.replaceTransport(oldTransport)

	activeTransport := &closeTrackingTransport{}
	next := &attachedClient{tr: activeTransport, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              next,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: next.transportSnapshot(),
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(result)
	d.attachmentCleanupWg.Wait()
	awaitFrame(t, oldTransport.sends, ports.MsgOutput)

	oldTransport.recv <- ports.Frame{Type: ports.MsgDetach, Payload: ports.MarshalDetach(ports.Detach{})}
	d.runConnLoop(old)

	detached := awaitFrame(t, oldTransport.sends, ports.MsgDetached)
	message, err := ports.UnmarshalDetached(detached.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonDetach, message.Reason)
	require.Equal(t, attachmentDetached, sess.attachmentRole(old))
	require.Equal(t, attachmentActive, sess.attachmentRole(next))
	require.Same(t, next, sess.client)
	require.False(t, activeTransport.Closed())
}

func TestReplacementClearsSnatchedOverlaysPreviewAndCaptures(t *testing.T) {
	d, sess, old, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	oldTransport := &closeTrackingTransport{}
	old.replaceTransport(oldTransport)

	d.enterPicker(sess, old)
	d.enterPalette(sess, old)
	d.enterPrompt(sess, old, " Rename ", "work", func(string) error { return nil })
	d.enterCopyMode(sess, old)
	d.enterNotices(sess, old)
	old.overlays.resizeMu.Lock()
	old.overlays.resizeActive = true
	old.overlays.resizePending = []byte("pending")
	old.overlays.resizeMu.Unlock()
	old.sendMu.Lock()
	old.captureFrames = map[*pane]capturedPaneRenderState{sess.activeTab().focusedPane(): {}}
	old.sendMu.Unlock()
	require.True(t, old.overlays.Active())

	next := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              next,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: next.transportSnapshot(),
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(result)
	d.attachmentCleanupWg.Wait()

	require.False(t, old.overlays.Active())
	old.overlays.pickerMu.Lock()
	require.Nil(t, old.overlays.pickerPreview)
	require.Nil(t, old.overlays.pickerPreviewSession)
	old.overlays.pickerMu.Unlock()
	old.sendMu.Lock()
	require.Empty(t, old.captureFrames)
	old.sendMu.Unlock()
	require.False(t, oldTransport.Closed())
}

type failingSnatchedTransport struct {
	mu     sync.Mutex
	closed bool
}

func (*failingSnatchedTransport) Send(ports.Frame) error     { return errors.New("send failed") }
func (*failingSnatchedTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (t *failingSnatchedTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}
func (t *failingSnatchedTransport) Closed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func TestSnatchedPanelSendFailureRemovesOnlyWaitingClient(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	oldTransport := &failingSnatchedTransport{}
	old := &attachedClient{tr: oldTransport, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	old.initOverlays()
	sess := &session{sessionCore: sessionCore{id: "work", client: old}}
	old.setSession(sess)
	d.sessions[sess.id] = sess

	activeTransport := &closeTrackingTransport{}
	next := &attachedClient{tr: activeTransport, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              next,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: next.transportSnapshot(),
	})
	require.NoError(t, err)
	d.deferAttachmentTransitionCleanups(result)
	d.attachmentCleanupWg.Wait()

	require.True(t, oldTransport.Closed())
	require.Equal(t, attachmentDetached, sess.attachmentRole(old))
	require.Equal(t, attachmentActive, sess.attachmentRole(next))
	require.Same(t, next, sess.client)
	require.False(t, activeTransport.Closed())
}

func TestSnatchedPanelFailurePreservesActivePaneThemeAndSize(t *testing.T) {
	pty := &transactionalResizePTY{}
	d, sess, waiting, _ := newManualSessionWithPTYs(t, pty)
	failed := &failingSnatchedTransport{}
	waiting.replaceTransport(failed)

	activeTransport := &closeTrackingTransport{}
	active := &attachedClient{tr: activeTransport, output: newOutputStateStream(), size: waiting.size}
	active.initOverlays()
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: active, expectedRole: attachmentDetached, targetRole: attachmentActive,
		expectedTransport: active.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)

	activeTheme := ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 12, G: 34, B: 56},
		HasBackground: true, Background: renderer.RGB{R: 65, G: 43, B: 21},
		TrueColor: true,
	}
	d.applyTheme(sess, active, activeTheme)
	activeSize := domain.Size{Cols: 103, Rows: 39}
	d.resize(sess, active, activeSize)
	require.Equal(t, activeSize, active.size)
	assertSessionDefaultColors(t, sess, activeTheme.Foreground, activeTheme.Background)
	requestedBeforeFailure := pty.requested()

	d.deferAttachmentTransitionCleanups(transition)
	d.attachmentCleanupWg.Wait()

	require.True(t, failed.Closed())
	require.Equal(t, attachmentDetached, sess.attachmentRole(waiting))
	require.Same(t, active, sess.client)
	require.False(t, activeTransport.Closed())
	require.Equal(t, activeSize, active.size)
	require.Equal(t, requestedBeforeFailure, pty.requested(), "snatched panel failure must not resize active panes")
	assertSessionDefaultColors(t, sess, activeTheme.Foreground, activeTheme.Background)
}

func TestSnatchedThemePanelFailureRemovesOnlySnatchedAttachment(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	oldTransport := &failingSnatchedTransport{}
	old := &attachedClient{tr: oldTransport, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	old.initOverlays()
	sess := &session{sessionCore: sessionCore{id: "work", client: old}}
	old.setSession(sess)
	d.sessions[sess.id] = sess

	activeTransport := &closeTrackingTransport{}
	next := &attachedClient{tr: activeTransport, output: newOutputStateStream(), size: old.size}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target:            sess,
		next:              next,
		expectedRole:      attachmentDetached,
		targetRole:        attachmentActive,
		expectedTransport: next.transportSnapshot(),
	})
	require.NoError(t, err)
	for _, cleanup := range result.cleanups {
		cleanup.finish()
	}

	theme := ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
		TrueColor: true,
	}
	require.True(t, d.handleSnatchedClientFrame(result.displaced, ports.Frame{
		Type: ports.MsgTheme, Payload: ports.MarshalTheme(theme),
	}))

	require.True(t, oldTransport.Closed())
	require.Equal(t, attachmentDetached, sess.attachmentRole(old))
	require.Equal(t, attachmentActive, sess.attachmentRole(next))
	require.Same(t, next, sess.client)
	require.False(t, activeTransport.Closed())
}

func TestClearForSnatchReleasesEachOverlayFamilyBeforeTakingNext(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	tr := &closeTrackingTransport{}
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	ac.roleGeneration.Store(1)
	sess := &session{sessionCore: sessionCore{id: "work", snatched: map[*attachedClient]struct{}{ac: {}}}}
	ac.setSession(sess)
	token := attachmentRoleToken{
		sess: sess, ac: ac, role: attachmentSnatched,
		generation: 1, transport: ac.transportSnapshot(),
	}

	pickerCleared := make(chan struct{})
	d.afterSnatchOverlayFamily = func(family string) {
		if family == "picker" {
			close(pickerCleared)
		}
	}
	ac.overlays.resizeMu.Lock()
	resizeLocked := true
	defer func() {
		if resizeLocked {
			ac.overlays.resizeMu.Unlock()
		}
	}()
	cleared := make(chan bool, 1)
	go func() { cleared <- d.clearForSnatch(token) }()
	select {
	case <-pickerCleared:
	case <-time.After(2 * time.Second):
		t.Fatal("picker family did not clear before blocked resize family")
	}
	require.True(t, ac.overlays.pickerMu.TryLock(), "clear retained pickerMu while waiting for resizeMu")
	ac.overlays.pickerMu.Unlock()

	ac.overlays.resizeMu.Unlock()
	resizeLocked = false
	require.True(t, <-cleared)
}

func TestClearForSnatchRejectsStaleGenerationWithoutClearingNewOverlay(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	tr := &closeTrackingTransport{}
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.initOverlays()
	ac.roleGeneration.Store(1)
	sess := &session{sessionCore: sessionCore{id: "work", snatched: map[*attachedClient]struct{}{ac: {}}}}
	ac.setSession(sess)
	token := attachmentRoleToken{
		sess: sess, ac: ac, role: attachmentSnatched,
		generation: 1, transport: ac.transportSnapshot(),
	}

	ac.overlays.pickerMu.Lock()
	started := make(chan struct{})
	cleared := make(chan bool, 1)
	go func() {
		close(started)
		cleared <- d.clearForSnatch(token)
	}()
	<-started
	ac.roleGeneration.Add(1)
	ac.overlays.resizeMu.Lock()
	ac.overlays.resizeActive = true
	ac.overlays.resizeMu.Unlock()
	ac.overlays.pickerMu.Unlock()

	require.False(t, <-cleared)
	require.True(t, ac.overlays.resizeModeActive(), "stale cleanup cleared a newer generation")
}

func TestTransitionAttachmentRejectsBeforeOwnershipCommit(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Daemon, *session, *attachedClient)
	}{
		{
			name: "stale transport",
			mutate: func(_ *Daemon, _ *session, next *attachedClient) {
				next.replaceTransport(&closeTrackingTransport{})
			},
		},
		{
			name: "session lifecycle ended",
			mutate: func(d *Daemon, sess *session, _ *attachedClient) {
				delete(d.sessions, sess.id)
			},
		},
		{
			name: "unexpected role",
			mutate: func(_ *Daemon, sess *session, next *attachedClient) {
				sess.mu.Lock()
				sess.addSnatchedLocked(next)
				sess.mu.Unlock()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			sess := &session{sessionCore: sessionCore{id: domain.SessionID("work")}}
			old := &attachedClient{tr: &closeTrackingTransport{}}
			old.setSession(sess)
			sess.client = old
			d.sessions[sess.id] = sess
			next := &attachedClient{tr: &closeTrackingTransport{}}
			expected := next.transportSnapshot()
			tt.mutate(d, sess, next)

			_, err := d.transitionAttachment(attachmentTransitionRequest{
				target:            sess,
				next:              next,
				expectedRole:      attachmentDetached,
				targetRole:        attachmentActive,
				expectedTransport: expected,
			})
			require.ErrorIs(t, err, errAttachmentTransition)
			require.Same(t, old, sess.client)
			require.Zero(t, old.roleGeneration.Load())
			require.Zero(t, next.roleGeneration.Load())
		})
	}
}

func TestCrossSessionTransitionPublishesCoherentOwnershipBeforeBlockedFirstPaint(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	from := &session{sessionCore: sessionCore{id: "z-source"}}
	target := &session{sessionCore: sessionCore{id: "a-target"}}
	moving := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	displaced := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: moving.size}
	moving.initOverlays()
	displaced.initOverlays()
	moving.setSession(from)
	displaced.setSession(target)
	from.client = moving
	target.client = displaced
	d.sessions[from.id] = from
	d.sessions[target.id] = target
	sourceCoordinator := d.attachCoordinator(from, nil, moving, true)
	targetCoordinator := d.attachCoordinator(target, nil, displaced, true)

	moving.sendMu.Lock()
	locked := true
	defer func() {
		if locked {
			moving.sendMu.Unlock()
		}
	}()
	transitionDone := make(chan attachmentTransitionResult, 1)
	transitionErr := make(chan error, 1)
	go func() {
		result, err := d.transitionAttachment(attachmentTransitionRequest{
			source: from, target: target, next: moving,
			expectedRole: attachmentActive, targetRole: attachmentActive,
			expectedTransport: moving.transportSnapshot(), ready: true,
		})
		transitionDone <- result
		transitionErr <- err
	}()

	var result attachmentTransitionResult
	select {
	case result = <-transitionDone:
		require.NoError(t, <-transitionErr)
	case <-time.After(2 * time.Second):
		t.Fatal("ownership publication waited for blocked sendMu")
	}

	from.mu.Lock()
	require.Nil(t, from.client)
	from.mu.Unlock()
	target.mu.Lock()
	require.Same(t, moving, target.client)
	require.Contains(t, target.snatched, displaced)
	target.mu.Unlock()
	require.Same(t, target, moving.currentSession())
	sourceCoordinator.mu.Lock()
	require.NotNil(t, sourceCoordinator.lease)
	require.False(t, sourceCoordinator.lease.active)
	sourceCoordinator.mu.Unlock()
	targetCoordinator.mu.Lock()
	require.Same(t, result.published.lease, targetCoordinator.lease)
	require.True(t, targetCoordinator.lease.active)
	require.Same(t, moving, targetCoordinator.lease.attachment)
	targetCoordinator.mu.Unlock()
	require.True(t, result.published.activeCurrent())

	beforeSendWait := make(chan struct{})
	d.beforeFirstPaintSendWait = func(token attachmentRoleToken) {
		if token.ac == moving {
			close(beforeSendWait)
		}
	}
	paintDone := make(chan bool, 1)
	go func() {
		paintDone <- d.firstPaintForTransition(result.published)
	}()
	<-beforeSendWait
	require.True(t, d.mu.TryLock(), "first paint retained daemon lock while waiting for sendMu")
	d.mu.Unlock()
	require.True(t, d.notices.routingMu.TryLock(), "first paint retained routing lock while waiting for sendMu")
	d.notices.routingMu.Unlock()
	require.True(t, from.mu.TryLock(), "first paint retained source session lock while waiting for sendMu")
	from.mu.Unlock()
	require.True(t, target.mu.TryLock(), "first paint retained target session lock while waiting for sendMu")
	target.mu.Unlock()
	require.True(t, sourceCoordinator.mu.TryLock(), "first paint retained source coordinator lock while waiting for sendMu")
	sourceCoordinator.mu.Unlock()
	require.True(t, targetCoordinator.mu.TryLock(), "first paint retained target coordinator lock while waiting for sendMu")
	targetCoordinator.mu.Unlock()

	moving.sendMu.Unlock()
	locked = false
	require.True(t, <-paintDone)
}

func TestFirstPaintForTransitionRejectsStaleRoleTokenBeforeRebaseOrReset(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	tr := &closeTrackingTransport{}
	ac := &attachedClient{
		tr:     tr,
		output: newOutputStateStream(),
		size:   domain.Size{Cols: 80, Rows: 24},
	}
	ac.initOverlays()
	ac.roleGeneration.Store(2)
	ac.output.next = 4
	ac.output.acked = 1
	sess := &session{sessionCore: sessionCore{id: "work", client: ac}}
	ac.setSession(sess)
	d.sessions[sess.id] = sess
	rc := d.attachCoordinator(sess, nil, ac, true)
	rebased := false
	ac.renderStages.handoffRebase = func() { rebased = true }

	stale := attachmentRoleToken{
		sess: sess, ac: ac, role: attachmentActive,
		generation: 1, transport: ac.transportSnapshot(), lease: rc.attachmentLease(ac), rebase: true,
	}
	require.False(t, d.firstPaintForTransition(stale))

	require.False(t, rebased)
	require.Equal(t, uint64(4), ac.output.next)
	require.Equal(t, uint64(1), ac.output.acked)
	require.Empty(t, tr.Sends(), "stale first paint emitted a reset")
}

func TestTransitionAttachmentMovesActiveBetweenSessions(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	from := &session{sessionCore: sessionCore{id: domain.SessionID("z-source")}}
	target := &session{sessionCore: sessionCore{id: domain.SessionID("a-target")}}
	moving := &attachedClient{tr: &closeTrackingTransport{}}
	displaced := &attachedClient{tr: &closeTrackingTransport{}}
	moving.setSession(from)
	displaced.setSession(target)
	from.client = moving
	target.client = displaced
	d.sessions[from.id] = from
	d.sessions[target.id] = target

	result, err := d.transitionAttachment(attachmentTransitionRequest{
		source:            from,
		target:            target,
		next:              moving,
		expectedRole:      attachmentActive,
		targetRole:        attachmentActive,
		expectedTransport: moving.transportSnapshot(),
		ready:             true,
	})
	require.NoError(t, err)

	from.mu.Lock()
	require.Nil(t, from.client)
	require.NotContains(t, from.snatched, moving)
	from.mu.Unlock()
	target.mu.Lock()
	require.Same(t, moving, target.client)
	require.Contains(t, target.snatched, displaced)
	target.mu.Unlock()
	require.True(t, result.published.current())
	require.True(t, result.displaced.current())
	require.Same(t, target, moving.currentSession())
}
