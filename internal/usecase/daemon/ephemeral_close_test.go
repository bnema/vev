package daemon

import (
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestEphemeralCloseOnExit(t *testing.T) {
	tests := []struct {
		name        string
		closeOnExit bool
		intent      uint8
		sessionName string
		clients     int
		renamed     bool
		wantRemoved bool
	}{
		{name: "last ephemeral client removes session", closeOnExit: true, intent: protocol.IntentEphemeral, clients: 1, wantRemoved: true},
		{name: "option off keeps session", closeOnExit: false, intent: protocol.IntentEphemeral, clients: 1},
		{name: "named session is never removed", closeOnExit: true, intent: protocol.IntentNew, sessionName: "work", clients: 1},
		{name: "renamed session is never removed", closeOnExit: true, intent: protocol.IntentEphemeral, clients: 1, renamed: true},
		{name: "remaining client keeps session", closeOnExit: true, intent: protocol.IntentEphemeral, clients: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pty, release := newBlockingPTY(t)
			defer release()
			d := newTestDaemon(t, newFactory(t, pty), stubClock{})
			d.ephemeralConfig.Store(&domain.EphemeralConfig{CloseOnExit: tt.closeOnExit})

			tr := &closeTrackingTransport{}
			sess, ac, err := d.route(helloResumeCapable(tt.intent, tt.sessionName, 0), tr)
			require.NoError(t, err)
			for i := 1; i < tt.clients; i++ {
				_, _, err := d.route(helloResumeCapable(protocol.IntentAttach, sess.nameSnapshot(), 0), &closeTrackingTransport{})
				require.NoError(t, err)
			}

			d.clientGone(sess, ac, tr, true)
			if tt.renamed {
				require.NoError(t, d.renameSession(sess, "kept"))
			}
			d.reapAbandonedEphemeral(sess)

			require.Equal(t, !tt.wantRemoved, d.sessionRegistered(sess))
			if !tt.wantRemoved {
				require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, false))
			}
			d.sessWg.Wait()
		})
	}
}

func TestEphemeralCloseOnExitWaitsForParkExpiry(t *testing.T) {
	tests := []struct {
		name        string
		otherClient bool
		wantRemoved bool
	}{
		{name: "expired park removes abandoned session", wantRemoved: true},
		{name: "expired park keeps session with another client", otherClient: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := &signalClock{timers: make(chan *signalTimer, 8)}
			pty, release := newBlockingPTY(t)
			defer release()
			d := newTestDaemon(t, newFactory(t, pty), clock)

			tr := &closeTrackingTransport{}
			sess, ac, err := d.route(helloResumeCapable(protocol.IntentEphemeral, "", 0), tr)
			require.NoError(t, err)
			if tt.otherClient {
				_, _, err := d.route(helloResumeCapable(protocol.IntentAttach, sess.nameSnapshot(), 0), &closeTrackingTransport{})
				require.NoError(t, err)
			}
			token := ac.resumeToken

			// A link loss parks: the session must survive so the client can resume.
			d.clientGone(sess, ac, tr, false)
			<-clock.timers
			d.reapAbandonedEphemeral(sess)
			require.True(t, d.sessionRegistered(sess), "parked attachment keeps the ephemeral session")

			d.mu.Lock()
			parked := d.parked[token]
			d.mu.Unlock()
			require.NotNil(t, parked)
			// Expire directly instead of firing the timer, so no watcher
			// goroutine races the assertion below.
			d.expireParked(token, parked)

			require.Equal(t, !tt.wantRemoved, d.sessionRegistered(sess))
			if !tt.wantRemoved {
				require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, false))
			}
			d.sessWg.Wait()
		})
	}
}

func TestDetachFrameEphemeralCloseOnExit(t *testing.T) {
	tests := []struct {
		name        string
		closeOnExit bool
		intent      uint8
		sessionName string
		detach      protocol.Detach
		wantRemoved bool
	}{
		{name: "closed client removes session", closeOnExit: true, intent: protocol.IntentEphemeral, detach: protocol.Detach{Closed: true}, wantRemoved: true},
		{name: "user detach keeps session", closeOnExit: true, intent: protocol.IntentEphemeral, detach: protocol.Detach{}},
		{name: "option off keeps session", closeOnExit: false, intent: protocol.IntentEphemeral, detach: protocol.Detach{Closed: true}},
		{name: "named session is kept", closeOnExit: true, intent: protocol.IntentNew, sessionName: "work", detach: protocol.Detach{Closed: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pty, release := newBlockingPTY(t)
			defer release()
			d := newTestDaemon(t, newFactory(t, pty), stubClock{})
			d.ephemeralConfig.Store(&domain.EphemeralConfig{CloseOnExit: tt.closeOnExit})
			tr, sends, _ := newConn(t,
				mustHello(tt.intent, tt.sessionName, domain.Size{Cols: 80, Rows: 24}),
				mustClientEnvelope(tt.detach),
			)

			var hg sync.WaitGroup
			hg.Go(func() { d.handleConn(tr) })
			awaitFrame(t, sends, "Welcome")
			hg.Wait()

			if tt.wantRemoved {
				require.Equal(t, 0, sessionCount(d))
			} else {
				require.Equal(t, 1, sessionCount(d))
				require.NoError(t, d.killSession(firstSession(d), protocol.ReasonSessionKilled, false))
			}
			d.sessWg.Wait()
			d.waitNotifies()
		})
	}
}
