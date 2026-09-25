package daemon

import (
	"sync"
	"testing"
	"time"

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
		// closeFirst ends the first client with a closed-process detach;
		// otherwise it ends with a user detach.
		closeFirst  bool
		wantRemoved bool
	}{
		{name: "closed last ephemeral client removes session", closeOnExit: true, intent: protocol.IntentEphemeral, clients: 1, closeFirst: true, wantRemoved: true},
		{name: "option off keeps session", closeOnExit: false, intent: protocol.IntentEphemeral, clients: 1, closeFirst: true},
		{name: "user detach keeps session", closeOnExit: true, intent: protocol.IntentEphemeral, clients: 1},
		{name: "named session is never removed", closeOnExit: true, intent: protocol.IntentNew, sessionName: "work", clients: 1, closeFirst: true},
		{name: "remaining client keeps session", closeOnExit: true, intent: protocol.IntentEphemeral, clients: 2, closeFirst: true},
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
			if tt.closeFirst {
				d.reapAbandonedEphemeral(sess)
			}

			require.Equal(t, !tt.wantRemoved, d.sessionRegistered(sess))
			if !tt.wantRemoved {
				require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, false))
			}
			d.sessWg.Wait()
		})
	}
}

func TestEphemeralCloseOnExitWaitsForParkExpiry(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 8)}
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), clock)

	tr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(protocol.IntentEphemeral, "", 0), tr)
	require.NoError(t, err)
	token := ac.resumeToken

	// A link loss parks: the session must survive so the client can resume.
	d.clientGone(sess, ac, tr, false)
	parkTimer := <-clock.timers
	d.reapAbandonedEphemeral(sess)
	require.True(t, d.sessionRegistered(sess), "parked attachment keeps the ephemeral session")

	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked)
	parkTimer.ch <- time.Time{}
	d.expireParked(token, parked)

	require.False(t, d.sessionRegistered(sess), "expired park removes the abandoned ephemeral session")
	d.sessWg.Wait()
}

func TestClosedDetachFrameRemovesEphemeralSession(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	tr, sends, _ := newConn(t,
		mustHello(protocol.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}),
		mustClientEnvelope(protocol.Detach{Closed: true}),
	)

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, "Welcome")
	hg.Wait()

	require.Equal(t, 0, sessionCount(d), "closed client removes its ephemeral session")
	d.sessWg.Wait()
	d.waitNotifies()
}
