package daemon

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol/wire"
)

// TestPaintPublicationAttempt drives one complete publication attempt through
// paint (capture, compose, publish, settle) and asserts its observable
// outcome: what reached the transport, which VT damage was acknowledged,
// whether the attachment stayed owned, and that settlement never ran under the
// attachment's send lock.
func TestPaintPublicationAttempt(t *testing.T) {
	tests := []struct {
		name         string
		sendErr      error
		unown        bool
		wantResult   paintResult
		wantSent     bool
		wantDamage   bool
		wantAttached bool
	}{
		{name: "success emits and commits damage", wantResult: paintEmitted, wantSent: true, wantAttached: true},
		{name: "send failure retains damage and detaches", sendErr: errors.New("link lost"), wantResult: paintEmitted, wantDamage: true},
		{name: "unowned attachment is rejected before capture", unown: true, wantResult: paintRejected, wantDamage: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
			healthy := ac.transport()
			d.paint(sess, ac, true, nil)
			<-sends

			p := sess.tabs[0].focusedPane()
			p.mu.Lock()
			p.screen.Write([]byte("\x1b[1;1Hchanged"))
			p.mu.Unlock()

			tr := newMockServerConnection(t)
			var sendLockHeld []bool
			tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f wire.Envelope) error {
				if tt.sendErr != nil {
					return tt.sendErr
				}
				sends <- f
				return nil
			}).Maybe()
			tr.EXPECT().Close().RunAndReturn(func() error {
				// Send-error cleanup belongs to settlement, after sendMu is released.
				locked := !ac.sendMu.TryLock()
				if !locked {
					ac.sendMu.Unlock()
				}
				sendLockHeld = append(sendLockHeld, locked)
				return nil
			}).Maybe()
			ac.replaceTransport(tr)
			if tt.unown {
				sess.mu.Lock()
				delete(sess.attachments, ac)
				sess.mu.Unlock()
			}

			result := d.paint(sess, ac, false, nil)
			d.attachmentCleanupWg.Wait()

			require.Equal(t, tt.wantResult, result)
			if tt.sendErr != nil {
				require.NotEmpty(t, sendLockHeld, "send failure must close the failed link")
			}
			require.NotContains(t, sendLockHeld, true, "settlement ran under sendMu")
			if tt.wantSent {
				out := unmarshalTestOutput(t, (<-sends).Payload)
				require.Contains(t, string(out.Data), "changed")
			} else {
				select {
				case f := <-sends:
					t.Fatalf("unexpected frame %s", envelopeMessageName(t, f.Payload))
				default:
				}
			}
			p.mu.Lock()
			require.Equal(t, tt.wantDamage, len(p.screen.Damage()) != 0, "damage is acknowledged only after emission")
			p.mu.Unlock()
			sess.mu.Lock()
			_, attached := sess.attachments[ac]
			sess.mu.Unlock()
			require.Equal(t, tt.wantAttached, attached)
			require.True(t, ac.sendMu.TryLock(), "paint must release sendMu")
			ac.sendMu.Unlock()

			if tt.wantDamage && !tt.unown {
				// A retry on a healthy link emits the retained change.
				sess.mu.Lock()
				sess.registerAttachmentLocked(ac)
				sess.mu.Unlock()
				ac.setSession(sess)
				ac.replaceTransport(healthy)
				require.Equal(t, paintEmitted, d.paint(sess, ac, false, nil))
				out := unmarshalTestOutput(t, (<-sends).Payload)
				require.Contains(t, string(out.Data), "changed")
			}
		})
	}
}
