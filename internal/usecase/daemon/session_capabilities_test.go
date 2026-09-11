package daemon

import (
	"errors"
	"testing"

	"github.com/bnema/vev/internal/protocol"

	"github.com/bnema/vev/internal/domain"
)

func TestCapabilitiesZeroValueIsFullyCapable(t *testing.T) {
	s := &session{sessionCore: sessionCore{id: domain.SessionID("s1"), name: "alpha"}}
	caps := s.capabilities()
	if caps.cannotYieldMoves || !caps.yieldsMoves() {
		t.Fatalf("zero-value capabilities = %+v, want fully capable", caps)
	}
}

func TestOpenPickerForIntentRejectsNonYieldingSource(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t)
	sess.caps = sessionCapabilities{cannotYieldMoves: true}
	effect := admitPickerEffectForTest(t, sess, ac)

	tests := []struct {
		name    string
		intent  protocol.PickerIntent
		wantErr error
	}{
		{name: "move tab", intent: protocol.PickerIntentMoveTab, wantErr: errSessionCannotYieldMoves},
		{name: "navigation", intent: protocol.PickerIntentNavigation, wantErr: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := d.openPickerForAttachment(ac, effect, tt.intent, moveSourceLocator{}, 0)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("openPickerForAttachment error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
