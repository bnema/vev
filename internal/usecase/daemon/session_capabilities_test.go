package daemon

import (
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestCapabilitiesZeroValueIsFullyCapable(t *testing.T) {
	s := &session{sessionCore: sessionCore{id: domain.SessionID("s1"), name: "alpha"}}
	caps := s.capabilities()
	if caps.cannotYieldMoves || !caps.yieldsMoves() {
		t.Fatalf("zero-value capabilities = %+v, want fully capable", caps)
	}
}

func TestEnterPickerForIntentRejectsNonYieldingSource(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t)
	sess.caps = sessionCapabilities{cannotYieldMoves: true}

	tests := []struct {
		name    string
		intent  pickerIntent
		wantErr error
	}{
		{name: "pickerMoveTab", intent: pickerMoveTab, wantErr: errSessionCannotYieldMoves},
		{name: "pickerNavigate", intent: pickerNavigate, wantErr: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := d.enterPickerForIntent(sess, ac, tt.intent, moveSourceLocator{})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("enterPickerForIntent error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
