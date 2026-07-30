package daemon

import (
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestCapabilitiesZeroValueIsFullyCapable(t *testing.T) {
	s := &session{sessionCore: sessionCore{id: domain.SessionID("s1"), name: "alpha"}}
	caps := s.capabilities()
	if caps.cannotAcceptMoves || caps.cannotYieldMoves || !caps.yieldsMoves() {
		t.Fatalf("zero-value capabilities = %+v, want fully capable", caps)
	}
}

func TestSnapshotViewCarriesCannotAcceptMoves(t *testing.T) {
	tests := []struct {
		name string
		caps sessionCapabilities
		want bool
	}{
		{name: "default local", caps: sessionCapabilities{}, want: false},
		{name: "restricted proxy", caps: sessionCapabilities{cannotAcceptMoves: true}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &session{sessionCore: sessionCore{id: domain.SessionID("s1"), name: "alpha", caps: tt.caps}}
			if got := s.snapshotView(viewOptions{}).cannotAcceptMoves; got != tt.want {
				t.Fatalf("snapshotView().cannotAcceptMoves = %v, want %v", got, tt.want)
			}
		})
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
