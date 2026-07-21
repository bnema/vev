package domain

import (
	"errors"
	"fmt"
	"testing"
)

func TestNoticeCodeString(t *testing.T) {
	for code := range noticeCodeLimit {
		if got := code.String(); got == "unknown" {
			t.Errorf("NoticeCode(%d).String() = unknown, want declared code slug", code)
		}
	}
	if got := NoticeCode(9999).String(); got != "unknown" {
		t.Errorf("NoticeCode(9999).String() = %q, want unknown", got)
	}
}

func TestUserError(t *testing.T) {
	cause := errors.New("boom")
	tests := []struct {
		name     string
		err      *UserError
		wantMsg  string
		wantSev  NoticeSeverity
		wantWrap error
	}{
		{"err with cause", UserErr(NoticePaneSpawn, "couldn't open pane", cause), "couldn't open pane: boom", NoticeError, cause},
		{"err without cause", UserErr(NoticeSessionUnavailable, "session is unavailable", nil), "session is unavailable", NoticeError, nil},
		{"warn", UserWarn(NoticeResizeFailed, "pane resize failed", cause), "pane resize failed: boom", NoticeWarn, cause},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.wantMsg {
				t.Errorf("Error() = %q, want %q", got, tt.wantMsg)
			}
			if tt.err.Severity != tt.wantSev {
				t.Errorf("Severity = %d, want %d", tt.err.Severity, tt.wantSev)
			}
			if !errors.Is(tt.err, tt.wantWrap) && tt.wantWrap != nil {
				t.Errorf("errors.Is(err, cause) = false, want true")
			}
			var ue *UserError
			if !errors.As(fmt.Errorf("wrapped: %w", tt.err), &ue) {
				t.Errorf("errors.As through wrapping = false, want true")
			}
		})
	}
}
