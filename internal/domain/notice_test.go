package domain

import (
	"errors"
	"fmt"
	"testing"
)

func TestNoticeCodeString(t *testing.T) {
	tests := []struct {
		code NoticeCode
		want string
	}{
		{NoticeInternal, "internal"},
		{NoticePaneSpawn, "pane-spawn"},
		{NoticeTabSpawn, "tab-spawn"},
		{NoticeFloatingSpawn, "floating-spawn"},
		{NoticeSessionSpawn, "session-spawn"},
		{NoticeLayoutTooSmall, "layout-too-small"},
		{NoticePaneNotFound, "pane-not-found"},
		{NoticeSessionUnavailable, "session-unavailable"},
		{NoticePersistDisabled, "persist-disabled"},
		{NoticeSnapshotWrite, "snapshot-write"},
		{NoticeSnapshotRestore, "snapshot-restore"},
		{NoticeSnapshotSaturated, "snapshot-saturated"},
		{NoticePersistDelete, "persist-delete"},
		{NoticeConfigReload, "config-reload"},
		{NoticeInputDropped, "input-dropped"},
		{NoticeResizeFailed, "resize-failed"},
		{NoticeClipboard, "clipboard"},
		{NoticeClipboardTooLarge, "clipboard-too-large"},
		{NoticeAutoResume, "auto-resume"},
		{NoticeConnection, "connection"},
		{NoticeCode(9999), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.code.String(); got != tt.want {
			t.Errorf("NoticeCode(%d).String() = %q, want %q", tt.code, got, tt.want)
		}
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
