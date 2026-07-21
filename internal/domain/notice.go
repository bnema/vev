package domain

import "time"

// NoticeSeverity ranks a user-facing notification.
//
// Its numeric values persist in JSON notice records and are a protocol/domain
// contract. Add values only at the end; never reorder existing values.
type NoticeSeverity uint8

const (
	NoticeInfo NoticeSeverity = iota
	NoticeWarn
	NoticeError
)

// NoticeCode identifies a failure meaning, not a call site.
//
// Its numeric values persist in JSON notice records and are a protocol/domain
// contract. Add values only at the end; never reorder existing values.
type NoticeCode uint16

const (
	NoticeInternal NoticeCode = iota
	NoticePaneSpawn
	NoticeTabSpawn
	NoticeFloatingSpawn
	NoticeSessionSpawn
	NoticeLayoutTooSmall
	NoticePaneNotFound
	NoticeSessionUnavailable
	NoticePersistDisabled
	NoticeSnapshotWrite
	NoticeSnapshotRestore
	NoticeSnapshotSaturated
	NoticePersistDelete
	NoticeConfigReload
	NoticeInputDropped
	NoticeResizeFailed
	NoticeClipboard
	NoticeClipboardTooLarge
	NoticeAutoResume
	NoticeConnection
)

var noticeSlugs = map[NoticeCode]string{
	NoticeInternal:           "internal",
	NoticePaneSpawn:          "pane-spawn",
	NoticeTabSpawn:           "tab-spawn",
	NoticeFloatingSpawn:      "floating-spawn",
	NoticeSessionSpawn:       "session-spawn",
	NoticeLayoutTooSmall:     "layout-too-small",
	NoticePaneNotFound:       "pane-not-found",
	NoticeSessionUnavailable: "session-unavailable",
	NoticePersistDisabled:    "persist-disabled",
	NoticeSnapshotWrite:      "snapshot-write",
	NoticeSnapshotRestore:    "snapshot-restore",
	NoticeSnapshotSaturated:  "snapshot-saturated",
	NoticePersistDelete:      "persist-delete",
	NoticeConfigReload:       "config-reload",
	NoticeInputDropped:       "input-dropped",
	NoticeResizeFailed:       "resize-failed",
	NoticeClipboard:          "clipboard",
	NoticeClipboardTooLarge:  "clipboard-too-large",
	NoticeAutoResume:         "auto-resume",
	NoticeConnection:         "connection",
}

func (c NoticeCode) String() string {
	if s, ok := noticeSlugs[c]; ok {
		return s
	}
	return "unknown"
}

// UserError is an error whose message is written for the attached user.
// The wrapped cause is shown only in yank details, never in the toast.
type UserError struct {
	Code     NoticeCode
	Severity NoticeSeverity
	Msg      string
	Err      error
}

func (e *UserError) Error() string {
	if e.Err == nil {
		return e.Msg
	}
	return e.Msg + ": " + e.Err.Error()
}

func (e *UserError) Unwrap() error { return e.Err }

// UserErr wraps err as an error-severity user-facing failure.
func UserErr(code NoticeCode, msg string, err error) *UserError {
	return &UserError{Code: code, Severity: NoticeError, Msg: msg, Err: err}
}

// UserWarn wraps err as a warn-severity user-facing failure.
func UserWarn(code NoticeCode, msg string, err error) *UserError {
	return &UserError{Code: code, Severity: NoticeWarn, Msg: msg, Err: err}
}

// Notification is one entry in the daemon's user-facing notice history.
type Notification struct {
	Code      NoticeCode
	Severity  NoticeSeverity
	Message   string
	Details   string // formatted error chain for yank
	Time      time.Time
	Count     int       // >1 when coalesced
	SessionID SessionID // "" = daemon-global
}
