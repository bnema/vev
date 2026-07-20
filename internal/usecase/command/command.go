package command

import (
	"errors"
	"strconv"
)

// ErrInvalidArguments reports arguments that do not have the exact command format.
var ErrInvalidArguments = errors.New("invalid command arguments")

// ParsePositiveDecimal accepts exactly one base-10 positive decimal value.
// It deliberately rejects signs, whitespace, zero, and non-canonical forms.
func ParsePositiveDecimal(args []string) (int, error) {
	if len(args) != 1 || args[0] == "" || (len(args[0]) > 1 && args[0][0] == '0') {
		return 0, ErrInvalidArguments
	}
	for _, r := range args[0] {
		if r < '0' || r > '9' {
			return 0, ErrInvalidArguments
		}
	}
	n, err := strconv.Atoi(args[0])
	if err != nil || n < 1 {
		return 0, ErrInvalidArguments
	}
	return n, nil
}

// Context is the set of application actions available to commands.
type Context interface {
	CreateTab() error
	CreateSession() error
	CloseTab() error
	SplitRight() error
	SplitLeft() error
	SplitUp() error
	SplitDown() error
	StackPane() error
	ToggleStack() error
	ToggleFloatingPane() error
	ClosePane() error
	FocusPaneLeft() error
	FocusPaneRight() error
	FocusPaneUp() error
	FocusPaneDown() error
	NextTab() error
	PrevTab() error
	BackSession() error
	Detach() error
	EnterVisualMode() error
	RenameSession() error
	RenameTab() error
	OpenSessionPicker() error
	OpenNotifications() error
	YankLastNotification() error
	JumpRecentSession(rank int) error
}

// Arguments declares whether a command accepts palette arguments.
type Arguments uint8

const (
	ArgumentsNone Arguments = iota
	ArgumentsRequired
)

// ContextHint describes optional contextual data needed to execute a command.
type ContextHint uint8

const (
	ContextHintNone ContextHint = iota
	ContextHintRecentSessions
)

// Command describes an executable command.
type Command struct {
	Slug, Code, Name, Desc string
	Arguments              Arguments
	ContextHint            ContextHint
	Run                    func(Context, []string) error
}
