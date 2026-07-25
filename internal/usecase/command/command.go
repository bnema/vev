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
	EnterResizeMode() error
	GrowPaneWidth() error
	ShrinkPaneWidth() error
	GrowPaneHeight() error
	ShrinkPaneHeight() error
	EqualizePanes() error
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

// TargetKind declares which resolved target a control command needs.
type TargetKind uint8

const (
	TargetNone TargetKind = iota
	TargetSession
	TargetTab
	TargetPane
)

// ControlOptions carries per-invocation output preferences.
type ControlOptions struct {
	JSON bool
}

// ControlResult carries a control command's output.
type ControlResult struct {
	Output string
}

// ControlContext exposes prompt-free daemon actions to control commands.
type ControlContext interface {
	CreateTab() error
	CreateSessionNamed(name string) error
	CloseTab() error
	ClosePane() error
	SplitRight() error
	SplitLeft() error
	SplitUp() error
	SplitDown() error
	StackPane() error
	ToggleStack() error
	GrowPaneWidth() error
	ShrinkPaneWidth() error
	GrowPaneHeight() error
	ShrinkPaneHeight() error
	EqualizePanes() error
	FocusPaneLeft() error
	FocusPaneRight() error
	FocusPaneUp() error
	FocusPaneDown() error
	NextTab() error
	PrevTab() error
	RenameSessionTo(name string) error
	RenameTabTo(name string) error
	Toast(severity, message string) error
	ListSessions(json bool) (string, error)
	ListTabs(json bool) (string, error)
	ListPanes(json bool) (string, error)
}

// Command describes an executable command.
type Command struct {
	Slug, Code, Name, Desc string
	Usage                  string
	Arguments              Arguments
	ContextHint            ContextHint
	Scriptable             bool
	PaletteVisible         bool
	Target                 TargetKind
	Run                    func(Context, []string) error
	Control                func(ControlContext, []string, ControlOptions) (ControlResult, error)
}
