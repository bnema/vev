package command

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
	JumpRecentSession(args []string) error
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
