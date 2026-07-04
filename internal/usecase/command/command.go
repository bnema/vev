package command

// Context is the set of application actions available to commands.
type Context interface {
	CreateTab() error
	CreateSession() error
	CloseTab() error
	NextTab() error
	PrevTab() error
	Detach() error
	EnterVisualMode() error
	RenameSession() error
	RenameTab() error
	OpenSessionPicker() error
}

// Command describes an executable command.
type Command struct {
	Code, Name, Desc string
	Run              func(Context) error
}
