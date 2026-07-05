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
	ClosePane() error
	FocusPaneLeft() error
	FocusPaneRight() error
	FocusPaneUp() error
	FocusPaneDown() error
	NextTab() error
	PrevTab() error
	BackSession() error
	ForwardSession() error
	Detach() error
	EnterVisualMode() error
	RenameSession() error
	OpenSessionPicker() error
}

// Command describes an executable command.
type Command struct {
	Slug, Code, Name, Desc string
	Run                    func(Context) error
}
