package protocol

// CommandRequest asks the daemon to run one control command. Version must stay
// first so a future payload layout can still be rejected cleanly.
type CommandRequest struct {
	Version       uint16
	RequestID     uint64
	Attached      bool
	Self          bool
	Slug          string
	Args          []string
	TargetSession string
	TargetTab     string
	TargetPane    string
	JSON          bool
}

// CommandResult reports a control command's outcome.
type CommandResult struct {
	RequestID uint64
	OK        bool
	Code      uint16
	Text      string
	Output    string
}
