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

// CommandOutcome is the closed terminal state of a dispatched command.
type CommandOutcome uint8

const (
	CommandSucceeded CommandOutcome = iota + 1
	CommandFailed
	CommandOutcomeUnknown
)

// CommandResult reports a control command's explicit terminal outcome.
type CommandResult struct {
	RequestID uint64
	Outcome   CommandOutcome
	Code      uint16
	Text      string
	Output    string
}

// Valid reports whether the result obeys the closed outcome contract.
func (r CommandResult) Valid() bool {
	switch r.Outcome {
	case CommandSucceeded:
		return r.Code == 0 && r.Text == ""
	case CommandFailed:
		return r.Code != 0 && r.Output == ""
	case CommandOutcomeUnknown:
		return r.Code == 0 && r.Output == ""
	default:
		return false
	}
}
