package agentactivity

// State is what an agent is doing, ordered by display priority.
type State uint8

const (
	// Unknown means no agent state is established yet.
	Unknown State = iota
	Idle
	Working
	Blocked
)

// String returns the lowercase state name used in logs.
func (s State) String() string {
	switch s {
	case Idle:
		return "idle"
	case Working:
		return "working"
	case Blocked:
		return "blocked"
	default:
		return "unknown"
	}
}

// Signal is the classification of one screen observation.
//
// The zero Signal means no rule matched: the screen carries no opinion.
type Signal struct {
	State State
	// Strong signals bypass the idle debounce and the startup grace.
	Strong bool
	// Hold means a transient user-driven overlay hides the agent status.
	// A Hold freezes the screen's opinion: it starts no transition, but an
	// idle hold already pending still resolves.
	Hold bool
	// Rule identifies the matching rule for debug logs.
	Rule string
}

// Matched reports whether a rule produced the signal.
func (s Signal) Matched() bool { return s.Rule != "" }

// Indicator is the activity shown for a tab, ordered by display priority. A
// finished agent ranks above a working one: it needs the user's eye.
type Indicator uint8

const (
	IndicatorNone Indicator = iota
	IndicatorIdle
	IndicatorWorking
	IndicatorDone
	IndicatorBlocked
)

// String returns the lowercase indicator name used in logs.
func (i Indicator) String() string {
	switch i {
	case IndicatorIdle:
		return "idle"
	case IndicatorDone:
		return "done"
	case IndicatorWorking:
		return "working"
	case IndicatorBlocked:
		return "blocked"
	default:
		return "none"
	}
}

// IndicatorFor combines one pane's state with its unseen-completion flag.
// done only matters while the agent is idle.
func IndicatorFor(s State, done bool) Indicator {
	switch s {
	case Blocked:
		return IndicatorBlocked
	case Working:
		return IndicatorWorking
	}
	if done {
		return IndicatorDone
	}
	if s == Idle {
		return IndicatorIdle
	}
	return IndicatorNone
}

// MaxIndicator aggregates panes of one tab and tabs of one session:
// Blocked > Done > Working > Idle > None.
func MaxIndicator(a, b Indicator) Indicator {
	if b > a {
		return b
	}
	return a
}
