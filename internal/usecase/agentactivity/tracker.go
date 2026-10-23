package agentactivity

import "time"

// Tracker timing.
const (
	// DefaultIdleHold is the quiet window a weak idle signal must survive
	// while Working or Blocked before Idle is published.
	DefaultIdleHold = 350 * time.Millisecond
	// DefaultIdleHoldCap bounds an idle hold that keeps being extended by
	// changing text.
	DefaultIdleHoldCap = 700 * time.Millisecond
	// DefaultStartupGrace is how long a freshly identified agent ignores
	// weak idle signals.
	DefaultStartupGrace = 3 * time.Second
	// DefaultMinDoneWorking is the minimum Working duration for a
	// Working → Idle transition to count as a completed turn.
	DefaultMinDoneWorking = 2 * time.Second
)

// Update is the outcome of one Tracker call.
type Update struct {
	Agent Agent
	From  State
	To    State
	// AgentChanged reports a new identification; From is the previous
	// agent's state and To is Unknown.
	AgentChanged bool
	// Done reports a completed turn: Working → Idle after at least
	// DefaultMinDoneWorking. The caller decides whether the completion is unseen.
	Done bool
	// Rule is the rule that caused the transition.
	Rule string
	// Deadline, when non-zero, is the time at which the caller must call
	// Tick (or Observe with a fresh scan) to resolve a pending decision.
	Deadline time.Time
}

// Changed reports whether the published state or agent changed.
func (u Update) Changed() bool { return u.AgentChanged || u.From != u.To }

// Tracker is the per-pane activity state machine. It is pure: every call
// receives the current time. The zero value tracks no agent. It is not safe
// for concurrent use.
type Tracker struct {
	agent        Agent
	state        State
	identifiedAt time.Time
	workingSince time.Time

	// last is the most recent observed signal; Tick re-applies it.
	last Signal

	holding   bool
	holdStart time.Time
	holdUntil time.Time
}

// Agent returns the tracked agent.
func (t *Tracker) Agent() Agent { return t.agent }

// State returns the published state.
func (t *Tracker) State() State { return t.state }

// Identify records the pane's foreground agent. A different agent, including
// None when the agent exits, resets the tracker to Unknown without Done.
func (t *Tracker) Identify(a Agent, now time.Time) Update {
	if a == t.agent {
		return t.update(t.state, t.state, "", now)
	}
	from := t.state
	*t = Tracker{agent: a, identifiedAt: now}
	u := t.update(from, Unknown, "identify", now)
	u.AgentChanged = true
	return u
}

// Observe applies the classification of a fresh scan.
func (t *Tracker) Observe(sig Signal, now time.Time) Update {
	if t.agent == None {
		return Update{}
	}
	t.last = sig
	return t.apply(sig, now, true)
}

// Tick resolves pending deadlines when the screen did not change. It
// re-applies the last observed signal without treating it as new text.
func (t *Tracker) Tick(now time.Time) Update {
	if t.agent == None {
		return Update{}
	}
	return t.apply(t.last, now, false)
}

func (t *Tracker) apply(sig Signal, now time.Time, fresh bool) Update {
	from := t.state
	switch {
	case !sig.Matched() || sig.Hold:
		if t.holding && !now.Before(t.holdUntil) {
			return t.publishIdle(from, t.holdRule(), now)
		}
		return t.update(from, from, "", now)
	case sig.State == Blocked:
		// workingSince is kept from a Working turn and never started here:
		// time spent waiting on the user before a turn is not work.
		t.cancelHold()
		t.state = Blocked
		return t.update(from, Blocked, sig.Rule, now)
	case sig.State == Working:
		t.cancelHold()
		if (from != Working && from != Blocked) || t.workingSince.IsZero() {
			t.workingSince = now
		}
		t.state = Working
		return t.update(from, Working, sig.Rule, now)
	case sig.State == Idle && sig.Strong:
		t.cancelHold()
		return t.publishIdle(from, sig.Rule, now)
	case sig.State == Idle:
		return t.weakIdle(from, sig.Rule, now, fresh)
	}
	return t.update(from, from, "", now)
}

func (t *Tracker) weakIdle(from State, rule string, now time.Time, fresh bool) Update {
	switch from {
	case Working, Blocked:
		// A weak idle signal may be a dialog redraw; hold it like a
		// spinner flicker.
		if !t.holding {
			t.holding = true
			t.holdStart = now
			t.holdUntil = now.Add(DefaultIdleHold)
		} else if fresh {
			t.holdUntil = minTime(now.Add(DefaultIdleHold), t.holdStart.Add(DefaultIdleHoldCap))
		}
		if !now.Before(t.holdUntil) {
			return t.publishIdle(from, rule, now)
		}
		return t.update(from, from, "", now)
	case Unknown:
		if now.Before(t.identifiedAt.Add(DefaultStartupGrace)) {
			return t.update(from, from, "", now)
		}
	}
	return t.publishIdle(from, rule, now)
}

func (t *Tracker) publishIdle(from State, rule string, now time.Time) Update {
	done := from == Working && !t.workingSince.IsZero() &&
		now.Sub(t.workingSince) >= DefaultMinDoneWorking
	t.cancelHold()
	t.state = Idle
	t.workingSince = time.Time{}
	u := t.update(from, Idle, rule, now)
	u.Done = done
	return u
}

func (t *Tracker) holdRule() string {
	if t.last.State == Idle && t.last.Matched() {
		return t.last.Rule
	}
	return "idle-hold"
}

func (t *Tracker) cancelHold() {
	t.holding = false
	t.holdStart = time.Time{}
	t.holdUntil = time.Time{}
}

// update builds an Update and computes the next deadline.
func (t *Tracker) update(from, to State, rule string, now time.Time) Update {
	u := Update{Agent: t.agent, From: from, To: to}
	if from != to {
		u.Rule = rule
	}
	switch {
	case t.holding:
		u.Deadline = t.holdUntil
	case t.agent != None && t.state == Unknown && t.last.State == Idle && t.last.Matched() && !t.last.Hold:
		if grace := t.identifiedAt.Add(DefaultStartupGrace); now.Before(grace) {
			u.Deadline = grace
		}
	}
	return u
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}
