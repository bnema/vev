package agentactivity

import (
	"testing"
	"time"
)

var (
	sigWorking    = working("t.working")
	sigBlocked    = blocked("t.blocked")
	sigIdleWeak   = idle("t.idle")
	sigIdleStrong = idleStrong("t.idle-strong")
	sigHold       = hold("t.hold")
	sigNone       = Signal{}
)

// step is one Tracker call at offset at from the start time.
type step struct {
	at       time.Duration
	op       string // observe, tick, identify
	sig      Signal
	agent    Agent
	want     State
	done     bool
	deadline time.Duration // 0 means no deadline
}

func observe(at time.Duration, sig Signal, want State) step {
	return step{at: at, op: "observe", sig: sig, want: want}
}

func tick(at time.Duration, want State) step { return step{at: at, op: "tick", want: want} }

func (s step) withDone() step                    { s.done = true; return s }
func (s step) withDeadline(d time.Duration) step { s.deadline = d; return s }
func identifyAt(at time.Duration, a Agent, want State) step {
	return step{at: at, op: "identify", agent: a, want: want}
}

const ms = time.Millisecond

func TestTracker(t *testing.T) {
	// Every scenario starts with Claude identified at t0 and, unless noted,
	// already Working since t0 (past the startup grace is irrelevant once
	// Working).
	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "spinner flicker cancels the idle hold",
			steps: []step{
				observe(0, sigWorking, Working),
				observe(3000*ms, sigIdleWeak, Working).withDeadline(3350 * ms),
				observe(3200*ms, sigWorking, Working),
				tick(3400*ms, Working),
			},
		},
		{
			name: "weak idle publishes after the hold",
			steps: []step{
				observe(0, sigWorking, Working),
				observe(3000*ms, sigIdleWeak, Working).withDeadline(3350 * ms),
				tick(3349*ms, Working).withDeadline(3350 * ms),
				tick(3350*ms, Idle).withDone(),
			},
		},
		{
			name: "fresh weak idle extends the hold up to the cap",
			steps: []step{
				observe(0, sigWorking, Working),
				observe(3000*ms, sigIdleWeak, Working).withDeadline(3350 * ms),
				observe(3300*ms, sigIdleWeak, Working).withDeadline(3650 * ms),
				observe(3600*ms, sigIdleWeak, Working).withDeadline(3700 * ms),
				observe(3700*ms, sigIdleWeak, Idle).withDone(),
			},
		},
		{
			name: "hold and unmatched text keep the idle hold running",
			steps: []step{
				observe(0, sigWorking, Working),
				observe(3000*ms, sigIdleWeak, Working).withDeadline(3350 * ms),
				observe(3100*ms, sigHold, Working).withDeadline(3350 * ms),
				observe(3200*ms, sigNone, Working).withDeadline(3350 * ms),
				tick(3360*ms, Idle).withDone(),
			},
		},
		{
			name: "hold never changes the published state",
			steps: []step{
				observe(0, sigWorking, Working),
				observe(100*ms, sigHold, Working),
				tick(10*time.Second, Working),
				observe(11*time.Second, sigIdleStrong, Idle).withDone(),
				observe(12*time.Second, sigHold, Idle),
			},
		},
		{
			name: "strong idle publishes immediately",
			steps: []step{
				observe(0, sigWorking, Working),
				observe(2500*ms, sigIdleStrong, Idle).withDone(),
			},
		},
		{
			name: "short turn is not done",
			steps: []step{
				observe(0, sigWorking, Working),
				observe(1999*ms, sigIdleStrong, Idle),
			},
		},
		{
			name: "done floor counts from working onset, not the hold end",
			steps: []step{
				observe(0, sigWorking, Working),
				observe(1700*ms, sigIdleWeak, Working).withDeadline(2050 * ms),
				tick(2050*ms, Idle).withDone(),
			},
		},
		{
			name: "blocked is immediate and cancels the hold",
			steps: []step{
				observe(0, sigWorking, Working),
				observe(3000*ms, sigIdleWeak, Working).withDeadline(3350 * ms),
				observe(3100*ms, sigBlocked, Blocked),
				tick(3400*ms, Blocked),
			},
		},
		{
			name: "dismissed dialog goes idle without hold and keeps the turn",
			steps: []step{
				observe(0, sigWorking, Working),
				observe(1000*ms, sigBlocked, Blocked),
				observe(5000*ms, sigWorking, Working),
				observe(6000*ms, sigIdleStrong, Idle).withDone(),
			},
		},
		{
			name: "waiting on the user before a turn does not count toward done",
			steps: []step{
				observe(0, sigBlocked, Blocked),
				observe(5000*ms, sigWorking, Working),
				observe(5100*ms, sigIdleStrong, Idle),
			},
		},
		{
			name: "blocked from idle does not start the turn",
			steps: []step{
				observe(4000*ms, sigIdleWeak, Idle),
				observe(5000*ms, sigBlocked, Blocked),
				observe(9000*ms, sigWorking, Working),
				observe(10000*ms, sigIdleStrong, Idle),
			},
		},
		{
			name: "blocked to weak idle waits for the hold",
			steps: []step{
				observe(0, sigBlocked, Blocked),
				observe(1000*ms, sigIdleWeak, Blocked).withDeadline(1350 * ms),
				tick(1349*ms, Blocked).withDeadline(1350 * ms),
				tick(1350*ms, Idle),
			},
		},
		{
			name: "blocked dialog redraw cancels the hold",
			steps: []step{
				observe(0, sigBlocked, Blocked),
				observe(1000*ms, sigIdleWeak, Blocked).withDeadline(1350 * ms),
				observe(1100*ms, sigBlocked, Blocked),
				tick(1400*ms, Blocked),
			},
		},
		{
			name: "blocked to strong idle publishes immediately",
			steps: []step{
				observe(0, sigBlocked, Blocked),
				observe(1000*ms, sigIdleStrong, Idle),
			},
		},
		{
			name: "startup grace delays weak idle",
			steps: []step{
				observe(500*ms, sigIdleWeak, Unknown).withDeadline(3000 * ms),
				tick(2999*ms, Unknown).withDeadline(3000 * ms),
				tick(3000*ms, Idle),
			},
		},
		{
			name: "startup grace yields to strong idle",
			steps: []step{
				observe(100*ms, sigIdleStrong, Idle),
			},
		},
		{
			name: "weak idle after grace publishes without done",
			steps: []step{
				observe(4000*ms, sigIdleWeak, Idle),
			},
		},
		{
			name: "idle to working back to idle",
			steps: []step{
				observe(4000*ms, sigIdleWeak, Idle),
				observe(5000*ms, sigWorking, Working),
				observe(6000*ms, sigIdleStrong, Idle),
				observe(7000*ms, sigWorking, Working),
				observe(9500*ms, sigIdleStrong, Idle).withDone(),
			},
		},
		{
			name: "agent change resets without done",
			steps: []step{
				observe(0, sigWorking, Working),
				identifyAt(5000*ms, Codex, Unknown),
				observe(5100*ms, sigIdleStrong, Idle),
			},
		},
		{
			name: "agent exit drops to unknown and ignores signals",
			steps: []step{
				observe(0, sigWorking, Working),
				identifyAt(5000*ms, None, Unknown),
				observe(5100*ms, sigWorking, Unknown),
				tick(6000*ms, Unknown),
			},
		},
		{
			name: "same agent identify is a no-op",
			steps: []step{
				observe(0, sigWorking, Working),
				identifyAt(1000*ms, Claude, Working),
				observe(2500*ms, sigIdleStrong, Idle).withDone(),
			},
		},
	}
	t0 := time.Unix(1_700_000_000, 0)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tr Tracker
			if u := tr.Identify(Claude, t0); !u.AgentChanged || u.To != Unknown {
				t.Fatalf("initial Identify = %+v", u)
			}
			for i, s := range tt.steps {
				now := t0.Add(s.at)
				var u Update
				switch s.op {
				case "observe":
					u = tr.Observe(s.sig, now)
				case "tick":
					u = tr.Tick(now)
				case "identify":
					u = tr.Identify(s.agent, now)
				}
				if (s.op != "identify" && u.To != s.want) || tr.State() != s.want {
					t.Fatalf("step %d (%s @%v): state %s (update %s→%s), want %s", i, s.op, s.at, tr.State(), u.From, u.To, s.want)
				}
				if u.Done != s.done {
					t.Fatalf("step %d (%s @%v): Done = %t, want %t", i, s.op, s.at, u.Done, s.done)
				}
				var wantDeadline time.Time
				if s.deadline != 0 {
					wantDeadline = t0.Add(s.deadline)
				}
				if !u.Deadline.Equal(wantDeadline) {
					t.Fatalf("step %d (%s @%v): Deadline = %v, want %v", i, s.op, s.at, u.Deadline.Sub(t0), s.deadline)
				}
			}
		})
	}
}

func TestTrackerUpdateReportsTransitions(t *testing.T) {
	t0 := time.Unix(0, 0)
	var tr Tracker
	tr.Identify(Zut, t0)
	u := tr.Observe(working("zut.busy"), t0)
	if !u.Changed() || u.From != Unknown || u.To != Working || u.Rule != "zut.busy" || u.Agent != Zut {
		t.Fatalf("onset = %+v", u)
	}
	u = tr.Observe(working("zut.busy"), t0.Add(time.Second))
	if u.Changed() || u.Rule != "" {
		t.Fatalf("steady = %+v", u)
	}
	tr.Observe(idle("zut.quiet"), t0.Add(3*time.Second))
	u = tr.Tick(t0.Add(4 * time.Second))
	if u.From != Working || u.To != Idle || u.Rule != "zut.quiet" || !u.Done {
		t.Fatalf("resolved hold = %+v", u)
	}
}

func TestZeroTrackerIgnoresSignals(t *testing.T) {
	var tr Tracker
	if u := tr.Observe(sigBlocked, time.Unix(0, 0)); u.Changed() || tr.State() != Unknown {
		t.Fatalf("zero tracker changed: %+v", u)
	}
}

// paneState is one pane's tracked state and unseen-completion flag.
type paneState struct {
	s    State
	done bool
}

func TestIndicatorAggregation(t *testing.T) {
	tests := []struct {
		name  string
		panes []paneState
		want  Indicator
	}{
		{"no panes", nil, IndicatorNone},
		{"done beats working", []paneState{{Idle, true}, {Working, false}}, IndicatorDone},
		{"blocked beats done", []paneState{{Blocked, false}, {Idle, true}}, IndicatorBlocked},
		{"working beats idle", []paneState{{Idle, false}, {Working, false}}, IndicatorWorking},
		{"blocked beats working", []paneState{{Working, false}, {Blocked, false}, {Idle, true}}, IndicatorBlocked},
		{"done beats idle", []paneState{{Idle, false}, {Idle, true}}, IndicatorDone},
		{"idle beats unknown", []paneState{{Unknown, false}, {Idle, false}}, IndicatorIdle},
		{"done survives agent exit", []paneState{{Unknown, true}}, IndicatorDone},
		{"working masks its own stale done", []paneState{{Working, true}}, IndicatorWorking},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IndicatorNone
			for _, p := range tt.panes {
				got = MaxIndicator(got, IndicatorFor(p.s, p.done))
			}
			if got != tt.want {
				t.Fatalf("aggregate = %s, want %s", got, tt.want)
			}
		})
	}
}
