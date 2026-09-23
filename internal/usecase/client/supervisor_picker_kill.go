package client

import (
	"context"
	"errors"
	"sync"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Picker kill.
//
// `x` on a local session row kills it through the broker operations seam: one
// independent, bounded control stream to the owning daemon, never the
// attachment stream. A live session is torn down; a stopped session's history
// record is deleted by the same typed Kill, which the daemon routes to its
// stopped-session purge. The operation runs off the supervisor's run goroutine
// so the picker keeps rendering; its typed outcome comes back as one notice and
// a catalogue reconcile. An unknown outcome is reported and never replayed.

// pickerKillResolver is the optional kill resolution of the real picker.
type pickerKillResolver interface {
	ResolveKill(key string) (pickerKillTarget, error)
}

// pickerCurrentHost is the optional cursor seam of the real picker.
type pickerCurrentHost interface {
	SetCurrent(pickerCurrent)
}

// pickerKillOutcome is one settled kill.
type pickerKillOutcome struct {
	target pickerKillTarget
	result protocol.KillResult
	err    error
}

// pickerKills owns at most one in-flight picker kill. It is only touched from
// the supervisor's run goroutine; the worker goroutine only sends on done.
type pickerKills struct {
	running bool
	done    chan pickerKillOutcome
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// results is the settled-kill wake, nil while no kill runs.
func (k *pickerKills) results() <-chan pickerKillOutcome {
	if k == nil || !k.running {
		return nil
	}
	return k.done
}

// setPickerCurrent names the attachment the picker is presented over.
func (s *Supervisor) setPickerCurrent(current pickerCurrent) {
	if host, ok := s.cfg.Picker.(pickerCurrentHost); ok {
		host.SetCurrent(current)
	}
}

// startPickerKill resolves one kill key against the latest catalogue and runs
// the kill. A refusal is already a picker notice; a second `x` while one kill
// runs is refused rather than queued.
func (s *Supervisor) startPickerKill(service ports.BrokerService, key string) {
	resolver, ok := s.cfg.Picker.(pickerKillResolver)
	if !ok || key == "" || supervisorNil(service) {
		return
	}
	if s.kills.running {
		s.offerPickerNotice("picker-kill", "a kill is already in progress")
		return
	}
	target, err := resolver.ResolveKill(key)
	if err != nil {
		return
	}
	operations, err := NewBrokerOperations(service, s.cfg.Clock)
	if err != nil {
		s.offerPickerNotice("picker-kill", "couldn't kill "+target.name+": broker unavailable")
		return
	}
	if s.kills.done == nil {
		s.kills.done = make(chan pickerKillOutcome, 1)
	}
	killCtx, cancel := context.WithCancel(context.Background())
	s.kills.running = true
	s.kills.cancel = cancel
	s.kills.wg.Add(1)
	go func() {
		defer s.kills.wg.Done()
		result, err := operations.Kill(killCtx, target.route, target.name)
		s.kills.done <- pickerKillOutcome{target: target, result: result, err: err}
	}()
}

// finishPickerKill reports one settled kill and asks the broker to re-observe
// the local daemon, so the killed row leaves the catalogue promptly.
func (s *Supervisor) finishPickerKill(service ports.BrokerService, outcome pickerKillOutcome) {
	s.settlePickerKill()
	s.offerPickerNotice("picker-kill", pickerKillNotice(outcome))
	if !supervisorNil(service) {
		service.RequestReconcile("")
	}
	s.renderCurrent()
}

// retirePickerKill cancels and joins an in-flight kill before its borrowed
// service closes. Its outcome is still reported, never replayed.
func (s *Supervisor) retirePickerKill() {
	if !s.kills.running {
		return
	}
	s.kills.cancel()
	s.kills.wg.Wait()
	outcome := <-s.kills.done
	s.settlePickerKill()
	s.offerPickerNotice("picker-kill", pickerKillNotice(outcome))
}

func (s *Supervisor) settlePickerKill() {
	if s.kills.cancel != nil {
		s.kills.cancel()
	}
	s.kills.running = false
	s.kills.cancel = nil
}

// pickerKillNotice renders one typed kill outcome.
func pickerKillNotice(outcome pickerKillOutcome) string {
	name := outcome.target.name
	verb, done := "kill", "killed session "
	if outcome.target.stopped {
		verb, done = "delete stopped session", "deleted stopped session "
	}
	if outcome.err != nil {
		if errors.Is(outcome.err, ErrBrokerOperationNotSent) {
			return "couldn't " + verb + " " + name + ": not sent"
		}
		return "couldn't " + verb + " " + name
	}
	switch outcome.result.Outcome {
	case protocol.KillSucceeded:
		return done + name
	case protocol.KillFailed:
		if outcome.result.Text != "" {
			return "couldn't " + verb + " " + name + ": " + outcome.result.Text
		}
		return "couldn't " + verb + " " + name
	default:
		// The request may have reached the daemon: never retried.
		return verb + " " + name + ": outcome unknown"
	}
}
