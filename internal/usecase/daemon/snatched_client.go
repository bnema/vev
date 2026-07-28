package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/ports"
)

type initialSnatchedPanelAttempt struct {
	done chan struct{}
	err  error
}

func (ac *attachedClient) claimInitialSnatchedPanel(generation uint64) (*initialSnatchedPanelAttempt, bool) {
	ac.initialSnatchedMu.Lock()
	defer ac.initialSnatchedMu.Unlock()
	if generation < ac.initialSnatchedGeneration {
		return nil, false
	}
	if generation == ac.initialSnatchedGeneration && ac.initialSnatchedAttempt != nil {
		return ac.initialSnatchedAttempt, false
	}
	attempt := &initialSnatchedPanelAttempt{done: make(chan struct{})}
	ac.initialSnatchedGeneration = generation
	ac.initialSnatchedAttempt = attempt
	return attempt, true
}

func (ac *attachedClient) initialSnatchedPanelClaimed(generation uint64) bool {
	ac.initialSnatchedMu.Lock()
	defer ac.initialSnatchedMu.Unlock()
	return ac.initialSnatchedGeneration == generation && ac.initialSnatchedAttempt != nil
}

func (ac *attachedClient) completeInitialSnatchedPanel(attempt *initialSnatchedPanelAttempt, err error) {
	ac.initialSnatchedMu.Lock()
	attempt.err = err
	close(attempt.done)
	ac.initialSnatchedMu.Unlock()
}

func (d *Daemon) sendInitialSnatchedPanel(token attachmentRoleToken, ticket *roleEffectTicket) error {
	attempt, owner := token.ac.claimInitialSnatchedPanel(token.generation)
	if attempt == nil {
		return errSnatchedOutputStale
	}
	if !owner {
		<-attempt.done
		return attempt.err
	}

	if !token.current() || !d.clearForSnatch(token) || !d.clearCaptureFramesForSnatch(token) {
		token.ac.completeInitialSnatchedPanel(attempt, errSnatchedOutputStale)
		return errSnatchedOutputStale
	}
	err := d.sendSnatchedPanel(token.ac, token.transport, token.generation, "", ticket)
	token.ac.completeInitialSnatchedPanel(attempt, err)
	return err
}

func (d *Daemon) clearCaptureFramesForSnatch(token attachmentRoleToken) bool {
	ac := token.ac
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	if ac.roleGeneration.Load() != token.generation || !ac.transportSnapshotCurrent(token.transport) {
		return false
	}
	ac.captureFrames = nil
	return true
}

func (d *Daemon) sendSnatchedControl(token attachmentRoleToken, frame ports.Frame) error {
	ac := token.ac
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	if ac.roleGeneration.Load() != token.generation {
		return errSnatchedOutputStale
	}
	if !ac.transportSnapshotCurrent(token.transport) {
		return errTransportReplaced
	}
	if token.effect == nil || !token.effect.beginTransportSend(token.transport) {
		return errAttachmentTransition
	}
	err := token.transport.transport.Send(frame)
	if err != nil {
		token.effect.reportTransportFailure(token.transport)
	}
	token.effect.endTransportSend()
	return err
}

// boundedSendSnatchedControl keeps a best-effort control acknowledgement bound
// to the exact admitted role and link incarnation. On deadline it closes only
// that captured link, releasing either the transport Send or a wait on sendMu.
func (d *Daemon) boundedSendSnatchedControl(token attachmentRoleToken, frame ports.Frame) error {
	if token.ac == nil || token.effect == nil || token.transport.transport == nil {
		return errAttachmentTransition
	}
	_, err := d.boundedSendWith(token.transport.transport, func() error {
		return d.sendSnatchedControl(token, frame)
	})
	if errors.Is(err, errSendTimedOut) {
		_ = token.ac.closeCapturedTransport(token.transport.transport)
	}
	return err
}
