package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bnema/vev/internal/ports"
)

const (
	proxyAttachedCommandTimeout = 10 * time.Second
	proxyResumeInitialBackoff   = 250 * time.Millisecond
	proxyResumeMaxBackoff       = 4 * time.Second
	proxyResumeMaxAttempts      = 5
	proxyResumeStableDuration   = 30 * time.Second
)

var (
	errProxyCommandTimeout     = errors.New("proxy session: attached command timed out")
	errProxyCommandUnavailable = errors.New("proxy session: attached command link is unavailable")
	errProxyCommandGeneration  = errors.New("proxy session: attached command link was replaced")
	// errProxyLinkSend marks a reply that could not be written to the remote.
	// The received frame was well formed, so the link is resumable rather than
	// terminally broken.
	errProxyLinkSend = errors.New("proxy session: link reply send failed")
)

type proxyCommandOutcome struct {
	result ports.CommandResult
	err    error
}

type proxyCommandPending struct {
	generation uint64
	outcome    chan proxyCommandOutcome
}

type proxyRecvResult struct {
	frame ports.Frame
	err   error
}

type proxyLinkResult uint8

const (
	proxyLinkStop proxyLinkResult = iota
	proxyLinkResume
	proxyLinkReplaced
)

// dialProxyHandshake installs a candidate transport, sends an ordinary Hello
// through the session's sole sender, and requires a matching Welcome. The
// candidate is never registry-visible.
func (d *Daemon) dialProxyHandshake(ctx context.Context, p *proxySession, intent uint8) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dialer, err := d.remoteDialerFactory.DialerForRemote(p.key.Host, p.key.Name, d.remoteTransportMode, d.log)
	if err != nil {
		return fmt.Errorf("proxy session: select remote dialer: %w", err)
	}
	if dialer == nil {
		return errors.New("proxy session: remote dialer factory returned nil dialer")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	transport, err := dialer.Dial(ctx)
	if err != nil {
		return fmt.Errorf("proxy session: dial: %w", err)
	}
	if transport == nil {
		return errors.New("proxy session: remote dialer returned nil transport")
	}
	if err := ctx.Err(); err != nil {
		_ = transport.Close()
		return err
	}

	p.mu.Lock()
	resumeToken := p.resumeToken
	clientID := p.clientID
	size := p.contentSize
	p.mu.Unlock()
	resume := intent == ports.IntentResume
	if !resume {
		resumeToken = 0
	}
	generation, previous := p.installTransport(transport, resume)
	if previous != nil && previous != transport {
		_ = previous.Close()
	}
	failed := true
	defer func() {
		if failed {
			p.retireTransport(generation, transport)
			_ = transport.Close()
		}
	}()
	hello := ports.Hello{
		Version:           ports.ProtocolVersion,
		Intent:            intent,
		ClientID:          clientID,
		ResumeToken:       resumeToken,
		Name:              p.key.Name,
		Size:              size,
		MaxOutputInFlight: proxyOutputWindow(transport),
	}
	if err := p.sendGeneration(generation, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)}); err != nil {
		return fmt.Errorf("proxy session: send hello: %w", err)
	}

	welcome, err := recvProxyHandshake(ctx, transport, p.key.Name)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	if p.linkGeneration != generation || p.transport != transport {
		p.mu.Unlock()
		return errors.New("proxy session: link replaced during handshake")
	}
	p.resumeToken = welcome.ResumeToken
	p.linkState = ports.LinkStateConnected
	p.expired = false
	p.mu.Unlock()
	failed = false
	return nil
}

func proxyOutputWindow(transport ports.Transport) uint8 {
	if _, ok := transport.(ports.DatagramTransport); ok {
		return 1
	}
	return maxUnackedOutputStates
}

func recvProxyHandshake(ctx context.Context, transport ports.Transport, sessionName string) (ports.Welcome, error) {
	frame, err := recvProxyHandshakeFrame(ctx, transport)
	if err != nil {
		return ports.Welcome{}, fmt.Errorf("proxy session: receive welcome: %w", err)
	}
	welcome, err := validateProxyWelcome(frame, sessionName)
	if err != nil {
		return ports.Welcome{}, err
	}
	return welcome, nil
}

func validateProxyWelcome(frame ports.Frame, sessionName string) (ports.Welcome, error) {
	if frame.Type == ports.MsgError {
		remoteErr, err := ports.UnmarshalErrorMsg(frame.Payload)
		if err != nil {
			return ports.Welcome{}, fmt.Errorf("proxy session: malformed remote error: %w", err)
		}
		return ports.Welcome{}, fmt.Errorf("proxy session: remote rejected handshake (%d): %s", remoteErr.Code, remoteErr.Text)
	}
	if frame.Type != ports.MsgWelcome {
		return ports.Welcome{}, fmt.Errorf("proxy session: first server frame is %d, want Welcome", frame.Type)
	}
	welcome, err := ports.UnmarshalWelcome(frame.Payload)
	if err != nil {
		return ports.Welcome{}, fmt.Errorf("proxy session: decode welcome: %w", err)
	}
	if welcome.SessionID == "" || welcome.SessionName != sessionName {
		return ports.Welcome{}, fmt.Errorf("proxy session: welcome identity mismatch: got %q", welcome.SessionName)
	}
	return welcome, nil
}

func recvProxyHandshakeFrame(ctx context.Context, transport ports.Transport) (ports.Frame, error) {
	result := make(chan proxyRecvResult, 1)
	go func() {
		frame, err := transport.Recv()
		result <- proxyRecvResult{frame: frame, err: err}
	}()
	select {
	case received := <-result:
		return received.frame, received.err
	case <-ctx.Done():
		_ = transport.Close()
		received := <-result // Transport.Close is required to unblock Recv.
		if received.err != nil && !errors.Is(received.err, io.EOF) {
			return ports.Frame{}, errors.Join(ctx.Err(), received.err)
		}
		return ports.Frame{}, ctx.Err()
	}
}

func (p *proxySession) installTransport(transport ports.Transport, _ bool) (uint64, ports.Transport) {
	// Link replacement participates in the same sender serialization as every
	// frame. A request can therefore only be published wholly before the
	// generation changes or wholly after it.
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	p.mu.Lock()
	previous := p.transport
	p.linkGeneration++
	generation := p.linkGeneration
	// Each transport must establish its ordinary output dependency with a
	// dependency-free full frame before incremental bytes are forwarded.
	p.resetOutputStateLocked()
	p.transport = transport
	// Authentication is complete after the remote Welcome; ordinary output
	// establishes the dependency chain asynchronously.
	p.linkState = ports.LinkStateProbing
	p.mu.Unlock()
	p.failPendingCommandsLocked(errProxyCommandGeneration)
	return generation, previous
}

func (p *proxySession) retireTransport(generation uint64, transport ports.Transport) {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	p.mu.Lock()
	current := p.linkGeneration == generation && p.transport == transport
	if current {
		p.transport = nil
	}
	p.mu.Unlock()
	if current {
		p.failPendingCommandsLocked(errProxyCommandUnavailable)
	}
}

func (p *proxySession) sendGeneration(generation uint64, frame ports.Frame) error {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	p.mu.Lock()
	if p.linkGeneration != generation || p.transport == nil {
		p.mu.Unlock()
		return errors.New("proxy session: stale or unavailable link")
	}
	transport := p.transport
	p.mu.Unlock()
	return transport.Send(frame)
}

// sendCommand sends one correlated attached palette command and waits for its
// exact result. commandMu serializes interactive requests while sendMu remains
// available to all other link traffic during the wait.
func (p *proxySession) sendCommand(ctx context.Context, clock ports.Clock, slug string, args []string) (ports.CommandResult, error) {
	if p == nil || clock == nil {
		return ports.CommandResult{}, errProxyCommandUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ports.CommandResult{}, err
	}
	p.commandMu.Lock()
	defer p.commandMu.Unlock()
	if err := ctx.Err(); err != nil {
		return ports.CommandResult{}, err
	}

	p.sendMu.Lock()
	p.mu.Lock()
	generation, transport := p.linkGeneration, p.transport
	p.mu.Unlock()
	if transport == nil {
		p.sendMu.Unlock()
		return ports.CommandResult{}, errProxyCommandUnavailable
	}
	p.commandNext++
	if p.commandNext == 0 {
		p.commandNext++
	}
	requestID := p.commandNext
	payload, err := ports.MarshalCommandRequest(ports.CommandRequest{
		Version: ports.ProtocolVersion, RequestID: requestID, Attached: true,
		Slug: slug, Args: append([]string(nil), args...),
	})
	if err != nil {
		p.sendMu.Unlock()
		return ports.CommandResult{}, err
	}
	outcome := make(chan proxyCommandOutcome, 1)
	p.commandPending[requestID] = proxyCommandPending{generation: generation, outcome: outcome}
	if err := transport.Send(ports.Frame{Type: ports.MsgCommand, Payload: payload}); err != nil {
		delete(p.commandPending, requestID)
		p.sendMu.Unlock()
		return ports.CommandResult{}, err
	}
	p.sendMu.Unlock()

	timer := clock.NewTimer(proxyAttachedCommandTimeout)
	if timer == nil {
		p.removePendingCommand(requestID, generation)
		return ports.CommandResult{}, errProxyCommandTimeout
	}
	defer timer.Stop()
	select {
	case completed := <-outcome:
		return completed.result, completed.err
	case <-ctx.Done():
		p.removePendingCommand(requestID, generation)
		return ports.CommandResult{}, ctx.Err()
	case <-timer.C():
		p.removePendingCommand(requestID, generation)
		return ports.CommandResult{}, errProxyCommandTimeout
	}
}

func (p *proxySession) removePendingCommand(requestID, generation uint64) {
	p.sendMu.Lock()
	pending, ok := p.commandPending[requestID]
	if ok && pending.generation == generation {
		delete(p.commandPending, requestID)
	}
	p.sendMu.Unlock()
}

// completeCommandResult ignores malformed correlation state, unknown/late IDs,
// and results received by a replacement generation.
func (p *proxySession) completeCommandResult(generation uint64, result ports.CommandResult) {
	if result.RequestID == 0 {
		return
	}
	p.sendMu.Lock()
	pending, ok := p.commandPending[result.RequestID]
	if !ok || pending.generation != generation {
		p.sendMu.Unlock()
		return
	}
	delete(p.commandPending, result.RequestID)
	p.sendMu.Unlock()
	pending.outcome <- proxyCommandOutcome{result: result}
}

// failPendingCommandsLocked requires sendMu and never blocks.
func (p *proxySession) failPendingCommandsLocked(err error) {
	for id, pending := range p.commandPending {
		delete(p.commandPending, id)
		pending.outcome <- proxyCommandOutcome{err: err}
	}
}

func (d *Daemon) runProxyLink(ctx context.Context, p *proxySession) {
	defer p.finish()
	connectedAt := d.clock.Now()
	resumeAttempts := 0
	for {
		p.mu.Lock()
		generation := p.linkGeneration
		transport := p.transport
		p.mu.Unlock()
		if transport == nil {
			return
		}

		result := d.runProxyTransport(ctx, p, generation, transport)
		if result == proxyLinkReplaced {
			// ReasonReplaced is terminal for the exact transport incarnation. Publish
			// expiry before retirement clears the transport identity used by the
			// callback fence; the switch below deliberately never redials it.
			d.markProxyReplaced(p, generation, transport)
		}
		p.retireTransport(generation, transport)
		switch result {
		case proxyLinkReplaced, proxyLinkStop:
			return
		case proxyLinkResume:
			if ctx.Err() != nil {
				return
			}
			if d.clock.Now().Sub(connectedAt) >= proxyResumeStableDuration {
				resumeAttempts = 0
			}
			d.updateProxyDisconnectedState(p, generation, ports.LinkStateOffline)

			for {
				if resumeAttempts >= proxyResumeMaxAttempts {
					d.log.Warn("proxy resume attempts exhausted", "host", p.key.Host, "session", p.key.Name, "attempts", resumeAttempts)
					return
				}
				delay := proxyResumeBackoff(resumeAttempts)
				if !waitProxyResume(ctx, d.clock, delay) {
					return
				}
				resumeAttempts++
				if err := d.dialProxyHandshake(ctx, p, ports.IntentResume); err != nil {
					p.mu.Lock()
					failedGeneration := p.linkGeneration
					p.mu.Unlock()
					d.updateProxyDisconnectedState(p, failedGeneration, ports.LinkStateOffline)
					if ctx.Err() != nil {
						return
					}
					d.log.Warn("proxy resume failed", "host", p.key.Host, "session", p.key.Name, "attempt", resumeAttempts, "err", err)
					continue
				}
				connectedAt = d.clock.Now()
				d.repaintProxyLifecycle(p)
				break
			}
		}
	}
}

func proxyResumeBackoff(attempt int) time.Duration {
	delay := proxyResumeInitialBackoff
	for range attempt {
		if delay >= proxyResumeMaxBackoff/2 {
			return proxyResumeMaxBackoff
		}
		delay *= 2
	}
	return min(delay, proxyResumeMaxBackoff)
}

func waitProxyResume(ctx context.Context, clock ports.Clock, delay time.Duration) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	timer := clock.NewTimer(delay)
	if timer == nil {
		return false
	}
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C():
		return true
	}
}

func (d *Daemon) runProxyTransport(ctx context.Context, p *proxySession, generation uint64, transport ports.Transport) proxyLinkResult {
	recvCtx, cancelRecv := context.WithCancel(ctx)
	recv := make(chan proxyRecvResult, 1)
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			frame, err := transport.Recv()
			select {
			case recv <- proxyRecvResult{frame: frame, err: err}:
			case <-recvCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		cancelRecv()
		_ = transport.Close()
		<-recvDone
	}()

	var events <-chan ports.LinkEvent
	if reporter, ok := transport.(ports.LinkStateReporter); ok {
		events = reporter.LinkEvents()
	}
	for {
		select {
		case <-ctx.Done():
			return proxyLinkStop
		case event, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			if !d.updateProxyLinkState(p, generation, transport, event.State) {
				return proxyLinkStop
			}
			if event.State == ports.LinkStateOffline || event.State == ports.LinkStateDead {
				_ = transport.Close()
				return proxyLinkResume
			}
		case received := <-recv:
			if received.err != nil {
				if ctx.Err() != nil {
					return proxyLinkStop
				}
				return proxyLinkResume
			}
			result, err := d.handleLinkFrame(p, generation, received.frame)
			if err != nil {
				// A failed reply says nothing about the remote's framing, so it
				// retires this transport instead of the whole proxy link.
				if errors.Is(err, errProxyLinkSend) {
					d.log.Warn("proxy link reply failed", "host", p.key.Host, "session", p.key.Name, "err", err)
					return proxyLinkResume
				}
				d.log.Warn("invalid proxy link frame", "host", p.key.Host, "session", p.key.Name, "err", err)
				return proxyLinkStop
			}
			if result != proxyLinkResume {
				return result
			}
		}
	}
}

// handleProxySideEffect forwards a stateless remote effect to the exact local
// attachment that currently owns this proxy. Connection admission and transport
// incarnation checks make a stale handoff a harmless drop rather than a send
// to the client's next session.
func (d *Daemon) handleProxySideEffect(p *proxySession, generation uint64, out ports.Output) error {
	if p == nil || !p.currentLinkGeneration(generation) {
		return nil
	}
	attachments := snapshotAttachmentSession(p)
	if len(attachments) == 0 {
		return nil
	}
	ac := attachments[0]
	if ac.output == nil {
		return nil
	}
	expected := ac.transportSnapshot()
	if expected.transport == nil {
		return nil
	}
	token := attachmentToken(p, ac, expected.transport)
	if token.sess != p {
		return nil
	}
	ticket, admitted := ac.beginAttachmentEffect(token)
	if !admitted {
		return nil
	}
	defer ticket.End()
	if !p.currentLinkGeneration(generation) {
		return nil
	}
	frame := ac.output.sideEffect(out.Data, ac.echoAck.Load())
	if err := ac.sendExpectedTransportForAttachment(expected, frame, ticket); err != nil {
		if errors.Is(err, errAttachmentTransition) {
			return nil
		}
		return fmt.Errorf("proxy session: side-effect send: %w: %w", errProxyLinkSend, err)
	}
	return nil
}

// handleLinkFrame returns proxyLinkResume to mean "continue this transport";
// the other values are terminal actions.
func (d *Daemon) handleLinkFrame(p *proxySession, generation uint64, frame ports.Frame) (proxyLinkResult, error) {
	switch frame.Type {
	case ports.MsgOutput:
		out, err := ports.UnmarshalOutput(frame.Payload)
		if err != nil {
			return proxyLinkStop, err
		}
		if err := d.handleProxyOutput(p, generation, out); err != nil {
			return proxyLinkStop, err
		}
		return proxyLinkResume, nil
	case ports.MsgCommandResult:
		result, err := ports.UnmarshalCommandResult(frame.Payload)
		if err != nil {
			return proxyLinkStop, err
		}
		p.completeCommandResult(generation, result)
		return proxyLinkResume, nil
	case ports.MsgPong:
		_, err := ports.UnmarshalPong(frame.Payload)
		return proxyLinkResume, err
	case ports.MsgPing:
		if _, err := ports.UnmarshalPing(frame.Payload); err != nil {
			return proxyLinkStop, err
		}
		if err := p.sendGeneration(generation, ports.Frame{Type: ports.MsgPong, Payload: ports.MarshalPong(ports.Pong{})}); err != nil {
			return proxyLinkStop, fmt.Errorf("proxy pong reply: %w: %w", errProxyLinkSend, err)
		}
		return proxyLinkResume, nil
	case ports.MsgDetached:
		detached, err := ports.UnmarshalDetached(frame.Payload)
		if err != nil {
			return proxyLinkStop, err
		}
		if detached.Reason == ports.ReasonReplaced {
			return proxyLinkReplaced, nil
		}
		return proxyLinkStop, nil
	case ports.MsgError:
		remoteErr, err := ports.UnmarshalErrorMsg(frame.Payload)
		if err != nil {
			return proxyLinkStop, err
		}
		return proxyLinkStop, fmt.Errorf("remote proxy error (%d): %s", remoteErr.Code, remoteErr.Text)
	default:
		return proxyLinkStop, fmt.Errorf("unexpected proxy server frame type %d", frame.Type)
	}
}
