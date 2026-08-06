package daemon

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/vt"
)

var (
	errRemoteViewUnavailable = errors.New("remote view: link is unavailable")
	errRemoteViewStale       = errors.New("remote view: link is stale")
)

// remoteViewConstruction reserves one remote-view key while its candidate
// dials and handshakes. It is guarded by Daemon.mu. cancel is invoked only
// after the daemon lock is released, because it may interrupt adapter I/O.
type remoteViewConstruction struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    error
}

// remoteLink is one exact transport generation for a remote view. sendMu
// orders every outbound remote frame; view.mu guards whether this exact link
// remains current. Neither lock is held while transport I/O runs.
type remoteLink struct {
	view       *remoteView
	generation uint64
	target     domain.RemoteSessionTarget
	clientID   [16]byte
	transport  ports.Transport
	cancel     context.CancelFunc

	sendMu        sync.Mutex
	startOnce     sync.Once
	workerStarted atomic.Bool
	done          chan struct{}
	active        bool // guarded by view.mu
}

// remoteOutputState is the remote-link equivalent of the thin client's
// dependency chain. It remains private to the daemon because this link owns a
// persistent VT rather than forwarding output bytes to a terminal.
type remoteOutputState struct {
	epoch        uint64
	state        uint64
	viewRevision uint64
	initialized  bool
}

func (s remoteOutputState) next(output ports.Output) (remoteOutputState, bool) {
	if output.Epoch == 0 {
		return remoteOutputState{}, false
	}
	if !s.initialized {
		if output.New == 0 {
			return s, true
		}
		if output.Base != 0 || !output.Full {
			return remoteOutputState{}, false
		}
		return remoteOutputState{epoch: output.Epoch, state: output.New, viewRevision: output.ViewRevision, initialized: true}, true
	}
	if output.Epoch < s.epoch {
		return remoteOutputState{}, false
	}
	if output.Epoch == s.epoch {
		if output.ViewRevision != s.viewRevision {
			return remoteOutputState{}, false
		}
		if output.New == 0 {
			if output.Base != 0 || output.Full {
				return remoteOutputState{}, false
			}
			return s, true
		}
		if output.Full || output.Base != s.state || output.New != output.Base+1 {
			return remoteOutputState{}, false
		}
		return remoteOutputState{epoch: s.epoch, state: output.New, viewRevision: s.viewRevision, initialized: true}, true
	}
	if !output.Full || output.Base != 0 {
		return remoteOutputState{}, false
	}
	return remoteOutputState{epoch: output.Epoch, state: output.New, viewRevision: output.ViewRevision, initialized: true}, true
}

// openRemoteView returns the exact warm view for target or elects one
// constructor. The reservation is protected only by Daemon.mu; the dial and
// all handshake I/O happen after every architecture lock is released.
func (d *Daemon) openRemoteView(ctx context.Context, target domain.RemoteSessionTarget, size domain.Size) (*remoteView, error) {
	if d == nil {
		return nil, errors.New("remote view: nil daemon")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := target.Validate(); err != nil {
		return nil, fmt.Errorf("remote view: invalid target: %w", err)
	}
	if !size.Valid() {
		return nil, fmt.Errorf("remote view: invalid size %dx%d", size.Cols, size.Rows)
	}
	if d.remoteDialerFactory == nil {
		return nil, errors.New("remote view: remote dialer is not configured")
	}
	key, err := remoteViewKeyForTarget(target)
	if err != nil {
		return nil, fmt.Errorf("remote view: key: %w", err)
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		d.mu.Lock()
		if existing := d.remoteViewByKeyLocked(key); existing != nil {
			d.mu.Unlock()
			return existing, nil
		}
		if d.closing {
			d.mu.Unlock()
			return nil, errors.New("remote view: daemon is shutting down")
		}
		if construction := d.remoteViewConstructions[key]; construction != nil {
			done := construction.done
			d.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			d.mu.Lock()
			winner := d.remoteViewByKeyLocked(key)
			constructionErr := construction.err
			d.mu.Unlock()
			if winner != nil {
				return winner, nil
			}
			if constructionErr != nil && !errors.Is(constructionErr, context.Canceled) && !errors.Is(constructionErr, context.DeadlineExceeded) {
				return nil, constructionErr
			}
			// A canceled candidate never poisons an independent waiter. Re-elect
			// if the caller still wants this exact remote view.
			continue
		}

		constructionCtx, cancel := context.WithCancel(ctx)
		construction := &remoteViewConstruction{done: make(chan struct{}), cancel: cancel}
		if d.remoteViewConstructions == nil {
			d.remoteViewConstructions = make(map[remoteViewKey]*remoteViewConstruction)
		}
		d.remoteViewConstructions[key] = construction
		d.mu.Unlock()

		candidate, constructionErr := d.constructRemoteView(constructionCtx, target, size)
		if constructionErr == nil {
			constructionErr = ctx.Err()
		}
		cancel()

		var winner *remoteView
		publishErr := constructionErr
		d.mu.Lock()
		if publishErr == nil {
			if d.closing {
				publishErr = errors.New("remote view: daemon is shutting down")
			} else if existing := d.remoteViewByKeyLocked(key); existing != nil {
				winner = existing
			} else if err := d.registerRemoteViewLocked(candidate); err != nil {
				publishErr = fmt.Errorf("remote view: publish candidate: %w", err)
			} else {
				winner = candidate
				// Starting the receiver while d.mu still protects publication keeps
				// shutdown from observing a registry-visible link with no worker
				// to join.
				d.startRemoteLink(candidate.link)
			}
		}
		construction.err = publishErr
		if d.remoteViewConstructions[key] == construction {
			delete(d.remoteViewConstructions, key)
		}
		close(construction.done)
		d.mu.Unlock()

		if candidate != nil && candidate != winner {
			d.stopUnpublishedRemoteView(candidate)
		}
		if winner != nil {
			return winner, nil
		}
		return nil, publishErr
	}
}

// constructRemoteView builds an unexposed candidate. Its caller owns a
// constructor reservation but holds no daemon, session, or view lock.
func (d *Daemon) constructRemoteView(ctx context.Context, target domain.RemoteSessionTarget, size domain.Size) (_ *remoteView, err error) {
	content := contentSize(size)
	if !content.Valid() {
		return nil, fmt.Errorf("remote view: invalid content size %dx%d", content.Cols, content.Rows)
	}
	key, err := remoteViewKeyForTarget(target)
	if err != nil {
		return nil, err
	}
	var clientID [16]byte
	if _, err := rand.Read(clientID[:]); err != nil {
		return nil, fmt.Errorf("remote view: generate client ID: %w", err)
	}
	root := context.Background()
	if d.serveCtx != nil {
		root = d.serveCtx
	}
	linkCtx, cancelLink := context.WithCancel(root)
	stopCallerCancellation := context.AfterFunc(ctx, cancelLink)
	defer stopCallerCancellation()
	view := &remoteView{
		key:            key,
		displayOrigin:  target.DisplayOrigin,
		screen:         vt.NewScreen(content.Cols, content.Rows),
		linkGeneration: 1,
	}
	link := &remoteLink{
		view:       view,
		generation: view.linkGeneration,
		target:     target,
		clientID:   clientID,
		cancel:     cancelLink,
		done:       make(chan struct{}),
		active:     true,
	}
	view.link = link
	published := false
	defer func() {
		if !published {
			d.stopUnpublishedRemoteView(view)
		}
	}()

	handshakeCtx, timedOut, finishHandshake := d.newHandshakeContext(linkCtx)
	defer finishHandshake()
	dialer, err := d.remoteDialerFactory.DialerForRemote(target.Endpoint, target.SessionName, d.remoteTransportMode, d.log)
	if err != nil {
		return nil, fmt.Errorf("remote view: select remote dialer: %w", err)
	}
	if dialer == nil {
		return nil, errors.New("remote view: remote dialer factory returned nil dialer")
	}
	transport, err := dialer.Dial(handshakeCtx)
	if err != nil {
		return nil, handshakeContextError(ctx, timedOut, fmt.Errorf("remote view: dial: %w", err))
	}
	if transport == nil {
		return nil, errors.New("remote view: remote dialer returned nil transport")
	}
	link.transport = transport
	stopWatch := watchHandshakeTransport(handshakeCtx, transport)
	defer stopWatch()

	helloTarget := target
	hello := ports.Hello{
		Version:           ports.ProtocolVersion,
		Intent:            ports.IntentAttach,
		ClientID:          clientID,
		Name:              target.SessionName,
		Size:              content,
		RenderMode:        ports.RenderModeProxiedContent,
		MaxOutputInFlight: remoteLinkOutputWindow(transport),
		RemoteTarget:      &helloTarget,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}
	if err := link.send(ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)}); err != nil {
		return nil, handshakeContextError(ctx, timedOut, fmt.Errorf("remote view: send hello: %w", err))
	}

	welcomeFrame, err := recvRemoteLinkHandshakeFrame(handshakeCtx, transport)
	if err != nil {
		return nil, handshakeContextError(ctx, timedOut, fmt.Errorf("remote view: receive welcome: %w", err))
	}
	if err := validateRemoteLinkWelcome(welcomeFrame, target); err != nil {
		return nil, err
	}
	metadataFrame, err := recvRemoteLinkHandshakeFrame(handshakeCtx, transport)
	if err != nil {
		return nil, handshakeContextError(ctx, timedOut, fmt.Errorf("remote view: receive metadata: %w", err))
	}
	metadata, err := validateInitialRemoteLinkMetadata(metadataFrame, target)
	if err != nil {
		return nil, err
	}
	if !view.applyRemoteMetadata(link, metadata, true) {
		return nil, errors.New("remote view: initial metadata became stale")
	}
	outputFrame, err := recvRemoteLinkHandshakeFrame(handshakeCtx, transport)
	if err != nil {
		return nil, handshakeContextError(ctx, timedOut, fmt.Errorf("remote view: receive initial output: %w", err))
	}
	if outputFrame.Type != ports.MsgOutput {
		return nil, fmt.Errorf("remote view: third server frame is %d, want Output", outputFrame.Type)
	}
	output, err := ports.UnmarshalOutput(outputFrame.Payload)
	if err != nil {
		return nil, fmt.Errorf("remote view: decode initial output: %w", err)
	}
	ack, reset, _, accepted := view.applyRemoteOutput(link, output)
	if !accepted || reset || output.New == 0 || !output.Full {
		return nil, errors.New("remote view: initial output is not an authoritative reset")
	}
	if ack != 0 {
		ackPayload, err := ports.MarshalAck(ports.Ack{Epoch: output.Epoch, State: ack})
		if err != nil {
			return nil, fmt.Errorf("remote view: encode initial output acknowledgement: %w", err)
		}
		if err := link.send(ports.Frame{Type: ports.MsgAck, Payload: ackPayload}); err != nil {
			return nil, handshakeContextError(ctx, timedOut, fmt.Errorf("remote view: acknowledge initial output: %w", err))
		}
	}
	if err := handshakeCtx.Err(); err != nil {
		return nil, handshakeContextError(ctx, timedOut, err)
	}
	published = true
	return view, nil
}

func remoteLinkOutputWindow(transport ports.Transport) uint8 {
	if _, datagram := transport.(ports.DatagramTransport); datagram {
		return 1
	}
	return maxUnackedOutputStates
}

func recvRemoteLinkHandshakeFrame(ctx context.Context, transport ports.Transport) (ports.Frame, error) {
	var frame ports.Frame
	err := boundedHandshakeOperation(ctx, transport, func() error {
		var recvErr error
		frame, recvErr = transport.Recv()
		return recvErr
	})
	if err != nil {
		return ports.Frame{}, err
	}
	return frame, nil
}

func validateRemoteLinkWelcome(frame ports.Frame, target domain.RemoteSessionTarget) error {
	if frame.Type == ports.MsgError {
		remoteErr, err := ports.UnmarshalErrorMsg(frame.Payload)
		if err != nil {
			return fmt.Errorf("remote view: malformed remote error: %w", err)
		}
		return fmt.Errorf("remote view: remote rejected handshake (%d): %s", remoteErr.Code, remoteErr.Text)
	}
	if frame.Type != ports.MsgWelcome {
		return fmt.Errorf("remote view: first server frame is %d, want Welcome", frame.Type)
	}
	welcome, err := ports.UnmarshalWelcome(frame.Payload)
	if err != nil {
		return fmt.Errorf("remote view: decode welcome: %w", err)
	}
	if welcome.SessionID == "" || welcome.SessionName != target.SessionName || welcome.RenderMode != ports.RenderModeProxiedContent {
		return errors.New("remote view: welcome identity or render mode mismatch")
	}
	return nil
}

func validateInitialRemoteLinkMetadata(frame ports.Frame, target domain.RemoteSessionTarget) (ports.SessionMeta, error) {
	if frame.Type != ports.MsgSessionMeta {
		return ports.SessionMeta{}, fmt.Errorf("remote view: second server frame is %d, want SessionMeta", frame.Type)
	}
	metadata, err := ports.UnmarshalSessionMeta(frame.Payload)
	if err != nil {
		return ports.SessionMeta{}, fmt.Errorf("remote view: decode initial metadata: %w", err)
	}
	if metadata.LifecycleID != target.LifecycleID || metadata.SessionName != target.SessionName || metadata.Revision != 1 {
		return ports.SessionMeta{}, errors.New("remote view: metadata identity mismatch")
	}
	if !target.Stopped && metadata.ActiveTabID != target.LiveTabID {
		return ports.SessionMeta{}, errors.New("remote view: metadata active tab mismatch")
	}
	return metadata, nil
}

func cloneRemoteSessionMeta(metadata ports.SessionMeta) ports.SessionMeta {
	metadata.Tabs = append([]ports.SessionTabMeta(nil), metadata.Tabs...)
	return metadata
}

// applyRemoteMetadata commits one validated update only for the exact current
// link. The view lock protects content state only; no renderer or callback runs
// before it is released.
func (v *remoteView) applyRemoteMetadata(link *remoteLink, metadata ports.SessionMeta, initial bool) bool {
	if v == nil || link == nil {
		return false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.link != link || v.linkGeneration != link.generation {
		return false
	}
	if initial {
		if v.metadata.Revision != 0 {
			return false
		}
	} else if metadata.Revision <= v.metadata.Revision {
		return false
	}
	v.metadata = cloneRemoteSessionMeta(metadata)
	return true
}

// applyRemoteOutput validates and applies state-bearing ANSI to the persistent
// private VT. It returns only immutable post-commit work; callers send ACK or
// reset frames and repaint attachments after view.mu is released.
func (v *remoteView) applyRemoteOutput(link *remoteLink, output ports.Output) (ack uint64, reset bool, attachments []*attachedClient, accepted bool) {
	if v == nil || link == nil {
		return 0, false, nil, false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.link != link || v.linkGeneration != link.generation {
		return 0, false, nil, false
	}
	next, valid := v.output.next(output)
	if !valid || (output.New != 0 && (v.screen == nil || v.screen.Frame.Width != output.Size.Cols || v.screen.Frame.Height != output.Size.Rows)) {
		if v.resetRequested {
			return 0, false, nil, false
		}
		v.resetRequested = true
		return 0, true, nil, false
	}
	if output.New == 0 {
		return 0, false, nil, true
	}
	v.output = next
	v.resetRequested = false
	if len(output.Data) != 0 {
		v.screen.Write(output.Data)
	}
	attachments = make([]*attachedClient, 0, len(v.attachments))
	for attachment := range v.attachments {
		attachments = append(attachments, attachment)
	}
	return output.New, false, attachments, true
}

func (d *Daemon) startRemoteLink(link *remoteLink) {
	if d == nil || link == nil || link.transport == nil {
		return
	}
	link.startOnce.Do(func() {
		link.workerStarted.Store(true)
		go d.runRemoteLink(link)
	})
}

// joinRemoteLink waits only for a receiver that was actually started. Candidate
// and test-only links may have no transport worker; treating their open done
// channel as joinable would deadlock terminal cleanup.
func joinRemoteLink(link *remoteLink) {
	if link != nil && link.workerStarted.Load() {
		<-link.done
	}
}

func (d *Daemon) runRemoteLink(link *remoteLink) {
	defer close(link.done)
	for {
		frame, err := link.transport.Recv()
		if err != nil {
			d.markRemoteLinkUnavailable(link)
			return
		}
		if err := d.handleRemoteLinkFrame(link, frame); err != nil {
			d.log.Warn("remote view link stopped", "endpoint", link.target.Endpoint, "session", link.target.SessionName, "err", err)
			d.markRemoteLinkUnavailable(link)
			return
		}
	}
}

func (d *Daemon) handleRemoteLinkFrame(link *remoteLink, frame ports.Frame) error {
	switch frame.Type {
	case ports.MsgOutput:
		output, err := ports.UnmarshalOutput(frame.Payload)
		if err != nil {
			return fmt.Errorf("remote view: decode output: %w", err)
		}
		ack, reset, attachments, accepted := link.view.applyRemoteOutput(link, output)
		if reset {
			if err := link.send(ports.Frame{Type: ports.MsgOutputResetRequest, Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{})}); err != nil {
				return fmt.Errorf("remote view: request output reset: %w", err)
			}
			return nil
		}
		if !accepted {
			return nil
		}
		if ack != 0 {
			ackPayload, err := ports.MarshalAck(ports.Ack{Epoch: output.Epoch, State: ack})
			if err != nil {
				return fmt.Errorf("remote view: encode output acknowledgement: %w", err)
			}
			if err := link.send(ports.Frame{Type: ports.MsgAck, Payload: ackPayload}); err != nil {
				return fmt.Errorf("remote view: acknowledge output: %w", err)
			}
		}
		for _, attachment := range attachments {
			token := attachmentOwnerToken(link.view, attachment, attachment.transport())
			if token.ac != nil {
				go d.paintRemoteView(link.view, attachment, false, token)
			}
		}
		return nil
	case ports.MsgSessionMeta:
		metadata, err := ports.UnmarshalSessionMeta(frame.Payload)
		if err != nil {
			return fmt.Errorf("remote view: decode metadata: %w", err)
		}
		if metadata.LifecycleID != link.target.LifecycleID || metadata.SessionName != link.target.SessionName || !link.view.applyRemoteMetadata(link, metadata, false) {
			return errors.New("remote view: invalid metadata update")
		}
		return nil
	case ports.MsgPing:
		if _, err := ports.UnmarshalPing(frame.Payload); err != nil {
			return fmt.Errorf("remote view: decode ping: %w", err)
		}
		return link.send(ports.Frame{Type: ports.MsgPong, Payload: ports.MarshalPong(ports.Pong{})})
	case ports.MsgPong:
		_, err := ports.UnmarshalPong(frame.Payload)
		return err
	case ports.MsgDetached:
		_, err := ports.UnmarshalDetached(frame.Payload)
		if err != nil {
			return fmt.Errorf("remote view: decode detached: %w", err)
		}
		return errors.New("remote view: remote session detached")
	case ports.MsgError:
		remoteErr, err := ports.UnmarshalErrorMsg(frame.Payload)
		if err != nil {
			return fmt.Errorf("remote view: malformed remote error: %w", err)
		}
		return fmt.Errorf("remote view: remote error (%d): %s", remoteErr.Code, remoteErr.Text)
	default:
		return fmt.Errorf("remote view: unexpected server frame type %d", frame.Type)
	}
}

// send serializes the complete outbound remote stream. It captures the exact
// current transport under view.mu, releases every architecture lock, then lets
// Close interrupt a blocked Send concurrently.
func (link *remoteLink) send(frame ports.Frame) error {
	if link == nil || link.view == nil {
		return errRemoteViewUnavailable
	}
	link.sendMu.Lock()
	view := link.view
	view.mu.Lock()
	current := !view.closed && view.link == link && view.linkGeneration == link.generation && link.active
	transport := link.transport
	view.mu.Unlock()
	if !current || transport == nil {
		link.sendMu.Unlock()
		return errRemoteViewStale
	}
	err := transport.Send(frame)
	link.sendMu.Unlock()
	if err != nil {
		link.cancel()
		_ = transport.Close()
	}
	return err
}

// handleRemoteViewInput forwards the raw local-terminal bytes and their
// original sequence through the exact current remote link. No local key,
// overlay, mouse, PTY, or persistence path observes those bytes.
func (d *Daemon) handleRemoteViewInput(view *remoteView, token attachmentConnectionToken, inputSeq uint64, data []byte) {
	if view == nil || len(data) == 0 || !token.attachmentEffectCurrent() {
		return
	}
	payload := ports.MarshalInput(ports.Input{InputSeq: inputSeq, Data: append([]byte(nil), data...)})
	if err := d.sendRemoteViewFrame(view, ports.Frame{Type: ports.MsgInput, Payload: payload}); err != nil {
		d.log.Warn("forwarding remote view input failed", "endpoint", view.key.endpoint, "session", view.key.sessionName, "err", err)
	}
}

// sendRemoteViewFrame snapshots the exact link while holding only view.mu;
// remoteLink.send performs the generation check and serialized transport I/O.
func (d *Daemon) sendRemoteViewFrame(view *remoteView, frame ports.Frame) error {
	if view == nil {
		return errRemoteViewUnavailable
	}
	view.mu.Lock()
	link := view.link
	view.mu.Unlock()
	if link == nil {
		return errRemoteViewUnavailable
	}
	return link.send(frame)
}

func (d *Daemon) markRemoteLinkUnavailable(link *remoteLink) {
	if link == nil || link.view == nil {
		return
	}
	view := link.view
	view.mu.Lock()
	current := !view.closed && view.link == link && view.linkGeneration == link.generation
	if current {
		link.active = false
	}
	view.mu.Unlock()
	if current {
		link.cancel()
		_ = link.transport.Close()
	}
}

// stopUnpublishedRemoteView is used for candidate failure and lost-election
// cleanup. It never touches the daemon registry and closes only the candidate
// transport generation after view.mu is released.
func (d *Daemon) stopUnpublishedRemoteView(view *remoteView) {
	if view == nil {
		return
	}
	view.mu.Lock()
	view.closed = true
	link := view.link
	view.link = nil
	view.linkGeneration++
	view.mu.Unlock()
	if link != nil {
		link.cancel()
		if link.transport != nil {
			_ = link.transport.Close()
		}
	}
}

// stopRemoteViewLink interrupts only the exact currently installed remote
// transport. It is called after registry publication has been revoked, so it
// cannot affect a successor view or link generation. The returned link has a
// started worker, which the terminal shutdown path joins after Close.
func (d *Daemon) stopRemoteViewLink(view *remoteView) *remoteLink {
	if view == nil {
		return nil
	}
	view.mu.Lock()
	link := view.link
	if link != nil {
		view.link = nil
		view.linkGeneration++
		link.active = false
	}
	view.mu.Unlock()
	if link != nil {
		link.cancel()
		if link.transport != nil {
			_ = link.transport.Close()
		}
	}
	return link
}
