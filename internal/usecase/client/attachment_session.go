package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Real attachment worker (Plan 001 P5.3b, offline and unactivated).
//
// sessionAttachmentWorker drives the existing typed session protocol on the
// broker logical stream the supervisor opened for one resolved request. It
// sends Hello, awaits Welcome, then awaits and commits the initial full output
// publication. Welcome alone is never attachment: the worker marks attached
// only after the validated frame was written, flushed, and committed through
// the foreground's UI output transaction. After attachment it keeps consuming
// output until the stream ends.
//
// The worker owns no endpoint, route, raw mode, terminal writer, or second
// reader. It reaches the terminal only through the supervisor-granted
// AttachmentForeground. Input, geometry and lifecycle sends therefore retain
// the foreground token and stop when the grant is superseded.

var (
	// errAttachmentDeadline is the stable local sentinel of an expired
	// attachment budget. The supervisor wraps it in a typed timeout.
	errAttachmentDeadline = errors.New("client: attachment deadline elapsed")
	// errAttachmentNoStream reports a foreground granted without a stream.
	errAttachmentNoStream = errors.New("client: attachment stream is missing")
	// errAttachmentPublication reports a publication frame that violates the
	// output contract before it was written.
	errAttachmentPublication = errors.New("client: invalid initial publication")
)

// AttachmentIdentityError reports a server commitment that disagrees with the
// broker request resolved from the selected picker row.
type AttachmentIdentityError struct {
	Stage string
	Want  protocol.ExactSessionTarget
	Got   protocol.ExactSessionTarget
	Name  string
}

func (e *AttachmentIdentityError) Error() string {
	if e.Name != "" {
		return fmt.Sprintf("client: %s committed session name %q, want %q", e.Stage, e.Got.SessionName, e.Name)
	}
	return fmt.Sprintf("client: %s committed target %+v, want %+v", e.Stage, e.Got, e.Want)
}

// sessionAttachmentConfig supplies the typed-session inputs of one worker run.
// The request is the exact request the supervisor opened the stream with; the
// rest is client-local presentation data the worker mirrors into Hello.
type sessionAttachmentConfig struct {
	Request            ports.BrokerOpenStreamRequest
	ClientID           [16]byte
	Geometry           domain.Geometry
	TermEnv            string
	Cwd                string
	TrueColor          bool
	SessionEnvironment SessionEnvironment
	// BeforeAttached runs after the initial frame commits but before attached
	// presentation becomes observable. A failure keeps the run unattached.
	BeforeAttached func() error
	// Clock bounds the move picker's lone-escape window and the terminal input
	// timers (DECRQM ambiguity, paste framing, palette deadlines). A nil clock
	// uses the system clock.
	Clock ports.Clock
	// Theme retains terminal-reported colors across attachments. When set,
	// the worker queries the terminal palette and sends protocol.Theme; when
	// nil, replies are still stripped from input but no query is written.
	Theme *terminalThemeState
	// Tab is the exact tab a picker tab row committed; its zero value
	// attaches at session level.
	Tab attachmentTab
}

// sessionAttachmentWorker implements AttachmentWorker for one broker logical
// stream. It holds no stream or terminal of its own: everything arrives through
// Run's foreground for the duration of one run.
type sessionAttachmentWorker struct {
	cfg sessionAttachmentConfig
}

// newSessionAttachmentWorker builds the real typed-session attachment worker.
func newSessionAttachmentWorker(cfg sessionAttachmentConfig) (*sessionAttachmentWorker, error) {
	if err := cfg.SessionEnvironment.Validate(); err != nil {
		return nil, fmt.Errorf("client: invalid session environment: %w", err)
	}
	if !slices.Equal(cfg.Request.Env, cfg.SessionEnvironment.Env) {
		return nil, errors.New("client: session environment does not match request environment")
	}
	cfg.Request.Env = append([]string(nil), cfg.Request.Env...)
	cfg.SessionEnvironment = cfg.SessionEnvironment.Clone()
	if supervisorNil(cfg.Clock) {
		cfg.Clock = systemClock{}
	}
	return &sessionAttachmentWorker{cfg: cfg}, nil
}

// serverReceive is one server message or the terminal receive error.
type serverReceive struct {
	message protocol.ServerMessage
	err     error
}

// startServerReceive owns the single reader of the logical stream. It is
// bounded: it buffers one message and blocks on the next read, and the stream's
// Close contract unblocks the read. The returned stop channel releases the
// reader when the run ends, so a message the worker no longer reads can never
// pin it.
func startServerReceive(stream ports.BrokerLogicalConnection) (<-chan serverReceive, chan struct{}) {
	out := make(chan serverReceive, 1)
	stop := make(chan struct{})
	go func() {
		for {
			message, err := stream.ReceiveServer()
			select {
			case out <- serverReceive{message: message, err: err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return out, stop
}

// Run drives Hello -> Welcome -> committed initial publication -> attached
// output until the stream ends or the context/deadline, the supervisor finalize
// signal, or the stream itself stops it. Every return path is a typed event;
// the supervisor stamps the granted token onto it.
func (w *sessionAttachmentWorker) Run(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
	token := fg.Token()
	if ctx == nil {
		ctx = context.Background()
	}
	stream := fg.Stream()
	if supervisorNil(stream) {
		return AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: errAttachmentNoStream}
	}
	incoming, stopReceive := startServerReceive(stream)
	defer close(stopReceive)

	if err := w.sendHello(ctx, fg, stream); err != nil {
		return w.settle(ctx, fg, stream, token, err)
	}

	welcome, err := w.awaitServer(ctx, fg, incoming)
	if err != nil {
		return w.settle(ctx, fg, stream, token, err)
	}
	switch message := welcome.(type) {
	case protocol.Welcome:
		// Welcome is a promise, not attachment: the daemon still owes the
		// committed initial publication.
		if err := w.validateWelcome(message); err != nil {
			return AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: err}
		}
	case protocol.ErrorMsg:
		return AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: &ProtocolError{Code: message.Code, Text: message.Text}}
	default:
		return AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: fmt.Errorf("vev: unexpected %T before welcome", welcome)}
	}

	state, event := w.awaitInitialPublication(ctx, fg, stream, token, incoming)
	if event != nil {
		return *event
	}

	return w.pumpAttached(ctx, fg, stream, token, state, incoming)
}

// awaitInitialPublication consumes server messages until the first valid full
// output frame is committed. It returns the output state to continue with, or
// a typed terminal event.
func (w *sessionAttachmentWorker) awaitInitialPublication(ctx context.Context, fg AttachmentForeground, stream ports.BrokerLogicalConnection, token AttachmentToken, incoming <-chan serverReceive) (outputApplyState, *AttachmentEvent) {
	state := outputApplyState{}
	for {
		message, err := w.awaitServer(ctx, fg, incoming)
		if err != nil {
			event := w.settle(ctx, fg, stream, token, err)
			return state, &event
		}
		switch typed := message.(type) {
		case protocol.Output:
			next, ok := state.next(typed)
			if !ok {
				event := AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: errAttachmentPublication}
				return state, &event
			}
			if err := w.validatePublication(typed); err != nil {
				event := AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: err}
				return state, &event
			}
			state = next
			attachmentNoteCommitted(fg, state.context)
			// The frame transaction is the pre-attach publication: it commits the first
			// session bytes with the Connecting presentation and, through the foreground
			// shape rule, a public generation of zero. Attached is published only after
			// that write, flush, and commit succeeded.
			if err := fg.Output(state.uiContext(ports.UIContext{Generation: attachmentActionableGeneration(fg, token)}, ports.UIStatusConnecting), typed.Data); err != nil {
				event := AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: fmt.Errorf("vev: publishing initial output: %w", err)}
				return state, &event
			}
			if err := w.send(ctx, fg, stream, protocol.Ack{Epoch: state.epoch, State: state.state}); err != nil {
				event := w.settle(ctx, fg, stream, token, err)
				return state, &event
			}
			// Attachment is the committed publication. Complete the deadline
			// boundary before presenting attached, so attached always implies the
			// handshake budget can no longer cancel the run.
			if w.cfg.BeforeAttached != nil {
				if err := w.cfg.BeforeAttached(); err != nil {
					event := AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: err}
					return state, &event
				}
			}
			// A refused marker means the grant was revoked underneath us, so the
			// run ends as a failure.
			if !fg.MarkAttached() {
				event := AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: errAttachmentForegroundRevoked}
				return state, &event
			}
			// The attached presentation is published last, once the committed frame and
			// the attached marker are in place. It carries the real action generation
			// with the same validated session identity and published boundary, and it
			// writes no bytes: the screen it commits is exactly the frame that just
			// drained.
			if err := fg.PublishAttached(state.uiContext(ports.UIContext{Generation: attachmentActionableGeneration(fg, token)}, ports.UIStatusAttached)); err != nil {
				event := AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: fmt.Errorf("vev: publishing attached presentation: %w", err)}
				return state, &event
			}
			return state, nil
		case protocol.ErrorMsg:
			event := AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: &ProtocolError{Code: typed.Code, Text: typed.Text}}
			return state, &event
		case protocol.Detached:
			// The daemon ended the attempt before publishing anything: it
			// never attached.
			event := AttachmentEvent{Token: token, Kind: AttachmentEventEnded}
			return state, &event
		default:
			// Everything else the daemon may publish before the initial frame
			// stays outside this slice's scope and is ignored.
		}
	}
}

type attachmentClientEvent struct {
	message protocol.ClientMessage
	input   *AttachmentInputEvent
	action  AttachmentLifecycleActionKind
}

// attachmentOverlay returns the optional overlay seam of fg, or nil.
func attachmentOverlay(fg AttachmentForeground) attachmentOverlayForeground {
	overlay, _ := fg.(attachmentOverlayForeground)
	return overlay
}

func attachmentNoteCommitted(fg AttachmentForeground, view protocol.ViewContext) {
	if overlay := attachmentOverlay(fg); overlay != nil {
		overlay.noteCommitted(view.Route.Target, view.TabID)
	}
}

var (
	errDetachToPicker = errors.New("client: detach to picker")
	errDetachAndExit  = errors.New("client: detach and exit")
)

// pumpAttached publishes output and concurrently forwards the foreground's
// authorized input, latest geometry and explicit lifecycle decision.
func (w *sessionAttachmentWorker) pumpAttached(ctx context.Context, fg AttachmentForeground, stream ports.BrokerLogicalConnection, token AttachmentToken, state outputApplyState, incoming <-chan serverReceive) AttachmentEvent {
	pumpCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	outgoing := make(chan attachmentClientEvent)
	go pumpAttachmentInput(pumpCtx, fg, outgoing)
	go pumpAttachmentGeometry(pumpCtx, fg, outgoing)
	go pumpAttachmentLifecycle(pumpCtx, fg, outgoing)
	outputResetRequested := false
	overlay := attachmentOverlay(fg)
	var repaint <-chan struct{}
	var tabSelections <-chan domain.TabStableID
	if overlay != nil {
		repaint = overlay.overlayRepaint()
		tabSelections = overlay.tabSelections()
	}
	picker := &attachmentMovePicker{worker: w, fg: fg, overlay: overlay, stream: stream, size: w.cfg.Geometry.Size, move: newMovePickerOverlay(w.cfg.TrueColor)}
	defer picker.stopEscape()
	input := newAttachmentInput(w, fg, stream, picker)
	defer input.close()
	if err := input.start(ctx); err != nil {
		return w.settle(ctx, fg, stream, token, err)
	}
	for {
		select {
		case <-input.wake:
			if err := input.flush(ctx); err != nil {
				return w.settle(ctx, fg, stream, token, err)
			}
		case <-input.markerC():
			if err := input.markerExpired(ctx, state); err != nil {
				return w.settle(ctx, fg, stream, token, err)
			}
		case <-input.drain.c():
			if err := input.drainFired(ctx); err != nil {
				return w.settle(ctx, fg, stream, token, err)
			}
		case <-input.completion.c():
			if err := input.completionFired(ctx); err != nil {
				return w.settle(ctx, fg, stream, token, err)
			}
		case <-repaint:
			// An overlay released the terminal: the picker box is still on screen
			// and suppressed frames were never written, so ask the daemon for an
			// authoritative full repaint.
			if err := w.send(ctx, fg, stream, protocol.OutputResetRequest{}); err != nil {
				return w.settle(ctx, fg, stream, token, err)
			}
			outputResetRequested = true
		case tab := <-tabSelections:
			// The picker overlay committed another tab of this session: switch
			// the attachment's view in place instead of reconnecting.
			if err := w.send(ctx, fg, stream, protocol.SelectTab{TabID: tab}); err != nil {
				return w.settle(ctx, fg, stream, token, err)
			}
		case <-picker.escape():
			picker.escapeFired()
			op, changed := picker.move.flush()
			if err := picker.applyOp(ctx, state, op, changed); err != nil {
				return w.settle(ctx, fg, stream, token, err)
			}
		case result := <-incoming:
			if result.err != nil {
				return w.settle(ctx, fg, stream, token, result.err)
			}
			switch typed := result.message.(type) {
			case protocol.Output:
				next, ok := state.next(typed)
				if !ok {
					continue
				}
				state = next
				outputResetRequested = false
				attachmentNoteCommitted(fg, state.context)
				if err := fg.Output(state.uiContext(ports.UIContext{Generation: attachmentActionableGeneration(fg, token)}, ports.UIStatusAttached), typed.Data); err != nil {
					return AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: fmt.Errorf("vev: publishing output: %w", err)}
				}
				if err := w.send(ctx, fg, stream, protocol.Ack{Epoch: state.epoch, State: state.state}); err != nil {
					return w.settle(ctx, fg, stream, token, err)
				}
			case protocol.UIViewUpdate:
				next, accepted, needsReset := state.nextView(typed)
				if needsReset && !outputResetRequested {
					if err := w.send(ctx, fg, stream, protocol.OutputResetRequest{}); err != nil {
						return w.settle(ctx, fg, stream, token, err)
					}
					outputResetRequested = true
				}
				if !accepted {
					continue
				}
				state = next
				attachmentNoteCommitted(fg, state.context)
				if err := fg.Output(state.uiContext(ports.UIContext{Generation: attachmentActionableGeneration(fg, token)}, ports.UIStatusAttached), nil); err != nil {
					return AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: fmt.Errorf("vev: publishing view update: %w", err)}
				}
			case protocol.UIReceipt:
				attachmentUIReceipt(fg, typed)
			case protocol.PickerOffer:
				if err := picker.offer(ctx, typed); err != nil {
					return w.settle(ctx, fg, stream, token, err)
				}
			case protocol.PickerSnapshot:
				if err := picker.snapshot(ctx, state, typed); err != nil {
					return w.settle(ctx, fg, stream, token, err)
				}
			case protocol.PickerClosed:
				picker.closed(typed.InteractionID)
			case protocol.ErrorMsg:
				return AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: &ProtocolError{Code: typed.Code, Text: typed.Text}}
			case protocol.Detached:
				if typed.Reason == protocol.ReasonDetachToPicker {
					return attachmentLifecycleEnd(token, AttachmentDetachToPicker)
				}
				return AttachmentEvent{Token: token, Kind: AttachmentEventEnded}
			}
		case event := <-outgoing:
			if event.input != nil && event.input.Err != nil {
				fg.PreserveInput(event.input.Data)
				return w.settle(ctx, fg, stream, token, event.input.Err)
			}
			if event.input != nil && event.input.actionID == 0 {
				// A physical read: replies are stripped and ordinary bytes
				// reach the overlay or the session inside read.
				if err := input.read(ctx, state, event.input.Data); err != nil {
					// Part of the read may already be delivered or held, so
					// it is committed rather than replayed.
					fg.AckInput()
					return w.settle(ctx, fg, stream, token, err)
				}
				fg.AckInput()
				continue
			}
			if event.input != nil {
				// A driver batch: bytes held for disambiguation go first.
				if err := input.flushHeld(ctx, state); err != nil {
					fg.PreserveInput(event.input.Data)
					return w.settle(ctx, fg, stream, token, err)
				}
				consumed, err := picker.consumeInput(ctx, state, *event.input)
				if err != nil {
					fg.PreserveInput(event.input.Data)
					return w.settle(ctx, fg, stream, token, err)
				}
				if consumed {
					// The overlay owned these bytes: nothing reaches the session.
					// A driver action still fences through the daemon so it
					// completes after the overlay applied it.
					fg.AckInput()
					if event.input.actionID != 0 {
						if err := w.send(ctx, fg, stream, protocol.UIFence{ActionID: event.input.actionID}); err != nil {
							return w.settle(ctx, fg, stream, token, err)
						}
					}
					continue
				}
				event.message = protocol.Input{InputSeq: input.nextSeq(), ActionID: event.input.actionID, Data: append([]byte(nil), event.input.Data...)}
			}
			if resize, ok := event.message.(protocol.Resize); ok {
				if err := picker.resize(ctx, state, resize.Size); err != nil {
					return w.settle(ctx, fg, stream, token, err)
				}
			}
			if err := w.send(ctx, fg, stream, event.message); err != nil {
				if event.input != nil {
					fg.PreserveInput(event.input.Data)
				}
				return w.settle(ctx, fg, stream, token, err)
			}
			if event.input != nil {
				fg.AckInput()
				if event.input.actionID != 0 {
					if err := w.send(ctx, fg, stream, protocol.UIFence{ActionID: event.input.actionID}); err != nil {
						return w.settle(ctx, fg, stream, token, err)
					}
				}
			}
			if event.action != 0 {
				return attachmentLifecycleEnd(token, event.action)
			}
		case <-ctx.Done():
			return w.settle(ctx, fg, stream, token, ctx.Err())
		case <-fg.Done():
			return w.settle(ctx, fg, stream, token, errAttachmentForegroundRevoked)
		}
	}
}

// attachmentLifecycleEnd gives local and daemon-triggered lifecycle actions
// the same settlement path: drain/revoke foreground before publishing picker.
func attachmentLifecycleEnd(token AttachmentToken, action AttachmentLifecycleActionKind) AttachmentEvent {
	err := errDetachToPicker
	if action == AttachmentDetachAndExit {
		err = errDetachAndExit
	}
	return AttachmentEvent{Token: token, Kind: AttachmentEventEnded, Err: err}
}

type attachmentUIForeground interface {
	actionableGeneration() uint64
	uiReceipt(protocol.UIReceipt)
}

func attachmentActionableGeneration(fg AttachmentForeground, token AttachmentToken) uint64 {
	if ui, ok := fg.(attachmentUIForeground); ok {
		return ui.actionableGeneration()
	}
	return token.Generation
}

func attachmentUIReceipt(fg AttachmentForeground, receipt protocol.UIReceipt) {
	if ui, ok := fg.(attachmentUIForeground); ok {
		ui.uiReceipt(receipt)
	}
}

// pumpAttachmentInput forwards each authorized delivery to the attached loop.
// The loop decides whether an overlay owns it or it becomes session Input, and
// sequences only the deliveries that actually reach the session.
func pumpAttachmentInput(ctx context.Context, fg AttachmentForeground, out chan<- attachmentClientEvent) {
	for {
		event, ok := fg.Input(ctx)
		if !ok {
			return
		}
		if event.Err != nil {
			select {
			case out <- attachmentClientEvent{input: &event}:
			case <-ctx.Done():
				fg.PreserveInput(event.Data)
			}
			return
		}
		copyEvent := event
		select {
		case out <- attachmentClientEvent{input: &copyEvent}:
		case <-ctx.Done():
			fg.PreserveInput(event.Data)
			return
		}
	}
}

func pumpAttachmentGeometry(ctx context.Context, fg AttachmentForeground, out chan<- attachmentClientEvent) {
	for {
		geometry, ok := fg.Resize(ctx)
		if !ok {
			return
		}
		geometry = geometry.NormalizePixels()
		select {
		case out <- attachmentClientEvent{message: protocol.Resize{Size: geometry.Size, PixelWidth: geometry.PixelWidth, PixelHeight: geometry.PixelHeight}}:
		case <-ctx.Done():
			return
		}
	}
}

func pumpAttachmentLifecycle(ctx context.Context, fg AttachmentForeground, out chan<- attachmentClientEvent) {
	action, ok := fg.Lifecycle(ctx)
	if !ok {
		return
	}
	select {
	case out <- attachmentClientEvent{message: protocol.Detach{}, action: action}:
	case <-ctx.Done():
		return
	}
}

func (w *sessionAttachmentWorker) validateWelcome(welcome protocol.Welcome) error {
	if welcome.CommittedIdentity != nil {
		return w.validateIdentity("welcome", welcome.CommittedIdentity.Target)
	}
	if w.cfg.Request.Admission == ports.BrokerAdmissionCreateNamed && welcome.SessionName != w.cfg.Request.Name {
		return &AttachmentIdentityError{Stage: "welcome", Got: protocol.ExactSessionTarget{SessionName: welcome.SessionName}, Name: w.cfg.Request.Name}
	}
	return nil
}

func (w *sessionAttachmentWorker) validatePublication(output protocol.Output) error {
	if output.Context == nil {
		return errAttachmentPublication
	}
	return w.validateIdentity("initial publication", output.Context.Route.Target)
}

func (w *sessionAttachmentWorker) validateIdentity(stage string, got protocol.ExactSessionTarget) error {
	request := w.cfg.Request
	switch request.Admission {
	case ports.BrokerAdmissionExact:
		if got != request.Target {
			return &AttachmentIdentityError{Stage: stage, Want: request.Target, Got: got}
		}
	case ports.BrokerAdmissionCreateNamed:
		if got.SessionName != request.Name {
			return &AttachmentIdentityError{Stage: stage, Got: got, Name: request.Name}
		}
	}
	return nil
}

// sendHello sends the typed Hello derived from the exact opened request.
func (w *sessionAttachmentWorker) sendHello(ctx context.Context, fg AttachmentForeground, stream ports.BrokerLogicalConnection) error {
	return w.send(ctx, fg, stream, w.hello(stream))
}

// hello builds the Hello the daemon expects for exactly one opened request. No
// endpoint, route, or resume token is invented: an exact request attaches, a
// named request creates, and an ephemeral request is ephemeral.
func (w *sessionAttachmentWorker) hello(stream ports.BrokerLogicalConnection) protocol.Hello {
	request := w.cfg.Request
	geometry := w.cfg.Geometry.NormalizePixels()
	var environmentPolicy protocol.EnvironmentPolicy
	var cwd string
	var env []string
	switch w.cfg.SessionEnvironment.Provenance {
	case SessionEnvironmentLocalCLI, SessionEnvironmentLocalPicker:
		environmentPolicy = protocol.EnvironmentPolicyClientOwned
		cwd = w.cfg.SessionEnvironment.Cwd
		env = append([]string(nil), request.Env...)
	case SessionEnvironmentRemote:
		environmentPolicy = protocol.EnvironmentPolicyDaemonOwned
	default:
		environmentPolicy = protocol.EnvironmentPolicyDaemonOwned
	}
	hello := protocol.Hello{
		Version:           protocol.Version,
		Intent:            protocol.IntentAttach,
		ClientID:          w.cfg.ClientID,
		Size:              geometry.Size,
		PixelWidth:        geometry.PixelWidth,
		PixelHeight:       geometry.PixelHeight,
		TermEnv:           w.cfg.TermEnv,
		Cwd:               cwd,
		TrueColor:         w.cfg.TrueColor,
		MaxOutputInFlight: requestedOutputWindow(stream),
		Env:               env,
		EnvironmentPolicy: environmentPolicy,
		Remote:            !request.Local,
	}
	switch request.Admission {
	case ports.BrokerAdmissionExact:
		target := request.Target
		hello.Intent = protocol.IntentAttach
		hello.ExactTarget = &target
		// protocol.ValidateHello requires Name to equal the exact target's
		// session name, and the daemon's exact-target routing compares it too, so
		// the name is carried rather than left empty. It is copied from the
		// request's own validated target, never invented.
		hello.Name = target.SessionName
		switch {
		case w.cfg.Tab.stopped != nil && !request.Local:
			// A stopped remote tab is restored through its exact selector.
			selector := *w.cfg.Tab.stopped
			hello.SessionTarget = &selector
		case w.cfg.Tab.preferred != "":
			hello.PreferredTabID = w.cfg.Tab.preferred
		}
	case ports.BrokerAdmissionCreateNamed:
		hello.Intent = protocol.IntentNew
		hello.Name = request.Name
	case ports.BrokerAdmissionCreateEphemeral:
		hello.Intent = protocol.IntentEphemeral
	}
	return hello
}

// send sends one client message while the run is still live. If cancellation
// wins a blocked send, closing the stream interrupts the carriage and the
// worker joins the send before returning, so no per-send goroutine is leaked.
func (w *sessionAttachmentWorker) send(ctx context.Context, fg AttachmentForeground, stream ports.BrokerLogicalConnection, message protocol.ClientMessage) error {
	sent := make(chan error, 1)
	go func() { sent <- stream.SendClient(message) }()
	select {
	case err := <-sent:
		return err
	case <-ctx.Done():
		_ = stream.Close()
		<-sent
		return ctx.Err()
	case <-fg.Done():
		_ = stream.Close()
		<-sent
		return errAttachmentForegroundRevoked
	}
}

// awaitServer waits for the next server message while the run is live.
func (w *sessionAttachmentWorker) awaitServer(ctx context.Context, fg AttachmentForeground, incoming <-chan serverReceive) (protocol.ServerMessage, error) {
	select {
	case result := <-incoming:
		if result.err != nil {
			return nil, result.err
		}
		return result.message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-fg.Done():
		return nil, errAttachmentForegroundRevoked
	}
}

// settle classifies a stopping cause into the correct typed event. A cancelled
// context is the supervisor's own deadline/teardown, a published stream cause
// is a loss, a bare EOF is an orderly end, and anything else is a protocol or
// application failure on the stream.
func (w *sessionAttachmentWorker) settle(ctx context.Context, fg AttachmentForeground, stream ports.BrokerLogicalConnection, token AttachmentToken, err error) AttachmentEvent {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: errors.Join(ctxErr, err)}
	}
	if cause := streamErr(stream); cause != nil {
		return AttachmentEvent{Token: token, Kind: AttachmentEventLost, Err: cause}
	}
	if errors.Is(err, io.EOF) {
		return AttachmentEvent{Token: token, Kind: AttachmentEventEnded, Err: err}
	}
	if err != nil {
		return AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: err}
	}
	return AttachmentEvent{Token: token, Kind: AttachmentEventEnded}
}

// streamErr reads the logical stream's stable terminal cause without treating a
// nil stream as a failure.
func streamErr(stream ports.BrokerLogicalConnection) error {
	if supervisorNil(stream) {
		return nil
	}
	return stream.Err()
}
