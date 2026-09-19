package client

import (
	"context"
	"errors"
	"fmt"
	"io"

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
	Request   ports.BrokerOpenStreamRequest
	ClientID  [16]byte
	Geometry  domain.Geometry
	TermEnv   string
	Cwd       string
	TrueColor bool
	// BeforeAttached runs after the initial frame commits but before attached
	// presentation becomes observable. A failure keeps the run unattached.
	BeforeAttached func() error
}

// sessionAttachmentWorker implements AttachmentWorker for one broker logical
// stream. It holds no stream or terminal of its own: everything arrives through
// Run's foreground for the duration of one run.
type sessionAttachmentWorker struct {
	cfg sessionAttachmentConfig
}

// newSessionAttachmentWorker builds the real typed-session attachment worker.
func newSessionAttachmentWorker(cfg sessionAttachmentConfig) *sessionAttachmentWorker {
	return &sessionAttachmentWorker{cfg: cfg}
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
			if err := fg.Output(state.uiContext(ports.UIContext{Generation: token.Generation}, ports.UIStatusTransitioning), typed.Data); err != nil {
				event := AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: fmt.Errorf("vev: publishing initial output: %w", err)}
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
	for {
		select {
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
				if err := fg.Output(state.uiContext(ports.UIContext{Generation: token.Generation}, ports.UIStatusAttached), typed.Data); err != nil {
					return AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: fmt.Errorf("vev: publishing output: %w", err)}
				}
			case protocol.ErrorMsg:
				return AttachmentEvent{Token: token, Kind: AttachmentEventFailed, Err: &ProtocolError{Code: typed.Code, Text: typed.Text}}
			case protocol.Detached:
				return AttachmentEvent{Token: token, Kind: AttachmentEventEnded}
			}
		case event := <-outgoing:
			if event.input != nil && event.input.Err != nil {
				fg.PreserveInput(event.input.Data)
				return w.settle(ctx, fg, stream, token, event.input.Err)
			}
			if err := w.send(ctx, fg, stream, event.message); err != nil {
				if event.input != nil {
					fg.PreserveInput(event.input.Data)
				}
				return w.settle(ctx, fg, stream, token, err)
			}
			if event.input != nil {
				fg.AckInput()
			}
			if event.action == AttachmentDetachToPicker {
				return AttachmentEvent{Token: token, Kind: AttachmentEventEnded, Err: errDetachToPicker}
			}
			if event.action == AttachmentDetachAndExit {
				return AttachmentEvent{Token: token, Kind: AttachmentEventEnded, Err: errDetachAndExit}
			}
		case <-ctx.Done():
			return w.settle(ctx, fg, stream, token, ctx.Err())
		case <-fg.Done():
			return w.settle(ctx, fg, stream, token, errAttachmentForegroundRevoked)
		}
	}
}

func pumpAttachmentInput(ctx context.Context, fg AttachmentForeground, out chan<- attachmentClientEvent) {
	var sequence uint64
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
		sequence++
		copyEvent := event
		select {
		case out <- attachmentClientEvent{message: protocol.Input{InputSeq: sequence, Data: append([]byte(nil), event.Data...)}, input: &copyEvent}:
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
	hello := protocol.Hello{
		Version:           protocol.Version,
		Intent:            protocol.IntentAttach,
		ClientID:          w.cfg.ClientID,
		Size:              geometry.Size,
		PixelWidth:        geometry.PixelWidth,
		PixelHeight:       geometry.PixelHeight,
		TermEnv:           w.cfg.TermEnv,
		Cwd:               w.cfg.Cwd,
		TrueColor:         w.cfg.TrueColor,
		MaxOutputInFlight: requestedOutputWindow(stream),
		Env:               append([]string(nil), request.Env...),
		EnvironmentPolicy: request.Policy.EnvironmentPolicy,
		Remote:            !request.Local,
	}
	switch request.Admission {
	case ports.BrokerAdmissionExact:
		target := request.Target
		hello.Intent = protocol.IntentAttach
		hello.ExactTarget = &target
	case ports.BrokerAdmissionCreateNamed:
		hello.Intent = protocol.IntentNew
		hello.Name = request.Name
	case ports.BrokerAdmissionCreateEphemeral:
		hello.Intent = protocol.IntentEphemeral
	}
	return hello
}

// send sends one client message while the run is still live. The send runs on
// its own goroutine so a cancellation or finalize wakes the worker without
// waiting for the stream's Close, which is the supervisor's close authority.
func (w *sessionAttachmentWorker) send(ctx context.Context, fg AttachmentForeground, stream ports.BrokerLogicalConnection, message protocol.ClientMessage) error {
	sent := make(chan error, 1)
	go func() { sent <- stream.SendClient(message) }()
	select {
	case err := <-sent:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-fg.Done():
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
