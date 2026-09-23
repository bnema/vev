package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/bnema/vev/internal/adapters/clipboard"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/terminalcap"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/usecase/client"
)

// Common broker client composition (Plan 001 P7.4a).
//
// runBrokerClient is the single terminal composition every frontend promotes:
// the sandbox terminal harness, the sandbox UI-driver harness, and the ordinary
// production terminal attach all build one brokerClientConfig and delegate
// here. It constructs exactly one client.Picker and exactly one
// client.Supervisor and hands the whole run to Supervisor.Run, so no frontend
// keeps a parallel picker, supervisor, attachment stream, or daemon dialer.
//
// The composition owns only what the supervisor cannot: the connector (or the
// production connect-or-spawn behind it), the terminal, the clock, the optional
// UI publication/action seam, the one-shot initial navigation intent (or the
// single-shot resolver that translates a CLI target into it), the copied
// attachment environment, the optional lifecycle-action channel, and the
// presentation callbacks. It never opens a stream, resolves a key, or drives
// picker input itself.

// brokerClientConfig is the common client configuration shared by every
// frontend. Connector, Terminal, and Clock are required (a nil Clock falls back
// to the process clock); the remaining fields are optional and keep the
// supervisor defaults when they are left zero.
type brokerClientConfig struct {
	Logger    *slog.Logger
	Connector ports.BrokerConnector
	Terminal  ports.Terminal
	Clock     ports.Clock
	Clipboard ports.ClipboardReader
	UI        *client.UI
	// InitialNavigation is the one-shot intent decided before the connection.
	// Its zero value is InitialNavigationPicker, which opens nothing.
	InitialNavigation client.InitialNavigation
	// ResolveInitialNavigation translates a composition-owned CLI target (an
	// attach name or a remote endpoint) into an exact navigation identity
	// against the first committed broker publication. It is mutually exclusive
	// with a non-zero InitialNavigation.
	ResolveInitialNavigation client.InitialNavigationResolver
	// AttachmentEnvironment is the copied process context of each attachment
	// Hello. It is never read from the broker process.
	AttachmentEnvironment client.AttachmentEnvironment
	// SessionEnvironment is the process snapshot for session creation. The
	// production frontends capture one LocalCLI snapshot before NewSupervisor;
	// offline and test compositions pass an explicit valid value (LocalCLI
	// with empty Env/Cwd unless the test asserts env). A zero value falls
	// back to the production LocalCLI snapshot inside runBrokerClient so the
	// ordinary terminal path keeps no second capture.
	SessionEnvironment client.SessionEnvironment
	// LifecycleActions carries explicit attachment lifecycle requests. A nil
	// channel disables external lifecycle actions.
	LifecycleActions <-chan client.AttachmentLifecycleAction
	// OnState observes every supervisor state. Optional.
	OnState func(client.State)
	// OnFailure surfaces one typed attachment or connectivity failure without
	// ending the run. Optional.
	OnFailure func(error)
	// OnLifecycle reports typed, bounded lifecycle transitions. Optional.
	OnLifecycle func(client.LifecycleNotice)
}

// brokerClientPresentation is the single terminal writer of one broker client
// run. Rendering the picker and reporting a bounded failure both go through it,
// so a failure notice can never interleave bytes with a picker frame.
type brokerClientPresentation struct {
	terminal ports.Terminal
	picker   *client.Picker
	// ui is the run's UI owner. When set, the picker frame is written inside the
	// terminal's UI output transaction under the Picker presentation, exactly like
	// daemon output, so the committed snapshot and the painted bytes describe the
	// same unattached state. Optional: a test may paint without an observation
	// channel.
	ui      *client.UI
	onState func(client.State)
	clock   ports.Clock
	spinner ports.Timer
	frame   int

	mu sync.Mutex
}

// Render observes the state and paints the picker frame and notice while the
// shared client.PickerPresentation fence admits the presentation. That is
// exactly PresentPicker: an admitted attachment foreground owns the same
// terminal writer before its attached marker and initial publication, and a
// terminating process is leaving the terminal, so neither is ever painted over.
// Picker is therefore never painted while the supervisor presents Connecting.
//
// The frame is committed through the terminal's UI output transaction under the
// Picker presentation, so the published context and the painted bytes are one
// coherent unattached state; Picker carries the run's handle with no session
// identity and no actionable generation. An unavailable observation channel
// leaves the frame written and flushed, exactly like the attachment foreground.
//
// ResizeEvents has exactly one consumer for the whole supervisor run (its
// attachment geometry collector); rendering samples Geometry here at render
// time instead of competing for that single event channel, so a repaint always
// observes the latest size.
func (p *brokerClientPresentation) Render(state client.State) {
	if p == nil {
		return
	}
	if p.onState != nil {
		p.onState(state)
	}
	if !client.PickerPresentation(state) && (state.Presentation != client.PresentConnecting || p.clock == nil) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	geometry, err := p.terminal.Geometry()
	if err != nil {
		return
	}
	var frame, notice []byte
	if client.PickerPresentation(state) {
		frame = p.picker.Render(geometry.Size)
		notice = p.picker.RenderNotice(geometry.Size)
	} else {
		frame = client.RenderTransitionNotice(geometry.Size, p.frame, "Connecting to session…")
	}
	if state.Connectivity == client.ConnectivityRetryWait {
		notice = client.RenderTransitionNotice(geometry.Size, p.frame, "Connection lost; retrying…")
	}
	if len(frame) == 0 && len(notice) == 0 {
		return
	}
	writer := p.terminal.Out()
	if writer == nil {
		return
	}
	transaction, _ := p.terminal.(ports.UIOutputTransaction)
	context := p.pickerContext()
	switch state.Presentation {
	case client.PresentConnecting:
		context.Status = ports.UIStatusConnecting
	case client.PresentAttachedPicker:
		// The picker is composed over a live attachment: the attachment stays
		// the actionable generation, so the frame keeps its attached context.
		if attached, ok := p.ui.OverlayContext(); ok {
			context = attached
		}
	}
	if transaction != nil {
		transaction.BeginOutput(context)
		if err := transaction.PublishContext(context); err != nil && !errors.Is(err, ports.ErrUIUnavailable) {
			transaction.EndOutput(false)
			return
		}
	}
	success := true
	if len(frame) != 0 {
		if _, writeErr := writer.Write(frame); writeErr != nil {
			success = false
		}
	}
	if success && len(notice) != 0 {
		if _, writeErr := writer.Write(notice); writeErr != nil {
			success = false
		}
	}
	if success && p.terminal.Flush() != nil {
		success = false
	}
	if transaction != nil {
		transaction.EndOutput(success)
	}
}

func (p *brokerClientPresentation) Spinner() <-chan time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.spinner == nil {
		p.spinner = p.clock.NewTimer(120 * time.Millisecond)
	}
	return p.spinner.C()
}

func (p *brokerClientPresentation) AdvanceSpinner(state client.State) {
	p.mu.Lock()
	p.frame++
	if p.spinner != nil {
		p.spinner.Reset(120 * time.Millisecond)
	}
	p.mu.Unlock()
	p.Render(state)
}

func (p *brokerClientPresentation) StopSpinner() {
	p.mu.Lock()
	if p.spinner != nil {
		p.spinner.Stop()
	}
	p.mu.Unlock()
}

// pickerContext is the Picker presentation of this run: the stable UI handle and
// the status alone. It is the shape every unattached publication uses, so a
// capture of the painted picker never shows an old attachment's session
// identity or generation.
func (p *brokerClientPresentation) pickerContext() ports.UIContext {
	context := ports.UIContext{Status: ports.UIStatusPicker}
	if p.ui != nil {
		context.AttachmentHandle = p.ui.Handle()
	}
	return context
}

// Failure reports one bounded attachment or connectivity failure on the same
// terminal writer. The supervisor already reports a typed, bounded lifecycle
// notice through the picker, so this is the complementary diagnostic: one
// bounded line under the shared writer lock, never a raw cause chain and never
// a competing terminal owner.
func (p *brokerClientPresentation) Failure(err error) {
	if p == nil || err == nil {
		return
	}
	text := err.Error()
	if len(text) > ports.BrokerMaxErrorBytes {
		text = text[:ports.BrokerMaxErrorBytes]
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	writer := p.terminal.Out()
	if writer == nil {
		return
	}
	if _, writeErr := writer.Write([]byte("\r\nvev: " + text + "\r\n")); writeErr != nil {
		return
	}
	_ = p.terminal.Flush()
}

// runBrokerClient builds exactly one picker and one supervisor over cfg and
// delegates the whole run to the supervisor. It returns the supervisor's
// terminal cause: a clean terminal EOF, an explicit exit, or the run's
// cancellation, joined with any failure the supervisor classified as terminal.
func runBrokerClient(ctx context.Context, cfg brokerClientConfig) error {
	if cfg.Terminal == nil {
		return errors.New("vev: broker client requires a terminal")
	}
	clk := cfg.Clock
	if clk == nil {
		clk = clock.New()
	}
	sessionEnv := cfg.SessionEnvironment
	if sessionEnv.Provenance == client.SessionEnvironmentUnspecified {
		// Production fallback: capture one LocalCLI process snapshot before
		// NewSupervisor so ordinary terminal attach keeps no second capture.
		sessionEnv = terminalSessionEnvironment()
	}
	attachmentEnv := cfg.AttachmentEnvironment
	if sessionEnv.Cwd != "" {
		attachmentEnv.Cwd = sessionEnv.Cwd
	}
	reader := cfg.Clipboard
	if reader == nil {
		reader = clipboard.New()
	}
	picker := client.NewPicker(clk, 0, attachmentEnv.TrueColor)
	presentation := &brokerClientPresentation{terminal: cfg.Terminal, picker: picker, ui: cfg.UI, onState: cfg.OnState, clock: clk}
	supervisor, err := client.NewSupervisor(client.SupervisorConfig{
		Logger:                   cfg.Logger,
		Connector:                cfg.Connector,
		Terminal:                 cfg.Terminal,
		Clock:                    clk,
		Clipboard:                reader,
		UI:                       cfg.UI,
		Picker:                   picker,
		InitialNavigation:        cfg.InitialNavigation,
		ResolveInitialNavigation: cfg.ResolveInitialNavigation,
		AttachmentEnvironment:    attachmentEnv,
		SessionEnvironment:       sessionEnv,
		LifecycleActions:         cfg.LifecycleActions,
		Render:                   presentation.Render,
		Notify: func(_ client.State, err error) {
			if cfg.OnFailure != nil {
				cfg.OnFailure(err)
				return
			}
			presentation.Failure(err)
		},
		NotifyLifecycle: func(notice client.LifecycleNotice) {
			if cfg.OnLifecycle != nil {
				cfg.OnLifecycle(notice)
			}
			if notice.Kind == client.LifecycleNoticeBrokerLost {
				picker.OfferNotice("broker-lost", "Connection lost; retrying…")
			}
			if notice.Kind == client.LifecycleNoticeBrokerReconnected {
				picker.OfferNotice("broker-reconnected", "Connection restored")
			}
		},
		Spinner: presentation,
	})
	if err != nil {
		return err
	}
	return supervisor.Run(ctx)
}

// localEphemeralNavigation is the no-argument product intent: create one
// ephemeral session on the broker-owned local daemon. It is decided before the
// connection, so it carries no epoch and needs no resolver.
func localEphemeralNavigation() client.InitialNavigation {
	return client.InitialNavigation{
		Kind:        client.InitialNavigationCreateEphemeral,
		Destination: ports.BrokerEndpointFence{Local: true},
	}
}

// errTerminalNavigationUnresolved reports that a CLI target could not be
// translated into the exact identity a snapshot carries. It is a local refusal:
// no destination was dialed, so the supervisor surfaces it as a bounded
// selection-unavailable notice and returns to the picker instead of attaching a
// fallback or creating implicitly.
var errTerminalNavigationUnresolved = errors.New("vev: attach target is not in the broker catalogue")

// terminalBrokerNavigation translates the parsed terminal CLI intent into the
// closed initial-navigation union. An intent that is fully determined before
// the connection (local creation) is a value; an intent whose exact identity
// only exists in a broker publication (`attach`, and any remote target) is a
// single-shot resolver that runs once against the first committed snapshot.
//
// The translation never invents identity: a remote destination is only ever the
// complete registration a snapshot carries, and an exact target is only ever
// the lifecycle/name pair the snapshot publishes. A target the snapshot does not
// carry is a refusal, never an implicit create and never a same-name fallback.
func terminalBrokerNavigation(intent uint8, name, remoteTarget string) (client.InitialNavigation, client.InitialNavigationResolver, error) {
	if remoteTarget == "" {
		switch intent {
		case protocol.IntentEphemeral:
			return localEphemeralNavigation(), nil, nil
		case protocol.IntentNew:
			if err := domain.ValidateSessionName(name); err != nil {
				return client.InitialNavigation{}, nil, err
			}
			return client.InitialNavigation{
				Kind:        client.InitialNavigationCreateNamed,
				Destination: ports.BrokerEndpointFence{Local: true},
				Name:        name,
			}, nil, nil
		case protocol.IntentAttach:
			if err := domain.ValidateSessionName(name); err != nil {
				return client.InitialNavigation{}, nil, err
			}
			return client.InitialNavigation{}, localExactAttachResolver(name), nil
		default:
			return client.InitialNavigation{}, nil, usagef("unsupported attach intent %d", intent)
		}
	}
	if err := domain.ValidateRemoteHostTarget(remoteTarget); err != nil {
		return client.InitialNavigation{}, nil, err
	}
	switch intent {
	case protocol.IntentEphemeral:
		// `attach user@host` without a session keeps its product behavior: a
		// fresh ephemeral session on that remote daemon.
		return client.InitialNavigation{}, remoteEphemeralCreateResolver(remoteTarget), nil
	case protocol.IntentNew:
		if err := domain.ValidateSessionName(name); err != nil {
			return client.InitialNavigation{}, nil, err
		}
		return client.InitialNavigation{}, remoteNamedCreateResolver(remoteTarget, name), nil
	case protocol.IntentAttach:
		if err := domain.ValidateSessionName(name); err != nil {
			return client.InitialNavigation{}, nil, err
		}
		return client.InitialNavigation{}, remoteExactAttachResolver(remoteTarget, name), nil
	default:
		return client.InitialNavigation{}, nil, usagef("unsupported attach intent %d", intent)
	}
}

// localExactAttachResolver resolves one local session name to its exact
// lifecycle in the committed publication. `attach <name>` is a target
// translation only: a name the local observation does not carry is refused, so
// the client returns to the picker with a notice instead of creating a session
// implicitly.
func localExactAttachResolver(name string) client.InitialNavigationResolver {
	return func(snapshot ports.BrokerSnapshot) (client.InitialNavigation, error) {
		observation, ok := localBrokerObservation(snapshot)
		if !ok {
			return client.InitialNavigation{}, fmt.Errorf("%w: no local daemon observation", errTerminalNavigationUnresolved)
		}
		target, ok := brokerObservationExactTarget(observation, name)
		if !ok {
			return client.InitialNavigation{}, fmt.Errorf("%w: %q", errTerminalNavigationUnresolved, name)
		}
		return client.InitialNavigation{
			Kind:        client.InitialNavigationAttachExact,
			Epoch:       snapshot.Epoch,
			Destination: ports.BrokerEndpointFence{Local: true},
			Target:      target,
		}, nil
	}
}

// remoteExactAttachResolver resolves one remote session name to its exact
// lifecycle under the endpoint's committed registration. The destination is the
// complete observed registration (endpoint, incarnation, and generation), never
// a bare hostname and never an invented daemon identity; an endpoint the broker
// does not carry is refused rather than dialed.
func remoteExactAttachResolver(endpoint, name string) client.InitialNavigationResolver {
	return func(snapshot ports.BrokerSnapshot) (client.InitialNavigation, error) {
		observation, ok := snapshot.Find(endpoint)
		if !ok {
			return client.InitialNavigation{}, fmt.Errorf("%w: %q is not a configured broker host", errTerminalNavigationUnresolved, endpoint)
		}
		target, ok := brokerObservationExactTarget(observation, name)
		if !ok {
			return client.InitialNavigation{}, fmt.Errorf("%w: %q on %q", errTerminalNavigationUnresolved, name, endpoint)
		}
		return client.InitialNavigation{
			Kind:        client.InitialNavigationAttachExact,
			Epoch:       snapshot.Epoch,
			Destination: ports.BrokerEndpointFence{Registration: observation.Registration},
			Target:      target,
		}, nil
	}
}

func remoteNamedCreateResolver(endpoint, name string) client.InitialNavigationResolver {
	return func(snapshot ports.BrokerSnapshot) (client.InitialNavigation, error) {
		observation, ok := snapshot.Find(endpoint)
		if !ok {
			return client.InitialNavigation{}, fmt.Errorf("%w: %q is not a configured broker host", errTerminalNavigationUnresolved, endpoint)
		}
		return client.InitialNavigation{
			Kind:        client.InitialNavigationCreateNamed,
			Epoch:       snapshot.Epoch,
			Destination: ports.BrokerEndpointFence{Registration: observation.Registration},
			Name:        name,
		}, nil
	}
}

// remoteAttachOrCreateResolver resolves one remote session name to an exact
// attach when the endpoint's committed observation carries it, and to a named
// creation fenced to the same registration once an inventory observed at or
// after requestedAt proves it absent. Before such an observation it asks the
// supervisor to request one and resolve again, with the creation as the
// fallback should the host stay unobserved. An endpoint the broker does not
// carry is refused rather than dialed.
func remoteAttachOrCreateResolver(endpoint, name string, requestedAt time.Time) client.InitialNavigationResolver {
	attach := remoteExactAttachResolver(endpoint, name)
	create := remoteNamedCreateResolver(endpoint, name)
	return func(snapshot ports.BrokerSnapshot) (client.InitialNavigation, error) {
		observation, ok := snapshot.Find(endpoint)
		if !ok {
			return client.InitialNavigation{}, fmt.Errorf("%w: %q is not a configured broker host", errTerminalNavigationUnresolved, endpoint)
		}
		if _, exists := brokerObservationExactTarget(observation, name); exists {
			return attach(snapshot)
		}
		if !observation.InventoryKnown || observation.LastSuccess.Before(requestedAt) {
			// Should the host stay unobserved, creating is still the only
			// resolvable intent; the remote daemon refuses a live name.
			fallback, err := create(snapshot)
			if err != nil {
				fallback = client.InitialNavigation{}
			}
			return client.InitialNavigation{}, client.InitialNavigationNotObserved{Endpoint: endpoint, Fallback: fallback}
		}
		return create(snapshot)
	}
}

// remoteEphemeralCreateResolver resolves `attach user@host` (no session) into a
// remote ephemeral creation fenced to the endpoint's committed registration and
// the epoch of the snapshot that resolved it.
func remoteEphemeralCreateResolver(endpoint string) client.InitialNavigationResolver {
	return func(snapshot ports.BrokerSnapshot) (client.InitialNavigation, error) {
		observation, ok := snapshot.Find(endpoint)
		if !ok {
			return client.InitialNavigation{}, fmt.Errorf("%w: %q is not a configured broker host", errTerminalNavigationUnresolved, endpoint)
		}
		return client.InitialNavigation{
			Kind:        client.InitialNavigationCreateEphemeral,
			Epoch:       snapshot.Epoch,
			Destination: ports.BrokerEndpointFence{Registration: observation.Registration},
		}, nil
	}
}

// localBrokerObservation returns the single local daemon observation of one
// publication.
func localBrokerObservation(snapshot ports.BrokerSnapshot) (ports.BrokerDaemonObservation, bool) {
	for _, observation := range snapshot.Daemons {
		if observation.Local {
			return observation, true
		}
	}
	return ports.BrokerDaemonObservation{}, false
}

// brokerObservationExactTarget resolves one session name to the exact
// lifecycle/name pair the observation publishes. A broken session is refused:
// its identity is known but not attachable, so an explicit attempt would only be
// a guaranteed refusal.
func brokerObservationExactTarget(observation ports.BrokerDaemonObservation, name string) (protocol.ExactSessionTarget, bool) {
	for _, session := range observation.Sessions {
		if session.Name != name || session.State == catalogue.RemoteCatalogSessionBroken {
			continue
		}
		target := protocol.ExactSessionTarget{LifecycleID: session.LifecycleID, SessionName: session.Name}
		if err := target.Validate(); err != nil {
			return protocol.ExactSessionTarget{}, false
		}
		return target, true
	}
	return protocol.ExactSessionTarget{}, false
}

// productionBrokerConnector is every frontend's production broker connector. It
// performs no I/O at construction: Connect connect-or-spawns the per-user
// broker on demand, so a broker that is absent or incompatible is an ordinary
// attempt failure under the supervisor's own backoff instead of a fatal startup
// error. It never dials a daemon and never selects a transport.
//
// It is stateless beyond its connect function: the supervisor owns and closes
// every service it adopts, so the connector keeps no eager connection and no
// close path of its own.
type productionBrokerConnector struct {
	connect func(context.Context) (ports.BrokerService, error)
}

func localBrokerOperationRoute(snapshot ports.BrokerSnapshot) client.BrokerOperationRoute {
	return client.BrokerOperationRoute{
		Epoch:       snapshot.Epoch,
		Destination: ports.BrokerEndpointFence{Local: true},
	}
}

var _ ports.BrokerConnector = (*productionBrokerConnector)(nil)

// newProductionBrokerConnector returns a lazy production connector. The
// connect function is a composition seam for tests; production uses the shared
// connect-or-spawn.
func newProductionBrokerConnector() ports.BrokerConnector {
	return &productionBrokerConnector{connect: func(ctx context.Context) (ports.BrokerService, error) {
		return connectProductionClientBroker(ctx)
	}}
}

// Connect connect-or-spawns one broker connection for the calling attempt,
// including the first one, so every attempt is under the supervisor's retry
// cadence.
func (c *productionBrokerConnector) Connect(ctx context.Context) (ports.BrokerService, error) {
	if c == nil || c.connect == nil {
		return nil, errors.New("vev: broker connector is not configured")
	}
	return c.connect(ctx)
}

// terminalBrokerSeams are the optional observation callbacks of one terminal
// run. Production supplies none: runBrokerClient reports a failure on the
// terminal itself through the same writer the picker paints with. The terminal
// composition tests substitute every callback to observe the same run's states,
// lifecycle notices, and failures.
type terminalBrokerSeams struct {
	OnState     func(client.State)
	OnLifecycle func(client.LifecycleNotice)
	OnFailure   func(error)
}

// terminalBrokerCallbacks is the composition-root seam producing one terminal
// run's callbacks.
var terminalBrokerCallbacks = func() terminalBrokerSeams { return terminalBrokerSeams{} }

// Composition-root seams for the terminal client. Production supplies the
// production connect-or-spawn, the physical terminal, and the detached local
// creation; tests substitute them so the terminal composition is exercised
// without a production broker process, a real controlling terminal, or a real
// daemon.
var (
	connectProductionClientBroker = connectProductionBroker
	terminalForAttach             = func() ports.Terminal { return term.New() }
	createDetachedTerminalSession = createDetachedLocalSession
)

// terminalAttachmentEnvironment copies the composition-owned process context an
// attachment Hello carries. The supervisor and the broker never inspect the
// process environment themselves.
func terminalAttachmentEnvironment() client.AttachmentEnvironment {
	return client.AttachmentEnvironment{
		TermEnv:   os.Getenv("TERM"),
		Cwd:       currentWorkingDirectory(),
		TrueColor: terminalcap.DetectTrueColor(os.Getenv("TERM"), os.Getenv("COLORTERM"), os.Environ()),
	}
}

// terminalSessionEnvironment captures one LocalCLI process snapshot for
// session creation before NewSupervisor. The supervisor remaps each remote
// request to Remote, so the base provenance stays LocalCLI without branching
// in the app.
func terminalSessionEnvironment() client.SessionEnvironment {
	return client.SessionEnvironment{
		Provenance: client.SessionEnvironmentLocalCLI,
		Env:        append([]string(nil), os.Environ()...),
		Cwd:        currentWorkingDirectory(),
	}
}

func currentWorkingDirectory() string {
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return cwd
}
