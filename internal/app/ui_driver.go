package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/adapters/uidriver"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/logging"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
)

const (
	uiDriverDefaultColumns = 80
	uiDriverDefaultRows    = 24
)

// uiDriverOptions is the parsed `--ui-driver` invocation. It carries no launch
// configuration, no provisioned endpoint, and no daemon ownership: the driver
// uses the production broker of the process context and never creates,
// reconfigures, or destroys a daemon or a session-scoped root.
type uiDriverOptions struct {
	socket  string
	session string
	cols    int
	rows    int
	remote  string
	// picker starts with no initial navigation at all: the driver presents the
	// picker and opens nothing. It is exclusive of --session and --remote.
	picker bool
}

type interactiveUIOptions struct {
	observe bool
	control bool
	socket  string
}

func parseInteractiveUIFlags(args []string, observe, control bool, socket string) (interactiveUIOptions, error) {
	options := interactiveUIOptions{observe: observe, control: control, socket: socket}
	for len(args) > 0 {
		switch args[0] {
		case "--ui-observe":
			options.observe = true
			args = args[1:]
		case "--ui-control":
			options.control = true
			args = args[1:]
		case "--ui-socket":
			if len(args) < 2 || args[1] == "" {
				return interactiveUIOptions{}, usagef("`--ui-socket` requires a path")
			}
			options.socket = args[1]
			args = args[2:]
		default:
			return interactiveUIOptions{}, usagef("unknown attach option %q", args[0])
		}
	}
	if options.socket != "" && !options.observe && !options.control {
		return interactiveUIOptions{}, usagef("`--ui-socket` requires `--ui-observe` or `--ui-control`")
	}
	if options.socket != "" && (!filepath.IsAbs(options.socket) || strings.IndexByte(options.socket, 0) >= 0) {
		return interactiveUIOptions{}, usagef("`--ui-socket` requires an absolute path")
	}
	return options, nil
}

// parseUIDriverArgs parses the closed `--ui-driver` option set. `--socket` is
// the JSONL bridge to an already running client and is exclusive of every
// headless option; `--picker` is exclusive of `--session` and `--remote`. Any
// other option, including the removed `--launch-config`, is an unknown option
// and is refused before any I/O.
func parseUIDriverArgs(args []string) (uiDriverOptions, error) {
	options := uiDriverOptions{cols: uiDriverDefaultColumns, rows: uiDriverDefaultRows}
	var socketSet, sessionSet, colsSet, rowsSet, remoteSet, pickerSet bool
	for len(args) > 0 {
		name := args[0]
		if name == "--help" || name == "-h" {
			return uiDriverOptions{}, usagef("`--ui-driver` options: --session NAME --cols N --rows N --remote ENDPOINT --picker, or --socket PATH")
		}
		if !strings.HasPrefix(name, "--") {
			return uiDriverOptions{}, usagef("`--ui-driver` does not accept positional arguments")
		}
		args = args[1:]
		value := func(flag string) (string, error) {
			if len(args) == 0 || args[0] == "" || strings.HasPrefix(args[0], "--") {
				return "", usagef("`--ui-driver %s` requires a value", flag)
			}
			value := args[0]
			args = args[1:]
			return value, nil
		}
		switch name {
		case "--socket":
			if socketSet {
				return uiDriverOptions{}, usagef("duplicate `--socket`")
			}
			value, err := value("--socket")
			if err != nil {
				return uiDriverOptions{}, err
			}
			if !filepath.IsAbs(value) || strings.IndexByte(value, 0) >= 0 {
				return uiDriverOptions{}, usagef("`--socket` requires an absolute path")
			}
			options.socket, socketSet = value, true
		case "--session":
			if sessionSet {
				return uiDriverOptions{}, usagef("duplicate `--session`")
			}
			value, err := value("--session")
			if err != nil {
				return uiDriverOptions{}, err
			}
			if err := domain.ValidateSessionName(value); err != nil {
				return uiDriverOptions{}, err
			}
			options.session, sessionSet = value, true
		case "--cols", "--rows":
			value, err := value(name)
			if err != nil {
				return uiDriverOptions{}, err
			}
			number, err := strconv.Atoi(value)
			if err != nil || number <= 0 {
				return uiDriverOptions{}, usagef("`%s` requires a positive integer", name)
			}
			if name == "--cols" {
				if colsSet {
					return uiDriverOptions{}, usagef("duplicate `--cols`")
				}
				options.cols, colsSet = number, true
			} else {
				if rowsSet {
					return uiDriverOptions{}, usagef("duplicate `--rows`")
				}
				options.rows, rowsSet = number, true
			}
		case "--remote":
			if remoteSet {
				return uiDriverOptions{}, usagef("duplicate `--remote`")
			}
			value, err := value("--remote")
			if err != nil {
				return uiDriverOptions{}, err
			}
			if err := domain.ValidateRemoteHostTarget(value); err != nil {
				return uiDriverOptions{}, err
			}
			options.remote, remoteSet = value, true
		case "--picker":
			if pickerSet {
				return uiDriverOptions{}, usagef("duplicate `--picker`")
			}
			options.picker, pickerSet = true, true
		default:
			return uiDriverOptions{}, usagef("unknown `--ui-driver` option %q", name)
		}
	}
	if socketSet && (sessionSet || colsSet || rowsSet || remoteSet || pickerSet) {
		return uiDriverOptions{}, usagef("`--socket` cannot be combined with headless options")
	}
	if pickerSet && (sessionSet || remoteSet) {
		return uiDriverOptions{}, usagef("`--picker` cannot be combined with `--session` or `--remote`")
	}
	return options, nil
}

func runUIDriver(ctx context.Context, options uiDriverOptions) error {
	if options.socket != "" {
		return uidriver.Bridge(ctx, options.socket, os.Stdin, os.Stdout)
	}
	return runHeadlessUIDriver(ctx, options)
}

// uiDriverNavigation translates the parsed `--ui-driver` intent into the closed
// initial-navigation union. Picker is the zero value and opens nothing. Every
// other intent reuses the shared terminal CLI translation, so the driver cannot
// invent a destination, an epoch, or a session identity of its own: a local
// name creates, a bare local invocation creates ephemerally, and a remote
// target is only ever the registration and lifecycle a broker publication
// carries.
func uiDriverNavigation(options uiDriverOptions) (client.InitialNavigation, client.InitialNavigationResolver, error) {
	if options.picker {
		return client.InitialNavigation{}, nil, nil
	}
	intent := protocol.IntentEphemeral
	switch {
	case options.remote == "" && options.session != "":
		intent = protocol.IntentNew
	case options.remote != "" && options.session != "":
		intent = protocol.IntentNew
	}
	return terminalBrokerNavigation(intent, options.session, options.remote)
}

// runHeadlessUIDriver composes one headless UI-driver run: a virtual terminal,
// the UI service over it, the production broker connector of the process'
// context, and the one-shot initial navigation the CLI intent names. It never
// provisions, reconfigures, or destroys an endpoint, and it owns no daemon.
func runHeadlessUIDriver(ctx context.Context, options uiDriverOptions) error {
	navigation, resolver, err := uiDriverNavigation(options)
	if err != nil {
		return err
	}
	_, logCloser, err := configureLogging(logging.Client, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	clk := clock.New()
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: options.cols, Rows: options.rows}}, "")
	if err != nil {
		return fmt.Errorf("vev: create headless terminal: %w", err)
	}
	defer terminal.Close()
	ui := client.NewUI(terminal, clk)
	sessionEnv := terminalSessionEnvironment()
	attachmentEnv := terminalAttachmentEnvironment()
	attachmentEnv.Cwd = sessionEnv.Cwd
	return runUIDriverClient(ctx, brokerClientConfig{
		Connector:                newProductionBrokerConnector(),
		Terminal:                 terminal,
		Clock:                    clk,
		UI:                       ui,
		InitialNavigation:        navigation,
		ResolveInitialNavigation: resolver,
		AttachmentEnvironment:    attachmentEnv,
		SessionEnvironment:       sessionEnv,
	}, &stdioStream{reader: os.Stdin, writer: os.Stdout})
}

// runUIDriverClient is the single JSON Lines UI-driver composition shared by the
// production `--ui-driver` entry point and the sandbox `_broker-client
// --harness ui-driver` harness. It launches runBrokerClient exactly once, waits
// for the run's valid initial UI publication, serves the JSONL protocol over
// stream, and on EOF or cancellation cancels and joins both workers.
//
// It builds no picker, supervisor, dialer, attachment, or navigation of its
// own: the connector it is given is the only transport authority, so the sandbox
// injects its own broker IPC connector with no production fallback. A failure of
// the broker or of a destination is not fatal to the JSONL service: the run
// stays in the picker with a bounded notice and the driver keeps answering
// capture and wait. Closing the stream ends only this run: the broker, its
// daemon, and every session stay alive for the other clients.
func runUIDriverClient(ctx context.Context, cfg brokerClientConfig, stream io.ReadWriteCloser) error {
	if cfg.UI == nil || cfg.Terminal == nil || cfg.Clock == nil {
		return errors.New("vev: UI driver requires a UI, a terminal, and a clock")
	}
	if stream == nil {
		return errors.New("vev: UI driver requires a stream")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// The composition-owned identity and presentation of this run are published
	// before the broker is reached, so the UI service is usable and describable
	// even when no broker and no daemon can be reached. It publishes no
	// actionable generation: an unattached publication reports the honest "no
	// session owns input" status and leaves the generation to the committed
	// attachment itself.
	publishUIDriverInitialContext(cfg.UI)
	runnerDone := make(chan error, 1)
	go func() { runnerDone <- runBrokerClient(runCtx, cfg) }()

	snapshot, err := waitForUIDriverPublication(runCtx, cfg.UI)
	if err != nil {
		cancel()
		return errors.Join(err, ignoreContextCancellation(<-runnerDone))
	}
	ready := uidriverReady(cfg.UI, snapshot, true)
	server := uidriver.New(cfg.UI, cfg.Clock)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(runCtx, stream, ready) }()

	select {
	case serveErr := <-serveDone:
		cancel()
		return errors.Join(serveErr, ignoreContextCancellation(<-runnerDone))
	case runErr := <-runnerDone:
		cancel()
		return errors.Join(runErr, ignoreContextCancellation(<-serveDone))
	}
}

// waitForUIDriverPublication waits for the run's first valid UI publication. A
// valid publication is a committed owned snapshot that carries this run's UI
// handle and a context that conforms to the presentation table: Picker and
// Connecting present neither a session identity nor an actionable generation,
// while Attached presents a real nonzero generation with a committed session
// boundary. It deliberately requires neither a broker nor a daemon nor a
// committed view, so a driver whose broker is absent still becomes usable and
// describable. The startup budget is the existing handshake timeout: a driver
// that never publishes anything is a real local error, while an unavailable
// destination is not.
func waitForUIDriverPublication(ctx context.Context, ui *client.UI) (ports.UISnapshot, error) {
	readyCtx, cancel := context.WithTimeout(ctx, protocol.HandshakeTimeout)
	defer cancel()
	snapshot, err := ui.WaitForSnapshot(readyCtx, func(snapshot ports.UISnapshot) bool {
		return snapshot.Revision != 0 && snapshot.Context.AttachmentHandle == ui.Handle() && uiDriverContextConforms(snapshot.Context)
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return ports.UISnapshot{}, fmt.Errorf("vev: UI driver published no initial state: %w", err)
		}
		return ports.UISnapshot{}, err
	}
	return snapshot, nil
}

// uiDriverContextConforms reports whether a published context matches the
// presentation table. An unattached presentation owns no session identity, no
// output boundary, and no actionable generation; a committed attachment is the
// only presentation that may carry them, and it must carry a real nonzero
// generation. A publication that fails this rule is never adopted as the
// driver's ready state, so a caller can never read a fabricated identity from
// the discovery response.
func uiDriverContextConforms(context ports.UIContext) bool {
	if !context.Status.Valid() {
		return false
	}
	if context.Status == ports.UIStatusAttached {
		return context.Generation != 0
	}
	return context.Generation == 0 && context.Route == (protocol.CommittedRouteIdentity{}) && context.TabID == "" && context.FocusedPaneID == "" &&
		context.OutputEpoch == 0 && context.OutputState == 0 && context.ViewRevision == 0 && context.ViewPublication == 0
}

// publishUIDriverInitialContext publishes the composition-owned identity and
// presentation of one driver run before any broker interaction, so the UI
// service is usable and describable — capture, wait, and the discovery response
// — even when no broker and no daemon can be reached.
//
// It publishes the Picker presentation, the honest state of a run that owns no
// session yet. It publishes no actionable generation and no session metadata:
// the publication carries the run's stable handle and its status only, so a
// caller can never fence an action against an invented generation.
func publishUIDriverInitialContext(ui *client.UI) {
	if ui == nil {
		return
	}
	_ = ui.PublishPresentation(ports.UIStatusPicker)
}

// uidriverReady projects one published UI state into the driver discovery
// response. The attachment handle is the run's stable UI service handle, never a
// session identity; the generation is the real attachment generation and stays
// zero while no committed attachment has published one.
func uidriverReady(ui *client.UI, snapshot ports.UISnapshot, control bool) uidriver.Ready {
	ready := uidriver.Ready{Attachment: ui.Handle(), Control: control, Status: snapshot.Context.Status}
	if snapshot.Context.Status == ports.UIStatusAttached {
		ready.Generation = snapshot.Context.Generation
	}
	return ready
}

func ignoreContextCancellation(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

type observedTerminal struct {
	ports.Terminal
	ports.UIOutputTransaction
}

type stdioStream struct {
	reader io.Reader
	writer io.Writer
	once   sync.Once
}

func (s *stdioStream) Read(data []byte) (int, error)  { return s.reader.Read(data) }
func (s *stdioStream) Write(data []byte) (int, error) { return s.writer.Write(data) }
func (s *stdioStream) Close() error {
	var closeErr error
	s.once.Do(func() {
		for _, value := range []io.Writer{s.writer} {
			if closer, ok := value.(io.Closer); ok {
				closeErr = closer.Close()
			}
		}
		if closer, ok := s.reader.(io.Closer); ok {
			if err := closer.Close(); closeErr == nil {
				closeErr = err
			}
		}
	})
	return closeErr
}

// runAttachWithOptions is the composition path for ordinary interactive
// clients. Observation is opt-in; the default path remains term.New().
//
// The observed path composes the physical terminal, one VT mirror, the UI
// service over that mirror, and the same broker connector and initial-navigation
// translation the ordinary terminal attach uses, then delegates the whole run to
// the shared runBrokerClient. It owns no daemon dialer, no remote carriage, and
// no session: mirroring observes bytes at the serialized terminal writer and the
// broker owns every transport.
func runAttachWithOptions(ctx context.Context, intent uint8, name, remoteTarget string, options interactiveUIOptions) (retErr error) {
	if !options.observe && !options.control && options.socket == "" {
		return runAttach(ctx, intent, name, remoteTarget)
	}
	if options.control {
		options.observe = true
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	_, logCloser, err := configureLogging(logging.Client, false)
	if err != nil {
		return err
	}
	defer func() { _ = logCloser.Close() }()
	clk := clock.New()
	// The process trace is still created and joined here so an operator's trace
	// file keeps its exact lifecycle, including a close failure joined into the
	// run's result. Client-side transport spans are no longer emitted: the
	// broker owns carriage, so the observed composition keeps no dialer seam.
	_, observerCloser, err := newPerformanceTrace(clk)
	if err != nil {
		return fmt.Errorf("vev: performance trace: %w", err)
	}
	if observerCloser != nil {
		defer func() { retErr = errors.Join(retErr, observerCloser.Close()) }()
	}
	physical := term.NewWithFilesAndObservation(os.Stdin, os.Stdout, nil)
	geometry, err := physical.Geometry()
	if err != nil {
		return fmt.Errorf("vev: reading terminal geometry: %w", err)
	}
	mirror, err := uiterm.NewMirror(ctx, geometry, "")
	if err != nil {
		return fmt.Errorf("vev: create UI mirror: %w", err)
	}
	defer mirror.Close()
	physical = term.NewWithFilesAndObservation(os.Stdin, os.Stdout, mirror)
	ui := client.NewUI(mirror, clk)
	var endpoint *uidriver.UnixEndpoint
	if options.observe {
		server := uidriver.New(ui, clk)
		socketPath := options.socket
		if socketPath == "" {
			socketPath = uidriver.DefaultSocketPath(ipc.SocketDir(), ui.Handle())
		}
		endpoint, err = uidriver.ListenUnix(socketPath, server, func() uidriver.Ready {
			ready := uidriver.Ready{Attachment: ui.Handle(), Control: options.control, Status: ports.UIStatusPicker}
			if snapshot, snapshotErr := ui.Capture(ui.Handle()); snapshotErr == nil {
				ready = uidriverReady(ui, snapshot, options.control)
			}
			return ready
		})
		if err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, socketPath)
		defer func() { _ = endpoint.Close() }()
	}
	terminal := observedTerminal{Terminal: physical, UIOutputTransaction: mirror}
	intent, preconnected, err := resolveAttachCreationIntent(ctx, intent, name, remoteTarget, terminal)
	if err != nil {
		return err
	}
	navigation, resolver, err := terminalBrokerNavigation(intent, name, remoteTarget)
	if err != nil {
		if preconnected != nil {
			_ = preconnected.Close()
		}
		return err
	}
	sessionEnv := terminalSessionEnvironment()
	attachmentEnv := terminalAttachmentEnvironment()
	attachmentEnv.Cwd = sessionEnv.Cwd
	return runBrokerClient(ctx, brokerClientConfig{
		Connector:                withPreconnected(newProductionBrokerConnector(), preconnected),
		Terminal:                 terminal,
		Clock:                    clk,
		UI:                       ui,
		InitialNavigation:        navigation,
		ResolveInitialNavigation: resolver,
		AttachmentEnvironment:    attachmentEnv,
		SessionEnvironment:       sessionEnv,
	})
}
