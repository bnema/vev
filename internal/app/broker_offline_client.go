package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/adapters/uidriver"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/client"
)

// brokerClientCommand is the hidden P5.4b client entry point. It is deliberately
// separate from every ordinary attach, UI-driver, and web command: only an
// explicitly supplied offline root can select the broker sandbox.
const brokerClientCommand = "_broker-client"

const (
	offlineClientTerminal = "terminal"
	offlineClientUIDriver = "ui-driver"
)

type brokerClientOptions struct {
	offlineRoot string
	harness     string
}

func parseBrokerClientArgs(args []string) (command, error) {
	options := brokerClientOptions{harness: offlineClientTerminal}
	var rootSet, harnessSet bool
	for index := 0; index < len(args); {
		switch args[index] {
		case "--offline-root":
			if rootSet {
				return command{}, usagef("`%s` received duplicate --offline-root", brokerClientCommand)
			}
			if index+1 >= len(args) || args[index+1] == "" {
				return command{}, usagef("`--offline-root` requires a path")
			}
			options.offlineRoot, rootSet = args[index+1], true
			index += 2
		case "--harness":
			if harnessSet {
				return command{}, usagef("`%s` received duplicate --harness", brokerClientCommand)
			}
			if index+1 >= len(args) {
				return command{}, usagef("`--harness` requires terminal or ui-driver")
			}
			options.harness, harnessSet = args[index+1], true
			if options.harness != offlineClientTerminal && options.harness != offlineClientUIDriver {
				return command{}, usagef("`--harness` requires terminal or ui-driver")
			}
			index += 2
		default:
			if strings.HasPrefix(args[index], "-") {
				return command{}, usagef("unknown flag %q for `%s`", args[index], brokerClientCommand)
			}
			return command{}, usagef("`%s` does not accept positional arguments", brokerClientCommand)
		}
	}
	if !rootSet {
		return command{}, usagef("`%s` requires --offline-root", brokerClientCommand)
	}
	return command{kind: kindBrokerClient, brokerClient: options}, nil
}

func runBrokerClientCommand(ctx context.Context, options brokerClientOptions) error {
	layout, err := offlineLayout(options.offlineRoot)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	switch options.harness {
	case offlineClientTerminal:
		return runOfflineClient(ctx, brokeripc.SocketPath(layout.Runtime), term.New(), nil, log, nil, nil)
	case offlineClientUIDriver:
		terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: uiDriverDefaultColumns, Rows: uiDriverDefaultRows}}, "")
		if err != nil {
			return err
		}
		defer terminal.Close()
		ui := client.NewUI(terminal, clock.New())
		return runOfflineUIDriver(ctx, brokeripc.SocketPath(layout.Runtime), terminal, ui, log, &stdioStream{reader: os.Stdin, writer: os.Stdout})
	default:
		return usagef("unknown offline client harness %q", options.harness)
	}
}

// runOfflineClient is the common offline terminal composition used by the
// physical terminal, headless UI driver, and browser terminal harnesses. The
// connector is the real broker IPC adapter and InitialNavigation opens a real
// broker logical stream; no direct daemon dialer is present.
func runOfflineClient(ctx context.Context, socket string, terminal ports.Terminal, ui *client.UI, log *slog.Logger, onState func(client.State), onFailure func(error)) error {
	if terminal == nil {
		return errors.New("vev: offline client requires a terminal")
	}
	clk := clock.New()
	picker := client.NewPicker(clk, 0)
	navigation := client.InitialNavigationCreateEphemeral
	supervisor, err := client.NewSupervisor(client.SupervisorConfig{
		Connector:         brokeripc.NewConnector(socket, brokeripc.Config{}),
		Terminal:          terminal,
		Clock:             clk,
		UI:                ui,
		Picker:            picker,
		InitialNavigation: &navigation,
		AttachmentEnvironment: client.AttachmentEnvironment{
			TermEnv: os.Getenv("TERM"), Cwd: currentWorkingDirectory(), TrueColor: client.DetectTrueColor(os.Getenv("TERM"), os.Getenv("COLORTERM"), os.Environ()),
		},
		Render: offlineClientRender(terminal, picker, onState),
		Notify: func(_ client.State, err error) {
			if onFailure != nil {
				onFailure(err)
			}
		},
	})
	if err != nil {
		return err
	}
	if log != nil {
		log.Debug("broker_offline_client", "socket", socket)
	}
	return supervisor.Run(ctx)
}

// offlineClientRender is the production offline render callback. Every state
// reaches the optional observer; the terminal is written only while the shared
// client.PickerPresentation fence admits the presentation, which is exactly
// PresentPicker. That is what lets a resize invalidation repaint the picker at
// its current geometry while refusing the pre-attachment connecting transition,
// whose admitted foreground already owns the same terminal writer.
//
// ResizeEvents has exactly one consumer for the whole supervisor run: its
// attachment geometry collector. Picker rendering samples Geometry here at
// render time instead of competing for that single event channel, so a repaint
// always observes the latest size.
//
// The fence is exactly PickerPresentation. An attached foreground owns the
// terminal writer and a terminating process is leaving it, so neither is ever
// painted over from here.
func offlineClientRender(terminal ports.Terminal, picker *client.Picker, onState func(client.State)) func(client.State) {
	var writer sync.Mutex
	return func(state client.State) {
		if onState != nil {
			onState(state)
		}
		if !client.PickerPresentation(state) {
			return
		}
		geometry, err := terminal.Geometry()
		if err != nil {
			return
		}
		frame := picker.Render(geometry.Size)
		notice := picker.RenderNotice(geometry.Size)
		if len(frame) == 0 && len(notice) == 0 {
			return
		}
		writer.Lock()
		defer writer.Unlock()
		if len(frame) != 0 {
			_, _ = terminal.Out().Write(frame)
		}
		if len(notice) != 0 {
			_, _ = terminal.Out().Write(notice)
		}
		_ = terminal.Flush()
	}
}

func currentWorkingDirectory() string {
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return cwd
}

func runOfflineUIDriver(ctx context.Context, socket string, terminal *uiterm.Terminal, ui *client.UI, log *slog.Logger, stream io.ReadWriteCloser) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runnerDone := make(chan error, 1)
	states := make(chan client.State, 1)
	go func() {
		runnerDone <- runOfflineClient(runCtx, socket, terminal, ui, log, func(state client.State) {
			select {
			case states <- state:
			default:
			}
		}, nil)
	}()

	select {
	case <-states:
	case err := <-runnerDone:
		return err
	case <-runCtx.Done():
		return runCtx.Err()
	}
	// Ready advertises the currently actionable UI generation. Before the first
	// attachment there is no actionable generation; attached snapshots publish
	// the non-zero generation clients must fence actions against.
	ready := uidriver.Ready{Attachment: ui.Handle(), Control: true, Status: ports.UIStatusReconnecting}
	server := uidriver.New(ui, clock.New())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(runCtx, stream, ready) }()
	select {
	case err := <-serveDone:
		cancel()
		return errors.Join(err, ignoreContextCancellation(<-runnerDone))
	case err := <-runnerDone:
		cancel()
		return errors.Join(err, ignoreContextCancellation(<-serveDone))
	}
}
