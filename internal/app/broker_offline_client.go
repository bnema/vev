package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/term"
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
	// picker starts the harness with no initial navigation at all, exactly like
	// `--ui-driver --picker`: the client presents the picker and opens nothing.
	picker bool
}

func parseBrokerClientArgs(args []string) (command, error) {
	options := brokerClientOptions{harness: offlineClientTerminal}
	var rootSet, harnessSet, pickerSet bool
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
		case "--picker":
			if pickerSet {
				return command{}, usagef("`%s` received duplicate --picker", brokerClientCommand)
			}
			options.picker, pickerSet = true, true
			index++
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
	if pickerSet && options.harness != offlineClientUIDriver {
		return command{}, usagef("`%s` --picker requires --harness ui-driver", brokerClientCommand)
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
		navigation := localEphemeralNavigation()
		if options.picker {
			navigation = client.InitialNavigation{}
		}
		return runOfflineUIDriverClient(ctx, offlineUIDriverHarness{
			socket:     brokeripc.SocketPath(layout.Runtime),
			terminal:   terminal,
			ui:         ui,
			log:        log,
			navigation: navigation,
			stream:     &stdioStream{reader: os.Stdin, writer: os.Stdout},
		})
	default:
		return usagef("unknown offline client harness %q", options.harness)
	}
}

// runOfflineClient is the sandbox terminal harness over the shared broker
// client composition. It supplies the real broker IPC connector for one
// offline root and the no-argument ephemeral local creation, then delegates the
// whole run to runBrokerClient: the sandbox adds no picker, supervisor, dialer,
// or stream of its own.
func runOfflineClient(ctx context.Context, socket string, terminal ports.Terminal, ui *client.UI, log *slog.Logger, onState func(client.State), onFailure func(error)) error {
	if terminal == nil {
		return errors.New("vev: offline client requires a terminal")
	}
	if log != nil {
		log.Debug("broker_offline_client", "socket", socket)
	}
	return runBrokerClient(ctx, brokerClientConfig{
		Connector: brokeripc.NewConnector(socket, brokeripc.Config{}),
		Terminal:  terminal,
		UI:        ui,
		// No-argument composition creates one ephemeral local session through
		// the closed initial-navigation union; there is no special attach path.
		InitialNavigation:     localEphemeralNavigation(),
		AttachmentEnvironment: terminalAttachmentEnvironment(),
		SessionEnvironment:    client.SessionEnvironment{Provenance: client.SessionEnvironmentLocalCLI},
		OnState:               onState,
		OnFailure:             onFailure,
	})
}

// offlineUIDriverHarness is the sandbox harness input: one explicitly supplied
// offline broker socket, the virtual terminal and UI service built over it, the
// one-shot navigation the harness was asked to perform, and the stream the JSONL
// protocol is served on. The CLI's `--picker` selects the zero picker intent; the
// default is the same no-argument local ephemeral creation every frontend uses.
type offlineUIDriverHarness struct {
	socket     string
	terminal   *uiterm.Terminal
	ui         *client.UI
	log        *slog.Logger
	navigation client.InitialNavigation
	resolver   client.InitialNavigationResolver
	stream     io.ReadWriteCloser
}

// runOfflineUIDriverClient composes the sandbox UI-driver harness over the same
// runUIDriverClient path production uses. Its connector is the real broker IPC
// connector of one explicitly supplied offline root: the sandbox adds no
// production fallback, starts no broker of its own, and never reaches the
// production XDG runtime or state. No session, endpoint, or process is
// provisioned here: the harness reaches sessions only through the broker of its
// own root, exactly like the production driver.
func runOfflineUIDriverClient(ctx context.Context, harness offlineUIDriverHarness) error {
	if harness.log != nil {
		harness.log.Debug("broker_offline_ui_driver", "socket", harness.socket)
	}
	return runUIDriverClient(ctx, brokerClientConfig{
		Connector:                brokeripc.NewConnector(harness.socket, brokeripc.Config{}),
		Terminal:                 harness.terminal,
		Clock:                    clock.New(),
		UI:                       harness.ui,
		InitialNavigation:        harness.navigation,
		ResolveInitialNavigation: harness.resolver,
		AttachmentEnvironment:    terminalAttachmentEnvironment(),
		SessionEnvironment:       client.SessionEnvironment{Provenance: client.SessionEnvironmentLocalCLI},
	}, harness.stream)
}
