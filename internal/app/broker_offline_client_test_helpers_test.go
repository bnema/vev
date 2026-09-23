package app

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/client"
)

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
