package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/pkg/safedir"
)

// productionBrokerLayout is the single production broker filesystem contract.
// Runtime ownership belongs to broker.Supervisor; state persists independently;
// configuration remains under the user's vev configuration directory.
func productionBrokerConfigPath() string {
	return filepath.Join(filepath.Dir(platform.ConfigPath()), "broker.json")
}

// ensureProductionBrokerConfig creates the empty production bootstrap on first
// use. Existing files are never rewritten: malformed or insecure operator
// configuration must still fail closed in brokerconfig.LoadProduction.
func ensureProductionBrokerConfig() error {
	path := productionBrokerConfigPath()
	if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
		return fmt.Errorf("vev: secure broker configuration directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("vev: create broker configuration: %w", err)
	}
	_, writeErr := file.WriteString(`{"marker":"vev.broker.offline/v1","registrations":[]}`)
	closeErr := file.Close()
	if writeErr == nil && closeErr == nil {
		return nil
	}
	removeErr := os.Remove(path)
	if writeErr != nil {
		return errors.Join(fmt.Errorf("vev: write broker configuration: %w", writeErr), closeErr, removeErr)
	}
	return errors.Join(fmt.Errorf("vev: close broker configuration: %w", closeErr), removeErr)
}

func remoteBrokerPolicy(transport string) ports.BrokerPolicy {
	if transport == "stdio" {
		transport = "ssh-stdio"
	} else {
		transport = "ssh-quic"
	}
	return ports.BrokerPolicy{
		ProtocolVersion: protocol.Version, CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, Transport: transport,
		Trust: "openssh-config-v1", Launch: "explicit", Isolation: "per-user",
	}
}

func localDaemonPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion: protocol.Version, CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion,
		EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned, Transport: "unix-mux",
		Trust: "same-user", Launch: "explicit", Isolation: "per-user",
	}
}

func productionBrokerLayout() brokerconfig.Layout {
	root := filepath.Join(platform.StateDir(), "broker")
	runtime := filepath.Join(ipc.SocketDir(), "broker")
	return brokerconfig.Layout{
		Root:    root,
		Runtime: runtime,
		State:   filepath.Join(root, brokerconfig.StateDirName),
		Log:     filepath.Join(root, brokerconfig.LogDirName),
		Spawn:   filepath.Join(runtime, brokerconfig.SpawnDirName),
	}
}
