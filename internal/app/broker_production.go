package app

import (
	"path/filepath"

	"github.com/bnema/vev/internal/adapters/brokerconfig"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// productionBrokerLayout is the single production broker filesystem contract.
// Runtime ownership belongs to broker.Supervisor; state persists independently;
// configuration remains under the user's vev configuration directory.
func productionBrokerConfigPath() string {
	return filepath.Join(filepath.Dir(platform.ConfigPath()), "broker.json")
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
