package app

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/daemonidentity"
	"github.com/bnema/vev/internal/adapters/daemonmux"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// startProductionBrokerFixture uses the same private XDG layout and real daemon
// entry point as a deployed broker. In particular no test provisions a local
// identity or socket in broker.json: the daemon publishes its own authority.
func startProductionBrokerFixture(t *testing.T) (ports.BrokerService, context.Context) {
	t.Helper()
	t.Setenv("VEV_ENV", "")
	t.Setenv("VEV_ENV_ROOT", "")
	t.Setenv("XDG_RUNTIME_DIR", shortTempDir(t, "vb"))
	t.Setenv("XDG_STATE_HOME", shortTempDir(t, "vb"))
	t.Setenv("XDG_CONFIG_HOME", shortTempDir(t, "vb"))
	t.Setenv(brokerHelperEnv, "1")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	daemon := exec.Command(os.Args[0], "--daemon")
	daemon.Env = os.Environ()
	daemon.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, daemon.Start())
	daemonDone := make(chan error, 1)
	go func() { daemonDone <- daemon.Wait() }()
	t.Cleanup(func() {
		_ = daemon.Process.Signal(syscall.SIGTERM)
		select {
		case <-daemonDone:
		case <-time.After(5 * time.Second):
			_ = daemon.Process.Kill()
			<-daemonDone
		}
	})
	muxPath := daemonmux.SocketPath(ipc.SocketDir())
	require.Eventually(t, func() bool {
		info, err := os.Stat(muxPath)
		return err == nil && info.Mode()&os.ModeSocket != 0
	}, 10*time.Second, 10*time.Millisecond, "daemon never published its mux socket at %s", muxPath)
	identity, err := daemonidentity.Load(filepath.Join(os.Getenv("XDG_STATE_HOME"), "vev"))
	require.NoError(t, err)
	require.NotEmpty(t, identity)
	broker := exec.Command(os.Args[0], productionBrokerServeCommand)
	broker.Env = os.Environ()
	broker.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, broker.Start())
	brokerDone := make(chan error, 1)
	go func() { brokerDone <- broker.Wait() }()
	t.Cleanup(func() {
		_ = broker.Process.Signal(syscall.SIGTERM)
		select {
		case <-brokerDone:
		case <-time.After(5 * time.Second):
			_ = broker.Process.Kill()
			<-brokerDone
		}
	})
	var service ports.BrokerService
	require.Eventually(t, func() bool {
		var dialErr error
		service, dialErr = connectExistingBroker(ctx)
		return dialErr == nil
	}, 10*time.Second, 10*time.Millisecond)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	return service, ctx
}

func TestProductionBrokerCompositionLocalAndMutableRemote(t *testing.T) {
	service, ctx := startProductionBrokerFixture(t)
	sub, err := service.Subscribe()
	require.NoError(t, err)
	defer sub.Close()
	require.NotEmpty(t, service.Snapshot().Daemons)
	require.True(t, service.Snapshot().Daemons[0].Local)

	streamID, err := service.NextStreamID()
	require.NoError(t, err)
	stream, err := service.OpenStream(ctx, ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamAttachment, Admission: ports.BrokerAdmissionCreateNamed,
		Name: "production-composition", Local: true, Stream: streamID,
		Policy: localDaemonPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded,
	})
	require.NoError(t, err)
	defer stream.Close()
	require.NoError(t, stream.SendClient(protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentNew, Name: "production-composition",
		Size: domain.Size{Cols: 80, Rows: 24}, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
	}))
	welcome := awaitWelcome(t, stream)
	require.NotNil(t, welcome.CommittedIdentity)
	require.Eventually(t, func() bool {
		snapshot := service.Snapshot()
		return len(snapshot.Daemons) > 0 && snapshot.Daemons[0].Identity != ""
	}, 10*time.Second, 10*time.Millisecond, "local observation: %+v", service.Snapshot())

	registration, err := service.AddHost(ctx, "harness@127.0.0.1", remoteBrokerPolicy("stdio"))
	require.NoError(t, err)
	require.Equal(t, "harness@127.0.0.1", registration.Endpoint)
	require.Eventually(t, func() bool {
		_, ok := service.Snapshot().Find(registration.Endpoint)
		return ok
	}, 10*time.Second, 10*time.Millisecond)
	_, err = service.RemoveHost(ctx, registration)
	require.NoError(t, err)
}

// Both production remote transports are reached through mutable AddHost,
// never through a broker.json fixture route. SSH is replaced by an owned test
// shim; the remote mux helper and daemon are real subprocesses.
func TestProductionBrokerRemoteMuxTransports(t *testing.T) {
	for _, tc := range []struct {
		name, helper string
	}{
		{"stdio", brokerMuxStdioCommand},
		{"quic", brokerMuxQUICBootstrapCommand},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binDir := shortTempDir(t, "vb")
			argsFile := filepath.Join(binDir, "ssh-args")
			script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$VEV_TEST_SSH_ARGS\"\nexec \"$VEV_TEST_SSH_BIN\" \"$VEV_TEST_SSH_MODE\" --production --daemon-start existing-only\n"
			require.NoError(t, os.WriteFile(filepath.Join(binDir, "ssh"), []byte(script), 0o755))
			t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv(fakeSSHArgsEnv, argsFile)
			t.Setenv(fakeSSHBinEnv, os.Args[0])
			t.Setenv(fakeSSHModeEnv, tc.helper)
			t.Setenv(brokerHelperRecordDirEnv, binDir)
			t.Cleanup(func() {
				for _, pid := range recordedPIDsIn(t, binDir, brokerMuxQUICProxyCommand) {
					_ = syscall.Kill(pid, syscall.SIGTERM)
					_ = waitForProcessExit(pid, 5*time.Second)
				}
			})
			service, ctx := startProductionBrokerFixture(t)
			registration, err := service.AddHost(ctx, "harness@127.0.0.1", remoteBrokerPolicy(tc.name))
			require.NoError(t, err)
			streamID, err := service.NextStreamID()
			require.NoError(t, err)
			stream, err := service.OpenStream(ctx, ports.BrokerOpenStreamRequest{
				Purpose: ports.BrokerStreamObservation, Stream: streamID,
				Endpoint: registration.Endpoint, Registration: registration,
				Policy: remoteBrokerPolicy(tc.name), StartMode: ports.BrokerDaemonExistingOnly,
			})
			require.NoError(t, err)
			defer stream.Close()
			require.NoError(t, stream.SendClient(protocol.CommandRequest{RequestID: 1, Version: protocol.Version, Slug: "remote-catalog", JSON: true}))
			message, err := stream.ReceiveServer()
			require.NoError(t, err)
			result, ok := message.(protocol.CommandResult)
			require.True(t, ok, "daemon observation reply: %T %v", message, message)
			require.Equal(t, protocol.CommandSucceeded, result.Outcome)
			argv, err := os.ReadFile(argsFile)
			require.NoError(t, err)
			require.Contains(t, string(argv), tc.helper)
			require.Contains(t, string(argv), "-T")
			require.Contains(t, string(argv), "--")
			require.NotContains(t, strings.ToLower(string(argv)), "stricthostkeychecking=no")
			_, err = service.RemoveHost(ctx, registration)
			require.NoError(t, err)
		})
	}
}
