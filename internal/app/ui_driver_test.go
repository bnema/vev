package app

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseUIDriverArgs(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		want       uiDriverOptions
		wantErr    bool
		wantSocket bool
	}{
		{name: "defaults", want: uiDriverOptions{cols: uiDriverDefaultColumns, rows: uiDriverDefaultRows}},
		{name: "headless", args: []string{"--session", "work", "--cols", "100", "--rows", "40", "--remote", "user@example.com"}, want: uiDriverOptions{session: "work", cols: 100, rows: 40, remote: "user@example.com"}},
		{name: "picker", args: []string{"--picker", "--cols", "100", "--rows", "40"}, want: uiDriverOptions{picker: true, cols: 100, rows: 40}},
		{name: "socket", args: []string{"--socket", "/tmp/ui.sock"}, want: uiDriverOptions{socket: "/tmp/ui.sock", cols: uiDriverDefaultColumns, rows: uiDriverDefaultRows}, wantSocket: true},
		{name: "socket conflicts", args: []string{"--socket", "/tmp/ui.sock", "--rows", "20"}, wantErr: true},
		{name: "socket conflicts with picker", args: []string{"--socket", "/tmp/ui.sock", "--picker"}, wantErr: true},
		{name: "picker conflicts with session", args: []string{"--picker", "--session", "work"}, wantErr: true},
		{name: "picker conflicts with remote", args: []string{"--picker", "--remote", "user@example.com"}, wantErr: true},
		{name: "duplicate picker", args: []string{"--picker", "--picker"}, wantErr: true},
		{name: "relative socket", args: []string{"--socket", "ui.sock"}, wantErr: true},
		{name: "duplicate socket", args: []string{"--socket", "/tmp/a.sock", "--socket", "/tmp/b.sock"}, wantErr: true},
		{name: "duplicate session", args: []string{"--session", "one", "--session", "two"}, wantErr: true},
		{name: "duplicate columns", args: []string{"--cols", "80", "--cols", "100"}, wantErr: true},
		{name: "duplicate rows", args: []string{"--rows", "24", "--rows", "30"}, wantErr: true},
		{name: "duplicate remote", args: []string{"--remote", "one.example", "--remote", "two.example"}, wantErr: true},
		{name: "missing socket", args: []string{"--socket"}, wantErr: true},
		{name: "missing session", args: []string{"--session"}, wantErr: true},
		{name: "missing columns", args: []string{"--cols"}, wantErr: true},
		{name: "missing rows", args: []string{"--rows"}, wantErr: true},
		{name: "missing remote", args: []string{"--remote"}, wantErr: true},
		// --launch-config is a clean break: the option no longer exists and is
		// refused as unknown, before any terminal, broker, or filesystem access.
		{name: "removed launch config", args: []string{"--launch-config", "/tmp/config.json"}, wantErr: true},
		{name: "removed launch config without value", args: []string{"--launch-config"}, wantErr: true},
		{name: "removed launch config after session", args: []string{"--session", "work", "--launch-config", "/tmp/config.json"}, wantErr: true},
		{name: "unknown", args: []string{"--nope"}, wantErr: true},
		{name: "positional", args: []string{"work"}, wantErr: true},
		{name: "bad geometry", args: []string{"--cols", "0"}, wantErr: true},
		{name: "bad remote", args: []string{"--remote", "bad host"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseUIDriverArgs(tt.args)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			if tt.wantSocket {
				require.NotEmpty(t, got.socket)
			}
		})
	}
}

// TestLaunchConfigOptionIsAnUnknownFlagWithUsageExit pins the clean break: the
// removed option is refused by argument parsing alone, with the usage exit code,
// before any terminal, broker, or filesystem interaction.
func TestLaunchConfigOptionIsAnUnknownFlagWithUsageExit(t *testing.T) {
	command, err := parseArgs([]string{"--ui-driver", "--launch-config", "/tmp/config.json"})
	require.Error(t, err)
	require.Zero(t, command.kind)
	require.Equal(t, 2, ExitCode(err), "an unknown option is a usage error")
	require.ErrorContains(t, err, "unknown `--ui-driver` option")
	require.ErrorContains(t, err, "--launch-config")
}

// TestUIDriverOptionsHelpAndUsage pins the usage text for the closed option set:
// the removed option never appears, and the new one does.
func TestUIDriverOptionsHelpAndUsage(t *testing.T) {
	_, err := parseUIDriverArgs([]string{"--help"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--picker")
	require.NotContains(t, err.Error(), "launch-config")
}

func TestParseArgsUIOptionsAreAttachOnly(t *testing.T) {
	command, err := parseArgs([]string{"--ui-control", "new", "work"})
	require.NoError(t, err)
	require.Equal(t, kindAttach, command.kind)
	require.True(t, command.uiControl)
	require.False(t, command.uiObserve)
	driver, err := parseArgs([]string{"--ui-driver", "--session", "work"})
	require.NoError(t, err)
	require.Equal(t, kindUIDriver, driver.kind)
	require.Equal(t, "work", driver.uiDriver.session)
	picker, err := parseArgs([]string{"--ui-driver", "--picker"})
	require.NoError(t, err)
	require.Equal(t, kindUIDriver, picker.kind)
	require.True(t, picker.uiDriver.picker)
	_, err = parseArgs([]string{"ui-driver"})
	require.Error(t, err)
	_, err = parseArgs([]string{"--ui-driver", "--ui-observe"})
	require.Error(t, err)
	trailing, err := parseArgs([]string{"attach", "work", "--ui-observe", "--ui-socket", "/tmp/ui.sock"})
	require.NoError(t, err)
	require.True(t, trailing.uiObserve)
	require.Equal(t, "/tmp/ui.sock", trailing.uiSocket)
	_, err = parseArgs([]string{"--ui-observe", "ls"})
	require.Error(t, err)
	_, err = parseArgs([]string{"--ui-socket", "/tmp/ui.sock"})
	require.Error(t, err)
}

// TestOfflineClientHarnessRejectsPickerForTheTerminalHarness pins the sandbox
// option boundary: --picker is a UI-driver start, never a terminal-harness one.
func TestOfflineClientHarnessRejectsPickerForTheTerminalHarness(t *testing.T) {
	_, err := parseArgs([]string{brokerClientCommand, "--offline-root", "/tmp/offline", "--picker"})
	require.Error(t, err)
	command, err := parseArgs([]string{brokerClientCommand, "--offline-root", "/tmp/offline", "--harness", offlineClientUIDriver, "--picker"})
	require.NoError(t, err)
	require.Equal(t, kindBrokerClient, command.kind)
	require.True(t, command.brokerClient.picker)
}

// TestInteractiveObservedCompositionUsesTheBrokerConnector pins that the
// interactive observed path and the headless driver keep no daemon dialer, no
// remote factory, and no launch configuration: both compose the shared broker
// client over the process' production connector.
func TestInteractiveObservedCompositionUsesTheBrokerConnector(t *testing.T) {
	sources := loadAppSources(t, false)
	body := sources["ui_driver.go"]
	require.Contains(t, body, "newProductionBrokerConnector()")
	for _, forbidden := range []string{
		"runAttachWithDeps", "localDaemonDialer", "remoteDialerFactory", "launchConfig",
		"configuredRemoteLaunches", "cleanupHeadlessEndpoint", "remoteLaunchAllowlistFromEnv",
		"runAttachDeps", "launchEndpoint", "uiRemoteCleanupCommand",
	} {
		require.NotContains(t, body, forbidden, "the UI-driver composition must not keep %q", forbidden)
	}
}
