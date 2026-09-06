package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/ports"
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
		{name: "headless", args: []string{"--session", "work", "--cols", "100", "--rows", "40", "--remote", "user@example.com", "--launch-config", "/tmp/config.json"}, want: uiDriverOptions{session: "work", cols: 100, rows: 40, remote: "user@example.com", launchConfig: "/tmp/config.json"}},
		{name: "socket", args: []string{"--socket", "/tmp/ui.sock"}, want: uiDriverOptions{socket: "/tmp/ui.sock", cols: uiDriverDefaultColumns, rows: uiDriverDefaultRows}, wantSocket: true},
		{name: "socket conflicts", args: []string{"--socket", "/tmp/ui.sock", "--rows", "20"}, wantErr: true},
		{name: "relative socket", args: []string{"--socket", "ui.sock"}, wantErr: true},
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

func TestParseArgsUIOptionsAreAttachOnly(t *testing.T) {
	command, err := parseArgs([]string{"--ui-control", "new", "work"})
	require.NoError(t, err)
	require.Equal(t, kindAttach, command.kind)
	require.True(t, command.uiControl)
	require.True(t, command.uiObserve == false)
	trailing, err := parseArgs([]string{"attach", "work", "--ui-observe", "--ui-socket", "/tmp/ui.sock"})
	require.NoError(t, err)
	require.True(t, trailing.uiObserve)
	require.Equal(t, "/tmp/ui.sock", trailing.uiSocket)
	_, err = parseArgs([]string{"--ui-observe", "ls"})
	require.Error(t, err)
	_, err = parseArgs([]string{"--ui-socket", "/tmp/ui.sock"})
	require.Error(t, err)
}

func TestParseLaunchConfigStrictAndOptionalEndpoints(t *testing.T) {
	root := filepath.Join(t.TempDir(), "endpoint-root")
	configPath := filepath.Join(t.TempDir(), "launch.json")
	data := `{"version":1,"local":{"binary":"/bin/vev","root":"` + root + `","env":{"HOME":"/home/test","PATH":"/bin"}}}`
	require.NoError(t, os.WriteFile(configPath, []byte(data), 0o600))
	config, err := parseLaunchConfig(configPath)
	require.NoError(t, err)
	require.NotNil(t, config.local)
	require.Empty(t, config.remotes)
	require.Equal(t, "/bin/vev", config.local.binary)

	for name, contents := range map[string]string{
		"duplicate":         `{"version":1,"version":1}`,
		"escaped duplicate": `{"version":1,"local":{"binary":"/bin/vev","root":"/tmp/root","env":{"PA\u0054H":"/bin","PATH":"/usr/bin"}}}`,
		"unknown":           `{"version":1,"bogus":true}`,
		"relative binary":   `{"version":1,"local":{"binary":"vev","root":"/tmp/root","env":{}}}`,
		"reserved env":      `{"version":1,"local":{"binary":"/bin/vev","root":"/tmp/root","env":{"XDG_RUNTIME_DIR":"/tmp"}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "launch.json")
			require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
			_, err := parseLaunchConfig(path)
			require.Error(t, err)
		})
	}
}

func TestParseLaunchConfigAcceptsRemoteEndpoint(t *testing.T) {
	root := filepath.Join(t.TempDir(), "remote-root")
	configPath := filepath.Join(t.TempDir(), "launch.json")
	data := `{"version":1,"remotes":[{"endpoint":"user@example.com","binary":"/bin/vev","root":"` + root + `","env":{"HOME":"/home/test","PATH":"/bin"}}]}`
	require.NoError(t, os.WriteFile(configPath, []byte(data), 0o600))

	config, err := parseLaunchConfig(configPath)
	require.NoError(t, err)
	require.Nil(t, config.local)
	require.Equal(t, launchEndpoint{binary: "/bin/vev", root: root, env: map[string]string{"HOME": "/home/test", "PATH": "/bin"}}, config.remotes["user@example.com"])
}

func TestLaunchEnvironmentForConfigIncludesRemoteAllowlist(t *testing.T) {
	config := &launchConfig{remotes: map[string]launchEndpoint{
		"user@z.example": {},
		"user@a.example": {},
	}}
	environment := launchEnvironmentSliceForConfig(launchEndpoint{root: "/tmp/root"}, config)
	var allowlist string
	for _, entry := range environment {
		if strings.HasPrefix(entry, launchAllowedRemoteEndpointsEnv+"=") {
			allowlist = strings.TrimPrefix(entry, launchAllowedRemoteEndpointsEnv+"=")
		}
	}
	require.Equal(t, "user@a.example\nuser@z.example", allowlist)
}

func TestRemoteLaunchAllowlistFromEnvironment(t *testing.T) {
	t.Setenv(launchAllowedRemoteEndpointsEnv, "user@z.example\nuser@a.example")
	allowed, configured, err := remoteLaunchAllowlistFromEnv()
	require.NoError(t, err)
	require.True(t, configured)
	require.Equal(t, map[string]struct{}{"user@z.example": {}, "user@a.example": {}}, allowed)

	t.Setenv(launchAllowedRemoteEndpointsEnv, "user@bad host")
	_, configured, err = remoteLaunchAllowlistFromEnv()
	require.Error(t, err)
	require.True(t, configured)

	t.Setenv(launchAllowedRemoteEndpointsEnv, "")
	allowed, configured, err = remoteLaunchAllowlistFromEnv()
	require.NoError(t, err)
	require.True(t, configured)
	require.Empty(t, allowed)
}

func TestLaunchRootIsExclusiveAndPrivate(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	require.NoError(t, createLaunchRoot(root))
	info, err := os.Stat(root)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	for _, child := range []string{"config", "state", "runtime", "tmp"} {
		info, err := os.Stat(filepath.Join(root, child))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	}
	require.Error(t, createLaunchRoot(root))
	require.NoError(t, removeOwnedLaunchRoot(root))
	require.NoError(t, removeOwnedLaunchRoot(root))
}

func TestConfiguredRemoteDialerRejectsUnlistedEndpoint(t *testing.T) {
	config := &launchConfig{remotes: map[string]launchEndpoint{"user@example.com": {binary: "/bin/vev", root: "/tmp/remote", env: map[string]string{}}}}
	factory := configuredRemoteDialerFactory(config)
	_, err := factory("other@example.com", "work", "udp", nil)
	var uiErr *ports.UIError
	require.ErrorAs(t, err, &uiErr)
	require.Equal(t, ports.UIErrEndpointNotConfigured, uiErr.Code)
	_, err = factory("user@example.com", "work", "unknown", nil)
	require.Error(t, err)
}
