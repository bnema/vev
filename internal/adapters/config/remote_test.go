package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/safedir"
	"github.com/stretchr/testify/require"
)

func TestParseRemote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		input              string
		want               domain.RemoteConfig
		wantWarnings       []domain.Warning
		wantTheme          domain.ThemeMode
		checkTheme         bool
		wantBindingEntries []domain.ConfigEntry
		checkBindings      bool
	}{
		{
			name: "defaults when section absent",
			want: domain.RemoteConfig{Enabled: true, Remember: true, Hosts: []string{}},
		},
		{
			name: "complete remote block",
			input: `[remote]
enabled = true
remember = true
hosts = ["arch", "build@mule"]
`,
			want: domain.RemoteConfig{
				Enabled:  true,
				Remember: true,
				Hosts:    []string{"arch", "build@mule"},
			},
		},
		{
			name: "enabled false",
			input: `[remote]
enabled = false
`,
			want: domain.RemoteConfig{Enabled: false, Remember: true, Hosts: []string{}},
		},
		{
			name: "remember false",
			input: `[remote]
remember = false
`,
			want: domain.RemoteConfig{Enabled: true, Remember: false, Hosts: []string{}},
		},
		{
			name: "legacy on off rejected and defaults retained",
			input: `[remote]
enabled = off
remember = on
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true, Hosts: []string{}},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `invalid enabled "off"`},
				{Line: 3, Msg: `invalid remember "on"`},
			},
		},
		{
			name: "empty hosts array",
			input: `[remote]
hosts = []
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true, Hosts: []string{}},
		},
		{
			name: "duplicate keys warn and last valid wins",
			input: `[remote]
enabled = false
enabled = true
remember = false
remember = true
hosts = ["arch"]
hosts = ["mule"]
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true, Hosts: []string{"mule"}},
			wantWarnings: []domain.Warning{
				{Line: 3, Msg: `duplicate key "enabled"`},
				{Line: 5, Msg: `duplicate key "remember"`},
				{Line: 7, Msg: `duplicate key "hosts"`},
			},
		},
		{
			name: "invalid booleans warn and retain defaults",
			input: `[remote]
enabled = yes
remember = True
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true, Hosts: []string{}},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `invalid enabled "yes"`},
				{Line: 3, Msg: `invalid remember "True"`},
			},
		},
		{
			name: "invalid hosts array retains default",
			input: `[remote]
hosts = [1, "arch"]
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true, Hosts: []string{}},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `invalid hosts "[1, \"arch\"]"`},
			},
		},
		{
			name: "malformed hosts array retains default",
			input: `[remote]
hosts = not-an-array
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true, Hosts: []string{}},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `invalid hosts "not-an-array"`},
			},
		},
		{
			name: "null hosts retains default",
			input: `[remote]
hosts = null
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true, Hosts: []string{}},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `invalid hosts "null"`},
			},
		},
		{
			name: "invalid and duplicate host entries retain default",
			input: `[remote]
hosts = ["arch", "arch", " bad", "ok"]
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true, Hosts: []string{}},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `duplicate host "arch"`},
				{Line: 2, Msg: `invalid remote host " bad": remote host target " bad" has surrounding whitespace`},
			},
		},
		{
			name: "invalid hosts assignment retains previous value",
			input: `[remote]
hosts = ["arch"]
hosts = ["arch", " bad", "ok"]
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true, Hosts: []string{"arch"}},
			wantWarnings: []domain.Warning{
				{Line: 3, Msg: `duplicate key "hosts"`},
				{Line: 3, Msg: `invalid remote host " bad": remote host target " bad" has surrounding whitespace`},
			},
		},
		{
			name: "unknown remote key warns",
			input: `[remote]
proxy = true
enabled = false
`,
			want: domain.RemoteConfig{Enabled: false, Remember: true, Hosts: []string{}},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `unknown key "proxy"`},
			},
		},
		{
			name: "unknown section warns and ignores its keys",
			input: `theme = dark
[other]
enabled = false
[remote]
enabled = false
`,
			want: domain.RemoteConfig{Enabled: false, Remember: true, Hosts: []string{}},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `unknown section "other"`},
			},
			wantTheme:  domain.ThemeDark,
			checkTheme: true,
		},
		{
			name: "flat keys preserved outside remote section",
			input: `theme = light
new-tab = alt+t
[remote]
hosts = ["arch"]
`,
			want:               domain.RemoteConfig{Enabled: true, Remember: true, Hosts: []string{"arch"}},
			wantTheme:          domain.ThemeLight,
			checkTheme:         true,
			wantBindingEntries: []domain.ConfigEntry{{Key: "new-tab", Value: "alt+t"}},
			checkBindings:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.Remote)
			require.Equal(t, tt.wantWarnings, warnings)
			if tt.checkTheme {
				require.Equal(t, tt.wantTheme, cfg.Theme)
			}
			if tt.checkBindings {
				require.Equal(t, tt.wantBindingEntries, cfg.BindingEntries)
			}
		})
	}
}

func TestUpdateRemoteHosts(t *testing.T) {
	t.Parallel()

	privateConfigPath := func(t *testing.T) string {
		t.Helper()
		return filepath.Join(t.TempDir(), "vev", "config")
	}

	t.Run("creates missing config", func(t *testing.T) {
		t.Parallel()
		path := privateConfigPath(t)

		require.NoError(t, UpdateRemoteHosts(path, []string{"arch", "build@mule"}))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "[remote]\nhosts = [\"arch\", \"build@mule\"]\n", string(data))
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	})

	t.Run("replaces existing hosts and preserves unrelated text", func(t *testing.T) {
		t.Parallel()
		path := privateConfigPath(t)
		require.NoError(t, safedir.EnsurePrivate(filepath.Dir(path)))
		original := "# keep me\ntheme = dark\n[remote]\nenabled = false\nhosts = [\"old\"]\nremember = false\nnew-tab = alt+t\n"
		require.NoError(t, os.WriteFile(path, []byte(original), 0o600))

		require.NoError(t, UpdateRemoteHosts(path, []string{"arch", "arch", "mule"}))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		got := string(data)
		require.Contains(t, got, "# keep me\n")
		require.Contains(t, got, "theme = dark\n")
		require.Contains(t, got, "enabled = false\n")
		require.Contains(t, got, "remember = false\n")
		require.Contains(t, got, "new-tab = alt+t\n")
		require.Contains(t, got, "hosts = [\"arch\", \"mule\"]\n")
		require.NotContains(t, got, "hosts = [\"old\"]")
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	})

	t.Run("appends remote section when missing", func(t *testing.T) {
		t.Parallel()
		path := privateConfigPath(t)
		require.NoError(t, safedir.EnsurePrivate(filepath.Dir(path)))
		require.NoError(t, os.WriteFile(path, []byte("theme = light\n"), 0o600))

		require.NoError(t, UpdateRemoteHosts(path, []string{"arch"}))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "theme = light\n[remote]\nhosts = [\"arch\"]\n", string(data))
	})

	t.Run("appends hosts line when remote section lacks hosts", func(t *testing.T) {
		t.Parallel()
		path := privateConfigPath(t)
		require.NoError(t, safedir.EnsurePrivate(filepath.Dir(path)))
		require.NoError(t, os.WriteFile(path, []byte("[remote]\nenabled = true\n"), 0o600))

		require.NoError(t, UpdateRemoteHosts(path, []string{"arch"}))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "[remote]\nenabled = true\nhosts = [\"arch\"]\n", string(data))
	})

	t.Run("preserves CRLF line endings when appending hosts", func(t *testing.T) {
		t.Parallel()
		path := privateConfigPath(t)
		require.NoError(t, safedir.EnsurePrivate(filepath.Dir(path)))
		original := "[remote]\r\nenabled = true\r\n"
		require.NoError(t, os.WriteFile(path, []byte(original), 0o600))

		require.NoError(t, UpdateRemoteHosts(path, []string{"arch"}))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "[remote]\r\nenabled = true\r\nhosts = [\"arch\"]\r\n", string(data))
	})

	t.Run("fails when existing path cannot be read", func(t *testing.T) {
		t.Parallel()
		path := privateConfigPath(t)
		require.NoError(t, os.MkdirAll(path, 0o700))

		err := UpdateRemoteHosts(path, []string{"arch"})
		require.Error(t, err)
	})

	t.Run("rejects invalid targets without changing existing file", func(t *testing.T) {
		t.Parallel()
		path := privateConfigPath(t)
		require.NoError(t, safedir.EnsurePrivate(filepath.Dir(path)))
		original := "[remote]\nhosts = [\"arch\"]\n"
		require.NoError(t, os.WriteFile(path, []byte(original), 0o600))

		invalid := []struct {
			name  string
			hosts []string
		}{
			{name: "empty", hosts: []string{""}},
			{name: "whitespace", hosts: []string{" bad"}},
			{name: "control", hosts: []string{"good", "bad\x00"}},
			{name: "mixed with valid", hosts: []string{"arch", " "}},
		}
		for _, tt := range invalid {
			t.Run(tt.name, func(t *testing.T) {
				err := UpdateRemoteHosts(path, tt.hosts)
				require.Error(t, err)
				data, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				require.Equal(t, original, string(data))
			})
		}
	})

	t.Run("rejects invalid targets before creating parent directory", func(t *testing.T) {
		t.Parallel()
		path := privateConfigPath(t)

		err := UpdateRemoteHosts(path, []string{" "})
		require.Error(t, err)
		_, statErr := os.Stat(filepath.Dir(path))
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("replaces last effective hosts assignment", func(t *testing.T) {
		t.Parallel()
		path := privateConfigPath(t)
		require.NoError(t, safedir.EnsurePrivate(filepath.Dir(path)))
		original := "[remote]\nhosts = [\"first\"]\nenabled = true\nhosts = [\"second\"]\n"
		require.NoError(t, os.WriteFile(path, []byte(original), 0o600))

		require.NoError(t, UpdateRemoteHosts(path, []string{"arch"}))

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		got := string(data)
		require.Contains(t, got, "hosts = [\"first\"]\n")
		require.Contains(t, got, "hosts = [\"arch\"]\n")
		require.NotContains(t, got, "hosts = [\"second\"]")
		cfg, warnings, err := Parse(strings.NewReader(got))
		require.NoError(t, err)
		require.Equal(t, []string{"arch"}, cfg.Remote.Hosts)
		require.Equal(t, []domain.Warning{{Line: 4, Msg: `duplicate key "hosts"`}}, warnings)
	})

	t.Run("ignores stale shared tmp permissions", func(t *testing.T) {
		t.Parallel()
		path := privateConfigPath(t)
		dir := filepath.Dir(path)
		require.NoError(t, safedir.EnsurePrivate(dir))
		staleTmp := path + ".tmp"
		require.NoError(t, os.WriteFile(staleTmp, []byte("STALE"), 0o644))

		require.NoError(t, UpdateRemoteHosts(path, []string{"arch"}))

		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		stale, err := os.ReadFile(staleTmp)
		require.NoError(t, err)
		require.Equal(t, "STALE", string(stale))
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "[remote]\nhosts = [\"arch\"]\n", string(data))
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		for _, entry := range entries {
			name := entry.Name()
			if name == "config" || name == "config.tmp" {
				continue
			}
			require.False(t, strings.Contains(name, ".tmp"), "leftover temp %q", name)
		}
	})

	t.Run("concurrent writes to same path leave complete parseable result", func(t *testing.T) {
		t.Parallel()
		path := privateConfigPath(t)
		dir := filepath.Dir(path)
		require.NoError(t, safedir.EnsurePrivate(dir))

		const n = 32
		lists := make([][]string, n)
		for i := 0; i < n; i++ {
			lists[i] = []string{"host-a-" + strconv.Itoa(i), "host-b-" + strconv.Itoa(i)}
		}
		errCh := make(chan error, n)
		for i := 0; i < n; i++ {
			go func(i int) {
				errCh <- UpdateRemoteHosts(path, lists[i])
			}(i)
		}
		for i := 0; i < n; i++ {
			require.NoError(t, <-errCh)
		}

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		cfg, warnings, err := Parse(strings.NewReader(string(data)))
		require.NoError(t, err)
		require.Empty(t, warnings)
		matched := false
		for _, list := range lists {
			if reflect.DeepEqual(cfg.Remote.Hosts, list) {
				matched = true
				break
			}
		}
		require.True(t, matched, "final hosts %#v must equal one complete submitted list", cfg.Remote.Hosts)

		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		for _, entry := range entries {
			name := entry.Name()
			if name == "config" {
				continue
			}
			require.False(t, strings.Contains(name, "tmp"), "leftover temp %q", name)
		}
	})
}
