package config

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestParseRemote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		input              string
		want               domain.RemoteConfig
		wantErr            string
		wantErrIs          error
		wantWarnings       []domain.Warning
		wantTheme          domain.ThemeMode
		checkTheme         bool
		wantBindingEntries []domain.ConfigEntry
		checkBindings      bool
	}{
		{
			name: "defaults when section absent",
			want: domain.RemoteConfig{Enabled: true, Remember: true},
		},
		{
			name: "complete remote block",
			input: `[remote]
enabled = true
remember = true
`,
			want: domain.RemoteConfig{
				Enabled:  true,
				Remember: true,
			},
		},
		{
			name: "enabled false",
			input: `[remote]
enabled = false
`,
			want: domain.RemoteConfig{Enabled: false, Remember: true},
		},
		{
			name: "remember false",
			input: `[remote]
remember = false
`,
			want: domain.RemoteConfig{Enabled: true, Remember: false},
		},
		{
			name: "legacy on off rejected and defaults retained",
			input: `[remote]
enabled = off
remember = on
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `invalid enabled "off"`},
				{Line: 3, Msg: `invalid remember "on"`},
			},
		},
		{
			name: "duplicate keys warn and last valid wins",
			input: `[remote]
enabled = false
enabled = true
remember = false
remember = true
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true},
			wantWarnings: []domain.Warning{
				{Line: 3, Msg: `duplicate key "enabled"`},
				{Line: 5, Msg: `duplicate key "remember"`},
			},
		},
		{
			name: "invalid booleans warn and retain defaults",
			input: `[remote]
enabled = yes
remember = True
`,
			want: domain.RemoteConfig{Enabled: true, Remember: true},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `invalid enabled "yes"`},
				{Line: 3, Msg: `invalid remember "True"`},
			},
		},
		{
			name: "hosts assignment fails closed",
			input: `[remote]
hosts = ["arch", "build@mule"]
`,
			wantErr:      `line 2: unsupported remote hosts assignment`,
			wantErrIs:    ErrUnsupportedRemoteHosts,
			wantWarnings: nil,
		},
		{
			name: "empty hosts array fails closed",
			input: `[remote]
hosts = []
`,
			wantErr:      `line 2: unsupported remote hosts assignment`,
			wantErrIs:    ErrUnsupportedRemoteHosts,
			wantWarnings: nil,
		},
		{
			name: "malformed hosts assignment fails closed",
			input: `[remote]
hosts = not-an-array
`,
			wantErr:      `line 2: unsupported remote hosts assignment`,
			wantErrIs:    ErrUnsupportedRemoteHosts,
			wantWarnings: nil,
		},
		{
			name: "hosts cannot fall through as a binding",
			input: `theme = light
[remote]
hosts = ["arch"]
new-tab = alt+t
`,
			wantErr:      `line 3: unsupported remote hosts assignment`,
			wantErrIs:    ErrUnsupportedRemoteHosts,
			wantWarnings: nil,
		},
		{
			name: "unknown remote key warns",
			input: `[remote]
proxy = true
enabled = false
`,
			want: domain.RemoteConfig{Enabled: false, Remember: true},
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
			want: domain.RemoteConfig{Enabled: false, Remember: true},
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
enabled = true
`,
			want:               domain.RemoteConfig{Enabled: true, Remember: true},
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
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				require.ErrorIs(t, err, tt.wantErrIs)
				require.Equal(t, tt.wantWarnings, warnings)
				require.Empty(t, cfg.BindingEntries)
				return
			}
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
