package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/kvconf"
	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestDefaultsDisableBarCommands(t *testing.T) {
	bar := domain.Defaults().Bar
	require.Empty(t, bar.TopRight)
	require.Empty(t, bar.BottomRight)
}

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		want         domain.Config
		wantWarnings []domain.Warning
	}{
		{
			name:  "comments and inline comments",
			input: "# full line\nnew-tab = alt+t # open tab\ndetach = alt+#\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				BindingEntries: []domain.ConfigEntry{
					{Key: "new-tab", Value: "alt+t"},
					{Key: "detach", Value: "alt+#"},
				},
				Codes: map[string]string{},
			},
		},
		{
			name:  "crlf and code prefix",
			input: "theme = light\r\ncode.detach = dt\r\n",
			want: domain.Config{
				Theme:          domain.ThemeLight,
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{"detach": "dt"},
			},
		},
		{
			name:  "invalid theme warns and keeps default",
			input: "theme = blue\n",
			want: domain.Config{
				Theme:          domain.ThemeAuto,
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
			wantWarnings: []domain.Warning{{Line: 1, Msg: "invalid theme \"blue\""}},
		},
		{
			name:  "unknown binding key stored",
			input: "unknown.action = alt+x\n",
			want: domain.Config{
				Theme:          domain.ThemeAuto,
				BindingEntries: []domain.ConfigEntry{{Key: "unknown.action", Value: "alt+x"}},
				Codes:          map[string]string{},
			},
		},
		{
			name:  "duplicates warn and last wins",
			input: "new-tab = alt+t\nnew-tab = alt+n\ncode.detach = DT\ncode.detach = DX\n",
			want: domain.Config{
				Theme:          domain.ThemeAuto,
				BindingEntries: []domain.ConfigEntry{{Key: "new-tab", Value: "alt+n"}},
				Codes:          map[string]string{"detach": "DX"},
			},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: "duplicate key \"new-tab\""},
				{Line: 4, Msg: "duplicate key \"code.detach\""},
			},
		},
		{
			name:  "invalid lines report line numbers",
			input: "\nno equals\n = value\ntheme = dark\n",
			want: domain.Config{
				Theme:          domain.ThemeDark,
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: "missing '='"},
				{Line: 3, Msg: "missing key"},
			},
		},
		{
			name:  "floating settings custom values",
			input: "floating.command = \"btop --utf-force\"\nfloating.width = 75%\nfloating.height = 60%\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Floating: domain.FloatingConfig{
					Command: "btop --utf-force",
					Width:   75,
					Height:  60,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
		},
		{
			name:  "floating command can be empty or quoted",
			input: "floating.command = echo ready\nfloating.command = \"\"\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Floating: domain.FloatingConfig{
					Width:  80,
					Height: 80,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
			wantWarnings: []domain.Warning{{Line: 2, Msg: "duplicate key \"floating.command\""}},
		},
		{
			name:  "a value that is only a comment is empty",
			input: "floating.command = echo ready\nfloating.command = # default shell\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Floating: domain.FloatingConfig{
					Width:  80,
					Height: 80,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
			wantWarnings: []domain.Warning{{Line: 2, Msg: "duplicate key \"floating.command\""}},
		},
		{
			name:  "floating percentage endpoints",
			input: "floating.width = 1%\nfloating.height = 100%\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Floating: domain.FloatingConfig{
					Width:  1,
					Height: 100,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
		},
		{
			name:  "floating invalid percentages warn and preserve defaults",
			input: "floating.width = 80\nfloating.height = 0%\nfloating.width = 101%\nfloating.height = eighty%\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Floating: domain.FloatingConfig{
					Width:  80,
					Height: 80,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
			wantWarnings: []domain.Warning{
				{Line: 1, Msg: "invalid floating.width \"80\""},
				{Line: 2, Msg: "invalid floating.height \"0%\""},
				{Line: 3, Msg: "duplicate key \"floating.width\""},
				{Line: 3, Msg: "invalid floating.width \"101%\""},
				{Line: 4, Msg: "duplicate key \"floating.height\""},
				{Line: 4, Msg: "invalid floating.height \"eighty%\""},
			},
		},
		{
			name:  "floating duplicate keys warn and last valid value wins",
			input: "floating.command = btop\nfloating.command = lazygit\nfloating.width = 70%\nfloating.width = 90%\nfloating.height = 60%\nfloating.height = 85%\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Floating: domain.FloatingConfig{
					Command: "lazygit",
					Width:   90,
					Height:  85,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: "duplicate key \"floating.command\""},
				{Line: 4, Msg: "duplicate key \"floating.width\""},
				{Line: 6, Msg: "duplicate key \"floating.height\""},
			},
		},
		{
			name:  "floating malformed quoted command warns and keeps default",
			input: "floating.command = \"bad\\q\"\n",
			want: domain.Config{
				Theme:          domain.ThemeAuto,
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
			wantWarnings: []domain.Warning{{Line: 1, Msg: "invalid floating.command \"\\\"bad\\\\q\\\"\""}},
		},
		{
			name:  "bar settings custom values",
			input: "bar.top-right = custom-top\nbar.bottom-right = custom-bottom\nbar.interval = 2s\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Bar: domain.BarConfig{
					TopRight:    "custom-top",
					BottomRight: "custom-bottom",
					Interval:    2 * time.Second,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
		},
		{
			name:  "bar settings quoted commands unquote",
			input: "bar.top-right = \"date +%H:%M\"\nbar.bottom-right = \"\"\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Bar: domain.BarConfig{
					TopRight:    "date +%H:%M",
					BottomRight: "",
					Interval:    5 * time.Second,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
		},
		{
			name:  "bar settings quoted commands preserve hash",
			input: "bar.top-right = \"echo foo #1\" # inline comment\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Bar: domain.BarConfig{
					TopRight:    "echo foo #1",
					BottomRight: "",
					Interval:    5 * time.Second,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
		},
		{
			name:  "bar settings malformed quoted command warns and keeps default",
			input: "bar.top-right = \"bad\\q\"\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Bar: domain.BarConfig{
					TopRight:    "",
					BottomRight: "",
					Interval:    5 * time.Second,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
			wantWarnings: []domain.Warning{{Line: 1, Msg: "invalid bar.top-right \"\\\"bad\\\\q\\\"\""}},
		},
		{
			name:  "bar settings empty commands disable anchors",
			input: "bar.top-right =\nbar.bottom-right =\nbar.interval = 5s\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Bar: domain.BarConfig{
					TopRight:    "",
					BottomRight: "",
					Interval:    5 * time.Second,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
		},
		{
			name:  "bar interval invalid warns and keeps default",
			input: "bar.interval = nope\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Bar: domain.BarConfig{
					TopRight:    "",
					BottomRight: "",
					Interval:    5 * time.Second,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
			wantWarnings: []domain.Warning{{Line: 1, Msg: "invalid bar.interval \"nope\""}},
		},
		{
			name:  "bar interval below minimum clamps and warns",
			input: "bar.interval = 100ms\n",
			want: domain.Config{
				Theme: domain.ThemeAuto,
				Bar: domain.BarConfig{
					TopRight:    "",
					BottomRight: "",
					Interval:    time.Second,
				},
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
			},
			wantWarnings: []domain.Warning{{Line: 1, Msg: "bar.interval below minimum \"1s\""}},
		},
		{
			name:  "snapshot restore processes",
			input: "snapshot.restore_processes = claude, codex, pi, opencode, btop\n",
			want: domain.Config{
				Theme:          domain.ThemeAuto,
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
				Snapshot: domain.SnapshotConfig{
					RestoreProcesses:    []string{"claude", "codex", "pi", "opencode", "btop"},
					RestoreProcessesSet: true,
				},
			},
		},
		{
			name:  "snapshot restore processes empty disables",
			input: "snapshot.restore_processes =\n",
			want: domain.Config{
				Theme:          domain.ThemeAuto,
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
				Snapshot: domain.SnapshotConfig{
					RestoreProcessesSet: true,
				},
			},
		},
		{
			name:  "snapshot restore processes warns on malformed entries",
			input: "snapshot.restore_processes = claude, custom-tool, /bin/sh, :all:, bad name, claude, , zsh\n",
			want: domain.Config{
				Theme:          domain.ThemeAuto,
				BindingEntries: []domain.ConfigEntry{},
				Codes:          map[string]string{},
				Snapshot: domain.SnapshotConfig{
					RestoreProcesses:    []string{"claude", "custom-tool", "zsh"},
					RestoreProcessesSet: true,
				},
			},
			wantWarnings: []domain.Warning{
				{Line: 1, Msg: "invalid snapshot restore process \"/bin/sh\""},
				{Line: 1, Msg: "invalid snapshot restore process \":all:\""},
				{Line: 1, Msg: "invalid snapshot restore process \"bad name\""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if !tt.want.ThemePalette {
				tt.want.ThemePalette = domain.Defaults().ThemePalette
			}
			if tt.want.Bar.Interval == 0 {
				tt.want.Bar = domain.Defaults().Bar
			}
			if tt.want.Floating.Width == 0 && tt.want.Floating.Height == 0 {
				tt.want.Floating = domain.Defaults().Floating
			}
			if !tt.want.Tabs.TerminalTitle {
				tt.want.Tabs = domain.Defaults().Tabs
			}
			if !tt.want.Ephemeral.CloseOnExit {
				tt.want.Ephemeral = domain.Defaults().Ephemeral
			}
			if !tt.want.Keyboard.KittyProtocol {
				tt.want.Keyboard = domain.Defaults().Keyboard
			}
			if tt.want.Echo.Predict == domain.EchoPredictNever {
				tt.want.Echo = domain.Defaults().Echo
			}
			if tt.want.Snapshot.RestoreProcesses == nil && !tt.want.Snapshot.RestoreProcessesSet {
				tt.want.Snapshot.RestoreProcesses = append([]string(nil), domain.DefaultSnapshotRestoreProcesses()...)
			}
			if tt.want.Copy.WordSeparators == "" {
				tt.want.Copy = domain.Defaults().Copy
			}
			if !tt.want.Palette.AnchorSet {
				tt.want.Palette = domain.Defaults().Palette
			}
			tt.want.Scrollback = domain.DefaultScrollbackConfig()
			got, warnings, err := Parse(strings.NewReader(tt.input))
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Parse() config = %#v, want %#v", got, tt.want)
			}
			if !reflect.DeepEqual(warnings, tt.wantWarnings) {
				t.Fatalf("Parse() warnings = %#v, want %#v", warnings, tt.wantWarnings)
			}
		})
	}
}

func TestParseThemePalette(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		want         bool
		wantWarnings []domain.Warning
	}{
		{
			name:  "on enables terminal palette inheritance",
			input: "theme.palette = on\n",
			want:  true,
		},
		{
			name:  "off disables terminal palette inheritance",
			input: "theme.palette = off\n",
			want:  false,
		},
		{
			name:  "invalid value warns once and keeps default",
			input: "theme.palette = maybe\n",
			want:  true,
			wantWarnings: []domain.Warning{
				{Line: 1, Msg: "invalid theme.palette \"maybe\""},
			},
		},
		{
			name:  "absent key keeps default",
			input: "theme = dark\n",
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, got.ThemePalette)
			require.Equal(t, tt.wantWarnings, warnings)
		})
	}
}

func TestParseBarBackground(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		input           string
		wantTransparent bool
		wantWarnings    []domain.Warning
	}{
		{name: "on keeps the theme surface", input: "bar.background = on\n"},
		{name: "off makes bars transparent", input: "bar.background = off\n", wantTransparent: true},
		{
			name:  "invalid value warns and keeps default",
			input: "bar.background = none\n",
			wantWarnings: []domain.Warning{
				{Line: 1, Msg: "invalid bar.background \"none\""},
			},
		},
		{name: "absent key keeps default", input: "bar.interval = 2s\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.wantTransparent, got.Bar.Transparent)
			require.Equal(t, tt.wantWarnings, warnings)
		})
	}
}

func TestDefaultsThemePalette(t *testing.T) {
	t.Parallel()

	require.True(t, domain.Defaults().ThemePalette)
}

func TestParseTabsTerminalTitle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		want         bool
		wantWarnings []domain.Warning
	}{
		{
			name:  "on enables terminal title",
			input: "tabs.terminal-title = on\n",
			want:  true,
		},
		{
			name:  "off disables terminal title",
			input: "tabs.terminal-title = off\n",
			want:  false,
		},
		{
			name:  "invalid value warns and keeps default",
			input: "tabs.terminal-title = maybe\n",
			want:  true,
			wantWarnings: []domain.Warning{
				{Line: 1, Msg: "invalid tabs.terminal-title \"maybe\""},
			},
		},
		{
			name:  "absent key keeps default",
			input: "theme = dark\n",
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, warnings, err := Parse(strings.NewReader(tt.input))
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if got.Tabs.TerminalTitle != tt.want {
				t.Fatalf("Parse() Tabs.TerminalTitle = %v, want %v", got.Tabs.TerminalTitle, tt.want)
			}
			if !reflect.DeepEqual(warnings, tt.wantWarnings) {
				t.Fatalf("Parse() warnings = %#v, want %#v", warnings, tt.wantWarnings)
			}
		})
	}
}

func TestParseEphemeralCloseOnExit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		want         bool
		wantWarnings []domain.Warning
	}{
		{name: "absent key defaults on", input: "theme = dark\n", want: true},
		{name: "on", input: "ephemeral.close-on-exit = on\n", want: true},
		{name: "off", input: "ephemeral.close-on-exit = off\n", want: false},
		{
			name:  "invalid value warns and keeps default",
			input: "ephemeral.close-on-exit = kill\n",
			want:  true,
			wantWarnings: []domain.Warning{
				{Line: 1, Msg: "invalid ephemeral.close-on-exit \"kill\""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, warnings, err := Parse(strings.NewReader(tt.input))
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if got.Ephemeral.CloseOnExit != tt.want {
				t.Fatalf("Parse() Ephemeral.CloseOnExit = %v, want %v", got.Ephemeral.CloseOnExit, tt.want)
			}
			if !reflect.DeepEqual(warnings, tt.wantWarnings) {
				t.Fatalf("Parse() warnings = %#v, want %#v", warnings, tt.wantWarnings)
			}
		})
	}
}

func TestParseKeyboardKittyProtocol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		want         bool
		wantWarnings []domain.Warning
	}{
		{name: "absent key defaults on", input: "theme = dark\n", want: true},
		{name: "on", input: "keyboard.kitty-protocol = on\n", want: true},
		{name: "off", input: "keyboard.kitty-protocol = off\n", want: false},
		{
			name:  "invalid value warns and keeps default",
			input: "keyboard.kitty-protocol = maybe\n",
			want:  true,
			wantWarnings: []domain.Warning{
				{Line: 1, Msg: "invalid keyboard.kitty-protocol \"maybe\""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, warnings, err := Parse(strings.NewReader(tt.input))
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if got.Keyboard.KittyProtocol != tt.want {
				t.Fatalf("Parse() Keyboard.KittyProtocol = %v, want %v", got.Keyboard.KittyProtocol, tt.want)
			}
			if !reflect.DeepEqual(warnings, tt.wantWarnings) {
				t.Fatalf("Parse() warnings = %#v, want %#v", warnings, tt.wantWarnings)
			}
		})
	}
}

func TestParseEchoPredict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		want         domain.EchoPredictMode
		wantWarnings []domain.Warning
	}{
		{name: "absent key defaults adaptive", input: "theme = dark\n", want: domain.EchoPredictAdaptive},
		{name: "adaptive", input: "echo.predict = adaptive\n", want: domain.EchoPredictAdaptive},
		{name: "always", input: "echo.predict = always\n", want: domain.EchoPredictAlways},
		{name: "never", input: "echo.predict = Never\n", want: domain.EchoPredictNever},
		{
			name:  "invalid value warns and keeps default",
			input: "echo.predict = sometimes\n",
			want:  domain.EchoPredictAdaptive,
			wantWarnings: []domain.Warning{
				{Line: 1, Msg: "invalid echo.predict \"sometimes\" (want adaptive, always, or never)"},
			},
		},
		{
			name:  "duplicate key warns and keeps last",
			input: "echo.predict = always\necho.predict = never\n",
			want:  domain.EchoPredictNever,
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: "duplicate key \"echo.predict\""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, warnings, err := Parse(strings.NewReader(tt.input))
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if got.Echo.Predict != tt.want {
				t.Fatalf("Parse() Echo.Predict = %v, want %v", got.Echo.Predict, tt.want)
			}
			if !reflect.DeepEqual(warnings, tt.wantWarnings) {
				t.Fatalf("Parse() warnings = %#v, want %#v", warnings, tt.wantWarnings)
			}
		})
	}
}

func TestParseNavigationOverflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		want         domain.NavConfig
		wantWarnings []domain.Warning
	}{
		{
			name: "absent defaults both settings off",
		},
		{
			name:  "tabs on enables only tab overflow",
			input: "nav.overflow-tabs = on\n",
			want:  domain.NavConfig{OverflowTabs: true},
		},
		{
			name:  "sessions on enables only session overflow",
			input: "nav.overflow-sessions = on\n",
			want:  domain.NavConfig{OverflowSessions: true},
		},
		{
			name:  "off disables both settings",
			input: "nav.overflow-tabs = off\nnav.overflow-sessions = off\n",
		},
		{
			name:  "settings are independent",
			input: "nav.overflow-tabs = on\nnav.overflow-sessions = off\n",
			want:  domain.NavConfig{OverflowTabs: true},
		},
		{
			name:  "invalid values warn and preserve defaults",
			input: "nav.overflow-tabs = maybe\nnav.overflow-sessions = always\n",
			wantWarnings: []domain.Warning{
				{Line: 1, Msg: `invalid nav.overflow-tabs "maybe"`},
				{Line: 2, Msg: `invalid nav.overflow-sessions "always"`},
			},
		},
		{
			name:  "duplicate keys warn and last valid value wins",
			input: "nav.overflow-tabs = off\nnav.overflow-tabs = on\nnav.overflow-sessions = on\nnav.overflow-sessions = off\n",
			want:  domain.NavConfig{OverflowTabs: true},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `duplicate key "nav.overflow-tabs"`},
				{Line: 4, Msg: `duplicate key "nav.overflow-sessions"`},
			},
		},
		{
			name:  "invalid duplicate still warns and keeps previous value",
			input: "nav.overflow-tabs = on\nnav.overflow-tabs = maybe\n",
			want:  domain.NavConfig{OverflowTabs: true},
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `duplicate key "nav.overflow-tabs"`},
				{Line: 2, Msg: `invalid nav.overflow-tabs "maybe"`},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.Nav)
			require.Equal(t, tt.wantWarnings, warnings)
		})
	}
}

func TestDefaultsDisableNavigationOverflow(t *testing.T) {
	t.Parallel()

	require.Equal(t, domain.NavConfig{}, domain.Defaults().Nav)
}

func TestDefaultsCopiesSnapshotRestoreProcesses(t *testing.T) {
	t.Parallel()

	first := domain.Defaults()
	first.Snapshot.RestoreProcesses[0] = "mutated"
	second := domain.Defaults()
	require.Equal(t, "vi", second.Snapshot.RestoreProcesses[0])
}

func TestParseCopyWordSeparators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        string
		want         string
		wantWarnings []domain.Warning
	}{
		{
			name:  "default",
			input: "",
			want:  domain.DefaultWordSeparators,
		},
		{
			name:  "quoted preserves leading separator space",
			input: `copy.word-separators = " -_@/"`,
			want:  " -_@/",
		},
		{
			name:  "quoted empty is valid",
			input: `copy.word-separators = ""`,
			want:  "",
		},
		{
			name:  "unquoted empty is valid",
			input: "copy.word-separators =",
			want:  "",
		},
		{
			name:  "malformed quote keeps default",
			input: `copy.word-separators = "unterminated`,
			want:  domain.DefaultWordSeparators,
			wantWarnings: []domain.Warning{
				{Line: 1, Msg: `invalid copy.word-separators "\"unterminated"`},
			},
		},
		{
			name:  "duplicate last value wins",
			input: "copy.word-separators = /\ncopy.word-separators = _",
			want:  "_",
			wantWarnings: []domain.Warning{
				{Line: 2, Msg: `duplicate key "copy.word-separators"`},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.Copy.WordSeparators)
			require.Equal(t, tt.wantWarnings, warnings)
		})
	}
}

func TestDefaultsCopyWordSeparators(t *testing.T) {
	t.Parallel()

	require.Equal(t, " -_@", domain.Defaults().Copy.WordSeparators)
}

func TestParsePreservesBindingFileOrder(t *testing.T) {
	t.Parallel()

	cfg, warnings, err := Parse(strings.NewReader("open-palette = alt+p\nfocus-pane-left = alt+p\n"))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("Parse() warnings = %#v, want none", warnings)
	}
	want := []domain.ConfigEntry{
		{Key: "open-palette", Value: "alt+p"},
		{Key: "focus-pane-left", Value: "alt+p"},
	}
	if !reflect.DeepEqual(cfg.BindingEntries, want) {
		t.Fatalf("Parse() BindingEntries = %#v, want %#v", cfg.BindingEntries, want)
	}
}

func TestLoadMissingFileReturnsDefaults(t *testing.T) {
	t.Parallel()

	cfg, warnings, err := Load(filepath.Join(t.TempDir(), "missing"))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("Load() warnings = %#v, want none", warnings)
	}
	if !reflect.DeepEqual(cfg, domain.Defaults()) {
		t.Fatalf("Load() config = %#v, want defaults", cfg)
	}
}

type watchChange struct {
	cfg      domain.Config
	warnings []domain.Warning
}

// startWatch runs watch on path and returns the onChange stream once the
// watches are installed. The returned stop cancels it and checks its result.
func startWatch(t *testing.T, path string) (<-chan watchChange, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	changes := make(chan watchChange, 8)
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- watch(ctx, path, kvconf.WatchOptions{Debounce: 10 * time.Millisecond, Ready: ready},
			func(cfg domain.Config, warnings []domain.Warning) {
				changes <- watchChange{cfg: cfg, warnings: warnings}
			})
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("watch() returned before ready: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the watcher to be ready")
	}
	return changes, func() {
		t.Helper()
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for watch() to return")
		}
	}
}

// writeAtomic replaces path in one step, like most editors: the watcher
// never sees a truncated file, whatever the scheduling.
func writeAtomic(t *testing.T, path string, data []byte) {
	t.Helper()
	tmp := path + ".tmp"
	require.NoError(t, os.WriteFile(tmp, data, 0o600))
	require.NoError(t, os.Rename(tmp, path))
}

func nextChange(t *testing.T, changes <-chan watchChange) watchChange {
	t.Helper()
	select {
	case got := <-changes:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for config reload")
		return watchChange{}
	}
}

func TestWatchReloadsOnChange(t *testing.T) {
	t.Parallel()

	light := domain.Defaults()
	light.Theme = domain.ThemeLight

	tests := []struct {
		name         string
		initial      *string // nil: the file does not exist yet
		write        func(t *testing.T, path string)
		wantConfig   domain.Config
		wantWarnings []domain.Warning
		wantAnyWarn  bool
	}{
		{
			name:    "changed file reloads with parse warnings",
			initial: ptr("theme = dark\n"),
			write: func(t *testing.T, path string) {
				writeAtomic(t, path, []byte("theme = light\nbad line\n"))
			},
			wantConfig:   light,
			wantWarnings: []domain.Warning{{Line: 2, Msg: "missing '='"}},
		},
		{
			name:    "created file loads",
			initial: nil,
			write: func(t *testing.T, path string) {
				writeAtomic(t, path, []byte("theme = light\n"))
			},
			wantConfig: light,
		},
		{
			name:    "deleted file reloads to defaults",
			initial: ptr("theme = light\n"),
			write: func(t *testing.T, path string) {
				require.NoError(t, os.Remove(path))
			},
			wantConfig: domain.Defaults(),
		},
		{
			name:    "load failure reports defaults and a warning",
			initial: ptr("theme = light\n"),
			write: func(t *testing.T, path string) {
				// Larger than kvconf.DefaultMaxBytes.
				writeAtomic(t, path, []byte(strings.Repeat("# x\n", kvconf.DefaultMaxBytes/4+1)))
			},
			wantConfig:  domain.Defaults(),
			wantAnyWarn: true,
		},
		{
			name:    "invalid UTF-8 reports defaults and a warning",
			initial: ptr("theme = light\n"),
			write: func(t *testing.T, path string) {
				writeAtomic(t, path, []byte("theme = \xff\n"))
			},
			wantConfig:  domain.Defaults(),
			wantAnyWarn: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "config")
			if tt.initial != nil {
				writeAtomic(t, path, []byte(*tt.initial))
			}
			changes, stop := startWatch(t, path)
			tt.write(t, path)

			got := nextChange(t, changes)
			require.Equal(t, tt.wantConfig, got.cfg)
			if tt.wantAnyWarn {
				require.NotEmpty(t, got.warnings)
			} else {
				require.Equal(t, tt.wantWarnings, got.warnings)
			}
			stop()
		})
	}
}

// A load error is reported once until a good load, then again.
func TestWatchReportsLoadErrorOnce(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config")
	writeAtomic(t, path, []byte("theme = light\n"))
	changes, stop := startWatch(t, path)
	invalid := []byte("theme = \xff\n")

	writeAtomic(t, path, invalid)
	got := nextChange(t, changes)
	require.Equal(t, domain.Defaults(), got.cfg)
	require.Len(t, got.warnings, 1)

	// The same failure again is not reported; the next good load is.
	writeAtomic(t, path, invalid)
	writeAtomic(t, path, []byte("theme = dark\n"))
	got = nextChange(t, changes)
	require.Equal(t, domain.ThemeDark, got.cfg.Theme)
	require.Empty(t, got.warnings)

	// After a good load, the failure is news again.
	writeAtomic(t, path, invalid)
	got = nextChange(t, changes)
	require.Len(t, got.warnings, 1)
	stop()
}

// A stat error at startup other than a missing file is reported.
func TestWatchReportsStartupStatError(t *testing.T) {
	t.Parallel()

	// A name longer than NAME_MAX makes every stat fail.
	path := filepath.Join(t.TempDir(), strings.Repeat("x", 5000))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changes := make(chan watchChange, 4)
	done := make(chan error, 1)
	go func() {
		done <- watch(ctx, path, kvconf.WatchOptions{Poll: time.Hour}, func(cfg domain.Config, warnings []domain.Warning) {
			changes <- watchChange{cfg: cfg, warnings: warnings}
		})
	}()
	got := nextChange(t, changes)
	require.Equal(t, domain.Defaults(), got.cfg)
	require.Len(t, got.warnings, 1)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch() to return")
	}
}

func ptr[T any](v T) *T { return &v }

// When native watching fails (inotify limits), watch keeps the current
// config, warns once and polls instead of giving up.
func TestWatchFallsBackToPolling(t *testing.T) { //nolint:paralleltest // replaces kvWatch
	path := filepath.Join(t.TempDir(), "config")
	writeAtomic(t, path, []byte("theme = light\n"))
	var polled atomic.Bool
	orig := kvWatch
	t.Cleanup(func() { kvWatch = orig })
	kvWatch = func(ctx context.Context, path string, opts kvconf.WatchOptions, out chan<- struct{}) error {
		if opts.Poll == 0 {
			return errors.New("too many open files")
		}
		polled.Store(true)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	changes := make(chan watchChange, 4)
	done := make(chan error, 1)
	go func() {
		done <- watch(ctx, path, kvconf.WatchOptions{}, func(cfg domain.Config, warnings []domain.Warning) {
			changes <- watchChange{cfg: cfg, warnings: warnings}
		})
	}()
	got := nextChange(t, changes)
	require.Equal(t, domain.ThemeLight, got.cfg.Theme, "the current config is kept")
	require.Len(t, got.warnings, 1)
	require.Contains(t, got.warnings[0].Msg, "polling every")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.True(t, polled.Load())
}
