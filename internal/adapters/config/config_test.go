package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
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
			if tt.want.Snapshot.RestoreProcesses == nil && !tt.want.Snapshot.RestoreProcessesSet {
				tt.want.Snapshot.RestoreProcesses = append([]string(nil), domain.DefaultSnapshotRestoreProcesses()...)
			}
			if tt.want.Copy.WordSeparators == "" {
				tt.want.Copy = domain.Defaults().Copy
			}
			if !tt.want.Palette.AnchorSet {
				tt.want.Palette = domain.Defaults().Palette
			}
			if tt.want.Remote == (domain.RemoteConfig{}) {
				tt.want.Remote = domain.Defaults().Remote
			}
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

func TestWatchReloadsChangedFileWithFakeClock(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte("theme = dark\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	clk := newWatchClockMock(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type change struct {
		cfg      domain.Config
		warnings []domain.Warning
	}
	changes := make(chan change, 2)
	done := make(chan error, 1)
	go func() {
		done <- Watch(ctx, clk.clock, path, func(cfg domain.Config, warnings []domain.Warning) {
			changes <- change{cfg: cfg, warnings: warnings}
		})
	}()

	clk.waitForTimer()
	if err := os.WriteFile(path, []byte("theme = light\nbad line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	clk.fire()

	var got change
	select {
	case got = <-changes:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for config reload")
	}
	if got.cfg.Theme != domain.ThemeLight {
		t.Fatalf("Watch() theme = %v, want %v", got.cfg.Theme, domain.ThemeLight)
	}
	if !reflect.DeepEqual(got.warnings, []domain.Warning{{Line: 2, Msg: "missing '='"}}) {
		t.Fatalf("Watch() warnings = %#v", got.warnings)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch() error = %v, want context.Canceled", err)
	}
}

func TestWatchUsesDefaultsWhenReloadLoadFails(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, []byte("theme = dark\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	clk := newWatchClockMock(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type change struct {
		cfg      domain.Config
		warnings []domain.Warning
	}
	changes := make(chan change, 2)
	done := make(chan error, 1)
	go func() {
		done <- Watch(ctx, clk.clock, path, func(cfg domain.Config, warnings []domain.Warning) {
			changes <- change{cfg: cfg, warnings: warnings}
		})
	}()

	clk.waitForTimer()
	tooLong := strings.Repeat("x", 70*1024)
	if err := os.WriteFile(path, []byte(tooLong), 0o600); err != nil {
		t.Fatal(err)
	}
	clk.fire()

	got := <-changes
	require.Equal(t, domain.Defaults(), got.cfg)
	require.NotEmpty(t, got.warnings)

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch() error = %v, want context.Canceled", err)
	}
}

func TestWatchDoesNotRepeatPersistentStatError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), strings.Repeat("x", 5000))
	clk := newWatchClockMock(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changes := make(chan []domain.Warning, 4)
	done := make(chan error, 1)
	go func() {
		done <- Watch(ctx, clk.clock, path, func(_ domain.Config, warnings []domain.Warning) {
			changes <- warnings
		})
	}()

	first := <-changes
	require.Len(t, first, 1)
	clk.waitForTimer()
	clk.fire()
	clk.fire()

	select {
	case extra := <-changes:
		t.Fatalf("Watch() repeated persistent stat error: %#v", extra)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch() error = %v, want context.Canceled", err)
	}
}

type watchClockMock struct {
	t      *testing.T
	clock  *portsmocks.MockClock
	ticks  chan time.Time
	ready  chan struct{}
	resets chan struct{}
}

func newWatchClockMock(t *testing.T) *watchClockMock {
	t.Helper()
	timer := portsmocks.NewMockTimer(t)
	clock := portsmocks.NewMockClock(t)
	m := &watchClockMock{
		t:      t,
		clock:  clock,
		ticks:  make(chan time.Time),
		ready:  make(chan struct{}),
		resets: make(chan struct{}),
	}
	timer.EXPECT().C().Return(m.ticks)
	timer.EXPECT().Reset(pollInterval).Run(func(time.Duration) {
		m.resets <- struct{}{}
	}).Return(false)
	timer.EXPECT().Stop().Return(true)
	clock.EXPECT().NewTimer(pollInterval).Run(func(time.Duration) {
		close(m.ready)
	}).Return(timer).Once()
	return m
}

func (m *watchClockMock) waitForTimer() {
	m.t.Helper()
	select {
	case <-m.ready:
	case <-time.After(time.Second):
		m.t.Fatal("timed out waiting for watcher timer creation")
	}
}

func (m *watchClockMock) fire() {
	m.t.Helper()
	select {
	case m.ticks <- time.Now():
	case <-time.After(time.Second):
		m.t.Fatal("timed out firing watcher timer")
	}
	select {
	case <-m.resets:
	case <-time.After(time.Second):
		m.t.Fatal("timed out waiting for watcher timer reset")
	}
}
