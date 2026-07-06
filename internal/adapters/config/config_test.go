package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

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
					TopRight:    "vev-bar-top-right",
					BottomRight: "vev-bar-bottom-right",
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
					TopRight:    "vev-bar-top-right",
					BottomRight: "vev-bar-bottom-right",
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

			if tt.want.Bar.Interval == 0 {
				tt.want.Bar = domain.Defaults().Bar
			}
			if tt.want.Snapshot.RestoreProcesses == nil && !tt.want.Snapshot.RestoreProcessesSet {
				tt.want.Snapshot.RestoreProcesses = append([]string(nil), domain.DefaultSnapshotRestoreProcesses()...)
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

func TestDefaultsCopiesSnapshotRestoreProcesses(t *testing.T) {
	t.Parallel()

	first := domain.Defaults()
	first.Snapshot.RestoreProcesses[0] = "mutated"
	second := domain.Defaults()
	require.Equal(t, "vi", second.Snapshot.RestoreProcesses[0])
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

	clk := newFakeClock(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type change struct {
		cfg      domain.Config
		warnings []domain.Warning
	}
	changes := make(chan change, 2)
	done := make(chan error, 1)
	go func() {
		done <- Watch(ctx, clk, path, func(cfg domain.Config, warnings []domain.Warning) {
			changes <- change{cfg: cfg, warnings: warnings}
		})
	}()

	clk.waitForTimers(t, 1)
	if err := os.WriteFile(path, []byte("theme = light\nbad line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)

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

	clk := newFakeClock(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type change struct {
		cfg      domain.Config
		warnings []domain.Warning
	}
	changes := make(chan change, 2)
	done := make(chan error, 1)
	go func() {
		done <- Watch(ctx, clk, path, func(cfg domain.Config, warnings []domain.Warning) {
			changes <- change{cfg: cfg, warnings: warnings}
		})
	}()

	clk.waitForTimers(t, 1)
	tooLong := strings.Repeat("x", 70*1024)
	if err := os.WriteFile(path, []byte(tooLong), 0o600); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Second)

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
	clk := newFakeClock(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changes := make(chan []domain.Warning, 4)
	done := make(chan error, 1)
	go func() {
		done <- Watch(ctx, clk, path, func(_ domain.Config, warnings []domain.Warning) {
			changes <- warnings
		})
	}()

	first := <-changes
	require.Len(t, first, 1)
	clk.waitForTimers(t, 1)
	clk.advance(2 * time.Second)
	clk.advance(2 * time.Second)

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

var _ ports.Clock = (*fakeClock)(nil)
var _ ports.Timer = (*fakeTimer)(nil)

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	notify chan struct{}
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now, notify: make(chan struct{}, 16)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(d time.Duration) ports.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &fakeTimer{clock: c, ch: make(chan time.Time, 1), next: c.now.Add(d), active: true}
	c.timers = append(c.timers, t)
	c.notify <- struct{}{}
	return t
}

func (c *fakeClock) waitForTimers(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for c.timerCount() < n {
		select {
		case <-c.notify:
		case <-deadline:
			t.Fatalf("timed out waiting for %d timers, got %d", n, c.timerCount())
		}
	}
}

func (c *fakeClock) timerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
	for _, timer := range c.timers {
		timer.mu.Lock()
		if timer.active && !timer.next.After(c.now) {
			timer.ch <- c.now
			timer.active = false
		}
		timer.mu.Unlock()
	}
}

type fakeTimer struct {
	clock  *fakeClock
	mu     sync.Mutex
	ch     chan time.Time
	next   time.Time
	active bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.mu.Lock()
	defer t.mu.Unlock()

	wasActive := t.active
	t.active = true
	t.next = t.clock.now.Add(d)
	return wasActive
}

func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	wasActive := t.active
	t.active = false
	return wasActive
}
