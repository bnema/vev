package config

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bnema/kvconf"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestRGBAccentParsing(t *testing.T) {
	for _, value := range []string{"rgb:74e7d4", "rgb:74E7D4", "rgb:000000", "rgb:ffffff", "rgb:xyzxyz", "rgb:12345", "rgb:1234567", "rgb:+12345"} {
		t.Run(value, func(t *testing.T) {
			cfg, warnings, err := Parse(strings.NewReader("theme.accent = " + value))
			require.NoError(t, err)
			valid := value == "rgb:74e7d4" || value == "rgb:74E7D4" || value == "rgb:000000" || value == "rgb:ffffff"
			if valid {
				require.Empty(t, warnings)
				require.Equal(t, domain.ThemeAccentRGB, cfg.ThemeAccent.Mode)
			} else {
				require.NotEmpty(t, warnings)
				require.Equal(t, domain.ThemeAccentAuto, cfg.ThemeAccent.Mode)
			}
		})
	}
}

func TestConfigIncludes(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	path := filepath.Join(home, "config")
	included := filepath.Join(home, "accent")
	require.NoError(t, os.WriteFile(included, []byte("theme.accent = rgb:74e7d4\n"), 0600))
	for _, target := range []string{"accent", "~/accent", included, "missing"} {
		t.Run(target, func(t *testing.T) {
			require.NoError(t, os.WriteFile(path, []byte("include "+target+"\n"), 0600))
			cfg, warnings, err := Load(path)
			require.NoError(t, err)
			require.Empty(t, warnings)
			if target != "missing" {
				require.Equal(t, [3]uint8{0x74, 0xe7, 0xd4}, cfg.ThemeAccent.RGB)
			} else {
				require.Equal(t, domain.ThemeAccentAuto, cfg.ThemeAccent.Mode)
			}
		})
	}
	require.NoError(t, os.WriteFile(filepath.Join(home, "nested"), []byte("include accent\n"), 0600))
	require.NoError(t, os.WriteFile(path, []byte("theme.accent = 3\ninclude nested\ntheme.accent = 5\n"), 0600))
	cfg, warnings, err := Load(path)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0].Msg, "nested include")
	require.Equal(t, domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 5}, cfg.ThemeAccent)
	require.NoError(t, os.WriteFile(path, []byte("include config\n"), 0600))
	_, warnings, err = Load(path)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0].Msg, "nested include")
}

// real I/O: exercises native directory watching and atomic replacement.
func TestWatchIncludedFile(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(map[bool]string{false: "created", true: "replaced"}[exists], func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config")
			included := filepath.Join(dir, "accent")
			writeAtomic(t, path, []byte("include accent\n"))
			if exists {
				writeAtomic(t, included, []byte("theme.accent = 2\n"))
			}
			changes, stop := startWatch(t, path)
			t.Cleanup(stop)
			writeAtomic(t, included, []byte("theme.accent = rgb:74e7d4\n"))
			got := nextChange(t, changes)
			require.Empty(t, got.warnings)
			require.Equal(t, [3]uint8{0x74, 0xe7, 0xd4}, got.cfg.ThemeAccent.RGB)
		})
	}
}

func TestIncludeLimitAndSources(t *testing.T) {
	for _, tc := range []struct {
		name, main, child string
		count             int
	}{
		{"repeated cap", strings.Repeat("include accent\n", maxIncludes+2), "theme.accent = 2\n", 2},
		{"source locations", "\xef\xbb\xbfinclude accent\nbad main\n", "\xef\xbb\xbfbad child\ninclude forbidden\n", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config")
			child := filepath.Join(dir, "accent")
			writeAtomic(t, path, []byte(tc.main))
			writeAtomic(t, child, []byte(tc.child))
			_, warnings, err := Load(path)
			require.NoError(t, err)
			require.Len(t, warnings, tc.count)
			if tc.name == "repeated cap" {
				require.Equal(t, 17, warnings[0].Line)
				require.Equal(t, 18, warnings[1].Line)
				require.Contains(t, warnings[0].Msg, "limit of 16")
			} else {
				require.Contains(t, warnings[0].Msg, child+":1:")
				require.Equal(t, 1, warnings[0].Line)
				require.Contains(t, warnings[1].Msg, path+":2:")
				require.Equal(t, 2, warnings[1].Line)
				require.Contains(t, warnings[2].Msg, child+":2: nested include")
			}
		})
	}
}

// real I/O: the watch hook deterministically exercises the read/install gap.
func TestWatchIncludeInstallationGap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	child := filepath.Join(dir, "accent")
	later := filepath.Join(dir, "later")
	writeAtomic(t, path, []byte("theme.accent = 2\n"))
	writeAtomic(t, child, []byte("theme.accent = 3\n"))
	writeAtomic(t, later, []byte("theme.palette = off\n"))
	original := kvWatch
	t.Cleanup(func() { kvWatch = original })
	kvWatch = func(ctx context.Context, p string, opts kvconf.WatchOptions, out chan<- struct{}) error {
		// Deliberately mutate before the native watch has a baseline: it cannot
		// emit an event for this write. Rescanning must discover both changes.
		switch p {
		case child:
			if err := os.WriteFile(child, []byte("theme.accent = 5\n"), 0600); err != nil {
				return err
			}
			if err := os.WriteFile(path, []byte("include accent\ninclude later\n"), 0600); err != nil {
				return err
			}
		}
		return original(ctx, p, opts, out)
	}
	changes, stop := startWatch(t, path)
	t.Cleanup(stop)
	writeAtomic(t, path, []byte("include accent\n"))
	got := nextChange(t, changes)
	require.Equal(t, uint8(5), got.cfg.ThemeAccent.Slot)
	require.False(t, got.cfg.ThemePalette)
	// The dependency added while the first include was installing must itself
	// have been installed before the above reload was delivered.
	writeAtomic(t, later, []byte("theme.accent = rgb:74e7d4\n"))
	for i := 0; i < 4; i++ {
		got = nextChange(t, changes)
		if got.cfg.ThemeAccent.Mode == domain.ThemeAccentRGB {
			return
		}
	}
	t.Fatal("new dependency was not watched")
}

// real I/O: the hook repairs a failed expansion before watch readiness.
func TestWatchInitialIncludeErrorRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	child := filepath.Join(dir, "accent")
	writeAtomic(t, path, []byte("include accent\n"))
	require.NoError(t, os.Mkdir(child, 0700))
	_, _, err := Load(path)
	require.ErrorIs(t, err, kvconf.ErrNotRegular)
	original := kvWatch
	t.Cleanup(func() { kvWatch = original })
	kvWatch = func(ctx context.Context, p string, opts kvconf.WatchOptions, out chan<- struct{}) error {
		if p == child {
			if err := os.Remove(child); err != nil {
				return err
			}
			if err := os.WriteFile(child, nil, 0600); err != nil {
				return err
			}
		}
		return original(ctx, p, opts, out)
	}
	// startWatch waits for and consumes the initial reload. Before the fix it
	// times out here: both the failed and repaired expansions contain no bytes.
	_, stop := startWatch(t, path)
	t.Cleanup(stop)
	cfg, warnings, err := Load(path)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, domain.Defaults(), cfg)
}
