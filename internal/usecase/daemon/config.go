package daemon

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/terminalcap"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/theme"
)

var commandCodePattern = regexp.MustCompile(`^[A-Z0-9]{2,3}$`)

// themeConfigSnapshot is the theme slice of runtimeConfig. Its zero value is
// the default theme configuration: automatic mode with palette inheritance on.
type themeConfigSnapshot struct {
	mode       domain.ThemeMode
	paletteOff bool
	accent     domain.ThemeAccent
}

// runtimeConfig is the immutable derived configuration published by
// ApplyConfig as one atomic snapshot, so a reader never observes values from
// two different reloads. Never mutate a published snapshot; build a new one.
//
// The key bindings are deliberately not part of the snapshot: keys.Router reads
// them through its own *atomic.Pointer[keys.Bindings] (see Daemon.bindings).
type runtimeConfig struct {
	codeOverrides           map[string]string
	restoreProcessAllowlist map[string]struct{}
	floating                domain.FloatingConfig
	copy                    domain.CopyConfig
	palette                 domain.PaletteConfig
	nav                     domain.NavConfig
	tabs                    domain.TabsConfig
	ephemeral               domain.EphemeralConfig
	scrollback              domain.ScrollbackConfig
	theme                   themeConfigSnapshot
}

// defaultRuntimeConfig is the configuration in force before the first
// ApplyConfig: vev defaults for every section, no code overrides, and the
// default theme snapshot.
func defaultRuntimeConfig() *runtimeConfig {
	defaults := domain.Defaults()
	return &runtimeConfig{
		codeOverrides:           map[string]string{},
		restoreProcessAllowlist: buildRestoreProcessAllowlist(domain.DefaultSnapshotRestoreProcesses()),
		floating:                defaults.Floating,
		copy:                    defaults.Copy,
		palette:                 defaults.Palette,
		nav:                     defaults.Nav,
		tabs:                    defaults.Tabs,
		ephemeral:               defaults.Ephemeral,
		scrollback:              domain.DefaultScrollbackConfig(),
	}
}

// currentRuntimeConfig returns the current snapshot, or the defaults when none has
// been published (zero-value Daemon). The result is never nil.
func (d *Daemon) currentRuntimeConfig() *runtimeConfig {
	if cfg := d.config.Load(); cfg != nil {
		return cfg
	}
	return defaultRuntimeConfig()
}

func (d *Daemon) currentThemeConfig() themeConfigSnapshot {
	return d.currentRuntimeConfig().theme
}

// ApplyConfig validates and atomically swaps daemon runtime configuration.
func (d *Daemon) ApplyConfig(cfg domain.Config) {
	bindings, warnings := keys.BuildBindingEntries(cfg.BindingEntries)
	overrides, codeWarnings := d.buildCodeOverrides(cfg.Codes)
	allWarnings := append(append([]domain.Warning{}, warnings...), codeWarnings...)
	scrollback := cfg.Scrollback
	if !scrollback.Valid() {
		scrollback = domain.DefaultScrollbackConfig()
		allWarnings = append(allWarnings, domain.Warning{Msg: "invalid scrollback limits; using vev defaults"})
	}
	snapshot := &runtimeConfig{
		codeOverrides:           overrides,
		restoreProcessAllowlist: restoreProcessAllowlistFromConfig(cfg.Snapshot),
		floating:                cfg.Floating,
		copy:                    cfg.Copy,
		palette:                 cfg.Palette,
		nav:                     cfg.Nav,
		tabs:                    cfg.Tabs,
		ephemeral:               cfg.Ephemeral,
		scrollback:              scrollback,
		theme:                   themeConfigSnapshot{mode: cfg.Theme, paletteOff: !cfg.ThemePalette, accent: cfg.ThemeAccent},
	}
	d.bindings.Store(bindings)
	d.config.Store(snapshot)
	for _, warning := range allWarnings {
		d.logConfigWarning(warning)
	}
	d.applyHistoryLimits()
	barChanged := false
	if d.barScripts != nil {
		d.barScripts.mu.Lock()
		d.barScripts.initLocked()
		barCfg := barConfigFromDomain(cfg.Bar)
		if d.barScripts.cfg != barCfg {
			d.barScripts.cfg = barCfg
			d.barScripts.outputs = make(map[domain.SessionID]barScriptOutputs)
			d.barScripts.lastRefresh = make(map[domain.SessionID]time.Time)
			d.barScripts.lastContext = make(map[domain.SessionID]barScriptContext)
			d.barScripts.pending = make(map[domain.SessionID]bool)
			d.barScripts.lastFailure = make(map[barScriptFailureKey]string)
			d.barScripts.version++
			barChanged = true
		}
		d.barScripts.mu.Unlock()
	}
	if barChanged {
		d.signalBarPollerReload()
		d.refreshBarScriptsAllSessions()
	}
	d.reapplyThemeAllSessions()
	d.repaintAllAttachedClients()

	if len(allWarnings) > 0 {
		d.notify(nil, domain.NoticeWarn, domain.NoticeConfigReload,
			configReloadNoticeMessage(allWarnings), configWarningsError(allWarnings))
	}
}

func configReloadNoticeMessage(warnings []domain.Warning) string {
	first := warnings[0]
	if first.Line > 0 {
		return fmt.Sprintf("config reloaded with %d warning(s): line %d: %s", len(warnings), first.Line, first.Msg)
	}
	return fmt.Sprintf("config reloaded with %d warning(s): %s", len(warnings), first.Msg)
}

// configWarningsError joins every config warning into a single error whose
// message lists them all, so noticeDetails (which renders a cause's own
// Error() string) surfaces every warning line in one notice's Details.
func configWarningsError(warnings []domain.Warning) error {
	lines := make([]string, len(warnings))
	for i, w := range warnings {
		if w.Line > 0 {
			lines[i] = fmt.Sprintf("line %d: %s", w.Line, w.Msg)
		} else {
			lines[i] = w.Msg
		}
	}
	return errors.New(strings.Join(lines, "; "))
}

func (d *Daemon) buildCodeOverrides(configured map[string]string) (map[string]string, []domain.Warning) {
	commands := command.PaletteRegistry()
	bySlug := make(map[string]command.Command, len(commands))
	for _, cmd := range commands {
		bySlug[cmd.Slug] = cmd
	}

	var warnings []domain.Warning
	desired := make(map[string]string)
	for _, slug := range sortedKeys(configured) {
		if _, ok := bySlug[slug]; !ok {
			warnings = append(warnings, domain.Warning{Msg: fmt.Sprintf("unknown command code slug %q", slug)})
			continue
		}
		code := strings.ToUpper(strings.TrimSpace(configured[slug]))
		if !commandCodePattern.MatchString(code) {
			warnings = append(warnings, domain.Warning{Msg: fmt.Sprintf("invalid command code for %q", slug)})
			continue
		}
		desired[slug] = code
	}

	// The effective code for a slug is its accepted override if it has one,
	// or its registry default otherwise. Resolve to a fixpoint rather than a
	// single sorted pass: dropping one slug's override to resolve a conflict
	// can free up its default code, which may in turn conflict with a
	// *different* slug's override — that must be re-checked regardless of
	// where the two slugs fall in sort order.
	accepted := make(map[string]string, len(desired))
	maps.Copy(accepted, desired)
	effectiveCode := func(slug string) string {
		if code, ok := accepted[slug]; ok {
			return code
		}
		return bySlug[slug].Code
	}

	for {
		claimants := make(map[string][]string, len(commands))
		for _, cmd := range commands {
			code := effectiveCode(cmd.Slug)
			claimants[code] = append(claimants[code], cmd.Slug)
		}

		dropped := false
		conflictCodes := make([]string, 0, len(claimants))
		for code, slugs := range claimants {
			if len(slugs) > 1 {
				conflictCodes = append(conflictCodes, code)
			}
		}
		sort.Strings(conflictCodes)
		for _, code := range conflictCodes {
			slugs := claimants[code]
			sort.Strings(slugs)

			// If one claimant holds this code by default (no accepted
			// override), every override claimant loses to it and is
			// dropped. Otherwise every claimant is an override; keep the
			// lexicographically smallest slug and drop the rest.
			keep := ""
			for _, slug := range slugs {
				if _, isOverride := accepted[slug]; !isOverride {
					keep = slug
					break
				}
			}
			if keep == "" {
				keep = slugs[0]
			}
			for _, slug := range slugs {
				if slug == keep {
					continue
				}
				if _, isOverride := accepted[slug]; !isOverride {
					continue
				}
				warnings = append(warnings, domain.Warning{Msg: fmt.Sprintf("command code %q for %q conflicts with %q", code, slug, keep)})
				delete(accepted, slug)
				dropped = true
			}
		}
		if !dropped {
			break
		}
	}
	return accepted, warnings
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func restoreProcessAllowlistFromConfig(cfg domain.SnapshotConfig) map[string]struct{} {
	if cfg.RestoreProcessesSet {
		return buildRestoreProcessAllowlist(cfg.RestoreProcesses)
	}
	if len(cfg.RestoreProcesses) > 0 {
		return buildRestoreProcessAllowlist(cfg.RestoreProcesses)
	}
	return buildRestoreProcessAllowlist(domain.DefaultSnapshotRestoreProcesses())
}

func buildRestoreProcessAllowlist(values []string) map[string]struct{} {
	allow := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		allow[value] = struct{}{}
	}
	return allow
}

func (d *Daemon) restoreProcessAllowlistSnapshot() map[string]struct{} {
	return d.currentRuntimeConfig().restoreProcessAllowlist
}

func (d *Daemon) logConfigWarning(w domain.Warning) {
	if w.Line > 0 {
		d.log.Warn("config warning", "line", w.Line, "msg", w.Msg)
		return
	}
	d.log.Warn("config warning", "msg", w.Msg)
}

func (d *Daemon) currentCopyConfig() domain.CopyConfig {
	return d.currentRuntimeConfig().copy
}

func (d *Daemon) currentPaletteConfig() domain.PaletteConfig {
	return d.currentRuntimeConfig().palette
}

func (d *Daemon) currentNavConfig() domain.NavConfig {
	return d.currentRuntimeConfig().nav
}

func (d *Daemon) currentTabsConfig() domain.TabsConfig {
	return d.currentRuntimeConfig().tabs
}

func (d *Daemon) currentEphemeralConfig() domain.EphemeralConfig {
	return d.currentRuntimeConfig().ephemeral
}

func (d *Daemon) currentFloatingConfig() domain.FloatingConfig {
	return d.currentRuntimeConfig().floating
}

func (d *Daemon) codeOverrideSnapshot() map[string]string {
	return d.currentRuntimeConfig().codeOverrides
}

func commandWithOverrides(cmd command.Command, overrides map[string]string) command.Command {
	if code, ok := overrides[cmd.Slug]; ok {
		cmd.Code = code
	}
	return cmd
}

func (d *Daemon) commandByEffectiveCode(code string) (command.Command, bool) {
	code = strings.ToUpper(strings.TrimSpace(code))
	overrides := d.codeOverrideSnapshot()
	for _, cmd := range command.PaletteRegistry() {
		cmd = commandWithOverrides(cmd, overrides)
		if cmd.Code == code {
			return cmd, true
		}
	}
	return command.Command{}, false
}

func effectiveThemeForConfig(clientTheme theme.Theme, config themeConfigSnapshot) theme.Theme {
	switch config.mode {
	case domain.ThemeDark:
		return theme.BuiltinDark
	case domain.ThemeLight:
		return theme.BuiltinLight
	default:
		clientTheme.UsePalette = !config.paletteOff
		return clientTheme
	}
}

func (d *Daemon) effectiveTheme(clientTheme theme.Theme) theme.Theme {
	return effectiveThemeForConfig(clientTheme, d.currentThemeConfig())
}

// resolveAppliedTheme resolves chrome for one attachment's color capability;
// ANSI16 and monochrome attachments get attribute-based chrome, so the result
// is never shared between attachments of different modes.
func (d *Daemon) resolveAppliedTheme(raw theme.Theme, color terminalcap.ColorCapabilities) appliedTheme {
	config := d.currentThemeConfig()
	effective := effectiveThemeForConfig(raw, config)
	resolved := theme.ResolveForColor(effective, config.accent, color)
	return appliedTheme{Raw: resolved.Theme, Resolved: resolved}
}

func (d *Daemon) reapplyThemeAllSessions() {
	d.mu.Lock()
	sessions := sessionsSnapshot(d.sessions)
	d.mu.Unlock()

	for _, sess := range sessions {
		d.reapplyThemeSession(sess)
	}
}

func (d *Daemon) reapplyThemeSession(sess *session) {
	attachments := sess.snapshotAttachments()
	if len(attachments) == 0 {
		d.applyHostTheme(sess, nil, theme.Theme{}, true)
		return
	}
	for _, ac := range attachments {
		ac.sendMu.Lock()
		sess.themeMu.Lock()
		d.applyHostThemeLocked(sess, ac, ac.getClientTheme(), false)
		sess.themeMu.Unlock()
		ac.sendMu.Unlock()
	}
}
