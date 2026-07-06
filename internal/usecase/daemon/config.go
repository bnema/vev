package daemon

import (
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/theme"
)

var commandCodePattern = regexp.MustCompile(`^[A-Z0-9]{2,3}$`)

// ApplyConfig validates and atomically swaps daemon runtime configuration.
func (d *Daemon) ApplyConfig(cfg domain.Config) {
	bindings, warnings := keys.BuildBindingEntries(cfg.BindingEntries)
	for _, warning := range warnings {
		d.logConfigWarning(warning)
	}
	overrides := d.buildCodeOverrides(cfg.Codes)
	allowlist := restoreProcessAllowlistFromConfig(cfg.Snapshot)
	d.bindings.Store(bindings)
	d.codeOverrides.Store(&overrides)
	d.restoreProcessAllowlist.Store(&allowlist)
	d.themeMode.Store(uint32(cfg.Theme))
	if d.barScripts != nil {
		d.barScripts.mu.Lock()
		d.barScripts.cfg = barConfigFromDomain(cfg.Bar)
		d.barScripts.outputs = make(map[domain.SessionID]barScriptOutputs)
		d.barScripts.lastRefresh = make(map[domain.SessionID]time.Time)
		d.barScripts.lastContext = make(map[domain.SessionID]barScriptContext)
		d.barScripts.version++
		d.barScripts.mu.Unlock()
	}
	d.reapplyThemeAllSessions()
	d.repaintAllAttachedClients()
}

func (d *Daemon) buildCodeOverrides(configured map[string]string) map[string]string {
	commands := command.Registry()
	bySlug := make(map[string]command.Command, len(commands))
	for _, cmd := range commands {
		bySlug[cmd.Slug] = cmd
	}

	desired := make(map[string]string)
	for _, slug := range sortedKeys(configured) {
		if _, ok := bySlug[slug]; !ok {
			d.logConfigWarning(domain.Warning{Msg: fmt.Sprintf("unknown command code slug %q", slug)})
			continue
		}
		code := strings.ToUpper(strings.TrimSpace(configured[slug]))
		if !commandCodePattern.MatchString(code) {
			d.logConfigWarning(domain.Warning{Msg: fmt.Sprintf("invalid command code for %q", slug)})
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
		for code, slugs := range claimants {
			if len(slugs) < 2 {
				continue
			}
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
				d.logConfigWarning(domain.Warning{Msg: fmt.Sprintf("command code %q for %q conflicts with %q", code, slug, keep)})
				delete(accepted, slug)
				dropped = true
			}
		}
		if !dropped {
			break
		}
	}
	return accepted
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
	allow := d.restoreProcessAllowlist.Load()
	if allow == nil {
		return buildRestoreProcessAllowlist(domain.DefaultSnapshotRestoreProcesses())
	}
	return *allow
}

func (d *Daemon) logConfigWarning(w domain.Warning) {
	if w.Line > 0 {
		d.log.Warn("config warning", "line", w.Line, "msg", w.Msg)
		return
	}
	d.log.Warn("config warning", "msg", w.Msg)
}

func (d *Daemon) codeOverrideSnapshot() map[string]string {
	overrides := d.codeOverrides.Load()
	if overrides == nil {
		return nil
	}
	return *overrides
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
	for _, cmd := range command.Registry() {
		cmd = commandWithOverrides(cmd, overrides)
		if cmd.Code == code {
			return cmd, true
		}
	}
	return command.Command{}, false
}

func (d *Daemon) effectiveTheme(clientTheme theme.Theme) theme.Theme {
	switch domain.ThemeMode(d.themeMode.Load()) {
	case domain.ThemeDark:
		return theme.BuiltinDark
	case domain.ThemeLight:
		return theme.BuiltinLight
	default:
		return clientTheme
	}
}

func (d *Daemon) reapplyThemeAllSessions() {
	d.mu.Lock()
	sessions := make([]*session, 0, len(d.sessions))
	for _, sess := range d.sessions {
		sessions = append(sessions, sess)
	}
	d.mu.Unlock()

	for _, sess := range sessions {
		sess.mu.Lock()
		ac := sess.client
		sess.mu.Unlock()
		var clientTheme theme.Theme
		if ac != nil {
			clientTheme = ac.getClientTheme()
		}
		d.applyHostTheme(sess, ac, d.effectiveTheme(clientTheme), ac == nil)
	}
}
