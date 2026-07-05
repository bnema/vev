package daemon

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/theme"
)

var commandCodePattern = regexp.MustCompile(`^[A-Z0-9]{2,3}$`)

// ApplyConfig validates and atomically swaps daemon runtime configuration.
func (d *Daemon) ApplyConfig(cfg domain.Config) {
	bindings, warnings := keys.BuildBindingEntries(cfg.BindingEntries)
	if len(cfg.BindingEntries) == 0 && len(cfg.Bindings) > 0 {
		bindings, warnings = keys.BuildBindings(cfg.Bindings)
	}
	for _, warning := range warnings {
		d.logConfigWarning(warning)
	}
	overrides := d.buildCodeOverrides(cfg.Codes)
	d.bindings.Store(bindings)
	d.codeOverrides.Store(&overrides)
	d.themeMode.Store(uint32(cfg.Theme))
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

	used := make(map[string]string, len(commands))
	for _, cmd := range commands {
		used[cmd.Code] = cmd.Slug
	}
	overrides := make(map[string]string)
	for _, slug := range sortedKeys(desired) {
		code := desired[slug]
		fallback := bySlug[slug].Code
		delete(used, fallback)
		if existing, ok := used[code]; ok && existing != slug {
			d.logConfigWarning(domain.Warning{Msg: fmt.Sprintf("command code %q for %q conflicts with %q", code, slug, existing)})
			if fallbackOwner, fallbackUsed := used[fallback]; fallbackUsed && fallbackOwner != slug {
				d.logConfigWarning(domain.Warning{Msg: fmt.Sprintf("default command code %q for %q conflicts with %q", fallback, slug, fallbackOwner)})
				continue
			}
			used[fallback] = slug
			continue
		}
		used[code] = slug
		overrides[slug] = code
	}
	return overrides
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
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
		d.applyHostTheme(sess, ac, d.effectiveTheme(clientTheme))
	}
}
