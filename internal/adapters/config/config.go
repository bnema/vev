package config

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const pollInterval = 2 * time.Second

var (
	processNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`)
	percentagePattern  = regexp.MustCompile(`^[0-9]{1,3}%$`)
	sectionPattern     = regexp.MustCompile(`^\[([^\]]+)\]$`)
)

// Parse reads vev's flat action = value config format. Duplicate action keys
// are accepted with a warning; the last value wins while first-seen action order
// is preserved for binding conflict resolution. An optional [remote] section
// accepts enabled/remember/hosts with TOML-style true/false values.
func Parse(r io.Reader) (domain.Config, []domain.Warning, error) {
	cfg := domain.Defaults()
	var warnings []domain.Warning
	seenBindingKeys := make(map[string]bool)
	seenFloatingKeys := make(map[string]bool)
	seenCopyKeys := make(map[string]bool)
	seenPaletteKeys := make(map[string]bool)
	seenNavKeys := make(map[string]bool)
	seenTabsKeys := make(map[string]bool)
	seenRemoteKeys := make(map[string]bool)
	section := ""

	scanner := bufio.NewScanner(r)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(strings.TrimSuffix(scanner.Text(), "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = stripInlineComment(line)
		if line == "" {
			continue
		}

		if matches := sectionPattern.FindStringSubmatch(line); matches != nil {
			name := matches[1]
			if name == "remote" {
				section = "remote"
			} else {
				section = "unknown:" + name
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("unknown section %q", name)})
			}
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			warnings = append(warnings, domain.Warning{Line: lineNo, Msg: "missing '='"})
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			warnings = append(warnings, domain.Warning{Line: lineNo, Msg: "missing key"})
			continue
		}

		if strings.HasPrefix(section, "unknown:") {
			continue
		}
		if section == "remote" {
			var remoteWarnings []domain.Warning
			cfg.Remote, remoteWarnings = parseRemoteKey(cfg.Remote, seenRemoteKeys, key, value, lineNo)
			warnings = append(warnings, remoteWarnings...)
			continue
		}

		switch {
		case key == "theme":
			mode, ok := parseTheme(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid theme %q", value)})
				continue
			}
			cfg.Theme = mode
		case key == "theme.palette":
			on, ok := parseOnOff(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid theme.palette %q", value)})
				continue
			}
			cfg.ThemePalette = on
		case key == "theme.accent":
			accent, warning := parseThemeAccent(value)
			cfg.ThemeAccent = accent
			if warning != "" {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: warning})
			}
		case key == "bar.top-right":
			command, ok := parseBarCommand(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid bar.top-right %q", value)})
				continue
			}
			cfg.Bar.TopRight = command
		case key == "bar.bottom-right":
			command, ok := parseBarCommand(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid bar.bottom-right %q", value)})
				continue
			}
			cfg.Bar.BottomRight = command
		case key == "bar.interval":
			interval, err := time.ParseDuration(value)
			if err != nil {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid bar.interval %q", value)})
				continue
			}
			if interval < domain.MinBarInterval {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("bar.interval below minimum %q", domain.MinBarInterval)})
				interval = domain.MinBarInterval
			}
			cfg.Bar.Interval = interval
		case key == "palette.anchor":
			warnings = warnDuplicateKey(warnings, seenPaletteKeys, key, lineNo)
			if strings.EqualFold(strings.TrimSpace(value), "auto") {
				cfg.Palette = domain.PaletteConfig{}
				continue
			}
			anchor, ok := domain.ParseAnchor(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid palette.anchor %q", value)})
				continue
			}
			cfg.Palette = domain.PaletteConfig{Anchor: anchor, AnchorSet: true}
		case key == "nav.overflow-tabs":
			warnings = warnDuplicateKey(warnings, seenNavKeys, key, lineNo)
			on, ok := parseOnOff(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid nav.overflow-tabs %q", value)})
				continue
			}
			cfg.Nav.OverflowTabs = on
		case key == "nav.overflow-sessions":
			warnings = warnDuplicateKey(warnings, seenNavKeys, key, lineNo)
			on, ok := parseOnOff(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid nav.overflow-sessions %q", value)})
				continue
			}
			cfg.Nav.OverflowSessions = on
		case key == "tabs.terminal-title":
			warnings = warnDuplicateKey(warnings, seenTabsKeys, key, lineNo)
			on, ok := parseOnOff(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid tabs.terminal-title %q", value)})
				continue
			}
			cfg.Tabs.TerminalTitle = on
		case key == "copy.word-separators":
			warnings = warnDuplicateKey(warnings, seenCopyKeys, key, lineNo)
			separators, ok := parseConfigString(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid copy.word-separators %q", value)})
				continue
			}
			cfg.Copy.WordSeparators = separators
		case key == "floating.command":
			warnings = warnDuplicateKey(warnings, seenFloatingKeys, key, lineNo)
			command, ok := parseBarCommand(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid floating.command %q", value)})
				continue
			}
			cfg.Floating.Command = command
		case key == "floating.width":
			warnings = warnDuplicateKey(warnings, seenFloatingKeys, key, lineNo)
			width, ok := parsePercentage(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid floating.width %q", value)})
				continue
			}
			cfg.Floating.Width = width
		case key == "floating.height":
			warnings = warnDuplicateKey(warnings, seenFloatingKeys, key, lineNo)
			height, ok := parsePercentage(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid floating.height %q", value)})
				continue
			}
			cfg.Floating.Height = height
		case strings.HasPrefix(key, "code."):
			codeKey := strings.TrimPrefix(key, "code.")
			if codeKey == "" {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: "missing code slug"})
				continue
			}
			if _, exists := cfg.Codes[codeKey]; exists {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("duplicate key %q", key)})
			}
			cfg.Codes[codeKey] = value
		case key == "snapshot.restore_processes":
			var processWarnings []domain.Warning
			cfg.Snapshot.RestoreProcesses, processWarnings = parseProcessList(value, lineNo)
			warnings = append(warnings, processWarnings...)
			cfg.Snapshot.RestoreProcessesSet = true
		default:
			if seenBindingKeys[key] {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("duplicate key %q", key)})
				updateBindingEntry(cfg.BindingEntries, key, value)
			} else {
				seenBindingKeys[key] = true
				cfg.BindingEntries = append(cfg.BindingEntries, domain.ConfigEntry{Key: key, Value: value})
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return cfg, warnings, err
	}
	return cfg, warnings, nil
}

// Load reads path and parses it. Missing config files are treated as defaults.
func Load(path string) (domain.Config, []domain.Warning, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return domain.Defaults(), nil, nil
	}
	if err != nil {
		return domain.Defaults(), nil, err
	}
	cfg, warnings, parseErr := Parse(f)
	if closeErr := f.Close(); parseErr == nil && closeErr != nil {
		return cfg, warnings, closeErr
	}
	return cfg, warnings, parseErr
}

// Watch polls path for mtime/size changes every two seconds and calls onChange
// with each successfully loaded config. Missing files are allowed and reload to
// defaults when the watched state changes.
func Watch(ctx context.Context, clock ports.Clock, path string, onChange func(domain.Config, []domain.Warning)) error {
	last, err := fileStamp(path)
	lastErrMsg := ""
	if err != nil {
		lastErrMsg = err.Error()
		onChange(domain.Defaults(), []domain.Warning{{Msg: lastErrMsg}})
	}
	timer := clock.NewTimer(pollInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C():
			current, statErr := fileStamp(path)
			if statErr != nil {
				if statErr.Error() != lastErrMsg {
					lastErrMsg = statErr.Error()
					onChange(domain.Defaults(), []domain.Warning{{Msg: lastErrMsg}})
				}
			} else if current != last || lastErrMsg != "" {
				lastErrMsg = ""
				last = current
				cfg, warnings, err := Load(path)
				if err != nil {
					cfg = domain.Defaults()
					warnings = append(warnings, domain.Warning{Msg: err.Error()})
				}
				onChange(cfg, warnings)
			}
			timer.Reset(pollInterval)
		}
	}
}

type stamp struct {
	modTime time.Time
	size    int64
	exists  bool
}

func fileStamp(path string) (stamp, error) {
	st, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return stamp{}, nil
	}
	if err != nil {
		return stamp{}, err
	}
	return stamp{modTime: st.ModTime(), size: st.Size(), exists: true}, nil
}

func warnDuplicateKey(warnings []domain.Warning, seen map[string]bool, key string, lineNo int) []domain.Warning {
	if seen[key] {
		warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("duplicate key %q", key)})
	}
	seen[key] = true
	return warnings
}

func updateBindingEntry(entries []domain.ConfigEntry, key, value string) {
	for i := range entries {
		if entries[i].Key == key {
			entries[i].Value = value
			return
		}
	}
}

func parseProcessList(value string, lineNo int) ([]string, []domain.Warning) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	warnings := make([]domain.Warning, 0)
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		if !processNamePattern.MatchString(item) {
			warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid snapshot restore process %q", item)})
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out, warnings
}

func parsePercentage(value string) (int, bool) {
	if !percentagePattern.MatchString(value) {
		return 0, false
	}
	percentage, err := strconv.Atoi(strings.TrimSuffix(value, "%"))
	return percentage, err == nil && percentage >= 1 && percentage <= 100
}

func parseOnOff(value string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "on":
		return true, true
	case "off":
		return false, true
	default:
		return false, false
	}
}

func parseBarCommand(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		unquoted, err := strconv.Unquote(value)
		if err != nil {
			return value, false
		}
		return unquoted, true
	}
	return value, true
}

func parseConfigString(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", true
	}
	startsQuoted := strings.HasPrefix(value, `"`)
	endsQuoted := strings.HasSuffix(value, `"`)
	if startsQuoted != endsQuoted {
		return "", false
	}
	if !startsQuoted {
		return value, true
	}
	unquoted, err := strconv.Unquote(value)
	if err != nil {
		return "", false
	}
	return unquoted, true
}

func parseTheme(value string) (domain.ThemeMode, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "auto", "":
		return domain.ThemeAuto, true
	case "dark":
		return domain.ThemeDark, true
	case "light":
		return domain.ThemeLight, true
	default:
		return domain.ThemeAuto, false
	}
}

func stripInlineComment(line string) string {
	inQuote := false
	escaped := false
	for i, r := range line {
		if escaped {
			escaped = false
			continue
		}
		if inQuote && r == '\\' {
			escaped = true
			continue
		}
		if r == '"' {
			inQuote = !inQuote
			continue
		}
		if !inQuote && r == '#' && (i == 0 || isSpace(rune(line[i-1]))) {
			return strings.TrimSpace(line[:i])
		}
	}
	return line
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\r' || r == '\n'
}
