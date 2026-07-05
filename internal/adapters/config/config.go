package config

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const pollInterval = 2 * time.Second

// Parse reads vev's flat action = value config format. Duplicate action keys
// are accepted with a warning; the last value wins while first-seen action order
// is preserved for binding conflict resolution.
func Parse(r io.Reader) (domain.Config, []domain.Warning, error) {
	cfg := domain.Defaults()
	var warnings []domain.Warning
	seenBindingKeys := make(map[string]bool)

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

		switch {
		case key == "theme":
			mode, ok := parseTheme(value)
			if !ok {
				warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid theme %q", value)})
				continue
			}
			cfg.Theme = mode
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

func updateBindingEntry(entries []domain.ConfigEntry, key, value string) {
	for i := range entries {
		if entries[i].Key == key {
			entries[i].Value = value
			return
		}
	}
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
	for i, r := range line {
		if r == '#' && (i == 0 || isSpace(rune(line[i-1]))) {
			return strings.TrimSpace(line[:i])
		}
	}
	return line
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\r' || r == '\n'
}
