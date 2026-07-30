package config

import (
	"fmt"
	"strings"

	"github.com/bnema/vev/internal/domain"
)

func parseRemoteKey(remote domain.RemoteConfig, seen map[string]bool, key, value string, lineNo int) (domain.RemoteConfig, []domain.Warning, error) {
	var warnings []domain.Warning
	switch key {
	case "enabled", "remember":
		warnings = warnDuplicateKey(warnings, seen, key, lineNo)
	case "hosts":
		return remote, warnings, fmt.Errorf("line %d: unsupported remote hosts assignment", lineNo)
	default:
		warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("unknown key %q", key)})
		return remote, warnings, nil
	}

	switch key {
	case "enabled":
		on, ok := parseTrueFalse(value)
		if !ok {
			warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid enabled %q", value)})
			return remote, warnings, nil
		}
		remote.Enabled = on
	case "remember":
		on, ok := parseTrueFalse(value)
		if !ok {
			warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid remember %q", value)})
			return remote, warnings, nil
		}
		remote.Remember = on
	}
	return remote, warnings, nil
}

func parseTrueFalse(value string) (bool, bool) {
	switch strings.TrimSpace(value) {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}
