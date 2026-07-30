package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bnema/vev/internal/domain"
)

// ErrUnsupportedRemoteHosts indicates a legacy remote hosts assignment.
var ErrUnsupportedRemoteHosts = errors.New("unsupported remote hosts assignment")

func parseRemoteKey(remote domain.RemoteConfig, seen map[string]bool, key, value string, lineNo int) (domain.RemoteConfig, []domain.Warning, error) {
	var warnings []domain.Warning
	switch key {
	case "enabled":
		warnings = warnDuplicateKey(warnings, seen, key, lineNo)
		on, ok := parseTrueFalse(value)
		if !ok {
			warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid enabled %q", value)})
			return remote, warnings, nil
		}
		remote.Enabled = on
	case "remember":
		warnings = warnDuplicateKey(warnings, seen, key, lineNo)
		on, ok := parseTrueFalse(value)
		if !ok {
			warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("invalid remember %q", value)})
			return remote, warnings, nil
		}
		remote.Remember = on
	case "hosts":
		return remote, warnings, fmt.Errorf("line %d: %w", lineNo, ErrUnsupportedRemoteHosts)
	default:
		warnings = append(warnings, domain.Warning{Line: lineNo, Msg: fmt.Sprintf("unknown key %q", key)})
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
