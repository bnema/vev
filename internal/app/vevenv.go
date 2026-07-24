package app

import "strings"

type vevEnv struct {
	session string
	tab     string
	pane    string
}

// parseVEVEnv strictly parses the identity injected into pane PTYs. All three
// unique fields are required and components use the daemon's uppercase %XX
// escaping.
func parseVEVEnv(value string) (vevEnv, bool) {
	if value == "" {
		return vevEnv{}, false
	}
	var out vevEnv
	seen := make(map[string]bool, 3)
	for part := range strings.SplitSeq(value, ",") {
		key, encoded, found := strings.Cut(part, "=")
		if !found || seen[key] {
			return vevEnv{}, false
		}
		decoded, ok := unescapeVEVComponent(encoded)
		if !ok || decoded == "" {
			return vevEnv{}, false
		}
		switch key {
		case "session":
			out.session = decoded
		case "tab":
			out.tab = decoded
		case "pane":
			out.pane = decoded
		default:
			return vevEnv{}, false
		}
		seen[key] = true
	}
	if len(seen) != 3 || out.session == "" || out.tab == "" || out.pane == "" {
		return vevEnv{}, false
	}
	return out, true
}

func unescapeVEVComponent(value string) (string, bool) {
	var out strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] != '%' {
			if !isUnescapedVEVByte(value[i]) {
				return "", false
			}
			out.WriteByte(value[i])
			continue
		}
		if i+2 >= len(value) {
			return "", false
		}
		hi, highOK := upperHexValue(value[i+1])
		lo, lowOK := upperHexValue(value[i+2])
		if !highOK || !lowOK {
			return "", false
		}
		out.WriteByte(hi<<4 | lo)
		i += 2
	}
	return out.String(), true
}

func isUnescapedVEVByte(value byte) bool {
	return value >= 'A' && value <= 'Z' ||
		value >= 'a' && value <= 'z' ||
		value >= '0' && value <= '9' ||
		value == '.' || value == '_' || value == '-'
}

func upperHexValue(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}
