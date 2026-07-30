package app

import "strings"

// parseRemoteAttachTarget distinguishes remote attach syntax from ordinary
// local session names. Only values with a user@host prefix are remote; local
// names may still contain ':' when they do not contain '@'.
func parseRemoteAttachTarget(s string) (target, session string, ok bool) {
	if strings.Count(s, "@") != 1 {
		return "", "", false
	}
	at := strings.IndexByte(s, '@')
	if at <= 0 || at == len(s)-1 {
		return "", "", false
	}
	afterAt := s[at+1:]
	if strings.HasPrefix(afterAt, "[") {
		closeBracket := strings.IndexByte(afterAt, ']')
		if closeBracket <= 1 || strings.Contains(afterAt[closeBracket+1:], "]") {
			return "", "", false
		}
		suffix := afterAt[closeBracket+1:]
		switch {
		case suffix == "":
			return s, "", true
		case strings.HasPrefix(suffix, ":"):
			targetEnd := at + 1 + closeBracket + 1
			return s[:targetEnd], suffix[1:], true
		default:
			return "", "", false
		}
	}
	if strings.ContainsAny(afterAt, "[]") || strings.Count(afterAt, ":") > 1 {
		return "", "", false
	}
	colon := strings.LastIndexByte(afterAt, ':')
	if colon < 0 {
		return s, "", true
	}
	if colon == 0 {
		return "", "", false
	}
	return s[:at+1+colon], afterAt[colon+1:], true
}
