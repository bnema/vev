package app

import "strings"

// parseRemoteAttachTarget distinguishes remote attach syntax from ordinary
// local session names. Only values with a user@host prefix are remote; local
// names may still contain ':' when they do not contain '@'.
func parseRemoteAttachTarget(s string) (target, session string, ok bool) {
	at := strings.LastIndexByte(s, '@')
	if at <= 0 || at == len(s)-1 {
		return "", "", false
	}
	afterAt := s[at+1:]
	colon := strings.LastIndexByte(afterAt, ':')
	if colon < 0 {
		return s, "", true
	}
	if colon == 0 {
		return "", "", false
	}
	return s[:at+1+colon], afterAt[colon+1:], true
}
