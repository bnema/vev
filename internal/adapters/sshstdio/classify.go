package sshstdio

import (
	"strings"

	"github.com/bnema/vev/internal/domain"
)

// ClassifyStderr maps captured ssh client stderr to a typed remote failure
// kind so the user sees why a host is unreachable instead of a generic
// connection failure. Unrecognized text is RemoteFailureNone; the raw text is
// never surfaced.
func ClassifyStderr(stderr string) domain.RemoteFailureKind {
	text := strings.ToLower(stderr)
	switch {
	case strings.Contains(text, "host key verification failed"),
		strings.Contains(text, "remote host identification has changed"):
		return domain.RemoteFailureTrust
	case strings.Contains(text, "permission denied"),
		strings.Contains(text, "unable to authenticate"),
		strings.Contains(text, "authentication required"),
		strings.Contains(text, "too many authentication failures"):
		return domain.RemoteFailureAuthentication
	case strings.Contains(text, "timed out"):
		return domain.RemoteFailureTimeout
	default:
		return domain.RemoteFailureNone
	}
}
