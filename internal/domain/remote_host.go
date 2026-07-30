package domain

import (
	"fmt"
	"strings"
	"unicode"
)

// RemoteHost is a merged remote host entry with its source markers.
type RemoteHost struct {
	Target  string
	Pinned  bool
	Learned bool
}

// ValidateRemoteHostTarget rejects empty targets, surrounding/internal whitespace,
// and control characters. SSH target grammar is left to OpenSSH.
func ValidateRemoteHostTarget(target string) error {
	if target == "" {
		return fmt.Errorf("remote host target is empty")
	}
	if strings.TrimSpace(target) != target {
		return fmt.Errorf("remote host target %q has surrounding whitespace", target)
	}
	for _, r := range target {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("remote host target %q contains whitespace or control characters", target)
		}
	}
	return nil
}

// UniqueRemoteHostTargets returns a de-duplicated copy preserving first occurrence order.
func UniqueRemoteHostTargets(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
