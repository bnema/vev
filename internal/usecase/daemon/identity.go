package daemon

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"io"
	"strings"
)

func newStableID(prefix string) (string, error) {
	var b [16]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return "", fmt.Errorf("generate stable id: %w", err)
	}
	enc := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:])
	return prefix + "_" + strings.ToLower(enc), nil
}

func fallbackStableID(prefix string) string {
	id, err := newStableID(prefix)
	if err != nil {
		return prefix + "_unknown"
	}
	return id
}
