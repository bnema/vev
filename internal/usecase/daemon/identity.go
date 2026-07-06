package daemon

import (
	"crypto/rand"
	"encoding/base32"
	"io"
	"strings"
)

var stableIDRand io.Reader = rand.Reader

func newStableID() (string, error) {
	var b [16]byte
	if _, err := io.ReadFull(stableIDRand, b[:]); err != nil {
		return "", err
	}
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:])), nil
}

func mustTestStableID(prefix string) string {
	id, err := newStableID()
	if err != nil {
		return prefix + "-unknown"
	}
	return id
}
