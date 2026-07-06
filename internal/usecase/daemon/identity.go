package daemon

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"
)

var fallbackStableIDCounter atomic.Uint64

func newStableID(prefix string) (string, error) {
	var b [16]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return "", fmt.Errorf("generate stable id: %w", err)
	}
	enc := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:])
	return prefix + "_" + strings.ToLower(enc), nil
}

func (d *Daemon) newTabPaneStableIDs() (string, string, error) {
	tabStableID, err := newStableID("t")
	if err != nil {
		return "", "", fmt.Errorf("daemon: generating tab identity: %w", err)
	}
	paneStableID, err := newStableID("p")
	if err != nil {
		return "", "", fmt.Errorf("daemon: generating pane identity: %w", err)
	}
	return tabStableID, paneStableID, nil
}

func fallbackStableID(prefix string) string {
	id, err := newStableID(prefix)
	if err != nil {
		return fmt.Sprintf("%s_fallback_%x_%x", prefix, time.Now().UnixNano(), fallbackStableIDCounter.Add(1))
	}
	return id
}

func escapeVEVComponent(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		const hex = "0123456789ABCDEF"
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&0x0f])
	}
	return b.String()
}
