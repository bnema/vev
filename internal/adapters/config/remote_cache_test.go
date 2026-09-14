package config

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseRemoteAttachmentCacheTTL(t *testing.T) {
	for _, tt := range []struct {
		name, input string
		want        time.Duration
		warnings    int
	}{
		{"default", "", 15 * time.Minute, 0},
		{"duration", "remote.attachment-cache-ttl = 30s", 30 * time.Second, 0},
		{"off", "remote.attachment-cache-ttl = OFF", -1, 0},
		{"negative disables", "remote.attachment-cache-ttl = -1s", -time.Second, 0},
		{"zero default", "remote.attachment-cache-ttl = 0", 15 * time.Minute, 0},
		{"invalid", "remote.attachment-cache-ttl = forever", 15 * time.Minute, 1},
		{"overflow", "remote.attachment-cache-ttl = 999999999999999h", 15 * time.Minute, 1},
		{"duplicate", "remote.attachment-cache-ttl = off\nremote.attachment-cache-ttl = 2m", 2 * time.Minute, 1},
		{"invalid keeps previous", "remote.attachment-cache-ttl = off\nremote.attachment-cache-ttl = broken", -1, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.RemoteAttachmentCacheTTL)
			require.Len(t, warnings, tt.warnings)
			require.Empty(t, cfg.BindingEntries)
		})
	}
}
