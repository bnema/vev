package config

import (
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestParseAttachmentCacheConfig(t *testing.T) {
	for _, tt := range []struct {
		name     string
		input    string
		want     domain.AttachmentCacheConfig
		warnings int
	}{
		{"default", "", domain.DefaultAttachmentCacheConfig(), 0},
		{"enabled", "remote.attachment-cache = on", domain.AttachmentCacheConfig{Enabled: true, Capacity: 8}, 0},
		{"disabled", "remote.attachment-cache = OFF", domain.AttachmentCacheConfig{Enabled: false, Capacity: 8}, 0},
		{"capacity", "remote.attachment-cache-capacity = 4", domain.AttachmentCacheConfig{Enabled: true, Capacity: 4}, 0},
		{"idle timeout", "remote.attachment-cache-idle-timeout = 30s", domain.AttachmentCacheConfig{Enabled: true, Capacity: 8, IdleTimeout: 30 * time.Second}, 0},
		{"idle timeout off", "remote.attachment-cache-idle-timeout = Off", domain.DefaultAttachmentCacheConfig(), 0},
		{"all keys", "remote.attachment-cache = on\nremote.attachment-cache-capacity = 2\nremote.attachment-cache-idle-timeout = 90s", domain.AttachmentCacheConfig{Enabled: true, Capacity: 2, IdleTimeout: 90 * time.Second}, 0},
		{"malformed boolean", "remote.attachment-cache = sometimes", domain.DefaultAttachmentCacheConfig(), 1},
		{"malformed capacity", "remote.attachment-cache-capacity = eight", domain.DefaultAttachmentCacheConfig(), 1},
		{"zero capacity", "remote.attachment-cache-capacity = 0", domain.DefaultAttachmentCacheConfig(), 1},
		{"negative capacity", "remote.attachment-cache-capacity = -3", domain.DefaultAttachmentCacheConfig(), 1},
		{"malformed timeout", "remote.attachment-cache-idle-timeout = forever", domain.DefaultAttachmentCacheConfig(), 1},
		{"zero timeout", "remote.attachment-cache-idle-timeout = 0s", domain.DefaultAttachmentCacheConfig(), 1},
		{"negative timeout", "remote.attachment-cache-idle-timeout = -5s", domain.DefaultAttachmentCacheConfig(), 1},
		{"overflow timeout", "remote.attachment-cache-idle-timeout = 999999999999999h", domain.DefaultAttachmentCacheConfig(), 1},
		{"duplicate", "remote.attachment-cache-capacity = 2\nremote.attachment-cache-capacity = 3", domain.AttachmentCacheConfig{Enabled: true, Capacity: 3}, 1},
		{"invalid keeps previous", "remote.attachment-cache-capacity = 4\nremote.attachment-cache-capacity = broken", domain.AttachmentCacheConfig{Enabled: true, Capacity: 4}, 2},
		{"disabled with values", "remote.attachment-cache = off\nremote.attachment-cache-capacity = 2\nremote.attachment-cache-idle-timeout = 1m", domain.AttachmentCacheConfig{Enabled: false, Capacity: 2, IdleTimeout: time.Minute}, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.AttachmentCache)
			require.Len(t, warnings, tt.warnings)
			require.Empty(t, cfg.BindingEntries)
		})
	}
}
