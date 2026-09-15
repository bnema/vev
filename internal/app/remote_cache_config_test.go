package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRunAttachThreadsAttachmentCacheConfig(t *testing.T) {
	for _, remote := range []string{"", "remote.example"} {
		for _, tt := range []struct {
			name, text string
			want       domain.AttachmentCacheConfig
		}{
			{"default", "", domain.DefaultAttachmentCacheConfig()},
			{"configured", "remote.attachment-cache-capacity = 3\nremote.attachment-cache-idle-timeout = 42s", domain.AttachmentCacheConfig{Enabled: true, Capacity: 3, IdleTimeout: 42 * time.Second}},
			{"disabled", "remote.attachment-cache = off", domain.AttachmentCacheConfig{Enabled: false, Capacity: 8}},
		} {
			t.Run(remote+"/"+tt.name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "config")
				require.NoError(t, os.WriteFile(path, []byte(tt.text), 0600))
				factory := newRemoteDialerFactoryMock(t)
				if remote != "" {
					factory.EXPECT().DialerForRemote(remote, "", remoteadapter.TransportQUIC, mock.Anything).Return(namedDialer{name: "remote"}, nil).Once()
				}
				calls := 0
				err := runAttachWithDeps(context.Background(), protocol.IntentNew, "work", remote, "", nil, runAttachDeps{
					configPath:          func() string { return path },
					remoteDialerFactory: factory.DialerForRemote,
					runClient: func(_ context.Context, deps client.Dependencies, _ client.AttachRequest) error {
						calls++
						require.Equal(t, tt.want, deps.AttachmentCache)
						return nil
					},
				})
				require.NoError(t, err)
				require.Equal(t, 1, calls)
			})
		}
	}
}
