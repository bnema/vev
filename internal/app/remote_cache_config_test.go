package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRunAttachThreadsRemoteCacheConfig(t *testing.T) {
	for _, remote := range []string{"", "remote.example"} {
		for _, tt := range []struct {
			name, text string
			want       time.Duration
		}{
			{"default", "", client.DefaultAttachmentCacheTTL},
			{"configured", "remote.attachment-cache-ttl = 42s", 42 * time.Second},
			{"disabled", "remote.attachment-cache-ttl = off", -1},
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
						require.Equal(t, tt.want, deps.AttachmentCacheTTL)
						return nil
					},
				})
				require.NoError(t, err)
				require.Equal(t, 1, calls)
			})
		}
	}
}
