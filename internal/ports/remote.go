package ports

import (
	"context"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// RemoteHostStore persists pinned and learned remote host targets as
// registration records. Each record carries a persistent random incarnation
// so remove/re-add cycles are observable even when the endpoint string is
// unchanged.
type RemoteHostStore interface {
	Hosts() (pinned, learned []domain.RemoteRegistration, err error)
	AddPinned(target string) error
	RemovePinned(target string) error
	Remember(target string) error
	Forget(target string) error
	Remove(target string) (deleted bool, err error)
}

// RemoteCatalogClient fetches a versioned session catalogue from a remote host.
type RemoteCatalogClient interface {
	List(ctx context.Context, target string) (catalogue.RemoteCatalog, error)
}

// RemotePreviewClient fetches one bounded, exact-target viewport.
type RemotePreviewClient interface {
	Preview(ctx context.Context, target domain.RemoteSessionTarget, width, height uint16) (protocol.RemotePreview, error)
}

// RemoteCatalogCache persists complete remote discovery snapshots independently
// from the remote host registry.
type RemoteCatalogCache interface {
	Load() ([]catalogue.RemoteCatalogCacheEntry, error)
	Store([]catalogue.RemoteCatalogCacheEntry) error
}

// RemoteEndpointBinding is one endpoint's resolved client carriage: the typed
// dialer every session on that endpoint attaches through, and the environment
// to advertise when the remote environment is daemon-owned. It never carries
// session identity, a resume token, or discovered inventory.
type RemoteEndpointBinding struct {
	Dialer      ClientDialer
	Environment []string
}

// RemoteEndpointFactory resolves one endpoint into its binding. The composition
// root implements it from the configured transport mode, the launch allowlist,
// and the endpoint environment; consumers never inspect those policies.
type RemoteEndpointFactory interface {
	ResolveEndpoint(ctx context.Context, endpoint string) (RemoteEndpointBinding, error)
}

// ClientHostRegistry resolves and caches the endpoint bindings one runner
// reuses across handoffs. Remote discovery remains daemon-owned.
type ClientHostRegistry interface {
	ResolveEndpoint(ctx context.Context, endpoint string) (RemoteEndpointBinding, error)
}
