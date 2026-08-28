package ports

import (
	"context"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// RemoteHostStore persists pinned and learned remote host targets.
type RemoteHostStore interface {
	Hosts() (pinned, learned []string, err error)
	AddPinned(target string) error
	RemovePinned(target string) error
	Remember(target string) error
	Forget(target string) error
	Remove(target string) (deleted bool, err error)
}

// RemoteHostLearner records the validated remote target after an attach.
type RemoteHostLearner interface {
	RememberRemoteHost() error
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
