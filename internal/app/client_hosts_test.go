package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
)

type clientHostsTestDialer struct{ name string }

func (d clientHostsTestDialer) Dial(context.Context) (wire.Transport, error) {
	return nil, errors.New("client hosts test dialer is never dialed")
}

var _ wire.Dialer = clientHostsTestDialer{}

func clientHostsEndpointFactory(factory remoteDialerForTarget, allowed map[string]struct{}, restricted bool, environment func(string) []string) clientEndpointFactory {
	return clientEndpointFactory{
		factory: factory, mode: remoteadapter.TransportUDP, environment: environment,
		allowed: allowed, restricted: restricted, log: slog.New(slog.DiscardHandler),
	}
}

func TestClientEndpointFactoryResolvesOneCarriagePerEndpoint(t *testing.T) {
	factory := newRemoteDialerFactoryMock(t)
	factory.EXPECT().DialerForRemote("arch", "", remoteadapter.TransportUDP, nil).Return(clientHostsTestDialer{name: "arch"}, nil).Once()
	factory.EXPECT().DialerForRemote("cache", "", remoteadapter.TransportUDP, nil).Return(clientHostsTestDialer{name: "cache"}, nil).Once()

	resolver := clientHostsEndpointFactory(factory.DialerForRemote, nil, false, func(endpoint string) []string {
		return []string{"ENDPOINT=" + endpoint}
	})
	first, err := resolver.ResolveEndpoint(context.Background(), "arch")
	require.NoError(t, err)
	require.NotNil(t, first.Dialer)
	require.Equal(t, []string{"ENDPOINT=arch"}, first.Environment)

	second, err := resolver.ResolveEndpoint(context.Background(), "cache")
	require.NoError(t, err)
	require.Equal(t, []string{"ENDPOINT=cache"}, second.Environment)
}

func TestClientEndpointFactoryRefusesEndpointsOutsideTheAllowlist(t *testing.T) {
	factory := newRemoteDialerFactoryMock(t)
	resolver := clientHostsEndpointFactory(factory.DialerForRemote, map[string]struct{}{"arch": {}}, true, nil)

	_, err := resolver.ResolveEndpoint(context.Background(), "other")
	require.ErrorContains(t, err, "not allowed")

	// A configured allowlist that lists nothing denies every endpoint, and the
	// refusal never reaches the dialer factory.
	resolver = clientHostsEndpointFactory(factory.DialerForRemote, map[string]struct{}{}, true, nil)
	_, err = resolver.ResolveEndpoint(context.Background(), "arch")
	require.ErrorContains(t, err, "not allowed")

	// Without a configured allowlist every valid endpoint resolves.
	factory.EXPECT().DialerForRemote("arch", "", remoteadapter.TransportUDP, nil).Return(clientHostsTestDialer{}, nil).Once()
	resolver = clientHostsEndpointFactory(factory.DialerForRemote, nil, false, nil)
	_, err = resolver.ResolveEndpoint(context.Background(), "arch")
	require.NoError(t, err)
}

func TestClientEndpointFactoryRejectsInvalidEndpoints(t *testing.T) {
	factory := newRemoteDialerFactoryMock(t)
	resolver := clientHostsEndpointFactory(factory.DialerForRemote, nil, false, nil)
	for _, endpoint := range []string{"", "bad\x1bhost", "spaces are not a host"} {
		_, err := resolver.ResolveEndpoint(context.Background(), endpoint)
		require.Error(t, err, "endpoint %q must be refused before a carriage is built", endpoint)
	}
}

func TestClientEndpointFactoryKeepsFactoryErrorsAuthoritative(t *testing.T) {
	factory := newRemoteDialerFactoryMock(t)
	want := errors.New("transport mode refused this endpoint")
	factory.EXPECT().DialerForRemote("arch", "", remoteadapter.TransportUDP, nil).Return(nil, want).Once()
	resolver := clientHostsEndpointFactory(factory.DialerForRemote, nil, false, nil)

	_, err := resolver.ResolveEndpoint(context.Background(), "arch")
	require.ErrorIs(t, err, want)
}

func TestClientEndpointFactoryHonoursContextCancellation(t *testing.T) {
	factory := newRemoteDialerFactoryMock(t)
	resolver := clientHostsEndpointFactory(factory.DialerForRemote, nil, false, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := resolver.ResolveEndpoint(ctx, "arch")
	require.ErrorIs(t, err, context.Canceled)
}

func TestClientEndpointFactoryKeepsNilEnvironmentDistinct(t *testing.T) {
	factory := newRemoteDialerFactoryMock(t)
	factory.EXPECT().DialerForRemote("arch", "", remoteadapter.TransportUDP, nil).Return(clientHostsTestDialer{}, nil).Once()
	factory.EXPECT().DialerForRemote("cache", "", remoteadapter.TransportUDP, nil).Return(clientHostsTestDialer{}, nil).Once()
	resolver := clientHostsEndpointFactory(factory.DialerForRemote, nil, false, func(endpoint string) []string {
		if endpoint == "cache" {
			return []string{}
		}
		return nil
	})

	absent, err := resolver.ResolveEndpoint(context.Background(), "arch")
	require.NoError(t, err)
	require.Nil(t, absent.Environment)

	empty, err := resolver.ResolveEndpoint(context.Background(), "cache")
	require.NoError(t, err)
	require.NotNil(t, empty.Environment)
	require.Empty(t, empty.Environment)
}

// memorySeedCache is a durable-cache stand-in that records writes, so a test
// can prove the client monitor never writes the daemon's snapshot file.
type memorySeedCache struct {
	mu      sync.Mutex
	entries []catalogue.RemoteCatalogCacheEntry
	loads   int
	stores  int
	loadErr error
}

func (c *memorySeedCache) Load() ([]catalogue.RemoteCatalogCacheEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loads++
	if c.loadErr != nil {
		return nil, c.loadErr
	}
	return append([]catalogue.RemoteCatalogCacheEntry(nil), c.entries...), nil
}

func (c *memorySeedCache) Store(entries []catalogue.RemoteCatalogCacheEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stores++
	c.entries = append([]catalogue.RemoteCatalogCacheEntry(nil), entries...)
	return nil
}

func (c *memorySeedCache) writeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stores
}

func (c *memorySeedCache) loadCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads
}

var _ ports.RemoteCatalogCache = (*memorySeedCache)(nil)

// TestSeededCatalogCacheNeverWritesTheDurableSnapshot pins the client cache
// contract: it seeds from the durable entries once and keeps every later write
// in its own memory, so a client monitor cannot race the daemon's writer.
func TestSeededCatalogCacheNeverWritesTheDurableSnapshot(t *testing.T) {
	seed := &memorySeedCache{entries: []catalogue.RemoteCatalogCacheEntry{{Host: "arch"}}}
	client := remoteadapter.NewSeededCatalogCache(seed)

	seeded, err := client.Load()
	require.NoError(t, err)
	require.Len(t, seeded, 1)
	require.Equal(t, "arch", seeded[0].Host)
	require.Equal(t, 1, seed.loadCount(), "the durable cache is read once")

	require.NoError(t, client.Store([]catalogue.RemoteCatalogCacheEntry{{Host: "cache"}}))
	require.Zero(t, seed.writeCount(), "the client cache must never write the durable snapshot")
	require.Equal(t, 1, seed.loadCount(), "a write suppresses the seed read")

	stored, err := client.Load()
	require.NoError(t, err)
	require.Equal(t, "cache", stored[0].Host)
}

func TestSeededCatalogCacheCopiesEntriesDefensively(t *testing.T) {
	seed := &memorySeedCache{entries: []catalogue.RemoteCatalogCacheEntry{{
		Host: "arch", Sessions: []catalogue.RemoteCatalogSession{{Name: "work", Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-1"}}}},
	}}}
	client := remoteadapter.NewSeededCatalogCache(seed)

	first, err := client.Load()
	require.NoError(t, err)
	first[0].Sessions[0].Tabs[0].ID = "mutated"
	first[0].Host = "mutated"

	second, err := client.Load()
	require.NoError(t, err)
	require.Equal(t, "arch", second[0].Host)
	require.Equal(t, "tab-1", second[0].Sessions[0].Tabs[0].ID)

	seed.mu.Lock()
	seed.entries[0].Host = "mutated"
	seed.mu.Unlock()
	third, err := client.Load()
	require.NoError(t, err)
	require.Equal(t, "arch", third[0].Host, "the client cache is not a live view of the durable file")
}

func TestSeededCatalogCacheReportsASeedFailureOnce(t *testing.T) {
	want := errors.New("durable cache is unreadable")
	seed := &memorySeedCache{loadErr: want}
	client := remoteadapter.NewSeededCatalogCache(seed)

	_, err := client.Load()
	require.ErrorIs(t, err, want)
	_, err = client.Load()
	require.ErrorIs(t, err, want)
	require.Equal(t, 1, seed.loadCount(), "a broken seed is reported, never retried")

	var nilCache *remoteadapter.SeededCatalogCache
	entries, err := nilCache.Load()
	require.NoError(t, err)
	require.Empty(t, entries)
	require.NoError(t, nilCache.Store(nil))
}
