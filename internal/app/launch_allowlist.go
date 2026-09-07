package app

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

func remoteLaunchAllowlistFromEnv() (map[string]struct{}, bool, error) {
	raw, configured := os.LookupEnv(launchAllowedRemoteEndpointsEnv)
	if !configured {
		return nil, false, nil
	}
	allowed := make(map[string]struct{})
	for _, target := range strings.Split(raw, "\n") {
		if target == "" {
			continue
		}
		if err := domain.ValidateRemoteHostTarget(target); err != nil {
			return nil, true, errors.New("vev: invalid configured remote endpoint allowlist")
		}
		allowed[target] = struct{}{}
	}
	return allowed, true, nil
}

func allowlistedRemoteEndpoint(allowed map[string]struct{}, target string) bool {
	_, ok := allowed[target]
	return ok
}

type allowlistedRemoteHostStore struct {
	delegate ports.RemoteHostStore
	allowed  map[string]struct{}
}

func (s allowlistedRemoteHostStore) Hosts() (pinned, learned []string, err error) {
	pinned, learned, err = s.delegate.Hosts()
	return filterRemoteTargets(pinned, s.allowed), filterRemoteTargets(learned, s.allowed), err
}

func (s allowlistedRemoteHostStore) AddPinned(target string) error {
	if !allowlistedRemoteEndpoint(s.allowed, target) {
		return endpointNotConfiguredError()
	}
	return s.delegate.AddPinned(target)
}

func (s allowlistedRemoteHostStore) RemovePinned(target string) error {
	if !allowlistedRemoteEndpoint(s.allowed, target) {
		return endpointNotConfiguredError()
	}
	return s.delegate.RemovePinned(target)
}

func (s allowlistedRemoteHostStore) Remember(target string) error {
	if !allowlistedRemoteEndpoint(s.allowed, target) {
		return endpointNotConfiguredError()
	}
	return s.delegate.Remember(target)
}

func (s allowlistedRemoteHostStore) Forget(target string) error {
	if !allowlistedRemoteEndpoint(s.allowed, target) {
		return endpointNotConfiguredError()
	}
	return s.delegate.Forget(target)
}

func (s allowlistedRemoteHostStore) Remove(target string) (bool, error) {
	if !allowlistedRemoteEndpoint(s.allowed, target) {
		return false, endpointNotConfiguredError()
	}
	return s.delegate.Remove(target)
}

type allowlistedRemoteCatalogClient struct {
	delegate ports.RemoteCatalogClient
	allowed  map[string]struct{}
}

func (c allowlistedRemoteCatalogClient) List(ctx context.Context, target string) (catalogue.RemoteCatalog, error) {
	if !allowlistedRemoteEndpoint(c.allowed, target) {
		return catalogue.RemoteCatalog{}, endpointNotConfiguredError()
	}
	return c.delegate.List(ctx, target)
}

type allowlistedRemotePreviewClient struct {
	delegate ports.RemotePreviewClient
	allowed  map[string]struct{}
}

func (c allowlistedRemotePreviewClient) Preview(ctx context.Context, target domain.RemoteSessionTarget, width, height uint16) (protocol.RemotePreview, error) {
	if !allowlistedRemoteEndpoint(c.allowed, target.Endpoint) {
		return protocol.RemotePreview{}, endpointNotConfiguredError()
	}
	return c.delegate.Preview(ctx, target, width, height)
}

type allowlistedRemoteCatalogCache struct {
	delegate ports.RemoteCatalogCache
	allowed  map[string]struct{}
}

func (c allowlistedRemoteCatalogCache) Load() ([]catalogue.RemoteCatalogCacheEntry, error) {
	entries, err := c.delegate.Load()
	if err != nil {
		return nil, err
	}
	filtered := make([]catalogue.RemoteCatalogCacheEntry, 0, len(entries))
	for _, entry := range entries {
		if allowlistedRemoteEndpoint(c.allowed, entry.Host) {
			filtered = append(filtered, entry)
		}
	}
	return filtered, nil
}

func (c allowlistedRemoteCatalogCache) Store(entries []catalogue.RemoteCatalogCacheEntry) error {
	filtered := make([]catalogue.RemoteCatalogCacheEntry, 0, len(entries))
	for _, entry := range entries {
		if allowlistedRemoteEndpoint(c.allowed, entry.Host) {
			filtered = append(filtered, entry)
		}
	}
	return c.delegate.Store(filtered)
}

func filterRemoteTargets(targets []string, allowed map[string]struct{}) []string {
	filtered := make([]string, 0, len(targets))
	for _, target := range targets {
		if allowlistedRemoteEndpoint(allowed, target) {
			filtered = append(filtered, target)
		}
	}
	return filtered
}

func endpointNotConfiguredError() error {
	return &ports.UIError{Code: ports.UIErrEndpointNotConfigured}
}
