package daemon

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type catalogCacheStub struct {
	load  func() ([]ports.RemoteCatalogCacheEntry, error)
	store func([]ports.RemoteCatalogCacheEntry) error
}

func (s catalogCacheStub) Load() ([]ports.RemoteCatalogCacheEntry, error) {
	if s.load == nil {
		return nil, nil
	}
	return s.load()
}

func (s catalogCacheStub) Store(entries []ports.RemoteCatalogCacheEntry) error {
	if s.store == nil {
		return nil
	}
	return s.store(entries)
}

func TestRemoteCatalogCacheStartup(t *testing.T) {
	now := time.Unix(0, 42)
	tests := []struct {
		name       string
		load       func() ([]ports.RemoteCatalogCacheEntry, error)
		wantCache  map[string]ports.RemoteCatalogCacheEntry
		wantStatus map[string]remoteHostStatus
	}{
		{
			name: "loads cached entries",
			load: func() ([]ports.RemoteCatalogCacheEntry, error) {
				return []ports.RemoteCatalogCacheEntry{{Host: "arch", FetchedAt: now, Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "running"}}}}, nil
			},
			wantCache:  map[string]ports.RemoteCatalogCacheEntry{"arch": {Host: "arch", FetchedAt: now, Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "running"}}}},
			wantStatus: map[string]remoteHostStatus{"arch": remoteHostCached},
		},
		{
			name: "invalid cache starts empty",
			load: func() ([]ports.RemoteCatalogCacheEntry, error) {
				return nil, errors.New("invalid cache")
			},
			wantCache:  map[string]ports.RemoteCatalogCacheEntry{},
			wantStatus: map[string]remoteHostStatus{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), WithRemoteDiscovery(nil, nil, catalogCacheStub{load: tt.load}, nil, ports.RemoteTransportUDP))

			d.remoteCatalog.mu.Lock()
			defer d.remoteCatalog.mu.Unlock()
			require.Equal(t, tt.wantCache, d.remoteCatalog.cache)
			require.Equal(t, tt.wantStatus, d.remoteCatalog.status)
		})
	}
}

func TestRemoteCatalogCacheStoreSerializesLockFreeIO(t *testing.T) {
	var d *Daemon
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var active, maxActive int
	var activeMu sync.Mutex

	cache := catalogCacheStub{store: func(entries []ports.RemoteCatalogCacheEntry) error {
		if !d.mu.TryLock() {
			return errors.New("daemon lock held during cache I/O")
		}
		d.mu.Unlock()
		if !d.remoteCatalog.mu.TryLock() {
			return errors.New("remote catalog lock held during cache I/O")
		}
		d.remoteCatalog.mu.Unlock()

		activeMu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		activeMu.Unlock()
		entered <- struct{}{}
		<-release
		activeMu.Lock()
		active--
		activeMu.Unlock()
		return nil
	}}
	d = New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), WithRemoteDiscovery(nil, nil, cache, nil, ports.RemoteTransportUDP))
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.cache["arch"] = ports.RemoteCatalogCacheEntry{Host: "arch", FetchedAt: time.Unix(0, 1), Sessions: []ports.RemoteCatalogSession{{Name: "one", State: "running"}}}
	d.remoteCatalog.mu.Unlock()

	done := make(chan struct{}, 2)
	go func() { d.persistRemoteCatalogCache(); done <- struct{}{} }()
	<-entered
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.cache["arch"] = ports.RemoteCatalogCacheEntry{Host: "arch", FetchedAt: time.Unix(0, 2), Sessions: []ports.RemoteCatalogSession{{Name: "two", State: "running"}}}
	d.remoteCatalog.mu.Unlock()
	go func() { d.persistRemoteCatalogCache(); done <- struct{}{} }()

	select {
	case <-entered:
		t.Fatal("concurrent cache stores were not serialized")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	<-entered
	<-done
	<-done

	activeMu.Lock()
	defer activeMu.Unlock()
	require.Equal(t, 1, maxActive)
}
