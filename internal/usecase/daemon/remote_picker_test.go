package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
)

const remotePickerReceiveTimeout = time.Second

func receiveRemotePicker[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed unexpectedly while waiting for %s", what)
		}
		return value
	case <-time.After(remotePickerReceiveTimeout):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func waitRemotePickerClose(ch <-chan struct{}, what string) error {
	select {
	case _, ok := <-ch:
		if ok {
			return fmt.Errorf("received signal instead of closure while waiting for %s", what)
		}
		return nil
	case <-time.After(remotePickerReceiveTimeout):
		return fmt.Errorf("timed out waiting for %s", what)
	}
}

func receiveRemotePickerClose(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	require.NoError(t, waitRemotePickerClose(ch, what))
}

func newRemotePickerDaemon(store ports.RemoteHostStore) *Daemon {
	return New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithRemoteDiscovery(store, nil, nil, nil, ports.RemoteTransportUDP))
}

func TestRemotePickerStoppedRowsUseCanonicalStateAndSafeSelection(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle}
	session := ports.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "work", State: ports.RemoteCatalogSessionDown,
		Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
	}
	view := remotePickerView(key, session, remoteHostFresh, time.Unix(100, 0))
	require.True(t, view.Stopped)
	require.NotNil(t, view.RemoteTarget)
	require.True(t, view.RemoteTarget.Stopped)
	require.Equal(t, picker.RemoteRestart, view.RemoteActivation, "structured stopped rows have an explicit safe restore target")
	require.Equal(t, "stopped — Enter to restart", view.RemoteDetail)

}

func TestRemotePickerClampsStoppedOrdinalTabCount(t *testing.T) {
	tabs := make([]ports.RemoteCatalogTab, math.MaxUint16+1)
	tabs[0] = ports.RemoteCatalogTab{Name: "main"}
	view := remotePickerView(domain.RemoteSessionKey{Host: "arch", Name: "work"}, ports.RemoteCatalogSession{
		LifecycleID: remoteLifecycleForTest(), Name: "work", State: "down", Tabs: tabs,
	}, remoteHostFresh, time.Unix(100, 0))

	require.NotNil(t, view.RemoteTarget)
	require.Equal(t, uint16(math.MaxUint16), view.RemoteTarget.StoppedTab.ExpectedCount)
}

func TestRemotePickerUsesCompactUpDetailRegardlessOfAttachment(t *testing.T) {
	for _, attached := range []bool{false, true} {
		view := remotePickerView(domain.RemoteSessionKey{Host: "remote", Name: "work"}, ports.RemoteCatalogSession{
			LifecycleID: remoteLifecycleForTest(), Name: "work", State: "up", Attached: attached,
			Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0}},
		}, remoteHostFresh, time.Now())
		require.Equal(t, "up", view.RemoteDetail)
	}
}

func TestRemotePickerScopesSameLifecycleBytesToEachEndpoint(t *testing.T) {
	store := &remoteRefreshHostStore{hosts: []string{"remote", "vev@remote"}}
	d := newRemotePickerDaemon(store)
	lifecycle := remoteLifecycleForTest()
	session := ports.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "work", State: "up",
		Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
	}
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{
		{Host: "remote", FetchedAt: time.Unix(100, 0), Sessions: []ports.RemoteCatalogSession{session}},
		{Host: "vev@remote", FetchedAt: time.Unix(100, 0), Sessions: []ports.RemoteCatalogSession{session}},
	})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["remote"] = remoteHostFresh
	d.remoteCatalog.status["vev@remote"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()

	views, _ := d.pickerViews(nil, nil)
	var targets []*domain.RemoteSessionTarget
	for _, view := range views {
		if view.RemoteTarget != nil {
			targets = append(targets, view.RemoteTarget)
		}
	}
	require.Len(t, targets, 2)
	require.Equal(t, "remote", targets[0].Endpoint)
	require.Equal(t, "vev@remote", targets[1].Endpoint)
}

func TestRemotePickerCatalogExpiresAtAttachTTL(t *testing.T) {
	now := time.Unix(100, 0)
	clock := &remotePickerClock{now: now}
	d := newTestDaemon(t, nil, clock)
	lifecycle := remoteLifecycleForTest()
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: now.Add(-remoteCatalogAttachTTL), Sessions: []ports.RemoteCatalogSession{{
			LifecycleID: lifecycle, Name: "work", State: "up", Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
		}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()

	entries := d.remoteCatalogSnapshot()
	require.Len(t, entries, 1)
	require.Equal(t, remoteHostStale, entries[0].status)
	target := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	require.False(t, d.remoteCatalogTargetReady(target), "attachment must reject a catalogue exactly at the TTL boundary")
}

func TestRemotePickerTargetReadyExpiresAfterPickerBuildWithoutSnapshot(t *testing.T) {
	now := time.Unix(100, 0)
	clock := &remotePickerClock{now: now}
	d := newTestDaemon(t, nil, clock)
	lifecycle := remoteLifecycleForTest()
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: now, Sessions: []ports.RemoteCatalogSession{{
			LifecycleID: lifecycle, Name: "work", State: "up", Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
		}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()

	model := d.newPickerModel(nil, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	target, ok := model.Selected()
	require.True(t, ok)
	require.NotNil(t, target.RemoteTarget)
	require.True(t, d.remoteCatalogTargetReady(*target.RemoteTarget), "fresh picker target must be ready before the attach TTL")

	clock.Advance(remoteCatalogAttachTTL)
	require.False(t, d.remoteCatalogTargetReady(*target.RemoteTarget), "handoff must reject a picker target whose catalog reached the attach TTL")
	d.remoteCatalog.mu.Lock()
	require.Equal(t, remoteHostStale, d.remoteCatalog.status["arch"])
	d.remoteCatalog.mu.Unlock()
}

func TestRemotePickerSelectsCatalogActiveTab(t *testing.T) {
	tests := []struct {
		name        string
		tabs        []ports.RemoteCatalogTab
		activeTabID string
		wantActive  int
		wantTabID   domain.TabStableID
	}{
		{
			name: "active first",
			tabs: []ports.RemoteCatalogTab{
				{ID: "tab-active", Index: 0, Name: "active"},
				{ID: "tab-last", Index: 1, Name: "last"},
			},
			activeTabID: "tab-active",
			wantActive:  0,
			wantTabID:   "tab-active",
		},
		{
			name: "active last",
			tabs: []ports.RemoteCatalogTab{
				{ID: "tab-first", Index: 0, Name: "first"},
				{ID: "tab-active", Index: 1, Name: "active"},
			},
			activeTabID: "tab-active",
			wantActive:  1,
			wantTabID:   "tab-active",
		},
		{
			name: "empty active tab ID falls back to first stable tab",
			tabs: []ports.RemoteCatalogTab{
				{ID: "tab-first", Index: 0, Name: "first"},
				{ID: "", Index: 1, Name: "malformed"},
			},
			wantActive: 0,
			wantTabID:  "tab-first",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := newRemotePickerDaemon(nil)
			lifecycle := remoteLifecycleForTest()
			d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
				Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{
					LifecycleID: lifecycle,
					Name:        "vive",
					State:       "up",
					Tabs:        test.tabs,
					ActiveTabID: test.activeTabID,
				}},
			}})
			d.remoteCatalog.mu.Lock()
			d.remoteCatalog.status["arch"] = remoteHostFresh
			d.remoteCatalog.mu.Unlock()

			views, _ := d.pickerViews(nil, nil)
			require.Len(t, views, 1)
			require.Equal(t, test.wantActive, views[0].Active)
			require.NotNil(t, views[0].RemoteTarget)
			require.Equal(t, test.wantTabID, views[0].RemoteTarget.LiveTabID)

			model := d.newPickerModel(nil, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
			target, ok := model.Selected()
			require.True(t, ok)
			require.NotNil(t, target.RemoteTarget)
			require.Equal(t, test.wantTabID, target.RemoteTarget.LiveTabID)
		})
	}
}

func TestRemotePickerAvailabilityUsesCachedRowsForFailures(t *testing.T) {
	tests := []struct {
		name   string
		status remoteHostStatus
		want   picker.RemoteAvailability
	}{
		{name: "cached", status: remoteHostCached, want: picker.RemoteCached},
		{name: "unreachable", status: remoteHostUnreachable, want: picker.RemoteStale},
		{name: "version mismatch", status: remoteHostVersionMismatch, want: picker.RemoteVersionMismatch},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := newRemotePickerDaemon(nil)
			d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
				Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "up"}},
			}})
			d.remoteCatalog.mu.Lock()
			d.remoteCatalog.status["arch"] = test.status
			d.remoteCatalog.mu.Unlock()

			views, _ := d.pickerViews(nil, nil)

			require.Len(t, views, 1)
			require.Equal(t, test.want, views[0].RemoteAvailability)
		})
	}
}

func TestRemotePickerStaleDetailUsesLastSuccessfulFetch(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	fetchedAt := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	failedAt := fetchedAt.Add(6 * time.Hour)
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: fetchedAt, Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "up"}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostUnreachable
	d.remoteCatalog.failure["arch"] = failedAt
	d.remoteCatalog.mu.Unlock()

	views, _ := d.pickerViews(nil, nil)

	require.Len(t, views, 1)
	require.Equal(t, "stale since "+fetchedAt.Format(time.RFC3339), views[0].RemoteDetail)
	require.NotContains(t, views[0].RemoteDetail, failedAt.Format(time.RFC3339), "failure time remains runtime-only and is not the freshness authority")
}

func TestRemotePickerNoCacheHostFailuresRemainVisible(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     remoteHostStatus
		want       picker.RemoteAvailability
		wantDetail string
	}{
		{name: "unreachable", status: remoteHostUnreachable, want: picker.RemoteStale, wantDetail: "unreachable"},
		{name: "version mismatch", status: remoteHostVersionMismatch, want: picker.RemoteVersionMismatch, wantDetail: "version mismatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := newRemotePickerDaemon(nil)
			d.remoteCatalog.mu.Lock()
			d.remoteCatalog.status["arch"] = test.status
			d.remoteCatalog.failure["arch"] = time.Unix(50, 0)
			d.remoteCatalog.mu.Unlock()

			views, _ := d.pickerViews(nil, nil)

			require.Len(t, views, 1)
			require.Equal(t, "arch", views[0].Name)
			require.Equal(t, test.want, views[0].RemoteAvailability)
			require.Equal(t, test.wantDetail, views[0].RemoteDetail)
			model := picker.New(views, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
			_, selectable := model.Selected()
			require.False(t, selectable)
		})
	}
}

type remoteRefreshHostStore struct {
	mu    sync.Mutex
	hosts []string
}

func (s *remoteRefreshHostStore) Hosts() ([]string, []string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.hosts...), nil, nil
}

func (s *remoteRefreshHostStore) set(hosts ...string) {
	s.mu.Lock()
	s.hosts = append([]string(nil), hosts...)
	s.mu.Unlock()
}

func (*remoteRefreshHostStore) AddPinned(string) error    { return nil }
func (*remoteRefreshHostStore) RemovePinned(string) error { return nil }
func (*remoteRefreshHostStore) Remember(string) error     { return nil }
func (*remoteRefreshHostStore) Forget(string) error       { return nil }
func (*remoteRefreshHostStore) Remove(string) (bool, error) {
	return false, nil
}

type remoteRefreshRequest struct {
	ctx    context.Context
	host   string
	result chan remoteRefreshResult
}

type remoteRefreshResult struct {
	catalog ports.RemoteCatalog
	err     error
}

type channelRemoteCatalog struct {
	d          *Daemon
	requests   chan remoteRefreshRequest
	probeMu    sync.Mutex
	probes     bool
	violations []string
}

func (c *channelRemoteCatalog) List(ctx context.Context, host string) (ports.RemoteCatalog, error) {
	c.probeMu.Lock()
	if c.probes && !c.d.mu.TryLock() {
		c.violations = append(c.violations, "daemon lock held during remote catalog I/O")
	} else if c.probes {
		c.d.mu.Unlock()
	}
	if c.probes && !c.d.remoteCatalog.mu.TryLock() {
		c.violations = append(c.violations, "remote catalog lock held during remote catalog I/O")
	} else if c.probes {
		c.d.remoteCatalog.mu.Unlock()
	}
	c.probeMu.Unlock()
	request := remoteRefreshRequest{ctx: ctx, host: host, result: make(chan remoteRefreshResult, 1)}
	c.requests <- request
	select {
	case result := <-request.result:
		return result.catalog, result.err
	case <-ctx.Done():
		return ports.RemoteCatalog{}, ctx.Err()
	}
}

type recordingRemoteCache struct {
	d          *Daemon
	stores     chan []ports.RemoteCatalogCacheEntry
	probeMu    sync.Mutex
	probes     bool
	violations []string
}

func (*recordingRemoteCache) Load() ([]ports.RemoteCatalogCacheEntry, error) { return nil, nil }

func (c *recordingRemoteCache) Store(entries []ports.RemoteCatalogCacheEntry) error {
	c.probeMu.Lock()
	if c.probes && !c.d.mu.TryLock() {
		c.violations = append(c.violations, "daemon lock held during remote cache write")
	} else if c.probes {
		c.d.mu.Unlock()
	}
	if c.probes && !c.d.remoteCatalog.mu.TryLock() {
		c.violations = append(c.violations, "remote catalog lock held during remote cache write")
	} else if c.probes {
		c.d.remoteCatalog.mu.Unlock()
	}
	c.probeMu.Unlock()
	copyEntries := make([]ports.RemoteCatalogCacheEntry, len(entries))
	for i, entry := range entries {
		copyEntries[i] = cloneRemoteCatalogEntry(entry)
	}
	c.stores <- copyEntries
	return nil
}

type fixedRemoteRefreshClock struct{ now time.Time }

func (c fixedRemoteRefreshClock) Now() time.Time                   { return c.now }
func (fixedRemoteRefreshClock) NewTimer(time.Duration) ports.Timer { return stubTimer{} }

type remotePickerClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *remotePickerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (*remotePickerClock) NewTimer(time.Duration) ports.Timer { return stubTimer{} }
func (c *remotePickerClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func requireNoRemoteLockViolations(t *testing.T, catalog *channelRemoteCatalog, cache *recordingRemoteCache) {
	t.Helper()
	if catalog != nil {
		catalog.probeMu.Lock()
		require.Empty(t, catalog.violations)
		catalog.probeMu.Unlock()
	}
	if cache != nil {
		cache.probeMu.Lock()
		require.Empty(t, cache.violations)
		cache.probeMu.Unlock()
	}
}

func remoteCatalogForTest(sessions ...ports.RemoteCatalogSession) ports.RemoteCatalog {
	return ports.RemoteCatalog{
		ProtocolVersion: protocol.Version,
		SchemaVersion:   ports.RemoteCatalogSchemaVersion,
		Sessions:        append([]ports.RemoteCatalogSession{}, sessions...),
	}
}

func newRemoteRefreshDaemon(t *testing.T, hosts *remoteRefreshHostStore, now time.Time) (*Daemon, *channelRemoteCatalog, *recordingRemoteCache) {
	t.Helper()
	catalog := &channelRemoteCatalog{requests: make(chan remoteRefreshRequest, 8)}
	cache := &recordingRemoteCache{stores: make(chan []ports.RemoteCatalogCacheEntry, 8)}
	d := New(nil, fixedRemoteRefreshClock{now: now}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithRemoteDiscovery(hosts, catalog, cache, nil, ports.RemoteTransportUDP))
	catalog.d, cache.d = d, d
	return d, catalog, cache
}

func TestRemoteRefreshStartsPerHostAndCancelsSupersededGeneration(t *testing.T) {
	hosts := &remoteRefreshHostStore{hosts: []string{"arch", "mule"}}
	d, catalog, cache := newRemoteRefreshDaemon(t, hosts, time.Unix(100, 0))
	ac := &attachedClient{}
	ac.initOverlays()
	model := picker.New(nil, picker.SelectionConfig{})
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = model
	ac.overlays.pickerGeneration++
	instance := remotePickerInstance{ac: ac, generation: ac.overlays.pickerGeneration, model: model}
	ac.overlays.pickerMu.Unlock()
	require.True(t, d.registerRemoteDiscoveryConsumer(instance.discoveryInstance()))
	lifecycle := remoteLifecycleForTest()
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{
			LifecycleID: lifecycle, Name: "cached", State: "up",
			Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
		}},
	}})

	firstGeneration := d.startRemoteDiscoveryRefresh(instance.discoveryInstance())
	first := map[string]remoteRefreshRequest{}
	for range 2 {
		request := receiveRemotePicker(t, catalog.requests, "catalog request")
		first[request.host] = request
	}
	require.Contains(t, first, "arch")
	require.Contains(t, first, "mule")
	requireNoRemoteLockViolations(t, catalog, nil)
	d.remoteCatalog.mu.Lock()
	require.Equal(t, []ports.RemoteCatalogSession{{
		LifecycleID: lifecycle, Name: "cached", State: "up",
		Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
	}}, d.remoteCatalog.cache["arch"].Sessions, "cached rows publish before any remote completion")
	require.Equal(t, remoteHostRefreshing, d.remoteCatalog.status["arch"])
	d.remoteCatalog.mu.Unlock()
	ac.overlays.pickerMu.Lock()
	refreshedModel := ac.overlays.picker
	ac.overlays.pickerMu.Unlock()
	_, selectable := refreshedModel.Selected()
	require.False(t, selectable, "refreshing status must gate picker activation")

	secondGeneration := d.startRemoteDiscoveryRefresh(instance.discoveryInstance())
	require.Greater(t, secondGeneration, firstGeneration)
	for _, request := range first {
		receiveRemotePickerClose(t, request.ctx.Done(), "catalog request cancellation")
	}
	for range 2 {
		request := receiveRemotePicker(t, catalog.requests, "catalog request")
		request.result <- remoteRefreshResult{catalog: remoteCatalogForTest()}
	}
	for range 2 {
		receiveRemotePicker(t, cache.stores, "remote cache store")
	}
	requireNoRemoteLockViolations(t, nil, cache)

	d.remoteCatalog.mu.Lock()
	require.Equal(t, secondGeneration, d.remoteCatalog.refresh)
	require.Equal(t, remoteHostFresh, d.remoteCatalog.status["arch"])
	require.Equal(t, remoteHostFresh, d.remoteCatalog.status["mule"])
	d.remoteCatalog.mu.Unlock()
}

func TestRemoteRefreshLockProbesSerialized(t *testing.T) {
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d, catalog, cache := newRemoteRefreshDaemon(t, hosts, time.Unix(125, 0))
	catalog.probes, cache.probes = true, true
	ac := &attachedClient{}
	ac.initOverlays()
	model := picker.New(nil, picker.SelectionConfig{})
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = model
	ac.overlays.pickerGeneration++
	instance := remotePickerInstance{ac: ac, generation: ac.overlays.pickerGeneration, model: model}
	ac.overlays.pickerMu.Unlock()
	require.True(t, d.registerRemoteDiscoveryConsumer(instance.discoveryInstance()))

	d.startRemoteDiscoveryRefresh(instance.discoveryInstance())
	request := receiveRemotePicker(t, catalog.requests, "catalog request")
	request.result <- remoteRefreshResult{catalog: remoteCatalogForTest()}
	receiveRemotePicker(t, cache.stores, "remote cache store")
	requireNoRemoteLockViolations(t, catalog, cache)
}

func TestRemoteRefreshWithoutCatalogClientDoesNotMarkHostsRefreshing(t *testing.T) {
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d := newRemotePickerDaemon(hosts)
	ac := &attachedClient{}
	ac.initOverlays()
	model := picker.New(nil, picker.SelectionConfig{})
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = model
	ac.overlays.pickerGeneration++
	instance := remotePickerInstance{ac: ac, generation: ac.overlays.pickerGeneration, model: model}
	ac.overlays.pickerMu.Unlock()
	require.True(t, d.registerRemoteDiscoveryConsumer(instance.discoveryInstance()))

	require.NotZero(t, d.startRemoteDiscoveryRefresh(instance.discoveryInstance()))
	d.remoteCatalog.mu.Lock()
	require.NotEqual(t, remoteHostRefreshing, d.remoteCatalog.status["arch"])
	d.remoteCatalog.mu.Unlock()
}

func TestRemoteRefreshResultPreservesFailuresAndEvictsSuccessfulOmissions(t *testing.T) {
	now := time.Unix(200, 0)
	hosts := &remoteRefreshHostStore{hosts: []string{"arch", "mule"}}
	d, _, cache := newRemoteRefreshDaemon(t, hosts, now)
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{
		{Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "old", State: "up"}}},
		{Host: "mule", FetchedAt: time.Unix(20, 0), Sessions: []ports.RemoteCatalogSession{{Name: "stale", State: "up"}}},
	})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.refresh = 7
	d.remoteCatalog.mu.Unlock()

	require.False(t, d.applyRemoteRefreshResult(7, "mule", ports.RemoteCatalog{}, errors.New("offline")))
	d.remoteCatalog.mu.Lock()
	require.Equal(t, []ports.RemoteCatalogSession{{Name: "stale", State: "up"}}, d.remoteCatalog.cache["mule"].Sessions)
	require.Equal(t, time.Unix(20, 0), d.remoteCatalog.cache["mule"].FetchedAt)
	require.Equal(t, remoteHostUnreachable, d.remoteCatalog.status["mule"])
	d.remoteCatalog.mu.Unlock()

	require.True(t, d.applyRemoteRefreshResult(7, "arch", remoteCatalogForTest(), nil))
	stored := receiveRemotePicker(t, cache.stores, "remote cache store")
	require.Equal(t, []ports.RemoteCatalogCacheEntry{
		{Host: "arch", FetchedAt: now, Sessions: []ports.RemoteCatalogSession{}},
		{Host: "mule", FetchedAt: time.Unix(20, 0), Sessions: []ports.RemoteCatalogSession{{Name: "stale", State: "up"}}},
	}, stored, "every successful write persists the newest full cache atomically")
	requireNoRemoteLockViolations(t, nil, cache)
	d.remoteCatalog.mu.Lock()
	require.Empty(t, d.remoteCatalog.cache["arch"].Sessions, "a successful omission authoritatively evicts old rows")
	require.Equal(t, remoteHostFresh, d.remoteCatalog.status["arch"])
	d.remoteCatalog.mu.Unlock()
}

func TestRemoteRefreshRejectsRemovedHostAndLateGeneration(t *testing.T) {
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d, _, cache := newRemoteRefreshDaemon(t, hosts, time.Unix(300, 0))
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "old", State: "up"}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.refresh = 9
	d.remoteCatalog.mu.Unlock()

	require.False(t, d.applyRemoteRefreshResult(8, "arch", remoteCatalogForTest(ports.RemoteCatalogSession{
		LifecycleID: remoteLifecycleForTest(), Name: "late", State: ports.RemoteCatalogSessionUp,
		Tabs: []ports.RemoteCatalogTab{{ID: "tab-1"}},
	}), nil))
	hosts.set()
	require.True(t, d.applyRemoteRefreshResult(9, "arch", remoteCatalogForTest(ports.RemoteCatalogSession{
		LifecycleID: remoteLifecycleForTest(), Name: "removed", State: ports.RemoteCatalogSessionUp,
		Tabs: []ports.RemoteCatalogTab{{ID: "tab-1"}},
	}), nil))
	require.Empty(t, receiveRemotePicker(t, cache.stores, "remote cache store"))
	requireNoRemoteLockViolations(t, nil, cache)
	d.remoteCatalog.mu.Lock()
	require.NotContains(t, d.remoteCatalog.cache, "arch")
	require.NotContains(t, d.remoteCatalog.status, "arch")
	d.remoteCatalog.mu.Unlock()
}

func TestRemoteRefreshValidationSentinelsRemainMalformed(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	for _, test := range []struct {
		name    string
		session ports.RemoteCatalogSession
	}{
		{name: "unknown state", session: ports.RemoteCatalogSession{LifecycleID: lifecycle, Name: "work", State: "unknown"}},
		{name: "invalid reason", session: ports.RemoteCatalogSession{LifecycleID: lifecycle, Name: "work", State: "up", Reason: "bogus"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
			d, _, _ := newRemoteRefreshDaemon(t, hosts, time.Unix(400, 0))
			d.remoteCatalog.refresh = 1
			catalog := ports.RemoteCatalog{ProtocolVersion: protocol.Version, SchemaVersion: ports.RemoteCatalogSchemaVersion, Sessions: []ports.RemoteCatalogSession{test.session}}
			require.False(t, d.applyRemoteRefreshResult(1, "arch", catalog, nil))
			d.remoteCatalog.mu.Lock()
			require.Equal(t, remoteHostMalformed, d.remoteCatalog.status["arch"])
			d.remoteCatalog.mu.Unlock()
		})
	}
}

func TestRemoteRefreshVersionMismatchPreservesStaleRows(t *testing.T) {
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d, _, _ := newRemoteRefreshDaemon(t, hosts, time.Unix(400, 0))
	old := ports.RemoteCatalogCacheEntry{Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "up"}}}
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{old})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.refresh = 3
	d.remoteCatalog.mu.Unlock()

	require.False(t, d.applyRemoteRefreshResult(3, "arch", ports.RemoteCatalog{}, &ports.RemoteCatalogVersionMismatchError{Got: 19, Want: protocol.Version}))
	d.remoteCatalog.mu.Lock()
	require.Equal(t, old, d.remoteCatalog.cache["arch"])
	require.Equal(t, remoteHostVersionMismatch, d.remoteCatalog.status["arch"])
	d.remoteCatalog.mu.Unlock()
}

func TestRemotePickerHandoffSendsTargetAndLeavesNoShadowSession(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	remoteSession := ports.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "work", State: ports.RemoteCatalogSessionUp,
		Tabs: []ports.RemoteCatalogTab{{ID: "tab-1"}},
	}
	d := newRemotePickerDaemon(nil)
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{remoteSession},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()
	sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
	token := sess.captureAttachmentCapability(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}
	require.NoError(t, d.sendRemoteAttachTargetForAttachment(effect, target, sessionHandoffGuard{closePicker: true}, "picker-select"))

	frame := receiveRemotePicker(t, sends, "attach target")
	require.Equal(t, ports.MsgAttachTarget, frame.Type)
	got, err := ports.UnmarshalAttachTarget(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.AttachTarget{Endpoint: "arch", Session: "work", Intent: protocol.IntentAttach, RemoteTarget: &remoteTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, got)
	require.Nil(t, ac.currentAttachmentSession())
	d.mu.Lock()
	require.NotContains(t, d.sessions, key.ID(), "remote picker handoff must not create a local session shadow")
	d.mu.Unlock()
}

func TestRemotePickerSelectsStoppedRemoteTabAndRestoresIt(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	remoteSession := ports.RemoteCatalogSession{
		LifecycleID: lifecycle,
		Name:        "work",
		State:       "down",
		Tabs: []ports.RemoteCatalogTab{
			{ID: "tab-a", Name: "alpha", Index: 0},
			{ID: "tab-b", Name: "beta", Index: 1},
		},
	}
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteView := remotePickerView(key, remoteSession, remoteHostFresh, time.Unix(10, 0))
	model := picker.New([]picker.SessionView{
		{ID: "local", Name: "local", Tabs: []picker.TabEntry{{TabID: "local-tab", Name: "local"}}},
		remoteView,
	}, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
	var selected picker.Target
	for range 4 {
		candidate, ok := model.Selected()
		if ok && candidate.RemoteTarget != nil && candidate.RemoteTarget.StoppedTab.StableID == "tab-b" {
			selected = candidate
			break
		}
		model.Down()
	}
	require.NotNil(t, selected.RemoteTarget)
	require.True(t, selected.RemoteTarget.Stopped)
	require.Equal(t, domain.NewStableTabSelector("tab-b"), selected.RemoteTarget.StoppedTab)

	local := newRemotePickerDaemon(nil)
	local.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{remoteSession}}})
	local.remoteCatalog.mu.Lock()
	local.remoteCatalog.status["arch"] = remoteHostFresh
	local.remoteCatalog.mu.Unlock()
	localSession, localAttachment, sends := addRemoteRefreshPickerOwner(t, local, "local")
	token := localSession.captureAttachmentCapability(localAttachment, localAttachment.transport())
	effect, admitted := localAttachment.beginAttachmentEffect(token)
	require.True(t, admitted)
	require.NoError(t, local.sendRemoteAttachTargetForAttachment(effect, selected, sessionHandoffGuard{closePicker: true}, "picker-select"))

	frame := receiveRemotePicker(t, sends, "stopped remote target")
	handoff, err := ports.UnmarshalAttachTarget(frame.Payload)
	require.NoError(t, err)
	require.NotNil(t, handoff.RemoteTarget)
	require.Equal(t, selected.RemoteTarget, handoff.RemoteTarget)
	local.mu.Lock()
	require.NotContains(t, local.sessions, key.ID(), "picker handoff must not create a local remote shadow")
	local.mu.Unlock()

	remote := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	remote.mu.Lock()
	remote.inactive["work"] = inactiveSession{
		name: "work", cwd: "/remote/work", incarnation: lifecycle, state: protocol.SessionDown,
		tabNames:   []string{"alpha", "beta"},
		tabRecords: []domain.CatalogueTabRecord{{StableID: "tab-a", Name: "alpha"}, {StableID: "tab-b", Name: "beta"}},
	}
	remote.mu.Unlock()
	transport, _ := newCapturingTransport(t)
	restored, attachment, err := remote.routeWithContext(context.Background(), protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: handoff.Session,
		Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: handoff.RemoteTarget,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}, transport)
	require.NoError(t, err)
	t.Cleanup(func() { remote.clientGone(restored, attachment, transport, false) })
	require.Equal(t, lifecycle, restored.incarnation)
	require.Equal(t, domain.TabStableID("tab-b"), attachment.viewSnapshot().tabID)
}

func TestRemotePickerResurrectsStoppedRemoteSessionWithoutTabMetadata(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	remoteSession := ports.RemoteCatalogSession{
		LifecycleID: lifecycle,
		Name:        "work",
		State:       "down",
		Tabs:        []ports.RemoteCatalogTab{},
	}
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteView := remotePickerView(key, remoteSession, remoteHostFresh, time.Unix(10, 0))
	require.Equal(t, picker.RemoteRestart, remoteView.RemoteActivation)

	model := picker.New([]picker.SessionView{
		{ID: "local", Name: "local", Tabs: []picker.TabEntry{{TabID: "local-tab", Name: "local"}}},
		remoteView,
	}, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
	var selected picker.Target
	for range 3 {
		model.Down()
		candidate, ok := model.Selected()
		if ok && candidate.RemoteTarget != nil {
			selected = candidate
			break
		}
	}
	require.NotNil(t, selected.RemoteTarget)
	require.True(t, selected.RemoteTarget.Stopped)
	require.Equal(t, domain.TabSelector{}, selected.RemoteTarget.StoppedTab)

	local := newRemotePickerDaemon(nil)
	local.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{remoteSession},
	}})
	local.remoteCatalog.mu.Lock()
	local.remoteCatalog.status["arch"] = remoteHostFresh
	local.remoteCatalog.mu.Unlock()
	localSession, localAttachment, sends := addRemoteRefreshPickerOwner(t, local, "local")
	token := localSession.captureAttachmentCapability(localAttachment, localAttachment.transport())
	effect, admitted := localAttachment.beginAttachmentEffect(token)
	require.True(t, admitted)
	require.NoError(t, local.sendRemoteAttachTargetForAttachment(effect, selected, sessionHandoffGuard{closePicker: true}, "picker-select"))

	frame := receiveRemotePicker(t, sends, "stopped remote target without tab metadata")
	handoff, err := ports.UnmarshalAttachTarget(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, selected.RemoteTarget, handoff.RemoteTarget)

	remote := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	remote.mu.Lock()
	remote.inactive["work"] = inactiveSession{name: "work", cwd: "/remote/work", incarnation: lifecycle, state: protocol.SessionDown}
	remote.mu.Unlock()
	transport, _ := newCapturingTransport(t)
	restored, attachment, err := remote.routeWithContext(context.Background(), protocol.Hello{
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: handoff.Session,
		Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: handoff.RemoteTarget,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}, transport)
	require.NoError(t, err)
	t.Cleanup(func() { remote.clientGone(restored, attachment, transport, false) })
	require.Equal(t, lifecycle, restored.incarnation)
	require.Len(t, restored.tabs, 1)
	require.Equal(t, restored.tabs[0].stableID, string(attachment.viewSnapshot().tabID))
}

func TestNavigationActionHandoffSendsBoundedAction(t *testing.T) {
	tests := []struct {
		name   string
		action protocol.NavigationAction
	}{
		{name: "home picker", action: protocol.NavigationOpenHomePicker},
		{name: "back", action: protocol.NavigationBack},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newRemotePickerDaemon(nil)
			sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
			if tt.action == protocol.NavigationOpenHomePicker {
				ac.navigationCapabilities = protocol.NavigationCapabilityHomePicker
			}
			token := sess.captureAttachmentCapability(ac, ac.transport())
			effect, admitted := ac.beginAttachmentEffect(token)
			require.True(t, admitted)
			defer effect.End()
			require.NoError(t, d.sendNavigationActionForAttachment(effect, tt.action))
			frame := receiveRemotePicker(t, sends, "navigation action")
			require.Equal(t, ports.MsgNavigationAction, frame.Type)
			directive, err := ports.UnmarshalNavigationDirective(frame.Payload)
			require.NoError(t, err)
			require.Equal(t, tt.action, directive.Action)
			if tt.action == protocol.NavigationOpenHomePicker {
				require.False(t, directive.LeaseID.IsZero())
			} else {
				require.True(t, directive.LeaseID.IsZero())
			}
		})
	}
}

func TestRemotePickerHandoffSendFailureKeepsPickerOpen(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	remoteSession := ports.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "work", State: ports.RemoteCatalogSessionUp,
		Tabs: []ports.RemoteCatalogTab{{ID: "tab-1"}},
	}
	d := newRemotePickerDaemon(nil)
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{remoteSession},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()
	cause := errors.New("remote attach send failed")
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(cause)
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "local", tr)
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = picker.New(nil, picker.SelectionConfig{})
	ac.overlays.pickerGeneration++
	ac.overlays.pickerMu.Unlock()

	gone := make(chan struct{})
	d.afterClientGoneDetach = func() { close(gone) }
	token := sess.captureAttachmentCapability(ac, tr)
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer effect.End()
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}
	err := d.sendRemoteAttachTargetForAttachment(effect, target, sessionHandoffGuard{closePicker: true}, "picker-select")
	var userErr *domain.UserError
	require.ErrorAs(t, err, &userErr)
	require.Equal(t, "couldn't attach to remote session", userErr.Msg)
	require.ErrorIs(t, err, cause)
	require.True(t, ac.overlays.pickerActive(), "failed control send must leave the picker open")
	select {
	case <-gone:
		t.Fatal("failed control send reached clientGoneForAttachment")
	default:
	}
	require.Same(t, sess, ac.currentAttachmentSession())
}

func addRemoteRefreshPickerOwner(t *testing.T, d *Daemon, id domain.SessionID, transports ...ports.Transport) (*session, *attachedClient, chan ports.Frame) {
	t.Helper()
	var tr ports.Transport
	var sends chan ports.Frame
	if len(transports) != 0 {
		tr = transports[0]
	} else {
		tr, sends = newCapturingTransport(t)
	}
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	tb := newTab(newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	tb.stableID = string(id) + "-tab"
	sess := &session{sessionCore: sessionCore{id: id, name: string(id), attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{tb}}
	publishTiledPaneOwners(sess, tb)
	ac.setSession(sess)
	d.sessions[id] = sess
	return sess, ac, sends
}

func TestRemoteRefreshUpdatesAllOpenPickersPreservingSelection(t *testing.T) {
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d, catalog, cache := newRemoteRefreshDaemon(t, hosts, time.Unix(450, 0))
	firstSession, first, _ := addRemoteRefreshPickerOwner(t, d, "first")
	_, second, _ := addRemoteRefreshPickerOwner(t, d, "second")
	before := make(map[*attachedClient]*picker.Model)

	for _, owner := range []*attachedClient{first, second} {
		owner.overlays.pickerMu.Lock()
		owner.overlays.picker = d.newPickerModel(firstSession, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{Session: "second", TabID: "second-tab"})
		owner.overlays.pickerIntent = pickerNavigate
		owner.overlays.pickerGeneration++
		instance := remotePickerInstance{ac: owner, generation: owner.overlays.pickerGeneration, model: owner.overlays.picker}
		before[owner] = owner.overlays.picker
		owner.overlays.pickerMu.Unlock()
		require.True(t, d.registerRemoteDiscoveryConsumer(instance.discoveryInstance()))
	}
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.refresh = 4
	d.remoteCatalog.mu.Unlock()

	require.True(t, d.applyRemoteRefreshResult(4, "arch", ports.RemoteCatalog{
		ProtocolVersion: protocol.Version,
		SchemaVersion:   ports.RemoteCatalogSchemaVersion,
		Sessions: []ports.RemoteCatalogSession{{
			LifecycleID: remoteLifecycleForTest(), Name: "work", State: "up",
			Tabs: []ports.RemoteCatalogTab{{ID: "tab-1"}, {ID: "tab-2", Index: 1}},
		}},
	}, nil))
	receiveRemotePicker(t, cache.stores, "remote cache store")
	requireNoRemoteLockViolations(t, catalog, cache)

	for _, owner := range []*attachedClient{first, second} {
		owner.overlays.pickerMu.Lock()
		updated := owner.overlays.picker
		selected, ok := updated.Selected()
		owner.overlays.pickerMu.Unlock()
		require.NotSame(t, before[owner], updated, "every open picker must receive the refreshed model")
		require.True(t, ok)
		require.Equal(t, domain.SessionID("second"), selected.Session)
	}
}

func TestRemotePickerTeardownLifecycle(t *testing.T) {
	for _, test := range []struct {
		name          string
		resumeCapable bool
		teardown      func(*Daemon, *session, *attachedClient)
	}{
		{name: "client gone parks resumable attachment", resumeCapable: true, teardown: func(d *Daemon, sess *session, ac *attachedClient) { d.clientGone(sess, ac, ac.transport(), false) }},
		{name: "send error parks resumable attachment", resumeCapable: true, teardown: func(d *Daemon, sess *session, ac *attachedClient) { d.detachOnSendError(sess, ac, ac.transport()) }},
		{name: "client gone removes non-resumable attachment", teardown: func(d *Daemon, sess *session, ac *attachedClient) { d.clientGone(sess, ac, ac.transport(), false) }},
		{name: "send error removes non-resumable attachment", teardown: func(d *Daemon, sess *session, ac *attachedClient) { d.detachOnSendError(sess, ac, ac.transport()) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := newRemotePickerDaemon(nil)
			sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
			ac.resumeCapable = test.resumeCapable
			model := d.newPickerModel(sess, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
			d.publishPicker(sess, ac, model, pickerNavigate, moveSourceLocator{})
			ac.overlays.pickerMu.Lock()
			generation := ac.overlays.pickerGeneration
			ac.overlays.pickerMu.Unlock()

			test.teardown(d, sess, ac)

			d.remoteCatalog.mu.Lock()
			_, registered := d.remoteCatalog.consumers[ac]
			d.remoteCatalog.mu.Unlock()
			ac.overlays.pickerMu.Lock()
			current, currentGeneration := ac.overlays.picker, ac.overlays.pickerGeneration
			ac.overlays.pickerMu.Unlock()
			if !test.resumeCapable {
				require.False(t, registered)
				require.Nil(t, current)
				return
			}
			require.True(t, ac.parked)
			require.True(t, registered)
			require.Same(t, model, current)
			require.Equal(t, generation, currentGeneration)
			d.mu.Lock()
			parked := d.parked[ac.resumeToken]
			d.mu.Unlock()
			require.NotNil(t, parked)
			require.Same(t, ac, parked.ac)
		})
	}
}

func TestParkedOverlayExpiryClosesCapturedDiscoveryGenerations(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d, catalog, _ := newRemoteRefreshDaemon(t, hosts, time.Unix(475, 0))
	d.clock = clock
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	ac.resumeCapable = true
	d.publishPicker(sess, ac, d.newPickerModel(sess, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{}), pickerNavigate, moveSourceLocator{})
	request := receiveRemotePicker(t, catalog.requests, "picker catalog request")
	d.enterPalette(sess, ac)
	receiveRemotePickerClose(t, request.ctx.Done(), "superseded picker catalog request")
	request = receiveRemotePicker(t, catalog.requests, "shared catalog request")

	require.True(t, d.parkAttachment(sess, ac))
	timer := receiveRemotePicker(t, clock.timers, "park expiry timer")
	timer.ch <- clock.Now()
	receiveRemotePickerClose(t, request.ctx.Done(), "catalog request cancellation")

	ac.overlays.pickerMu.Lock()
	require.Nil(t, ac.overlays.picker)
	ac.overlays.pickerMu.Unlock()
	ac.overlays.paletteMu.Lock()
	require.Nil(t, ac.overlays.palette)
	ac.overlays.paletteMu.Unlock()
	d.remoteCatalog.mu.Lock()
	require.NotContains(t, d.remoteCatalog.consumers, ac)
	d.remoteCatalog.mu.Unlock()
}

func TestParkedPickerRetirementClosesCapturedGenerationOnly(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	ac.resumeCapable = true
	d.publishPicker(sess, ac, d.newPickerModel(sess, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{}), pickerNavigate, moveSourceLocator{})
	d.refreshPickerOpts(ac, pickerRefreshOptions{preserveSelection: true, nearestRow: -1})
	ac.overlays.pickerMu.Lock()
	refreshed := ac.overlays.picker
	ac.overlays.pickerMu.Unlock()
	require.NotNil(t, refreshed)
	require.True(t, d.parkAttachment(sess, ac))
	token := ac.resumeToken
	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked)

	d.mu.Lock()
	retirement := d.retireParkedAttachmentLocked(token, parked)
	d.mu.Unlock()
	d.finishParkedAttachmentRetirements([]parkedAttachmentRetirement{retirement})
	ac.overlays.pickerMu.Lock()
	require.Nil(t, ac.overlays.picker, "terminal retirement closes a refreshed model in its captured generation")
	ac.overlays.pickerMu.Unlock()
}

func TestParkedPickerTokenReplacementRetiresPreviousGeneration(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	firstSession, first, _ := addRemoteRefreshPickerOwner(t, d, "first")
	secondSession, second, _ := addRemoteRefreshPickerOwner(t, d, "second")
	first.resumeCapable, second.resumeCapable = true, true
	d.publishPicker(firstSession, first, d.newPickerModel(firstSession, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{}), pickerNavigate, moveSourceLocator{})
	require.True(t, d.parkAttachment(firstSession, first))
	token := first.resumeToken
	d.publishPicker(secondSession, second, d.newPickerModel(secondSession, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{}), pickerNavigate, moveSourceLocator{})
	second.resumeToken = token
	require.True(t, d.parkAttachment(secondSession, second))

	first.overlays.pickerMu.Lock()
	require.Nil(t, first.overlays.picker)
	first.overlays.pickerMu.Unlock()
	second.overlays.pickerMu.Lock()
	require.NotNil(t, second.overlays.picker)
	second.overlays.pickerMu.Unlock()
}

func TestParkedPickerResumePreservesGeneration(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	ac.resumeCapable = true
	ac.clientID = [16]byte{1, 2, 3, 4}
	d.publishPicker(sess, ac, d.newPickerModel(sess, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{}), pickerNavigate, moveSourceLocator{})
	ac.overlays.pickerMu.Lock()
	model, generation := ac.overlays.picker, ac.overlays.pickerGeneration
	ac.overlays.pickerMu.Unlock()
	d.clientGone(sess, ac, ac.transport(), false)
	token := ac.resumeToken
	tr := &closeTrackingTransport{}
	resumedSess, resumedAC, ok, err := d.resumeParked(helloResumeCapable(protocol.IntentResume, sess.name, token), tr, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, sess, resumedSess)
	require.Same(t, ac, resumedAC)
	ac.overlays.pickerMu.Lock()
	require.Same(t, model, ac.overlays.picker)
	require.Equal(t, generation, ac.overlays.pickerGeneration)
	ac.overlays.pickerMu.Unlock()
}

func TestStaleParkedPickerExpiryPreservesNewGeneration(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	ac.resumeCapable = true
	d.publishPicker(sess, ac, d.newPickerModel(sess, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{}), pickerNavigate, moveSourceLocator{})
	require.True(t, d.parkAttachment(sess, ac))
	token := ac.resumeToken
	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked)

	newer := d.newPickerModel(sess, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, newer, pickerNavigate, moveSourceLocator{})
	d.expireParked(token, parked)
	ac.overlays.pickerMu.Lock()
	require.Same(t, newer, ac.overlays.picker)
	ac.overlays.pickerMu.Unlock()
}

func TestRemoteRefreshCancellationWhenLastPickerCloses(t *testing.T) {
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d, catalog, _ := newRemoteRefreshDaemon(t, hosts, time.Unix(500, 0))
	newInstance := func() remotePickerInstance {
		ac := &attachedClient{}
		ac.initOverlays()
		model := picker.New(nil, picker.SelectionConfig{})
		ac.overlays.pickerMu.Lock()
		ac.overlays.picker = model
		ac.overlays.pickerGeneration++
		instance := remotePickerInstance{ac: ac, generation: ac.overlays.pickerGeneration, model: model}
		ac.overlays.pickerMu.Unlock()
		return instance
	}
	closeInstance := func(instance remotePickerInstance) {
		instance.ac.overlays.pickerMu.Lock()
		instance.ac.overlays.picker = nil
		instance.ac.overlays.pickerMu.Unlock()
		d.remoteDiscoveryClosed(instance.discoveryInstance())
	}
	first, second := newInstance(), newInstance()
	d.remoteDiscoveryOpened(first.discoveryInstance())
	request := receiveRemotePicker(t, catalog.requests, "catalog request")
	d.remoteDiscoveryOpened(second.discoveryInstance())
	receiveRemotePickerClose(t, request.ctx.Done(), "catalog request cancellation")
	request = receiveRemotePicker(t, catalog.requests, "catalog request")

	closeInstance(first)
	select {
	case <-request.ctx.Done():
		t.Fatal("refresh canceled while another picker remained open")
	default:
	}
	closeInstance(second)
	receiveRemotePickerClose(t, request.ctx.Done(), "catalog request cancellation")

	d.remoteCatalog.mu.Lock()
	require.Empty(t, d.remoteCatalog.consumers)
	require.Nil(t, d.remoteCatalog.cancel)
	d.remoteCatalog.mu.Unlock()
}

func TestRemoteRefreshCannotStartAfterLastPickerCloseWins(t *testing.T) {
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d, catalog, _ := newRemoteRefreshDaemon(t, hosts, time.Unix(525, 0))
	ac := &attachedClient{}
	ac.initOverlays()
	model := picker.New(nil, picker.SelectionConfig{})
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = model
	ac.overlays.pickerGeneration++
	instance := remotePickerInstance{ac: ac, generation: ac.overlays.pickerGeneration, model: model}
	ac.overlays.pickerMu.Unlock()

	// Stop at the production seam after exact picker ownership registration but
	// before refresh startup, then let the last-picker close linearize first.
	require.True(t, d.registerRemoteDiscoveryConsumer(instance.discoveryInstance()))
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = nil
	ac.overlays.pickerMu.Unlock()
	d.remoteDiscoveryClosed(instance.discoveryInstance())

	d.remoteCatalog.mu.Lock()
	refreshBefore := d.remoteCatalog.refresh
	d.remoteCatalog.mu.Unlock()
	require.Zero(t, d.startRemoteDiscoveryRefresh(instance.discoveryInstance()))

	d.remoteCatalog.mu.Lock()
	require.Empty(t, d.remoteCatalog.consumers, "the closing picker must remain unregistered")
	require.Equal(t, refreshBefore, d.remoteCatalog.refresh, "a close that wins before startup must prevent a later refresh generation")
	require.Nil(t, d.remoteCatalog.cancel, "a close that wins before startup must prevent a later cancel installation")
	d.remoteCatalog.mu.Unlock()
	select {
	case request := <-catalog.requests:
		t.Fatalf("catalog call remained running after the last picker closed: %s", request.host)
	default:
	}
}

func TestRemotePickerStaleAfterCloseCannotRemoveReopenedPicker(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	first := d.newPickerModel(sess, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, first, pickerNavigate, moveSourceLocator{})

	staleClose := d.pickerListInputState(ac)
	ac.overlays.pickerMu.Lock()
	staleClose.closeLocked()
	ac.overlays.pickerMu.Unlock()

	reopened := d.newPickerModel(sess, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, reopened, pickerNavigate, moveSourceLocator{})
	staleClose.afterClose()

	d.remoteCatalog.mu.Lock()
	_, registered := d.remoteCatalog.consumers[ac]
	d.remoteCatalog.mu.Unlock()
	require.True(t, registered, "stale close cleanup must retain the reopened picker registration")
	ac.overlays.pickerMu.Lock()
	require.Same(t, reopened, ac.overlays.picker)
	ac.overlays.pickerMu.Unlock()
}

func TestRemotePickerStaleRefreshCannotOverwriteReopenedModel(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	first := d.newPickerModel(sess, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, first, pickerNavigate, moveSourceLocator{})

	rebuildReached := make(chan struct{})
	allowPublication := make(chan struct{})
	publicationWait := make(chan error, 1)
	ac.overlays.afterPickerRefreshBuild = func(*picker.Model) {
		close(rebuildReached)
		publicationWait <- waitRemotePickerClose(allowPublication, "picker publication release")
	}
	refreshed := make(chan struct{})
	go func() {
		d.refreshRemoteDiscoveryConsumers()
		close(refreshed)
	}()
	receiveRemotePickerClose(t, rebuildReached, "picker rebuild")

	d.closePicker(ac)
	reopened := d.newPickerModel(sess, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, reopened, pickerNavigate, moveSourceLocator{})
	close(allowPublication)
	require.NoError(t, receiveRemotePicker(t, publicationWait, "picker publication release"))
	receiveRemotePickerClose(t, refreshed, "picker refresh")

	ac.overlays.pickerMu.Lock()
	require.Same(t, reopened, ac.overlays.picker, "a rebuild for the old picker must not publish into its replacement")
	ac.overlays.pickerMu.Unlock()
}
