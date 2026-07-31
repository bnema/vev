package daemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/picker"
)

func newRemotePickerDaemon(store ports.RemoteHostStore) *Daemon {
	return New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithRemoteDiscovery(store, nil, nil, nil, ports.RemoteTransportUDP))
}

func TestRemotePickerMerge(t *testing.T) {
	store := portsmocks.NewMockRemoteHostStore(t)
	store.EXPECT().Hosts().Return([]string{"pinned"}, []string{"learned"}, nil).Once()
	d := newRemotePickerDaemon(store)
	local := &session{sessionCore: sessionCore{id: "local", name: "local"}, tabs: []*tab{{}}}
	local.mruAt.Store(6)
	key := domain.RemoteSessionKey{Host: "pinned", Name: "work"}
	proxy := &proxySession{
		sessionCore: sessionCore{id: key.ID(), name: key.Display()},
		key:         key,
		meta: ports.SessionMeta{SessionName: key.Name, Tabs: []ports.SessionTabMeta{
			{Index: 0, Name: "shell"},
		}},
	}
	proxy.mruAt.Store(5)
	d.sessions[local.id] = local
	d.sessions[proxy.id] = proxy
	d.stopped["stopped"] = stoppedSession{name: "stopped", createdAt: 2, lastUsedSeq: 9}
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{
		{Host: "learned", FetchedAt: time.Unix(20, 0), Sessions: []ports.RemoteCatalogSession{{Name: "alpha", State: "running", Tabs: 3}}},
		{Host: "pinned", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "running", Tabs: 9}, {Name: "alpha", State: "running", Tabs: 2}}},
	})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["pinned"] = remoteHostFresh
	d.remoteCatalog.status["learned"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()

	views, _ := d.pickerViews(local)

	require.Equal(t, []string{"local", "work@pinned", "alpha@pinned", "alpha@learned", "stopped"}, pickerViewNames(views))
	require.False(t, views[0].Stopped)
	require.Equal(t, key, *views[1].RemoteKey)
	require.Equal(t, picker.RemoteFresh, views[1].RemoteAvailability)
	require.True(t, views[1].ConnectOnly)
	require.Equal(t, "2 tabs", views[2].RemoteDetail)
	require.Equal(t, picker.RemoteFresh, views[2].RemoteAvailability)
	require.Equal(t, picker.RemoteFresh, views[3].RemoteAvailability)
	require.True(t, views[4].Stopped)
}

func TestRemotePickerDedupeUsesExactKeyAndKeepsLiveID(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	proxy := &proxySession{
		sessionCore: sessionCore{id: key.ID(), name: key.Display()},
		key:         key,
		meta: ports.SessionMeta{SessionName: key.Name, Tabs: []ports.SessionTabMeta{
			{Index: 0, Name: "live tab"},
		}},
	}
	d.sessions[key.ID()] = proxy
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{
			{Name: "work", State: "running", Tabs: 7},
		},
	}})

	views, _ := d.pickerViews(nil)

	require.Len(t, views, 1)
	require.Equal(t, key.ID(), views[0].ID)
	require.Equal(t, key, *views[0].RemoteKey)
	require.Equal(t, []picker.TabEntry{{Name: "live tab"}}, views[0].Tabs, "live proxy snapshot wins over cached discovery")
	require.Equal(t, picker.RemoteFresh, views[0].RemoteAvailability)
}

func TestRemotePickerSnapshotsDoNotRetainProxyLock(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	proxy := &proxySession{
		sessionCore: sessionCore{id: key.ID(), name: key.Display()},
		key:         key,
		meta:        ports.SessionMeta{SessionName: key.Name, Tabs: []ports.SessionTabMeta{{Index: 0, Name: "shell"}}},
	}
	d.sessions[key.ID()] = proxy

	views, _ := d.pickerViews(nil)

	require.Len(t, views, 1)
	require.True(t, proxy.mu.TryLock(), "picker construction must release the proxy leaf lock")
	proxy.mu.Unlock()
	require.True(t, d.remoteCatalog.mu.TryLock(), "picker construction must release the remote catalog lock")
	d.remoteCatalog.mu.Unlock()
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
				Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "running"}},
			}})
			d.remoteCatalog.mu.Lock()
			d.remoteCatalog.status["arch"] = test.status
			d.remoteCatalog.mu.Unlock()

			views, _ := d.pickerViews(nil)

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
		Host: "arch", FetchedAt: fetchedAt, Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "running"}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostUnreachable
	d.remoteCatalog.failure["arch"] = failedAt
	d.remoteCatalog.mu.Unlock()

	views, _ := d.pickerViews(nil)

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

			views, _ := d.pickerViews(nil)

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

func TestRemotePickerGroupedSortTreatsProxiesAsLiveSessions(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	d.pickerSort.Store(uint32(pickerSortGrouped))
	ephemeral := &session{sessionCore: sessionCore{id: "ephemeral", name: "1", ephemeral: true}, tabs: []*tab{{}}}
	ephemeral.mruAt.Store(9)
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	proxy := &proxySession{sessionCore: sessionCore{id: key.ID(), name: key.Display()}, key: key}
	proxy.mruAt.Store(1)
	named := &session{sessionCore: sessionCore{id: "named", name: "named"}, tabs: []*tab{{}}}
	named.mruAt.Store(2)
	d.sessions[ephemeral.id] = ephemeral
	d.sessions[proxy.id] = proxy
	d.sessions[named.id] = named

	views, _ := d.pickerViews(nil)

	require.Equal(t, []string{"named", "work@arch", "1"}, pickerViewNames(views))
}

func pickerViewNames(views []picker.SessionView) []string {
	names := make([]string, len(views))
	for i, view := range views {
		names[i] = view.Name
	}
	return names
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
	d        *Daemon
	requests chan remoteRefreshRequest
	probeMu  sync.Mutex
}

func (c *channelRemoteCatalog) List(ctx context.Context, host string) (ports.RemoteCatalog, error) {
	c.probeMu.Lock()
	if !c.d.mu.TryLock() {
		c.probeMu.Unlock()
		return ports.RemoteCatalog{}, errors.New("daemon lock held during remote catalog I/O")
	}
	c.d.mu.Unlock()
	if !c.d.remoteCatalog.mu.TryLock() {
		c.probeMu.Unlock()
		return ports.RemoteCatalog{}, errors.New("remote catalog lock held during remote catalog I/O")
	}
	c.d.remoteCatalog.mu.Unlock()
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
	d      *Daemon
	stores chan []ports.RemoteCatalogCacheEntry
}

func (*recordingRemoteCache) Load() ([]ports.RemoteCatalogCacheEntry, error) { return nil, nil }

func (c *recordingRemoteCache) Store(entries []ports.RemoteCatalogCacheEntry) error {
	if !c.d.mu.TryLock() {
		return errors.New("daemon lock held during remote cache write")
	}
	c.d.mu.Unlock()
	if !c.d.remoteCatalog.mu.TryLock() {
		return errors.New("remote catalog lock held during remote cache write")
	}
	c.d.remoteCatalog.mu.Unlock()
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
	require.True(t, d.registerRemotePicker(instance))
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "cached", State: "running"}},
	}})

	firstGeneration := d.startRemotePickerRefresh(instance)
	first := map[string]remoteRefreshRequest{}
	for range 2 {
		request := <-catalog.requests
		first[request.host] = request
	}
	require.Contains(t, first, "arch")
	require.Contains(t, first, "mule")
	d.remoteCatalog.mu.Lock()
	require.Equal(t, []ports.RemoteCatalogSession{{Name: "cached", State: "running"}}, d.remoteCatalog.cache["arch"].Sessions, "cached rows publish before any remote completion")
	require.Equal(t, remoteHostRefreshing, d.remoteCatalog.status["arch"])
	d.remoteCatalog.mu.Unlock()

	secondGeneration := d.startRemotePickerRefresh(instance)
	require.Greater(t, secondGeneration, firstGeneration)
	for _, request := range first {
		<-request.ctx.Done()
	}
	for range 2 {
		request := <-catalog.requests
		request.result <- remoteRefreshResult{catalog: ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion}}
	}
	for range 2 {
		<-cache.stores
	}

	d.remoteCatalog.mu.Lock()
	require.Equal(t, secondGeneration, d.remoteCatalog.refresh)
	require.Equal(t, remoteHostFresh, d.remoteCatalog.status["arch"])
	require.Equal(t, remoteHostFresh, d.remoteCatalog.status["mule"])
	d.remoteCatalog.mu.Unlock()
}

func TestRemoteRefreshResultPreservesFailuresAndEvictsSuccessfulOmissions(t *testing.T) {
	now := time.Unix(200, 0)
	hosts := &remoteRefreshHostStore{hosts: []string{"arch", "mule"}}
	d, _, cache := newRemoteRefreshDaemon(t, hosts, now)
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{
		{Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "old", State: "running"}}},
		{Host: "mule", FetchedAt: time.Unix(20, 0), Sessions: []ports.RemoteCatalogSession{{Name: "stale", State: "running"}}},
	})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.refresh = 7
	d.remoteCatalog.mu.Unlock()

	require.False(t, d.applyRemoteRefreshResult(7, "mule", ports.RemoteCatalog{}, errors.New("offline")))
	d.remoteCatalog.mu.Lock()
	require.Equal(t, []ports.RemoteCatalogSession{{Name: "stale", State: "running"}}, d.remoteCatalog.cache["mule"].Sessions)
	require.Equal(t, time.Unix(20, 0), d.remoteCatalog.cache["mule"].FetchedAt)
	require.Equal(t, remoteHostUnreachable, d.remoteCatalog.status["mule"])
	d.remoteCatalog.mu.Unlock()

	require.True(t, d.applyRemoteRefreshResult(7, "arch", ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion}, nil))
	stored := <-cache.stores
	require.Equal(t, []ports.RemoteCatalogCacheEntry{
		{Host: "arch", FetchedAt: now, Sessions: []ports.RemoteCatalogSession{}},
		{Host: "mule", FetchedAt: time.Unix(20, 0), Sessions: []ports.RemoteCatalogSession{{Name: "stale", State: "running"}}},
	}, stored, "every successful write persists the newest full cache atomically")
	d.remoteCatalog.mu.Lock()
	require.Empty(t, d.remoteCatalog.cache["arch"].Sessions, "a successful omission authoritatively evicts old rows")
	require.Equal(t, remoteHostFresh, d.remoteCatalog.status["arch"])
	d.remoteCatalog.mu.Unlock()
}

func TestRemoteRefreshRejectsRemovedHostAndLateGeneration(t *testing.T) {
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d, _, cache := newRemoteRefreshDaemon(t, hosts, time.Unix(300, 0))
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "old", State: "running"}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.refresh = 9
	d.remoteCatalog.mu.Unlock()

	require.False(t, d.applyRemoteRefreshResult(8, "arch", ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion, Sessions: []ports.RemoteCatalogSession{{Name: "late", State: "running"}}}, nil))
	hosts.set()
	require.True(t, d.applyRemoteRefreshResult(9, "arch", ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion, Sessions: []ports.RemoteCatalogSession{{Name: "removed", State: "running"}}}, nil))
	require.Empty(t, <-cache.stores)
	d.remoteCatalog.mu.Lock()
	require.NotContains(t, d.remoteCatalog.cache, "arch")
	require.NotContains(t, d.remoteCatalog.status, "arch")
	d.remoteCatalog.mu.Unlock()
}

func TestRemoteRefreshVersionMismatchPreservesStaleRows(t *testing.T) {
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d, _, _ := newRemoteRefreshDaemon(t, hosts, time.Unix(400, 0))
	old := ports.RemoteCatalogCacheEntry{Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "running"}}}
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{old})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.refresh = 3
	d.remoteCatalog.mu.Unlock()

	require.False(t, d.applyRemoteRefreshResult(3, "arch", ports.RemoteCatalog{}, &ports.RemoteCatalogVersionMismatchError{Got: 19, Want: ports.ProtocolVersion}))
	d.remoteCatalog.mu.Lock()
	require.Equal(t, old, d.remoteCatalog.cache["arch"])
	require.Equal(t, remoteHostVersionMismatch, d.remoteCatalog.status["arch"])
	d.remoteCatalog.mu.Unlock()
}

func addRemoteRefreshPickerOwner(t *testing.T, d *Daemon, id domain.SessionID) (*session, *attachedClient, chan ports.Frame) {
	t.Helper()
	tr, sends := newCapturingTransport(t)
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	tb := newTab(newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	tb.stableID = string(id) + "-tab"
	sess := &session{sessionCore: sessionCore{id: id, name: string(id), client: ac}, tabs: []*tab{tb}, active: 0}
	publishTiledPaneOwners(sess, tb)
	ac.setSession(sess)
	d.sessions[id] = sess
	return sess, ac, sends
}

func TestRemoteRefreshUpdatesAllOpenPickersPreservingSelection(t *testing.T) {
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d, _, cache := newRemoteRefreshDaemon(t, hosts, time.Unix(450, 0))
	firstSession, first, _ := addRemoteRefreshPickerOwner(t, d, "first")
	_, second, _ := addRemoteRefreshPickerOwner(t, d, "second")
	before := make(map[*attachedClient]*picker.Model)

	for _, owner := range []*attachedClient{first, second} {
		owner.overlays.pickerMu.Lock()
		owner.overlays.picker = d.newPickerModel(firstSession, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{Session: "second", TabID: "second-tab"})
		owner.overlays.pickerIntent = pickerNavigate
		before[owner] = owner.overlays.picker
		owner.overlays.pickerMu.Unlock()
		d.remoteCatalog.mu.Lock()
		d.remoteCatalog.pickers[owner] = struct{}{}
		d.remoteCatalog.mu.Unlock()
	}
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.refresh = 4
	d.remoteCatalog.mu.Unlock()

	require.True(t, d.applyRemoteRefreshResult(4, "arch", ports.RemoteCatalog{
		ProtocolVersion: ports.ProtocolVersion,
		Sessions:        []ports.RemoteCatalogSession{{Name: "work", State: "running", Tabs: 2}},
	}, nil))
	<-cache.stores

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

func TestRemoteRefreshPreservesRemoteCursorAcrossCacheToProxyUpgrade(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	ownerSession, owner, _ := addRemoteRefreshPickerOwner(t, d, "local")
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: key.Name, State: "running"}},
	}})
	owner.overlays.pickerMu.Lock()
	owner.overlays.picker = d.newPickerModel(ownerSession, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{Session: key.ID(), RemoteKey: &key})
	owner.overlays.pickerIntent = pickerNavigate
	before, ok := owner.overlays.picker.Cursor()
	_, beforeSelectable := owner.overlays.picker.Selected()
	owner.overlays.pickerMu.Unlock()
	require.True(t, ok)
	require.Equal(t, &key, before.RemoteKey)
	require.True(t, beforeSelectable, "a reachable cache-derived remote row is selectable")

	proxy := &proxySession{
		sessionCore: sessionCore{id: key.ID(), name: key.Display()},
		key:         key,
		meta:        ports.SessionMeta{SessionName: key.Name, Tabs: []ports.SessionTabMeta{{Index: 0, Name: "live"}}},
	}
	d.mu.Lock()
	d.sessions[key.ID()] = proxy
	d.mu.Unlock()

	d.refreshPickerOpts(owner, pickerRefreshOptions{preserveSelection: true, nearestRow: -1})

	owner.overlays.pickerMu.Lock()
	after, ok := owner.overlays.picker.Cursor()
	_, selectable := owner.overlays.picker.Selected()
	owner.overlays.pickerMu.Unlock()
	require.True(t, ok)
	require.Equal(t, before.Session, after.Session)
	require.Equal(t, before.RemoteKey, after.RemoteKey)
	require.True(t, selectable, "the cache-to-live upgrade must not drop the row's selectability")
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
		d.remotePickerClosed(instance)
	}
	first, second := newInstance(), newInstance()
	d.remotePickerOpened(first)
	request := <-catalog.requests
	d.remotePickerOpened(second)
	<-request.ctx.Done()
	request = <-catalog.requests

	closeInstance(first)
	select {
	case <-request.ctx.Done():
		t.Fatal("refresh canceled while another picker remained open")
	default:
	}
	closeInstance(second)
	<-request.ctx.Done()

	d.remoteCatalog.mu.Lock()
	require.Empty(t, d.remoteCatalog.pickers)
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
	require.True(t, d.registerRemotePicker(instance))
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = nil
	ac.overlays.pickerMu.Unlock()
	d.remotePickerClosed(instance)

	d.remoteCatalog.mu.Lock()
	refreshBefore := d.remoteCatalog.refresh
	d.remoteCatalog.mu.Unlock()
	require.Zero(t, d.startRemotePickerRefresh(instance))

	d.remoteCatalog.mu.Lock()
	require.Empty(t, d.remoteCatalog.pickers, "the closing picker must remain unregistered")
	require.Equal(t, refreshBefore, d.remoteCatalog.refresh, "a close that wins before startup must prevent a later refresh generation")
	require.Nil(t, d.remoteCatalog.cancel, "a close that wins before startup must prevent a later cancel installation")
	d.remoteCatalog.mu.Unlock()
	select {
	case request := <-catalog.requests:
		t.Fatalf("catalog call remained running after the last picker closed: %s", request.host)
	default:
	}
}

func TestRemotePickerPublishCannotRegisterAfterSnatchClear(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	ac.roleGeneration.Store(1)
	model := d.newPickerModel(sess, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	registrationReached := make(chan struct{})
	allowRegistration := make(chan struct{})
	ac.overlays.beforeRemotePickerRegistration = func() {
		close(registrationReached)
		<-allowRegistration
	}
	published := make(chan struct{})
	go func() {
		d.publishPicker(sess, ac, model, pickerNavigate, moveSourceLocator{})
		close(published)
	}()
	<-registrationReached

	token := attachmentRoleToken{
		sess: sess, ac: ac, role: attachmentSnatched,
		generation: 1, transport: ac.transportSnapshot(),
	}
	require.True(t, d.clearForSnatch(token))
	close(allowRegistration)
	<-published

	d.remoteCatalog.mu.Lock()
	_, registered := d.remoteCatalog.pickers[ac]
	d.remoteCatalog.mu.Unlock()
	require.False(t, registered, "a picker cleared before delayed registration must not become a refresh owner")
}

func TestRemotePickerStaleAfterCloseCannotRemoveReopenedPicker(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	first := d.newPickerModel(sess, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, first, pickerNavigate, moveSourceLocator{})

	staleClose := d.pickerListInputState(ac)
	ac.overlays.pickerMu.Lock()
	staleClose.closeLocked()
	ac.overlays.pickerMu.Unlock()

	reopened := d.newPickerModel(sess, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, reopened, pickerNavigate, moveSourceLocator{})
	staleClose.afterClose()

	d.remoteCatalog.mu.Lock()
	_, registered := d.remoteCatalog.pickers[ac]
	d.remoteCatalog.mu.Unlock()
	require.True(t, registered, "stale close cleanup must retain the reopened picker registration")
	ac.overlays.pickerMu.Lock()
	require.Same(t, reopened, ac.overlays.picker)
	ac.overlays.pickerMu.Unlock()
}

func TestRemotePickerStaleRefreshCannotOverwriteReopenedModel(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	first := d.newPickerModel(sess, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, first, pickerNavigate, moveSourceLocator{})

	rebuildReached := make(chan struct{})
	allowPublication := make(chan struct{})
	ac.overlays.afterPickerRefreshBuild = func(*picker.Model) {
		close(rebuildReached)
		<-allowPublication
	}
	refreshed := make(chan struct{})
	go func() {
		d.refreshRemoteOpenPickers()
		close(refreshed)
	}()
	<-rebuildReached

	d.closePicker(ac)
	reopened := d.newPickerModel(sess, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, reopened, pickerNavigate, moveSourceLocator{})
	close(allowPublication)
	<-refreshed

	ac.overlays.pickerMu.Lock()
	require.Same(t, reopened, ac.overlays.picker, "a rebuild for the old picker must not publish into its replacement")
	ac.overlays.pickerMu.Unlock()
}

func TestRemoteRowsRemainNonSelectableDuringRefreshPhase(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	for _, availability := range []picker.RemoteAvailability{picker.RemoteCached, picker.RemoteFresh, picker.RemoteStale, picker.RemoteVersionMismatch} {
		model := picker.New([]picker.SessionView{{
			ID: key.ID(), Name: key.Display(), RemoteKey: &key, RemoteAvailability: availability,
			ConnectOnly: true, RemoteAttachReady: false,
		}}, picker.SelectionConfig{Mode: picker.SelectNavigationTab})
		_, selectable := model.Selected()
		require.False(t, selectable, "availability %d became remotely attachable in phase 5", availability)
	}
}
