package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
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

func TestRemotePickerCatalogExpiresAtAttachTTL(t *testing.T) {
	now := time.Unix(100, 0)
	clock := &remotePreviewTestClock{now: now}
	d := newTestDaemon(t, nil, clock)
	lifecycle := remoteLifecycleForTest()
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: now.Add(-remotePickerAttachTTL), Sessions: []ports.RemoteCatalogSession{{
			LifecycleID: lifecycle, Name: "work", State: "running", Tabs: []ports.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
		}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()

	entries := d.remotePickerCatalogSnapshot()
	require.Len(t, entries, 1)
	require.Equal(t, remoteHostStale, entries[0].status)
	target := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	require.False(t, d.remotePickerTargetReadyTarget(target), "attachment must reject a catalogue exactly at the TTL boundary")
}

func TestRemotePickerSelectsCatalogActiveTab(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	lifecycle := remoteLifecycleForTest()
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{
			LifecycleID: lifecycle,
			Name:        "vive",
			State:       "running",
			Tabs: []ports.RemoteCatalogTab{
				{ID: "tab-first", Index: 0, Name: "first"},
				{ID: "tab-active", Index: 1, Name: "active"},
			},
			ActiveTabID: "tab-active",
		}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.status["arch"] = remoteHostFresh
	d.remoteCatalog.mu.Unlock()

	views, _ := d.pickerViews(nil, nil)
	require.Len(t, views, 1)
	require.Equal(t, 1, views[0].Active)
	require.NotNil(t, views[0].RemoteTarget)
	require.Equal(t, domain.TabStableID("tab-active"), views[0].RemoteTarget.LiveTabID)

	model := d.newPickerModel(nil, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	target, ok := model.Selected()
	require.True(t, ok)
	require.NotNil(t, target.RemoteTarget)
	require.Equal(t, domain.TabStableID("tab-active"), target.RemoteTarget.LiveTabID)
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
		Host: "arch", FetchedAt: fetchedAt, Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "running"}},
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
		request := receiveRemotePicker(t, catalog.requests, "catalog request")
		first[request.host] = request
	}
	require.Contains(t, first, "arch")
	require.Contains(t, first, "mule")
	requireNoRemoteLockViolations(t, catalog, nil)
	d.remoteCatalog.mu.Lock()
	require.Equal(t, []ports.RemoteCatalogSession{{Name: "cached", State: "running"}}, d.remoteCatalog.cache["arch"].Sessions, "cached rows publish before any remote completion")
	require.Equal(t, remoteHostRefreshing, d.remoteCatalog.status["arch"])
	d.remoteCatalog.mu.Unlock()

	secondGeneration := d.startRemotePickerRefresh(instance)
	require.Greater(t, secondGeneration, firstGeneration)
	for _, request := range first {
		receiveRemotePickerClose(t, request.ctx.Done(), "catalog request cancellation")
	}
	for range 2 {
		request := receiveRemotePicker(t, catalog.requests, "catalog request")
		request.result <- remoteRefreshResult{catalog: ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion}}
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
	require.True(t, d.registerRemotePicker(instance))

	d.startRemotePickerRefresh(instance)
	request := receiveRemotePicker(t, catalog.requests, "catalog request")
	request.result <- remoteRefreshResult{catalog: ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion}}
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
	require.True(t, d.registerRemotePicker(instance))

	require.NotZero(t, d.startRemotePickerRefresh(instance))
	d.remoteCatalog.mu.Lock()
	require.NotEqual(t, remoteHostRefreshing, d.remoteCatalog.status["arch"])
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
	stored := receiveRemotePicker(t, cache.stores, "remote cache store")
	require.Equal(t, []ports.RemoteCatalogCacheEntry{
		{Host: "arch", FetchedAt: now, Sessions: []ports.RemoteCatalogSession{}},
		{Host: "mule", FetchedAt: time.Unix(20, 0), Sessions: []ports.RemoteCatalogSession{{Name: "stale", State: "running"}}},
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
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "old", State: "running"}},
	}})
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.refresh = 9
	d.remoteCatalog.mu.Unlock()

	require.False(t, d.applyRemoteRefreshResult(8, "arch", ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion, Sessions: []ports.RemoteCatalogSession{{Name: "late", State: "running"}}}, nil))
	hosts.set()
	require.True(t, d.applyRemoteRefreshResult(9, "arch", ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion, Sessions: []ports.RemoteCatalogSession{{Name: "removed", State: "running"}}}, nil))
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
		{name: "invalid reason", session: ports.RemoteCatalogSession{LifecycleID: lifecycle, Name: "work", State: "running", Reason: "bogus"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
			d, _, _ := newRemoteRefreshDaemon(t, hosts, time.Unix(400, 0))
			d.remoteCatalog.refresh = 1
			catalog := ports.RemoteCatalog{ProtocolVersion: ports.ProtocolVersion, SchemaVersion: ports.RemoteCatalogSchemaVersion, Sessions: []ports.RemoteCatalogSession{test.session}}
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

func TestRemotePickerHandoffSendsTargetAndLeavesNoShadowSession(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "running"}},
	}})
	sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
	token := sess.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key}
	require.NoError(t, d.sendRemoteAttachTargetForAttachment(token, target, key, sessionHandoffGuard{closePicker: true}, "picker-select"))

	frame := receiveRemotePicker(t, sends, "attach target")
	require.Equal(t, ports.MsgAttachTarget, frame.Type)
	got, err := ports.UnmarshalAttachTarget(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.AttachTarget{Endpoint: "arch", Session: "work", Intent: ports.IntentAttach}, got)
	require.Nil(t, ac.currentAttachmentSession())
	d.mu.Lock()
	require.NotContains(t, d.sessions, key.ID(), "remote picker handoff must not create a local session shadow")
	d.mu.Unlock()
}

func TestRemotePickerHandoffSendFailureKeepsPickerOpen(t *testing.T) {
	d := newRemotePickerDaemon(nil)
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: "arch", FetchedAt: time.Unix(10, 0), Sessions: []ports.RemoteCatalogSession{{Name: "work", State: "running"}},
	}})
	const sendErr = "remote attach send failed"
	tr := &remotePickerSendErrorTransport{err: errors.New(sendErr)}
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	tb := newTab(newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	sess := &session{sessionCore: sessionCore{id: "local", name: "local", attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{tb}}
	publishTiledPaneOwners(sess, tb)
	ac.setSession(sess)
	d.sessions[sess.id] = sess
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = picker.New(nil, picker.SelectionConfig{})
	ac.overlays.pickerGeneration++
	ac.overlays.pickerMu.Unlock()

	gone := make(chan struct{})
	d.afterClientGoneDetach = func() { close(gone) }
	token := sess.attachmentToken(ac, tr)
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	defer effect.End()
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key}
	err := d.sendRemoteAttachTargetForAttachment(token, target, key, sessionHandoffGuard{closePicker: true}, "picker-select")
	require.EqualError(t, err, sendErr)
	require.True(t, ac.overlays.pickerActive(), "failed control send must leave the picker open")
	select {
	case <-gone:
		t.Fatal("failed control send reached clientGoneForAttachment")
	default:
	}
	require.Same(t, sess, ac.currentAttachmentSession())
}

type remotePickerSendErrorTransport struct {
	err error
}

func (t *remotePickerSendErrorTransport) Send(ports.Frame) error   { return t.err }
func (*remotePickerSendErrorTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (*remotePickerSendErrorTransport) Close() error               { return nil }

func addRemoteRefreshPickerOwner(t *testing.T, d *Daemon, id domain.SessionID) (*session, *attachedClient, chan ports.Frame) {
	t.Helper()
	tr, sends := newCapturingTransport(t)
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
		require.True(t, d.registerRemotePicker(instance))
	}
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.refresh = 4
	d.remoteCatalog.mu.Unlock()

	require.True(t, d.applyRemoteRefreshResult(4, "arch", ports.RemoteCatalog{
		ProtocolVersion: ports.ProtocolVersion,
		Sessions:        []ports.RemoteCatalogSession{{Name: "work", State: "running", Tabs: 2}},
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
			_, registered := d.remoteCatalog.pickers[ac]
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

func TestParkedPickerExpiryClosesCapturedGenerationAndCancelsRefresh(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	hosts := &remoteRefreshHostStore{hosts: []string{"arch"}}
	d, catalog, _ := newRemoteRefreshDaemon(t, hosts, time.Unix(475, 0))
	d.clock = clock
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	ac.resumeCapable = true
	d.publishPicker(sess, ac, d.newPickerModel(sess, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{}), pickerNavigate, moveSourceLocator{})
	request := receiveRemotePicker(t, catalog.requests, "catalog request")

	require.True(t, d.parkAttachment(sess, ac))
	timer := receiveRemotePicker(t, clock.timers, "park expiry timer")
	timer.ch <- clock.Now()
	receiveRemotePickerClose(t, request.ctx.Done(), "catalog request cancellation")

	ac.overlays.pickerMu.Lock()
	require.Nil(t, ac.overlays.picker)
	ac.overlays.pickerMu.Unlock()
	d.remoteCatalog.mu.Lock()
	require.NotContains(t, d.remoteCatalog.pickers, ac)
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
	resumedSess, resumedAC, ok, err := d.resumeParked(helloResumeCapable(ports.IntentResume, sess.name, token), tr, domain.Size{Cols: 80, Rows: 24})
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
		d.remotePickerClosed(instance)
	}
	first, second := newInstance(), newInstance()
	d.remotePickerOpened(first)
	request := receiveRemotePicker(t, catalog.requests, "catalog request")
	d.remotePickerOpened(second)
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
		d.refreshRemoteOpenPickers()
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

func TestRemoteRowsBecomeSelectableOnlyAfterOwnershipCompletion(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	for _, status := range []remoteHostStatus{remoteHostCached, remoteHostRefreshing, remoteHostFresh, remoteHostUnreachable} {
		view := remotePickerView(key, ports.RemoteCatalogSession{Name: key.Name, State: "running"}, status, time.Unix(1, 0))
		_, selectable := picker.New([]picker.SessionView{view}, picker.SelectionConfig{Mode: picker.SelectNavigationTab}).Selected()
		require.True(t, selectable, "status %d is ready after ownership completion", status)
	}
	mismatch := remotePickerView(key, ports.RemoteCatalogSession{Name: key.Name, State: "running"}, remoteHostVersionMismatch, time.Time{})
	require.False(t, mismatch.RemoteAttachReady)
	_, selectable := picker.New([]picker.SessionView{mismatch}, picker.SelectionConfig{Mode: picker.SelectNavigationTab}).Selected()
	require.False(t, selectable)
}
