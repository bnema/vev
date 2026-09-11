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
	appports "github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
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

func newRemotePickerDaemon() *Daemon {
	return New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// seedRemoteDirectory installs a fully defensive directory publication on the
// daemon. Presentation reads it snapshot-only and performs no I/O.
func seedRemoteDirectory(t *testing.T, d *Daemon, hosts ...ports.RemoteHostSnapshot) {
	t.Helper()
	for i := range hosts {
		hosts[i].Rank = i
		if hosts[i].DisplayOrigin == "" && hosts[i].Endpoint != "" {
			hosts[i].DisplayOrigin = domain.RemoteDisplayOrigin(hosts[i].Endpoint)
		}
	}
	d.remoteDirectory = &stubRemoteDirectory{
		snapshot: ports.RemoteDirectorySnapshot{Revision: 1, Initialized: true, Hosts: hosts},
	}
}

func reachableDirectoryHost(endpoint string, fetchedAt time.Time, sessions ...catalogue.RemoteCatalogSession) ports.RemoteHostSnapshot {
	return ports.RemoteHostSnapshot{
		Endpoint:       endpoint,
		Availability:   domain.RemoteAvailabilityReachable,
		LastSuccess:    fetchedAt,
		InventoryKnown: true,
		Sessions:       sessions,
	}
}

func directorySessionForTest(name string) catalogue.RemoteCatalogSession {
	return catalogue.RemoteCatalogSession{
		LifecycleID: remoteLifecycleForTest(), Name: name, State: catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-" + name, Index: 0, Name: "main"}},
		ActiveTabID: "tab-" + name,
	}
}

func TestRemotePickerStoppedRowsUseCanonicalStateAndSafeSelection(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle}
	session := catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionDown,
		Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
	}
	host := reachableDirectoryHost("arch", time.Unix(100, 0), session)
	view := remotePickerView(key, session, host, time.Unix(100, 0))
	require.True(t, view.Stopped)
	require.NotNil(t, view.RemoteTarget)
	require.True(t, view.RemoteTarget.Stopped)
	require.Equal(t, pickerRemoteRestart, view.RemoteActivation, "structured stopped rows have an explicit safe restore target")
	require.Equal(t, "stopped — Enter to restart", view.RemoteDetail)

}

func TestRemotePickerClampsStoppedOrdinalTabCount(t *testing.T) {
	tabs := make([]catalogue.RemoteCatalogTab, math.MaxUint16+1)
	tabs[0] = catalogue.RemoteCatalogTab{Name: "main"}
	view := remotePickerView(domain.RemoteSessionKey{Host: "arch", Name: "work"}, catalogue.RemoteCatalogSession{
		LifecycleID: remoteLifecycleForTest(), Name: "work", State: "down", Tabs: tabs,
	}, reachableDirectoryHost("arch", time.Unix(100, 0)), time.Unix(100, 0))

	require.NotNil(t, view.RemoteTarget)
	require.Equal(t, uint16(math.MaxUint16), view.RemoteTarget.StoppedTab.ExpectedCount)
}

func TestRemotePickerUsesCompactUpDetailRegardlessOfAttachment(t *testing.T) {
	for _, attached := range []bool{false, true} {
		session := catalogue.RemoteCatalogSession{
			LifecycleID: remoteLifecycleForTest(), Name: "work", State: "up", Attached: attached,
			Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0}},
		}
		view := remotePickerView(domain.RemoteSessionKey{Host: "remote", Name: "work"}, session,
			reachableDirectoryHost("remote", time.Now(), session), time.Now())
		require.Equal(t, "up", view.RemoteDetail)
	}
}

func TestRemotePickerScopesSameLifecycleBytesToEachEndpoint(t *testing.T) {
	d := newRemotePickerDaemon()
	lifecycle := remoteLifecycleForTest()
	session := catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "work", State: "up",
		Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
	}
	fetchedAt := time.Unix(100, 0)
	seedRemoteDirectory(t, d,
		reachableDirectoryHost("remote", fetchedAt, session),
		reachableDirectoryHost("vev@remote", fetchedAt, session),
	)

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

// TestRemoteCatalogTargetReadyIgnoresAge proves freshness is presentation,
// not attach authority: a stale-but-known target stays valid and the
// destination rejects precisely instead of the source declaring it gone.
func TestRemoteCatalogTargetReadyIgnoresAge(t *testing.T) {
	now := time.Unix(100, 0)
	d := newTestDaemon(t, nil, &remotePickerClock{now: now})
	lifecycle := remoteLifecycleForTest()
	session := catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "work", State: "up",
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
		ActiveTabID: "tab-1",
	}
	seedRemoteDirectory(t, d, reachableDirectoryHost("arch", now.Add(-24*time.Hour), session))

	target := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	require.True(t, d.remoteCatalogTargetReady(target), "known inventory stays attemptable at any age")

	views, _ := d.pickerViews(nil, nil)
	require.Len(t, views, 1)
	require.Equal(t, pickerRemoteAttach, views[0].RemoteActivation)
	require.Equal(t, pickerRemoteStale, views[0].RemoteAvailability)
}

func TestRemotePickerSelectsCatalogActiveTab(t *testing.T) {
	tests := []struct {
		name        string
		tabs        []catalogue.RemoteCatalogTab
		activeTabID string
		wantActive  int
		wantTabID   domain.TabStableID
	}{
		{
			name: "active first",
			tabs: []catalogue.RemoteCatalogTab{
				{ID: "tab-active", Index: 0, Name: "active"},
				{ID: "tab-last", Index: 1, Name: "last"},
			},
			activeTabID: "tab-active",
			wantActive:  0,
			wantTabID:   "tab-active",
		},
		{
			name: "active last",
			tabs: []catalogue.RemoteCatalogTab{
				{ID: "tab-first", Index: 0, Name: "first"},
				{ID: "tab-active", Index: 1, Name: "active"},
			},
			activeTabID: "tab-active",
			wantActive:  1,
			wantTabID:   "tab-active",
		},
		{
			name: "empty active tab ID falls back to first stable tab",
			tabs: []catalogue.RemoteCatalogTab{
				{ID: "tab-first", Index: 0, Name: "first"},
				{ID: "", Index: 1, Name: "malformed"},
			},
			wantActive: 0,
			wantTabID:  "tab-first",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := newRemotePickerDaemon()
			lifecycle := remoteLifecycleForTest()
			session := catalogue.RemoteCatalogSession{
				LifecycleID: lifecycle,
				Name:        "vive",
				State:       "up",
				Tabs:        test.tabs,
				ActiveTabID: test.activeTabID,
			}
			seedRemoteDirectory(t, d, reachableDirectoryHost("arch", time.Unix(10, 0), session))

			views, _ := d.pickerViews(nil, nil)
			require.Len(t, views, 1)
			require.Equal(t, test.wantActive, views[0].Active)
			require.NotNil(t, views[0].RemoteTarget)
			require.Equal(t, test.wantTabID, views[0].RemoteTarget.LiveTabID)

			set := pickerLineSetFor(views, protocol.PickerIntentNavigation, pickerSourceFilter{}, pickerSourceFilter{})
			target, ok := firstSelectableTarget(t, set)
			require.True(t, ok)
			require.NotNil(t, target.RemoteTarget)
			require.Equal(t, test.wantTabID, target.RemoteTarget.LiveTabID)
		})
	}
}

func TestRemotePickerAvailabilityUsesCachedRowsForFailures(t *testing.T) {
	session := catalogue.RemoteCatalogSession{
		LifecycleID: remoteLifecycleForTest(), Name: "work", State: "up",
		Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-work", Index: 0, Name: "main"}}, ActiveTabID: "tab-work",
	}
	tests := []struct {
		name         string
		host         ports.RemoteHostSnapshot
		want         pickerRemoteAvailability
		wantActivate bool
	}{
		{
			name: "unknown renders cached and stays attemptable",
			host: ports.RemoteHostSnapshot{
				Endpoint: "arch", Availability: domain.RemoteAvailabilityUnknown,
				Checking: true, InventoryKnown: true, Sessions: []catalogue.RemoteCatalogSession{session},
			},
			want: pickerRemoteCached, wantActivate: true,
		},
		{
			name: "unreachable with inventory renders stale and stays attemptable",
			host: ports.RemoteHostSnapshot{
				Endpoint: "arch", Availability: domain.RemoteAvailabilityUnreachable,
				LastSuccess: time.Unix(10, 0), InventoryKnown: true,
				Sessions: []catalogue.RemoteCatalogSession{session},
			},
			want: pickerRemoteStale, wantActivate: true,
		},
		{
			name: "version mismatch gates activation",
			host: ports.RemoteHostSnapshot{
				Endpoint: "arch", Availability: domain.RemoteAvailabilityIncompatible,
				InventoryKnown: true, Sessions: []catalogue.RemoteCatalogSession{session},
			},
			want: pickerRemoteVersionMismatch, wantActivate: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := newRemotePickerDaemon()
			seedRemoteDirectory(t, d, test.host)

			views, _ := d.pickerViews(nil, nil)

			require.Len(t, views, 1)
			require.Equal(t, test.want, views[0].RemoteAvailability)
			if test.wantActivate {
				require.NotEqual(t, pickerRemoteUnavailable, views[0].RemoteActivation)
			} else {
				require.Equal(t, pickerRemoteUnavailable, views[0].RemoteActivation)
			}
		})
	}
}

func TestRemotePickerStaleDetailUsesLastSuccessfulFetch(t *testing.T) {
	fetchedAt := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	session := catalogue.RemoteCatalogSession{Name: "work", State: "up"}
	d := newRemotePickerDaemon()
	seedRemoteDirectory(t, d, ports.RemoteHostSnapshot{
		Endpoint: "arch", Availability: domain.RemoteAvailabilityUnreachable,
		LastSuccess: fetchedAt, InventoryKnown: true,
		Sessions: []catalogue.RemoteCatalogSession{session},
	})

	views, _ := d.pickerViews(nil, nil)

	require.Len(t, views, 1)
	require.Equal(t, "stale since "+fetchedAt.Format(time.RFC3339), views[0].RemoteDetail)
}

func TestRemotePickerNoSessionHostFailuresRemainVisible(t *testing.T) {
	for _, test := range []struct {
		name       string
		host       ports.RemoteHostSnapshot
		want       pickerRemoteAvailability
		wantDetail string
	}{
		{
			name:       "unreachable",
			host:       ports.RemoteHostSnapshot{Endpoint: "arch", Availability: domain.RemoteAvailabilityUnreachable},
			want:       pickerRemoteStale,
			wantDetail: "unreachable",
		},
		{
			name:       "version mismatch",
			host:       ports.RemoteHostSnapshot{Endpoint: "arch", Availability: domain.RemoteAvailabilityIncompatible},
			want:       pickerRemoteVersionMismatch,
			wantDetail: "version mismatch",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := newRemotePickerDaemon()
			seedRemoteDirectory(t, d, test.host)

			views, _ := d.pickerViews(nil, nil)

			require.Len(t, views, 1)
			require.Equal(t, "arch", views[0].Name)
			require.Equal(t, test.want, views[0].RemoteAvailability)
			require.Equal(t, test.wantDetail, views[0].RemoteDetail)
			model := picker.New(pickerLineSetFor(views, protocol.PickerIntentNavigation, pickerSourceFilter{}, pickerSourceFilter{}).lines, picker.Config{Intent: protocol.PickerIntentNavigation})
			_, selectable := model.Selected()
			require.False(t, selectable)
		})
	}
}

// TestRemotePickerStaleAndCheckingInventoryRemainsAttemptable is the
// presentation half of the P1.1 regression: age and in-flight observation
// render while rows stay attemptable; only broken/invalid/incompatible
// states gate activation.
func TestRemotePickerStaleAndCheckingInventoryRemainsAttemptable(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	session := catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "work", State: "up",
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "main"}},
		ActiveTabID: "tab-1",
	}
	now := time.Unix(1_000, 0)
	for _, test := range []struct {
		name string
		host ports.RemoteHostSnapshot
	}{
		{name: "stale age", host: reachableDirectoryHost("arch", now.Add(-time.Hour), session)},
		{name: "checking", host: func() ports.RemoteHostSnapshot {
			host := reachableDirectoryHost("arch", now, session)
			host.Checking = true
			return host
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			view := remotePickerView(
				domain.RemoteSessionKey{Host: "arch", Name: "work"},
				session, test.host, now,
			)
			require.Equal(t, pickerRemoteAttach, view.RemoteActivation)
			require.NotNil(t, view.RemoteTarget)
			require.NoError(t, view.RemoteTarget.Validate())
		})
	}
}

// TestRemotePickerOverlayCausesNoDirectoryIO proves presentation performs no
// registry, cache, or observation I/O: opening, rebuilding, and closing the
// picker only re-reads the latest snapshot.
func TestRemotePickerOverlayCausesNoDirectoryIO(t *testing.T) {
	d := newRemotePickerDaemon()
	directory := &countingRemoteDirectory{
		snapshot: ports.RemoteDirectorySnapshot{Initialized: true},
	}
	d.remoteDirectory = directory
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
	effect := admitPickerEffectForTest(t, sess, ac)
	require.NoError(t, d.openPickerForAttachment(ac, effect, protocol.PickerIntentNavigation, moveSourceLocator{}, 0))
	d.refreshPickerSnapshot(ac)
	ac.overlays.pickerMu.Lock()
	interaction := ac.overlays.pickerInteraction
	ac.overlays.pickerMu.Unlock()
	require.True(t, d.closePickerForAttachment(ac, effect, interaction))

	require.Zero(t, directory.reconciles, "overlay lifecycle must never request reconciliation")
	require.GreaterOrEqual(t, directory.snapshots, 1, "presentation renders from snapshots")
}

type countingRemoteDirectory struct {
	mu         sync.Mutex
	snapshot   ports.RemoteDirectorySnapshot
	snapshots  int
	reconciles int
	registry   int
}

func (s *countingRemoteDirectory) Snapshot() ports.RemoteDirectorySnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots++
	return s.snapshot.Clone()
}

func (s *countingRemoteDirectory) Subscribe() ports.RemoteDirectorySubscription {
	return &stubDirectorySubscription{changed: make(chan struct{}, 1)}
}

func (s *countingRemoteDirectory) RequestReconcile(endpoint string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconciles++
}

func (s *countingRemoteDirectory) RegistryChanged() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registry++
}

type fixedRemoteRefreshClock struct{ now time.Time }

func (c fixedRemoteRefreshClock) Now() time.Time                      { return c.now }
func (fixedRemoteRefreshClock) NewTimer(time.Duration) appports.Timer { return stubTimer{} }

type remotePickerClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *remotePickerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (*remotePickerClock) NewTimer(time.Duration) appports.Timer { return stubTimer{} }
func (c *remotePickerClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func remoteCatalogForTest(sessions ...catalogue.RemoteCatalogSession) catalogue.RemoteCatalog {
	return catalogue.RemoteCatalog{
		ProtocolVersion: protocol.Version,
		SchemaVersion:   catalogue.RemoteCatalogSchemaVersion,
		Sessions:        append([]catalogue.RemoteCatalogSession{}, sessions...),
	}
}

func seedPickerDirectoryHost(t *testing.T, d *Daemon, endpoint string, fetchedAt time.Time, sessions ...catalogue.RemoteCatalogSession) {
	t.Helper()
	seedRemoteDirectory(t, d, reachableDirectoryHost(endpoint, fetchedAt, sessions...))
}

func TestRemotePickerHandoffSendsTargetAndLeavesNoShadowSession(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	remoteSession := catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionUp,
		Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-1"}},
	}
	d := newRemotePickerDaemon()
	seedPickerDirectoryHost(t, d, "arch", time.Unix(10, 0), remoteSession)
	sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
	token := sess.captureAttachmentCapability(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}
	require.NoError(t, d.sendRemoteAttachTargetForAttachment(effect, target, sessionHandoffGuard{}, "picker-select"))

	frame := receiveRemotePicker(t, sends, "attach target")
	require.Equal(t, wire.MsgAttachTarget, frame.Type)
	got, err := wire.UnmarshalAttachTarget(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.AttachTarget{Endpoint: "arch", Session: "work", Intent: protocol.IntentAttach, RemoteTarget: &remoteTarget, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}, got)
	require.Nil(t, ac.currentAttachmentSession())
	d.mu.Lock()
	require.NotContains(t, d.sessions, key.ID(), "remote picker handoff must not create a local session shadow")
	d.mu.Unlock()
}

func TestRemotePickerSelectsStoppedRemoteTabAndRestoresIt(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	remoteSession := catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle,
		Name:        "work",
		State:       "down",
		Tabs: []catalogue.RemoteCatalogTab{
			{ID: "tab-a", Name: "alpha", Index: 0},
			{ID: "tab-b", Name: "beta", Index: 1},
		},
	}
	// The stopping tab is offered by the daemon's published keys: a stopped
	// remote session keeps a structured restore target per tab.
	local := newRemotePickerDaemon()
	seedPickerDirectoryHost(t, local, "arch", time.Unix(10, 0), remoteSession)
	localSession, localAttachment, _ := addRemoteRefreshPickerOwner(t, local, "local")
	effect := admitPickerEffectForTest(t, localSession, localAttachment)
	require.NoError(t, local.openPickerForAttachment(localAttachment, effect, protocol.PickerIntentNavigation, moveSourceLocator{}, 0))
	localAttachment.overlays.pickerMu.Lock()
	selected, ok := selectableTargetMatching(t, pickerLineSet{keys: localAttachment.overlays.pickerKeys}, func(target picker.Target) bool {
		return target.RemoteTarget != nil && target.RemoteTarget.StoppedTab.StableID == "tab-b"
	})
	localAttachment.overlays.pickerMu.Unlock()
	if !ok {
		localAttachment.overlays.pickerMu.Lock()
		for candidate, target := range localAttachment.overlays.pickerKeys {
			if target.RemoteTarget != nil {
				t.Logf("key=%q session=%q remote=%+v", candidate, target.Session, *target.RemoteTarget)
			} else {
				t.Logf("key=%q session=%q remote=nil", candidate, target.Session)
			}
		}
		localAttachment.overlays.pickerMu.Unlock()
	}
	require.True(t, ok)
	require.NotNil(t, selected.RemoteTarget)
	require.True(t, selected.RemoteTarget.Stopped)
	require.Equal(t, domain.NewStableTabSelector("tab-b"), selected.RemoteTarget.StoppedTab)
	require.Equal(t, selected.RemoteKey.ID(), selected.Session, "the row identity must match its remote key")
	require.True(t, local.remoteCatalogTargetReady(*selected.RemoteTarget), "the published stopped target must be ready to attach")

	// The handoff itself is covered by TestRemotePickerHandoffSendsTargetAndLeavesNoShadowSession;
	// this suite pins the per-tab restore target the stopped rows publish.
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
		Version: protocol.Version, Intent: protocol.IntentAttach, Name: selected.RemoteTarget.SessionName,
		Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: selected.RemoteTarget,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}, transport)
	require.NoError(t, err)
	t.Cleanup(func() { remote.clientGone(restored, attachment, transport, false) })
	require.Equal(t, lifecycle, restored.incarnation)
	require.Equal(t, domain.TabStableID("tab-b"), attachment.viewSnapshot().tabID)
}

func TestRemotePickerResurrectsStoppedRemoteSessionWithoutTabMetadata(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	remoteSession := catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle,
		Name:        "work",
		State:       "down",
		Tabs:        []catalogue.RemoteCatalogTab{},
	}
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	host := reachableDirectoryHost("arch", time.Unix(10, 0), remoteSession)
	remoteView := remotePickerView(key, remoteSession, host, time.Unix(10, 0))
	require.Equal(t, pickerRemoteRestart, remoteView.RemoteActivation)

	set := pickerLineSetFor([]pickerSessionView{
		{ID: "local", Name: "local", Tabs: []pickerTabEntry{{TabID: "local-tab", Name: "local"}}},
		remoteView,
	}, protocol.PickerIntentNavigation, pickerSourceFilter{}, pickerSourceFilter{})
	selected, ok := selectableTargetMatching(t, set, func(target picker.Target) bool {
		return target.RemoteTarget != nil
	})
	require.True(t, ok)
	require.NotNil(t, selected.RemoteTarget)
	require.True(t, selected.RemoteTarget.Stopped)
	require.Equal(t, domain.TabSelector{}, selected.RemoteTarget.StoppedTab)

	local := newRemotePickerDaemon()
	seedPickerDirectoryHost(t, local, "arch", time.Unix(10, 0), remoteSession)
	localSession, localAttachment, sends := addRemoteRefreshPickerOwner(t, local, "local")
	token := localSession.captureAttachmentCapability(localAttachment, localAttachment.transport())
	effect, admitted := localAttachment.beginAttachmentEffect(token)
	require.True(t, admitted)
	require.NoError(t, local.sendRemoteAttachTargetForAttachment(effect, selected, sessionHandoffGuard{}, "picker-select"))

	frame := receiveRemotePicker(t, sends, "stopped remote target without tab metadata")
	handoff, err := wire.UnmarshalAttachTarget(frame.Payload)
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
			d := newRemotePickerDaemon()
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
			require.Equal(t, wire.MsgNavigationAction, frame.Type)
			directive, err := wire.UnmarshalNavigationDirective(frame.Payload)
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
	remoteSession := catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionUp,
		Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-1"}},
	}
	d := newRemotePickerDaemon()
	seedPickerDirectoryHost(t, d, "arch", time.Unix(10, 0), remoteSession)
	cause := errors.New("remote attach send failed")
	tr := portsmocks.NewMockServerConnection(t)
	tr.EXPECT().SendServer(mock.Anything).Return(cause)
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "local", tr)
	openPickerStateForTest(ac)

	gone := make(chan struct{})
	d.afterClientGoneDetach = func() { close(gone) }
	token := sess.captureAttachmentCapability(ac, tr)
	sendEffect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer sendEffect.End()
	key := domain.RemoteSessionKey{Host: "arch", Name: "work", LifecycleID: lifecycle, DisplayOrigin: "arch"}
	remoteTarget := domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle, SessionName: "work", LiveTabID: "tab-1"}
	target := picker.Target{Session: key.ID(), RemoteKey: &key, RemoteTarget: &remoteTarget, TabID: "tab-1"}
	err := d.sendRemoteAttachTargetForAttachment(sendEffect, target, sessionHandoffGuard{}, "picker-select")
	var userErr *domain.UserError
	require.ErrorAs(t, err, &userErr)
	require.Equal(t, "couldn't attach to remote session", userErr.Msg)
	require.ErrorIs(t, err, cause)
	require.True(t, ac.overlays.pickerClientActive(), "failed control send must leave the picker open")
	select {
	case <-gone:
		t.Fatal("failed control send reached clientGoneForAttachment")
	default:
	}
	require.Same(t, sess, ac.currentAttachmentSession())
}

// selectableTargetMatching returns the resolved target of the first
// selectable line whose target satisfies the predicate. A key-only line set
// (every published key is selectable) is accepted too.
func selectableTargetMatching(t *testing.T, set pickerLineSet, match func(picker.Target) bool) (picker.Target, bool) {
	t.Helper()
	if len(set.lines) == 0 {
		for _, target := range set.keys {
			if match(target) {
				return target, true
			}
		}
		return picker.Target{}, false
	}
	for _, line := range set.lines {
		if line.Actions == 0 {
			continue
		}
		target, ok := set.keys[line.Key]
		if !ok {
			continue
		}
		if match(target) {
			return target, true
		}
	}
	return picker.Target{}, false
}

// openPickerStateForTest marks the attachment as owning an interaction without
// publishing frames. Suites that observe teardown or send failures on the wire
// must not hold a live effect while they run.
func openPickerStateForTest(ac *attachedClient) uint64 {
	ac.overlays.pickerMu.Lock()
	defer ac.overlays.pickerMu.Unlock()
	ac.overlays.pickerInteraction++
	if ac.overlays.pickerInteraction == 0 {
		ac.overlays.pickerInteraction = 1
	}
	ac.overlays.pickerOpen = true
	ac.overlays.pickerRevisions = map[string]uint64{servingPickerSourceID: 1}
	return ac.overlays.pickerInteraction
}

// admitPickerEffectForTest admits one effect for the attachment, so typed
// picker opens and closes travel the same path a client frame would.
func admitPickerEffectForTest(t *testing.T, sess *session, ac *attachedClient) *attachmentEffect {
	t.Helper()
	effect, admitted := ac.beginAttachmentEffect(captureAttachmentCapability(sess, ac, ac.transport()))
	require.True(t, admitted)
	t.Cleanup(effect.End)
	return effect
}

// firstSelectableTarget returns the resolved target of the first line a source
// authorised for navigation.
func firstSelectableTarget(t *testing.T, set pickerLineSet) (picker.Target, bool) {
	t.Helper()
	// The presented selection wins: the daemon's default cursor is a
	// session's active tab, so it is the row a fresh interaction would commit.
	if set.cursor.Key != "" {
		if target, ok := set.keys[set.cursor.Key]; ok {
			return target, true
		}
	}
	for _, line := range set.lines {
		if line.Actions&protocol.PickerCanNavigate == 0 {
			continue
		}
		target, ok := set.keys[line.Key]
		require.True(t, ok, "selectable line %q has no resolved target", line.Key)
		return target, true
	}
	return picker.Target{}, false
}

func addRemoteRefreshPickerOwner(t *testing.T, d *Daemon, id domain.SessionID, transports ...appports.ServerConnection) (*session, *attachedClient, chan wire.Frame) {
	t.Helper()
	var tr appports.ServerConnection
	var sends chan wire.Frame
	if len(transports) != 0 {
		tr = transports[0]
	} else {
		tr, sends = newCapturingTransport(t)
	}
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	tb := newTab(newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	tb.stableID = string(id) + "-tab"
	sess := &session{sessionCore: sessionCore{id: id, name: string(id), incarnation: newTestLifecycle(t), attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{tb}}
	ac.output.lastRoutePosition = protocol.RoutePosition{Target: protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}, ActiveTabID: domain.TabStableID(tb.stableID)}
	publishTiledPaneOwners(sess, tb)
	ac.setSession(sess)
	d.sessions[id] = sess
	return sess, ac, sends
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
			d := newRemotePickerDaemon()
			sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")
			ac.resumeCapable = test.resumeCapable
			interaction := openPickerStateForTest(ac)

			test.teardown(d, sess, ac)

			ac.overlays.pickerMu.Lock()
			open, currentInteraction := ac.overlays.pickerOpen, ac.overlays.pickerInteraction
			ac.overlays.pickerMu.Unlock()
			if !test.resumeCapable {
				require.False(t, open)
				return
			}
			require.True(t, ac.parked)
			require.True(t, open, "a parked attachment keeps its interaction")
			require.Equal(t, interaction, currentInteraction)
			d.mu.Lock()
			parked := d.parked[ac.resumeToken]
			d.mu.Unlock()
			require.NotNil(t, parked)
			require.Same(t, ac, parked.ac)
		})
	}
}

// TestPickerShowsCheckingRowUntilDirectoryInitializes proves the picker
// distinguishes "still checking remotes" from "no remotes": an
// uninitialized directory publishes a non-actionable placeholder row, and
// the first publication (even empty) retires it.
func TestPickerShowsCheckingRowUntilDirectoryInitializes(t *testing.T) {
	d := newRemotePickerDaemon()
	// An installed but unpublished monitor reads as "checking"; a nil
	// directory would mean remote monitoring is not installed at all.
	d.remoteDirectory = &stubRemoteDirectory{}
	sess, ac, _ := addRemoteRefreshPickerOwner(t, d, "owner")

	findChecking := func(views []pickerSessionView) bool {
		for _, view := range views {
			if view.ID == domain.SessionID("remote:checking") {
				if view.RemoteActivation != pickerRemoteUnavailable || !view.CannotAcceptMoves {
					t.Fatal("checking row must be non-actionable")
				}
				return true
			}
		}
		return false
	}

	views, _ := d.pickerViews(sess, ac)
	require.True(t, findChecking(views), "uninitialized directory must publish a checking row")

	seedRemoteDirectory(t, d)
	views, _ = d.pickerViews(sess, ac)
	require.False(t, findChecking(views), "initialized directory must retire the checking row")
}
