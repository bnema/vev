package client

import (
	"testing"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	pickerusecase "github.com/bnema/vev/internal/usecase/picker"
)

func previewTestSession(name string, seed byte, tabs ...string) catalogue.RemoteCatalogSession {
	session := pickerTestSession(name, seed, catalogue_Up)
	for _, tab := range tabs {
		session.Tabs = append(session.Tabs, catalogue.RemoteCatalogTab{ID: tab, Name: tab})
	}
	if len(tabs) > 0 {
		session.ActiveTabID = tabs[0]
	}
	return session
}

func previewTestController(t *testing.T, observations ...ports.BrokerDaemonObservation) *pickerController {
	t.Helper()
	controller, _ := pickerTestController(t)
	controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 7, Revision: 1, Daemons: observations})
	require.NotEmpty(t, controller.Render(domain.Size{Cols: 100, Rows: 40}))
	return controller
}

// previewHarness drives one manager over a mocked broker service with a
// manual debounce clock.
type previewHarness struct {
	t          *testing.T
	controller *pickerController
	service    *portsmocks.MockBrokerService
	clock      *supervisorTestClock
	manager    *previewManager
	size       domain.Size
	connection ports.BrokerConnectionID
}

func newPreviewHarness(t *testing.T, observations ...ports.BrokerDaemonObservation) *previewHarness {
	t.Helper()
	clock := newSupervisorTestClock()
	h := &previewHarness{
		t: t, controller: previewTestController(t, observations...), service: portsmocks.NewMockBrokerService(t),
		clock: clock, manager: &previewManager{clock: clock}, size: domain.Size{Cols: 100, Rows: 40},
		connection: ports.BrokerConnectionID{11},
	}
	h.service.EXPECT().ConnectionID().Return(h.connection).Maybe()
	h.service.EXPECT().Snapshot().Return(ports.BrokerSnapshot{Epoch: 7}).Maybe()
	t.Cleanup(func() { h.manager.stop() })
	return h
}

// expected is the broker request the manager must subscribe for the current
// selection and generation.
func (h *previewHarness) expected(generation ports.BrokerPreviewGeneration) ports.BrokerPreviewRequest {
	h.t.Helper()
	route, preview, ok := h.controller.PreviewRequest(h.size)
	require.True(h.t, ok, "the selected row must be previewable")
	return ports.BrokerPreviewRequest{Epoch: 7, Connection: h.connection, Generation: generation, Route: route, Preview: preview}
}

// settle fires the armed debounce and runs the manager's wake.
func (h *previewHarness) settle() bool {
	h.t.Helper()
	h.clock.awaitTimer(h.t).fire()
	h.awaitWake()
	return h.manager.publish(h.controller)
}

func (h *previewHarness) awaitWake() {
	h.t.Helper()
	select {
	case <-h.manager.changed():
	case <-time.After(5 * time.Second):
		h.t.Fatal("the preview manager did not wake")
	}
}

func (h *previewHarness) subscribe(generation ports.BrokerPreviewGeneration) (*portsmocks.MockBrokerPreviewSubscription, ports.BrokerPreviewRequest) {
	h.t.Helper()
	expected := h.expected(generation)
	sub := portsmocks.NewMockBrokerPreviewSubscription(h.t)
	sub.EXPECT().Changed().Return(make(chan struct{})).Maybe()
	h.service.EXPECT().SubscribePreview(expected).Return(sub, nil).Once()
	h.manager.refresh(h.service, h.controller, h.size)
	require.False(h.t, h.settle(), "subscribing alone changes nothing on screen")
	require.Equal(h.t, expected, h.manager.request)
	return sub, expected
}

func (h *previewHarness) shown() previewShown {
	h.controller.mu.Lock()
	defer h.controller.mu.Unlock()
	return previewShownFrom(h.controller.preview)
}

type previewShown struct {
	text string
	dim  bool
}

func previewShownFrom(view pickerusecase.Preview) previewShown {
	var shown previewShown
	for _, row := range view.Rows {
		for _, cell := range row {
			if cell.Rune != 0 {
				shown.text += string(cell.Rune)
			}
			shown.dim = shown.dim || cell.Style.Attrs&renderer.AttrDim != 0
		}
	}
	return shown
}

func previewFrame(request ports.BrokerPreviewRequest, text string) protocol.RemotePreview {
	cells := make([]renderer.Cell, 0, len(text))
	for _, r := range text {
		cells = append(cells, renderer.Cell{Rune: r})
	}
	return protocol.RemotePreview{
		Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewOK,
		LifecycleID: request.Preview.Target.LifecycleID, TabID: request.Preview.Target.LiveTabID,
		Revision: 1, Width: uint16(len(cells)), Height: 1, Cells: cells,
	}
}

func publication(request ports.BrokerPreviewRequest, preview protocol.RemotePreview, err error) ports.BrokerPreviewPublication {
	return ports.BrokerPreviewPublication{Epoch: request.Epoch, Connection: request.Connection, Generation: request.Generation,
		Target: request.Preview.Target, Width: request.Preview.Width, Height: request.Preview.Height, Preview: preview, Err: err}
}

func TestPreviewManagerRequestNamesRouteAndSelectedTab(t *testing.T) {
	now := time.Unix(100, 0)
	registration := pickerTestRegistration("ssh://example.test", 2)
	cases := []struct {
		name         string
		observations []ports.BrokerDaemonObservation
		moves        string
		wantRoute    ports.BrokerPreviewRoute
		wantEndpoint string
		wantTab      domain.TabStableID
	}{
		{
			name:         "local active tab",
			observations: []ports.BrokerDaemonObservation{pickerTestLocalObservation(now, previewTestSession("local-session", 1, "tab-local"))},
			wantRoute:    ports.BrokerPreviewRoute{Local: true, Policy: pickerTestPolicy()},
			wantEndpoint: ports.BrokerPreviewLocalEndpoint, wantTab: "tab-local",
		},
		{
			name:         "remote active tab",
			observations: []ports.BrokerDaemonObservation{pickerTestRemoteObservation("ssh://example.test", 2, 3, now, previewTestSession("remote-session", 4, "tab-remote"))},
			wantRoute:    ports.BrokerPreviewRoute{Endpoint: "ssh://example.test", Registration: registration, Policy: pickerTestPolicy()},
			wantEndpoint: "ssh://example.test", wantTab: "tab-remote",
		},
		{
			name:         "selected non-active tab row",
			observations: []ports.BrokerDaemonObservation{pickerTestLocalObservation(now, previewTestSession("work", 1, "tab-1", "tab-2"))},
			moves:        "j",
			wantRoute:    ports.BrokerPreviewRoute{Local: true, Policy: pickerTestPolicy()},
			wantEndpoint: ports.BrokerPreviewLocalEndpoint, wantTab: "tab-2",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			controller := previewTestController(t, tc.observations...)
			if tc.moves != "" {
				require.True(t, controller.ConsumeTerminalRead([]byte(tc.moves)))
			}
			size := domain.Size{Cols: 100, Rows: 40}
			route, preview, ok := controller.PreviewRequest(size)
			require.True(t, ok)
			require.Equal(t, tc.wantRoute, route)
			require.Equal(t, tc.wantEndpoint, preview.Target.Endpoint)
			require.Equal(t, tc.wantTab, preview.Target.LiveTabID)
			require.Equal(t, pickerPreviewSize(size), domain.Size{Cols: int(preview.Width), Rows: int(preview.Height)})
			require.NoError(t, ports.BrokerPreviewRequest{Epoch: 7, Connection: ports.BrokerConnectionID{1}, Generation: 1, Route: route, Preview: preview}.Validate())
		})
	}
}

func TestPreviewManagerDebouncesSelectionOnTheClock(t *testing.T) {
	now := time.Unix(100, 0)
	h := newPreviewHarness(t, pickerTestLocalObservation(now, previewTestSession("alpha", 1, "tab-a"), previewTestSession("beta", 2, "tab-b")))

	h.manager.refresh(h.service, h.controller, h.size)
	first := h.clock.awaitTimer(t)
	require.Equal(t, pickerPreviewDebounce, first.delay)
	require.Equal(t, previewShown{text: "loading preview…", dim: true}, h.shown(), "a miss shows loading at once")

	// Moving before the debounce elapses replaces it: nothing subscribes for
	// the row the cursor only passed over.
	require.True(t, h.controller.ConsumeTerminalRead([]byte("j")))
	h.manager.refresh(h.service, h.controller, h.size)
	require.Eventually(t, first.stopped, 5*time.Second, time.Millisecond, "the superseded debounce stops its timer")

	expected := h.expected(1)
	require.Equal(t, domain.TabStableID("tab-b"), expected.Preview.Target.LiveTabID)
	sub := portsmocks.NewMockBrokerPreviewSubscription(t)
	h.service.EXPECT().SubscribePreview(expected).Return(sub, nil).Once()
	require.False(t, h.settle())
	sub.EXPECT().Close().Once()
	h.manager.stop()
}

func TestPreviewManagerPublicationStates(t *testing.T) {
	now := time.Unix(100, 0)
	unavailable := ports.BrokerError{Code: ports.BrokerErrorUnavailable, Text: "preview unavailable"}
	cases := []struct {
		name      string
		withFrame bool
		next      func(ports.BrokerPreviewRequest) ports.BrokerPreviewPublication
		want      previewShown
	}{
		{name: "ok shows fresh frame", next: func(r ports.BrokerPreviewRequest) ports.BrokerPreviewPublication {
			return publication(r, previewFrame(r, "hi"), nil)
		}, want: previewShown{text: "hi"}},
		{name: "error without frame", next: func(r ports.BrokerPreviewRequest) ports.BrokerPreviewPublication {
			return publication(r, protocol.RemotePreview{}, unavailable)
		}, want: previewShown{text: "preview unavailable", dim: true}},
		{name: "error keeps last frame stale", withFrame: true, next: func(r ports.BrokerPreviewRequest) ports.BrokerPreviewPublication {
			return publication(r, protocol.RemotePreview{}, unavailable)
		}, want: previewShown{text: "hi", dim: true}},
		{name: "gone without frame", next: func(r ports.BrokerPreviewRequest) ports.BrokerPreviewPublication {
			return publication(r, protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewNoSuchTarget}, nil)
		}, want: previewShown{text: "session gone", dim: true}},
		{name: "gone keeps last frame stale", withFrame: true, next: func(r ports.BrokerPreviewRequest) ports.BrokerPreviewPublication {
			return publication(r, protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewNoSuchTarget}, nil)
		}, want: previewShown{text: "hi", dim: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newPreviewHarness(t, pickerTestLocalObservation(now, previewTestSession("alpha", 1, "tab-a")))
			sub, request := h.subscribe(1)
			sub.EXPECT().Close().Maybe()
			if tc.withFrame {
				sub.EXPECT().Latest().Return(publication(request, previewFrame(request, "hi"), nil)).Once()
				require.True(t, h.manager.publish(h.controller))
			}
			sub.EXPECT().Latest().Return(tc.next(request)).Once()
			require.True(t, h.manager.publish(h.controller))
			require.Equal(t, tc.want, h.shown())
		})
	}
}

func TestPreviewManagerIgnoresStaleGeneration(t *testing.T) {
	now := time.Unix(100, 0)
	h := newPreviewHarness(t, pickerTestLocalObservation(now, previewTestSession("alpha", 1, "tab-a")))
	sub, request := h.subscribe(1)
	sub.EXPECT().Close().Maybe()
	stale := publication(request, previewFrame(request, "old"), nil)
	stale.Generation = request.Generation + 1
	sub.EXPECT().Latest().Return(stale).Once()
	require.False(t, h.manager.publish(h.controller))
	require.Equal(t, previewShown{text: "loading preview…", dim: true}, h.shown())
}

func TestPreviewManagerCacheHitNeverBlanks(t *testing.T) {
	now := time.Unix(100, 0)
	h := newPreviewHarness(t, pickerTestLocalObservation(now, previewTestSession("alpha", 1, "tab-a"), previewTestSession("beta", 2, "tab-b")))
	first, firstRequest := h.subscribe(1)
	first.EXPECT().Latest().Return(publication(firstRequest, previewFrame(firstRequest, "aa"), nil)).Once()
	require.True(t, h.manager.publish(h.controller))
	require.Equal(t, previewShown{text: "aa"}, h.shown())

	first.EXPECT().Close().Once()
	require.True(t, h.controller.ConsumeTerminalRead([]byte("j")))
	second, secondRequest := h.subscribe(2)
	second.EXPECT().Latest().Return(publication(secondRequest, previewFrame(secondRequest, "bb"), nil)).Once()
	require.True(t, h.manager.publish(h.controller))

	// Returning to the first row shows its cached frame immediately, marked
	// stale, before the debounce even fires.
	second.EXPECT().Close().Once()
	require.True(t, h.controller.ConsumeTerminalRead([]byte("k")))
	h.manager.refresh(h.service, h.controller, h.size)
	require.Equal(t, previewShown{text: "aa", dim: true}, h.shown())
}

func TestPreviewCacheEvictsLeastRecentlyUsed(t *testing.T) {
	var cache previewCache
	key := func(i int) previewCacheKey {
		return previewCacheKey{endpoint: "local", width: uint16(i + 1), height: 1}
	}
	for i := range previewCacheSize {
		cache.put(key(i), protocol.RemotePreview{Revision: uint64(i + 1)})
	}
	_, ok := cache.get(key(0)) // key 0 becomes most recent
	require.True(t, ok)
	cache.put(key(previewCacheSize), protocol.RemotePreview{Revision: 99})
	require.Len(t, cache.entries, previewCacheSize)
	_, ok = cache.get(key(1))
	require.False(t, ok, "the least recently used entry is evicted")
	for _, i := range []int{0, 2, previewCacheSize} {
		_, ok := cache.get(key(i))
		require.True(t, ok, "entry %d is retained", i)
	}
	cache.put(key(0), protocol.RemotePreview{Revision: 100})
	frame, ok := cache.get(key(0))
	require.True(t, ok)
	require.Equal(t, uint64(100), frame.Revision, "a re-put replaces the frame in place")
	require.Len(t, cache.entries, previewCacheSize)
}

func TestPreviewManagerSubscribeFailureShowsUnavailable(t *testing.T) {
	now := time.Unix(100, 0)
	h := newPreviewHarness(t, pickerTestLocalObservation(now, previewTestSession("alpha", 1, "tab-a")))
	h.service.EXPECT().SubscribePreview(h.expected(1)).Return(nil, ports.BrokerAdmissionClosed).Once()
	h.manager.refresh(h.service, h.controller, h.size)
	require.True(t, h.settle())
	require.Equal(t, previewShown{text: "preview unavailable", dim: true}, h.shown())
	require.Nil(t, h.manager.changed())
}

func TestPreviewManagerCloseStopsSubscription(t *testing.T) {
	now := time.Unix(100, 0)
	h := newPreviewHarness(t, pickerTestLocalObservation(now, previewTestSession("alpha", 1, "tab-a")))
	sub, _ := h.subscribe(1)
	sub.EXPECT().Close().Once()
	h.manager.close(h.controller)
	require.Nil(t, h.manager.sub)
	require.Nil(t, h.manager.changed())
	require.Equal(t, previewShown{}, h.shown())
}

func TestSupervisorRefreshPreviewWiresTerminalGeometryToPreviewManager(t *testing.T) {
	now := time.Unix(100, 0)
	h := newPreviewHarness(t, pickerTestLocalObservation(now, previewTestSession("alpha", 1, "tab-a")))
	h.size = domain.Size{Cols: 80, Rows: 24}
	expected := h.expected(1)
	sub := portsmocks.NewMockBrokerPreviewSubscription(t)
	h.service.EXPECT().SubscribePreview(expected).Return(sub, nil).Once()
	sub.EXPECT().Close().Once()

	supervisor := &Supervisor{cfg: SupervisorConfig{Picker: h.controller, Terminal: &pickerRecordingTerminal{}}, preview: previewManager{clock: h.clock}}
	supervisor.refreshPreview(h.service)
	h.clock.awaitTimer(t).fire()
	select {
	case <-supervisor.preview.changed():
	case <-time.After(5 * time.Second):
		t.Fatal("the supervisor preview did not wake")
	}
	supervisor.preview.publish(h.controller)
	require.Equal(t, expected, supervisor.preview.request)
	supervisor.preview.close(h.controller)
}
