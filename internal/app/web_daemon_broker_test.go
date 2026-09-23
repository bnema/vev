//go:build linux

package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/webterm"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/mock"
)

// Browser gateway broker composition tests (Plan 001 P7.4d).
//
// The gateway server is one runBrokerClient per authenticated WebSocket. These
// tests drive the production composition — runWebTerminalClient, the real
// webterm.Terminal, and the real webterm.Server admission — over a private
// broker sandbox whose daemon serves a real shell, so each assertion holds for
// the production entry point. Only the connector composition seam and the
// observation callbacks are substituted, exactly as the terminal composition
// tests substitute theirs.
//
// Ownership is one-directional: the fixture stops the tabs it started *before*
// the broker and daemon it created are torn down, and never interprets a tab's
// disconnect as proof that the shared daemon went away.

const (
	// webGatewayCookieName mirrors the adapter's authenticated cookie. It is
	// duplicated here because the adapter keeps it unexported.
	webGatewayCookieName = "vev-web-session"
	// webGatewayShellScript prints a per-session ready marker and answers one
	// PING line with a PONG line, so a tab proves real session content and a
	// real session action rather than a parked shell.
	webGatewayShellScript = `printf 'READY-%s\n' "$$"; trap : TERM; while :; do read line || exit; case "$line" in PING*) printf 'PONG:%s\n' "$line";; esac; done`
)

var webGatewayReadyPID = regexp.MustCompile(`READY-(\d+)`)

// webFrame is one gateway websocket frame: the transactional VT update plus the
// mouse-tracking flag.
type webFrame struct {
	Update json.RawMessage `json:"update"`
	Mouse  bool            `json:"mouse"`
}

// webUpdate is the adapter's rendered update, decoded only far enough to read
// the committed cell text.
type webUpdate struct {
	SchemaVersion uint16 `json:"schemaVersion"`
	Width         int    `json:"width"`
	Height        int    `json:"height"`
	Snapshot      bool   `json:"snapshot"`
	Rows          []struct {
		Row   int `json:"row"`
		Cells []struct {
			Column int    `json:"column"`
			Width  int    `json:"width"`
			Text   string `json:"text"`
		} `json:"cells"`
	} `json:"rows"`
}

// text returns the whole rendered frame as row-major text, which is exactly
// what the browser displays.
func (u webUpdate) text() string {
	var builder strings.Builder
	for _, row := range u.Rows {
		for _, cell := range row.Cells {
			builder.WriteString(cell.Text)
		}
		builder.WriteByte('\n')
	}
	return builder.String()
}

// maxWebTabText bounds the accumulated frame text so a long test cannot grow
// without bound.
const maxWebTabText = 1 << 20

// webBrowserTab is one authenticated browser terminal connection plus its
// single owned reader. The reader is the only consumer of the socket, so an
// assertion that is not reading can never cancel another read or lose a frame.
type webBrowserTab struct {
	conn      *websocket.Conn
	cancel    context.CancelFunc
	aborted   chan struct{}
	changed   chan struct{}
	closeOnce sync.Once

	mu      sync.Mutex
	text    string
	frames  int
	readErr error
}

func (tab *webBrowserTab) run(ctx context.Context) {
	defer close(tab.aborted)
	for {
		kind, data, err := tab.conn.Read(ctx)
		if err != nil {
			tab.setReadErr(err)
			return
		}
		if kind != websocket.MessageText {
			continue
		}
		var frame webFrame
		if err := json.Unmarshal(data, &frame); err != nil {
			tab.setReadErr(err)
			return
		}
		if len(frame.Update) == 0 {
			continue
		}
		var update webUpdate
		if err := json.Unmarshal(frame.Update, &update); err != nil {
			tab.setReadErr(err)
			return
		}
		tab.append(update.text())
	}
}

func (tab *webBrowserTab) append(text string) {
	tab.mu.Lock()
	tab.text += text
	if len(tab.text) > maxWebTabText {
		tab.text = tab.text[len(tab.text)-maxWebTabText:]
	}
	tab.frames++
	tab.mu.Unlock()
	select {
	case tab.changed <- struct{}{}:
	default:
	}
}

func (tab *webBrowserTab) setReadErr(err error) {
	tab.mu.Lock()
	tab.readErr = err
	tab.mu.Unlock()
	select {
	case tab.changed <- struct{}{}:
	default:
	}
}

func (tab *webBrowserTab) accumulated() string {
	tab.mu.Lock()
	defer tab.mu.Unlock()
	return tab.text
}

func (tab *webBrowserTab) frameCount() int {
	tab.mu.Lock()
	defer tab.mu.Unlock()
	return tab.frames
}

func (tab *webBrowserTab) readerErr() error {
	tab.mu.Lock()
	defer tab.mu.Unlock()
	return tab.readErr
}

// close ends exactly this tab: the socket and this run's own context.
func (tab *webBrowserTab) close() {
	tab.closeOnce.Do(func() {
		_ = tab.conn.CloseNow()
		tab.cancel()
	})
}

// sendText types one browser text event at the tab.
func (tab *webBrowserTab) sendText(t *testing.T, text string) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"schemaVersion": 1, "type": "text", "text": text})
	require.NoError(t, err)
	require.NoError(t, tab.conn.Write(t.Context(), websocket.MessageText, payload))
}

// sendEnter sends one browser Enter key event, which the adapter encodes as CR.
// A text event deliberately cannot carry a control byte.
func (tab *webBrowserTab) sendEnter(t *testing.T) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"schemaVersion": 1, "type": "key", "key": "Enter", "code": "Enter"})
	require.NoError(t, err)
	require.NoError(t, tab.conn.Write(t.Context(), websocket.MessageText, payload))
}

// submit types one command line and presses Enter: one real session action.
func (tab *webBrowserTab) submit(t *testing.T, line string) {
	t.Helper()
	tab.sendText(t, line)
	tab.sendEnter(t)
}

// awaitText waits until the accumulated frames contain marker.
func (tab *webBrowserTab) awaitText(t *testing.T, marker string) {
	t.Helper()
	deadline := time.NewTimer(brokerTestWait)
	defer deadline.Stop()
	for {
		if strings.Contains(tab.accumulated(), marker) {
			return
		}
		select {
		case <-tab.changed:
		case <-tab.aborted:
			t.Fatalf("browser tab ended before rendering %q (reader error: %v); accumulated: %q", marker, tab.readerErr(), tab.accumulated())
		case <-deadline.C:
			t.Fatalf("browser tab never rendered %q; accumulated: %q", marker, tab.accumulated())
		}
	}
}

// requireTextAbsent asserts marker is not rendered within the observation
// window. It is the independence assertion: one tab's session content must
// never appear in another tab's terminal.
func (tab *webBrowserTab) requireTextAbsent(t *testing.T, marker string, window time.Duration) {
	t.Helper()
	deadline := time.NewTimer(window)
	defer deadline.Stop()
	for {
		require.NotContains(t, tab.accumulated(), marker, "an independent tab rendered another tab's session content")
		select {
		case <-tab.changed:
		case <-tab.aborted:
			return
		case <-deadline.C:
			return
		}
	}
}

// awaitMoreFrames waits until at least count frames were rendered, proving the
// run is still serving after a failure.
func (tab *webBrowserTab) awaitMoreFrames(t *testing.T, count int) {
	t.Helper()
	deadline := time.NewTimer(brokerTestWait)
	defer deadline.Stop()
	for {
		if tab.frameCount() >= count {
			return
		}
		select {
		case <-tab.changed:
		case <-tab.aborted:
			t.Fatalf("browser tab ended before rendering %d frames (reader error: %v)", count, tab.readerErr())
		case <-deadline.C:
			t.Fatalf("browser tab rendered only %d frames, want %d", tab.frameCount(), count)
		}
	}
}

// readyPID returns the session shell PID the tab rendered, or "" while no ready
// marker arrived. Two tabs must report distinct PIDs: their sessions are
// separate even though they share one broker and one physical carriage.
func (tab *webBrowserTab) readyPID() string {
	match := webGatewayReadyPID.FindStringSubmatch(tab.accumulated())
	if len(match) != 2 {
		return ""
	}
	return match[1]
}

// webGatewayFixture is the shared private broker, server, and tab registry of
// one browser composition test family.
type webGatewayFixture struct {
	broker  offlineClientFixture
	handler *webterm.Server
	server  *httptest.Server
	token   string
	wsURL   string

	states   chan client.State
	notices  chan client.LifecycleNotice
	failures chan error

	mu            sync.Mutex
	tabs          []*webBrowserTab
	presentations map[client.Presentation]int
}

// startWebGatewayFixture provisions one private broker sandbox with a real
// daemon shell, then one authenticated gateway server whose every WebSocket runs
// the production runWebTerminalClient composition over that sandbox.
func startWebGatewayFixture(t *testing.T) *webGatewayFixture {
	t.Helper()
	broker := startOfflineClientFixtureWithShell(t, "/bin/sh", []string{"-c", webGatewayShellScript})
	return startWebGatewayFixtureOver(t, broker, func() ports.BrokerConnector {
		return brokeripc.NewConnector(broker.socket, brokeripc.Config{})
	})
}

// startWebGatewayFixtureOver starts one gateway over an explicitly supplied
// connector composition, so a test can script the broker without a process.
func startWebGatewayFixtureOver(t *testing.T, broker offlineClientFixture, connector func() ports.BrokerConnector) *webGatewayFixture {
	t.Helper()
	// The gateway composition reads the nested-session and transport environment;
	// a test drives the broker client with both unset.
	t.Setenv("VEV", "")
	t.Setenv(envRemoteTransport, "")
	settings, err := webterm.ParseSettings("", webterm.Origin)
	require.NoError(t, err)
	fixture := &webGatewayFixture{
		broker:        broker,
		token:         strings.Repeat("w", 43),
		states:        make(chan client.State, 256),
		notices:       make(chan client.LifecycleNotice, 64),
		failures:      make(chan error, 64),
		presentations: make(map[client.Presentation]int),
	}
	previousConnector := webTerminalConnector
	previousCallbacks := terminalBrokerCallbacks
	webTerminalConnector = connector
	terminalBrokerCallbacks = func() terminalBrokerSeams {
		return terminalBrokerSeams{
			OnState: func(state client.State) {
				// Keep a durable observation as well as the bounded notification.
				// Awaiters intentionally consume notifications, so a later barrier
				// must not infer that a presentation never happened merely because
				// an earlier await already dequeued it.
				fixture.mu.Lock()
				fixture.presentations[state.Presentation]++
				fixture.mu.Unlock()
				select {
				case fixture.states <- state:
				default:
				}
			},
			OnLifecycle: func(notice client.LifecycleNotice) {
				select {
				case fixture.notices <- notice:
				default:
				}
			},
			OnFailure: func(err error) {
				select {
				case fixture.failures <- err:
				default:
				}
			},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	handler, err := webterm.NewServer(ctx, settings, fixture.token, func(runCtx context.Context, terminal *webterm.Terminal) error {
		return runWebTerminalClient(runCtx, terminal)
	})
	require.NoError(t, err)
	fixture.handler = handler
	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Host = webterm.Address
		handler.ServeHTTP(w, r)
	}))
	fixture.wsURL = "ws" + strings.TrimPrefix(fixture.server.URL, "http") + "/ws"
	t.Cleanup(func() {
		// Tabs first: the broker and daemon the fixture created are torn down by
		// their own cleanup, which runs after this one.
		fixture.closeTabs()
		fixture.server.Close()
		cancel()
		handler.Wait()
		webTerminalConnector = previousConnector
		terminalBrokerCallbacks = previousCallbacks
	})
	return fixture
}

// openTab dials one authenticated browser tab and starts its single reader.
func (f *webGatewayFixture) openTab(t *testing.T) *webBrowserTab {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	headers := http.Header{"Origin": {webterm.Origin}, "Cookie": {webGatewayCookieName + "=" + f.token}}
	conn, _, err := websocket.Dial(ctx, f.wsURL, &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		cancel()
		require.NoError(t, err)
	}
	conn.SetReadLimit(64 << 20)
	tab := &webBrowserTab{conn: conn, cancel: cancel, aborted: make(chan struct{}), changed: make(chan struct{}, 1)}
	f.mu.Lock()
	f.tabs = append(f.tabs, tab)
	f.mu.Unlock()
	go tab.run(ctx)
	return tab
}

// closeTabs ends every tab the fixture started.
func (f *webGatewayFixture) closeTabs() {
	f.mu.Lock()
	tabs := append([]*webBrowserTab(nil), f.tabs...)
	f.mu.Unlock()
	for _, tab := range tabs {
		tab.close()
	}
}

// awaitNotice waits for one bounded lifecycle notice.
func (f *webGatewayFixture) awaitNotice(t *testing.T) client.LifecycleNotice {
	t.Helper()
	select {
	case notice := <-f.notices:
		return notice
	case <-time.After(brokerTestWait):
		t.Fatal("the browser run reported no lifecycle notice")
		return client.LifecycleNotice{}
	}
}

// awaitFailure waits for one typed failure surfaced without ending the run.
func (f *webGatewayFixture) awaitFailure(t *testing.T) error {
	t.Helper()
	select {
	case err := <-f.failures:
		require.Error(t, err)
		return err
	case <-time.After(brokerTestWait):
		t.Fatal("the browser run reported no failure")
		return nil
	}
}

// awaitPresentation waits for one supervisor presentation.
func (f *webGatewayFixture) awaitPresentation(t *testing.T, want client.Presentation) client.State {
	t.Helper()
	deadline := time.NewTimer(brokerTestWait)
	defer deadline.Stop()
	for {
		select {
		case state := <-f.states:
			if state.Presentation == want {
				return state
			}
		case <-deadline.C:
			t.Fatalf("the browser run never presented %v", want)
			return client.State{}
		}
	}
}

// observedPresentations returns a defensive snapshot of all presentations
// reported by every browser run in this fixture. Unlike states, this is a
// durable barrier and therefore remains valid after awaitPresentation consumes
// its edge-triggered notification.
func (f *webGatewayFixture) observedPresentations() map[client.Presentation]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	observed := make(map[client.Presentation]int, len(f.presentations))
	for presentation, count := range f.presentations {
		observed[presentation] = count
	}
	return observed
}

// TestWebGatewayTabsShareOneBrokerWithIndependentStreamsAndServices pins the
// product contract of two browser tabs: each tab owns exactly one broker
// service and one logical stream (so detaching one is isolated), the two
// sessions are distinct and render their own real content, a real session
// action round-trips through the browser path, and the broker still dials
// exactly one shared physical carriage for both.
func TestWebGatewayTabsShareOneBrokerWithIndependentStreamsAndServices(t *testing.T) {
	fixture := startWebGatewayFixture(t)

	first := fixture.openTab(t)
	awaitLogicalStream(t, fixture.broker.streams)
	fixture.awaitPresentation(t, client.PresentAttached)
	first.awaitText(t, "READY-")
	firstPID := first.readyPID()
	require.NotEmpty(t, firstPID, "an ephemeral creation renders its session shell")
	carriageAfterFirst := fixture.broker.physicalCount()

	second := fixture.openTab(t)
	awaitLogicalStream(t, fixture.broker.streams)
	fixture.awaitPresentation(t, client.PresentAttached)
	second.awaitText(t, "READY-")
	secondPID := second.readyPID()
	require.NotEmpty(t, secondPID, "the second tab renders its own session shell")

	// Independent services and streams: two admissions, and two distinct
	// sessions even though both tabs share the broker process.
	require.NotEqual(t, firstPID, secondPID, "two tabs must attach distinct sessions")
	require.Equal(t, carriageAfterFirst, fixture.broker.physicalCount(),
		"the second tab opened another daemonmux carriage instead of reusing the pooled one")

	// The identical state machine: each browser run presents Picker, Connecting,
	// and Attached exactly like every other frontend. Consult the durable
	// observation barrier: the two Attached notifications above were consumed
	// while synchronizing startup and must still count here.
	require.Eventually(t, func() bool {
		presentations := fixture.observedPresentations()
		return presentations[client.PresentPicker] >= 2 &&
			presentations[client.PresentConnecting] >= 2 &&
			presentations[client.PresentAttached] >= 2
	}, brokerTestWait, 5*time.Millisecond, "both browser runs must present picker, connecting, and attached; observed %v", fixture.observedPresentations())

	// Real content and real actions, and each tab renders only its own session.
	first.submit(t, "PING-FIRST")
	second.submit(t, "PING-SECOND")
	first.awaitText(t, "PONG:PING-FIRST")
	second.awaitText(t, "PONG:PING-SECOND")
	first.requireTextAbsent(t, "PONG:PING-SECOND", 300*time.Millisecond)
	second.requireTextAbsent(t, "PONG:PING-FIRST", 300*time.Millisecond)

	require.NoError(t, first.readerErr())
	require.NoError(t, second.readerErr())
}

// TestWebGatewayTabClosingIsIsolated pins that disconnecting one tab cancels
// only that tab's supervisor, service, and logical stream: the shared daemon,
// the other tab, and the pooled physical carriage survive, and a new tab still
// reaches a session through the same broker.
func TestWebGatewayTabClosingIsIsolated(t *testing.T) {
	fixture := startWebGatewayFixture(t)

	first := fixture.openTab(t)
	awaitLogicalStream(t, fixture.broker.streams)
	fixture.awaitPresentation(t, client.PresentAttached)
	first.awaitText(t, "READY-")
	carriage := fixture.broker.physicalCount()
	first.submit(t, "PING-ONE")
	first.awaitText(t, "PONG:PING-ONE")

	second := fixture.openTab(t)
	awaitLogicalStream(t, fixture.broker.streams)
	fixture.awaitPresentation(t, client.PresentAttached)
	second.awaitText(t, "READY-")

	// Close the first tab and join that run before asserting on the survivor.
	first.close()
	select {
	case <-first.aborted:
	case <-time.After(brokerTestWait):
		t.Fatal("the closed browser tab kept reading")
	}

	// The surviving tab keeps its own session, its own content, and its own
	// service: the closed tab's session output never reaches the survivor.
	second.submit(t, "PING-TWO")
	second.awaitText(t, "PONG:PING-TWO")
	second.requireTextAbsent(t, "PONG:PING-ONE", 150*time.Millisecond)
	require.NoError(t, second.readerErr())

	// The daemon and broker outlive the closed tab: a fresh tab reaches a new
	// session through the same broker without opening another carriage.
	third := fixture.openTab(t)
	awaitLogicalStream(t, fixture.broker.streams)
	fixture.awaitPresentation(t, client.PresentAttached)
	third.awaitText(t, "READY-")
	require.NotEmpty(t, third.readyPID())
	require.Equal(t, carriage, fixture.broker.physicalCount(),
		"a later tab dialed a second physical carriage instead of reusing the pooled one")
}

// TestWebGatewayTabStaysPickerWhenDestinationIsRefused pins the failure
// contract of the browser composition: a broker publication that carries no
// local destination is a local refusal. The tab reports the same bounded
// selection-unavailable notice, opens no stream, stays usable in the picker, and
// keeps serving frames.
func TestWebGatewayTabStaysPickerWhenDestinationIsRefused(t *testing.T) {
	service := newTerminalCompositionService(ports.BrokerSnapshot{Epoch: terminalCompositionEpoch, Revision: 1})
	connector := portsmocks.NewMockBrokerConnector(t)
	connector.EXPECT().Connect(mock.Anything).Return(service, nil).Maybe()
	fixture := startWebGatewayFixtureOver(t, offlineClientFixture{}, func() ports.BrokerConnector {
		return connector
	})

	tab := fixture.openTab(t)
	fixture.awaitPresentation(t, client.PresentPicker)
	require.Equal(t, client.LifecycleNoticeSelectionUnavailable, fixture.awaitNotice(t).Kind)
	fixture.awaitFailure(t)

	require.Empty(t, service.openedRequests(), "a refused destination never opens a broker stream")

	// The run stays alive in the picker: it keeps painting and never ends the
	// socket, exactly like the ordinary terminal run.
	before := tab.frameCount()
	tab.awaitMoreFrames(t, before+1)
	select {
	case <-tab.aborted:
		t.Fatalf("the refused destination ended the browser run: %v", tab.readerErr())
	default:
	}
}

// TestWebGatewayTabReturnsToPickerOnBrokerLoss pins that losing the established
// broker connection does not end the browser run and does not leave the tab
// attached: the supervisor reports the typed broker loss, returns to the picker,
// and keeps serving frames while it retries.
func TestWebGatewayTabReturnsToPickerOnBrokerLoss(t *testing.T) {
	service := newTerminalCompositionService(terminalCompositionSnapshot([]string{"work"}, nil))
	connector := portsmocks.NewMockBrokerConnector(t)
	var calls atomic.Int32
	connector.EXPECT().Connect(mock.Anything).RunAndReturn(func(context.Context) (ports.BrokerService, error) {
		if calls.Add(1) == 1 {
			return service, nil
		}
		return nil, errors.New("broker is gone")
	}).Maybe()
	fixture := startWebGatewayFixtureOver(t, offlineClientFixture{}, func() ports.BrokerConnector { return connector })

	tab := fixture.openTab(t)
	fixture.awaitPresentation(t, client.PresentAttached)

	// Losing the established connection is a broker loss, not an exit.
	require.NoError(t, service.Close())

	require.Equal(t, client.LifecycleNoticeBrokerLost, fixture.awaitNotice(t).Kind)
	fixture.awaitPresentation(t, client.PresentPicker)
	fixture.awaitFailure(t)

	// The tab is still live and still serving: the reconnect cadence repaints the
	// picker, so frames keep arriving and the socket is never closed by the loss.
	before := tab.frameCount()
	tab.awaitMoreFrames(t, before+1)
	select {
	case <-tab.aborted:
		t.Fatalf("broker loss ended the browser run: %v", tab.readerErr())
	default:
	}
	// A repaint can land before the first reconnect attempt, so wait for it.
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, brokerTestWait, 5*time.Millisecond,
		"a lost connection must be retried, never fatal")
}

// TestWebCompositionUsesTheBrokerConnector pins the production composition
// source: the gateway delegates every WebSocket to the shared broker client
// over the lazy production connector, and keeps no direct daemon dialer, no
// sessionwire carriage, no remote factory, and no attachment cache of its own.
func TestWebCompositionUsesTheBrokerConnector(t *testing.T) {
	sources := loadAppSources(t, false)
	body := sources["web_daemon.go"]
	require.Contains(t, body, "runBrokerClient(ctx, brokerClientConfig{")
	require.Contains(t, body, "newProductionBrokerConnector")
	require.Contains(t, body, "localEphemeralNavigation()")
	for _, forbidden := range []string{
		"runAttachWithDeps", "runAttachDeps", "sessionwire", "localDaemonDialer", "dialOnlyLocalDialer",
		"remoteDialerFactory", "launchConfig", "launchEndpoint", "AttachmentCache",
		"createDetachedLocalSession", "platform.StateDir", "HostRegistry",
	} {
		require.NotContains(t, body, forbidden, "the web composition must not keep %q", forbidden)
	}
}

// TestWebGatewayTerminalIsNotDialedBeforeConnect pins the laziness of the
// production connector through the gateway composition: constructing the
// connector performs no I/O, so an absent broker is an ordinary attempt failure
// under the supervisor, never a gateway startup error.
func TestWebGatewayTerminalIsNotDialedBeforeConnect(t *testing.T) {
	var connects atomic.Int64
	previous := connectProductionClientBroker
	connectProductionClientBroker = func(context.Context) (ports.BrokerService, error) {
		connects.Add(1)
		return nil, errors.New("broker not reachable")
	}
	t.Cleanup(func() { connectProductionClientBroker = previous })

	fixture := startWebGatewayFixtureOver(t, offlineClientFixture{}, newProductionBrokerConnector)
	tab := fixture.openTab(t)
	fixture.awaitPresentation(t, client.PresentPicker)
	// Construction performs no I/O: the gateway reaches the broker only through
	// Connect, and an unreachable broker is an ordinary attempt failure.
	require.Eventually(t, func() bool { return connects.Load() >= 1 }, brokerTestWait, 5*time.Millisecond,
		"the gateway must reach the broker only through Connect")
	// A broker that is never reachable keeps the tab usable in the picker.
	before := tab.frameCount()
	tab.awaitMoreFrames(t, before+1)
	select {
	case <-tab.aborted:
		t.Fatalf("an unreachable broker ended the browser run: %v", tab.readerErr())
	default:
	}
}
