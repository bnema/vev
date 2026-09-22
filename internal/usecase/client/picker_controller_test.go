package client

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/usecase/ui"
)

// Client-owned picker controller tests (Plan 001 P5.2b): rendering, local
// search/cursor behaviour across live updates, and the input-ownership proof
// that picker input never reaches a session.

// pickerChunkReader yields pre-pushed chunks to the supervisor's single input
// lifetime. push blocks until the lifetime reads the chunk, so a test can
// order a read deterministically without a sleep.
type pickerChunkReader struct {
	chunks chan []byte
}

func newPickerChunkReader() *pickerChunkReader {
	return &pickerChunkReader{chunks: make(chan []byte)}
}

func (r *pickerChunkReader) push(data []byte) {
	r.chunks <- append([]byte(nil), data...)
}

func (r *pickerChunkReader) close() { close(r.chunks) }

func (r *pickerChunkReader) Read(buffer []byte) (int, error) {
	chunk, ok := <-r.chunks
	if !ok {
		return 0, io.EOF
	}
	return copy(buffer, chunk), nil
}

// pickerInputSpy forwards each read to the real consumer and signals that it
// was delivered, so a test can wait exactly for consumption.
type pickerInputSpy struct {
	inner pickerInputConsumer
	done  chan struct{}
}

func newPickerInputSpy(inner pickerInputConsumer) *pickerInputSpy {
	return &pickerInputSpy{inner: inner, done: make(chan struct{}, 1)}
}

func (s *pickerInputSpy) ConsumeTerminalRead(data []byte) bool {
	consumed := s.inner.ConsumeTerminalRead(data)
	select {
	case s.done <- struct{}{}:
	default:
	}
	return consumed
}

func (s *pickerInputSpy) await(t *testing.T) {
	t.Helper()
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal input was not consumed")
	}
}

// pickerRecordingTerminal records every byte written to the terminal output,
// which is where a leaked picker key would surface if it reached a session.
type pickerRecordingTerminal struct {
	in     io.Reader
	out    bytes.Buffer
	mu     sync.Mutex
	enters int
}

func (t *pickerRecordingTerminal) EnterRaw() (func() error, error) {
	t.mu.Lock()
	t.enters++
	t.mu.Unlock()
	return func() error { return nil }, nil
}

func (t *pickerRecordingTerminal) Geometry() (domain.Geometry, error) {
	return domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil
}

func (t *pickerRecordingTerminal) ResizeEvents() <-chan domain.Geometry { return nil }

func (t *pickerRecordingTerminal) In() io.Reader { return t.in }

func (t *pickerRecordingTerminal) Out() io.Writer { return &t.out }

func (t *pickerRecordingTerminal) Flush() error { return nil }

func pickerTestSnapshot(epoch ports.BrokerEpoch, revision ports.BrokerRevision, now time.Time, sessions ...string) ports.BrokerSnapshot {
	catalogSessions := make([]catalogue.RemoteCatalogSession, 0, len(sessions))
	for i, name := range sessions {
		catalogSessions = append(catalogSessions, pickerTestSession(name, byte(i+1), catalogue_Up))
	}
	return ports.BrokerSnapshot{Epoch: epoch, Revision: revision, Daemons: []ports.BrokerDaemonObservation{
		pickerTestLocalObservation(now, catalogSessions...),
	}}
}

func pickerTestController(t *testing.T) (*pickerController, *supervisorTestClock) {
	t.Helper()
	clock := newSupervisorTestClock()
	return newPickerController(clock, pickerTestFreshness, true), clock
}

func pickerRowKeyByLabel(t *testing.T, controller *pickerController, label string) string {
	t.Helper()
	line, ok := pickerLineByLabel(controller.Catalogue().Lines(), label)
	require.True(t, ok, "row %q must be projected", label)
	return line.Key
}

func mustCursorKey(t *testing.T, controller *pickerController) string {
	t.Helper()
	key, ok := controller.CursorKey()
	require.True(t, ok, "the picker must have a selected row")
	return key
}

func (p *pickerController) modelQuery(t *testing.T) string {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.NotNil(t, p.loop)
	return p.loop.model.Query()
}

func (p *pickerController) modelSearchActive(t *testing.T) bool {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.NotNil(t, p.loop)
	return p.loop.model.SearchActive()
}

func TestPickerControllerRendersLatestSnapshotImmediately(t *testing.T) {
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha", "beta"))
	firstRender := controller.Render(domain.Size{Cols: 80, Rows: 24})
	require.NotEmpty(t, firstRender)
	alphaKey := pickerRowKeyByLabel(t, controller, "alpha")
	require.Equal(t, alphaKey, mustCursorKey(t, controller))

	// A newer publication is folded synchronously: the very next render already
	// reflects it, with no debounce or intervening frame.
	controller.ApplySnapshot(pickerTestSnapshot(3, 2, clock.Now(), "beta"))
	require.Equal(t, pickerRowKeyByLabel(t, controller, "beta"), mustCursorKey(t, controller))
	require.NotEmpty(t, controller.Render(domain.Size{Cols: 80, Rows: 24}))
}

func TestPickerControllerSearchSurvivesHostUpdate(t *testing.T) {
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha", "beta"))

	spy := newPickerInputSpy(controller)
	reader := newPickerChunkReader()
	t.Cleanup(reader.close)
	_ = startTerminalInputLifetime(reader, spy)

	reader.push([]byte("/alp"))
	spy.await(t)
	require.Equal(t, "alp", controller.modelQuery(t))
	require.True(t, controller.modelSearchActive(t))

	// A host update while the query is typed keeps the editor and the matched
	// cursor; it never resets the local search.
	controller.ApplySnapshot(pickerTestSnapshot(3, 2, clock.Now(), "alpha", "beta", "gamma"))
	require.Equal(t, "alp", controller.modelQuery(t))
	require.True(t, controller.modelSearchActive(t))
	require.Equal(t, pickerRowKeyByLabel(t, controller, "alpha"), mustCursorKey(t, controller))
}

// TestPickerControllerInputNeverReachesPTY feeds every input shape a picker can
// receive and proves the only effects are local: each batch is consumed, the
// terminal receives no bytes, and the decisions are presentation facts, never a
// session write.
func TestPickerControllerInputNeverReachesPTY(t *testing.T) {
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha", "beta"))

	alphaKey := pickerRowKeyByLabel(t, controller, "alpha")
	betaKey := pickerRowKeyByLabel(t, controller, "beta")

	spy := newPickerInputSpy(controller)
	reader := newPickerChunkReader()
	t.Cleanup(reader.close)
	terminal := &pickerRecordingTerminal{in: reader}
	_ = startTerminalInputLifetime(terminal.In(), spy)

	chunks := [][]byte{
		[]byte("\x1b[B"),                     // Down -> beta
		[]byte("k"),                          // up -> alpha
		[]byte("j"),                          // down -> beta
		[]byte("\x1b[A"),                     // Up -> alpha
		[]byte("\x1b[<0;10;5M"),              // mouse report: consumed and dropped
		[]byte("\x1b[200~rm -rf /\x1b[201~"), // bracketed paste: content dropped
		[]byte("\x00\x01\x7f"),               // control bytes: consumed and dropped
		[]byte("\x1b"),                       // lone ESC withheld for disambiguation
	}
	for _, chunk := range chunks {
		reader.push(chunk)
		spy.await(t)
	}
	require.Equal(t, alphaKey, mustCursorKey(t, controller), "arrow keys moved the local cursor")
	require.False(t, controller.modelSearchActive(t), "mouse, paste, and control bytes never opened search")
	require.Equal(t, betaKey, pickerRowKeyByLabel(t, controller, "beta"))

	// A withheld lone ESC resolves through the bounded window as one Escape.
	controller.FlushPending()
	op, _ := controller.TakeOp()
	require.True(t, op.close, "Escape retires the picker locally")

	require.Empty(t, terminal.out.Bytes(), "the picker wrote nothing to the terminal")
}

func TestPickerControllerInputRouting(t *testing.T) {
	tests := []struct {
		name            string
		chunk           []byte
		flush           bool
		wantCursor      string
		wantNoSelection bool
		wantSearch      bool
		wantQuery       string
		wantOp          pickerOp
	}{
		{name: "down", chunk: []byte("\x1b[B"), wantCursor: "beta"},
		{name: "up skips informational host row", chunk: []byte("\x1b[A"), wantCursor: "alpha"},
		{name: "search entry", chunk: []byte("/"), wantCursor: "alpha", wantSearch: true},
		{name: "search text", chunk: []byte("/al"), wantCursor: "alpha", wantSearch: true, wantQuery: "al"},
		{name: "enter commits", chunk: []byte("/al\r"), wantCursor: "alpha", wantSearch: true, wantQuery: "al", wantOp: pickerOp{commit: true}},
		{name: "kill", chunk: []byte("x"), wantCursor: "alpha", wantOp: pickerOp{kill: true}},
		{name: "escape exits search first", chunk: []byte("/al\x1b"), flush: true, wantCursor: "alpha"},
		{name: "escape closes", chunk: []byte("\x1b"), flush: true, wantCursor: "alpha", wantOp: pickerOp{close: true}},
		{name: "f1 is consumed", chunk: []byte("\x1b[11~"), wantCursor: "alpha"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller, clock := pickerTestController(t)
			controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha", "beta"))
			alphaKey := pickerRowKeyByLabel(t, controller, "alpha")
			spy := newPickerInputSpy(controller)
			reader := newPickerChunkReader()
			t.Cleanup(reader.close)
			_ = startTerminalInputLifetime(reader, spy)

			reader.push(tt.chunk)
			spy.await(t)
			if tt.flush {
				controller.FlushPending()
			}
			if tt.wantOp != (pickerOp{}) {
				op, _ := controller.TakeOp()
				require.Equal(t, tt.wantOp, op)
			}
			require.Equal(t, tt.wantSearch, controller.modelSearchActive(t))
			if tt.wantQuery != "" {
				require.Equal(t, tt.wantQuery, controller.modelQuery(t))
			}
			if tt.wantNoSelection {
				_, ok := controller.CursorKey()
				require.False(t, ok, "the host row is a cursor destination but not a selection")
				return
			}
			want := alphaKey
			if tt.wantCursor == "beta" {
				want = pickerRowKeyByLabel(t, controller, "beta")
			}
			require.Equal(t, want, mustCursorKey(t, controller))
		})
	}
}

// TestPickerControllerNotOwningInputLeavesReadUnconsumed pins the ownership
// boundary: once the picker releases input, a read is not consumed, so a later
// attach slice keeps its ordinary pipeline.
func TestPickerControllerNotOwningInputLeavesReadUnconsumed(t *testing.T) {
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha"))
	controller.SetOwnsInput(false)
	require.False(t, controller.ConsumeTerminalRead([]byte("j")))
	controller.SetOwnsInput(true)
	require.True(t, controller.ConsumeTerminalRead([]byte("j")))
}

func TestPickerControllerKeepsProgressInRowsAndSurfacesFailuresAsNotices(t *testing.T) {
	controller, clock := pickerTestController(t)
	checking := pickerTestRemoteObservation("user@arch", 1, 1, clock.Now(), pickerTestSession("alpha", 1, catalogue_Up))
	checking.Checking = true
	controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{checking}})
	require.Empty(t, controller.RenderNotice(domain.Size{Cols: 80, Rows: 24}), "ordinary refreshing progress stays in the rows")

	unreachable := pickerTestRemoteObservation("user@arch", 1, 1, clock.Now())
	unreachable.Availability = domain.RemoteAvailabilityUnreachable
	unreachable.FailureEpisode = 1
	controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 2, Daemons: []ports.BrokerDaemonObservation{unreachable}})
	require.NotEmpty(t, controller.RenderNotice(domain.Size{Cols: 80, Rows: 24}), "an unavailable host is surfaced as a failure")
}

// TestPickerControllerResolveInspectableRowStillResolvesExplicitly: a stale row
// the picker displays for inspection still resolves to one exact request,
// because freshness is presentation information and only the destination may
// refuse the exact identity. The informational row stays dimmed and badged.
func TestPickerControllerResolveInspectableRowStillResolvesExplicitly(t *testing.T) {
	controller, clock := pickerTestController(t)
	stale := pickerTestLocalObservation(clock.Now().Add(-2*pickerTestFreshness), pickerTestSession("alpha", 1, catalogue_Up))
	controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{stale}})

	// The stale session row stays a cursor destination and admits an explicit
	// attempt; only the host status row itself is never committable.
	key, ok := controller.CursorKey()
	require.True(t, ok, "a stale session row stays a cursor destination")
	require.NotEmpty(t, key)
	require.False(t, controller.modelSearchActive(t))

	request, err := controller.Resolve(pickerTestBase())
	require.NoError(t, err)
	require.Equal(t, ports.BrokerAdmissionExact, request.Admission)
	require.Equal(t, "alpha", request.Target.SessionName)
}

// TestPickerControllerResolveUsesLatestSnapshot proves resolution revalidates
// against the snapshot in force at resolve time, not the one displayed.
func TestPickerControllerResolveUsesLatestSnapshot(t *testing.T) {
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha"))
	key := mustCursorKey(t, controller)

	// The host disappears before the selection resolves.
	controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 2})

	_, err := controller.Resolve(pickerTestBase())
	require.Error(t, err)
	require.True(t, pickerCatalogueErrorIs(err, pickerCatalogueNoSelection), "the vanished row leaves no selection")
	_, err = controller.Catalogue().Resolve(key, pickerTestBase())
	require.Error(t, err)
	require.True(t, pickerCatalogueErrorIs(err, pickerCatalogueGone))
	require.NotEmpty(t, controller.RenderNotice(domain.Size{Cols: 80, Rows: 24}), "the refusal is surfaced as a bounded notice")
}

func TestPickerControllerResolveReturnsExactRequest(t *testing.T) {
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha"))

	request, err := controller.Resolve(pickerTestBase())
	require.NoError(t, err)
	require.NoError(t, request.Validate())
	require.Equal(t, ports.BrokerAdmissionExact, request.Admission)
	require.Equal(t, ports.BrokerStreamAttachment, request.Purpose)
	require.Equal(t, "alpha", request.Target.SessionName)
}

// TestPickerControllerConcurrentUpdatesAndInput exercises the mutex boundaries
// under -race: publications, input consumption, rendering, and resolution run
// concurrently and must not race or panic.
func TestPickerControllerConcurrentUpdatesAndInput(t *testing.T) {
	const iterations = 200
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha"))

	var wait sync.WaitGroup
	wait.Add(3)

	go func() {
		defer wait.Done()
		for revision := ports.BrokerRevision(2); revision < 2+iterations; revision++ {
			controller.ApplySnapshot(pickerTestSnapshot(3, revision, clock.Now(), "alpha", "beta"))
		}
	}()

	go func() {
		defer wait.Done()
		for range iterations {
			controller.ConsumeTerminalRead([]byte("\x1b[B\x1b[Aj"))
			controller.FlushPending()
			controller.TakeOp()
		}
	}()

	go func() {
		defer wait.Done()
		for range iterations {
			_ = controller.Render(domain.Size{Cols: 80, Rows: 24})
			_ = controller.RenderNotice(domain.Size{Cols: 80, Rows: 24})
			_, _ = controller.Resolve(pickerTestBase())
			_, _ = controller.CursorKey()
		}
	}()

	wait.Wait()
}

// TestSupervisorPickerFoldsPublicationsAndInput proves the supervisor wiring:
// a broker publication reaches the picker through the established connection's
// subscription, and terminal input is consumed by the picker from the same
// single input lifetime the supervisor uses for EOF, never written to the
// terminal.
func TestSupervisorPickerFoldsPublicationsAndInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := newSupervisorTestClock()
	reader := newPickerChunkReader()
	t.Cleanup(reader.close)
	terminal := &pickerRecordingTerminal{in: reader}
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		service := newSupervisorTestService(ports.BrokerConnectionID{1})
		service.hub.publish(pickerTestSnapshot(3, 1, clock.Now(), "alpha", "beta"))
		return service, nil
	})
	controller := newPickerController(clock, pickerTestFreshness, true)
	sup := mustSupervisor(t, SupervisorConfig{Connector: connector, Terminal: terminal, Clock: clock, Picker: controller})

	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()

	require.Eventually(t, func() bool {
		return controller.Catalogue().Epoch() == 3
	}, time.Second, time.Millisecond, "the supervisor folds the committed publication into the picker")
	require.Equal(t, []string{"alpha", "beta"}, pickerSessionLabels(controller.Catalogue().Lines()))
	require.True(t, controller.OwnsInput())

	// The same input lifetime the supervisor uses for EOF delivers the read to
	// the picker: the cursor moves locally and nothing is written anywhere.
	betaKey := pickerRowKeyByLabel(t, controller, "beta")
	reader.push([]byte("\x1b[B"))
	require.Eventually(t, func() bool {
		key, ok := controller.CursorKey()
		return ok && key == betaKey
	}, time.Second, time.Millisecond, "the picker consumed the terminal read")

	cancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
	require.Empty(t, terminal.out.Bytes(), "the picker never wrote a session or terminal byte")
}

// TestSupervisorPickerFoldsLaterPublication covers the second-and-later
// publication: a snapshot delivered after the connection is established must
// reach the picker through the subscription awaitLoss parks on. This is the
// core of P5.2b and the fake hub's non-blocking, never-replaced channel is what
// keeps it from busy-spinning.
func TestSupervisorPickerFoldsLaterPublication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := newSupervisorTestClock()
	reader := newPickerChunkReader()
	t.Cleanup(reader.close)
	terminal := &pickerRecordingTerminal{in: reader}

	service := newSupervisorTestService(ports.BrokerConnectionID{1})
	connector := newSupervisorTestConnector(func(context.Context, int) (ports.BrokerService, error) {
		service.hub.publish(pickerTestSnapshot(3, 1, clock.Now(), "alpha"))
		return service, nil
	})
	controller := newPickerController(clock, pickerTestFreshness, true)
	sup := mustSupervisor(t, SupervisorConfig{Connector: connector, Terminal: terminal, Clock: clock, Picker: controller})

	runErr := make(chan error, 1)
	go func() { runErr <- sup.Run(ctx) }()

	require.Eventually(t, func() bool {
		return controller.Catalogue().Revision() == 1
	}, time.Second, time.Millisecond, "the first committed publication reaches the picker")
	require.Equal(t, []string{"alpha"}, pickerSessionLabels(controller.Catalogue().Lines()))

	service.publishSnapshot(pickerTestSnapshot(3, 2, clock.Now(), "alpha", "beta"))
	require.Eventually(t, func() bool {
		return controller.Catalogue().Revision() == 2
	}, time.Second, time.Millisecond, "a publication delivered after the connection is established reaches the picker")
	require.Equal(t, []string{"alpha", "beta"}, pickerSessionLabels(controller.Catalogue().Lines()))

	cancel()
	require.ErrorIs(t, <-runErr, context.Canceled)
}

// TestPickerControllerResolveKeyUsesCommittedRow proves commit and resolve are
// one action: the key captured with the commit decision resolves the committed
// row even when a concurrent publication moved the cursor afterwards.
func TestPickerControllerResolveKeyUsesCommittedRow(t *testing.T) {
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha", "beta"))
	alphaKey := pickerRowKeyByLabel(t, controller, "alpha")

	// The user commits the row under the cursor.
	require.True(t, controller.ConsumeTerminalRead([]byte("\r")))
	op, committedKey := controller.TakeOp()
	require.True(t, op.commit)
	require.Equal(t, alphaKey, committedKey)

	// A concurrent publication moves the cursor to another row, and the driver
	// commits a row different from the one the user selected.
	controller.ApplySnapshot(pickerTestSnapshot(3, 2, clock.Now(), "alpha", "beta", "gamma"))
	require.True(t, controller.ConsumeTerminalRead([]byte("j")))

	// Plain Resolve describes the row displayed now...
	displayed, err := controller.Resolve(pickerTestBase())
	require.NoError(t, err)
	require.Equal(t, "beta", displayed.Target.SessionName)

	// ...while the captured key resolves exactly the committed row.
	committed, err := controller.ResolveKey(committedKey, pickerTestBase())
	require.NoError(t, err)
	require.Equal(t, "alpha", committed.Target.SessionName)
}

// TestPickerControllerCommittedKeySurvivesConcurrentApply runs the commit path
// under -race against a driver that keeps republishing. The captured key must
// always resolve the row the user committed, never the row a publication left
// under the cursor.
func TestPickerControllerCommittedKeySurvivesConcurrentApply(t *testing.T) {
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha", "beta"))

	stop := make(chan struct{})
	var applying sync.WaitGroup
	applying.Add(1)
	go func() {
		defer applying.Done()
		revision := ports.BrokerRevision(2)
		for {
			select {
			case <-stop:
				return
			default:
			}
			controller.ApplySnapshot(pickerTestSnapshot(3, revision, clock.Now(), "alpha", "beta"))
			revision++
		}
	}()

	for range 200 {
		require.True(t, controller.ConsumeTerminalRead([]byte("\r")))
		op, key := controller.TakeOp()
		if !op.commit || key == "" {
			continue
		}
		request, err := controller.ResolveKey(key, pickerTestBase())
		if err != nil {
			continue
		}
		require.Equal(t, "alpha", request.Target.SessionName, "the committed key must never resolve another row")
	}
	close(stop)
	applying.Wait()
}

// TestPickerControllerApplySnapshotRejectsInvalidSnapshot proves the controller
// keeps the previous projection and surfaces a bounded notice when a publication
// fails the broker snapshot contract.
func TestPickerControllerApplySnapshotRejectsInvalidSnapshot(t *testing.T) {
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(pickerTestSnapshot(3, 1, clock.Now(), "alpha"))
	before := controller.Catalogue().Lines()

	invalid := pickerTestSnapshot(3, 2, clock.Now(), "beta")
	invalid.Daemons[0].Availability = 0
	controller.ApplySnapshot(invalid)

	require.Equal(t, before, controller.Catalogue().Lines(), "the previous projection is kept")
	require.Equal(t, ports.BrokerRevision(1), controller.Catalogue().Revision())

	controller.mu.Lock()
	active := controller.notices.Active(clock.Now())
	controller.mu.Unlock()
	require.Len(t, active, 1)
	require.Equal(t, "broker catalogue update rejected", active[0].Message)
	require.NotContains(t, active[0].Message, "user@")
}

// TestPickerControllerUnobservedDaemonIsRefreshingNotVersionMismatch pins the
// availability-first classification at the controller boundary: an unobserved
// daemon (Availability Unknown, ProtocolVersion 0) keeps refreshing on its
// row and surfaces no failure toast.
func TestPickerControllerUnobservedDaemonIsRefreshingNotVersionMismatch(t *testing.T) {
	controller, clock := pickerTestController(t)
	unobserved := pickerTestUnobservedObservation(clock.Now())
	controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 3, Revision: 1, Daemons: []ports.BrokerDaemonObservation{unobserved}})

	controller.mu.Lock()
	active := controller.notices.Active(clock.Now())
	controller.mu.Unlock()
	require.Empty(t, active)
	lines := controller.Catalogue().Lines()
	host, ok := pickerHostLine(lines)
	require.True(t, ok)
	require.Equal(t, domain.RemoteReasonRefreshing, host.StatusDetail)
	require.NotEqual(t, domain.RemoteReasonVersionMismatch, host.StatusDetail)
}

// TestPickerControllerRenderNoticeSelectsNewest pins that the notice shown is
// the newest by ShownAt (tie-broken by ID), independent of the toast manager's
// map-iteration order.
func TestPickerControllerRenderNoticeSelectsNewest(t *testing.T) {
	controller, _ := pickerTestController(t)
	size := domain.Size{Cols: 80, Rows: 24}

	controller.mu.Lock()
	controller.notices.Show(time.Unix(20, 0), ui.Toast{ID: "zzz-older", Message: "older-message"})
	controller.notices.Show(time.Unix(30, 0), ui.Toast{ID: "aaa-newer", Message: "newer-message"})
	controller.mu.Unlock()
	require.Contains(t, string(controller.RenderNotice(size)), "newer-message")

	// Equal ShownAt: the lexicographically greatest ID wins, deterministically.
	controller.mu.Lock()
	controller.notices.Clear()
	controller.notices.Show(time.Unix(40, 0), ui.Toast{ID: "aaa", Message: "from-aaa"})
	controller.notices.Show(time.Unix(40, 0), ui.Toast{ID: "bbb", Message: "from-bbb"})
	controller.mu.Unlock()
	for range 10 {
		require.Contains(t, string(controller.RenderNotice(size)), "from-bbb")
	}
}
