package daemon

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// stubRemoteDirectory is a passive RemoteDirectory: every method is safe to
// call from any goroutine and performs no I/O.
type stubRemoteDirectory struct {
	mu        sync.Mutex
	snapshot  ports.RemoteDirectorySnapshot
	snapshots int
}

func (s *stubRemoteDirectory) Snapshot() ports.RemoteDirectorySnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots++
	return s.snapshot.Clone()
}

func (s *stubRemoteDirectory) Subscribe() ports.RemoteDirectorySubscription {
	return &stubDirectorySubscription{changed: make(chan struct{}, 1)}
}

func (s *stubRemoteDirectory) RequestReconcile(endpoint string) {}
func (s *stubRemoteDirectory) RegistryChanged()                 {}

type stubDirectorySubscription struct {
	changed chan struct{}
	once    sync.Once
}

func (s *stubDirectorySubscription) Changed() <-chan struct{} { return s.changed }
func (s *stubDirectorySubscription) Close() {
	s.once.Do(func() { close(s.changed) })
}

// scriptRemoteMonitor records runner start/stop. When hang is set it ignores
// cancellation, modeling an uncooperative worker for bounded-join tests.
type scriptRemoteMonitor struct {
	started chan struct{}
	stopped chan struct{}
	hang    bool
}

func (m *scriptRemoteMonitor) run(ctx context.Context) error {
	close(m.started)
	if m.hang {
		select {}
	}
	<-ctx.Done()
	close(m.stopped)
	return nil
}

func newMonitorServeHarness(t *testing.T, monitor *scriptRemoteMonitor) (*Daemon, *mockServerListener, chan wire.Transport, chan wire.Frame, func()) {
	t.Helper()
	p, releasePTY := newBlockingPTY(t)
	tr, sends, _ := newConn(t, mustHello(protocol.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}))

	l := newMockServerListener(t)
	connCh := make(chan wire.Transport, 1)
	connCh <- tr
	closed := make(chan struct{})
	var once sync.Once
	l.EXPECT().Accept().RunAndReturn(func() (wire.Transport, error) {
		select {
		case c := <-connCh:
			return c, nil
		case <-closed:
			return nil, io.EOF
		}
	}).Maybe()
	l.EXPECT().Close().RunAndReturn(func() error { once.Do(func() { close(closed) }); return nil }).Maybe()
	l.EXPECT().Addr().Return("mock").Maybe()

	// No remote ports reach the daemon: constructors, socket publication
	// and first paint are structurally I/O-free. All remote work flows
	// through the monitor runner below.
	d := New(newFactory(t, p), stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithRemoteMonitor(&stubRemoteDirectory{}, monitor.run),
	)
	return d, l, connCh, sends, releasePTY
}

func TestDaemonNewWithMonitorPerformsNoIO(t *testing.T) {
	started := make(chan struct{}, 1)
	d := New(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithRemoteMonitor(&stubRemoteDirectory{}, func(ctx context.Context) error {
			started <- struct{}{}
			return nil
		}),
	)
	require.NotNil(t, d)
	// The constructor only stores the runner; Serve owns starting it.
	select {
	case <-started:
		t.Fatal("monitor runner started in the constructor")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestServeStartsMonitorWithoutWaitingForRemote proves the nonblocking
// startup guarantee: socket publication, local attach and first paint
// complete while every remote lane is deliberately blocked, and the monitor
// runner starts without consuming any admission.
func TestServeStartsMonitorWithoutWaitingForRemote(t *testing.T) {
	monitor := &scriptRemoteMonitor{started: make(chan struct{}), stopped: make(chan struct{})}
	d, l, _, sends, releasePTY := newMonitorServeHarness(t, monitor)
	defer releasePTY()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- d.Serve(ctx, l) }()

	select {
	case <-monitor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("monitor runner did not start with Serve")
	}

	// Local attach and first paint flow while remote I/O stays blocked.
	awaitFrame(t, sends, wire.MsgWelcome)
	awaitFrame(t, sends, wire.MsgOutput)

	// The runner is not an attachment: it holds no session and performs no
	// admission of its own.
	require.Equal(t, 1, sessionCount(d))

	cancel()
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
	select {
	case <-monitor.stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("monitor runner did not stop with Serve")
	}
}

// TestServeIdleExitWithMonitorRunning proves monitoring never counts as an
// attachment: the last session exit still drives Serve to return, and the
// monitor stops with the daemon.
func TestServeIdleExitWithMonitorRunning(t *testing.T) {
	monitor := &scriptRemoteMonitor{started: make(chan struct{}), stopped: make(chan struct{})}
	d, l, _, sends, releasePTY := newMonitorServeHarness(t, monitor)

	served := make(chan error, 1)
	go func() { served <- d.Serve(context.Background(), l) }()

	awaitFrame(t, sends, wire.MsgWelcome)
	awaitFrame(t, sends, wire.MsgOutput)

	// Child exits -> session removed -> registry empties -> Serve returns,
	// even though the monitor runner is still parked in its context.
	releasePTY()
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after last session exited")
	}
	select {
	case <-monitor.stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("monitor runner did not stop after idle exit")
	}
}

// TestServeShutdownJoinBoundedForStuckMonitor proves an uncooperative
// monitor worker cannot extend daemon shutdown: Serve returns once the
// bounded join expires. It uses a real clock so the join budget fires.
func TestServeShutdownJoinBoundedForStuckMonitor(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	tr, sends, _ := newConn(t, mustHello(protocol.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}))

	l := newMockServerListener(t)
	connCh := make(chan wire.Transport, 1)
	connCh <- tr
	closed := make(chan struct{})
	var once sync.Once
	l.EXPECT().Accept().RunAndReturn(func() (wire.Transport, error) {
		select {
		case c := <-connCh:
			return c, nil
		case <-closed:
			return nil, io.EOF
		}
	}).Maybe()
	l.EXPECT().Close().RunAndReturn(func() error { once.Do(func() { close(closed) }); return nil }).Maybe()
	l.EXPECT().Addr().Return("mock").Maybe()

	monitor := &scriptRemoteMonitor{started: make(chan struct{}), stopped: make(chan struct{}), hang: true}
	d := New(newFactory(t, p), clock.New(), slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithRemoteMonitor(&stubRemoteDirectory{}, monitor.run),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- d.Serve(ctx, l) }()

	select {
	case <-monitor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("monitor runner did not start with Serve")
	}
	awaitFrame(t, sends, wire.MsgWelcome)
	awaitFrame(t, sends, wire.MsgOutput)

	start := time.Now()
	cancel()
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("stuck monitor wedged daemon shutdown")
	}
	t.Logf("shutdown with stuck monitor took %v", time.Since(start))
}
