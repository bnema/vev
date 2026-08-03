package daemon

import (
	"io"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestP4StructuredRuntimeDiagnosticsRecordAcceptedSnapshotAndDelta(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	observer := &daemonRuntimeObserver{}
	d.runtimeObserver = observer
	clientTransport, sent := newCapturingTransport(t)
	proxy, ac := newAttachedProxyFixture(t, d, clientTransport, newProxyTestTransport())
	ac.proxied = true
	ac.screenOutput = newStructuredOutputStream(ac.output)
	require.True(t, applyTestScreenText(proxy, 0, 0, "snapshot"))

	emit := func(reset bool) ports.ScreenUpdate {
		t.Helper()
		ac.sendMu.Lock()
		state, ok := proxy.captureRenderState(ac, renderCaptureRequest{reset: reset})
		require.True(t, ok)
		composed := composeFrame(*state, ac.pipelineCache, ac.pipelineScratch)
		require.True(t, d.emitFrame(proxy, ac, state, composed))
		frame := awaitTestValue(t, sent, "structured screen update")
		if frame.Type == ports.MsgSessionMeta {
			frame = awaitTestValue(t, sent, "structured screen update after metadata")
		}
		require.Equal(t, ports.MsgScreenUpdate, frame.Type)
		update, err := ports.UnmarshalScreenUpdate(frame.Payload)
		require.NoError(t, err)
		return update
	}

	snapshot := emit(true)
	marks := structuredScreenMarks(t, observer)
	require.Len(t, marks, 1)
	require.Equal(t, ports.RuntimeScreenSnapshot, marks[0].Kind)
	require.Equal(t, uint64(len(mustMarshalScreenUpdate(t, snapshot))), marks[0].Bytes)
	require.Equal(t, uint64(len(snapshot.Spans)), marks[0].Fragments)

	proxy.mu.Lock()
	size := proxy.contentSize
	proxy.mu.Unlock()
	_, _, changed := proxy.applyScreenUpdateForGeneration(1, ports.ScreenUpdate{
		Kind: ports.ScreenUpdateDelta, BaseStateNum: snapshot.NewStateNum, NewStateNum: snapshot.NewStateNum + 1,
		Size: size, Spans: []ports.ScreenSpan{{Y: 0, X: 0, Cells: cells("delta")}},
	})
	require.True(t, changed)
	observer.marks = nil

	delta := emit(false)
	marks = structuredScreenMarks(t, observer)
	require.Len(t, marks, 1)
	require.Equal(t, ports.RuntimeScreenDelta, marks[0].Kind)
	require.Equal(t, uint64(len(mustMarshalScreenUpdate(t, delta))), marks[0].Bytes)
	require.Equal(t, uint64(len(delta.Spans)), marks[0].Fragments)
}

func TestP4StructuredRuntimeDiagnosticsDoNotRecordFailedSend(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	observer := &daemonRuntimeObserver{}
	d.runtimeObserver = observer
	clientTransport := newStructuredDiagnosticFailingTransport(t)
	proxy, ac := newAttachedProxyFixture(t, d, clientTransport, newProxyTestTransport())
	ac.proxied = true
	ac.screenOutput = newStructuredOutputStream(ac.output)
	require.True(t, applyTestScreenText(proxy, 0, 0, "failed"))

	ac.sendMu.Lock()
	state, ok := proxy.captureRenderState(ac, renderCaptureRequest{reset: true})
	require.True(t, ok)
	composed := composeFrame(*state, composeCacheInput{})
	require.True(t, d.emitFrame(proxy, ac, state, composed))
	require.Empty(t, structuredScreenMarks(t, observer))
}

func TestP4StructuredRuntimeDiagnosticsResetOnlyAfterAcceptedRequest(t *testing.T) {
	d, proxy, transport, generation := newProxyOutputSession(t)
	observer := &daemonRuntimeObserver{}
	d.runtimeObserver = observer
	invalid := ports.ScreenUpdate{
		Kind: ports.ScreenUpdateDelta, BaseStateNum: 1, NewStateNum: 2,
		Size: domain.Size{Cols: 80, Rows: 22},
	}
	require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, invalid))
	marks := structuredScreenMarks(t, observer)
	require.Len(t, marks, 1)
	require.Equal(t, ports.RuntimeScreenResetRequested, marks[0].Kind)
	require.Zero(t, marks[0].Bytes)
	require.Zero(t, marks[0].Fragments)
	require.Equal(t, ports.MsgOutputResetRequest, awaitTestValue(t, transport.sent, "reset request").Type)

	// The already-requested reset is not sent or measured a second time.
	require.NoError(t, d.handleProxyScreenUpdate(proxy, generation, invalid))
	require.Len(t, structuredScreenMarks(t, observer), 1)

	d, proxy, transport, generation = newProxyOutputSession(t)
	d.runtimeObserver = &daemonRuntimeObserver{}
	transport.sendFails.Store(true)
	require.Error(t, d.handleProxyScreenUpdate(proxy, generation, invalid))
	require.Empty(t, structuredScreenMarks(t, d.runtimeObserver))
}

func TestP4StructuredRuntimeDiagnosticsObserverRunsOutsideAttachmentAndProxyLocks(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	observer := &blockingStructuredDiagnosticObserver{entered: make(chan struct{}), release: make(chan struct{})}
	d.runtimeObserver = observer
	clientTransport, sent := newCapturingTransport(t)
	proxy, ac := newAttachedProxyFixture(t, d, clientTransport, newProxyTestTransport())
	ac.proxied = true
	ac.screenOutput = newStructuredOutputStream(ac.output)
	require.True(t, applyTestScreenText(proxy, 0, 0, "locks"))

	done := make(chan struct{})
	go func() {
		ac.sendMu.Lock()
		state, ok := proxy.captureRenderState(ac, renderCaptureRequest{reset: true})
		if !ok {
			ac.sendMu.Unlock()
			close(done)
			return
		}
		composed := composeFrame(*state, composeCacheInput{})
		d.emitFrame(proxy, ac, state, composed)
		close(done)
	}()
	awaitDaemonObserver(t, observer.entered, "structured diagnostic observer")
	_ = awaitTestValue(t, sent, "structured screen update")

	attachmentUnlocked := make(chan struct{})
	go func() {
		ac.sendMu.Lock()
		_ = ac.size
		ac.sendMu.Unlock()
		close(attachmentUnlocked)
	}()
	proxyUnlocked := make(chan struct{})
	go func() {
		proxy.mu.Lock()
		_ = proxy.contentSize
		proxy.mu.Unlock()
		close(proxyUnlocked)
	}()
	awaitDaemonObserver(t, attachmentUnlocked, "attachment lock")
	awaitDaemonObserver(t, proxyUnlocked, "proxy lock")
	close(observer.release)
	awaitDaemonObserver(t, done, "structured render")
}

func structuredScreenMarks(t *testing.T, observer ports.RuntimeObserver) []ports.RuntimeMark {
	t.Helper()
	collector, ok := observer.(*daemonRuntimeObserver)
	require.True(t, ok, "runtime observer must be the daemon test collector")
	var marks []ports.RuntimeMark
	for _, mark := range collector.marks {
		switch mark.Kind {
		case ports.RuntimeScreenSnapshot, ports.RuntimeScreenDelta, ports.RuntimeScreenResetRequested:
			marks = append(marks, mark)
		}
	}
	return marks
}

func mustMarshalScreenUpdate(t *testing.T, update ports.ScreenUpdate) []byte {
	t.Helper()
	data, err := ports.MarshalScreenUpdate(update)
	require.NoError(t, err)
	return data
}

type blockingStructuredDiagnosticObserver struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (o *blockingStructuredDiagnosticObserver) ObserveRuntime(mark ports.RuntimeMark) {
	if mark.Kind != ports.RuntimeScreenSnapshot && mark.Kind != ports.RuntimeScreenDelta {
		return
	}
	o.once.Do(func() {
		close(o.entered)
		<-o.release
	})
}

func newStructuredDiagnosticFailingTransport(t *testing.T) *portsmocks.MockTransport {
	t.Helper()
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(io.ErrClosedPipe).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	return tr
}
