package daemon

// Explicit KillResult daemon contract. handleKill answers every normally
// decoded request with exactly one correlated KillResult before the control
// connection closes; a KillDaemon result is delivered before teardown begins.
// These tests are deterministic (no sleeps) and bounded by awaitFrame timeouts.

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func awaitKillResult(t *testing.T, sends chan wire.Envelope, requestID uint64) protocol.KillResult {
	t.Helper()
	frame := awaitFrame(t, sends, "KillResult")
	result, ok := decodeServerMessage(t, frame).(protocol.KillResult)
	require.True(t, ok, "expected a KillResult, got %T", decodeServerMessage(t, frame))
	require.Equal(t, requestID, result.RequestID, "result must correlate to its request")
	return result
}

// requireNoFurtherSends proves the handler produced exactly one result.
func requireNoFurtherSends(t *testing.T, sends chan wire.Envelope) {
	t.Helper()
	select {
	case extra := <-sends:
		t.Fatalf("unexpected extra send %s", envelopeMessageName(t, extra.Payload))
	default:
	}
}

// TestHandleKillSessionSendsExactlyOneSucceededResult proves a direct kill of a
// live session answers with one KillSucceeded result correlated to the request
// and removes the session.
func TestHandleKillSessionSendsExactlyOneSucceededResult(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	addControlSession(d, "work", "t_work", "p_work")

	tr, sends := newCapturingTransport(t)
	d.handleKill(tr, protocol.Kill{RequestID: 7, Scope: protocol.KillSession, Name: "work"})

	result := awaitKillResult(t, sends, 7)
	require.Equal(t, protocol.KillResult{RequestID: 7, Outcome: protocol.KillSucceeded}, result)
	require.Zero(t, sessionCount(d), "a succeeded kill must remove the session")
	requireNoFurtherSends(t, sends)
}

// TestHandleKillSessionSendsDefiniteFailureResult proves a kill of a missing
// session answers with one definite KillFailed result, not a close-as-success,
// and leaves the registry untouched.
func TestHandleKillSessionSendsDefiniteFailureResult(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})

	tr, sends := newCapturingTransport(t)
	d.handleKill(tr, protocol.Kill{RequestID: 9, Scope: protocol.KillSession, Name: "ghost"})

	result := awaitKillResult(t, sends, 9)
	require.Equal(t, protocol.KillFailed, result.Outcome)
	require.Equal(t, protocol.ErrNoSuchSession, result.Code)
	require.Equal(t, "no such session: ghost", result.Text)
	requireNoFurtherSends(t, sends)
}

// TestHandleKillInvalidScopeSendsDefiniteFailureResult proves an invalid scope
// still receives exactly one definite failure instead of a silent close.
func TestHandleKillInvalidScopeSendsDefiniteFailureResult(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})

	tr, sends := newCapturingTransport(t)
	d.handleKill(tr, protocol.Kill{RequestID: 3, Scope: protocol.KillScope(9)})

	result := awaitKillResult(t, sends, 3)
	require.Equal(t, protocol.KillFailed, result.Outcome)
	require.Equal(t, protocol.ErrInternal, result.Code)
	require.Equal(t, "invalid kill scope", result.Text)
}

// TestHandleKillAllEmptyDaemonSendsOneSucceededResult proves a successful
// kill-all answers with one KillSucceeded result and leaves the daemon serving.
func TestHandleKillAllEmptyDaemonSendsOneSucceededResult(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})

	tr, sends := newCapturingTransport(t)
	d.handleKill(tr, protocol.Kill{RequestID: 11, Scope: protocol.KillAll})

	result := awaitKillResult(t, sends, 11)
	require.Equal(t, protocol.KillSucceeded, result.Outcome)
	select {
	case <-d.done:
		t.Fatal("kill-all must not end the daemon")
	default:
	}
	requireNoFurtherSends(t, sends)
}

// TestHandleKillAllReportsBoundedStructuredPartialFailures proves a kill-all
// whose stopped records cannot be purged answers with one definite KillFailed
// result carrying the total count plus at most purgeSummaryFailureLimit
// structured failures, so a control response stays bounded.
func TestHandleKillAllReportsBoundedStructuredPartialFailures(t *testing.T) {
	const total = purgeSummaryFailureLimit + 3

	fail := make(map[domain.IncarnationID]error, total)
	repository := newSelectivePurgeRepository(fail)
	d := newTestDaemon(t, nil, stubClock{})
	WithSnapshotRepository(repository)(d)
	store, _ := newMockStore(t)
	WithStore(t, store)(d)

	for i := 0; i < total; i++ {
		incarnation := domain.IncarnationID{byte(i + 1)}
		fail[incarnation] = errors.New("delete failed")
		record := domain.CatalogueRecord{Name: fmt.Sprintf("s%02d", i), IncarnationID: incarnation, CreatedAt: int64(i + 1)}
		require.NoError(t, d.catalogue.Create(record))
		d.inactive[record.Name] = inactiveSessionFromRecord(record, protocol.SessionDown, nil)
	}

	tr, sends := newCapturingTransport(t)
	d.handleKill(tr, protocol.Kill{RequestID: 13, Scope: protocol.KillAll})

	result := awaitKillResult(t, sends, 13)
	require.Equal(t, protocol.KillFailed, result.Outcome)
	require.Equal(t, protocol.ErrInternal, result.Code)
	require.Len(t, result.Failures, purgeSummaryFailureLimit, "wire detail must be bounded")
	require.Contains(t, result.Text, fmt.Sprintf("%d record(s) could not be purged", total))
	require.Contains(t, result.Text, "more omitted")
	requireNoFurtherSends(t, sends)
}

// killOrderTransport records the order of the KillResult send and the control
// close, and whether the daemon's shutdown signal was already closed when the
// result was written.
type killOrderTransport struct {
	mu                   sync.Mutex
	order                []string
	resultBeforeTeardown bool
}

func (r *killOrderTransport) record(name string) {
	r.mu.Lock()
	r.order = append(r.order, name)
	r.mu.Unlock()
}

func (r *killOrderTransport) snapshot() ([]string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.order...), r.resultBeforeTeardown
}

func newKillOrderTransport(t *testing.T, teardown <-chan struct{}) (*mockServerConnection, *killOrderTransport) {
	t.Helper()
	recorder := &killOrderTransport{}
	tr := newMockServerConnection(t)
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f wire.Envelope) error {
		name := envelopeMessageName(t, f.Payload)
		if name == "KillResult" {
			recorder.mu.Lock()
			recorder.order = append(recorder.order, "result")
			select {
			case <-teardown:
			default:
				recorder.resultBeforeTeardown = true
			}
			recorder.mu.Unlock()
			return nil
		}
		recorder.record(name)
		return nil
	}).Maybe()
	tr.EXPECT().Close().RunAndReturn(func() error {
		recorder.record("close")
		return nil
	}).Maybe()
	return tr, recorder
}

// TestHandleKillDaemonSendsResultBeforeTeardownAndClose proves the explicit
// daemon-stop result is delivered before shutdown begins and before the control
// connection closes, so a client can observe success instead of inferring it
// from a close.
func TestHandleKillDaemonSendsResultBeforeTeardownAndClose(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})

	tr, recorder := newKillOrderTransport(t, d.done)
	d.handleKill(tr, protocol.Kill{RequestID: 42, Scope: protocol.KillDaemon})

	order, beforeTeardown := recorder.snapshot()
	require.Equal(t, []string{"result", "close"}, order)
	require.True(t, beforeTeardown, "the daemon-stop result must be sent before teardown begins")
	select {
	case <-d.done:
	default:
		t.Fatal("KillDaemon must end the daemon")
	}
}

// recordFailTransport records every sent envelope then fails, so a test can
// inspect the exact result an undeliverable control send attempted.
type recordFailTransport struct {
	mu   sync.Mutex
	sent []wire.Envelope
	err  error
}

func (t *recordFailTransport) Send(envelope wire.Envelope) error {
	t.mu.Lock()
	t.sent = append(t.sent, envelope)
	t.mu.Unlock()
	return t.err
}

func (*recordFailTransport) Recv() (wire.Envelope, error) { return wire.Envelope{}, io.EOF }
func (*recordFailTransport) Close() error                 { return nil }

func (t *recordFailTransport) payloads() []wire.Envelope {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]wire.Envelope(nil), t.sent...)
}

// TestHandleKillAllSupersededByDaemonShutdownSendsOneUnknownOutcome proves a
// KillAll that an explicit daemon shutdown superseded answers with exactly one
// correlated KillOutcomeUnknown carrying ErrServerShutdown, records the outcome
// and code in the send-failure log, and sends nothing further (no success and no
// second result).
func TestHandleKillAllSupersededByDaemonShutdownSendsOneUnknownOutcome(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	var logged syncedLogBuffer
	d.log = slog.New(slog.NewTextHandler(&logged, nil))

	// An explicit stop already owns the lifecycle set, so the purge defers.
	d.mu.Lock()
	d.closing = true
	d.mu.Unlock()

	recorder := &recordFailTransport{err: errors.New("client gone")}
	d.handleKill(&rawServerConnection{raw: recorder}, protocol.Kill{RequestID: 5, Scope: protocol.KillAll})

	payloads := recorder.payloads()
	require.Len(t, payloads, 1, "a superseded kill-all must attempt exactly one result")
	message, err := sessionwire.DecodeServerEnvelope(payloads[0].Payload)
	require.NoError(t, err)
	result, ok := message.(protocol.KillResult)
	require.True(t, ok, "expected KillResult, got %T", message)
	require.Equal(t, protocol.KillResult{RequestID: 5, Outcome: protocol.KillOutcomeUnknown, Code: protocol.ErrServerShutdown, Text: "daemon is shutting down"}, result)

	require.Contains(t, logged.String(), "kill result")
	require.Contains(t, logged.String(), "client gone")
	require.Contains(t, logged.String(), "outcome="+strconv.Itoa(int(protocol.KillOutcomeUnknown)), "the send-failure log must name the unobserved outcome")
	require.Contains(t, logged.String(), "code="+strconv.Itoa(int(protocol.ErrServerShutdown)), "the send-failure log must name the unobserved code")
}

// TestKillAllUndeliverableResultIsLogged proves an explicit partial-failure
// result that cannot be delivered is logged (never silently closing as
// success) and still carries the bounded structured failures.
func TestKillAllUndeliverableResultIsLogged(t *testing.T) {
	repository := newSelectivePurgeRepository(map[domain.IncarnationID]error{{2}: errors.New("delete failed")})
	d := newTestDaemon(t, nil, stubClock{})
	var logged syncedLogBuffer
	d.log = slog.New(slog.NewTextHandler(&logged, nil))
	WithSnapshotRepository(repository)(d)
	store, _ := newMockStore(t)
	WithStore(t, store)(d)

	record := domain.CatalogueRecord{Name: "stopped", IncarnationID: domain.IncarnationID{2}, CreatedAt: 7}
	require.NoError(t, d.catalogue.Create(record))
	d.inactive["stopped"] = inactiveSessionFromRecord(record, protocol.SessionDown, nil)

	recorder := &recordFailTransport{err: errors.New("client gone")}
	d.handleKill(&rawServerConnection{raw: recorder}, protocol.Kill{RequestID: 1, Scope: protocol.KillAll})

	require.Contains(t, logged.String(), "kill result")
	require.Contains(t, logged.String(), "client gone")

	payloads := recorder.payloads()
	require.Len(t, payloads, 1, "exactly one result is attempted")
	message, err := sessionwire.DecodeServerEnvelope(payloads[0].Payload)
	require.NoError(t, err)
	result, ok := message.(protocol.KillResult)
	require.True(t, ok, "expected KillResult, got %T", message)
	require.Equal(t, protocol.KillFailed, result.Outcome)
	require.Len(t, result.Failures, 1)
	require.Equal(t, "stopped", result.Failures[0].Name)
}
