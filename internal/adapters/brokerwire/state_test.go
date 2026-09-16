package brokerwire

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

func testBrokerScope() Scope {
	epoch, connection := testSnapshotScope()
	return Scope{Epoch: epoch, Connection: connection}
}

func testForeignScope() Scope {
	return Scope{Epoch: 8, Connection: testConnectionID(0x32)}
}

// connectionOperations and connectionStreams expose the trackers bound to a
// connection to in-package tests. Production code never touches them: the
// Connection lifecycle methods hold the connection lock across the ready check
// and the tracker mutation, so no caller can bypass the connection state
// machine. The trackers are assigned once by NewConnection and never replaced,
// so these reads need no lock.
func connectionOperations(c *Connection) *OperationTracker { return c.operations }

func connectionStreams(c *Connection) *StreamTracker { return c.streams }

// operationIDAt builds one unique non-zero operation identity.
func operationIDAt(i int) ports.BrokerOperationID {
	var id ports.BrokerOperationID
	id[0] = byte(i >> 8)
	id[1] = byte(i)
	id[2] = 0x5a
	id[15] = 0x01
	return id
}

func newTestConnection(t *testing.T) *Connection {
	t.Helper()
	conn, err := NewConnection(testBrokerScope())
	require.NoError(t, err)
	require.Equal(t, ConnNew, conn.State())
	return conn
}

func awaitRegisterConnection(t *testing.T) *Connection {
	t.Helper()
	conn := newTestConnection(t)
	require.NoError(t, conn.SignalPreamble())
	require.Equal(t, ConnAwaitRegister, conn.State())
	return conn
}

func readyTestConnection(t *testing.T) *Connection {
	t.Helper()
	conn := awaitRegisterConnection(t)
	require.NoError(t, conn.Register())
	require.Equal(t, ConnReady, conn.State())
	require.True(t, conn.Ready())
	return conn
}

func closingTestConnection(t *testing.T) *Connection {
	t.Helper()
	conn := readyTestConnection(t)
	require.NoError(t, conn.BeginClose())
	require.Equal(t, ConnClosing, conn.State())
	return conn
}

func closedTestConnection(t *testing.T) *Connection {
	t.Helper()
	conn := closingTestConnection(t)
	require.NoError(t, conn.FinishClose())
	require.Equal(t, ConnClosed, conn.State())
	return conn
}

func TestNewConnectionScope(t *testing.T) {
	cases := []struct {
		name    string
		scope   Scope
		wantErr bool
	}{
		{"valid", testBrokerScope(), false},
		{"zero epoch", Scope{Connection: testConnectionID(0x31)}, true},
		{"zero connection", Scope{Epoch: 7}, true},
		{"zero scope", Scope{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := NewConnection(tc.scope)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrInvalidScope)
				require.Nil(t, conn)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.scope, conn.Scope())
			require.Equal(t, ConnNew, conn.State())
			require.False(t, conn.Ready())
			require.Zero(t, connectionOperations(conn).Pending())
			require.Zero(t, connectionStreams(conn).Active())
		})
	}
}

func TestConnStateString(t *testing.T) {
	require.Equal(t, "new", ConnNew.String())
	require.Equal(t, "await_register", ConnAwaitRegister.String())
	require.Equal(t, "ready", ConnReady.String())
	require.Equal(t, "closing", ConnClosing.String())
	require.Equal(t, "closed", ConnClosed.String())
	require.Equal(t, "unknown", ConnState(200).String())
}

// TestConnectionLifecycle is the handshake and shutdown table: New ->
// AwaitRegister after the preamble signal -> Ready after exactly one
// Register -> Closing -> Closed.
func TestConnectionLifecycle(t *testing.T) {
	op := operationIDAt(1)
	part := snapshotPartsFor(1, 1, sampleSnapshotHosts(), nil)[0]
	cases := []struct {
		name      string
		setup     func(*testing.T) *Connection
		run       func(*Connection) error
		wantErr   error
		wantState ConnState
	}{
		{"preamble in new", newTestConnection, func(c *Connection) error { return c.SignalPreamble() }, nil, ConnAwaitRegister},
		{"register before preamble", newTestConnection, func(c *Connection) error { return c.Register() }, ErrConnectionState, ConnNew},
		{"subscribe before preamble", newTestConnection, func(c *Connection) error { return c.Subscribe(1) }, ErrConnectionState, ConnNew},
		{"operation before preamble", newTestConnection, func(c *Connection) error { return c.AdmitOperation(op) }, ErrConnectionState, ConnNew},
		{"stream before preamble", newTestConnection, func(c *Connection) error { return c.OpenStream(1) }, ErrConnectionState, ConnNew},
		{"snapshot part before preamble", newTestConnection, func(c *Connection) error {
			_, _, err := c.AddSnapshotPart(part)
			return err
		}, ErrConnectionState, ConnNew},
		{"preamble twice", awaitRegisterConnection, func(c *Connection) error { return c.SignalPreamble() }, ErrConnectionState, ConnAwaitRegister},
		{"subscribe before register", awaitRegisterConnection, func(c *Connection) error { return c.Subscribe(1) }, ErrConnectionState, ConnAwaitRegister},
		{"register after preamble", awaitRegisterConnection, func(c *Connection) error { return c.Register() }, nil, ConnReady},
		{"register twice", readyTestConnection, func(c *Connection) error { return c.Register() }, ErrDuplicateRegister, ConnReady},
		{"preamble when ready", readyTestConnection, func(c *Connection) error { return c.SignalPreamble() }, ErrConnectionState, ConnReady},
		{"finish close before closing", readyTestConnection, func(c *Connection) error { return c.FinishClose() }, ErrConnectionState, ConnReady},
		{"finish close in new", newTestConnection, func(c *Connection) error { return c.FinishClose() }, ErrConnectionState, ConnNew},
		{"begin close when ready", readyTestConnection, func(c *Connection) error { return c.BeginClose() }, nil, ConnClosing},
		{"begin close idempotent", closingTestConnection, func(c *Connection) error { return c.BeginClose() }, nil, ConnClosing},
		{"finish close after begin", closingTestConnection, func(c *Connection) error { return c.FinishClose() }, nil, ConnClosed},
		{"finish close idempotent", closedTestConnection, func(c *Connection) error { return c.FinishClose() }, nil, ConnClosed},
		{"subscribe while closing", closingTestConnection, func(c *Connection) error { return c.Subscribe(1) }, ErrConnectionClosed, ConnClosing},
		{"operation while closing", closingTestConnection, func(c *Connection) error { return c.AdmitOperation(op) }, ErrConnectionClosed, ConnClosing},
		{"stream while closing", closingTestConnection, func(c *Connection) error { return c.OpenStream(1) }, ErrConnectionClosed, ConnClosing},
		{"part while closing", closingTestConnection, func(c *Connection) error {
			_, _, err := c.AddSnapshotPart(part)
			return err
		}, ErrConnectionClosed, ConnClosing},
		{"data while closing", closingTestConnection, func(c *Connection) error {
			_, err := c.StreamData(1)
			return err
		}, ErrConnectionClosed, ConnClosing},
		{"operation while closed", closedTestConnection, func(c *Connection) error { return c.AdmitOperation(op) }, ErrConnectionClosed, ConnClosed},
		{"preamble while closed", closedTestConnection, func(c *Connection) error { return c.SignalPreamble() }, ErrConnectionClosed, ConnClosed},
		{"begin close while closed", closedTestConnection, func(c *Connection) error { return c.BeginClose() }, nil, ConnClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := tc.setup(t)
			err := tc.run(conn)
			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
			require.Equal(t, tc.wantState, conn.State())
		})
	}
}

// TestConnectionLifecycleSequence walks the exact accepted lifecycle and
// proves close is idempotent in both steps.
func TestConnectionLifecycleSequence(t *testing.T) {
	conn := newTestConnection(t)
	require.Equal(t, ConnNew, conn.State())
	require.NoError(t, conn.SignalPreamble())
	require.Equal(t, ConnAwaitRegister, conn.State())
	require.NoError(t, conn.Register())
	require.Equal(t, ConnReady, conn.State())
	require.NoError(t, conn.BeginClose())
	require.Equal(t, ConnClosing, conn.State())
	require.NoError(t, conn.BeginClose())
	require.Equal(t, ConnClosing, conn.State())
	require.NoError(t, conn.FinishClose())
	require.Equal(t, ConnClosed, conn.State())
	require.NoError(t, conn.FinishClose())
	require.Equal(t, ConnClosed, conn.State())
	require.False(t, conn.Ready())
}

// TestConnectionSubscriptionGenerations is the subscription generation table:
// resubscribe advances strictly, resync and unsubscribe must carry the active
// generation exactly.
func TestConnectionSubscriptionGenerations(t *testing.T) {
	type step struct {
		name  string
		run   func(*Connection) error
		want  error
		check func(*testing.T, *Connection)
	}
	cases := []struct {
		name  string
		steps []step
	}{
		{
			name: "zero generation refused",
			steps: []step{
				{name: "subscribe zero", run: func(c *Connection) error { return c.Subscribe(0) }, want: ErrInvalidGeneration},
				{name: "no active subscription", run: func(c *Connection) error { return c.Unsubscribe(0) }, want: ErrNotSubscribed},
			},
		},
		{
			name: "resubscribe advances strictly",
			steps: []step{
				{name: "subscribe one", run: func(c *Connection) error { return c.Subscribe(1) }},
				{name: "subscribe one again", run: func(c *Connection) error { return c.Subscribe(1) }, want: ErrStaleGeneration},
				{name: "subscribe two", run: func(c *Connection) error { return c.Subscribe(2) }},
				{name: "subscribe zero", run: func(c *Connection) error { return c.Subscribe(0) }, want: ErrInvalidGeneration},
			},
		},
		{
			name: "resync must match the active generation",
			steps: []step{
				{name: "resync before subscribe", run: func(c *Connection) error { return c.Resync(1) }, want: ErrNotSubscribed},
				{name: "subscribe", run: func(c *Connection) error { return c.Subscribe(3) }},
				{name: "resync active", run: func(c *Connection) error { return c.Resync(3) }},
				{name: "resync stale", run: func(c *Connection) error { return c.Resync(2) }, want: ErrStaleGeneration},
				{name: "resync future", run: func(c *Connection) error { return c.Resync(4) }, want: ErrFutureGeneration},
			},
		},
		{
			name: "unsubscribe must match and generations are never reused",
			steps: []step{
				{name: "subscribe", run: func(c *Connection) error { return c.Subscribe(2) }},
				{name: "unsubscribe stale", run: func(c *Connection) error { return c.Unsubscribe(1) }, want: ErrStaleGeneration},
				{name: "unsubscribe future", run: func(c *Connection) error { return c.Unsubscribe(3) }, want: ErrFutureGeneration},
				{name: "unsubscribe active", run: func(c *Connection) error { return c.Unsubscribe(2) }},
				{name: "unsubscribe twice", run: func(c *Connection) error { return c.Unsubscribe(2) }, want: ErrNotSubscribed},
				{name: "resubscribe consumed generation", run: func(c *Connection) error { return c.Subscribe(2) }, want: ErrStaleGeneration},
				{name: "resubscribe newer generation", run: func(c *Connection) error { return c.Subscribe(3) }},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := readyTestConnection(t)
			for _, s := range tc.steps {
				err := s.run(conn)
				if s.want == nil {
					require.NoError(t, err, s.name)
				} else {
					require.ErrorIs(t, err, s.want, s.name)
				}
				if s.check != nil {
					s.check(t, conn)
				}
			}
		})
	}
}

// stageConnectionPart stages one snapshot part through the connection and
// requires acceptance.
func stageConnectionPart(t *testing.T, conn *Connection, part SnapshotPart) {
	t.Helper()
	_, _, err := conn.AddSnapshotPart(part)
	require.NoError(t, err)
}

// TestConnectionSubscriptionStagingRetention proves a generation advance
// discards in-flight staging, keeps the committed snapshot, and that close
// retains the commit until the connection is finally closed.
func TestConnectionSubscriptionStagingRetention(t *testing.T) {
	conn := readyTestConnection(t)
	require.NoError(t, conn.Subscribe(1))
	for _, part := range snapshotPartsFor(1, 4, sampleSnapshotHosts(), sampleSnapshotTombstones()) {
		stageConnectionPart(t, conn, part)
	}
	committed, ok := conn.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(4), committed.Revision)

	// In-flight staging of the active generation is discarded on advance.
	inFlight := snapshotPartsFor(1, 6, sampleSnapshotHosts(), nil)
	stageConnectionPart(t, conn, inFlight[0])
	require.True(t, conn.StagingActive())
	require.NoError(t, conn.Subscribe(2))
	require.False(t, conn.StagingActive())
	committed, ok = conn.Snapshot()
	require.True(t, ok, "generation advance keeps the committed snapshot")
	require.Equal(t, ports.BrokerRevision(4), committed.Revision)

	// A part of the superseded generation is refused and never applied, a
	// part of a future generation is refused, and the fresh generation must
	// start at index 0.
	stale := inFlight[1]
	_, _, err := conn.AddSnapshotPart(stale)
	require.ErrorIs(t, err, ErrStaleGeneration)
	future := inFlight[1]
	future.Generation = 3
	_, _, err = conn.AddSnapshotPart(future)
	require.ErrorIs(t, err, ErrFutureGeneration)
	next := snapshotPartsFor(2, 5, sampleSnapshotHosts(), nil)
	_, _, err = conn.AddSnapshotPart(next[1])
	require.ErrorIs(t, err, ErrSnapshotInvalid)
	require.False(t, conn.StagingActive())
	for _, part := range next {
		stageConnectionPart(t, conn, part)
	}
	committed, ok = conn.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(5), committed.Revision)

	require.NoError(t, conn.BeginClose())
	_, ok = conn.Snapshot()
	require.True(t, ok, "closing keeps the committed snapshot")
	require.NoError(t, conn.FinishClose())
	_, ok = conn.Snapshot()
	require.False(t, ok, "closed releases the snapshot")
	require.False(t, conn.StagingActive())
}

// TestConnectionSnapshotParts proves snapshot staging requires Ready plus an
// active subscription, and that every part is fenced by scope and generation.
func TestConnectionSnapshotParts(t *testing.T) {
	t.Run("part without subscription", func(t *testing.T) {
		conn := readyTestConnection(t)
		_, _, err := conn.AddSnapshotPart(snapshotPartsFor(1, 1, nil, nil)[0])
		require.ErrorIs(t, err, ErrNotSubscribed)
	})

	t.Run("unsubscribe stops staging", func(t *testing.T) {
		conn := readyTestConnection(t)
		require.NoError(t, conn.Subscribe(1))
		require.NoError(t, conn.Unsubscribe(1))
		_, _, err := conn.AddSnapshotPart(snapshotPartsFor(1, 1, nil, nil)[0])
		require.ErrorIs(t, err, ErrNotSubscribed)
	})

	t.Run("future generation rejected", func(t *testing.T) {
		conn := readyTestConnection(t)
		require.NoError(t, conn.Subscribe(2))
		part := snapshotPartsFor(1, 1, sampleSnapshotHosts(), nil)[0]
		part.Generation = 3
		_, _, err := conn.AddSnapshotPart(part)
		require.ErrorIs(t, err, ErrFutureGeneration)
		part.Generation = 1
		_, _, err = conn.AddSnapshotPart(part)
		require.ErrorIs(t, err, ErrStaleGeneration)
	})

	t.Run("foreign scope rejected", func(t *testing.T) {
		conn := readyTestConnection(t)
		require.NoError(t, conn.Subscribe(2))
		part := snapshotPartsFor(2, 1, sampleSnapshotHosts(), nil)[0]
		part.Epoch = 8
		_, _, err := conn.AddSnapshotPart(part)
		require.ErrorIs(t, err, ErrScopeMismatch)
		part = snapshotPartsFor(2, 1, sampleSnapshotHosts(), nil)[0]
		part.Connection = testConnectionID(0x77)
		_, _, err = conn.AddSnapshotPart(part)
		require.ErrorIs(t, err, ErrScopeMismatch)
	})

	t.Run("complete transfer through the connection", func(t *testing.T) {
		conn := readyTestConnection(t)
		require.NoError(t, conn.Subscribe(9))
		parts := snapshotPartsFor(9, 2, sampleSnapshotHosts(), sampleSnapshotTombstones())
		var published bool
		for _, part := range parts {
			completed, _, err := conn.AddSnapshotPart(part)
			require.NoError(t, err)
			if completed {
				published = true
			}
		}
		require.True(t, published)
		committed, ok := conn.Snapshot()
		require.True(t, ok)
		require.Equal(t, ports.BrokerRevision(2), committed.Revision)
	})
}

// TestConnectionExactScopeRegistration proves the connection keeps exactly
// the scope assigned at accept: every frame must carry it, refused frames
// change nothing, and the scope survives the whole lifecycle.
func TestConnectionExactScopeRegistration(t *testing.T) {
	conn := readyTestConnection(t)
	assigned := conn.Scope()
	require.Equal(t, testBrokerScope(), assigned)
	other := Scope{Epoch: assigned.Epoch + 1, Connection: assigned.Connection}

	require.ErrorIs(t, connectionOperations(conn).Admit(other, operationIDAt(1)), ErrScopeMismatch)
	require.ErrorIs(t, connectionOperations(conn).Complete(other, operationIDAt(1), ports.BrokerOutcomeOK), ErrScopeMismatch)
	require.ErrorIs(t, connectionStreams(conn).Open(other, 1), ErrScopeMismatch)
	_, err := connectionStreams(conn).Data(other, 1)
	require.ErrorIs(t, err, ErrScopeMismatch)
	_, err = connectionStreams(conn).Close(other, 1)
	require.ErrorIs(t, err, ErrScopeMismatch)

	part := snapshotPartsFor(1, 1, sampleSnapshotHosts(), nil)[0]
	part.Connection = other.Connection
	part.Epoch = other.Epoch
	require.NoError(t, conn.Subscribe(1))
	_, _, err = conn.AddSnapshotPart(part)
	require.ErrorIs(t, err, ErrScopeMismatch)

	// Refused frames leave no state behind, and the accepted scope still works.
	require.Equal(t, assigned, conn.Scope())
	require.Zero(t, connectionOperations(conn).Pending())
	require.Zero(t, connectionStreams(conn).Active())
	require.False(t, conn.StagingActive())
	require.NoError(t, conn.AdmitOperation(operationIDAt(2)))
	require.Equal(t, 1, connectionOperations(conn).Pending())

	require.NoError(t, conn.BeginClose())
	require.NoError(t, conn.FinishClose())
	require.Equal(t, assigned, conn.Scope())
}

// TestOperationTrackerErrors is the operation admission taxonomy table.
func TestOperationTrackerErrors(t *testing.T) {
	scope := testBrokerScope()
	op := operationIDAt(3)
	cases := []struct {
		name  string
		setup func(*testing.T, *OperationTracker)
		run   func(*OperationTracker) error
		want  error
	}{
		{"admit accepted", nil, func(tr *OperationTracker) error { return tr.Admit(scope, op) }, nil},
		{"admit zero identity", nil, func(tr *OperationTracker) error { return tr.Admit(scope, ports.BrokerOperationID{}) }, ErrInvalidOperation},
		{"admit foreign scope", nil, func(tr *OperationTracker) error { return tr.Admit(testForeignScope(), op) }, ErrScopeMismatch},
		{"admit duplicate pending", func(t *testing.T, tr *OperationTracker) {
			require.NoError(t, tr.Admit(scope, op))
		}, func(tr *OperationTracker) error { return tr.Admit(scope, op) }, ErrOperationPending},
		{"admit completed", func(t *testing.T, tr *OperationTracker) {
			require.NoError(t, tr.Admit(scope, op))
			require.NoError(t, tr.Complete(scope, op, ports.BrokerOutcomeOK))
		}, func(tr *OperationTracker) error { return tr.Admit(scope, op) }, ErrOperationCompleted},
		{"complete unknown", nil, func(tr *OperationTracker) error {
			return tr.Complete(scope, op, ports.BrokerOutcomeOK)
		}, ErrUnknownOperation},
		{"complete invalid outcome", func(t *testing.T, tr *OperationTracker) {
			require.NoError(t, tr.Admit(scope, op))
		}, func(tr *OperationTracker) error { return tr.Complete(scope, op, 0) }, ErrInvalidOutcome},
		{"complete foreign scope", func(t *testing.T, tr *OperationTracker) {
			require.NoError(t, tr.Admit(scope, op))
		}, func(tr *OperationTracker) error {
			return tr.Complete(testForeignScope(), op, ports.BrokerOutcomeOK)
		}, ErrScopeMismatch},
		{"complete twice", func(t *testing.T, tr *OperationTracker) {
			require.NoError(t, tr.Admit(scope, op))
			require.NoError(t, tr.Complete(scope, op, ports.BrokerOutcomeFailed))
		}, func(tr *OperationTracker) error { return tr.Complete(scope, op, ports.BrokerOutcomeOK) }, ErrOperationCompleted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr, err := newOperationTracker(scope)
			require.NoError(t, err)
			if tc.setup != nil {
				tc.setup(t, tr)
			}
			err = tc.run(tr)
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}
}

func TestNewOperationTrackerValidatesScope(t *testing.T) {
	tr, err := newOperationTracker(Scope{})
	require.ErrorIs(t, err, ErrInvalidScope)
	require.Nil(t, tr)
}

// TestOperationTrackerPendingBound proves at most 64 operations are in flight.
func TestOperationTrackerPendingBound(t *testing.T) {
	scope := testBrokerScope()
	tr, err := newOperationTracker(scope)
	require.NoError(t, err)
	for i := 0; i < MaxPendingOperations; i++ {
		require.NoError(t, tr.Admit(scope, operationIDAt(i)))
	}
	require.Equal(t, MaxPendingOperations, tr.Pending())
	require.ErrorIs(t, tr.Admit(scope, operationIDAt(MaxPendingOperations)), ErrTooManyPendingOperations)
	require.Equal(t, MaxPendingOperations, tr.Pending())

	// Completing one frees exactly one slot.
	require.NoError(t, tr.Complete(scope, operationIDAt(0), ports.BrokerOutcomeOK))
	require.Equal(t, MaxPendingOperations-1, tr.Pending())
	require.NoError(t, tr.Admit(scope, operationIDAt(MaxPendingOperations)))
	require.Equal(t, MaxPendingOperations, tr.Pending())
	require.ErrorIs(t, tr.Admit(scope, operationIDAt(MaxPendingOperations+1)), ErrTooManyPendingOperations)
}

// TestOperationTrackerDedupBound proves the dedup set is bounded to 4096
// identities and 64 KiB, evicting the oldest record first.
func TestOperationTrackerDedupBound(t *testing.T) {
	scope := testBrokerScope()
	tr, err := newOperationTracker(scope)
	require.NoError(t, err)
	require.Equal(t, 4096, MaxTrackedOperations)
	require.Equal(t, 65536, MaxOperationDedupBytes)

	total := MaxTrackedOperations + 128
	for i := 0; i < total; i++ {
		op := operationIDAt(i)
		require.NoError(t, tr.Admit(scope, op))
		require.NoError(t, tr.Complete(scope, op, ports.BrokerOutcomeOK))
		require.LessOrEqual(t, tr.DedupBytes(), MaxOperationDedupBytes)
	}
	require.Equal(t, MaxTrackedOperations, tr.Tracked())
	require.Equal(t, MaxOperationDedupBytes, tr.DedupBytes())
	require.Zero(t, tr.Pending())

	// The oldest records were evicted: they are no longer deduped, while the
	// newest records still are.
	evicted := operationIDAt(0)
	_, ok := tr.Outcome(evicted)
	require.False(t, ok)
	require.NoError(t, tr.Admit(scope, evicted))
	require.ErrorIs(t, tr.Admit(scope, operationIDAt(total-1)), ErrOperationCompleted)
}

// TestOperationTrackerOutcomeUnknownNoReplay proves outcome-unknown is
// recorded like any outcome and a completed identity is never replayed.
func TestOperationTrackerOutcomeUnknownNoReplay(t *testing.T) {
	scope := testBrokerScope()
	tr, err := newOperationTracker(scope)
	require.NoError(t, err)
	cases := []ports.BrokerMutationOutcome{ports.BrokerOutcomeOK, ports.BrokerOutcomeFailed, ports.BrokerOutcomeUnknown}
	for i, outcome := range cases {
		op := operationIDAt(100 + i)
		require.NoError(t, tr.Admit(scope, op))
		require.NoError(t, tr.Complete(scope, op, outcome))
		recorded, ok := tr.Outcome(op)
		require.True(t, ok)
		require.Equal(t, outcome, recorded)
		require.ErrorIs(t, tr.Admit(scope, op), ErrOperationCompleted, "never replays %s", outcome)
		require.ErrorIs(t, tr.Complete(scope, op, ports.BrokerOutcomeFailed), ErrOperationCompleted)
	}
	// An unknown outcome still frees its pending slot.
	require.Zero(t, tr.Pending())
}

// streamAction is one stream lifecycle event under test.
type streamAction func(*StreamTracker, Scope, ports.BrokerStreamID) (StreamDisposition, error)

func actionOpen(tr *StreamTracker, scope Scope, stream ports.BrokerStreamID) (StreamDisposition, error) {
	if err := tr.Open(scope, stream); err != nil {
		return 0, err
	}
	return StreamAccepted, nil
}

func actionOpened(tr *StreamTracker, scope Scope, stream ports.BrokerStreamID) (StreamDisposition, error) {
	if err := tr.Opened(scope, stream); err != nil {
		return 0, err
	}
	return StreamAccepted, nil
}

func actionData(tr *StreamTracker, scope Scope, stream ports.BrokerStreamID) (StreamDisposition, error) {
	return tr.Data(scope, stream)
}

func actionProgress(tr *StreamTracker, scope Scope, stream ports.BrokerStreamID) (StreamDisposition, error) {
	return tr.Progress(scope, stream)
}

func actionClose(tr *StreamTracker, scope Scope, stream ports.BrokerStreamID) (StreamDisposition, error) {
	return tr.Close(scope, stream)
}

func TestStreamStateStrings(t *testing.T) {
	require.Equal(t, "opening", StreamOpening.String())
	require.Equal(t, "open", StreamOpen.String())
	require.Equal(t, "retired", StreamRetired.String())
	require.Equal(t, "unknown", StreamState(0).String())
}

func TestNewStreamTrackerValidatesScope(t *testing.T) {
	tr, err := newStreamTracker(Scope{})
	require.ErrorIs(t, err, ErrInvalidScope)
	require.Nil(t, tr)
}

// TestStreamTrackerLifecycle is the stream state table: opening -> open ->
// retired, with legal progress/data/close and late retired discard.
func TestStreamTrackerLifecycle(t *testing.T) {
	cases := []struct {
		name      string
		stream    ports.BrokerStreamID
		steps     []streamStep
		wantState StreamState
		wantFound bool
	}{
		{
			name:   "open then data then close",
			stream: 1,
			steps: []streamStep{
				{name: "open", action: actionOpen, want: StreamAccepted},
				{name: "data while opening", action: actionData, wantErr: ErrStreamState},
				{name: "progress while opening", action: actionProgress, want: StreamAccepted},
				{name: "opened", action: actionOpened, want: StreamAccepted},
				{name: "opened twice", action: actionOpened, wantErr: ErrStreamState},
				{name: "data when open", action: actionData, want: StreamAccepted},
				{name: "progress when open", action: actionProgress, want: StreamAccepted},
				{name: "close", action: actionClose, want: StreamAccepted},
				{name: "late data", action: actionData, want: StreamDiscarded},
				{name: "late progress", action: actionProgress, want: StreamDiscarded},
				{name: "late close", action: actionClose, want: StreamDiscarded},
				{name: "opened after close", action: actionOpened, wantErr: ErrStreamState},
			},
			wantState: StreamRetired,
			wantFound: true,
		},
		{
			name:   "close while opening is terminal",
			stream: 4,
			steps: []streamStep{
				{name: "open", action: actionOpen, want: StreamAccepted},
				{name: "close", action: actionClose, want: StreamAccepted},
				{name: "data after close", action: actionData, want: StreamDiscarded},
				{name: "opened after close", action: actionOpened, wantErr: ErrStreamState},
			},
			wantState: StreamRetired,
			wantFound: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scope := testBrokerScope()
			tr, err := newStreamTracker(scope)
			require.NoError(t, err)
			for _, s := range tc.steps {
				disposition, err := s.action(tr, scope, tc.stream)
				if s.wantErr != nil {
					require.ErrorIs(t, err, s.wantErr, s.name)
					require.Zero(t, disposition, s.name)
					continue
				}
				require.NoError(t, err, s.name)
				require.Equal(t, s.want, disposition, s.name)
			}
			state, ok := tr.State(tc.stream)
			require.Equal(t, tc.wantFound, ok)
			require.Equal(t, tc.wantState, state)
			require.Zero(t, tr.Active())
		})
	}
}

type streamStep struct {
	name    string
	action  streamAction
	want    StreamDisposition
	wantErr error
}

// TestStreamTrackerOpenRules proves stream identities strictly increase, are
// never reused, and that at most MaxStreams streams are concurrent.
func TestStreamTrackerOpenRules(t *testing.T) {
	scope := testBrokerScope()

	t.Run("zero identity refused", func(t *testing.T) {
		tr, err := newStreamTracker(scope)
		require.NoError(t, err)
		require.ErrorIs(t, tr.Open(scope, 0), ErrInvalidStream)
	})

	t.Run("strictly increasing and never reused", func(t *testing.T) {
		tr, err := newStreamTracker(scope)
		require.NoError(t, err)
		require.NoError(t, tr.Open(scope, 5))
		require.ErrorIs(t, tr.Open(scope, 5), ErrStreamIDReused)
		require.ErrorIs(t, tr.Open(scope, 1), ErrStreamIDReused)
		require.NoError(t, tr.Opened(scope, 5))
		require.NoError(t, tr.Open(scope, 6))
		if disposition, err := tr.Close(scope, 5); err != nil || disposition != StreamAccepted {
			t.Fatalf("close: %v %v", disposition, err)
		}
		require.ErrorIs(t, tr.Open(scope, 5), ErrStreamIDReused)
		require.ErrorIs(t, tr.Open(scope, 6), ErrStreamIDReused)
		require.NoError(t, tr.Open(scope, 7))
	})

	t.Run("concurrent ceiling", func(t *testing.T) {
		tr, err := newStreamTracker(scope)
		require.NoError(t, err)
		for i := 1; i <= MaxStreams; i++ {
			require.NoError(t, tr.Open(scope, ports.BrokerStreamID(i)))
		}
		require.Equal(t, MaxStreams, tr.Active())
		require.ErrorIs(t, tr.Open(scope, ports.BrokerStreamID(MaxStreams+1)), ErrTooManyStreams)
		// Retiring one stream frees exactly one slot for a fresh higher ID.
		disposition, err := tr.Close(scope, 1)
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)
		require.Equal(t, MaxStreams-1, tr.Active())
		require.NoError(t, tr.Open(scope, ports.BrokerStreamID(MaxStreams+2)))
		require.ErrorIs(t, tr.Open(scope, ports.BrokerStreamID(MaxStreams+3)), ErrTooManyStreams)
	})

	t.Run("scope fence", func(t *testing.T) {
		tr, err := newStreamTracker(scope)
		require.NoError(t, err)
		require.ErrorIs(t, tr.Open(testForeignScope(), 1), ErrScopeMismatch)
		require.ErrorIs(t, tr.Opened(testForeignScope(), 1), ErrScopeMismatch)
		_, err = tr.Data(testForeignScope(), 1)
		require.ErrorIs(t, err, ErrScopeMismatch)
		_, err = tr.Progress(testForeignScope(), 1)
		require.ErrorIs(t, err, ErrScopeMismatch)
		_, err = tr.Close(testForeignScope(), 1)
		require.ErrorIs(t, err, ErrScopeMismatch)
		require.Zero(t, tr.Active())
	})
}

// TestStreamTrackerFutureReject proves a stream identity that was never
// allocated is refused rather than treated as retired, and that a retired
// record evicted after the retention bound is still discarded.
func TestStreamTrackerFutureReject(t *testing.T) {
	scope := testBrokerScope()
	tr, err := newStreamTracker(scope)
	require.NoError(t, err)
	require.NoError(t, tr.Open(scope, 2))
	require.NoError(t, tr.Opened(scope, 2))
	for _, action := range []streamAction{actionOpened, actionData, actionProgress, actionClose} {
		disposition, err := action(tr, scope, 3)
		require.ErrorIs(t, err, ErrFutureStream)
		require.Zero(t, disposition)
	}
	disposition, err := tr.Close(scope, 2)
	require.NoError(t, err)
	require.Equal(t, StreamAccepted, disposition)

	// Retain more retired records than the bound so the oldest is evicted.
	for i := 0; i < MaxRetiredStreams+8; i++ {
		stream := ports.BrokerStreamID(10 + i)
		require.NoError(t, tr.Open(scope, stream))
		require.NoError(t, tr.Opened(scope, stream))
		disposition, err := tr.Close(scope, stream)
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)
	}
	evicted := ports.BrokerStreamID(2)
	_, tracked := tr.State(evicted)
	require.False(t, tracked, "oldest retired record is evicted")
	disposition, err = tr.Data(scope, evicted)
	require.NoError(t, err)
	require.Equal(t, StreamDiscarded, disposition)
	disposition, err = tr.Close(scope, evicted)
	require.NoError(t, err)
	require.Equal(t, StreamDiscarded, disposition)
	_, err = tr.Data(scope, ports.BrokerStreamID(10+MaxRetiredStreams+8))
	require.ErrorIs(t, err, ErrFutureStream)
}

// TestStreamTrackerNilSafety proves the nil-receiver paths.
func TestStreamTrackerNilSafety(t *testing.T) {
	var tr *StreamTracker
	scope := testBrokerScope()
	require.ErrorIs(t, tr.Open(scope, 1), ErrConnectionClosed)
	require.ErrorIs(t, tr.Opened(scope, 1), ErrConnectionClosed)
	_, err := tr.Data(scope, 1)
	require.ErrorIs(t, err, ErrConnectionClosed)
	_, err = tr.Progress(scope, 1)
	require.ErrorIs(t, err, ErrConnectionClosed)
	_, err = tr.Close(scope, 1)
	require.ErrorIs(t, err, ErrConnectionClosed)
	require.Zero(t, tr.Active())
	_, ok := tr.State(1)
	require.False(t, ok)
}

// TestConnectionStreamsAndOperations proves the connection-bound trackers
// share the connection scope while every mutation still travels through the
// Connection lifecycle methods.
func TestConnectionStreamsAndOperations(t *testing.T) {
	conn := readyTestConnection(t)
	scope := conn.Scope()
	op := operationIDAt(7)
	require.NoError(t, conn.AdmitOperation(op))
	require.Equal(t, 1, connectionOperations(conn).Pending())
	require.NoError(t, conn.CompleteOperation(op, ports.BrokerOutcomeUnknown))
	require.Zero(t, connectionOperations(conn).Pending())
	recorded, ok := connectionOperations(conn).Outcome(op)
	require.True(t, ok)
	require.Equal(t, ports.BrokerOutcomeUnknown, recorded)

	require.NoError(t, conn.OpenStream(1))
	require.NoError(t, conn.StreamOpened(1))
	disposition, err := conn.StreamProgress(1)
	require.NoError(t, err)
	require.Equal(t, StreamAccepted, disposition)
	disposition, err = conn.StreamData(1)
	require.NoError(t, err)
	require.Equal(t, StreamAccepted, disposition)
	disposition, err = conn.CloseStream(1)
	require.NoError(t, err)
	require.Equal(t, StreamAccepted, disposition)
	disposition, err = conn.StreamData(1)
	require.NoError(t, err)
	require.Equal(t, StreamDiscarded, disposition)
	require.Zero(t, connectionStreams(conn).Active())
	require.Equal(t, scope, testBrokerScope())

	// FinishClose releases both trackers.
	require.NoError(t, conn.BeginClose())
	require.NoError(t, conn.FinishClose())
	require.Zero(t, connectionOperations(conn).Tracked())
	require.Zero(t, connectionStreams(conn).Active())
}

// TestOperationTrackerConcurrentAdmitComplete is a deterministic race test:
// exactly MaxPendingOperations distinct operations are admitted concurrently
// and completed concurrently.
func TestOperationTrackerConcurrentAdmitComplete(t *testing.T) {
	scope := testBrokerScope()
	tr, err := newOperationTracker(scope)
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 2*MaxPendingOperations)
	for i := 0; i < MaxPendingOperations; i++ {
		op := operationIDAt(i)
		wg.Add(1)
		go func(op ports.BrokerOperationID) {
			defer wg.Done()
			if err := tr.Admit(scope, op); err != nil {
				errs <- err
			}
		}(op)
	}
	wg.Wait()
	require.Equal(t, MaxPendingOperations, tr.Pending())
	for i := 0; i < MaxPendingOperations; i++ {
		op := operationIDAt(i)
		wg.Add(1)
		go func(op ports.BrokerOperationID) {
			defer wg.Done()
			if err := tr.Complete(scope, op, ports.BrokerOutcomeOK); err != nil {
				errs <- err
			}
		}(op)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Zero(t, tr.Pending())
	require.Equal(t, MaxPendingOperations, tr.Tracked())
	require.Equal(t, MaxPendingOperations*operationIDBytes, tr.DedupBytes())
}

// TestStreamTrackerConcurrentFrames is a deterministic race test: one stream
// per goroutine progresses and retires concurrently.
func TestStreamTrackerConcurrentFrames(t *testing.T) {
	scope := testBrokerScope()
	tr, err := newStreamTracker(scope)
	require.NoError(t, err)
	for i := 1; i <= MaxStreams; i++ {
		require.NoError(t, tr.Open(scope, ports.BrokerStreamID(i)))
	}
	var wg sync.WaitGroup
	errs := make(chan error, MaxStreams)
	for i := 1; i <= MaxStreams; i++ {
		stream := ports.BrokerStreamID(i)
		wg.Add(1)
		go func(stream ports.BrokerStreamID) {
			defer wg.Done()
			if _, err := tr.Progress(scope, stream); err != nil {
				errs <- err
			}
			if err := tr.Opened(scope, stream); err != nil {
				errs <- err
			}
			if disposition, err := tr.Data(scope, stream); err != nil || disposition != StreamAccepted {
				errs <- err
			}
			if disposition, err := tr.Close(scope, stream); err != nil || disposition != StreamAccepted {
				errs <- err
			}
		}(stream)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Zero(t, tr.Active())
}

// TestConnectionConcurrentWork is a deterministic race test: the connection
// publishes sequential revisions while readers observe it and operations
// complete concurrently.
func TestConnectionConcurrentWork(t *testing.T) {
	conn := readyTestConnection(t)
	require.NoError(t, conn.Subscribe(1))
	hosts := sampleSnapshotHosts()
	const revisions = 20
	const operations = 32
	for i := 0; i < operations; i++ {
		require.NoError(t, conn.AdmitOperation(operationIDAt(i)))
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	errs := make(chan error, operations)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				assert.Equal(t, ConnReady, conn.State())
				assert.Equal(t, testBrokerScope(), conn.Scope())
				if snapshot, ok := conn.Snapshot(); ok {
					assert.NotZero(t, snapshot.Revision)
				}
			}
		}()
	}
	for i := 0; i < operations; i++ {
		op := operationIDAt(i)
		wg.Add(1)
		go func(op ports.BrokerOperationID) {
			defer wg.Done()
			if err := conn.CompleteOperation(op, ports.BrokerOutcomeOK); err != nil {
				errs <- err
			}
		}(op)
	}
	for revision := 1; revision <= revisions; revision++ {
		for _, part := range snapshotPartsFor(1, ports.BrokerRevision(revision), hosts, nil) {
			_, _, err := conn.AddSnapshotPart(part)
			require.NoError(t, err)
		}
	}
	close(done)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Zero(t, connectionOperations(conn).Pending())
	require.Equal(t, operations, connectionOperations(conn).Tracked())
	snapshot, ok := conn.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(revisions), snapshot.Revision)
}

// TestConnectionCloseRaceNoPostCloseAdmission proves the connection lock
// covers both the ready check and the tracker mutation: whenever a close and
// an admit/open race, the close wins the state transition and leaves no
// admitted operation or opened stream behind, and every attempt after close
// is refused.
func TestConnectionCloseRaceNoPostCloseAdmission(t *testing.T) {
	t.Run("admit", func(t *testing.T) {
		for i := 0; i < 500; i++ {
			conn := readyTestConnection(t)
			op := operationIDAt(1)
			var wg sync.WaitGroup
			start := make(chan struct{})
			var admitErr error
			wg.Add(2)
			go func() { defer wg.Done(); <-start; admitErr = conn.AdmitOperation(op) }()
			go func() { defer wg.Done(); <-start; _ = conn.BeginClose(); _ = conn.FinishClose() }()
			close(start)
			wg.Wait()
			require.Equal(t, ConnClosed, conn.State(), "iteration %d", i)
			require.Zero(t, connectionOperations(conn).Pending(), "iteration %d: no admitted operation survives close", i)
			if admitErr != nil {
				require.ErrorIs(t, admitErr, ErrConnectionClosed, "iteration %d", i)
			}
			// The closed connection admits nothing, and a refused attempt
			// mutates nothing.
			require.ErrorIs(t, conn.AdmitOperation(op), ErrConnectionClosed)
			require.Zero(t, connectionOperations(conn).Pending())
		}
	})

	t.Run("open", func(t *testing.T) {
		for i := 0; i < 500; i++ {
			conn := readyTestConnection(t)
			var wg sync.WaitGroup
			start := make(chan struct{})
			var openErr error
			wg.Add(2)
			go func() { defer wg.Done(); <-start; openErr = conn.OpenStream(1) }()
			go func() { defer wg.Done(); <-start; _ = conn.BeginClose(); _ = conn.FinishClose() }()
			close(start)
			wg.Wait()
			require.Equal(t, ConnClosed, conn.State(), "iteration %d", i)
			require.Zero(t, connectionStreams(conn).Active(), "iteration %d: no opened stream survives close", i)
			if openErr != nil {
				require.ErrorIs(t, openErr, ErrConnectionClosed, "iteration %d", i)
			}
			require.ErrorIs(t, conn.OpenStream(1), ErrConnectionClosed)
			require.Zero(t, connectionStreams(conn).Active())
		}
	})

	t.Run("burst", func(t *testing.T) {
		conn := readyTestConnection(t)
		const attempts = MaxPendingOperations
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, attempts)
		wg.Add(attempts + 1)
		for i := 0; i < attempts; i++ {
			go func(i int) {
				defer wg.Done()
				<-start
				errs[i] = conn.AdmitOperation(operationIDAt(i))
			}(i)
		}
		go func() {
			defer wg.Done()
			<-start
			_ = conn.BeginClose()
			_ = conn.FinishClose()
		}()
		close(start)
		wg.Wait()
		require.Equal(t, ConnClosed, conn.State())
		require.Zero(t, connectionOperations(conn).Pending(), "no admitted operation survives the racing close")
		for i, err := range errs {
			if err != nil {
				require.ErrorIs(t, err, ErrConnectionClosed, "attempt %d", i)
			}
		}
		require.ErrorIs(t, conn.AdmitOperation(operationIDAt(attempts+1)), ErrConnectionClosed)
	})
}

// TestConnectionCloseRaceNoPostCloseSnapshotCommit proves the connection lock
// covers both the ready check and the assembler mutation: whenever a close and
// an Add race, no snapshot can commit after BeginClose returned, no part can be
// accepted after the close fence, and an Add that wins the race holds the lock
// across its commit, so its revision is already visible when BeginClose
// returns.
func TestConnectionCloseRaceNoPostCloseSnapshotCommit(t *testing.T) {
	const revision = 6
	const attempts = 500

	t.Run("staged end", func(t *testing.T) {
		for i := 0; i < attempts; i++ {
			conn := readyTestConnection(t)
			require.NoError(t, conn.Subscribe(2))
			parts := snapshotPartsFor(2, revision, sampleSnapshotHosts(), nil)
			end := parts[len(parts)-1]
			for _, part := range parts[:len(parts)-1] {
				stageConnectionPart(t, conn, part)
			}
			require.True(t, conn.StagingActive())
			_, committedBefore := conn.Snapshot()
			require.False(t, committedBefore, "iteration %d: nothing publishes before End", i)

			var completed bool
			var addedRevision ports.BrokerRevision
			committedAtClose, closeRevision, errs := raceConnectionClose(t, conn, func() error {
				done, published, err := conn.AddSnapshotPart(end)
				if done {
					completed = true
					addedRevision = published.Revision
				}
				return err
			})
			addErr := errs[0]

			if completed {
				// A committed End is already visible when BeginClose returned;
				// it never lands in the closing window.
				require.NoError(t, addErr, "iteration %d", i)
				require.True(t, committedAtClose, "iteration %d: commit landed after close", i)
				require.Equal(t, ports.BrokerRevision(revision), addedRevision, "iteration %d", i)
				require.Equal(t, addedRevision, closeRevision, "iteration %d: commit changed after close", i)
			} else {
				require.ErrorIs(t, addErr, ErrConnectionClosed, "iteration %d", i)
				require.False(t, committedAtClose, "iteration %d: refused part must not publish", i)
			}
			requireClosedConnectionRejects(t, conn, i, end)
		}
	})

	t.Run("empty transfer", func(t *testing.T) {
		// An empty transfer's Begin stages and its End publishes. Racing the
		// close against both parts proves no commit can land after BeginClose
		// returned, whatever order the two Adds run in: an End that runs before
		// its Begin is refused as an invalid transfer, and a part that runs
		// after the close is refused as closed.
		for i := 0; i < attempts; i++ {
			conn := readyTestConnection(t)
			require.NoError(t, conn.Subscribe(2))
			parts := snapshotPartsFor(2, revision, nil, nil)
			begin, end := parts[0], parts[1]

			var completed bool
			var addedRevision ports.BrokerRevision
			committedAtClose, _, errs := raceConnectionClose(t, conn,
				func() error {
					_, _, err := conn.AddSnapshotPart(begin)
					return err
				},
				func() error {
					done, published, err := conn.AddSnapshotPart(end)
					if done {
						completed = true
						addedRevision = published.Revision
					}
					return err
				},
			)
			beginErr, endErr := errs[0], errs[1]

			if beginErr != nil {
				require.ErrorIs(t, beginErr, ErrConnectionClosed, "iteration %d", i)
			}
			if completed {
				require.NoError(t, endErr, "iteration %d", i)
				require.True(t, committedAtClose, "iteration %d: commit landed after close", i)
				require.Equal(t, ports.BrokerRevision(revision), addedRevision, "iteration %d", i)
			} else {
				require.Error(t, endErr, "iteration %d", i)
				require.False(t, committedAtClose, "iteration %d: refused transfer must not publish", i)
			}
			requireClosedConnectionRejects(t, conn, i, end)
		}
	})
}

// raceConnectionClose runs every supplied Add concurrently with one full close
// and reports whether a snapshot was committed at the moment BeginClose
// returned, that revision, and each Add's error in argument order.
func raceConnectionClose(t *testing.T, conn *Connection, adds ...func() error) (bool, ports.BrokerRevision, []error) {
	t.Helper()
	errs := make([]error, len(adds))
	var committedAtClose bool
	var closeRevision ports.BrokerRevision
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(len(adds) + 1)
	for i, add := range adds {
		go func(i int, add func() error) {
			defer wg.Done()
			<-start
			errs[i] = add()
		}(i, add)
	}
	go func() {
		defer wg.Done()
		<-start
		_ = conn.BeginClose()
		published, ok := conn.Snapshot()
		committedAtClose = ok
		closeRevision = published.Revision
		_ = conn.FinishClose()
	}()
	close(start)
	wg.Wait()
	return committedAtClose, closeRevision, errs
}

// requireClosedConnectionRejects proves a closed connection retains no snapshot
// and accepts no further part.
func requireClosedConnectionRejects(t *testing.T, conn *Connection, iteration int, part SnapshotPart) {
	t.Helper()
	require.Equal(t, ConnClosed, conn.State(), "iteration %d", iteration)
	require.False(t, conn.StagingActive(), "iteration %d", iteration)
	_, ok := conn.Snapshot()
	require.False(t, ok, "iteration %d: closed releases the snapshot", iteration)
	_, _, err := conn.AddSnapshotPart(part)
	require.ErrorIs(t, err, ErrConnectionClosed, "iteration %d", iteration)
}

// TestConnectionNilSafety proves the nil-receiver paths fail closed.
func TestConnectionNilSafety(t *testing.T) {
	var conn *Connection
	require.Equal(t, ConnClosed, conn.State())
	require.False(t, conn.Ready())
	require.Equal(t, Scope{}, conn.Scope())
	require.ErrorIs(t, conn.SignalPreamble(), ErrConnectionClosed)
	require.ErrorIs(t, conn.Register(), ErrConnectionClosed)
	require.ErrorIs(t, conn.Subscribe(1), ErrConnectionClosed)
	require.ErrorIs(t, conn.Resync(1), ErrConnectionClosed)
	require.ErrorIs(t, conn.Unsubscribe(1), ErrConnectionClosed)
	require.ErrorIs(t, conn.AdmitOperation(operationIDAt(1)), ErrConnectionClosed)
	require.ErrorIs(t, conn.CompleteOperation(operationIDAt(1), ports.BrokerOutcomeOK), ErrConnectionClosed)
	require.ErrorIs(t, conn.OpenStream(1), ErrConnectionClosed)
	_, err := conn.StreamData(1)
	require.ErrorIs(t, err, ErrConnectionClosed)
	_, err = conn.StreamProgress(1)
	require.ErrorIs(t, err, ErrConnectionClosed)
	_, err = conn.CloseStream(1)
	require.ErrorIs(t, err, ErrConnectionClosed)
	_, _, err = conn.AddSnapshotPart(snapshotPartsFor(1, 1, nil, nil)[0])
	require.ErrorIs(t, err, ErrConnectionClosed)
	_, ok := conn.Snapshot()
	require.False(t, ok)
	require.NoError(t, conn.BeginClose())
	require.NoError(t, conn.FinishClose())
}

// TestOperationTrackerNilSafety and TestStreamTrackerNilSafety above cover the
// tracker-receiver paths; this proves the nil tracker pointer paths.
func TestNilOperationTracker(t *testing.T) {
	var tr *OperationTracker
	scope := testBrokerScope()
	require.ErrorIs(t, tr.Admit(scope, operationIDAt(1)), ErrConnectionClosed)
	require.ErrorIs(t, tr.Complete(scope, operationIDAt(1), ports.BrokerOutcomeOK), ErrConnectionClosed)
	require.Zero(t, tr.Pending())
	require.Zero(t, tr.Tracked())
	require.Zero(t, tr.DedupBytes())
	_, ok := tr.Outcome(operationIDAt(1))
	require.False(t, ok)
}
