package brokeripc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
)

// TestSnapshotPartsLayoutRoundTrips proves the emitter produces exactly the
// multipart layout the P3.1 assembler commits, including tombstones and
// session-less hosts.
func TestSnapshotPartsLayoutRoundTrips(t *testing.T) {
	snapshot := testSnapshot(9, 12)
	snapshot.Daemons = append(snapshot.Daemons, testDaemonAt(1))
	snapshot.Removed = []ports.BrokerHostTombstone{{
		Endpoint:        "retired@old:22",
		Registration:    testDaemonAt(0).Registration,
		RetiredRevision: 3,
	}}
	snapshot.Removed[0].Registration.Endpoint = "retired@old:22"
	snapshot.Removed[0].Endpoint = "retired@old:22"
	require.NoError(t, snapshot.Validate())

	parts, err := snapshotParts(snapshot, 9, ports.BrokerConnectionID{9, 9}, 3)
	require.NoError(t, err)
	require.Len(t, parts, 1+2+2+1+1)

	begin, ok := parts[0].Part.(brokerwire.SnapshotBegin)
	require.True(t, ok)
	require.Equal(t, uint32(2), begin.HostCount)
	require.Equal(t, uint32(2), begin.SessionCount)
	require.Equal(t, uint32(1), begin.TombstoneCount)
	_, ok = parts[len(parts)-1].Part.(brokerwire.SnapshotEnd)
	require.True(t, ok)

	assembler := brokerwire.NewSnapshotAssembler(9, ports.BrokerConnectionID{9, 9}, brokerwire.WithGeneration(3))
	var committed ports.BrokerSnapshot
	found := false
	for index, part := range parts {
		require.Equal(t, uint32(index), part.Index)
		require.Equal(t, brokerwire.SubscriptionGeneration(3), part.Generation)
		done, assembled, err := assembler.Add(part)
		require.NoError(t, err, "part %d", index)
		if done {
			committed = assembled
			found = true
		}
	}
	require.True(t, found, "the transfer must commit")
	require.Equal(t, snapshot.Revision, committed.Revision)
	require.Len(t, committed.Daemons, 2)
	require.Len(t, committed.Daemons[0].Sessions, 2)
	for _, host := range committed.Daemons {
		for _, session := range host.Sessions {
			require.Equal(t, []catalogue.RemoteCatalogTab{}, session.Tabs)
		}
	}
	require.Len(t, committed.Removed, 1)
	require.Equal(t, "retired@old:22", committed.Removed[0].Endpoint)
}

// TestSnapshotPartsLocalDaemonLayoutRoundTrips proves the emitter and the
// assembler agree on a local-first transfer: Begin advertises local_present,
// the local daemon is index 0 with its session bound locally, and the committed
// snapshot reproduces the local-first publication.
func TestSnapshotPartsLocalDaemonLayoutRoundTrips(t *testing.T) {
	snapshot := testLocalSnapshot(4, 2)
	require.NoError(t, snapshot.Validate())

	parts, err := snapshotParts(snapshot, 4, ports.BrokerConnectionID{4, 4}, 2)
	require.NoError(t, err)
	require.Len(t, parts, 1+2+2+1)

	begin, ok := parts[0].Part.(brokerwire.SnapshotBegin)
	require.True(t, ok)
	require.True(t, begin.LocalPresent)
	require.Equal(t, uint32(2), begin.HostCount)
	require.Equal(t, uint32(2), begin.SessionCount)

	assembler := brokerwire.NewSnapshotAssembler(4, ports.BrokerConnectionID{4, 4}, brokerwire.WithGeneration(2))
	var committed ports.BrokerSnapshot
	found := false
	for index, part := range parts {
		require.Equal(t, uint32(index), part.Index)
		done, assembled, err := assembler.Add(part)
		require.NoError(t, err, "part %d", index)
		if done {
			committed = assembled
			found = true
		}
	}
	require.True(t, found, "the transfer must commit")
	require.Equal(t, snapshot.Revision, committed.Revision)
	require.Len(t, committed.Daemons, 2)
	require.True(t, committed.Daemons[0].Local)
	require.Empty(t, committed.Daemons[0].Endpoint)
	require.Len(t, committed.Daemons[0].Sessions, 1)
	require.Equal(t, snapshot.Daemons[0].Sessions[0].Name, committed.Daemons[0].Sessions[0].Name)
	require.Equal(t, "user0@host0:22", committed.Daemons[1].Endpoint)
	require.Len(t, committed.Daemons[1].Sessions, 1)
}

// TestSnapshotPartsRefusesOverBoundInventory proves the emitter fails closed on
// an inventory the peer's assembler would refuse, instead of silently sending a
// transfer that can never commit.
func TestSnapshotPartsRefusesOverBoundInventory(t *testing.T) {
	snapshot := testSnapshot(1, 1)
	over := make([]catalogue.RemoteCatalogSession, ports.BrokerMaxSessionsPerHost+1)
	snapshot.Daemons[0].Sessions = over
	_, err := snapshotParts(snapshot, 1, ports.BrokerConnectionID{1}, 1)
	require.Error(t, err)

	snapshot = testSnapshot(0, 1)
	_, err = snapshotParts(snapshot, 0, ports.BrokerConnectionID{1}, 1)
	require.ErrorIs(t, err, ErrScopeMismatch)
}

// TestSnapshotPartsRequiresRevision proves a snapshot without a publication
// revision is refused rather than published as revision zero.
func TestSnapshotPartsRequiresRevision(t *testing.T) {
	snapshot := testSnapshot(1, 1)
	snapshot.Revision = 0
	_, err := snapshotParts(snapshot, 1, ports.BrokerConnectionID{1}, 1)
	require.Error(t, err)
}

// TestConfigDefaultsAndValidation proves the zero configuration is valid and an
// unusable explicit bound is refused.
func TestConfigDefaultsAndValidation(t *testing.T) {
	cfg := Config{}.withDefaults()
	require.NoError(t, cfg.validate())
	require.Equal(t, DefaultMaxClients, cfg.MaxClients)
	require.Equal(t, DefaultStreamInboundChunks, cfg.StreamInboundChunks)
	require.Equal(t, uint64(DefaultStreamInboundBytes), cfg.StreamInboundBytes)
	require.Equal(t, brokerwire.MaxPendingOperations, cfg.MaxPendingOperations)

	require.ErrorIs(t, (Config{StreamInboundBytes: 1}).withDefaults().validate(), ErrConfig)
	require.ErrorIs(t, (Config{MaxPendingOperations: brokerwire.MaxPendingOperations + 1}).withDefaults().validate(), ErrConfig)
	// A negative explicit bound is replaced by its default rather than refused.
	require.NoError(t, (Config{MaxClients: -1, StreamInboundChunks: -1}).withDefaults().validate())
}

// TestStreamPipeBoundsAndClose proves the per-stream byte channel is bounded,
// reports backpressure instead of growing, and releases a parked reader on
// close.
func TestStreamPipeBoundsAndClose(t *testing.T) {
	pipe := newStreamPipe(2, 4096, func([]byte) error { return nil })
	require.NoError(t, pipe.deliver([]byte("one")))
	require.NoError(t, pipe.deliver([]byte("two")))
	require.ErrorIs(t, pipe.deliver([]byte("three")), ErrStreamBackpressure)

	buf := make([]byte, 3)
	n, err := pipe.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "one", string(buf[:n]))
	require.NoError(t, pipe.deliver([]byte("three")))

	// An orderly Close keeps what is already queued ("two" and "three"): Read
	// drains it before observing io.EOF, so a chunk delivered just ahead of an
	// orderly close is never discarded underneath its reader (see
	// TestStreamPipeCloseWithPreservesQueuedDataOnOrderlyClose for the focused
	// coverage of this behavior).
	require.NoError(t, pipe.Close())
	require.ErrorIs(t, pipe.deliver([]byte("four")), ErrStreamGone)
	drained := make([]byte, 8)
	n, err = pipe.Read(drained)
	require.NoError(t, err)
	require.Equal(t, "two", string(drained[:n]))
	n, err = pipe.Read(drained)
	require.NoError(t, err)
	require.Equal(t, "three", string(drained[:n]))
	_, err = pipe.Read(drained)
	require.ErrorIs(t, err, io.EOF)

	// A reader parked on an empty pipe is released by closeWith with its cause.
	parked := newStreamPipe(1, 4096, func([]byte) error { return nil })
	done := make(chan error, 1)
	go func() {
		_, err := parked.Read(buf)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	parked.closeWith(ErrStreamBackpressure)
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrStreamBackpressure)
	case <-time.After(5 * time.Second):
		t.Fatal("closeWith must release a parked reader")
	}
}

// TestStreamPipeOutboundError proves a failing outbound hook surfaces instead of
// silently dropping bytes.
func TestStreamPipeOutboundError(t *testing.T) {
	pipe := newStreamPipe(1, 4096, func([]byte) error { return ErrStreamBackpressure })
	_, err := pipe.Write([]byte("payload"))
	require.ErrorIs(t, err, ErrStreamBackpressure)
	require.NoError(t, pipe.Close())
	_, err = pipe.Write([]byte("payload"))
	require.ErrorIs(t, err, ErrStreamGone)
}

// TestErrorDetailMapping proves the wire detail and the local typed failure stay
// mutually consistent, including admission and context outcomes.
func TestErrorDetailMapping(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		code      ports.BrokerErrorCode
		admission uint32
	}{
		{"typed", ports.BrokerError{Code: ports.BrokerErrorIncompatible, Text: "no"}, ports.BrokerErrorIncompatible, 0},
		{"no daemon", ports.BrokerError{Code: ports.BrokerErrorNoDaemon, Text: "remote daemon is absent"}, ports.BrokerErrorNoDaemon, 0},
		{"limit", ports.BrokerAdmissionLimit, ports.BrokerErrorUnavailable, 1},
		{"closed", ports.BrokerAdmissionClosed, ports.BrokerErrorUnavailable, 2},
		{"stale", ports.BrokerAdmissionStale, ports.BrokerErrorUnavailable, 3},
		{"invalid", ports.BrokerAdmissionInvalid, ports.BrokerErrorUnavailable, 4},
		{"deadline", context.DeadlineExceeded, ports.BrokerErrorTimeout, 0},
		{"cancelled", context.Canceled, ports.BrokerErrorCancelled, 0},
		// An indeterminate store outcome is classified before the context and
		// transport rules: a failure after the durable commit point must never be
		// presented as a retryable timeout, however its cause would classify
		// alone. Both the value and the pointer form are legal errors because the
		// ports type carries value-receiver methods.
		{"outcome unknown value", ports.BrokerStoreOutcomeUnknownError{Err: errors.New("power loss")}, ports.BrokerErrorOutcomeUnknown, 0},
		{"outcome unknown wrapping deadline", ports.BrokerStoreOutcomeUnknownError{Err: context.DeadlineExceeded}, ports.BrokerErrorOutcomeUnknown, 0},
		{"outcome unknown pointer", &ports.BrokerStoreOutcomeUnknownError{Err: context.DeadlineExceeded}, ports.BrokerErrorOutcomeUnknown, 0},
		{"unknown", errors.New("boom"), ports.BrokerErrorUnavailable, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			detail := errorDetail(tc.err)
			require.Equal(t, tc.code, detail.Code)
			require.Equal(t, tc.admission, detail.AdmissionCode)
			// The wire encoder validates the detail exactly as a peer would.
			_, err := brokerwire.EncodeServer(brokerwire.StreamClosed{
				Epoch: 1, Connection: ports.BrokerConnectionID{1}, Stream: 1,
				Error: detail, HasError: detail != (brokerwire.ErrorDetail{}),
			}, brokerwire.MaxBrokerEnvelopeBytes, brokerwire.MaxStreamChunkBytes)
			require.NoError(t, err, "a produced detail must always be wire-valid")
		})
	}
	require.Equal(t, brokerwire.ErrorDetail{}, errorDetail(nil))
	require.NoError(t, failureFromDetail(brokerwire.ErrorDetail{}), "absent detail is an orderly outcome")

	scoped := failureFromDetail(errorDetail(ports.BrokerAdmissionLimit))
	require.ErrorIs(t, scoped, ports.BrokerAdmissionLimit)
}

// TestErrorDetailKeepsStreamLossCause proves a physical stream loss reaches
// the client with its failure kind and a bounded, display-safe cause in the
// display text instead of a bare attachment_lost.
func TestErrorDetailKeepsStreamLossCause(t *testing.T) {
	lost := func(err error) ports.BrokerStreamLost {
		return ports.BrokerStreamLost{Connection: ports.BrokerConnectionID{1}, Stream: 1, Epoch: 1, Cause: domain.RemoteFailureTimeout, Err: err}
	}
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"with cause", lost(errors.New("quic: idle timeout")), "timeout: quic: idle timeout"},
		{"without cause", lost(nil), "timeout"},
		{"wrapped", fmt.Errorf("attach: %w", lost(errors.New("reset"))), "timeout: reset"},
		{"control characters removed", lost(errors.New("bad\x1b[31m\nline")), "timeout: bad[31mline"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			detail := errorDetail(tc.err)
			require.Equal(t, ports.BrokerErrorAttachmentLost, detail.Code)
			require.Equal(t, tc.want, detail.Text)
			_, err := brokerwire.EncodeServer(brokerwire.StreamClosed{
				Epoch: 1, Connection: ports.BrokerConnectionID{1}, Stream: 1, Error: detail, HasError: true,
			}, brokerwire.MaxBrokerEnvelopeBytes, brokerwire.MaxStreamChunkBytes)
			require.NoError(t, err)

			var typed ports.BrokerError
			require.ErrorAs(t, failureFromDetail(detail), &typed)
			require.Equal(t, tc.want, typed.Text)
		})
	}
}

// TestAdmissionErrorMapping proves connection-tracker refusals map onto the
// closed ports admission taxonomy.
func TestAdmissionErrorMapping(t *testing.T) {
	require.NoError(t, admissionError(nil))
	require.ErrorIs(t, admissionError(brokerwire.ErrTooManyStreams), ports.BrokerAdmissionLimit)
	require.ErrorIs(t, admissionError(brokerwire.ErrTooManyPendingOperations), ports.BrokerAdmissionLimit)
	require.ErrorIs(t, admissionError(brokerwire.ErrConnectionClosed), ports.BrokerAdmissionClosed)
	require.ErrorIs(t, admissionError(brokerwire.ErrStreamIDReused), ports.BrokerAdmissionStale)
	require.ErrorIs(t, admissionError(brokerwire.ErrFutureStream), ports.BrokerAdmissionInvalid)
}

// TestScopeRequestFillsIdentity proves an absent epoch/connection identity is
// filled from the connection, a foreign one is refused as stale, and a request
// without a stream identity is refused as invalid rather than allocated here.
func TestScopeRequestFillsIdentity(t *testing.T) {
	c := &client{
		scope:     brokerwire.Scope{Epoch: 4, Connection: ports.BrokerConnectionID{4}},
		ceilings:  brokerwire.DefaultCeilings(),
		pending:   map[ports.BrokerOperationID]chan operationResult{},
		streams:   map[ports.BrokerStreamID]*clientStream{},
		done:      make(chan struct{}),
		cfg:       Config{}.withDefaults(),
		transport: nil,
	}
	scoped, err := c.scopeRequest(openRequest(1))
	require.NoError(t, err)
	require.Equal(t, ports.BrokerEpoch(4), scoped.Epoch)
	require.Equal(t, ports.BrokerConnectionID{4}, scoped.Connection)
	require.Equal(t, ports.BrokerStreamID(1), scoped.Stream)

	_, err = c.scopeRequest(ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamControl, Local: true, Stream: 1, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded,
		Connection: ports.BrokerConnectionID{0x11},
	})
	require.ErrorIs(t, err, ports.BrokerAdmissionStale)

	_, err = c.scopeRequest(ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamControl, Local: true, Stream: 1, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded, Epoch: 99,
	})
	require.ErrorIs(t, err, ErrScopeMismatch)

	scoped, err = c.scopeRequest(openRequest(5))
	require.NoError(t, err)
	require.Equal(t, ports.BrokerStreamID(5), scoped.Stream)

	// The adapter never invents a stream identity: an incomplete request is
	// refused, and the calling service is the only allocator.
	_, err = c.scopeRequest(ports.BrokerOpenStreamRequest{
		Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(), StartMode: ports.BrokerDaemonStartIfNeeded,
	})
	require.ErrorIs(t, err, ports.BrokerAdmissionInvalid)
}

// TestSocketPaths proves the endpoint name is distinct from the daemon socket
// and lives in the per-user runtime directory.
func TestSocketPaths(t *testing.T) {
	require.Equal(t, "/run/x/vev/broker.sock", SocketPath("/run/x/vev"))
	require.Equal(t, "broker.sock", SocketFileName)
	require.NotEqual(t, "daemon.sock", SocketFileName)
	require.Contains(t, DefaultSocketPath(), SocketFileName)
}

// TestPreambleCarriageIgnoresUnboundedTransport proves a carriage that cannot be
// bounded is refused rather than read without a bound.
func TestPreambleCarriageIgnoresUnboundedTransport(t *testing.T) {
	_, err := recvPreamble(unboundedTransport{})
	require.ErrorIs(t, err, ErrConfig)
}

// unboundedTransport implements only wire.Transport.
type unboundedTransport struct{}

func (unboundedTransport) Send(wire.Envelope) error { return nil }
func (unboundedTransport) Recv() (wire.Envelope, error) {
	return wire.Envelope{}, io.EOF
}
func (unboundedTransport) Close() error { return nil }

// TestServerSessionRejectsInvalidConstruction proves construction refuses a nil
// carriage, a nil core, and a zero epoch instead of building a session that can
// never serve.
func TestServerSessionRejectsInvalidConstruction(t *testing.T) {
	core := &fakeCore{id: ports.BrokerConnectionID{1}, hub: newSnapshotHub(ports.BrokerSnapshot{}), streams: map[ports.BrokerStreamID]*fakeLogicalConn{}, done: make(chan struct{})}
	_, err := newServerSession(0, nil, brokerwire.DefaultCeilings(), core, Config{}, nil, time.Time{})
	require.ErrorIs(t, err, ErrConfig)
	_, err = newServerSession(1, nil, brokerwire.DefaultCeilings(), nil, Config{}, nil, time.Time{})
	require.ErrorIs(t, err, ErrConfig)
	require.NoError(t, brokerwire.DefaultCeilings().Validate())
	require.Error(t, brokerwire.Ceilings{}.Validate())
}

// TestOrderlyDisconnectClassification proves a peer disconnect, a local close,
// and a peer registration timeout are orderly ends while a protocol violation
// is not.
func TestOrderlyDisconnectClassification(t *testing.T) {
	require.True(t, orderlyDisconnect(nil))
	require.True(t, orderlyDisconnect(io.EOF))
	require.True(t, orderlyDisconnect(ErrSessionClosed))
	require.True(t, orderlyDisconnect(ErrConnectionClosed))
	require.True(t, orderlyDisconnect(errors.Join(ErrRegistrationTimeout, context.DeadlineExceeded)))
	require.True(t, orderlyDisconnect(&net.OpError{Op: "read", Net: "unix", Err: syscall.ECONNRESET}))
	require.True(t, orderlyDisconnect(net.ErrClosed))
	require.True(t, orderlyDisconnect(fs.ErrClosed))
	require.False(t, orderlyDisconnect(ErrMalformedFrame))
	require.False(t, orderlyDisconnect(ErrProtocol))
}

// TestStreamBridgeRequiresConfig proves the bridge refuses missing participants
// rather than building a half-wired stream.
func TestStreamBridgeRequiresConfig(t *testing.T) {
	_, err := newServerStream(nil, 1, nil)
	require.ErrorIs(t, err, ErrConfig)
	_, err = newClientStream(nil, 1)
	require.ErrorIs(t, err, ErrConfig)
}

// TestClientSubscriptionCoalesces proves the local subscription wakes once per
// publication and closes exactly once.
func TestClientSubscriptionCoalesces(t *testing.T) {
	sub := newClientSubscription()
	sub.notify()
	sub.notify()
	<-sub.Changed()
	select {
	case <-sub.Changed():
		t.Fatal("notifications must coalesce")
	default:
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sub.Close() }()
	}
	wg.Wait()
	_, open := <-sub.Changed()
	require.False(t, open)
}
