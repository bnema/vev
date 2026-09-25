package daemonmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// errMemCarrierClosed is the synthetic carrier error an in-memory endpoint
// returns once its shared link is closed, mirroring the adapter prompt-close
// contract.
var errMemCarrierClosed = errors.New("memCarrier: closed")

// memCarrier is one end of a paired, in-memory FramedCarrier link. Two
// endpoints share a closed channel and exchange complete envelopes over two
// buffered channels, so one pump's reader is the other pump's writer.
type memCarrier struct {
	out  chan []byte
	in   chan []byte
	done chan struct{}
	once *sync.Once
}

func newMemCarrierPair(buffer int) (*memCarrier, *memCarrier) {
	aToB := make(chan []byte, buffer)
	bToA := make(chan []byte, buffer)
	done := make(chan struct{})
	once := &sync.Once{}
	a := &memCarrier{out: aToB, in: bToA, done: done, once: once}
	b := &memCarrier{out: bToA, in: aToB, done: done, once: once}
	return a, b
}

func (m *memCarrier) Send(ctx context.Context, payload []byte) error {
	select {
	case m.out <- append([]byte(nil), payload...):
		return nil
	case <-m.done:
		return errMemCarrierClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *memCarrier) Receive(ctx context.Context) ([]byte, error) {
	select {
	case payload := <-m.in:
		return payload, nil
	case <-m.done:
		return nil, errMemCarrierClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *memCarrier) Close() error {
	m.once.Do(func() { close(m.done) })
	return nil
}

// newPairedPumps builds a broker-side pump (decodes server frames, emits client
// frames) and a daemon-side pump (decodes client frames, emits server frames)
// joined by one in-memory link, starts both, and closes them at test end.
func newPairedPumps(t *testing.T, ceilings MuxCeilings) (*Pump, *Pump) {
	t.Helper()
	broker, daemon, start := newPairedPumpsUnstarted(t, ceilings)
	start()
	return broker, daemon
}

// newPairedPumpsUnstarted builds the same paired pump link as newPairedPumps
// but leaves both pumps unstarted, so a daemon-side consumer can register its
// admission observer before the pump's reader begins (the pump refuses a late
// registration with ErrAdmissionObserverLate). The returned start function
// starts both pumps exactly where the caller needs them running; the pumps are
// closed at test end.
func newPairedPumpsUnstarted(t *testing.T, ceilings MuxCeilings) (*Pump, *Pump, func()) {
	t.Helper()
	brokerCarrier, daemonCarrier := newMemCarrierPair(4096)
	broker, err := NewPump(brokerCarrier, DirectionServer, ceilings)
	require.NoError(t, err)
	daemon, err := NewPump(daemonCarrier, DirectionClient, ceilings)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = broker.Close()
		_ = daemon.Close()
	})
	start := func() {
		broker.Start(ctx)
		daemon.Start(ctx)
	}
	return broker, daemon, start
}

func mustConnector(t *testing.T, pump *Pump) *LogicalConnector {
	t.Helper()
	connector, err := NewLogicalConnector(pump)
	require.NoError(t, err)
	return connector
}

func logicalTestPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: 1,
		EnvironmentPolicy:    protocol.EnvironmentPolicyClientOwned,
		Transport:            "mux",
		Trust:                "trust",
		Launch:               "launch",
		Isolation:            "isolation",
	}
}

// controlRequest builds one valid control-purpose open request for stream.
func controlRequest(stream int) ports.BrokerOpenStreamRequest {
	return ports.BrokerOpenStreamRequest{
		Epoch:      1,
		Purpose:    ports.BrokerStreamControl,
		Local:      true,
		Connection: ports.BrokerConnectionID{1},
		Stream:     ports.BrokerStreamID(stream),
		Policy:     logicalTestPolicy(),
		StartMode:  ports.BrokerDaemonStartIfNeeded,
	}
}

// awaitStream waits until a stream identity has been admitted on the daemon
// side.
func awaitStream(pump *Pump, id PhysicalStreamID, deadline time.Time) (StreamStatus, error) {
	for time.Now().Before(deadline) {
		if status, ok := pump.Engine().Status(id); ok && status.State != StreamTerminal {
			return status, nil
		}
		time.Sleep(time.Millisecond)
	}
	return StreamStatus{}, fmt.Errorf("daemonmux test: stream %d was not admitted", id)
}

// sessionPeer is a test-only daemon-side typed session: it frames the same mux
// byte stream with streamframe and wraps it with sessionwire's server
// connection, so the client's typed connection speaks to a real typed peer.
type sessionPeer struct {
	raw   *muxTransport
	srv   ports.ServerConnection
	flood int
}

func newSessionPeer(pump *Pump, ref StreamRef, flood int) *sessionPeer {
	status, _ := pump.Engine().Status(ref.Physical)
	pipe := newMuxStreamPipe(pump, ref.Physical, pump.Ceilings().StreamChunkLimit, status.Done)
	raw := newMuxTransport(pipe)
	peer := &sessionPeer{raw: raw, srv: sessionwire.NewServerConnection(raw), flood: flood}
	go peer.serve()
	return peer
}

// serve answers each client message with p.flood Pong frames, triggering the
// server preamble on the first call.
func (p *sessionPeer) serve() {
	for {
		message, err := p.srv.ReceiveClient()
		if err != nil {
			return
		}
		switch message.(type) {
		case protocol.Ping, protocol.Input, protocol.Resize, protocol.Detach:
			for i := 0; i < p.flood; i++ {
				if err := p.srv.SendServer(protocol.Pong{}); err != nil {
					return
				}
			}
		}
	}
}

func (p *sessionPeer) close() {
	if p.raw != nil {
		_ = p.raw.Close()
	}
}

// daemonAcceptor accepts mux opens on the daemon side: it confirms each one
// with Opened (or refuses it), and optionally wraps it as a typed session
// peer. It never uses testing.T from its goroutines.
type daemonAcceptor struct {
	pump  *Pump
	flood int

	mu    sync.Mutex
	count int
	peers map[PhysicalStreamID]*sessionPeer
	errCh chan error
}

func newDaemonAcceptor(pump *Pump, flood int) *daemonAcceptor {
	return &daemonAcceptor{pump: pump, flood: flood, peers: map[PhysicalStreamID]*sessionPeer{}, errCh: make(chan error, 256)}
}

func (a *daemonAcceptor) start(id PhysicalStreamID, deadline time.Time) {
	go func() {
		status, err := awaitStream(a.pump, id, deadline)
		if err != nil {
			a.fail(err)
			return
		}
		if err := a.pump.Engine().Opened(Opened{Ref: status.Ref}); err != nil {
			a.fail(err)
			return
		}
		if err := a.pump.Send(Opened{Ref: status.Ref}); err != nil {
			a.fail(err)
			return
		}
		a.add(id, newSessionPeer(a.pump, status.Ref, a.flood))
	}()
}

func (a *daemonAcceptor) startBare(id PhysicalStreamID, deadline time.Time) {
	go func() {
		status, err := awaitStream(a.pump, id, deadline)
		if err != nil {
			a.fail(err)
			return
		}
		if err := a.pump.Engine().Opened(Opened{Ref: status.Ref}); err != nil {
			a.fail(err)
			return
		}
		if err := a.pump.Send(Opened{Ref: status.Ref}); err != nil {
			a.fail(err)
			return
		}
		a.add(id, nil)
	}()
}

func (a *daemonAcceptor) startRefused(id PhysicalStreamID, deadline time.Time) {
	go func() {
		status, err := awaitStream(a.pump, id, deadline)
		if err != nil {
			a.fail(err)
			return
		}
		refusal := ErrorDetail{Code: ports.BrokerErrorUnavailable}
		if err := a.pump.Send(Refused{Ref: status.Ref, Error: refusal}); err != nil {
			a.fail(err)
			return
		}
		if err := a.pump.Engine().Refused(Refused{Ref: status.Ref, Error: refusal}); err != nil {
			a.fail(err)
			return
		}
		a.add(id, nil)
	}()
}

func (a *daemonAcceptor) add(id PhysicalStreamID, peer *sessionPeer) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.count++
	if peer != nil {
		a.peers[id] = peer
	}
}

func (a *daemonAcceptor) fail(err error) {
	select {
	case a.errCh <- err:
	default:
	}
}

func (a *daemonAcceptor) wait(count int, deadline time.Time) error {
	for time.Now().Before(deadline) {
		select {
		case err := <-a.errCh:
			return err
		default:
		}
		a.mu.Lock()
		accepted := a.count
		a.mu.Unlock()
		if accepted >= count {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	a.mu.Lock()
	accepted := a.count
	a.mu.Unlock()
	select {
	case err := <-a.errCh:
		return err
	default:
	}
	return fmt.Errorf("daemonmux test: accepted %d of %d streams", accepted, count)
}

func (a *daemonAcceptor) closePeers() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, peer := range a.peers {
		peer.close()
	}
}

func openWithDeadline(t *testing.T, connector *LogicalConnector, stream int) typedLogical {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connection, err := connector.Open(ctx, controlRequest(stream))
	require.NoError(t, err)
	return newTypedLogical(connection)
}

// watchRegistered reports whether the pump still holds an inbound watch entry
// for one physical stream.
func watchRegistered(pump *Pump, physical PhysicalStreamID) bool {
	pump.watchMu.Lock()
	defer pump.watchMu.Unlock()
	_, ok := pump.watch[physical]
	return ok
}

func TestNewLogicalConnectorValidation(t *testing.T) {
	t.Run("nil pump", func(t *testing.T) {
		connector, err := NewLogicalConnector(nil)
		require.ErrorIs(t, err, ErrLogicalConfig)
		require.Nil(t, connector)
	})
	t.Run("server-side pump is refused", func(t *testing.T) {
		pump, _ := newTestPump(t, DirectionClient)
		connector, err := NewLogicalConnector(pump)
		require.ErrorIs(t, err, ErrLogicalConfig)
		require.Nil(t, connector)
	})
	t.Run("broker-side pump is accepted", func(t *testing.T) {
		pump, _ := newTestPump(t, DirectionServer)
		connector, err := NewLogicalConnector(pump)
		require.NoError(t, err)
		require.NotNil(t, connector)
	})
}

// TestMuxStreamPipeChunkAndOrder proves the byte stream chunks outbound writes
// at the negotiated ceiling in order and drains inbound chunks in order.
func TestMuxStreamPipeChunkAndOrder(t *testing.T) {
	pump, carrier := newTestPump(t, DirectionClient)
	pump.Start(context.Background())
	require.NoError(t, pump.Engine().Open(openFor(1)))
	require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))

	pipe := newMuxStreamPipe(pump, 1, 4, mustStatus(t, pump.Engine(), 1).Done)
	written, err := pipe.Write([]byte("0123456789"))
	require.NoError(t, err)
	require.Equal(t, 10, written)

	var chunks [][]byte
	for i := 0; i < 3; i++ {
		message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		data, ok := message.(Data)
		require.True(t, ok)
		require.LessOrEqual(t, uint64(len(data.Data)), uint64(4))
		chunks = append(chunks, data.Data)
	}
	require.Equal(t, [][]byte{[]byte("0123"), []byte("4567"), []byte("89")}, chunks)

	deliverClientFrame(t, carrier, Data{Physical: 1, Data: []byte("abc")})
	deliverClientFrame(t, carrier, Data{Physical: 1, Data: []byte("de")})
	buffer := make([]byte, 3)
	_, err = io.ReadFull(pipe, buffer)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), buffer)
	buffer = make([]byte, 2)
	_, err = io.ReadFull(pipe, buffer)
	require.NoError(t, err)
	require.Equal(t, []byte("de"), buffer)
}

// TestLogicalTwoConnections opens two typed logical connections over one
// physical pair, exchanges a typed round trip on each, and closes them
// independently.
func TestLogicalTwoConnections(t *testing.T) {
	const count = 2
	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)
	deadline := time.Now().Add(30 * time.Second)

	acceptor := newDaemonAcceptor(daemonPump, 1)
	for i := 1; i <= count; i++ {
		acceptor.start(PhysicalStreamID(i), deadline)
	}

	connections := make([]typedLogical, 0, count)
	for i := 1; i <= count; i++ {
		connection := openWithDeadline(t, connector, i)
		require.NoError(t, connection.SendClient(protocol.Ping{}), "connection %d", i)
		message, err := connection.ReceiveServer()
		require.NoError(t, err, "connection %d", i)
		require.Equal(t, protocol.Pong{}, message, "connection %d", i)
		connections = append(connections, connection)
	}
	require.NoError(t, acceptor.wait(count, deadline))

	for i, connection := range connections {
		require.False(t, channelClosed(connection.Done()), "connection %d", i)
		require.NoError(t, connection.Close(), "connection %d", i)
		require.True(t, channelClosed(connection.Done()), "connection %d", i)
		require.NoError(t, connection.Err(), "connection %d", i)
	}
	acceptor.closePeers()
	require.False(t, channelClosed(brokerPump.Done()))
	require.False(t, channelClosed(daemonPump.Done()))
}

// TestLogicalHundredConnectionsConcurrentAdmission opens 100 typed logical
// connections concurrently over one physical pair, proves all 100 are live
// before any traffic, interleaves a typed round trip on every stream, and then
// drains and closes them. It fills the physical connection to its 128-stream
// ceiling, proves the 129th stream is refused with the typed admission limit,
// and proves a retry after a Close succeeds with a fresh monotonic identity.
func TestLogicalHundredConnectionsConcurrentAdmission(t *testing.T) {
	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)
	deadline := time.Now().Add(60 * time.Second)

	const count = 100
	const ceiling = int(MaxMuxStreams)
	require.Equal(t, 128, ceiling, "the default physical connection admits 128 streams")

	acceptor := newDaemonAcceptor(daemonPump, 1)
	// Every physical identity the connector can allocate, plus the retry after
	// the refused 129th attempt (which still consumes its monotonic identity).
	for i := 1; i <= ceiling; i++ {
		acceptor.start(PhysicalStreamID(i), deadline)
	}
	acceptor.start(PhysicalStreamID(ceiling+2), deadline)

	openConcurrently := func(requests []ports.BrokerOpenStreamRequest) ([]*LogicalConnection, []error) {
		connections := make([]*LogicalConnection, len(requests))
		errs := make([]error, len(requests))
		var wg sync.WaitGroup
		for i, request := range requests {
			wg.Add(1)
			go func(index int, request ports.BrokerOpenStreamRequest) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				connections[index], errs[index] = connector.Open(ctx, request)
			}(i, request)
		}
		wg.Wait()
		return connections, errs
	}

	requests := make([]ports.BrokerOpenStreamRequest, count)
	for i := range requests {
		requests[i] = controlRequest(i + 1)
	}
	connections, errs := openConcurrently(requests)
	for i, err := range errs {
		require.NoError(t, err, "connection %d", i)
		require.NotNil(t, connections[i], "connection %d", i)
	}
	require.Equal(t, count, brokerPump.Engine().Live(), "all connections are open before any traffic")
	require.NoError(t, acceptor.wait(count, deadline))

	// Interleave a typed round trip across every stream, then drain it.
	trafficErrs := make([]error, count)
	var traffic sync.WaitGroup
	for i := range connections {
		traffic.Add(1)
		go func(index int) {
			defer traffic.Done()
			typed := asTyped(connections[index])
			if err := typed.SendClient(protocol.Ping{}); err != nil {
				trafficErrs[index] = err
				return
			}
			message, err := typed.ReceiveServer()
			if err != nil {
				trafficErrs[index] = err
				return
			}
			if message != (protocol.Pong{}) {
				trafficErrs[index] = fmt.Errorf("connection %d: received %v", index, message)
			}
		}(i)
	}
	traffic.Wait()
	for i, err := range trafficErrs {
		require.NoError(t, err, "connection %d", i)
	}
	require.Equal(t, count, brokerPump.Engine().Live(), "traffic never settles a live stream")

	// Fill the physical connection to its concurrent-stream ceiling.
	fill := make([]ports.BrokerOpenStreamRequest, ceiling-count)
	for i := range fill {
		fill[i] = controlRequest(count + i + 1)
	}
	extra, fillErrs := openConcurrently(fill)
	for i, err := range fillErrs {
		require.NoError(t, err, "extra connection %d", i)
	}
	require.Equal(t, ceiling, brokerPump.Engine().Live())
	require.NoError(t, acceptor.wait(ceiling, deadline))

	// The 129th stream is refused by the admission ceiling before it reaches
	// the daemon.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	refused, err := connector.Open(ctx, controlRequest(1000))
	require.ErrorIs(t, err, ports.BrokerAdmissionLimit)
	require.Nil(t, refused)
	require.Equal(t, ceiling, brokerPump.Engine().Live(), "a refused admission frees nothing")

	// A Close frees one slot; the retry uses a fresh monotonic identity.
	require.NoError(t, connections[0].Close())
	require.True(t, channelClosed(connections[0].Done()))
	require.Equal(t, ceiling-1, brokerPump.Engine().Live())
	retry := openWithDeadline(t, connector, 1001)
	require.NoError(t, retry.SendClient(protocol.Ping{}))
	message, err := retry.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)
	require.Equal(t, ceiling, brokerPump.Engine().Live())
	require.NoError(t, acceptor.wait(ceiling+1, deadline))

	for i := 1; i < len(connections); i++ {
		require.False(t, channelClosed(connections[i].Done()), "connection %d", i)
		require.NoError(t, connections[i].Close(), "connection %d", i)
		require.True(t, channelClosed(connections[i].Done()), "connection %d", i)
		require.NoError(t, connections[i].Err(), "connection %d", i)
	}
	for i := range extra {
		require.NoError(t, extra[i].Close(), "extra connection %d", i)
	}
	require.NoError(t, retry.Close())
	require.Zero(t, brokerPump.Engine().Live(), "every stream is retired")
	acceptor.closePeers()
	require.False(t, channelClosed(brokerPump.Done()))
	require.False(t, channelClosed(daemonPump.Done()))
}

// TestLogicalBlockedReaderSiblingProgresses is the regression for a slow
// remote consumer killing its own attachment. The daemon floods far more
// frames than one stream window holds while the consumer is not reading: the
// daemon's writer must wait for credit instead of resetting the stream, the
// sibling keeps exchanging typed messages, and once the consumer reads again
// every flooded frame arrives in order on the still-open stream.
func TestLogicalBlockedReaderSiblingProgresses(t *testing.T) {
	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)
	deadline := time.Now().Add(15 * time.Second)

	// Each Pong is one tiny frame, so the flood is far more frames than the
	// eight queued frames that used to reset the stream, and more credit than
	// one window.
	const flood = 20000
	blockedAcceptor := newDaemonAcceptor(daemonPump, flood)
	blockedAcceptor.start(1, deadline)
	siblingAcceptor := newDaemonAcceptor(daemonPump, 1)
	siblingAcceptor.start(2, deadline)

	blocked := openWithDeadline(t, connector, 1)
	sibling := openWithDeadline(t, connector, 2)
	require.NoError(t, blockedAcceptor.wait(1, deadline))
	require.NoError(t, siblingAcceptor.wait(1, deadline))

	require.NoError(t, sibling.SendClient(protocol.Ping{}))
	message, err := sibling.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)

	// Trigger the flood, then do not read: the daemon fills the window and
	// its writer waits for credit.
	require.NoError(t, blocked.SendClient(protocol.Ping{}))
	require.Eventually(t, func() bool {
		status, ok := daemonPump.Engine().Status(1)
		return ok && status.SendCredit < chunkCredit(int(MaxMuxChunkBytes))
	}, 5*time.Second, time.Millisecond, "the daemon never exhausted its credit")
	require.False(t, channelClosed(blocked.Done()), "a slow consumer must not reset its stream")

	// The sibling still progresses and the physical connection is untouched.
	require.NoError(t, sibling.SendClient(protocol.Ping{}))
	message, err = sibling.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)
	require.False(t, channelClosed(brokerPump.Done()))
	require.Equal(t, StreamOpen, mustStatus(t, brokerPump.Engine(), 2).State)

	// The consumer catches up: every flooded frame arrives on the same stream.
	for i := 0; i < flood; i++ {
		message, err := blocked.ReceiveServer()
		require.NoError(t, err, "frame %d", i)
		require.Equal(t, protocol.Pong{}, message)
	}
	require.False(t, channelClosed(blocked.Done()))
	require.Equal(t, StreamOpen, mustStatus(t, brokerPump.Engine(), 1).State)
}

// TestLogicalMalformedInnerFrameResetsOnlyStream proves a malformed inner
// session frame resets exactly its own stream and leaves the physical
// connection and a sibling untouched.
func TestLogicalMalformedInnerFrameResetsOnlyStream(t *testing.T) {
	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)
	deadline := time.Now().Add(15 * time.Second)

	bareAcceptor := newDaemonAcceptor(daemonPump, 1)
	bareAcceptor.startBare(1, deadline)
	siblingAcceptor := newDaemonAcceptor(daemonPump, 1)
	siblingAcceptor.start(2, deadline)

	malformed := openWithDeadline(t, connector, 1)
	sibling := openWithDeadline(t, connector, 2)
	require.NoError(t, bareAcceptor.wait(1, deadline))
	require.NoError(t, siblingAcceptor.wait(1, deadline))

	require.NoError(t, sibling.SendClient(protocol.Ping{}))
	message, err := sibling.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)

	// A zero-length inner frame is malformed: the client's first read fails and
	// must reset only this stream.
	require.NoError(t, daemonPump.SendData(1, []byte{0, 0, 0, 0}, nil))
	_, err = malformed.ReceiveServer()
	require.Error(t, err)
	require.True(t, channelClosed(malformed.Done()))

	var loss ports.BrokerStreamLost
	require.ErrorAs(t, malformed.Err(), &loss)
	require.Equal(t, ports.BrokerStreamID(1), loss.Stream)
	require.Equal(t, ports.BrokerConnectionID{1}, loss.Connection)
	require.Equal(t, ports.BrokerEpoch(1), loss.Epoch)
	require.Equal(t, domain.RemoteFailureInvalidResponse, loss.Cause)

	// The daemon observed exactly a stream-local reset.
	require.Eventually(t, func() bool {
		status, ok := daemonPump.Engine().Status(1)
		return ok && status.State == StreamTerminal
	}, 5*time.Second, time.Millisecond)
	reset := mustStatus(t, daemonPump.Engine(), 1)
	require.Equal(t, domain.RemoteFailureInvalidResponse, reset.FailureKind)

	// The sibling still progresses and the physical pumps stay open.
	require.NoError(t, sibling.SendClient(protocol.Ping{}))
	message, err = sibling.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)
	require.False(t, channelClosed(brokerPump.Done()))
	require.False(t, channelClosed(daemonPump.Done()))
}

// TestLogicalOpenCancellationReleasesReservation proves a cancelled open
// releases its local reservation and a later open still succeeds.
func TestLogicalOpenCancellationReleasesReservation(t *testing.T) {
	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)
	deadline := time.Now().Add(15 * time.Second)

	// No daemon acceptor: the open waits for an Opened that never arrives.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	connection, err := connector.Open(ctx, controlRequest(1))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, connection)

	require.Eventually(t, func() bool {
		status, ok := brokerPump.Engine().Status(1)
		return ok && status.State == StreamTerminal
	}, 5*time.Second, time.Millisecond)
	require.False(t, watchRegistered(brokerPump, 1), "cancelled open leaked a watch entry")

	// A sibling stream on the same physical connection still opens and works.
	acceptor := newDaemonAcceptor(daemonPump, 1)
	acceptor.start(2, deadline)
	sibling := openWithDeadline(t, connector, 2)
	require.NoError(t, acceptor.wait(1, deadline))
	require.NoError(t, sibling.SendClient(protocol.Ping{}))
	message, err := sibling.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)
	require.False(t, channelClosed(brokerPump.Done()))
}

// TestLogicalOpenRefused proves a daemon refusal fails the open with the typed
// refusal and leaves the physical connection open.
func TestLogicalOpenRefused(t *testing.T) {
	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)
	deadline := time.Now().Add(15 * time.Second)

	acceptor := newDaemonAcceptor(daemonPump, 1)
	acceptor.startRefused(1, deadline)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connection, err := connector.Open(ctx, controlRequest(1))
	require.Error(t, err)
	require.Nil(t, connection)
	require.NoError(t, acceptor.wait(1, deadline))
	require.False(t, channelClosed(brokerPump.Done()))
}

// TestLogicalCloseIsolation proves closing one logical connection settles only
// itself and leaves a sibling and the physical connection untouched.
func TestLogicalCloseIsolation(t *testing.T) {
	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)
	deadline := time.Now().Add(15 * time.Second)

	firstAcceptor := newDaemonAcceptor(daemonPump, 1)
	firstAcceptor.start(1, deadline)
	secondAcceptor := newDaemonAcceptor(daemonPump, 1)
	secondAcceptor.start(2, deadline)

	first := openWithDeadline(t, connector, 1)
	second := openWithDeadline(t, connector, 2)
	require.NoError(t, firstAcceptor.wait(1, deadline))
	require.NoError(t, secondAcceptor.wait(1, deadline))

	require.NoError(t, first.Close())
	require.True(t, channelClosed(first.Done()))
	require.NoError(t, first.Err())
	require.Equal(t, domain.RemoteFailureNone, first.FailureKind())
	require.False(t, watchRegistered(brokerPump, 1), "closed logical connection leaked a watch entry")

	// The daemon observed the orderly Close for stream 1.
	require.Eventually(t, func() bool {
		status, ok := daemonPump.Engine().Status(1)
		return ok && status.State == StreamTerminal && status.Err == nil
	}, 5*time.Second, time.Millisecond)

	require.NoError(t, second.SendClient(protocol.Ping{}))
	message, err := second.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)
	require.False(t, channelClosed(brokerPump.Done()))
	require.False(t, channelClosed(daemonPump.Done()))
}

// TestLogicalCloseDiscardsQueuedInboundReleasesAccounting proves a local Close
// first discards the inbound chunks the engine already accepted for its stream:
// the engine's orderly Close reaches its terminal state, releasing both the
// stream's admission slot and its queued aggregate bytes, instead of parking
// the stream in closing with a queue this side will never read again. Every
// cycle closes with one queued inbound chunk under a two-stream ceiling, so a
// leaked slot would refuse the third admission, and the peer still observes
// exactly one orderly mux Close for each cycle.
func TestLogicalCloseDiscardsQueuedInboundReleasesAccounting(t *testing.T) {
	ceilings := DefaultMuxCeilings()
	ceilings.MaxStreams = 2
	pump, carrier := newTestPumpWithCeilings(t, DirectionServer, ceilings)
	pump.Start(context.Background())

	chunk := []byte("queued")
	const cycles = 3
	for i := 1; i <= cycles; i++ {
		id := PhysicalStreamID(i)
		require.NoError(t, pump.Engine().Open(openFor(id)), "cycle %d admission", i)
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(id)}))
		status := mustStatus(t, pump.Engine(), id)
		connection := newLogicalConnection(pump, status.Ref, status.Done, pump.cause(id))

		// The peer pipelined one chunk that this side never reads, so the stream
		// holds accepted inbound data when the local Close arrives.
		deliverServerFrame(t, carrier, Data{Physical: id, Data: chunk})
		requireEngineEventually(t, pump, func(e *StreamEngine) bool { return e.AggregateBytes() > 0 },
			"cycle %d: the accepted chunk was never queued", i)

		require.NoError(t, connection.Close())
		require.NoError(t, connection.Close(), "Close is idempotent")
		require.True(t, channelClosed(connection.Done()))
		require.NoError(t, connection.Err())

		closed := mustStatus(t, pump.Engine(), id)
		require.Equal(t, StreamTerminal, closed.State, "cycle %d: the closed stream must be terminal", i)
		require.Zero(t, closed.QueuedChunks, "cycle %d", i)
		require.Zero(t, pump.Engine().Live(), "cycle %d: a closing stream leaked its admission slot", i)
		require.Zero(t, pump.Engine().AggregateBytes(), "cycle %d: a closing stream leaked its queued bytes", i)
		require.False(t, watchRegistered(pump, id), "cycle %d: the closed connection leaked a watch entry", i)

		// Exactly one orderly mux Close reached the peer; the drain is local and
		// never replaces it with a Reset.
		message, err := DecodeClient(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		closedFrame, ok := message.(Close)
		require.True(t, ok, "cycle %d: the peer frame is not a Close", i)
		require.Equal(t, Close{Physical: id}, closedFrame)
		carrier.requireNoSent(t, 20*time.Millisecond)
	}
	require.False(t, pump.Engine().Closed())
	require.False(t, channelClosed(pump.Done()))
}

// TestLogicalPhysicalLossFanout proves a physical loss publishes one stable
// ports.BrokerStreamLost per affected logical stream with its exact scope.
func TestLogicalPhysicalLossFanout(t *testing.T) {
	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)
	deadline := time.Now().Add(15 * time.Second)

	const count = 3
	acceptor := newDaemonAcceptor(daemonPump, 1)
	for i := 1; i <= count; i++ {
		acceptor.start(PhysicalStreamID(i), deadline)
	}
	connections := make([]typedLogical, 0, count)
	for i := 1; i <= count; i++ {
		connection := openWithDeadline(t, connector, i)
		require.NoError(t, connection.SendClient(protocol.Ping{}))
		message, err := connection.ReceiveServer()
		require.NoError(t, err)
		require.Equal(t, protocol.Pong{}, message)
		connections = append(connections, connection)
	}
	require.NoError(t, acceptor.wait(count, deadline))

	// Failing the daemon-side carrier is a physical loss on the broker pump.
	require.NoError(t, daemonPump.Close())
	require.Eventually(t, func() bool { return channelClosed(brokerPump.Done()) }, 5*time.Second, time.Millisecond)

	for i, connection := range connections {
		require.Eventually(t, func() bool { return channelClosed(connection.Done()) }, 5*time.Second, time.Millisecond)
		var loss ports.BrokerStreamLost
		require.ErrorAs(t, connection.Err(), &loss, "connection %d", i)
		require.Equal(t, ports.BrokerStreamID(i+1), loss.Stream, "connection %d", i)
		require.Equal(t, ports.BrokerConnectionID{1}, loss.Connection, "connection %d", i)
		require.Equal(t, domain.RemoteFailureTransport, loss.Cause, "connection %d", i)
		var boxed ports.BrokerError
		require.ErrorAs(t, connection.Err(), &boxed, "connection %d", i)
		require.Equal(t, ports.BrokerErrorAttachmentLost, boxed.Code, "connection %d", i)
	}
	require.Error(t, brokerPump.Err())
	require.Equal(t, domain.RemoteFailureTransport, brokerPump.FailureKind())
}

// connections leaves no goroutine behind.
func TestLogicalNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)
	deadline := time.Now().Add(15 * time.Second)

	const count = 5
	acceptor := newDaemonAcceptor(daemonPump, 1)
	for i := 1; i <= count; i++ {
		acceptor.start(PhysicalStreamID(i), deadline)
	}
	connections := make([]typedLogical, 0, count)
	for i := 1; i <= count; i++ {
		connection := openWithDeadline(t, connector, i)
		require.NoError(t, connection.SendClient(protocol.Ping{}))
		message, err := connection.ReceiveServer()
		require.NoError(t, err)
		require.Equal(t, protocol.Pong{}, message)
		connections = append(connections, connection)
	}
	require.NoError(t, acceptor.wait(count, deadline))

	for _, connection := range connections {
		require.NoError(t, connection.Close())
	}
	acceptor.closePeers()
	require.NoError(t, brokerPump.Close())
	require.NoError(t, daemonPump.Close())

	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+3 }, 5*time.Second, time.Millisecond,
		"logical connection goroutines leaked")
}

// TestLogicalPeerCloseReleasesGoroutines proves a stream terminated by a peer
// mux Close joins its watcher and framed-transport goroutines without an
// explicit local Close.
func TestLogicalPeerCloseReleasesGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)
	deadline := time.Now().Add(15 * time.Second)

	acceptor := newDaemonAcceptor(daemonPump, 1)
	acceptor.start(1, deadline)
	connection := openWithDeadline(t, connector, 1)
	require.NoError(t, acceptor.wait(1, deadline))
	require.NoError(t, connection.SendClient(protocol.Ping{}))
	message, err := connection.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)

	// The daemon ends the stream orderly; the logical connection must settle
	// itself and join its goroutines without a local Close.
	if _, err := daemonPump.Engine().Close(Close{Physical: 1}); err != nil {
		t.Fatalf("daemon close: %v", err)
	}
	require.NoError(t, daemonPump.Send(Close{Physical: 1}))
	require.Eventually(t, func() bool { return channelClosed(connection.Done()) }, 5*time.Second, time.Millisecond)
	require.NoError(t, connection.Err())

	acceptor.closePeers()
	require.NoError(t, brokerPump.Close())
	require.NoError(t, daemonPump.Close())

	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= before+3 }, 5*time.Second, time.Millisecond,
		"peer-closed logical connection goroutines leaked")
}

// TestLogicalPhysicalLossBlockedReadReturnsLoss proves a read blocked on an
// idle logical stream wakes when the physical connection fails and reports a
// typed ports.BrokerStreamLost rather than a clean io.EOF. The pump publishes
// its terminal state before the engine terminalizes each stream, so the read
// must consult the pump's physical cause instead of assuming an orderly end.
func TestLogicalPhysicalLossBlockedReadReturnsLoss(t *testing.T) {
	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)
	deadline := time.Now().Add(15 * time.Second)

	acceptor := newDaemonAcceptor(daemonPump, 1)
	acceptor.start(1, deadline)
	connection := openWithDeadline(t, connector, 1)
	require.NoError(t, acceptor.wait(1, deadline))

	require.NoError(t, connection.SendClient(protocol.Ping{}))
	message, err := connection.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)

	type result struct {
		message protocol.ServerMessage
		err     error
	}
	blocked := make(chan result, 1)
	go func() {
		msg, err := connection.ReceiveServer()
		blocked <- result{message: msg, err: err}
	}()
	// Let the reader reach the inner stream and block there.
	time.Sleep(50 * time.Millisecond)
	require.False(t, channelClosed(connection.Done()), "the read is blocked, not settled")

	// Fail the daemon side; the broker observes a physical loss.
	require.NoError(t, daemonPump.Close())

	select {
	case got := <-blocked:
		require.Nil(t, got.message)
		require.Error(t, got.err)
		require.NotErrorIs(t, got.err, io.EOF, "a physical failure must not surface as a clean EOF")
		var loss ports.BrokerStreamLost
		require.ErrorAs(t, got.err, &loss)
		require.Equal(t, ports.BrokerStreamID(1), loss.Stream)
		require.Equal(t, domain.RemoteFailureTransport, loss.Cause)
	case <-time.After(5 * time.Second):
		t.Fatal("blocked read did not return after the physical failure")
	}
	require.True(t, channelClosed(connection.Done()))
}

// TestLogicalTerminalCauseSurvivesEngineEviction proves a settled logical
// connection reports its exact stream-local cause after the engine evicts the
// bounded record. The retained authority outlives the record, so a Reset can
// never degrade into an orderly close for a reader that observes the end late.
func TestLogicalTerminalCauseSurvivesEngineEviction(t *testing.T) {
	pump, _ := newTestPump(t, DirectionServer)
	require.NoError(t, pump.Engine().Open(openFor(1)))
	require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))
	status := mustStatus(t, pump.Engine(), 1)

	connection := &LogicalConnection{
		pump:     pump,
		ref:      status.Ref,
		terminal: newTerminalState(),
		cause:    pump.cause(status.Ref.Physical),
	}
	require.NotNil(t, connection.cause)

	// Retire stream 1 with a stream-local Reset, then retire more streams than
	// the engine retains so its record is evicted before any reader consults it.
	_, err := pump.Engine().Reset(Reset{
		Physical: 1,
		HasError: true,
		Error:    ErrorDetail{Code: ports.BrokerErrorUnavailable, Text: "reset", FailureKind: domain.RemoteFailureInvalidResponse},
	})
	require.NoError(t, err)
	for i := 2; i <= MaxRetiredStreamRecords+2; i++ {
		id := PhysicalStreamID(i)
		require.NoError(t, pump.Engine().Open(openFor(id)))
		_, _ = pump.Engine().Close(Close{Physical: id})
	}
	_, tracked := pump.Engine().Status(1)
	require.False(t, tracked, "stream 1 record was evicted")

	connection.syncFromEngine()

	require.True(t, channelClosed(connection.Done()))
	var loss ports.BrokerStreamLost
	require.ErrorAs(t, connection.Err(), &loss)
	require.Equal(t, status.Ref.Client, loss.Stream)
	require.Equal(t, domain.RemoteFailureInvalidResponse, loss.Cause)
	require.NotEqual(t, domain.RemoteFailureNone, connection.FailureKind(),
		"a Reset never degrades into an orderly close")
}

// TestLogicalWatcherCauseSurvivesEvictionStress retires a stream and then more
// streams than the engine retains before the watcher is scheduled, proving a
// late watcher still publishes the stream-local Reset as a loss instead of an
// orderly close.
func TestLogicalWatcherCauseSurvivesEvictionStress(t *testing.T) {
	const rounds = 12
	for round := 0; round < rounds; round++ {
		t.Run(fmt.Sprintf("round %d", round), func(t *testing.T) {
			brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
			connector := mustConnector(t, brokerPump)
			deadline := time.Now().Add(15 * time.Second)

			acceptor := newDaemonAcceptor(daemonPump, 1)
			acceptor.start(1, deadline)
			connection := openWithDeadline(t, connector, 1)
			require.NoError(t, acceptor.wait(1, deadline))

			// Retire the stream with a stream-local Reset, then immediately
			// retire more stream records than the engine retains so a watcher
			// scheduled late observes an evicted record.
			_, err := brokerPump.Engine().Reset(Reset{
				Physical: 1,
				HasError: true,
				Error:    ErrorDetail{Code: ports.BrokerErrorUnavailable, Text: "reset", FailureKind: domain.RemoteFailureInvalidResponse},
			})
			require.NoError(t, err)
			for i := 2; i <= MaxRetiredStreamRecords+2; i++ {
				id := PhysicalStreamID(i)
				require.NoError(t, brokerPump.Engine().Open(openFor(id)))
				_, _ = brokerPump.Engine().Close(Close{Physical: id})
			}
			_, tracked := brokerPump.Engine().Status(1)
			require.False(t, tracked, "stream 1 record was evicted")

			require.Eventually(t, func() bool { return channelClosed(connection.Done()) }, 5*time.Second, time.Millisecond)
			var loss ports.BrokerStreamLost
			require.ErrorAs(t, connection.Err(), &loss,
				"a late watcher must not degrade a Reset into an orderly close")
			require.Equal(t, ports.BrokerStreamID(1), loss.Stream)
			require.Equal(t, domain.RemoteFailureInvalidResponse, loss.Cause)
			require.Zero(t, brokerPump.Engine().Live())
			acceptor.closePeers()
		})
	}
}

// TestLogicalFreshOpenRefusedByDaemonCeiling proves the daemon's fresh-refusal
// path reaches the broker: the 129th physical identity is refused by the
// daemon's stream ceiling with one typed Mux Refused, and LogicalConnector
// classifies it as the admission limit without handing back a connection or
// recording a daemon-side stream.
func TestLogicalFreshOpenRefusedByDaemonCeiling(t *testing.T) {
	brokerPump, daemonPump := newPairedPumps(t, DefaultMuxCeilings())
	connector := mustConnector(t, brokerPump)

	// Fill the daemon's stream ceiling directly so the broker's own admission
	// stays free: the connector's next allocated identity is the 129th.
	for i := 1; i <= int(MaxMuxStreams); i++ {
		require.NoError(t, daemonPump.Engine().Open(openFor(PhysicalStreamID(i))))
	}
	require.Equal(t, int(MaxMuxStreams), daemonPump.Engine().Live())
	connector.next = uint64(MaxMuxStreams)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connection, err := connector.Open(ctx, controlRequest(1))
	require.ErrorIs(t, err, ports.BrokerAdmissionLimit)
	require.Nil(t, connection)

	refusedID := PhysicalStreamID(MaxMuxStreams + 1)
	require.Eventually(t, func() bool {
		status, ok := brokerPump.Engine().Status(refusedID)
		return ok && status.State == StreamTerminal
	}, 15*time.Second, time.Millisecond)
	status := mustStatus(t, brokerPump.Engine(), refusedID)
	require.True(t, status.Refused, "the daemon's fresh refusal reached the broker")
	require.Equal(t, uint32(1), status.Refusal.AdmissionCode)

	// The daemon recorded no stream for the refused identity, the broker
	// released its local reservation, and the physical connection stays open.
	_, tracked := daemonPump.Engine().Status(refusedID)
	require.False(t, tracked, "a refused fresh open records no daemon stream")
	require.Equal(t, int(MaxMuxStreams), daemonPump.Engine().Live())
	require.Zero(t, brokerPump.Engine().Live(), "the broker released its local reservation")
	require.False(t, channelClosed(brokerPump.Done()))
	require.False(t, channelClosed(daemonPump.Done()))
}
