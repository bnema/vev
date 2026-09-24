// Integration coverage for the real SSH stdio daemonmux carriage.
//
// These tests exercise the daemonmux multiplexer over an actual SSH stdio
// carriage: a real owned subprocess whose stdio is the physical carriage,
// dialed by sshstdio.DialMuxContext and served by the helper half,
// sshstdio.NewStdioTransport, not over the in-memory carrier used by the unit
// tests or the socket/QUIC carriages. One subprocess is one physical daemonmux
// connection: every logical stream rides its stdio, and no logical attachment
// is ever mapped to a second process.
//
// The broker half (EndpointConnector, PhysicalConnection, LogicalConnector)
// runs in the test process exactly as it would in the client. The daemon half
// runs in a re-executed copy of this test binary (the helper process), so the
// process start, pipe ownership, reaping, and teardown are real; no production
// composition is started. For the raw pump-accounting checks the daemon-side
// pump runs in the test process over the subprocess stdio relay so the stream
// accounting can be asserted directly, exactly as the Unix and QUIC carriage
// tests do.
package daemonmux

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// sshMuxCarriageDeadline bounds every real subprocess carriage wait.
const sshMuxCarriageDeadline = 10 * time.Second

// sshMuxHelperMarker distinguishes the re-executed helper process from an
// ordinary test run. It travels as a positional argument, so a normal test run
// (no positional args) skips every helper entry point.
const sshMuxHelperMarker = "vev-daemonmux-ssh-carriage-helper"

// TestSSHMuxHelperProcess is the daemon (or relay) half of one SSH stdio
// carriage. It is inert unless re-executed by a carriage test with the helper
// marker, at which point its stdio is the physical carriage and it must never
// write anything but framed envelopes to stdout.
func TestSSHMuxHelperProcess(t *testing.T) {
	args := flag.Args()
	if len(args) < 2 || args[0] != sshMuxHelperMarker {
		t.Skip("ssh stdio mux helper process")
	}
	switch mode := args[1]; mode {
	case "daemon":
		runSSHMuxDaemonHelper(t, sshMuxHelperArg(args, 2))
	case "relay":
		runSSHMuxRelayHelper(sshMuxHelperArg(args, 2))
	case "silent":
		runSSHMuxSilentHelper()
	default:
		fmt.Fprintf(os.Stderr, "daemonmux ssh mux helper: unknown mode %q\n", mode)
		os.Exit(2)
	}
}

func sshMuxHelperArg(args []string, index int) string {
	if index < len(args) {
		return args[index]
	}
	return ""
}

// runSSHMuxDaemonHelper serves one daemonmux physical connection over this
// process' own stdin/stdout using the helper-side stdio carriage constructor
// and the real ServerSupervisor/AggregateListener/typed Listener path. It stops
// when the carriage ends and never lets the test framework's report reach the
// carriage's stdout, which would corrupt the framing stream.
func runSSHMuxDaemonHelper(t *testing.T, pidPath string) {
	if pidPath != "" {
		if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			sshMuxHelperFatal(err)
		}
	}
	raw := sshstdio.NewStdioTransport()
	binding := mustServerBinding(t)
	aggregate := NewAggregateListener()
	supervisor, err := NewServerSupervisor(aggregate, binding, DefaultMuxCeilings(), 0)
	sshMuxHelperFatal(err)
	sshMuxHelperFatal(supervisor.Adopt(context.Background(), raw))
	go func() {
		for {
			conn, err := aggregate.Accept()
			if err != nil {
				return
			}
			go func() { _ = servePongs(conn) }()
		}
	}()
	if pump := sshMuxHelperPump(supervisor); pump != nil {
		<-pump.Done()
	}
	_ = supervisor.Close()
	_ = aggregate.Close()
	sshMuxSilenceStdout()
}

// sshMuxHelperPump returns the supervisor's one owned child pump, so the helper
// exits exactly when the physical carriage reaches its terminal outcome.
func sshMuxHelperPump(s *ServerSupervisor) *Pump {
	s.mu.Lock()
	defer s.mu.Unlock()
	for child := range s.children {
		return child.pump
	}
	return nil
}

// runSSHMuxRelayHelper relays this process' stdio and one parent-bound Unix
// socket transparently. It lets the raw pump tests keep both mux ends in the
// test process while every byte still crosses a real subprocess pipe.
func runSSHMuxRelayHelper(sockPath string) {
	if sockPath == "" {
		sshMuxHelperFatal(errors.New("relay: missing socket path"))
	}
	conn, err := net.Dial("unix", sockPath)
	sshMuxHelperFatal(err)
	go func() {
		_, _ = io.Copy(conn, os.Stdin)
		_ = conn.Close()
	}()
	_, _ = io.Copy(os.Stdout, conn)
	_ = conn.Close()
	sshMuxSilenceStdout()
}

// runSSHMuxSilentHelper reads its stdin to EOF without ever answering, so a
// setup deadline is the only way a caller can give up. It exits cleanly once the
// caller closes the carriage.
func runSSHMuxSilentHelper() {
	_, _ = io.Copy(io.Discard, os.Stdin)
	sshMuxSilenceStdout()
}

// sshMuxHelperFatal reports a helper failure on stderr (the bounded diagnostic
// channel) and exits non-cleanly.
func sshMuxHelperFatal(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, "daemonmux ssh mux helper:", err)
	os.Exit(2)
}

// sshMuxSilenceStdout points the test framework's final report at /dev/null so
// a helper that returns normally never writes its report into the carriage.
func sshMuxSilenceStdout() {
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err == nil {
		os.Stdout = devnull
	}
}

// sshMuxHelperCommand builds an explicit command specification that re-executes
// this test binary in one helper mode. The command is never the session
// `_broker-mux-stdio` command: the mux carriage takes its whole command from the caller.
func sshMuxHelperCommand(mode string, extra ...string) sshstdio.CommandSpec {
	args := append([]string{"-test.run=^TestSSHMuxHelperProcess$", sshMuxHelperMarker, mode}, extra...)
	return sshstdio.CommandSpec{Path: os.Args[0], Args: args}
}

// sshMuxDialer returns a RawCarrierDialer over one explicit command
// specification and a counter of how many physical carriages it opened.
func sshMuxDialer(spec sshstdio.CommandSpec) (RawCarrierDialer, *atomic.Int32) {
	dials := &atomic.Int32{}
	return func(ctx context.Context, _ ports.BrokerDialTarget) (RawFramedTransport, error) {
		dials.Add(1)
		return sshstdio.DialMuxContext(ctx, spec, nil)
	}, dials
}

// sshMuxConnect dials one helper carriage and returns the live broker physical
// connection, registering its teardown.
func sshMuxConnect(t *testing.T, binding ServerBinding, spec sshstdio.CommandSpec) ports.BrokerPhysicalConnection {
	t.Helper()
	dial, _ := sshMuxDialer(spec)
	connector, err := NewEndpointConnector(dial, DefaultMuxCeilings())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), sshMuxCarriageDeadline)
	defer cancel()
	physical, err := connector.Connect(ctx, muxEndpoint(binding.Identity(), binding.Policy(), "ssh://helper"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = physical.Close() })
	return physical
}

// sshMuxReadPID waits for the helper to publish its process id.
func sshMuxReadPID(t *testing.T, path string) int {
	t.Helper()
	pid := 0
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(path)
		if err != nil {
			return false
		}
		value, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			return false
		}
		pid = value
		return true
	}, sshMuxCarriageDeadline, 2*time.Millisecond)
	return pid
}

// requireNoGoroutineLeak waits for the test process to return to its baseline
// goroutine count once every carriage was torn down.
func requireNoGoroutineLeak(t *testing.T, baseline int) {
	t.Helper()
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= baseline+4 }, 5*time.Second, 20*time.Millisecond,
		"goroutines leaked: baseline %d, now %d", baseline, runtime.NumGoroutine())
}

// TestSSHCarriageSupervisorTwoThenHundredTypedAdmissions drives the full daemon
// path over one real SSH stdio physical connection: EndpointConnector dials a
// real subprocess started by DialMuxContext, the helper process adopts it with
// ServerSupervisor/AggregateListener, and the typed Listener delivers
// independent daemon admissions. Only one physical connection and one process
// exist for all 102 streams.
func TestSSHCarriageSupervisorTwoThenHundredTypedAdmissions(t *testing.T) {
	baseline := runtime.NumGoroutine()
	binding := mustServerBinding(t)
	policy := binding.Policy()
	pidPath := filepath.Join(t.TempDir(), "helper.pid")
	spec := sshMuxHelperCommand("daemon", pidPath)

	dial, dials := sshMuxDialer(spec)
	connector, err := NewEndpointConnector(dial, DefaultMuxCeilings())
	require.NoError(t, err)
	physical, err := connector.Connect(context.Background(), muxEndpoint(binding.Identity(), policy, "ssh://helper"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = physical.Close() })
	pid := sshMuxReadPID(t, pidPath)

	// Two independent typed admissions over the one physical carriage.
	first, err := openTyped(physical, context.Background(), muxOpenRequest(1, policy))
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	second, err := openTyped(physical, context.Background(), muxOpenRequest(2, policy))
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })
	for _, stream := range []ports.BrokerLogicalConnection{first, second} {
		require.NoError(t, stream.SendClient(protocol.Ping{}))
		message, err := stream.ReceiveServer()
		require.NoError(t, err)
		require.Equal(t, protocol.Pong{}, message)
	}

	// One hundred more independently admitted typed streams, opened
	// concurrently over the same physical carriage.
	const extra = 100
	connections := make([]ports.BrokerLogicalConnection, extra)
	openErrs := make([]error, extra)
	var wg sync.WaitGroup
	for i := 0; i < extra; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			connections[i], openErrs[i] = openTyped(physical, context.Background(), muxOpenRequest(uint64(3+i), policy))
		}(i)
	}
	wg.Wait()
	for i, openErr := range openErrs {
		require.NoError(t, openErr, "stream %d", i+3)
	}
	for i, connection := range connections {
		require.NotNil(t, connection, "stream %d", i+3)
		t.Cleanup(func() { _ = connection.Close() })
	}
	for i, connection := range connections {
		require.NoError(t, connection.SendClient(protocol.Ping{}), "stream %d", i+3)
		message, err := connection.ReceiveServer()
		require.NoError(t, err, "stream %d", i+3)
		require.Equal(t, protocol.Pong{}, message)
	}

	// All 102 streams share exactly one physical connection: exactly one dial,
	// one subprocess, and one live broker carriage.
	require.Equal(t, int32(1), dials.Load(), "all streams must share one ssh carriage")
	require.False(t, channelClosed(physical.Done()), "the physical carriage must stay open while streams are live")

	all := make([]ports.BrokerLogicalConnection, 0, 2+extra)
	all = append(all, first, second)
	all = append(all, connections...)
	for _, connection := range all {
		require.NoError(t, connection.Close())
	}
	require.False(t, channelClosed(physical.Done()), "closing every stream must leave the physical carriage open")

	// Teardown terminates and reaps the one owned subprocess, and no worker
	// outlives the carriage.
	require.NoError(t, physical.Close())
	require.Eventually(t, func() bool { return sshMuxProcessGone(pid) }, sshMuxCarriageDeadline, 5*time.Millisecond,
		"the ssh carriage subprocess was never reaped")
	requireNoGoroutineLeak(t, baseline)
}

// TestSSHCarriagePhysicalFailureFansOutAndIsIsolated proves a lost SSH stdio
// physical carriage publishes its terminal outcome before fanning out to every
// logical stream it carried, while a sibling physical carriage on the same
// supervisor is untouched and a fresh stream on the lost physical is refused.
func TestSSHCarriagePhysicalFailureFansOutAndIsIsolated(t *testing.T) {
	binding := mustServerBinding(t)
	policy := binding.Policy()
	pidA := filepath.Join(t.TempDir(), "a.pid")
	pidB := filepath.Join(t.TempDir(), "b.pid")

	physicalA := sshMuxConnect(t, binding, sshMuxHelperCommand("daemon", pidA))
	physicalB := sshMuxConnect(t, binding, sshMuxHelperCommand("daemon", pidB))
	lostPID := sshMuxReadPID(t, pidA)
	_ = sshMuxReadPID(t, pidB)

	lost := make([]ports.BrokerLogicalConnection, 3)
	for i := range lost {
		connection, err := openTyped(physicalA, context.Background(), muxOpenRequest(uint64(i+1), policy))
		require.NoError(t, err, "stream %d", i+1)
		lost[i] = connection
		t.Cleanup(func() { _ = connection.Close() })
	}
	survivor, err := openTyped(physicalB, context.Background(), muxOpenRequest(1, policy))
	require.NoError(t, err)
	t.Cleanup(func() { _ = survivor.Close() })

	// The physical terminal outcome is ordered before its logical fan-out.
	ordered := make(chan bool, 1)
	go func() {
		select {
		case <-lost[0].Done():
		case <-time.After(sshMuxCarriageDeadline):
			ordered <- false
			return
		}
		select {
		case <-physicalA.Done():
			ordered <- true
		case <-time.After(sshMuxCarriageDeadline):
			ordered <- false
		}
	}()

	// Lose the ssh carriage entirely: the subprocess dies, its stdout closes,
	// and the broker observes the pipe failure.
	process, err := os.FindProcess(lostPID)
	require.NoError(t, err)
	require.NoError(t, process.Kill())

	require.True(t, <-ordered, "the logical Done fired before the physical Done")
	require.True(t, channelClosed(physicalA.Done()))
	require.Error(t, physicalA.Err())
	require.NotEqual(t, domain.RemoteFailureNone, physicalA.FailureKind())
	for i, stream := range lost {
		require.Eventually(t, func() bool {
			return channelClosed(stream.Done()) && stream.Err() != nil
		}, sshMuxCarriageDeadline, time.Millisecond, "lost stream %d never received the physical failure", i+1)
	}

	// The sibling physical carriage is unaffected.
	require.False(t, channelClosed(physicalB.Done()))
	require.False(t, channelClosed(survivor.Done()))
	require.NoError(t, survivor.SendClient(protocol.Ping{}))
	message, err := survivor.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)

	// The lost physical refuses a fresh stream.
	_, err = openTyped(physicalA, context.Background(), muxOpenRequest(9, policy))
	require.Error(t, err)

	// The failed subprocess is reaped, not left a zombie.
	require.Eventually(t, func() bool { return sshMuxProcessGone(lostPID) }, sshMuxCarriageDeadline, 5*time.Millisecond,
		"the failed ssh carriage subprocess was never reaped")
}

// TestSSHCarriageSetupContextDetachedFromPhysicalLifetime proves the setup
// context covers only setup: once EndpointConnector.Connect returned over a real
// ssh carriage, cancelling the setup context never stops the pooled physical
// connection, which Close alone owns.
func TestSSHCarriageSetupContextDetachedFromPhysicalLifetime(t *testing.T) {
	binding := mustServerBinding(t)
	spec := sshMuxHelperCommand("daemon", filepath.Join(t.TempDir(), "helper.pid"))
	dial, _ := sshMuxDialer(spec)
	connector, err := NewEndpointConnector(dial, DefaultMuxCeilings())
	require.NoError(t, err)

	setupCtx, cancel := context.WithCancel(context.Background())
	physical, err := connector.Connect(setupCtx, muxEndpoint(binding.Identity(), binding.Policy(), "ssh://helper"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = physical.Close() })
	cancel()

	require.False(t, channelClosed(physical.Done()))
	logical, err := openTyped(physical, context.Background(), muxOpenRequest(1, binding.Policy()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = logical.Close() })
	require.NoError(t, logical.SendClient(protocol.Ping{}))
	message, err := logical.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)
	require.False(t, channelClosed(physical.Done()))
}

// TestSSHCarriageCanceledSetupAdmitsNothing proves one canceled setup context
// prevents the subprocess from starting at all: no process, no physical
// connection, no stream.
func TestSSHCarriageCanceledSetupAdmitsNothing(t *testing.T) {
	binding := mustServerBinding(t)
	pidPath := filepath.Join(t.TempDir(), "helper.pid")
	spec := sshMuxHelperCommand("daemon", pidPath)
	dial, _ := sshMuxDialer(spec)
	connector, err := NewEndpointConnector(dial, DefaultMuxCeilings())
	require.NoError(t, err)

	setupCtx, cancel := context.WithCancel(context.Background())
	cancel()
	physical, err := connector.Connect(setupCtx, muxEndpoint(binding.Identity(), binding.Policy(), "ssh://helper"))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, physical)

	_, statErr := os.Stat(pidPath)
	require.ErrorIs(t, statErr, os.ErrNotExist, "a canceled setup must not start the ssh subprocess")
}

// TestSSHCarriageSetupDeadlineBoundsTheMuxHandshake proves the setup deadline
// covers the daemonmux physical handshake over a real ssh carriage: a helper
// that never answers the preamble fails the whole Connect within the setup
// deadline and publishes no physical connection.
func TestSSHCarriageSetupDeadlineBoundsTheMuxHandshake(t *testing.T) {
	binding := mustServerBinding(t)
	spec := sshMuxHelperCommand("silent")
	dial, _ := sshMuxDialer(spec)
	connector, err := NewEndpointConnector(dial, DefaultMuxCeilings())
	require.NoError(t, err)

	setupCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	physical, err := connector.Connect(setupCtx, muxEndpoint(binding.Identity(), binding.Policy(), "ssh://helper"))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, physical)
}

// TestSSHCarriageConnectErrorBoundedAndSanitized proves a failed ssh carriage
// surfaces a bounded, control-byte-free public error that never carries raw
// remote stderr.
func TestSSHCarriageConnectErrorBoundedAndSanitized(t *testing.T) {
	const marker = "remote-secret-value"
	binding := mustServerBinding(t)
	script := "sleep 0.2; printf '\\033[31m" + marker + "\\033[0m' >&2; printf 'B%.0s' $(seq 1 20000) >&2; exit 3"
	spec := sshstdio.CommandSpec{Path: "sh", Args: []string{"-c", script}}
	dial, _ := sshMuxDialer(spec)
	connector, err := NewEndpointConnector(dial, DefaultMuxCeilings())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), sshMuxCarriageDeadline)
	defer cancel()
	physical, err := connector.Connect(ctx, muxEndpoint(binding.Identity(), binding.Policy(), "ssh://helper"))
	require.Error(t, err)
	require.Nil(t, physical)

	cause := errors.Unwrap(err)
	if cause == nil {
		cause = err
	}
	require.NotContains(t, cause.Error(), marker)
	require.NotContains(t, cause.Error(), "\x1b")
	require.Less(t, len(cause.Error()), 4096)
}

// sshRawCarriagePair establishes one real SSH stdio carriage whose subprocess is
// a transparent relay to an in-process Unix socket, and returns its two raw
// ends: the ssh client end (the subprocess stdio DialMuxContext owns) and the
// in-process relay end. Every byte crosses the real subprocess pipes.
func sshRawCarriagePair(t *testing.T) (RawFramedTransport, RawFramedTransport) {
	t.Helper()
	sockPath := filepath.Join(filepath.Dir(muxCarriagePath(t)), "relay.sock")
	listener, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()

	client, err := sshstdio.DialMuxContext(context.Background(), sshMuxHelperCommand("relay", sockPath), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	var conn net.Conn
	select {
	case conn = <-accepted:
	case err := <-acceptErr:
		t.Fatalf("relay accept: %v", err)
	case <-time.After(sshMuxCarriageDeadline):
		t.Fatal("relay subprocess never connected")
	}
	t.Cleanup(func() { _ = conn.Close() })
	daemon := sshstdio.NewTransport(conn, conn, conn.Close).(wire.BoundedTransport)
	return client, daemon
}

// sshDaemonPump builds a daemon-side pump over one end of a real SSH stdio
// carriage and returns the raw ssh client end for direct frame injection, so a
// test can drive deterministic inbound traffic over the actual subprocess
// pipes.
func sshDaemonPump(t *testing.T) (*Pump, RawFramedTransport) {
	t.Helper()
	client, daemon := sshRawCarriagePair(t)
	carrier, err := NewPreambleCarrier(daemon)
	require.NoError(t, err)
	require.NoError(t, carrier.Negotiate(DefaultMuxCeilings()))
	pump, err := NewPump(carrier, DirectionClient, DefaultMuxCeilings())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pump.Close() })
	pump.Start(context.Background())
	return pump, client
}

// injectSSHClientFrame writes one client frame over the real ssh carriage.
func injectSSHClientFrame(t *testing.T, client RawFramedTransport, message ClientMessage) {
	t.Helper()
	require.NoError(t, client.Send(wire.Envelope{Payload: mustEncodeClient(t, message)}))
}

// nextSSHServerFrame reads one daemon frame from the real ssh carriage.
func nextSSHServerFrame(t *testing.T, client RawFramedTransport) []byte {
	t.Helper()
	envelope, err := client.RecvBounded(testEnvelopeCeiling)
	require.NoError(t, err)
	return envelope.Payload
}

// TestSSHCarriageBlockedConsumerSiblingProgress proves, over a real ssh
// carriage, that a logical consumer which never drains one stream's inbound
// queue neither blocks the reader nor a sibling: the stalled stream is reset by
// its own bound while the sibling keeps receiving.
func TestSSHCarriageBlockedConsumerSiblingProgress(t *testing.T) {
	pump, client := sshDaemonPump(t)

	const siblings = 2
	for i := 1; i <= siblings; i++ {
		injectSSHClientFrame(t, client, openFor(PhysicalStreamID(i)))
	}
	requireEngineEventually(t, pump, func(e *StreamEngine) bool { return e.Live() == siblings })
	for i := 1; i <= siblings; i++ {
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(PhysicalStreamID(i))}))
	}

	// The consumer never Takes stream 1: its bounded queue fills.
	for i := 0; i < MaxMuxStreamQueueChunks; i++ {
		injectSSHClientFrame(t, client, Data{Physical: 1, Data: []byte{byte(i)}})
	}
	requireEngineEventually(t, pump, func(e *StreamEngine) bool {
		status, ok := e.Status(1)
		return ok && status.QueuedChunks == MaxMuxStreamQueueChunks
	})

	// One more chunk overflows stream 1's own bound; a sibling chunk still
	// lands, so neither the reader nor the sibling was blocked.
	injectSSHClientFrame(t, client, Data{Physical: 1, Data: []byte{'x'}})
	injectSSHClientFrame(t, client, Data{Physical: 2, Data: []byte("sibling")})

	taken, ok := takeEventually(t, pump, 2)
	require.True(t, ok)
	require.Equal(t, []byte("sibling"), taken)

	stalled := mustStatus(t, pump.Engine(), 1)
	require.Equal(t, StreamTerminal, stalled.State)
	require.ErrorIs(t, stalled.Err, ErrStreamQueueFull)
	require.Equal(t, domain.RemoteFailureTransport, stalled.FailureKind)
	require.Equal(t, StreamOpen, mustStatus(t, pump.Engine(), 2).State)
	require.False(t, pump.Engine().Closed())
	require.False(t, channelClosed(pump.Done()))

	reset := decodeReset(t, nextSSHServerFrame(t, client), DirectionServer)
	require.Equal(t, PhysicalStreamID(1), reset.Physical)
	require.Equal(t, domain.RemoteFailureTransport, reset.Error.FailureKind)
}

// TestSSHCarriageResetIsolation proves, over a real ssh carriage, that one
// stream-local refusal schedules exactly one Reset for its own stream and
// leaves the physical connection and every sibling healthy.
func TestSSHCarriageResetIsolation(t *testing.T) {
	pump, client := sshDaemonPump(t)

	for _, id := range []PhysicalStreamID{1, 2, 3} {
		injectSSHClientFrame(t, client, openFor(id))
	}
	requireEngineEventually(t, pump, func(e *StreamEngine) bool { return e.Live() == 3 })
	for _, id := range []PhysicalStreamID{2, 3} {
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(id)}))
	}

	// Stream 1 is still opening: inbound data is a stream-local state refusal.
	injectSSHClientFrame(t, client, Data{Physical: 1, Data: []byte("early")})
	requireEngineEventually(t, pump, func(e *StreamEngine) bool {
		status, ok := e.Status(1)
		return ok && status.State == StreamTerminal
	})
	settled := mustStatus(t, pump.Engine(), 1)
	require.ErrorContains(t, settled.Err, "invalid stream state")
	require.Equal(t, domain.RemoteFailureInvalidResponse, settled.FailureKind)

	injectSSHClientFrame(t, client, Data{Physical: 2, Data: []byte("sibling")})
	taken, ok := takeEventually(t, pump, 2)
	require.True(t, ok)
	require.Equal(t, []byte("sibling"), taken)

	reset := decodeReset(t, nextSSHServerFrame(t, client), DirectionServer)
	require.Equal(t, PhysicalStreamID(1), reset.Physical)
	require.True(t, reset.HasError)
	require.Equal(t, domain.RemoteFailureInvalidResponse, reset.Error.FailureKind)
	require.False(t, pump.Engine().Closed())
	require.False(t, channelClosed(pump.Done()))
}
