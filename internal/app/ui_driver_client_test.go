//go:build linux

package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/uidriver"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/require"
)

// UI-driver JSONL composition tests (Plan 001 P7 UI-driver slice 2).
//
// The driver service is one shared composition: runUIDriverClient launches the
// shared broker client exactly once, serves the JSONL protocol over one stream,
// and joins both workers on EOF or cancellation. These tests drive it directly,
// so every assertion holds for the production `--ui-driver` entry point and for
// the sandbox harness that delegates to the same function.

// failingBrokerConnector is a connector whose every attempt fails, so a test can
// drive the non-fatal broker path without any transport.
type failingBrokerConnector struct{ err error }

func (c failingBrokerConnector) Connect(context.Context) (ports.BrokerService, error) {
	return nil, c.err
}

// uidriverTestStream is an owned bidirectional JSONL stream over two private
// pipes: the driver reads requests from the request pipe and writes responses to
// the response pipe, exactly as a socket would. Closing it is the driver's EOF,
// and it owns its work exactly once so a duplicate EOF can never end another run.
// Request IDs are allocated here, so every caller shares one monotonic sequence.
//
// One decoder goroutine owns the single read lifetime of the response pipe and
// feeds whole envelopes into a channel, so a slow assertion can never make a
// decoder buffer and silently drop the next response's bytes.
type uidriverTestStream struct {
	requests   *io.PipeReader
	requestsW  *io.PipeWriter
	responses  *io.PipeReader
	responsesW *io.PipeWriter

	decoded   sync.Once
	results   chan driverTestEnvelope
	closeOnce sync.Once
	closeErr  error

	idMu chan struct{}
	next uint64
}

// driverTestEnvelope is one JSON Lines response envelope, exactly as the driver
// service writes it.
type driverTestEnvelope struct {
	Version int             `json:"version"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code     ports.UIErrorCode `json:"code"`
		Accepted bool              `json:"accepted"`
		ActionID uint64            `json:"action_id,omitempty"`
	} `json:"error,omitempty"`
}

func newUIDriverTestStream() *uidriverTestStream {
	requests, requestsW := io.Pipe()
	responses, responsesW := io.Pipe()
	stream := &uidriverTestStream{
		requests: requests, requestsW: requestsW, responses: responses, responsesW: responsesW,
		results: make(chan driverTestEnvelope, 8), idMu: make(chan struct{}, 1),
	}
	stream.idMu <- struct{}{}
	return stream
}

func (s *uidriverTestStream) Read(data []byte) (int, error)  { return s.requests.Read(data) }
func (s *uidriverTestStream) Write(data []byte) (int, error) { return s.responsesW.Write(data) }
func (s *uidriverTestStream) Close() error {
	s.closeOnce.Do(func() {
		closeErr := s.requestsW.Close()
		if err := s.responsesW.Close(); closeErr == nil {
			closeErr = err
		}
		s.closeErr = closeErr
	})
	return s.closeErr
}

// awaitReady reads the single discovery response of one driver stream.
func (s *uidriverTestStream) awaitReady(t *testing.T) uidriver.Ready {
	t.Helper()
	envelope := s.awaitEnvelope(t, 0)
	var ready uidriver.Ready
	require.NoError(t, remarshal(envelope.Result, &ready))
	return ready
}

// awaitCapture sends one capture request and returns its result.
func (s *uidriverTestStream) awaitCapture(t *testing.T, attachment string) map[string]any {
	t.Helper()
	id := s.nextID()
	require.NoError(t, s.writeRequest(map[string]any{
		"version": 1, "id": id, "op": "capture", "attachment": attachment,
	}))
	envelope := s.awaitEnvelope(t, id)
	require.Nil(t, envelope.Error)
	var capture map[string]any
	require.NoError(t, remarshal(envelope.Result, &capture))
	return capture
}

// nextID allocates the next monotonically increasing JSONL request id of this
// stream.
func (s *uidriverTestStream) nextID() uint64 {
	<-s.idMu
	s.next++
	id := s.next
	s.idMu <- struct{}{}
	return id
}

func (s *uidriverTestStream) writeRequest(request map[string]any) error {
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}
	_, err = s.requestsW.Write(append(data, '\n'))
	return err
}

// awaitEnvelope returns the next response envelope, bounded so a driver that
// stops answering fails the test instead of blocking it.
func (s *uidriverTestStream) awaitEnvelope(t *testing.T, wantID uint64) driverTestEnvelope {
	t.Helper()
	s.decoded.Do(func() {
		go func() {
			defer close(s.results)
			decoder := json.NewDecoder(s.responses)
			for {
				var envelope driverTestEnvelope
				if err := decoder.Decode(&envelope); err != nil {
					return
				}
				s.results <- envelope
			}
		}()
	})
	select {
	case envelope, ok := <-s.results:
		require.True(t, ok, "the driver stream ended before response %d", wantID)
		require.Equal(t, wantID, envelope.ID)
		return envelope
	case <-time.After(brokerTestWait):
		t.Fatal("the driver stream produced no response")
		return driverTestEnvelope{}
	}
}

func remarshal(value any, target any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// TestProductionBrokerConnectorIsLazy pins that constructing the production
// connector performs no I/O at all: the connect-or-spawn step runs inside
// Connect, so an absent or unreachable broker is an ordinary attempt failure
// under the supervisor's cadence instead of a fatal startup error.
func TestProductionBrokerConnectorIsLazy(t *testing.T) {
	startupErr := errors.New("broker not reachable")
	calls := 0
	previous := connectProductionClientBroker
	connectProductionClientBroker = func(context.Context) (ports.BrokerService, error) {
		calls++
		return nil, startupErr
	}
	t.Cleanup(func() { connectProductionClientBroker = previous })

	connector := newProductionBrokerConnector()
	require.NotNil(t, connector)
	require.Zero(t, calls, "construction must not connect")

	_, err := connector.Connect(context.Background())
	require.ErrorIs(t, err, startupErr)
	require.Equal(t, 1, calls)

	// Every attempt, the first included, is a fresh connect-or-spawn.
	_, err = connector.Connect(context.Background())
	require.ErrorIs(t, err, startupErr)
	require.Equal(t, 2, calls)
}

// TestUIDriverClientReadyOnBrokerFailureKeepsServing pins the non-fatal broker
// contract of the shared JSONL composition: a broker that can never be reached
// neither prevents the discovery response nor ends the service, and the driver
// keeps answering capture while it is stuck in the picker.
func TestUIDriverClientReadyOnBrokerFailureKeepsServing(t *testing.T) {
	startCtx := t.Context()
	terminal, err := uiterm.New(startCtx, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, "")
	require.NoError(t, err)
	defer terminal.Close()
	clk := clock.New()
	ui := client.NewUI(terminal, clk)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newUIDriverTestStream()
	done := make(chan error, 1)
	go func() {
		done <- runUIDriverClient(ctx, brokerClientConfig{
			Connector:             failingBrokerConnector{err: errors.New("broker not reachable")},
			Terminal:              terminal,
			Clock:                 clk,
			UI:                    ui,
			AttachmentEnvironment: terminalAttachmentEnvironment(),
			SessionEnvironment:    client.SessionEnvironment{Provenance: client.SessionEnvironmentLocalCLI},
		}, stream)
	}()

	ready := stream.awaitReady(t)
	require.NotEmpty(t, ready.Attachment)
	require.True(t, ready.Control)
	require.Zero(t, ready.Generation, "no actionable generation before a committed attachment")
	// A broker that can never be reached is a Picker, never an Attached
	// presentation and never a legacy transition state.
	require.Equal(t, ports.UIStatusPicker, ready.Status, "an unreachable broker is published as the picker")

	capture := stream.awaitCapture(t, ready.Attachment)
	require.NotNil(t, capture)
	// The Picker publication carries the run's handle and no session metadata.
	require.Equal(t, map[string]any{
		"attachment": ready.Attachment, "generation": float64(0), "session": map[string]any{"lifecycle_id": "", "session_name": ""},
		"focus": map[string]any{"tab_id": "", "pane_id": ""}, "output_epoch": float64(0), "output_state": float64(0),
		"view_revision": float64(0), "view_publication": float64(0), "status": "picker",
	}, capture["context"])

	// The run is still alive: a failed broker attempt must not end Serve.
	select {
	case err := <-done:
		t.Fatalf("the driver ended on a broker failure: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	require.NoError(t, stream.Close())
	select {
	case err := <-done:
		require.NoError(t, ignoreContextCancellation(err))
	case <-time.After(brokerTestWait):
		t.Fatal("the driver did not stop after its stream closed")
	}
}

// TestUIDriverClientRejectsMissingCompositionInputs pins the composition
// contract: the shared JSONL function requires a UI, a terminal, a clock, and a
// stream, and never invents one.
func TestUIDriverClientRejectsMissingCompositionInputs(t *testing.T) {
	clk := clock.New()
	stream := newUIDriverTestStream()
	defer func() { _ = stream.Close() }()
	require.Error(t, runUIDriverClient(t.Context(), brokerClientConfig{Connector: failingBrokerConnector{}}, stream))
	require.Error(t, runUIDriverClient(t.Context(), brokerClientConfig{Clock: clk, UI: client.NewUI(nil, clk)}, stream))
	require.Error(t, runUIDriverClient(t.Context(), brokerClientConfig{Clock: clk, UI: client.NewUI(nil, clk), Terminal: nil}, stream))
	require.Error(t, runUIDriverClient(t.Context(), brokerClientConfig{Clock: clk, UI: client.NewUI(nil, clk)}, nil))
}

// TestUIDriverSandboxHarnessUsesItsOwnConnector pins the sandbox boundary: the
// hidden harness reaches sessions only through the broker IPC connector of its
// own offline root, and never through the production connect-or-spawn.
func TestUIDriverSandboxHarnessUsesItsOwnConnector(t *testing.T) {
	productionConnects := 0
	previousConnect := connectProductionClientBroker
	connectProductionClientBroker = func(context.Context) (ports.BrokerService, error) {
		productionConnects++
		return nil, errors.New("the sandbox harness must never reach the production broker")
	}
	t.Cleanup(func() { connectProductionClientBroker = previousConnect })

	fixture := startUIDriverBrokerFixture(t)
	run := fixture.StartDriver(t)
	ready := run.Ready(t)
	require.NotEmpty(t, ready.Attachment)
	awaitLogicalStream(t, fixture.Streams())
	attached := run.awaitAttachedSnapshot(t, ready.Attachment, "", "offline-ready")
	require.Contains(t, offlineSnapshotText(attached), "offline-ready")
	require.Zero(t, productionConnects, "the sandbox harness keeps its own connector")

	run.EOF(t)
}

// TestUIDriverSandboxHarnessPickerOpensNothing pins the sandbox picker start: no
// initial navigation at all, so the harness reaches ready without creating a
// session or attaching anything.
func TestUIDriverSandboxHarnessPickerOpensNothing(t *testing.T) {
	fixture := startUIDriverBrokerFixture(t)
	run := fixture.StartPickerDriver(t)
	ready := run.Ready(t)
	require.NotEmpty(t, ready.Attachment)
	require.Zero(t, ready.Generation)
	// A picker start opens nothing: the driver stays usable while the broker
	// publishes no session for it.
	require.NotNil(t, run.capture(t, ready.Attachment))
	require.Empty(t, fixture.localSessionNames(t), "a picker start creates no session")
	run.EOF(t)
}

// TestUIDriverClientKeepsSharedDaemonOnEOF pins the EOF contract: closing a
// driver's JSONL stream ends only that run. The daemon and the broker stay alive,
// so a second driver reaches the same session afterwards.
func TestUIDriverClientKeepsSharedDaemonOnEOF(t *testing.T) {
	fixture := startUIDriverBrokerFixture(t)
	first := fixture.StartDriver(t)
	firstReady := first.Ready(t)
	awaitLogicalStream(t, fixture.Streams())
	first.awaitAttachedSnapshot(t, firstReady.Attachment, "", "offline-ready")
	first.EOF(t)

	// The shared daemon outlives the driver: a fresh driver reaches the same
	// session through the same fixture broker.
	second := fixture.StartDriver(t)
	secondReady := second.Ready(t)
	require.NotEqual(t, firstReady.Attachment, secondReady.Attachment)
	awaitLogicalStream(t, fixture.Streams())
	second.awaitAttachedSnapshot(t, secondReady.Attachment, "", "offline-ready")
	second.EOF(t)
}

// TestUIDriverFixtureUsesIsolatedProductionBroker pins the fixture isolation:
// the driver reaches the broker within its private production XDG roots.
func TestUIDriverFixtureUsesIsolatedProductionBroker(t *testing.T) {
	fixture := startUIDriverBrokerFixture(t)
	require.DirExists(t, filepath.Join(fixture.prodRuntime, "vev", "broker"))
	require.FileExists(t, filepath.Join(fixture.prodState, "vev", "broker", "state", "state.json"))
	connector := fixture.Connector()
	require.NotNil(t, connector)
	service, err := connector.Connect(t.Context())
	require.NoError(t, err)
	require.NoError(t, service.Close())
}
