package app

// Hidden `_broker-ready` dial-only probe contract (Plan 001 P3.4).
//
// The probe selects one broker IPC endpoint, dials it, registers, subscribes,
// and reads complete snapshots. These tests pin the whole caller contract
// without a production broker process: argument parsing, the bounded JSON
// document, the closed exit-code table, the readiness evaluation of both
// `--require` modes, the terminal classification that must never retry, the
// reconnect on a new broker epoch, and the zero-effect guarantee (no ensure, no
// spawn, no logical stream, no reconcile, and no membership mutation).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokeripc"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

const (
	readyTestEpoch      = ports.BrokerEpoch(0x2a)
	readyTestRevision   = ports.BrokerRevision(7)
	readyTestIdentity   = ports.BrokerDaemonIdentity("ready-local-daemon")
	readyTestObserveMax = 60 * time.Millisecond
)

// readyTestIncarnate is one valid, non-zero daemon incarnation.
var readyTestIncarnate = ports.BrokerDaemonIncarnation{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

// readyTestPolicy is one valid exactly-provisioned connection policy.
func readyTestPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: 3,
		EnvironmentPolicy:    protocol.EnvironmentPolicyDaemonOwned,
		Transport:            "unix-mux",
		Trust:                "same-user",
		Launch:               "explicit",
		Isolation:            "per-user",
	}
}

// readyTestLocal builds one local daemon observation with the supplied observed
// state, so a test states exactly which readiness input it varies.
func readyTestLocal(mutate func(*ports.BrokerDaemonObservation)) ports.BrokerDaemonObservation {
	local := ports.BrokerDaemonObservation{
		Local:           true,
		DisplayOrigin:   "local",
		Policy:          readyTestPolicy(),
		Identity:        readyTestIdentity,
		Incarnation:     readyTestIncarnate,
		ProtocolVersion: protocol.Version,
		Availability:    domain.RemoteAvailabilityReachable,
		InventoryKnown:  true,
		LastSuccess:     time.Now().UTC(),
	}
	if mutate != nil {
		mutate(&local)
	}
	return local
}

// readyTestSnapshot builds one valid publication carrying exactly one local
// daemon observation.
func readyTestSnapshot(mutate func(*ports.BrokerDaemonObservation)) ports.BrokerSnapshot {
	return ports.BrokerSnapshot{
		Epoch:    readyTestEpoch,
		Revision: readyTestRevision,
		Daemons:  []ports.BrokerDaemonObservation{readyTestLocal(mutate)},
	}
}

// readyScriptedSubscription is one coalescing capacity-one subscription.
type readyScriptedSubscription struct {
	mu      sync.Mutex
	changed chan struct{}
	closed  int
}

func newReadyScriptedSubscription() *readyScriptedSubscription {
	return &readyScriptedSubscription{changed: make(chan struct{}, 1)}
}

func (s *readyScriptedSubscription) Changed() <-chan struct{} { return s.changed }

func (s *readyScriptedSubscription) Close() {
	s.mu.Lock()
	s.closed++
	s.mu.Unlock()
}

func (s *readyScriptedSubscription) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// readyScriptedService is a scripted ports.BrokerService. It serves one fixed
// snapshot, records every mutating or stream method it is asked for, and lets a
// test end its connection to drive the reconnect path.
type readyScriptedService struct {
	snapshot ports.BrokerSnapshot
	subErr   error

	mu        sync.Mutex
	done      chan struct{}
	doneOnce  sync.Once
	subs      []*readyScriptedSubscription
	closes    int
	mutations []string
}

func newReadyScriptedService(snapshot ports.BrokerSnapshot) *readyScriptedService {
	return &readyScriptedService{snapshot: snapshot, done: make(chan struct{})}
}

func (s *readyScriptedService) record(name string) {
	s.mu.Lock()
	s.mutations = append(s.mutations, name)
	s.mu.Unlock()
}

func (s *readyScriptedService) ConnectionID() ports.BrokerConnectionID {
	return ports.BrokerConnectionID{0x2a}
}

func (s *readyScriptedService) NextStreamID() (ports.BrokerStreamID, error) { return 1, nil }

func (s *readyScriptedService) Done() <-chan struct{} { return s.done }

func (s *readyScriptedService) Err() error { return nil }

func (s *readyScriptedService) Snapshot() ports.BrokerSnapshot { return s.snapshot.Clone() }

func (s *readyScriptedService) Subscribe() (ports.BrokerSubscription, error) {
	if s.subErr != nil {
		return nil, s.subErr
	}
	sub := newReadyScriptedSubscription()
	s.mu.Lock()
	s.subs = append(s.subs, sub)
	s.mu.Unlock()
	return sub, nil
}

func (s *readyScriptedService) SubscribePreview(ports.BrokerPreviewRequest) (ports.BrokerPreviewSubscription, error) {
	s.record("SubscribePreview")
	return nil, errors.New("ready scripted service does not support preview")
}

func (s *readyScriptedService) OpenStream(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	s.record("OpenStream")
	return nil, ports.BrokerAdmissionClosed
}

func (s *readyScriptedService) CloseStream(ports.BrokerConnectionID, ports.BrokerStreamID) error {
	s.record("CloseStream")
	return nil
}

func (s *readyScriptedService) AddHost(context.Context, string, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	s.record("AddHost")
	return domain.RemoteRegistration{}, nil
}

func (s *readyScriptedService) RemoveHost(context.Context, domain.RemoteRegistration) (bool, error) {
	s.record("RemoveHost")
	return false, nil
}

func (s *readyScriptedService) UpdateHostPolicy(context.Context, domain.RemoteRegistration, ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	s.record("UpdateHostPolicy")
	return domain.RemoteRegistration{}, nil
}

func (s *readyScriptedService) RequestReconcile(string) { s.record("RequestReconcile") }

func (s *readyScriptedService) Close() error {
	s.mu.Lock()
	s.closes++
	s.mu.Unlock()
	return nil
}

// finish closes Done, so the probe observes the connection going terminal.
func (s *readyScriptedService) finish() { s.doneOnce.Do(func() { close(s.done) }) }

func (s *readyScriptedService) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

// mutationNames returns the effectful methods the probe asked for, if any.
func (s *readyScriptedService) mutationNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.mutations...)
}

// subscriptionCloses returns the total Close count across this service's
// subscriptions.
func (s *readyScriptedService) subscriptionCloses() int {
	s.mu.Lock()
	subs := append([]*readyScriptedSubscription(nil), s.subs...)
	s.mu.Unlock()
	total := 0
	for _, sub := range subs {
		total += sub.closeCount()
	}
	return total
}

var _ ports.BrokerService = (*readyScriptedService)(nil)

// readyDialScript records every dial the probe performs and serves one fresh
// scripted service per call, so a probe is proven to reconnect rather than
// reuse a connection.
type readyDialScript struct {
	mu       sync.Mutex
	dials    int
	services []*readyScriptedService
	produce  func() (ports.BrokerService, error)
}

// scriptReadyDial replaces the probe's dial seam for one test.
func scriptReadyDial(t *testing.T, produce func() (ports.BrokerService, error)) *readyDialScript {
	t.Helper()
	script := &readyDialScript{produce: produce}
	previous := brokerReadyDial
	brokerReadyDial = func(context.Context, string) (ports.BrokerService, error) {
		script.mu.Lock()
		script.dials++
		script.mu.Unlock()
		service, err := script.produce()
		if recorded, ok := service.(*readyScriptedService); ok {
			script.mu.Lock()
			script.services = append(script.services, recorded)
			script.mu.Unlock()
		}
		return service, err
	}
	t.Cleanup(func() { brokerReadyDial = previous })
	return script
}

func (s *readyDialScript) dialCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dials
}

func (s *readyDialScript) createdServices() []*readyScriptedService {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*readyScriptedService(nil), s.services...)
}

// readyTestOptions builds one parsed probe invocation over a fresh isolated
// offline root, so no production path is ever inspected.
func readyTestOptions(t *testing.T, requirement string, timeout time.Duration) brokerReadyOptions {
	t.Helper()
	emptyProductionBrokerLayout(t, "")
	options, err := parseBrokerReadyArgs([]string{"--require", requirement, "--timeout", timeout.String()})
	require.NoError(t, err)
	return options
}

// runReadyProbe drives the real probe entry point over the scripted dial seam.
func runReadyProbe(t *testing.T, options brokerReadyOptions) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := runBrokerReady(context.Background(), options, &out)
	return out.String(), err
}

// decodeReadyDocument asserts stdout holds exactly one bounded JSON line and
// returns the decoded object.
func decodeReadyDocument(t *testing.T, raw string) map[string]any {
	t.Helper()
	trimmed := strings.TrimSpace(raw)
	require.Equal(t, 1, strings.Count(trimmed, "\n")+1, "the probe writes exactly one JSON line")
	require.LessOrEqual(t, len(trimmed)+1, 4096, "the document stays inside its bound")
	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(trimmed), &decoded))
	require.Equal(t, "vev.broker-ready/v1", decoded["schema"])
	return decoded
}

// TestParseBrokerReadyArgs pins the strict argument contract: unknown,
// duplicated, incomplete, and non-positive options are refused, a require mode
// outside the closed pair is refused, the observation-age bound is admitted only
// for the catalogue mode, and every default is explicit.
func TestParseBrokerReadyArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		want    brokerReadyOptions
		wantErr string
	}{
		{
			name: "authority defaults",
			args: []string{"--require", "local-authority"},
			want: brokerReadyOptions{require: "local-authority", timeout: 10 * time.Second, maxAge: 5 * time.Second},
		},
		{
			name: "catalogue with an explicit observation age",
			args: []string{"--require", "local-catalogue", "--max-observation-age", "2s", "--timeout", "3s"},
			want: brokerReadyOptions{require: "local-catalogue", timeout: 3 * time.Second, maxAge: 2 * time.Second},
		},
		{name: "missing require", args: nil, wantErr: "`--require` must be local-authority or local-catalogue"},
		{name: "unknown require", args: []string{"--require", "remote-catalogue"}, wantErr: "`--require` must be local-authority or local-catalogue"},
		{name: "missing value", args: []string{"--require"}, wantErr: "requires a value"},
		{name: "duplicate option", args: []string{"--require", "local-authority", "--require", "local-authority"}, wantErr: "duplicate"},
		{name: "unknown option", args: []string{"--require", "local-authority", "--ensure", "1"}, wantErr: "unknown `--ensure` option"},
		{name: "timeout is not a duration", args: []string{"--require", "local-authority", "--timeout", "soon"}, wantErr: "`--timeout` must be positive"},
		{name: "timeout is zero", args: []string{"--require", "local-authority", "--timeout", "0s"}, wantErr: "`--timeout` must be positive"},
		{name: "timeout is negative", args: []string{"--require", "local-authority", "--timeout", "-1s"}, wantErr: "`--timeout` must be positive"},
		{name: "observation age is not a duration", args: []string{"--require", "local-catalogue", "--max-observation-age", "later"}, wantErr: "`--max-observation-age` must be positive"},
		{name: "observation age is zero", args: []string{"--require", "local-catalogue", "--max-observation-age", "0s"}, wantErr: "`--max-observation-age` must be positive"},
		{
			name:    "observation age is authority-only",
			args:    []string{"--require", "local-authority", "--max-observation-age", "2s"},
			wantErr: "`--max-observation-age` is only valid for local-catalogue",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseBrokerReadyArgs(tt.args)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, 2, ExitCode(err), "an argument refusal is a usage exit")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestBrokerReadyDocumentIsOneBoundedJSONLine pins the machine contract: exactly
// one JSON line, decimal-string epoch/revision IDs, the pending list, and the
// observed local projection, including its hex incarnation and RFC3339
// observation time.
func TestBrokerReadyDocumentIsOneBoundedJSONLine(t *testing.T) {
	t.Parallel()

	local := readyTestLocal(func(local *ports.BrokerDaemonObservation) {
		local.LastSuccess = time.Date(2026, 9, 21, 12, 0, 0, 123456789, time.UTC)
	})
	options := brokerReadyOptions{require: "local-catalogue", timeout: time.Second, maxAge: time.Second}

	tests := []struct {
		name   string
		result brokerReadyResult
	}{
		{name: "absent local", result: readyResult(options, "pending", "local-not-ready", nil)},
		{name: "observed local", result: readyResult(options, "pending", "local-not-ready", &local)},
		{name: "ready snapshot", result: evaluateReady(options, readyTestSnapshot(func(observed *ports.BrokerDaemonObservation) { *observed = local }), local)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			// emitReady reports the exit code for a non-ready status; the document
			// is written either way, which is the property under test here.
			_ = emitReady(&out, tt.result)

			raw := out.String()
			require.True(t, strings.HasSuffix(raw, "\n"), "the document ends with exactly one newline")
			decoded := decodeReadyDocument(t, raw)
			require.Equal(t, tt.result.Status, decoded["status"])
			require.Equal(t, tt.result.Require, decoded["require"])
			require.Equal(t, tt.result.Reason, decoded["reason"])
			require.Equal(t, toAnySlice(tt.result.Pending), decoded["pending"])
			require.Equal(t, tt.result.Epoch, decoded["epoch"], "epoch travels as a decimal string")
			require.Equal(t, tt.result.Revision, decoded["revision"], "revision travels as a decimal string")

			if tt.result.Local == nil {
				require.Nil(t, decoded["local"])
				return
			}
			observed, ok := decoded["local"].(map[string]any)
			require.True(t, ok, "an observed local is a JSON object")
			require.Equal(t, "ready-local-daemon", observed["identity"])
			require.Equal(t, "0102030405060708090a0b0c0d0e0f10", observed["incarnation"])
			require.Equal(t, strconv.FormatUint(uint64(protocol.Version), 10), observed["version"])
			require.Equal(t, "reachable", observed["availability"])
			require.Equal(t, true, observed["inventory_known"])
			require.Equal(t, "2026-09-21T12:00:00.123456789Z", observed["last_success"])
		})
	}
}

// TestBrokerReadyExitCodeTable pins the closed status-to-exit-code mapping: 0
// ready, 3 timeout (including the default pending case), 4 terminal, and 5
// cancellation. The document is written before the exit code is returned.
func TestBrokerReadyExitCodeTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     string
		wantCode   int
		wantErrMsg string
	}{
		{name: "ready", status: "ready", wantCode: 0},
		{name: "timeout", status: "timeout", wantCode: 3, wantErrMsg: "broker readiness timeout"},
		{name: "terminal", status: "terminal", wantCode: 4, wantErrMsg: "broker readiness terminal failure"},
		{name: "canceled", status: "canceled", wantCode: 5, wantErrMsg: "context canceled"},
		{name: "pending", status: "pending", wantCode: 3, wantErrMsg: "broker readiness timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var out bytes.Buffer
			result := brokerReadyResult{Schema: "vev.broker-ready/v1", Status: tt.status, Require: "local-authority", Pending: []string{}}
			err := emitReady(&out, result)
			require.Equal(t, tt.wantCode, ExitCode(err))
			if tt.wantErrMsg == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErrMsg)
			}
			require.Equal(t, 1, strings.Count(out.String(), "\n"), "the report is written even when the exit code is non-zero")
		})
	}
}

// TestBrokerReadyObservationTable pins the readiness evaluation itself:
// `local-authority` is satisfied by any snapshot that carries a valid local
// entry, `local-catalogue` requires a reachable, known, fresh, version-exact
// observation, an empty known inventory is a success, and a structurally
// invalid or doubled local publication is terminal rather than pending.
func TestBrokerReadyObservationTable(t *testing.T) {
	tests := []struct {
		name         string
		require      string
		snapshot     ports.BrokerSnapshot
		subErr       error
		wantNil      bool
		wantStatus   string
		wantReason   string
		wantEpoch    string
		wantRevision string
		wantPending  []string
	}{
		{
			name:         "unknown availability satisfies the authority requirement",
			require:      "local-authority",
			snapshot:     readyTestSnapshot(func(local *ports.BrokerDaemonObservation) { local.Availability = domain.RemoteAvailabilityUnknown }),
			wantStatus:   "ready",
			wantReason:   "authority",
			wantEpoch:    "42",
			wantRevision: "7",
			wantPending:  []string{},
		},
		{
			name:         "reachable known empty inventory satisfies the catalogue requirement",
			require:      "local-catalogue",
			snapshot:     readyTestSnapshot(nil),
			wantStatus:   "ready",
			wantReason:   "catalogue",
			wantEpoch:    "42",
			wantRevision: "7",
			wantPending:  []string{},
		},
		{
			name:        "unreachable local stays pending",
			require:     "local-catalogue",
			snapshot:    readyTestSnapshot(func(local *ports.BrokerDaemonObservation) { local.Availability = domain.RemoteAvailabilityUnreachable }),
			wantNil:     true,
			wantStatus:  "pending",
			wantPending: []string{"local-catalogue"},
		},
		{
			name:        "unknown inventory stays pending",
			require:     "local-catalogue",
			snapshot:    readyTestSnapshot(func(local *ports.BrokerDaemonObservation) { local.InventoryKnown = false }),
			wantNil:     true,
			wantStatus:  "pending",
			wantPending: []string{"local-catalogue"},
		},
		{
			name:        "stale observation stays pending",
			require:     "local-catalogue",
			snapshot:    readyTestSnapshot(func(local *ports.BrokerDaemonObservation) { local.LastSuccess = time.Now().Add(-time.Hour) }),
			wantNil:     true,
			wantStatus:  "pending",
			wantPending: []string{"local-catalogue"},
		},
		{
			name:        "missing observation time stays pending",
			require:     "local-catalogue",
			snapshot:    readyTestSnapshot(func(local *ports.BrokerDaemonObservation) { local.LastSuccess = time.Time{} }),
			wantNil:     true,
			wantStatus:  "pending",
			wantPending: []string{"local-catalogue"},
		},
		{
			name:        "incompatible protocol version stays pending",
			require:     "local-catalogue",
			snapshot:    readyTestSnapshot(func(local *ports.BrokerDaemonObservation) { local.ProtocolVersion = protocol.Version + 1 }),
			wantNil:     true,
			wantStatus:  "pending",
			wantPending: []string{"local-catalogue"},
		},
		{
			name:    "unobserved local stays pending",
			require: "local-catalogue",
			snapshot: readyTestSnapshot(func(local *ports.BrokerDaemonObservation) {
				*local = ports.BrokerDaemonObservation{Local: true, DisplayOrigin: "local", Policy: readyTestPolicy(), Availability: domain.RemoteAvailabilityUnknown}
			}),
			wantNil:     true,
			wantStatus:  "pending",
			wantPending: []string{"local-catalogue"},
		},
		{
			name:        "snapshot without an epoch is no observation at all",
			require:     "local-authority",
			snapshot:    ports.BrokerSnapshot{},
			wantNil:     true,
			wantStatus:  "pending",
			wantPending: []string{"local-authority"},
		},
		{
			name:         "snapshot without a revision is terminal",
			require:      "local-authority",
			snapshot:     ports.BrokerSnapshot{Epoch: readyTestEpoch, Revision: 0, Daemons: []ports.BrokerDaemonObservation{readyTestLocal(nil)}},
			wantStatus:   "terminal",
			wantReason:   "protocol",
			wantEpoch:    "0",
			wantRevision: "0",
			wantPending:  []string{"local-authority"},
		},
		{
			name:    "two local daemons are terminal",
			require: "local-authority",
			snapshot: ports.BrokerSnapshot{
				Epoch: readyTestEpoch, Revision: readyTestRevision,
				Daemons: []ports.BrokerDaemonObservation{readyTestLocal(nil), readyTestLocal(nil)},
			},
			wantStatus:   "terminal",
			wantReason:   "protocol",
			wantEpoch:    "0",
			wantRevision: "0",
			wantPending:  []string{"local-authority"},
		},
		{
			name:         "subscription refusal is terminal",
			require:      "local-authority",
			snapshot:     readyTestSnapshot(nil),
			subErr:       errors.Join(brokeripc.ErrProtocol, errors.New("broker refused the subscription")),
			wantStatus:   "terminal",
			wantReason:   "protocol",
			wantEpoch:    "0",
			wantRevision: "0",
			wantPending:  []string{"local-authority"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := newReadyScriptedService(tt.snapshot)
			service.subErr = tt.subErr
			ctx, cancel := context.WithTimeout(context.Background(), readyTestObserveMax)
			defer cancel()

			result := observeBrokerReady(ctx, brokerReadyOptions{require: tt.require, timeout: time.Second, maxAge: time.Second}, service)
			require.Empty(t, service.mutationNames(), "readiness must never open a stream, reconcile, or mutate membership")
			if tt.subErr != nil {
				require.Zero(t, service.subscriptionCloses(), "a refused subscription is never closed as if it existed")
			} else {
				require.Equal(t, 1, service.subscriptionCloses(), "the probe closes the subscription it opened")
			}
			if tt.wantNil {
				require.Nil(t, result, "an unsatisfied observation publishes nothing")
				return
			}
			require.NotNil(t, result)
			require.Equal(t, tt.wantStatus, result.Status)
			require.Equal(t, tt.wantReason, result.Reason)
			require.Equal(t, tt.wantEpoch, result.Epoch)
			require.Equal(t, tt.wantRevision, result.Revision)
			require.Equal(t, tt.wantPending, result.Pending)
			require.Equal(t, "vev.broker-ready/v1", result.Schema)
		})
	}
}

// TestBrokerReadyRunOutcomeTable drives the real probe entry point for every
// exit class: an on-demand broker that is merely absent or refused is retried
// into the deadline (3), a security, configuration, or protocol refusal is
// terminal (4), a satisfied requirement is 0, and every connection the probe
// opened is closed exactly once with no effectful call.
func TestBrokerReadyRunOutcomeTable(t *testing.T) {
	tests := []struct {
		name        string
		require     string
		produce     func() (ports.BrokerService, error)
		wantCode    int
		wantStatus  string
		wantReason  string
		wantPending []string
	}{
		{
			name:        "authority satisfied",
			require:     "local-authority",
			produce:     func() (ports.BrokerService, error) { return newReadyScriptedService(readyTestSnapshot(nil)), nil },
			wantCode:    0,
			wantStatus:  "ready",
			wantReason:  "authority",
			wantPending: []string{},
		},
		{
			name:        "catalogue satisfied by a known empty inventory",
			require:     "local-catalogue",
			produce:     func() (ports.BrokerService, error) { return newReadyScriptedService(readyTestSnapshot(nil)), nil },
			wantCode:    0,
			wantStatus:  "ready",
			wantReason:  "catalogue",
			wantPending: []string{},
		},
		{
			name:    "catalogue with an incompatible version times out",
			require: "local-catalogue",
			produce: func() (ports.BrokerService, error) {
				return newReadyScriptedService(readyTestSnapshot(func(local *ports.BrokerDaemonObservation) { local.ProtocolVersion = protocol.Version + 1 })), nil
			},
			wantCode:    3,
			wantStatus:  "timeout",
			wantReason:  "deadline",
			wantPending: []string{"local-catalogue"},
		},
		{
			name:    "stale catalogue observation times out",
			require: "local-catalogue",
			produce: func() (ports.BrokerService, error) {
				return newReadyScriptedService(readyTestSnapshot(func(local *ports.BrokerDaemonObservation) { local.LastSuccess = time.Now().Add(-time.Hour) })), nil
			},
			wantCode:    3,
			wantStatus:  "timeout",
			wantReason:  "deadline",
			wantPending: []string{"local-catalogue"},
		},
		{
			name:        "no snapshot times out",
			require:     "local-authority",
			produce:     func() (ports.BrokerService, error) { return newReadyScriptedService(ports.BrokerSnapshot{}), nil },
			wantCode:    3,
			wantStatus:  "timeout",
			wantReason:  "deadline",
			wantPending: []string{"local-authority"},
		},
		{
			name:    "absent endpoint is retried then times out",
			require: "local-authority",
			produce: func() (ports.BrokerService, error) {
				return nil, &net.OpError{Op: "dial", Net: "unix", Err: syscall.ENOENT}
			},
			wantCode:    3,
			wantStatus:  "timeout",
			wantReason:  "deadline",
			wantPending: []string{"local-authority"},
		},
		{
			name:    "refused endpoint is retried then times out",
			require: "local-authority",
			produce: func() (ports.BrokerService, error) {
				return nil, &net.OpError{Op: "dial", Net: "unix", Err: syscall.ECONNREFUSED}
			},
			wantCode:    3,
			wantStatus:  "timeout",
			wantReason:  "deadline",
			wantPending: []string{"local-authority"},
		},
		{
			name:    "invalid snapshot is terminal",
			require: "local-authority",
			produce: func() (ports.BrokerService, error) {
				return newReadyScriptedService(ports.BrokerSnapshot{Epoch: readyTestEpoch}), nil
			},
			wantCode:    4,
			wantStatus:  "terminal",
			wantReason:  "protocol",
			wantPending: []string{"local-authority"},
		},
		{
			name:        "rejected peer is terminal",
			require:     "local-authority",
			produce:     func() (ports.BrokerService, error) { return nil, ipc.ErrMuxPeerRejected },
			wantCode:    4,
			wantStatus:  "terminal",
			wantReason:  "security",
			wantPending: []string{"local-authority"},
		},
		{
			name:        "permission refusal is terminal",
			require:     "local-authority",
			produce:     func() (ports.BrokerService, error) { return nil, os.ErrPermission },
			wantCode:    4,
			wantStatus:  "terminal",
			wantReason:  "security",
			wantPending: []string{"local-authority"},
		},
		{
			name:        "invalid configuration is terminal",
			require:     "local-authority",
			produce:     func() (ports.BrokerService, error) { return nil, brokeripc.ErrConfig },
			wantCode:    4,
			wantStatus:  "terminal",
			wantReason:  "config",
			wantPending: []string{"local-authority"},
		},
		{
			name:    "broker protocol violation is terminal",
			require: "local-authority",
			produce: func() (ports.BrokerService, error) {
				return nil, errors.Join(brokeripc.ErrMalformedFrame, errors.New("truncated"))
			},
			wantCode:    4,
			wantStatus:  "terminal",
			wantReason:  "protocol",
			wantPending: []string{"local-authority"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := readyTestOptions(t, tt.require, 80*time.Millisecond)
			script := scriptReadyDial(t, tt.produce)

			out, err := runReadyProbe(t, options)
			require.Equal(t, tt.wantCode, ExitCode(err), "probe outcome %q", out)

			decoded := decodeReadyDocument(t, out)
			require.Equal(t, tt.wantStatus, decoded["status"])
			require.Equal(t, tt.wantReason, decoded["reason"])
			require.Equal(t, tt.require, decoded["require"])
			require.Equal(t, toAnySlice(tt.wantPending), decoded["pending"])

			services := script.createdServices()
			for _, service := range services {
				require.Equal(t, 1, service.closeCount(), "every dialed connection is closed exactly once")
				require.Empty(t, service.mutationNames(), "readiness opens no stream, reconciles nothing, and mutates no membership")
				require.Equal(t, 1, service.subscriptionCloses(), "the probe closes the subscription it opened")
			}
			if tt.wantCode == 0 {
				require.Equal(t, 1, script.dialCount(), "a satisfied probe dials exactly once")
			}
		})
	}
}

// TestBrokerReadyNeverEnsuresSpawnsOrMutates pins the zero-effect guarantee at
// the probe level: the dial seam is the only I/O seam it uses, every connection
// it opens is closed, every subscription is closed, and no logical stream,
// reconcile, or membership mutation is ever attempted.
func TestBrokerReadyNeverEnsuresSpawnsOrMutates(t *testing.T) {
	options := readyTestOptions(t, "local-authority", 80*time.Millisecond)
	script := scriptReadyDial(t, func() (ports.BrokerService, error) {
		return newReadyScriptedService(readyTestSnapshot(nil)), nil
	})

	out, err := runReadyProbe(t, options)
	require.NoError(t, err, "outcome %q", out)
	require.Equal(t, 1, script.dialCount(), "a satisfied probe dials exactly once")
	require.Len(t, script.createdServices(), 1)

	service := script.createdServices()[0]
	require.Empty(t, service.mutationNames(), "readiness opens no stream, reconciles nothing, and mutates no membership")
	require.Equal(t, 1, service.closeCount())
	require.Equal(t, 1, service.subscriptionCloses())

	decoded := decodeReadyDocument(t, out)
	require.Equal(t, "ready", decoded["status"])
	observed, ok := decoded["local"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "reachable", observed["availability"])
	require.Equal(t, true, observed["inventory_known"])
}

// TestBrokerReadyReconnectUsesTheNewEpoch pins that a broker connection that
// ends before readiness is replaced by a fresh dial, and that the report carries
// the new broker epoch: a retired epoch is never reused as readiness authority.
func TestBrokerReadyReconnectUsesTheNewEpoch(t *testing.T) {
	freshSnapshot := ports.BrokerSnapshot{Epoch: ports.BrokerEpoch(2), Revision: 3, Daemons: []ports.BrokerDaemonObservation{readyTestLocal(nil)}}

	options := readyTestOptions(t, "local-authority", 2*time.Second)
	var (
		mu       sync.Mutex
		first    *readyScriptedService
		attempts int
	)
	scriptReadyDial(t, func() (ports.BrokerService, error) {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts == 1 {
			// The first connection commits no snapshot before it retires: the
			// probe must reconnect rather than report the retired connection.
			first = newReadyScriptedService(ports.BrokerSnapshot{})
			first.finish()
			return first, nil
		}
		return newReadyScriptedService(freshSnapshot), nil
	})

	out, err := runReadyProbe(t, options)
	require.NoError(t, err, "outcome %q", out)

	decoded := decodeReadyDocument(t, out)
	require.Equal(t, "ready", decoded["status"])
	require.Equal(t, "2", decoded["epoch"], "the report carries the epoch of the connection that answered")
	require.NotEqual(t, "1", decoded["epoch"], "a retired epoch is never reused")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, attempts, "a retired connection is replaced by exactly one fresh dial")
	require.NotNil(t, first)
	require.Equal(t, 1, first.closeCount(), "the retired connection is closed exactly once")
}

// TestBrokerReadyCancellationIsExitFiveAndClosesEverything pins the
// cancellation contract: the caller sees exit 5, the document says canceled,
// and the connection and subscription the probe opened are closed rather than
// leaked.
func TestBrokerReadyCancellationIsExitFiveAndClosesEverything(t *testing.T) {
	options := readyTestOptions(t, "local-authority", 5*time.Second)
	script := scriptReadyDial(t, func() (ports.BrokerService, error) {
		return newReadyScriptedService(ports.BrokerSnapshot{}), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	results := make(chan error, 1)
	go func() { results <- runBrokerReady(ctx, options, &out) }()

	require.Eventually(t, func() bool { return script.dialCount() == 1 }, time.Second, 5*time.Millisecond, "the probe dials before it is cancelled")
	cancel()

	select {
	case err := <-results:
		require.Equal(t, 5, ExitCode(err))
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled probe must return promptly")
	}

	decoded := decodeReadyDocument(t, out.String())
	require.Equal(t, "canceled", decoded["status"])
	require.Equal(t, "canceled", decoded["reason"])

	services := script.createdServices()
	require.Len(t, services, 1)
	require.Equal(t, 1, services[0].closeCount(), "cancellation closes the connection")
	require.Equal(t, 1, services[0].subscriptionCloses(), "cancellation closes the subscription")
	require.Empty(t, services[0].mutationNames())
}

// TestBrokerReadyEndpointSecurityClassificationTable pins the endpoint check the
// probe performs before any dial: absence is retryable, and a foreign path or an
// uninspectable path is a terminal security failure.
func TestBrokerReadyEndpointSecurityClassificationTable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	regular := filepath.Join(dir, "not-a-socket")
	require.NoError(t, os.WriteFile(regular, []byte("x"), 0o600))

	socketDir := shortTempDir(t, "ready")
	socketPath := filepath.Join(socketDir, "broker.sock")
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	unreadableDir := filepath.Join(t.TempDir(), "unreadable")
	require.NoError(t, os.Mkdir(unreadableDir, 0o700))
	unreadable := filepath.Join(unreadableDir, "broker.sock")
	require.NoError(t, os.Chmod(unreadableDir, 0o000))
	t.Cleanup(func() { _ = os.Chmod(unreadableDir, 0o700) })

	tests := []struct {
		name         string
		path         string
		wantTerminal bool
		wantReason   string
	}{
		{name: "absent endpoint is retryable", path: filepath.Join(dir, "absent.sock")},
		{name: "bound socket is dialable", path: socketPath},
		{name: "regular file is terminal", path: regular, wantTerminal: true, wantReason: "security"},
		{name: "directory is terminal", path: socketDir, wantTerminal: true, wantReason: "security"},
		{name: "uninspectable path is terminal", path: unreadable, wantTerminal: true, wantReason: "security"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reason, terminal := brokerReadyEndpointSecurity(tt.path)
			require.Equal(t, tt.wantTerminal, terminal)
			require.Equal(t, tt.wantReason, reason)
		})
	}
}

// TestBrokerReadyDialClassificationTable pins the dial-failure taxonomy: only a
// refusal that proves the peer, the path, the credentials, the configuration, or
// the broker conversation itself is not the expected broker is terminal.
func TestBrokerReadyDialClassificationTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		err          error
		wantTerminal bool
		wantReason   string
	}{
		{name: "nil is not a failure", err: nil},
		{name: "absent endpoint is retryable", err: &net.OpError{Op: "dial", Net: "unix", Err: syscall.ENOENT}},
		{name: "refused endpoint is retryable", err: &net.OpError{Op: "dial", Net: "unix", Err: syscall.ECONNREFUSED}},
		{name: "cancellation is retryable", err: context.Canceled},
		{name: "deadline is retryable", err: context.DeadlineExceeded},
		{name: "unclassified failure is retryable", err: errors.New("io error")},
		{name: "rejected peer is terminal", err: ipc.ErrMuxPeerRejected, wantTerminal: true, wantReason: "security"},
		{name: "foreign path is terminal", err: ipc.ErrMuxForeignPath, wantTerminal: true, wantReason: "security"},
		{name: "invalid mux path is terminal", err: ipc.ErrMuxPath, wantTerminal: true, wantReason: "security"},
		{name: "unsupported peer check is terminal", err: ipc.ErrMuxUnsupported, wantTerminal: true, wantReason: "security"},
		{name: "permission failure is terminal", err: &net.OpError{Op: "dial", Net: "unix", Err: syscall.EACCES}, wantTerminal: true, wantReason: "security"},
		{name: "invalid configuration is terminal", err: brokeripc.ErrConfig, wantTerminal: true, wantReason: "config"},
		{name: "broker protocol violation is terminal", err: brokeripc.ErrProtocol, wantTerminal: true, wantReason: "protocol"},
		{name: "malformed frame is terminal", err: brokeripc.ErrMalformedFrame, wantTerminal: true, wantReason: "protocol"},
		{name: "oversize non-broker frame is terminal", err: ipc.ErrFrameTooLarge, wantTerminal: true, wantReason: "protocol"},
		{name: "zero-length non-broker frame is terminal", err: ipc.ErrZeroLengthFrame, wantTerminal: true, wantReason: "protocol"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reason, terminal := terminalReadyReason(tt.err)
			require.Equal(t, tt.wantTerminal, terminal, "cause %v", tt.err)
			require.Equal(t, tt.wantReason, reason)
		})
	}
}

// toAnySlice renders a decoded JSON string array comparison input.
func toAnySlice(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}
