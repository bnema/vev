package app

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestPerformanceTraceUsesHarnessProcessMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "process.jsonl")
	t.Setenv("VEV_PERF_TRACE", path)
	t.Setenv("VEV_PERF_PROCESS_ID", "idle-local-r001-daemon-01")
	t.Setenv("VEV_PERF_SCENARIO", "1x4-idle-local")
	t.Setenv("VEV_PERF_RUN", "1")

	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().Now().Return(time.Unix(0, 1)).Once()
	observer, closer, err := performanceTrace(clock)
	if err != nil {
		t.Fatal(err)
	}
	if observer == nil || closer == nil {
		t.Fatal("performance trace was not configured")
	}
	// performanceTrace's serialized return type makes a raw JSONL sink
	// unrepresentable to application transport wiring.
	observer.ObserveRuntime(ports.RuntimeMark{Schema: ports.RuntimeMarkSchema, Component: "daemon", Scenario: "runtime", Run: 1, Kind: ports.RuntimeEmitEnd, Valid: true})
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var mark struct {
		ProcessID string `json:"process_id"`
		Scenario  string `json:"scenario"`
		Run       uint64 `json:"run"`
		Sequence  uint64 `json:"sequence"`
		RequestID uint64 `json:"request_id"`
		Epoch     uint64 `json:"epoch"`
	}
	if err := json.NewDecoder(bufio.NewReader(f)).Decode(&mark); err != nil {
		t.Fatal(err)
	}
	if mark.ProcessID != "idle-local-r001-daemon-01" || mark.Scenario != "1x4-idle-local" || mark.Run != 1 || mark.Sequence == 0 || mark.RequestID == 0 || mark.Epoch == 0 {
		t.Fatalf("mark does not match harness mapping: %+v", mark)
	}
}

func TestPerformanceTraceSetupRollbackJoinsCloseError(t *testing.T) {
	setupErr := errors.New("correlation setup failed")
	closeErr := errors.New("rollback close failed")

	tests := []struct {
		name            string
		rawRun          string
		correlationErr  error
		wantPrimaryText string
		wantPrimaryErr  error
		wantNumError    bool
	}{
		{
			name:            "validation",
			rawRun:          "not-a-run",
			wantPrimaryText: `invalid VEV_PERF_RUN "not-a-run"`,
			wantNumError:    true,
		},
		{
			name:            "correlation",
			rawRun:          "1",
			correlationErr:  setupErr,
			wantPrimaryText: "runtime trace correlation",
			wantPrimaryErr:  setupErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("VEV_PERF_TRACE", "unused-by-test-sink")
			t.Setenv("VEV_PERF_PROCESS_ID", "test-process")
			t.Setenv("VEV_PERF_RUN", tc.rawRun)

			newSink := func(string, ports.Clock, string) (ports.RuntimeObserver, io.Closer, error) {
				return runtimeObserverFunc(func(ports.RuntimeMark) {}), errorCloser{err: closeErr}, nil
			}
			newCorrelation := func(observer ports.RuntimeObserver, inputs ports.RuntimeCorrelationInputs) (ports.RuntimeObserver, error) {
				if tc.correlationErr != nil {
					return nil, tc.correlationErr
				}
				return ports.NewRuntimeCorrelationObserver(observer, inputs)
			}

			observer, closer, err := performanceTraceWithFactories(nil, newSink, newCorrelation)
			if observer != nil || closer != nil {
				t.Fatalf("performanceTrace() returned observer=%v closer=%v after setup failure", observer, closer)
			}
			if !errors.Is(err, closeErr) {
				t.Errorf("performanceTrace() error = %v, want joined rollback error %v", err, closeErr)
			}
			if tc.wantPrimaryErr != nil && !errors.Is(err, tc.wantPrimaryErr) {
				t.Errorf("performanceTrace() error = %v, want primary setup error %v", err, tc.wantPrimaryErr)
			}
			if tc.wantNumError {
				var numErr *strconv.NumError
				if !errors.As(err, &numErr) {
					t.Errorf("performanceTrace() error = %v, want discoverable parse error", err)
				}
			}
			if !strings.Contains(err.Error(), tc.wantPrimaryText) {
				t.Errorf("performanceTrace() error = %q, want context %q", err, tc.wantPrimaryText)
			}
		})
	}
}

func TestRunAttachJoinsTraceCloseError(t *testing.T) {
	traceCloseErr := errors.New("trace close failed")
	original := newPerformanceTrace
	newPerformanceTrace = func(ports.Clock) (ports.SerializedRuntimeObserver, io.Closer, error) {
		return ports.NewSerializedRuntimeObserver(runtimeObserverFunc(func(ports.RuntimeMark) {}), 1), errorCloser{err: traceCloseErr}, nil
	}
	t.Cleanup(func() { newPerformanceTrace = original })
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv(envRemoteTransport, "invalid")

	err := runAttach(context.Background(), ports.IntentEphemeral, "", "host")
	if !errors.Is(err, traceCloseErr) {
		t.Fatalf("runAttach() error = %v, want joined trace close error %v", err, traceCloseErr)
	}
}

type errorCloser struct{ err error }

func (c errorCloser) Close() error { return c.err }

type runtimeObserverFunc func(ports.RuntimeMark)

func (f runtimeObserverFunc) ObserveRuntime(mark ports.RuntimeMark) { f(mark) }

func TestPerformanceTraceAppWiresOneObserverPerProcess(t *testing.T) {
	source, err := os.ReadFile("run.go")
	if err != nil {
		t.Fatalf("ReadFile(run.go): %v", err)
	}
	text := string(source)
	for _, required := range []string{
		"VEV_PERF_TRACE",
		"VEV_PERF_PROCESS_ID",
		"observability.NewJSONL",
		"WithRuntimeObserver",
	} {
		if !strings.Contains(text, required) {
			t.Errorf("runtime trace wiring missing %q", required)
		}
	}
	if strings.Contains(text, "WithRuntimeObserver(clock") || strings.Contains(text, "WithRuntimeObserver(clk") {
		t.Error("consumer observer configuration must not accept a clock")
	}
}
