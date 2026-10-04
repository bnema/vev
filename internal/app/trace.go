package app

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/bnema/vev/internal/adapters/observability"
	"github.com/bnema/vev/internal/ports"
)

// newPerformanceTrace is the trace factory seam; tests replace it to observe
// propagation without opening real trace files.
var newPerformanceTrace = performanceTrace

// performanceTrace creates one serialized timestamp owner for this process.
// An empty trace environment leaves all production behavior and wire bytes
// unchanged.
func performanceTrace(clk ports.Clock) (ports.SerializedRuntimeObserver, io.Closer, error) {
	return performanceTraceWithFactories(clk, observability.NewJSONL, ports.NewRuntimeCorrelationObserver)
}

// performanceTraceWithFactories keeps setup rollback behavior directly
// testable without requiring an operating-system file close failure.
func performanceTraceWithFactories(
	clk ports.Clock,
	newSink func(string, ports.Clock, string) (ports.RuntimeObserver, io.Closer, error),
	newCorrelation func(ports.RuntimeObserver, ports.RuntimeCorrelationInputs) (ports.RuntimeObserver, error),
) (ports.SerializedRuntimeObserver, io.Closer, error) {
	path, processID := os.Getenv("VEV_PERF_TRACE"), os.Getenv("VEV_PERF_PROCESS_ID")
	if path == "" {
		return nil, nil, nil
	}
	observer, closer, err := newSink(path, clk, processID)
	if err != nil {
		return nil, nil, err
	}
	// The harness supplies these manifest fields to every launched role. Keep a
	// valid standalone trace for operators that set only the original trace
	// variables, while ensuring harness traces match their process mapping.
	inputs := ports.RuntimeCorrelationInputs{Scenario: os.Getenv("VEV_PERF_SCENARIO"), Run: 1}
	if inputs.Scenario == "" {
		inputs.Scenario = "runtime"
	}
	if rawRun := os.Getenv("VEV_PERF_RUN"); rawRun != "" {
		inputs.Run, err = strconv.ParseUint(rawRun, 10, 64)
		if err != nil {
			setupErr := fmt.Errorf("invalid VEV_PERF_RUN %q: %w", rawRun, err)
			return nil, nil, closeTraceAfterSetupFailure(closer, setupErr)
		}
		if inputs.Run == 0 {
			setupErr := fmt.Errorf("invalid VEV_PERF_RUN %q", rawRun)
			return nil, nil, closeTraceAfterSetupFailure(closer, setupErr)
		}
	}
	observer, err = newCorrelation(observer, inputs)
	if err != nil {
		setupErr := fmt.Errorf("configure runtime trace correlation: %w", err)
		return nil, nil, closeTraceAfterSetupFailure(closer, setupErr)
	}
	reporter := observability.NewSerialized(observer, runtimeTraceQueueDepth)
	return reporter, &runtimeTraceCloser{reporter: reporter, closer: closer}, nil
}

func closeTraceAfterSetupFailure(closer io.Closer, setupErr error) error {
	if closer == nil {
		return setupErr
	}
	if closeErr := closer.Close(); closeErr != nil {
		return errors.Join(setupErr, fmt.Errorf("close performance trace after setup failure: %w", closeErr))
	}
	return setupErr
}

// runtimeTraceQueueDepth bounds all process-local trace producer handoffs.
// A full queue emits the serialized diagnostic gap rather than delaying a
// terminal, transport, or ACK progress path.
const runtimeTraceQueueDepth = 256

// runtimeTraceCloser owns the two-stage trace shutdown: drain the reporter
// before closing the concrete timestamp/file owner. sync.Once makes every
// deferred and explicit close path safe without closing shared workers twice.
type runtimeTraceCloser struct {
	reporter ports.SerializedRuntimeObserver
	closer   io.Closer
	once     sync.Once
	err      error
}

func (c *runtimeTraceCloser) Close() error {
	if c == nil {
		return nil
	}
	c.once.Do(func() {
		if c.reporter != nil {
			c.reporter.Flush()
			c.reporter.Close()
		}
		if c.closer != nil {
			c.err = c.closer.Close()
		}
	})
	return c.err
}
