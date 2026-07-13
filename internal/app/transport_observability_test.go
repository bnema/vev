package app

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
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
	observer.ObserveRuntime(ports.RuntimeMark{Schema: ports.RuntimeMarkSchema, Component: "daemon", Scenario: "runtime", Run: 1, Kind: ports.RuntimeEmitEnd, Valid: true})
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
