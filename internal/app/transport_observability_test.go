package app

import (
	"os"
	"strings"
	"testing"
)

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
