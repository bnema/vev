package daemon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These are deliberately source-level RED contracts.  S1 has no coordinator
// yet, so exercising the production transitions would either require test-only
// production shims or fail to compile.  Keeping the contract in the daemon
// package makes the missing fan-in and retained PR #71 hand-off explicit while
// the implementation is introduced in the following GREEN slice.
func TestRenderCoordinatorProducerFanIn(t *testing.T) {
	producers := []string{
		"attention.go", "client.go", "copymode.go", "floating.go", "input.go",
		"palette.go", "pane_actions.go", "picker.go", "prompt.go", "render.go",
		"session.go", "session_back.go",
	}

	for _, name := range producers {
		t.Run(name, func(t *testing.T) {
			body := daemonSource(t, name)
			require.NotContainsf(t, body, "d.paint(", "%s must invalidate the session coordinator rather than compose/send directly", name)
			require.Containsf(t, body, "invalidate(", "%s must publish its state transition to the render coordinator", name)
		})
	}
}

func TestRenderCoordinatorContract(t *testing.T) {
	coordinator := daemonSource(t, "render_coordinator.go")

	for _, contract := range []struct {
		name string
		want []string
	}{
		{
			name: "cap-one wake and sticky reset",
			want: []string{"type renderCoordinator", "invalidate(", "wake", "reset"},
		},
		{
			name: "urgent and adaptive fake-clock deadlines",
			want: []string{"minDebounceInterval", "maxDebounceInterval", "ports.Clock"},
		},
		{
			name: "ack gated composition",
			want: []string{"output", "deferIfAtCapacity", "ack"},
		},
		{
			name: "sync completion and watchdog",
			want: []string{"sync", "watchdog"},
		},
		{
			name: "preview and lifecycle teardown",
			want: []string{"preview", "detach", "replace", "park", "teardown"},
		},
		{
			name: "latest resize metadata has monotonic epoch",
			want: []string{"domain.Size", "resizeEpoch", "resize", "snapshot"},
		},
	} {
		t.Run(contract.name, func(t *testing.T) {
			for _, token := range contract.want {
				require.Containsf(t, coordinator, token, "render coordinator must own %s", contract.name)
			}
		})
	}
}

// PR #71 remains the sole resize timer/generation owner in S1.  Its callback
// must only request coordinator work; it must not become a second compositor.
func TestRenderCoordinatorRetainsPR71ResizeDispatch(t *testing.T) {
	body := daemonSource(t, "render.go")
	for _, symbol := range []string{
		"scheduleResizePaintLocked", "paintForResizeGeneration", "resizePaintGeneration",
		"resizePaintPending", "cancelResizePaint",
	} {
		require.Contains(t, body+daemonSource(t, "client.go"), symbol)
	}

	start := strings.Index(body, "func (d *Daemon) paintForResizeGeneration")
	require.NotEqual(t, -1, start)
	callback := body[start:]
	require.Contains(t, callback, "invalidate(", "the retained PR #71 callback must enter coordinator invalidation")
	require.NotContains(t, callback, "composeClientFrame", "the retained PR #71 callback must not compose directly")
}

func daemonSource(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(".", name))
	require.NoError(t, err)
	return string(body)
}
