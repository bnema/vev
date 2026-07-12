package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

func requireNoOutputFrame(t *testing.T, sends chan ports.Frame) {
	t.Helper()
	deadline := time.After(50 * time.Millisecond)
	for {
		select {
		case f := <-sends:
			if f.Type == ports.MsgOutput {
				t.Fatalf("unexpected output frame: %+v", f)
			}
		case <-deadline:
			return
		}
	}
}
