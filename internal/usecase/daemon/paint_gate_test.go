package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
)

func requireNoOutputFrame(t *testing.T, sends chan wire.Frame) {
	t.Helper()
	deadline := time.After(50 * time.Millisecond)
	for {
		select {
		case f := <-sends:
			if f.Type == wire.MsgOutput {
				t.Fatalf("unexpected output frame: %+v", f)
			}
		case <-deadline:
			return
		}
	}
}
