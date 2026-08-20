package term

import (
	"os"

	"github.com/bnema/vev/internal/domain"
)

// resizeLoop watches sig for signal notifications (SIGWINCH in
// production) and emits the coalesced terminal geometry on out: a burst of
// signals collapses to a single emitted value, since any signals still
// queued once the first is observed are drained before querying the
// geometry. getGeometry is called to resolve the value to emit; an error is
// treated as "no event this round" and the loop continues waiting.
//
// The loop exits and closes out when quit is closed.
func resizeLoop(sig <-chan os.Signal, out chan<- domain.Geometry, quit <-chan struct{}, getGeometry func() (domain.Geometry, error)) {
	defer close(out)
	for {
		select {
		case <-quit:
			return
		case <-sig:
			drainSignals(sig)
			geometry, err := getGeometry()
			if err != nil {
				continue
			}
			select {
			case out <- geometry:
			case <-quit:
				return
			}
		}
	}
}

// drainSignals removes any currently queued signals from sig without
// blocking. Called right after receiving one signal, it collapses a
// burst that arrived (or queued up) concurrently into that single
// wakeup.
func drainSignals(sig <-chan os.Signal) {
	for {
		select {
		case <-sig:
		default:
			return
		}
	}
}
