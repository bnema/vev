package app

import (
	"os"
	"os/signal"
	"syscall"
)

// The first signal.Notify call of a process starts the runtime's signal-mask
// goroutine. Started inside a synctest bubble it would be bubbled and the
// runtime aborts ("select on synctest channel from outside bubble"), so the
// goroutine is started here, outside any bubble, before a test runs.
func init() {
	warm := make(chan os.Signal, 1)
	signal.Notify(warm, syscall.SIGUSR2)
	signal.Stop(warm)
}
