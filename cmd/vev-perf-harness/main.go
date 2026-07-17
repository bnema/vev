//go:build linux

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/bnema/vev/pkg/safedir"
)

func main() {
	if err := run(os.Args[1:], defaultHarness()); err != nil {
		fmt.Fprintln(os.Stderr, "vev-perf-harness:", err)
		os.Exit(2)
	}
}

func defaultHarness() *harness {
	return &harness{clock: systemClock{time.Now()}, mkdir: safedir.EnsurePrivate, createRunDir: createExclusiveRunDir, create: func(p string) (*os.File, error) { return os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) }, removeAll: os.RemoveAll, gitSHA: recordedGitSHA}
}
