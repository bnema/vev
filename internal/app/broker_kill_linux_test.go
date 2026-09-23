//go:build linux

package app

import (
	"context"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitProcessExit(t *testing.T) {
	tests := []struct {
		name    string
		signal  bool
		wantErr bool
	}{
		{name: "terminated process is reported gone", signal: true},
		{name: "live process times out", signal: false, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command("sleep", "30")
			require.NoError(t, cmd.Start())
			// Reap the child so it never lingers as a zombie that kill(pid, 0)
			// would still report as present.
			reaped := make(chan struct{})
			go func() { _ = cmd.Wait(); close(reaped) }()
			t.Cleanup(func() {
				_ = cmd.Process.Kill()
				<-reaped
			})
			if tt.signal {
				require.NoError(t, syscall.Kill(cmd.Process.Pid, syscall.SIGTERM))
			}
			err := waitProcessExit(context.Background(), cmd.Process.Pid, 300*time.Millisecond)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
