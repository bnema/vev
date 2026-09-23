//go:build linux

package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/bnema/vev/internal/adapters/brokeripc"
)

// runKillBroker signals only the process proven by kernel credentials to own
// the current broker socket. Dialing does not launch a broker or mutate state.
func runKillBroker(ctx context.Context) error {
	path := brokeripc.SocketPath(productionBrokerLayout().Runtime)
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
			fmt.Println("no broker running")
			return nil
		}
		return fmt.Errorf("vev: connect broker to stop: %w", err)
	}
	defer func() { _ = conn.Close() }()
	unix, ok := conn.(*net.UnixConn)
	if !ok {
		return errors.New("vev: broker endpoint is not a Unix socket")
	}
	raw, err := unix.SyscallConn()
	if err != nil {
		return err
	}
	var peer *syscall.Ucred
	var peerErr error
	if err := raw.Control(func(fd uintptr) {
		peer, peerErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return err
	}
	if peerErr != nil {
		return peerErr
	}
	if peer == nil || peer.Uid != uint32(os.Geteuid()) || peer.Pid <= 1 || int(peer.Pid) == os.Getpid() {
		return errors.New("vev: broker peer identity is invalid")
	}
	if err := syscall.Kill(int(peer.Pid), syscall.SIGTERM); err != nil {
		return fmt.Errorf("vev: stop broker: %w", err)
	}
	// Report success only once the process is gone, so a command that follows
	// immediately never reaches the draining broker.
	if err := waitProcessExit(ctx, int(peer.Pid), brokerStopTimeout); err != nil {
		return fmt.Errorf("vev: stop broker (pid %d): %w", peer.Pid, err)
	}
	fmt.Println("stopped broker")
	return nil
}

// brokerStopTimeout bounds how long kill --broker waits for an orderly exit.
const brokerStopTimeout = 5 * time.Second

// waitProcessExit polls until pid no longer exists. The broker is reparented
// at launch, so it is never this process' zombie child.
func waitProcessExit(ctx context.Context, pid int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("still running after %s: %w", timeout, ctx.Err())
		case <-ticker.C:
		}
	}
}
