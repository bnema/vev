package broker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

func TestAuthorizeDaemonStart(t *testing.T) {
	dialErr := errors.New("dial failed")
	target := func(mode ports.BrokerDaemonStartMode, launch string) ports.BrokerDialTarget {
		return ports.BrokerDialTarget{StartMode: mode, Policy: ports.BrokerPolicy{Launch: launch}}
	}
	tests := []struct {
		name     string
		target   ports.BrokerDialTarget
		dialErr  error
		absent   bool
		wantCode ports.BrokerErrorCode
		wantSame bool // the dial error is reported unchanged
		wantOK   bool
	}{
		{name: "running daemon is never started", target: target(ports.BrokerDaemonStartIfNeeded, DaemonLaunchAuthority)},
		{name: "existing-only absence is no daemon", target: target(ports.BrokerDaemonExistingOnly, DaemonLaunchAuthority), dialErr: dialErr, absent: true, wantCode: ports.BrokerErrorNoDaemon},
		{name: "existing-only other failure is unchanged", target: target(ports.BrokerDaemonExistingOnly, DaemonLaunchAuthority), dialErr: dialErr, wantSame: true},
		{name: "start-if-needed non-absence is never repaired", target: target(ports.BrokerDaemonStartIfNeeded, DaemonLaunchAuthority), dialErr: dialErr, wantSame: true},
		{name: "policy without launch authority refuses", target: target(ports.BrokerDaemonStartIfNeeded, "sandbox-private"), dialErr: dialErr, absent: true, wantCode: ports.BrokerErrorConflictingPolicy},
		{name: "unknown mode refuses", target: target(ports.BrokerDaemonStartMode(99), DaemonLaunchAuthority), dialErr: dialErr, absent: true, wantCode: ports.BrokerErrorConflictingPolicy},
		{name: "absent daemon with authority may start", target: target(ports.BrokerDaemonStartIfNeeded, DaemonLaunchAuthority), dialErr: dialErr, absent: true, wantOK: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AuthorizeDaemonStart(tt.target, tt.dialErr, tt.absent)
			switch {
			case tt.wantOK:
				require.NoError(t, err)
			case tt.wantSame:
				require.Same(t, tt.dialErr, err)
			case tt.wantCode != 0:
				var typed ports.BrokerError
				require.ErrorAs(t, err, &typed)
				require.Equal(t, tt.wantCode, typed.Code)
			default:
				require.Error(t, err)
			}
		})
	}
}

func TestRequireLocalDaemonTarget(t *testing.T) {
	require.NoError(t, RequireLocalDaemonTarget(ports.BrokerDialTarget{Fence: ports.BrokerEndpointFence{Local: true}}))
	err := RequireLocalDaemonTarget(ports.BrokerDialTarget{})
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorConflictingPolicy, typed.Code)
}
