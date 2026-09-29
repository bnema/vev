package broker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

func daemonStartTarget(mode ports.BrokerDaemonStartMode, launch string) ports.BrokerDialTarget {
	return ports.BrokerDialTarget{StartMode: mode, Policy: ports.BrokerPolicy{Launch: launch}}
}

func requireBrokerCode(t *testing.T, err error, code ports.BrokerErrorCode) {
	t.Helper()
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, code, typed.Code)
}

func TestClassifyDaemonDialFailure(t *testing.T) {
	dialErr := errors.New("dial failed")
	tests := []struct {
		name     string
		mode     ports.BrokerDaemonStartMode
		dialErr  error
		absent   bool
		wantCode ports.BrokerErrorCode
		wantSame bool // the dial error is reported unchanged
		wantOK   bool
	}{
		{name: "running daemon is never started", mode: ports.BrokerDaemonStartIfNeeded},
		{name: "existing-only absence is no daemon", mode: ports.BrokerDaemonExistingOnly, dialErr: dialErr, absent: true, wantCode: ports.BrokerErrorNoDaemon},
		{name: "existing-only other failure is unchanged", mode: ports.BrokerDaemonExistingOnly, dialErr: dialErr, wantSame: true},
		{name: "start-if-needed non-absence is never repaired", mode: ports.BrokerDaemonStartIfNeeded, dialErr: dialErr, wantSame: true},
		{name: "start-if-needed absence may continue", mode: ports.BrokerDaemonStartIfNeeded, dialErr: dialErr, absent: true, wantOK: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ClassifyDaemonDialFailure(daemonStartTarget(tt.mode, DaemonLaunchAuthority), tt.dialErr, tt.absent)
			switch {
			case tt.wantOK:
				require.NoError(t, err)
			case tt.wantSame:
				require.Same(t, tt.dialErr, err)
			case tt.wantCode != 0:
				requireBrokerCode(t, err, tt.wantCode)
			default:
				require.Error(t, err)
			}
		})
	}
}

func TestAuthorizeDaemonLaunch(t *testing.T) {
	tests := []struct {
		name   string
		target ports.BrokerDialTarget
		wantOK bool
	}{
		{name: "start-if-needed with launch authority", target: daemonStartTarget(ports.BrokerDaemonStartIfNeeded, DaemonLaunchAuthority), wantOK: true},
		{name: "policy without launch authority refuses", target: daemonStartTarget(ports.BrokerDaemonStartIfNeeded, "sandbox-private")},
		{name: "unknown mode refuses", target: daemonStartTarget(ports.BrokerDaemonStartMode(99), DaemonLaunchAuthority)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AuthorizeDaemonLaunch(tt.target)
			if tt.wantOK {
				require.NoError(t, err)
				return
			}
			requireBrokerCode(t, err, ports.BrokerErrorConflictingPolicy)
		})
	}
}

func TestRequireLocalDaemonTarget(t *testing.T) {
	require.NoError(t, RequireLocalDaemonTarget(ports.BrokerDialTarget{Fence: ports.BrokerEndpointFence{Local: true}}))
	requireBrokerCode(t, RequireLocalDaemonTarget(ports.BrokerDialTarget{}), ports.BrokerErrorConflictingPolicy)
}
