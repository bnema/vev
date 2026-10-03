package app

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestParseHostArgs(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		wantAction    string
		wantTarget    string
		wantTransport string
		wantErr       string
		nonUsageErr   bool
	}{
		{name: "add defaults to quic", args: []string{"add", "arch"}, wantAction: "add", wantTarget: "arch", wantTransport: "quic"},
		{name: "add explicit quic", args: []string{"add", "--transport", "quic", "arch"}, wantAction: "add", wantTarget: "arch", wantTransport: "quic"},
		{name: "add ssh", args: []string{"add", "--transport", "ssh", "user@arch"}, wantAction: "add", wantTarget: "user@arch", wantTransport: "ssh"},
		{name: "add ssh equals form", args: []string{"add", "--transport=ssh", "arch"}, wantAction: "add", wantTarget: "arch", wantTransport: "ssh"},
		{name: "add flag after target", args: []string{"add", "arch", "--transport", "ssh"}, wantAction: "add", wantTarget: "arch", wantTransport: "ssh"},
		{name: "add invalid value", args: []string{"add", "--transport", "stdio", "arch"}, wantErr: `want "quic" or "ssh"`},
		{name: "add invalid equals value", args: []string{"add", "--transport=udp", "arch"}, wantErr: `want "quic" or "ssh"`},
		{name: "add empty equals value", args: []string{"add", "--transport=", "arch"}, wantErr: `want "quic" or "ssh"`},
		{name: "add missing value", args: []string{"add", "arch", "--transport"}, wantErr: "requires a value"},
		{name: "add repeated flag", args: []string{"add", "--transport", "ssh", "--transport", "quic", "arch"}, wantErr: "once"},
		{name: "add unknown flag", args: []string{"add", "--bogus", "arch"}, wantErr: "unknown flag"},
		{name: "add missing target", args: []string{"add"}, wantErr: "requires a host target"},
		{name: "add flag without target", args: []string{"add", "--transport", "ssh"}, wantErr: "requires a host target"},
		{name: "add extra target", args: []string{"add", "--transport", "ssh", "arch", "extra"}, wantErr: "exactly one host target"},
		{name: "add invalid target", args: []string{"add", "--transport", "ssh", " bad"}, wantErr: "host", nonUsageErr: true},
		{name: "rm", args: []string{"rm", "arch"}, wantAction: "rm", wantTarget: "arch"},
		{name: "rm rejects transport", args: []string{"rm", "--transport", "ssh", "arch"}, wantErr: "unknown flag"},
		{name: "rm rejects transport after target", args: []string{"rm", "arch", "--transport=ssh"}, wantErr: "exactly one host target"},
		{name: "rm missing target", args: []string{"rm"}, wantErr: "requires a host target"},
		{name: "list", args: []string{"list"}, wantAction: "list"},
		{name: "list rejects transport", args: []string{"list", "--transport", "ssh"}, wantErr: "unknown flag"},
		{name: "list extra", args: []string{"list", "arch"}, wantErr: "accepts no arguments"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseHostArgs(tt.args)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
				_, usage := errors.AsType[*usageError](err)
				require.Equal(t, !tt.nonUsageErr, usage, "error type %T", err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, kindHost, got.kind)
			require.Equal(t, tt.wantAction, got.hostAction)
			require.Equal(t, tt.wantTarget, got.hostTarget)
			require.Equal(t, tt.wantTransport, got.hostTransport)
		})
	}
}

func TestHostAddPolicyFollowsTransport(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantPolicy ports.BrokerPolicy
	}{
		{name: "default", args: []string{"add", "arch"}, wantPolicy: remoteBrokerPolicy(hostTransportQUIC)},
		{name: "quic", args: []string{"add", "--transport", "quic", "arch"}, wantPolicy: remoteBrokerPolicy(hostTransportQUIC)},
		{name: "ssh", args: []string{"add", "--transport", "ssh", "arch"}, wantPolicy: remoteBrokerPolicy(hostTransportSSH)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := parseHostArgs(tt.args)
			require.NoError(t, err)
			service := portsmocks.NewMockBrokerService(t)
			service.EXPECT().AddHost(mock.Anything, "arch", tt.wantPolicy).Return(domain.RemoteRegistration{}, nil).Once()
			service.EXPECT().Close().Return(nil).Once()
			deps := remoteHostDeps{connect: func(context.Context) (ports.BrokerService, error) { return service, nil }}
			require.NoError(t, runHostCommand(context.Background(), cmd, deps))
		})
	}
}

func TestHostAddReportsTransportConflict(t *testing.T) {
	// ipcConflict is the shape a conflict has after crossing broker IPC: only
	// the code survives, not the sentinel.
	ipcConflict := ports.BrokerError{Code: ports.BrokerErrorHostConflict, Text: "host registration conflict"}
	storedSSH := ports.BrokerSnapshot{Daemons: []ports.BrokerDaemonObservation{
		{Endpoint: "arch", Policy: remoteBrokerPolicy(hostTransportSSH)},
	}}
	storedLegacy := ports.BrokerSnapshot{Daemons: []ports.BrokerDaemonObservation{
		{Endpoint: "arch", Policy: ports.BrokerPolicy{Transport: "stdio"}},
	}}
	tests := []struct {
		name     string
		addErr   error
		snapshot ports.BrokerSnapshot
		wantHint bool
	}{
		{name: "in-process sentinel with other transport", addErr: ports.ErrBrokerHostConflict, snapshot: storedSSH, wantHint: true},
		{name: "IPC code with other transport", addErr: ipcConflict, snapshot: storedSSH, wantHint: true},
		{name: "same transport conflicts elsewhere", addErr: ipcConflict, snapshot: ports.BrokerSnapshot{Daemons: []ports.BrokerDaemonObservation{
			{Endpoint: "arch", Policy: remoteBrokerPolicy(hostTransportQUIC)},
		}}},
		{name: "host not in snapshot", addErr: ipcConflict, snapshot: ports.BrokerSnapshot{}},
		{name: "unrecognised stored transport", addErr: ipcConflict, snapshot: storedLegacy},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := portsmocks.NewMockBrokerService(t)
			service.EXPECT().AddHost(mock.Anything, "arch", remoteBrokerPolicy(hostTransportQUIC)).Return(domain.RemoteRegistration{}, tt.addErr).Once()
			service.EXPECT().Snapshot().Return(tt.snapshot).Once()
			service.EXPECT().Close().Return(nil).Once()
			deps := remoteHostDeps{connect: func(context.Context) (ports.BrokerService, error) { return service, nil }}
			err := runHostCommand(context.Background(), command{kind: kindHost, hostAction: hostActionAdd, hostTarget: "arch", hostTransport: hostTransportQUIC}, deps)
			require.ErrorIs(t, err, tt.addErr)
			if !tt.wantHint {
				require.NotContains(t, err.Error(), "vev host rm")
				return
			}
			require.Contains(t, err.Error(), `transport "ssh"`)
			require.Contains(t, err.Error(), "vev host rm arch")
		})
	}
}
