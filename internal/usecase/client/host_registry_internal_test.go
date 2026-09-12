package client

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// recordingDialer is a typed dialer that never dials: these tests only assert
// how a handoff request is derived from the endpoint binding.
type recordingDialer struct{ name string }

func (d recordingDialer) Dial(context.Context) (ports.ClientConnection, error) {
	return nil, errors.New("recording dialer is never dialed")
}

var _ ports.ClientDialer = recordingDialer{}

func TestRunnerHandoffRequestDerivesFromBindingAndTarget(t *testing.T) {
	source := domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch.example", LifecycleID: domain.SessionLifecycleID{2},
		SessionName: "work", LiveTabID: "tab-1",
	}
	target := protocol.AttachTarget{
		Endpoint: "arch", Session: "work", Intent: protocol.IntentAttach,
		RemoteTarget: &source, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}
	binding := ports.RemoteEndpointBinding{Dialer: recordingDialer{name: "arch"}, Environment: []string{"FOO=1"}}
	var resolvedEndpoint string
	runner := &Runner{
		hostRegistry: internalStubHostRegistry{resolve: func(endpoint string) (ports.RemoteEndpointBinding, error) {
			resolvedEndpoint = endpoint
			return binding, nil
		}},
	}
	dialer, request, err := runner.resolveHandoff(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, "arch", resolvedEndpoint)
	require.Equal(t, binding.Dialer, dialer)
	require.Equal(t, AttachRequest{
		Intent: protocol.IntentAttach, SessionName: "work", Remote: true,
		Environment:       []string{"FOO=1"},
		Origin:            protocol.RouteOriginDiscovery,
		OriginKey:         "arch",
		RemoteTarget:      &source,
		HostLabel:         "arch.example",
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}, request)
}

func TestRunnerHandoffRequestUsesEndpointLabelWithoutSelection(t *testing.T) {
	target := protocol.AttachTarget{
		Endpoint: "arch", Session: "work", Intent: protocol.IntentNew, RequestID: 1,
		EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
	}
	runner := &Runner{
		launchedRemote: true,
		hostRegistry: internalStubHostRegistry{resolve: func(string) (ports.RemoteEndpointBinding, error) {
			return ports.RemoteEndpointBinding{Dialer: recordingDialer{}}, nil
		}},
	}
	_, request, err := runner.resolveHandoff(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, domain.RemoteDisplayOrigin("arch"), request.HostLabel)
	require.Equal(t, protocol.EnvironmentPolicyClientOwned, request.EnvironmentPolicy)
}

// TestRunnerHandoffEnvironmentPolicy pins the picker environment rule the
// composition root used to own: a route chosen from a locally launched client
// has no explicit remote selection, so its environment belongs to the daemon.
func TestRunnerHandoffEnvironmentPolicy(t *testing.T) {
	source := &domain.RemoteSessionTarget{Endpoint: "arch", DisplayOrigin: "arch", SessionName: "work"}
	tests := []struct {
		name           string
		launchedRemote bool
		target         protocol.AttachTarget
		want           protocol.EnvironmentPolicy
	}{
		{
			name:   "local picker attach is daemon-owned",
			target: protocol.AttachTarget{Endpoint: "arch", Session: "work", Intent: protocol.IntentAttach, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned},
			want:   protocol.EnvironmentPolicyDaemonOwned,
		},
		{
			name:   "local picker create keeps the daemon policy",
			target: protocol.AttachTarget{Endpoint: "arch", Session: "work", Intent: protocol.IntentNew, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned},
			want:   protocol.EnvironmentPolicyDaemonOwned,
		},
		{
			name:   "local picker with a remote selection keeps its policy",
			target: protocol.AttachTarget{Endpoint: "arch", Session: "work", Intent: protocol.IntentAttach, RemoteTarget: source, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned},
			want:   protocol.EnvironmentPolicyDaemonOwned,
		},
		{
			name:           "directly launched remote keeps the client policy",
			launchedRemote: true,
			target:         protocol.AttachTarget{Endpoint: "arch", Session: "work", Intent: protocol.IntentAttach, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned},
			want:           protocol.EnvironmentPolicyClientOwned,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &Runner{launchedRemote: tt.launchedRemote}
			require.Equal(t, tt.want, runner.handoffEnvironmentPolicy(tt.target))
		})
	}
}

// TestRunnerValidatesHandoffTargetsBeforeResolving pins that a malformed
// daemon-authored target never reaches registry policy: resolution is not even
// attempted.
func TestRunnerValidatesHandoffTargetsBeforeResolving(t *testing.T) {
	mismatched := domain.RemoteSessionTarget{
		Endpoint: "other", DisplayOrigin: "other", LifecycleID: domain.SessionLifecycleID{3}, SessionName: "elsewhere",
	}
	tests := []struct {
		name   string
		target protocol.AttachTarget
	}{
		{name: "invalid endpoint", target: protocol.AttachTarget{Endpoint: "bad host", Session: "work", Intent: protocol.IntentAttach}},
		{name: "invalid session name", target: protocol.AttachTarget{Endpoint: "arch", Session: "bad name", Intent: protocol.IntentAttach}},
		{name: "selection identity mismatch", target: protocol.AttachTarget{Endpoint: "arch", Session: "work", Intent: protocol.IntentAttach, RemoteTarget: &mismatched, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}},
		{name: "selection without daemon-owned environment", target: protocol.AttachTarget{Endpoint: "arch", Session: "work", Intent: protocol.IntentAttach, RemoteTarget: &mismatched, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolved := false
			runner := &Runner{hostRegistry: internalStubHostRegistry{resolve: func(string) (ports.RemoteEndpointBinding, error) {
				resolved = true
				return ports.RemoteEndpointBinding{Dialer: recordingDialer{}}, nil
			}}}
			_, _, err := runner.resolveHandoff(context.Background(), tt.target)
			require.Error(t, err)
			require.False(t, resolved, "an invalid target must be refused before endpoint policy runs")
		})
	}
}

func TestRunnerHandoffReportsResolutionFailure(t *testing.T) {
	want := errors.New("launch policy refused this endpoint")
	runner := &Runner{hostRegistry: internalStubHostRegistry{resolve: func(string) (ports.RemoteEndpointBinding, error) {
		return ports.RemoteEndpointBinding{}, want
	}}}
	_, _, err := runner.resolveHandoff(context.Background(), protocol.AttachTarget{
		Endpoint: "arch", Session: "work", Intent: protocol.IntentAttach,
	})
	require.ErrorIs(t, err, want)
}
