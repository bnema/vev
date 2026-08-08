package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestMalformedAttachRequestsNeverDial(t *testing.T) {
	tests := []struct {
		name    string
		request AttachRequest
	}{
		{name: "missing session name", request: AttachRequest{Intent: ports.IntentAttach}},
		{name: "unsafe session name", request: AttachRequest{Intent: ports.IntentAttach, SessionName: "bad name"}},
		{name: "daemon-owned without remote", request: AttachRequest{Intent: ports.IntentAttach, SessionName: "main", EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned}},
		{name: "ephemeral with navigation capability", request: AttachRequest{Intent: ports.IntentEphemeral, NavigationCapabilities: ports.NavigationCapabilityHomePicker}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No EXPECT: any Dial call fails the test.
			dialer := portsmocks.NewMockDialer(t)
			err := NewRunner(Dependencies{Dialer: dialer}).Run(context.Background(), tt.request)
			require.Error(t, err)
		})
	}
}

func TestValidateAttachRequestRequiresDaemonOwnedEnvironmentForRemoteTarget(t *testing.T) {
	var lifecycle domain.SessionLifecycleID
	lifecycle[0] = 1
	target := &domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}

	tests := []struct {
		name   string
		policy ports.EnvironmentPolicy
		wantOK bool
	}{
		{name: "daemon owned", policy: ports.EnvironmentPolicyDaemonOwned, wantOK: true},
		{name: "client owned", policy: ports.EnvironmentPolicyClientOwned},
		{name: "unknown", policy: ports.EnvironmentPolicy(99)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateAttachRequest(AttachRequest{
				Intent: ports.IntentAttach, SessionName: target.SessionName,
				RemoteTarget: target, EnvironmentPolicy: test.policy,
			})
			if test.wantOK {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, "vev: remote attach target requires daemon-owned environment")
		})
	}
}
