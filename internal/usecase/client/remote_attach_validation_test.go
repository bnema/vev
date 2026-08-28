package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func TestMalformedAttachRequestsNeverDial(t *testing.T) {
	tests := []struct {
		name    string
		request AttachRequest
	}{
		{name: "missing session name", request: AttachRequest{Intent: protocol.IntentAttach}},
		{name: "unsafe session name", request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "bad name"}},
		{name: "daemon-owned without remote", request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "main", EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}},
		{name: "ephemeral with navigation capability", request: AttachRequest{Intent: protocol.IntentEphemeral, NavigationCapabilities: protocol.NavigationCapabilityHomePicker}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No EXPECT: any Dial call fails the test.
			dialer := newMockClientDialer(t)
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
		policy protocol.EnvironmentPolicy
		wantOK bool
	}{
		{name: "daemon owned", policy: protocol.EnvironmentPolicyDaemonOwned, wantOK: true},
		{name: "client owned", policy: protocol.EnvironmentPolicyClientOwned},
		{name: "unknown", policy: protocol.EnvironmentPolicy(99)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateAttachRequest(AttachRequest{
				Intent: protocol.IntentAttach, SessionName: target.SessionName,
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
