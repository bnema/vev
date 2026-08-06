package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

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
