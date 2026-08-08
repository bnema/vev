package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

type validationCountingDialer struct{ calls int }

func (d *validationCountingDialer) Dial(context.Context) (ports.Transport, error) {
	d.calls++
	return nil, nil
}

func TestMalformedAttachRequestsNeverDial(t *testing.T) {
	tests := []AttachRequest{
		{Intent: ports.IntentAttach},
		{Intent: ports.IntentAttach, SessionName: "bad name"},
		{Intent: ports.IntentAttach, SessionName: "main", EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned},
		{Intent: ports.IntentEphemeral, NavigationCapabilities: ports.NavigationCapabilityHomePicker},
	}
	for i, request := range tests {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			dialer := &validationCountingDialer{}
			err := NewRunner(Dependencies{Dialer: dialer}).Run(context.Background(), request)
			require.Error(t, err)
			require.Zero(t, dialer.calls)
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
