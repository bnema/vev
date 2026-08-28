package ports

import (
	"bytes"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func testRemoteTarget(stopped bool) *domain.RemoteSessionTarget {
	var lifecycle domain.SessionLifecycleID
	for i := range lifecycle {
		lifecycle[i] = byte(i + 1)
	}
	target := &domain.RemoteSessionTarget{
		Endpoint:      "build@mule:2222",
		DisplayOrigin: "mule:2222",
		LifecycleID:   lifecycle,
		SessionName:   "work",
		Stopped:       stopped,
	}
	if stopped {
		target.StoppedTab = domain.NewOrdinalTabSelector(2, "", 4)
	} else {
		target.LiveTabID = "tab-3"
	}
	return target
}

func TestRemoteTargetWireRoundTripPreservesExactSelector(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		name := "live"
		if stopped {
			name = "stopped"
		}
		t.Run(name, func(t *testing.T) {
			target := testRemoteTarget(stopped)
			msg := protocol.Hello{
				Version:           protocol.Version,
				Intent:            protocol.IntentAttach,
				Name:              target.SessionName,
				Size:              domain.Size{Cols: 80, Rows: 24},
				RemoteTarget:      target,
				EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
			}
			payload := MarshalHello(msg)
			if payload == nil {
				t.Fatal("MarshalHello returned nil")
			}
			got, err := UnmarshalHello(payload)
			if err != nil {
				t.Fatal(err)
			}
			if got.RemoteTarget == nil || *got.RemoteTarget != *target {
				t.Fatalf("remote target = %#v, want %#v", got.RemoteTarget, target)
			}
			if got.EnvironmentPolicy != protocol.EnvironmentPolicyDaemonOwned {
				t.Fatalf("environment policy = %d, want daemon-owned", got.EnvironmentPolicy)
			}

			attachPayload := MarshalAttachTarget(protocol.AttachTarget{
				Endpoint:          target.Endpoint,
				Session:           target.SessionName,
				Intent:            protocol.IntentAttach,
				RemoteTarget:      target,
				EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
			})
			if attachPayload == nil {
				t.Fatal("MarshalAttachTarget returned nil")
			}
			attach, err := UnmarshalAttachTarget(attachPayload)
			if err != nil {
				t.Fatal(err)
			}
			if attach.RemoteTarget == nil || *attach.RemoteTarget != *target {
				t.Fatalf("attach target = %#v, want %#v", attach.RemoteTarget, target)
			}
		})
	}
}

func TestRemoteTargetWireRejectsClientOwnedEnvironmentPolicy(t *testing.T) {
	target := testRemoteTarget(false)
	tests := []struct {
		name         string
		validate     func() error
		marshalBad   func() []byte
		marshalGood  func() []byte
		unmarshal    func([]byte) error
		policyOffset int
		wantErr      error
	}{
		{
			name: "hello",
			validate: func() error {
				return protocol.ValidateHello(protocol.Hello{
					Version: protocol.Version, Intent: protocol.IntentAttach,
					Name: target.SessionName, Size: domain.Size{Cols: 80, Rows: 24},
					RemoteTarget: target, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
				})
			},
			marshalBad: func() []byte {
				return MarshalHello(protocol.Hello{
					Version: protocol.Version, Intent: protocol.IntentAttach,
					Name: target.SessionName, Size: domain.Size{Cols: 80, Rows: 24},
					RemoteTarget: target, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
				})
			},
			marshalGood: func() []byte {
				return MarshalHello(protocol.Hello{
					Version: protocol.Version, Intent: protocol.IntentAttach,
					Name: target.SessionName, Size: domain.Size{Cols: 80, Rows: 24},
					RemoteTarget: target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
				})
			},
			unmarshal:    func(payload []byte) error { _, err := UnmarshalHello(payload); return err },
			policyOffset: 10,
			wantErr:      protocol.ErrInvalidHello,
		},
		{
			name: "attach target",
			validate: func() error {
				return protocol.ValidateAttachTarget(protocol.AttachTarget{
					Endpoint: target.Endpoint, Session: target.SessionName, Intent: protocol.IntentAttach,
					RemoteTarget: target, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
				})
			},
			marshalBad: func() []byte {
				return MarshalAttachTarget(protocol.AttachTarget{
					Endpoint: target.Endpoint, Session: target.SessionName, Intent: protocol.IntentAttach,
					RemoteTarget: target, EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
				})
			},
			marshalGood: func() []byte {
				return MarshalAttachTarget(protocol.AttachTarget{
					Endpoint: target.Endpoint, Session: target.SessionName, Intent: protocol.IntentAttach,
					RemoteTarget: target, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
				})
			},
			unmarshal:    func(payload []byte) error { _, err := UnmarshalAttachTarget(payload); return err },
			policyOffset: 3,
			wantErr:      protocol.ErrInvalidAttachTarget,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.validate(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("direct validation error = %v, want %v", err, tt.wantErr)
			}
			if payload := tt.marshalBad(); payload != nil {
				t.Fatalf("marshal accepted client-owned policy: %x", payload)
			}
			payload := tt.marshalGood()
			if payload == nil {
				t.Fatal("failed to marshal daemon-owned control payload")
			}
			for i := range len(payload) {
				if err := tt.unmarshal(payload[:i]); err == nil {
					t.Fatalf("prefix %d unexpectedly decoded", i)
				}
			}
			trailing := append(append([]byte(nil), payload...), 0xff)
			if err := tt.unmarshal(trailing); err == nil {
				t.Fatal("trailing garbage unexpectedly decoded")
			}
			payload[len(payload)-tt.policyOffset] = byte(protocol.EnvironmentPolicyClientOwned)
			if err := tt.unmarshal(payload); !errors.Is(err, tt.wantErr) {
				t.Fatalf("mutated policy error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestAttachTargetRejectsResumeIntent(t *testing.T) {
	tests := []protocol.AttachTarget{
		{Endpoint: "host", Session: "work", Intent: protocol.IntentResume},
		{
			Endpoint:          "build@mule:2222",
			Session:           "work",
			Intent:            protocol.IntentResume,
			RemoteTarget:      testRemoteTarget(false),
			EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
		},
	}
	for _, target := range tests {
		if err := protocol.ValidateAttachTarget(target); !errors.Is(err, protocol.ErrInvalidAttachTarget) {
			t.Fatalf("ValidateAttachTarget(%#v) error = %v, want %v", target, err, protocol.ErrInvalidAttachTarget)
		}
		if got := MarshalAttachTarget(target); got != nil {
			t.Fatalf("MarshalAttachTarget(%#v) = %x, want nil", target, got)
		}
	}
}

func TestTargetWireIncludesRequiredV27Section(t *testing.T) {
	want := []byte{0, 4, 'h', 'o', 's', 't', 0, 4, 'w', 'o', 'r', 'k', protocol.IntentAttach, 0, 0, 0, 0}
	got := MarshalAttachTarget(protocol.AttachTarget{Endpoint: "host", Session: "work", Intent: protocol.IntentAttach})
	if !bytes.Equal(got, want) {
		t.Fatalf("target bytes = %x, want %x", got, want)
	}
}
