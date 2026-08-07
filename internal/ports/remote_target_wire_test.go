package ports

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/bnema/vev/internal/domain"
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
			msg := Hello{
				Version:           ProtocolVersion,
				Intent:            IntentAttach,
				Name:              target.SessionName,
				Size:              domain.Size{Cols: 80, Rows: 24},
				RemoteTarget:      target,
				EnvironmentPolicy: EnvironmentPolicyDaemonOwned,
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
			if got.EnvironmentPolicy != EnvironmentPolicyDaemonOwned {
				t.Fatalf("environment policy = %d, want daemon-owned", got.EnvironmentPolicy)
			}

			attachPayload := MarshalAttachTarget(AttachTarget{
				Endpoint:          target.Endpoint,
				Session:           target.SessionName,
				Intent:            IntentAttach,
				RemoteTarget:      target,
				EnvironmentPolicy: EnvironmentPolicyDaemonOwned,
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

func TestProxiedHelloGoldenAndStrict(t *testing.T) {
	target := testRemoteTarget(false)
	msg := Hello{
		Version: ProtocolVersion, Intent: IntentAttach, RenderMode: RenderModeProxiedContent,
		ClientID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, ResumeToken: 0x0102030405060708,
		Name: target.SessionName, Size: domain.Size{Cols: 80, Rows: 24}, MaxOutputInFlight: 2,
		RemoteTarget: target, EnvironmentPolicy: EnvironmentPolicyDaemonOwned,
	}
	want := []byte{
		0, 25, IntentAttach, byte(RenderModeProxiedContent),
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		1, 2, 3, 4, 5, 6, 7, 8,
		0, 4, 'w', 'o', 'r', 'k', 0, 80, 0, 24, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0,
		1,
		0, 15, 'b', 'u', 'i', 'l', 'd', '@', 'm', 'u', 'l', 'e', ':', '2', '2', '2', '2',
		0, 9, 'm', 'u', 'l', 'e', ':', '2', '2', '2', '2',
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		0, 4, 'w', 'o', 'r', 'k', 0, 0, 5, 't', 'a', 'b', '-', '3', 0, byte(EnvironmentPolicyDaemonOwned),
	}
	payload := MarshalHello(msg)
	if !bytes.Equal(payload, want) {
		t.Fatalf("proxied Hello bytes = %x, want %x", payload, want)
	}
	decoded, err := UnmarshalHello(payload)
	if err != nil || !reflect.DeepEqual(decoded, msg) || decoded.RemoteTarget == msg.RemoteTarget {
		t.Fatalf("proxied Hello = %#v, error %v", decoded, err)
	}
	assertAllPrefixesFail(t, payload, UnmarshalHello)
	assertTrailingGarbageFails(t, payload, UnmarshalHello)

	if MarshalHello(Hello{Version: ProtocolVersion, Intent: IntentAttach, RenderMode: RenderModeProxiedContent, Size: domain.Size{Cols: 80, Rows: 24}}) != nil {
		t.Fatal("proxied Hello without exact target marshaled")
	}
	malformed := append([]byte(nil), payload...)
	malformed[3] = 2
	if _, err := UnmarshalHello(malformed); err == nil {
		t.Fatal("unknown render mode decoded")
	}
}

func TestRemoteTargetWireRejectsClientOwnedEnvironmentPolicy(t *testing.T) {
	target := testRemoteTarget(false)
	tests := []struct {
		name        string
		validate    func() error
		marshalBad  func() []byte
		marshalGood func() []byte
		unmarshal   func([]byte) error
	}{
		{
			name: "hello",
			validate: func() error {
				return ValidateHello(Hello{
					Version: ProtocolVersion, Intent: IntentAttach,
					Name: target.SessionName, Size: domain.Size{Cols: 80, Rows: 24},
					RemoteTarget: target, EnvironmentPolicy: EnvironmentPolicyClientOwned,
				})
			},
			marshalBad: func() []byte {
				return MarshalHello(Hello{
					Version: ProtocolVersion, Intent: IntentAttach,
					Name: target.SessionName, Size: domain.Size{Cols: 80, Rows: 24},
					RemoteTarget: target, EnvironmentPolicy: EnvironmentPolicyClientOwned,
				})
			},
			marshalGood: func() []byte {
				return MarshalHello(Hello{
					Version: ProtocolVersion, Intent: IntentAttach,
					Name: target.SessionName, Size: domain.Size{Cols: 80, Rows: 24},
					RemoteTarget: target, EnvironmentPolicy: EnvironmentPolicyDaemonOwned,
				})
			},
			unmarshal: func(payload []byte) error { _, err := UnmarshalHello(payload); return err },
		},
		{
			name: "attach target",
			validate: func() error {
				return ValidateAttachTarget(AttachTarget{
					Endpoint: target.Endpoint, Session: target.SessionName, Intent: IntentAttach,
					RemoteTarget: target, EnvironmentPolicy: EnvironmentPolicyClientOwned,
				})
			},
			marshalBad: func() []byte {
				return MarshalAttachTarget(AttachTarget{
					Endpoint: target.Endpoint, Session: target.SessionName, Intent: IntentAttach,
					RemoteTarget: target, EnvironmentPolicy: EnvironmentPolicyClientOwned,
				})
			},
			marshalGood: func() []byte {
				return MarshalAttachTarget(AttachTarget{
					Endpoint: target.Endpoint, Session: target.SessionName, Intent: IntentAttach,
					RemoteTarget: target, EnvironmentPolicy: EnvironmentPolicyDaemonOwned,
				})
			},
			unmarshal: func(payload []byte) error { _, err := UnmarshalAttachTarget(payload); return err },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.validate(); err == nil {
				t.Fatal("direct validation accepted client-owned policy for remote target")
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
			payload[len(payload)-1] = byte(EnvironmentPolicyClientOwned)
			if err := tt.unmarshal(payload); err == nil {
				t.Fatal("unmarshal accepted client-owned policy for remote target")
			}
		})
	}
}

func TestTargetWireIncludesRequiredV25Section(t *testing.T) {
	want := []byte{0, 4, 'h', 'o', 's', 't', 0, 4, 'w', 'o', 'r', 'k', IntentAttach, 0, 0}
	got := MarshalAttachTarget(AttachTarget{Endpoint: "host", Session: "work", Intent: IntentAttach})
	if !bytes.Equal(got, want) {
		t.Fatalf("target bytes = %x, want %x", got, want)
	}
}
