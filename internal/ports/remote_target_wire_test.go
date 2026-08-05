package ports

import (
	"bytes"
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
		t.Run(map[bool]string{false: "live", true: "stopped"}[stopped], func(t *testing.T) {
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

func TestRemoteTargetWireRejectsTruncationAndTrailingData(t *testing.T) {
	target := testRemoteTarget(true)
	payload := MarshalAttachTarget(AttachTarget{
		Endpoint:          target.Endpoint,
		Session:           target.SessionName,
		Intent:            IntentAttach,
		RemoteTarget:      target,
		EnvironmentPolicy: EnvironmentPolicyDaemonOwned,
	})
	legacyLen := len(MarshalAttachTarget(AttachTarget{Endpoint: target.Endpoint, Session: target.SessionName, Intent: IntentAttach}))
	for i := 0; i < len(payload); i++ {
		if i == legacyLen {
			// The extension is optional for old peers; the complete legacy
			// prefix is therefore a valid count-only target by design.
			continue
		}
		if _, err := UnmarshalAttachTarget(payload[:i]); err == nil {
			t.Fatalf("prefix %d unexpectedly decoded", i)
		}
	}
	trailing := append(append([]byte(nil), payload...), 0xff)
	if _, err := UnmarshalAttachTarget(trailing); err == nil {
		t.Fatal("trailing garbage unexpectedly decoded")
	}
}

func TestLegacyTargetWireBytesRemainUnchanged(t *testing.T) {
	want := []byte{0, 4, 'h', 'o', 's', 't', 0, 4, 'w', 'o', 'r', 'k', IntentAttach}
	got := MarshalAttachTarget(AttachTarget{Endpoint: "host", Session: "work", Intent: IntentAttach})
	if !bytes.Equal(got, want) {
		t.Fatalf("legacy target bytes = %x, want %x", got, want)
	}
}
