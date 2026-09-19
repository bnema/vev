package ports

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/stretchr/testify/require"
)

func TestBrokerDaemonObservationValidate(t *testing.T) {
	observed := func(o BrokerDaemonObservation) BrokerDaemonObservation {
		o.Identity = BrokerDaemonIdentity("authed-daemon")
		o.Incarnation = BrokerDaemonIncarnation{9}
		o.ProtocolVersion = protocol.Version
		o.Capabilities = protocol.CapabilityResume | protocol.CapabilityUDP
		return o
	}
	tests := []struct {
		name   string
		mutate func(*BrokerDaemonObservation)
		valid  bool
	}{
		{name: "unobserved remote daemon", mutate: func(*BrokerDaemonObservation) {}, valid: true},
		{name: "unknown availability", mutate: func(o *BrokerDaemonObservation) {
			o.Availability = domain.RemoteAvailabilityUnknown
		}, valid: true},
		{name: "observed daemon", mutate: func(o *BrokerDaemonObservation) { *o = observed(*o) }, valid: true},
		{name: "observed daemon without capabilities", mutate: func(o *BrokerDaemonObservation) {
			*o = observed(*o)
			o.Capabilities = 0
		}, valid: true},
		{name: "observed daemon reports a different protocol version", mutate: func(o *BrokerDaemonObservation) {
			*o = observed(*o)
			o.ProtocolVersion = protocol.Version + 1
			o.Availability = domain.RemoteAvailabilityIncompatible
		}, valid: true},
		{name: "observed identity without protocol version", mutate: func(o *BrokerDaemonObservation) {
			*o = observed(*o)
			o.ProtocolVersion = 0
		}, valid: false},
		{name: "identity without incarnation", mutate: func(o *BrokerDaemonObservation) {
			o.Identity = BrokerDaemonIdentity("authed-daemon")
		}, valid: false},
		{name: "incarnation without identity", mutate: func(o *BrokerDaemonObservation) {
			o.Incarnation = BrokerDaemonIncarnation{9}
		}, valid: false},
		{name: "empty display origin", mutate: func(o *BrokerDaemonObservation) {
			o.DisplayOrigin = ""
		}, valid: false},
		{name: "control characters in display origin", mutate: func(o *BrokerDaemonObservation) {
			o.DisplayOrigin = "bad\x1border"
		}, valid: false},
		{name: "oversized display origin", mutate: func(o *BrokerDaemonObservation) {
			o.DisplayOrigin = strings.Repeat("a", BrokerMaxDisplayOriginBytes+1)
		}, valid: false},
		{name: "missing policy", mutate: func(o *BrokerDaemonObservation) {
			o.Policy = BrokerPolicy{}
		}, valid: false},
		{name: "zero availability is never invented", mutate: func(o *BrokerDaemonObservation) {
			o.Availability = 0
		}, valid: false},
		{name: "failure kind past the closed range", mutate: func(o *BrokerDaemonObservation) {
			o.LastFailure.Kind = domain.RemoteFailureInvalidResponse + 1
		}, valid: false},
		{name: "empty inventory is known inventory", mutate: func(o *BrokerDaemonObservation) {
			o.InventoryKnown = true
		}, valid: true},
		{name: "sessions without known inventory", mutate: func(o *BrokerDaemonObservation) {
			o.Sessions = []catalogue.RemoteCatalogSession{{
				LifecycleID: testBrokerLifecycle(),
				Name:        "work",
				State:       catalogue.RemoteCatalogSessionUp,
				Tabs:        []catalogue.RemoteCatalogTab{},
			}}
		}, valid: false},
		{name: "local daemon without remote authority", mutate: func(o *BrokerDaemonObservation) {
			*o = testLocalBrokerObservation()
		}, valid: true},
		{name: "local daemon carrying an endpoint", mutate: func(o *BrokerDaemonObservation) {
			*o = testLocalBrokerObservation()
			o.Endpoint = "user@arch"
		}, valid: false},
		{name: "local daemon carrying a registration", mutate: func(o *BrokerDaemonObservation) {
			*o = testLocalBrokerObservation()
			o.Registration = testBrokerRegistration("user@arch", 1, 1)
		}, valid: false},
		{name: "remote daemon missing registration", mutate: func(o *BrokerDaemonObservation) {
			o.Registration = domain.RemoteRegistration{}
		}, valid: false},
		{name: "endpoint registration mismatch", mutate: func(o *BrokerDaemonObservation) {
			o.Registration = testBrokerRegistration("user@other", 1, 1)
		}, valid: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obs := testBrokerObservation("user@arch")
			tc.mutate(&obs)
			if (obs.Validate() == nil) != tc.valid {
				t.Fatalf("Validate() = %v, want valid %t", obs.Validate(), tc.valid)
			}
		})
	}
}

// TestBrokerDaemonObservationValidateDisplayOrigin pins the display-origin
// bound and character rules with exact refusals: a bidi control or a Unicode
// line separator is refused as disallowed text, an origin at the byte bound is
// accepted, and one byte over is refused.
func TestBrokerDaemonObservationValidateDisplayOrigin(t *testing.T) {
	tests := []struct {
		name            string
		origin          string
		wantErrContains string
	}{
		{
			name:            "bidi control is refused",
			origin:          "user\u202earch",
			wantErrContains: "display origin contains disallowed characters",
		},
		{
			name:            "line separator is refused",
			origin:          "user@arch\u2028",
			wantErrContains: "display origin contains disallowed characters",
		},
		{
			name:   "origin at the byte bound is accepted",
			origin: strings.Repeat("a", BrokerMaxDisplayOriginBytes),
		},
		{
			name:            "origin one byte over the bound is refused",
			origin:          strings.Repeat("a", BrokerMaxDisplayOriginBytes+1),
			wantErrContains: "display origin too long",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obs := testBrokerObservation("user@arch")
			obs.DisplayOrigin = tc.origin
			err := obs.Validate()
			if tc.wantErrContains != "" {
				require.ErrorContains(t, err, tc.wantErrContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestBrokerDaemonObservationCloneIsDefensive(t *testing.T) {
	original := testBrokerObservation("user@arch")
	original.InventoryKnown = true
	original.Sessions = []catalogue.RemoteCatalogSession{{
		LifecycleID: testBrokerLifecycle(),
		Name:        "work",
		State:       catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "shell"}},
	}}
	cloned := original.Clone()
	cloned.Sessions[0].Name = "mutated"
	cloned.Sessions[0].Tabs[0].ID = "mutated"
	if original.Sessions[0].Name != "work" || original.Sessions[0].Tabs[0].ID != "tab-1" {
		t.Fatal("Clone() shares memory with the original")
	}
	if (BrokerDaemonObservation{}).Clone().Sessions != nil {
		t.Fatal("Clone() of an empty observation must keep a nil session slice")
	}
}
