package ports

import (
	"errors"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestBrokerPolicyTokens(t *testing.T) {
	for _, field := range []string{"trust", "launch", "isolation"} {
		for _, value := range []string{"", "bad token", "\xff", "bad\u202e", strings.Repeat("x", BrokerMaxPolicyTokenBytes+1), strings.Repeat("x", BrokerMaxPolicyTokenBytes)} {
			t.Run(field+"/"+value, func(t *testing.T) {
				p := testBrokerPolicy()
				switch field {
				case "trust":
					p.Trust = value
				case "launch":
					p.Launch = value
				case "isolation":
					p.Isolation = value
				}
				err := p.Validate()
				if len(value) == BrokerMaxPolicyTokenBytes {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "policy "+field)
				}
			})
		}
	}
}

func TestBrokerDialTargetValidation(t *testing.T) {
	for _, tc := range []struct {
		name, address string
		valid         bool
	}{
		{"valid", "route", true}, {"bound", strings.Repeat("x", BrokerMaxAddressBytes), true},
		{"empty", "", false}, {"over", strings.Repeat("x", BrokerMaxAddressBytes+1), false},
		{"utf8", "\xff", false}, {"control", "route\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := BrokerDialTarget{Fence: BrokerEndpointFence{Local: true}, Policy: testBrokerPolicy(), Address: tc.address, StartMode: BrokerDaemonExistingOnly, ExpectedIdentity: BrokerExpectedIdentity{Identity: "daemon", Bound: true}}
			require.Equal(t, tc.valid, target.Validate() == nil)
		})
	}
	target := BrokerDialTarget{Fence: BrokerEndpointFence{Local: true}, Policy: testBrokerPolicy(), Address: "route", StartMode: BrokerDaemonExistingOnly, ExpectedIdentity: BrokerExpectedIdentity{Identity: "daemon", Bound: true}}
	target.ExpectedIdentity.Identity = ""
	require.Error(t, target.Validate())
	target.ExpectedIdentity.Identity = "daemon"
	target.Policy.Trust = ""
	require.Error(t, target.Validate())
}

func TestBrokerStreamLostErrorChain(t *testing.T) {
	cause := errors.New("transport diagnostic")
	loss := BrokerStreamLost{Epoch: 1, Connection: BrokerConnectionID{1}, Stream: 1, Cause: domain.RemoteFailureTrust, Err: cause}
	require.NoError(t, loss.Validate())
	require.Equal(t, "vev: broker attachment_lost", loss.Error())
	require.ErrorIs(t, loss, cause)
	var typed BrokerError
	require.ErrorAs(t, loss, &typed)
	require.Equal(t, BrokerErrorAttachmentLost, typed.Code)
	loss.Cause = domain.RemoteFailureNone
	require.Error(t, loss.Validate())
	loss.Cause = 99
	require.Error(t, loss.Validate())
}

func TestBrokerRequestLocalRemoteFencing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*BrokerOpenStreamRequest)
		valid  bool
	}{
		{"local", func(*BrokerOpenStreamRequest) {}, true},
		{"local endpoint", func(r *BrokerOpenStreamRequest) { r.Endpoint = "user@host" }, false},
		{"local registration", func(r *BrokerOpenStreamRequest) { r.Registration = testBrokerRegistration("user@host", 1, 1) }, false},
		{"remote missing", func(r *BrokerOpenStreamRequest) { r.Local = false }, false},
		{"remote valid", func(r *BrokerOpenStreamRequest) {
			r.Local = false
			r.Endpoint = "user@host"
			r.Registration = testBrokerRegistration(r.Endpoint, 1, 1)
		}, true},
		{"remote mismatch", func(r *BrokerOpenStreamRequest) {
			r.Local = false
			r.Endpoint = "user@other"
			r.Registration = testBrokerRegistration("user@host", 1, 1)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := BrokerOpenStreamRequest{Epoch: 1, Connection: BrokerConnectionID{1}, Stream: 1, Local: true, Purpose: BrokerStreamControl, Policy: testBrokerPolicy(), StartMode: BrokerDaemonStartIfNeeded}
			tc.mutate(&r)
			require.Equal(t, tc.valid, r.Validate() == nil)
		})
	}
}
