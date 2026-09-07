package domain

import (
	"errors"
	"fmt"
	"testing"
)

func TestRemoteAvailabilityString(t *testing.T) {
	tests := []struct {
		name  string
		value RemoteAvailability
		want  string
	}{
		{name: "unknown", value: RemoteAvailabilityUnknown, want: "unknown"},
		{name: "reachable", value: RemoteAvailabilityReachable, want: "reachable"},
		{name: "unreachable", value: RemoteAvailabilityUnreachable, want: "unreachable"},
		{name: "incompatible", value: RemoteAvailabilityIncompatible, want: "incompatible"},
		{name: "auth failed", value: RemoteAvailabilityAuthFailed, want: "authentication_failed"},
		{name: "invalid response", value: RemoteAvailabilityInvalidResponse, want: "invalid_response"},
		{name: "zero maps to unknown", value: RemoteAvailability(0), want: "unknown"},
		{name: "out of range maps to unknown", value: RemoteAvailability(99), want: "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.value.String(); got != test.want {
				t.Fatalf("String() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRemoteFailureKindString(t *testing.T) {
	tests := []struct {
		name  string
		value RemoteFailureKind
		want  string
	}{
		{name: "none", value: RemoteFailureNone, want: "none"},
		{name: "transport", value: RemoteFailureTransport, want: "transport"},
		{name: "timeout", value: RemoteFailureTimeout, want: "timeout"},
		{name: "trust", value: RemoteFailureTrust, want: "trust"},
		{name: "authentication", value: RemoteFailureAuthentication, want: "authentication"},
		{name: "incompatible", value: RemoteFailureIncompatible, want: "incompatible"},
		{name: "invalid response", value: RemoteFailureInvalidResponse, want: "invalid_response"},
		{name: "out of range stays transport", value: RemoteFailureKind(99), want: "transport"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.value.String(); got != test.want {
				t.Fatalf("String() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRemoteFailurePreservesCause(t *testing.T) {
	cause := errors.New("ssh: connection refused")
	failure := RemoteFailure{Kind: RemoteFailureTimeout, Err: cause}
	if !errors.Is(failure, cause) {
		t.Fatalf("errors.Is(failure, cause) = false, want true")
	}
	var asFailure RemoteFailure
	if !errors.As(fmt.Errorf("observe: %w", failure), &asFailure) || asFailure.Kind != RemoteFailureTimeout {
		t.Fatalf("errors.As did not recover %+v", failure)
	}
	if failure.Error() == "" || failure.Error() == cause.Error() {
		t.Fatalf("Error() must report the sanitized kind, got %q", failure.Error())
	}
}

func TestNewRemoteRegistration(t *testing.T) {
	first, err := NewRemoteRegistration("user@arch", [16]byte{1})
	if err != nil {
		t.Fatalf("NewRemoteRegistration: %v", err)
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if first.IsZero() {
		t.Fatal("fresh registration must not be zero")
	}
	second, err := NewRemoteRegistration("user@arch", [16]byte{2})
	if err != nil {
		t.Fatalf("NewRemoteRegistration: %v", err)
	}
	if first.Incarnation == second.Incarnation {
		t.Fatal("incarnations must be unique per registration")
	}
	if first.Equal(second) {
		t.Fatal("distinct incarnations must not compare equal")
	}
	if _, err := NewRemoteRegistration("not a host", [16]byte{1}); err == nil {
		t.Fatal("invalid endpoint must be rejected")
	}
}

func TestRemoteRegistrationIdentity(t *testing.T) {
	valid := RemoteRegistration{Endpoint: "user@arch", Incarnation: [16]byte{1}, Generation: 2}
	tests := []struct {
		name     string
		value    RemoteRegistration
		zero     bool
		validate bool
	}{
		{name: "valid", value: valid, zero: false, validate: true},
		{name: "empty", value: RemoteRegistration{}, zero: true, validate: false},
		{name: "zero incarnation", value: RemoteRegistration{Endpoint: "user@arch", Generation: 1}, zero: true, validate: false},
		{name: "zero generation", value: RemoteRegistration{Endpoint: "user@arch", Incarnation: [16]byte{1}}, zero: false, validate: false},
		{name: "bad endpoint", value: RemoteRegistration{Endpoint: "not a host", Incarnation: [16]byte{1}, Generation: 1}, zero: false, validate: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.value.IsZero(); got != test.zero {
				t.Fatalf("IsZero() = %t, want %t", got, test.zero)
			}
			if err := test.value.Validate(); (err == nil) != test.validate {
				t.Fatalf("Validate() err = %v, want valid=%t", err, test.validate)
			}
		})
	}
	if !valid.Equal(valid) {
		t.Fatal("registration must equal itself")
	}
	renamed := valid
	renamed.Endpoint = "user@mule"
	if valid.Equal(renamed) {
		t.Fatal("different endpoints must not compare equal")
	}
	advanced := valid
	advanced.Generation++
	if valid.Equal(advanced) {
		t.Fatal("different generations must not compare equal")
	}
}
