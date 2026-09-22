package ports

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func admissionTestPolicy() BrokerPolicy {
	return BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: 3,
		EnvironmentPolicy:    protocol.EnvironmentPolicyClientOwned,
		Transport:            "unix-mux",
		Trust:                "same-user",
		Launch:               "explicit",
		Isolation:            "per-user",
	}
}

func admissionTestTarget() protocol.ExactSessionTarget {
	return protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1, 2, 3}, SessionName: "work"}
}

// TestSessionConnectionOriginValidateMatrix pins the closed origin taxonomy: the
// two provisioned localities validate and Unknown plus every out-of-range value
// is refused, so an admission can never inherit an implicit locality.
func TestSessionConnectionOriginValidateMatrix(t *testing.T) {
	tests := []struct {
		name   string
		origin SessionConnectionOrigin
		valid  bool
	}{
		{name: "unknown", origin: SessionOriginUnknown, valid: false},
		{name: "local", origin: SessionOriginLocal, valid: true},
		{name: "remote", origin: SessionOriginRemote, valid: true},
		{name: "out of range", origin: SessionConnectionOrigin(9), valid: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.origin.Validate()
			if tt.valid {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
		})
	}
}

// TestSessionAdmissionValidateMatrix pins the closed admission contract: origin,
// policy, purpose, admission variant, name, target, and environment bounds are
// each enforced, and a shape that contradicts its purpose is refused rather than
// narrowed.
func TestSessionAdmissionValidateMatrix(t *testing.T) {
	env := []string{"TERM=xterm"}
	atEnvLimit := make([]string, BrokerMaxEnvEntries)
	for i := range atEnvLimit {
		atEnvLimit[i] = "K=v"
	}
	oversizeEnv := []string{"K=" + strings.Repeat("x", BrokerMaxEnvEntryBytes)}

	base := SessionAdmission{Origin: SessionOriginLocal, Policy: admissionTestPolicy(), Purpose: BrokerStreamControl}

	tests := []struct {
		name      string
		admission SessionAdmission
		valid     bool
	}{
		{name: "control", admission: base, valid: true},
		{name: "observation", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamObservation
			return a
		}(), valid: true},
		{name: "exact attach", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionExact
			a.Target = admissionTestTarget()
			a.Env = append([]string(nil), env...)
			return a
		}(), valid: true},
		{name: "named creation", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionCreateNamed
			a.Name = "work"
			return a
		}(), valid: true},
		{name: "ephemeral creation", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionCreateEphemeral
			return a
		}(), valid: true},
		{name: "remote exact attach", admission: func() SessionAdmission {
			a := base
			a.Origin = SessionOriginRemote
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionExact
			a.Target = admissionTestTarget()
			return a
		}(), valid: true},
		{name: "environment at bound", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionExact
			a.Target = admissionTestTarget()
			a.Env = atEnvLimit
			return a
		}(), valid: true},

		{name: "unknown origin", admission: func() SessionAdmission {
			a := base
			a.Origin = SessionOriginUnknown
			return a
		}(), valid: false},
		{name: "invalid origin", admission: func() SessionAdmission {
			a := base
			a.Origin = SessionConnectionOrigin(7)
			return a
		}(), valid: false},
		{name: "invalid policy", admission: func() SessionAdmission {
			a := base
			a.Policy.ProtocolVersion = 0
			return a
		}(), valid: false},
		{name: "zero purpose", admission: func() SessionAdmission {
			a := base
			a.Purpose = 0
			return a
		}(), valid: false},
		{name: "invalid purpose", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamPurpose(9)
			return a
		}(), valid: false},
		{name: "attachment without admission", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			return a
		}(), valid: false},
		{name: "exact without target", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionExact
			return a
		}(), valid: false},
		{name: "exact with name", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionExact
			a.Target = admissionTestTarget()
			a.Name = "work"
			return a
		}(), valid: false},
		{name: "named creation without name", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionCreateNamed
			return a
		}(), valid: false},
		{name: "named creation with target", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionCreateNamed
			a.Name = "work"
			a.Target = admissionTestTarget()
			return a
		}(), valid: false},
		{name: "ephemeral with name", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionCreateEphemeral
			a.Name = "work"
			return a
		}(), valid: false},
		{name: "ephemeral with target", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionCreateEphemeral
			a.Target = admissionTestTarget()
			return a
		}(), valid: false},
		{name: "control carries admission", admission: func() SessionAdmission {
			a := base
			a.Admission = BrokerAdmissionExact
			return a
		}(), valid: false},
		{name: "control carries name", admission: func() SessionAdmission {
			a := base
			a.Name = "work"
			return a
		}(), valid: false},
		{name: "control carries target", admission: func() SessionAdmission {
			a := base
			a.Target = admissionTestTarget()
			return a
		}(), valid: false},
		{name: "control carries env", admission: func() SessionAdmission {
			a := base
			a.Env = append([]string(nil), env...)
			return a
		}(), valid: false},
		{name: "observation carries env", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamObservation
			a.Env = append([]string(nil), env...)
			return a
		}(), valid: false},
		{name: "too many environment entries", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionExact
			a.Target = admissionTestTarget()
			a.Env = append(append([]string(nil), atEnvLimit...), "K=v")
			return a
		}(), valid: false},
		{name: "oversize environment entry", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionExact
			a.Target = admissionTestTarget()
			a.Env = oversizeEnv
			return a
		}(), valid: false},
		{name: "invalid utf8 environment entry", admission: func() SessionAdmission {
			a := base
			a.Purpose = BrokerStreamAttachment
			a.Admission = BrokerAdmissionExact
			a.Target = admissionTestTarget()
			a.Env = []string{"K=\xff"}
			return a
		}(), valid: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.admission.Validate()
			if tt.valid {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
		})
	}
}

// TestSessionAdmissionCloneIsDeep pins the defensive copy: mutating the clone's
// environment or its nested values never changes the original, and a nil
// environment stays nil.
func TestSessionAdmissionCloneIsDeep(t *testing.T) {
	original := SessionAdmission{
		Origin: SessionOriginRemote, Policy: admissionTestPolicy(), Purpose: BrokerStreamAttachment,
		Admission: BrokerAdmissionExact, Target: admissionTestTarget(), Env: []string{"TERM=xterm"},
	}
	clone := original.Clone()
	require.Equal(t, original, clone)

	clone.Env[0] = "TERM=changed"
	clone.Policy.Trust = "changed"
	clone.Target.SessionName = "changed"
	require.Equal(t, "TERM=xterm", original.Env[0], "clone environment is independent")
	require.Equal(t, "same-user", original.Policy.Trust)
	require.Equal(t, "work", original.Target.SessionName)

	empty := SessionAdmission{Origin: SessionOriginLocal, Policy: admissionTestPolicy(), Purpose: BrokerStreamControl}
	require.Nil(t, empty.Clone().Env, "an absent environment stays nil")
	require.Equal(t, empty, empty.Clone())
}
