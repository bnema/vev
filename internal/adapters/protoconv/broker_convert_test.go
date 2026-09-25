package protoconv

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestBrokerConversionRoundTrips(t *testing.T) {
	registration := domain.RemoteRegistration{Endpoint: "dev@host:22", Incarnation: [16]byte{1, 2, 3}, Generation: 9}
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1, 2, 3}, SessionName: "work"}
	policy := ports.BrokerPolicy{ProtocolVersion: protocol.Version, CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, Transport: "quic", Trust: "pinned", Launch: "agent", Isolation: "user"}
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"registration", func(t *testing.T) {
			encoded := BrokerRegistrationToWire(registration)
			got, err := BrokerRegistrationFromWire(encoded)
			require.NoError(t, err)
			require.Equal(t, registration, got)
			encoded.Incarnation[0] = 0
			require.Equal(t, byte(1), registration.Incarnation[0])
		}},
		{"target", func(t *testing.T) {
			encoded := BrokerExactTargetToWire(target)
			got, err := BrokerExactTargetFromWire(encoded)
			require.NoError(t, err)
			require.Equal(t, target, got)
			encoded.LifecycleId.Value[0] = 0
			require.Equal(t, byte(1), target.LifecycleID[0])
		}},
		{"policy", func(t *testing.T) {
			got, err := BrokerPolicyFromWire(BrokerPolicyToWire(policy))
			require.NoError(t, err)
			require.Equal(t, policy, got)
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

func TestBrokerStartModeCodes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mode  ports.BrokerDaemonStartMode
		code  uint32
		valid bool
	}{
		{"existing", ports.BrokerDaemonExistingOnly, 1, true},
		{"spawn", ports.BrokerDaemonStartIfNeeded, 2, true},
		{"absent", 0, 0, false},
		{"unknown", 3, 3, false},
		{"aliased", 0, 257, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, encodeErr := BrokerStartModeToWire(tc.mode)
			mode, decodeErr := BrokerStartModeFromWire(tc.code)
			if !tc.valid {
				require.ErrorIs(t, encodeErr, ErrOutOfRange)
				require.ErrorIs(t, decodeErr, ErrOutOfRange)
				return
			}
			require.NoError(t, encodeErr)
			require.NoError(t, decodeErr)
			require.Equal(t, tc.code, code)
			require.Equal(t, tc.mode, mode)
		})
	}
}

func TestBrokerConversionFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func() error
		want error
	}{
		{"nil registration", func() error { _, err := BrokerRegistrationFromWire(nil); return err }, ErrOutOfRange},
		{"short incarnation", func() error {
			_, err := BrokerRegistrationFromWire(&wire.RemoteRegistration{Endpoint: "dev@host:22", Incarnation: []byte{1}, Generation: 1})
			return err
		}, ErrOutOfRange},
		{"nil target", func() error { _, err := BrokerExactTargetFromWire(nil); return err }, ErrOutOfRange},
		{"nil policy", func() error { _, err := BrokerPolicyFromWire(nil); return err }, ErrOutOfRange},
		{"policy narrowing", func() error {
			_, err := BrokerPolicyFromWire(&wire.BrokerWirePolicy{ProtocolVersion: 1<<16 + uint32(protocol.Version)})
			return err
		}, ErrOutOfRange},
		{"policy validation", func() error { _, err := BrokerPolicyFromWire(&wire.BrokerWirePolicy{}); return err }, ErrInvalid},
		{"oversize text", func() error { return BrokerDisplayText("abc", 2) }, ErrTooLarge},
		{"bidi text", func() error { return BrokerDisplayText("a\u202eb", 20) }, ErrInvalid},
		{"invalid utf8", func() error { return BrokerDisplayText("\xff", 20) }, ErrInvalid},
		{"oversize env", func() error { return BrokerEnvEntries([]string{strings.Repeat("a", ports.BrokerMaxEnvEntryBytes+1)}) }, ErrTooLarge},
		{"missing equals", func() error { return BrokerEnvEntries([]string{"TERM"}) }, ErrInvalid},
		{"nul env", func() error { return BrokerEnvEntries([]string{"A=x\x00y"}) }, ErrInvalid},
		{"ansi prompt env", func() error { return BrokerEnvEntries([]string{"PS1=\x1b[32m$ \x1b[0m"}) }, nil},
		{"valid env", func() error { return BrokerEnvEntries([]string{"TERM=x"}) }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}
}
