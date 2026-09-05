package wire

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func TestAttachTargetPreferredTabStrict(t *testing.T) {
	valid := protocol.AttachTarget{
		Session: "work", Intent: protocol.IntentAttach,
		ExactTarget:    &protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"},
		PreferredTabID: "tab",
	}
	payload := MarshalAttachTarget(valid)
	want := append(make([]byte, 8), 0, 0, 0, 4, 'w', 'o', 'r', 'k', protocol.IntentAttach, 0, 0, 1, 1)
	want = append(want, make([]byte, 15)...)
	want = append(want, 0, 4, 'w', 'o', 'r', 'k', 0, 0, 3, 't', 'a', 'b')
	require.Equal(t, want, payload)
	decoded, err := UnmarshalAttachTarget(payload)
	require.NoError(t, err)
	require.Equal(t, valid, decoded)
	assertAllPrefixesFail(t, payload, UnmarshalAttachTarget)
	assertTrailingGarbageFails(t, payload, UnmarshalAttachTarget)
	malformed := append([]byte(nil), payload...)
	malformed[len(malformed)-1] = '\n'
	_, err = UnmarshalAttachTarget(malformed)
	require.ErrorIs(t, err, protocol.ErrInvalidAttachTarget)

	for _, tc := range []struct {
		name   string
		change func(*protocol.AttachTarget)
	}{
		{name: "no exact lifecycle", change: func(m *protocol.AttachTarget) { m.ExactTarget = nil }},
		{name: "remote endpoint", change: func(m *protocol.AttachTarget) { m.Endpoint = "host" }},
		{name: "remote target", change: func(m *protocol.AttachTarget) { m.RemoteTarget = &domain.RemoteSessionTarget{} }},
		{name: "creation", change: func(m *protocol.AttachTarget) { m.Intent = protocol.IntentNew }},
		{name: "ephemeral creation", change: func(m *protocol.AttachTarget) { m.Intent = protocol.IntentEphemeral }},
		{name: "control byte", change: func(m *protocol.AttachTarget) { m.PreferredTabID = "tab\n" }},
		{name: "whitespace", change: func(m *protocol.AttachTarget) { m.PreferredTabID = "tab name" }},
		{name: "invalid UTF8", change: func(m *protocol.AttachTarget) { m.PreferredTabID = "\xff" }},
		{name: "oversize", change: func(m *protocol.AttachTarget) { m.PreferredTabID = domain.TabStableID(strings.Repeat("x", 129)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := valid
			tc.change(&message)
			require.ErrorIs(t, protocol.ValidateAttachTarget(message), protocol.ErrInvalidAttachTarget)
			require.Nil(t, MarshalAttachTarget(message))
		})
	}
}
