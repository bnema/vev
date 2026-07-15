package palette

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/stretchr/testify/require"
)

func TestResultKindsAndSessionLifecycleTargets(t *testing.T) {
	created := time.Date(2026, time.March, 1, 2, 3, 4, 0, time.UTC)
	commandResult := NewCommandResult(command.Command{Code: "NT", Desc: "Create tab"})
	active := NewActiveSessionResult("work", created, domain.SessionID("session-work"))
	stopped := NewStoppedSessionResult("archive", created)

	require.Equal(t, ResultKindCommand, commandResult.Kind())
	require.Equal(t, "NT", commandResult.DisplayText())
	require.Equal(t, "NT", commandResult.SearchText())

	require.Equal(t, ResultKindActiveSession, active.Kind())
	require.Equal(t, "work", active.Name())
	require.Equal(t, created, active.CreatedAt())
	require.Equal(t, "Switch to session work", active.DisplayText())
	require.Equal(t, "work", active.SearchText())
	id, ok := active.SessionID()
	require.True(t, ok)
	require.Equal(t, domain.SessionID("session-work"), id)

	require.Equal(t, ResultKindStoppedSession, stopped.Kind())
	require.Equal(t, "archive", stopped.Name())
	require.Equal(t, created, stopped.CreatedAt())
	_, ok = stopped.SessionID()
	require.False(t, ok)
}
