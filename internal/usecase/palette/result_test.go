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
	commandInfo, ok := commandResult.Command()
	require.True(t, ok)
	require.Equal(t, "NT", commandInfo.Code)

	require.Equal(t, ResultKindActiveSession, active.Kind())
	name, ok := active.SessionName()
	require.True(t, ok)
	require.Equal(t, "work", name)
	createdAt, ok := active.SessionCreatedAt()
	require.True(t, ok)
	require.Equal(t, created, createdAt)
	require.Equal(t, "Switch to session work", active.DisplayText())
	require.Equal(t, "work", active.SearchText())
	id, ok := active.SessionID()
	require.True(t, ok)
	require.Equal(t, domain.SessionID("session-work"), id)

	require.Equal(t, ResultKindStoppedSession, stopped.Kind())
	name, ok = stopped.SessionName()
	require.True(t, ok)
	require.Equal(t, "archive", name)
	createdAt, ok = stopped.SessionCreatedAt()
	require.True(t, ok)
	require.Equal(t, created, createdAt)
	require.Equal(t, "Resume session archive", stopped.DisplayText())
	_, ok = stopped.SessionID()
	require.False(t, ok)
}
