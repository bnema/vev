package palette

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/stretchr/testify/require"
)

func testExactTarget(name string, marker byte) ports.ExactSessionTarget {
	return ports.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{marker}, SessionName: name}
}

func TestResultKindsAndSessionLifecycleTargets(t *testing.T) {
	created := time.Date(2026, time.March, 1, 2, 3, 4, 0, time.UTC)
	commandResult := NewCommandResult(command.Command{Code: "NT", Desc: "Create tab"})
	activeTarget := testExactTarget("work", 1)
	stoppedTarget := testExactTarget("archive", 2)
	active := NewActiveSessionResult("work", created, activeTarget)
	stopped := NewStoppedSessionResult("archive", created, stoppedTarget)

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
	require.Equal(t, "Switch to session work", active.SearchText())
	target, ok := active.SessionTarget()
	require.True(t, ok)
	require.Equal(t, activeTarget, target)

	require.Equal(t, ResultKindStoppedSession, stopped.Kind())
	name, ok = stopped.SessionName()
	require.True(t, ok)
	require.Equal(t, "archive", name)
	createdAt, ok = stopped.SessionCreatedAt()
	require.True(t, ok)
	require.Equal(t, created, createdAt)
	require.Equal(t, "Resume session archive", stopped.DisplayText())
	require.Equal(t, "Resume session archive", stopped.SearchText())
	target, ok = stopped.SessionTarget()
	require.True(t, ok)
	require.Equal(t, stoppedTarget, target)
}

func TestRecentRouteResultCarriesExactNavigationAction(t *testing.T) {
	action := ports.RouteNavigationAction{SnapshotGeneration: 4, Key: 7, Generation: 3}
	result := NewRecentRouteResult("logs@edge", action)

	require.Equal(t, ResultKindRecentRoute, result.Kind())
	require.Equal(t, "Switch to session logs@edge", result.DisplayText())
	require.Equal(t, "Switch to session logs@edge", result.SearchText())
	got, ok := result.RouteNavigationAction()
	require.True(t, ok)
	require.Equal(t, action, got)
	_, ok = result.SessionName()
	require.False(t, ok)
}

func TestSessionResultsSearchDisplayedActionText(t *testing.T) {
	created := time.Date(2026, time.March, 1, 2, 3, 4, 0, time.UTC)
	tests := []struct {
		name      string
		result    Result
		query     string
		positions []int
	}{
		{name: "active prefix", result: NewActiveSessionResult("work", created, testExactTarget("work", 1)), query: "switch", positions: []int{0, 1, 2, 3, 4, 5}},
		{name: "active name", result: NewActiveSessionResult("work", created, testExactTarget("work", 1)), query: "work", positions: []int{1, 8, 20, 21}},
		{name: "stopped prefix", result: NewStoppedSessionResult("archive", created, testExactTarget("archive", 2)), query: "resume", positions: []int{0, 1, 2, 3, 4, 5}},
		{name: "stopped name", result: NewStoppedSessionResult("archive", created, testExactTarget("archive", 2)), query: "archive", positions: []int{15, 16, 17, 18, 19, 20, 21}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := Fuzzy([]Result{tt.result}, tt.query)
			require.Len(t, matches, 1)
			require.Equal(t, tt.result.DisplayText(), tt.result.SearchText())
			require.Equal(t, tt.positions, matches[0].Positions)
		})
	}
}
