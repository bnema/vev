package palette

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/stretchr/testify/require"
)

func cmd(code, name, desc string) command.Command {
	return command.Command{Code: code, Name: name, Desc: desc}
}

func codes(matches []Match) []string {
	out := make([]string, len(matches))
	for i, match := range matches {
		cmd, ok := match.Result.Command()
		if !ok {
			continue
		}
		out[i] = cmd.Code
	}
	return out
}

func TestFuzzyOrdersVisibleFieldScoringTiers(t *testing.T) {
	matches := Fuzzy(CommandResults([]command.Command{
		cmd("ZQQ", "unused", "alpha beta"),
		cmd("XAB", "unused", "subsequence code"),
		cmd("ABX", "unused", "prefix code"),
		cmd("AB", "unused", "exact code"),
	}), "ab")

	require.Equal(t, []string{"AB", "ABX", "XAB", "ZQQ"}, codes(matches))
	require.Empty(t, matches[3].Positions, "description matches do not expose code highlight positions")
}

func TestFuzzyDoesNotMatchCommandNames(t *testing.T) {
	matches := Fuzzy(CommandResults([]command.Command{
		cmd("CPY", "Create Alpha Beta", "Enter copy mode"),
		cmd("ZQQ", "unused", "Alpha Beta tools"),
	}), "ab")

	require.Equal(t, []string{"ZQQ"}, codes(matches))
}

func TestFuzzyDescriptionMatchingIsCaseInsensitiveWithoutCodeHighlights(t *testing.T) {
	matches := Fuzzy(CommandResults([]command.Command{cmd("CPY", "unused", "Enter COPY mode")}), "copy")

	require.Len(t, matches, 1)
	require.Empty(t, matches[0].Positions)
}

func TestFuzzyTieBreaksBySpanFirstThenCode(t *testing.T) {
	matches := Fuzzy(CommandResults([]command.Command{
		cmd("ZAXQB", "", ""), // span 3, first 1
		cmd("BAXQ", "", ""),  // span 3, first 1, code sorts first
		cmd("PXAQ", "", ""),  // span 2, first 2
		cmd("BAQ", "", ""),   // span 2, first 0
	}), "aq")

	require.Equal(t, []string{"BAQ", "PXAQ", "BAXQ", "ZAXQB"}, codes(matches))
}

func TestFuzzyIsCaseInsensitiveAndHighlightsCodeRunePositions(t *testing.T) {
	matches := Fuzzy(CommandResults([]command.Command{cmd("CpY", "Copy mode", "Enter copy mode")}), "cy")

	require.Len(t, matches, 1)
	require.Equal(t, []int{0, 2}, matches[0].Positions)
}

func TestFuzzyEmptyQueryPreservesRegistryOrder(t *testing.T) {
	commands := []command.Command{cmd("B", "", ""), cmd("A", "", "")}
	matches := Fuzzy(CommandResults(commands), "")

	require.Equal(t, []string{"B", "A"}, codes(matches))
}

func TestFuzzyRanksExactSessionNameAheadOfPrefix(t *testing.T) {
	tests := []struct {
		name      string
		results   []Result
		wantFirst string
		offset    int
	}{
		{
			name: "active session",
			results: []Result{
				NewActiveSessionResultWithDisplayOrigin(testExactTarget("vev-vt", 1), time.Time{}, "arch"),
				NewActiveSessionResultWithDisplayOrigin(testExactTarget("vev", 2), time.Time{}, "arch"),
			},
			wantFirst: "Switch to session vev@arch",
			offset:    18,
		},
		{
			name: "stopped session",
			results: []Result{
				NewStoppedSessionResultWithDisplayOrigin(testExactTarget("vev-vt", 1), time.Time{}, "arch"),
				NewStoppedSessionResultWithDisplayOrigin(testExactTarget("vev", 2), time.Time{}, "arch"),
			},
			wantFirst: "Resume session vev@arch",
			offset:    15,
		},
		{
			name: "remote catalog session",
			results: []Result{
				NewRemoteSessionResult(domain.RemoteSessionKey{Host: "arch", Name: "vev-vt", DisplayOrigin: "arch"}, domain.RemoteSessionTarget{}, ""),
				NewRemoteSessionResult(domain.RemoteSessionKey{Host: "arch", Name: "vev", DisplayOrigin: "arch"}, domain.RemoteSessionTarget{}, ""),
			},
			wantFirst: "Switch to session vev@arch",
			offset:    18,
		},
		{
			name: "recent route",
			results: []Result{
				NewRecentRouteResult("vev-vt", "vev-vt@arch", protocol.RouteNavigationAction{Key: 1}),
				NewRecentRouteResult("vev", "vev@arch", protocol.RouteNavigationAction{Key: 2}),
			},
			wantFirst: "Switch to session vev@arch",
			offset:    18,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := Fuzzy(tt.results, "vev")

			require.Len(t, matches, 2)
			require.Equal(t, tt.wantFirst, matches[0].Result.DisplayText())
			require.Equal(t, []int{tt.offset, tt.offset + 1, tt.offset + 2}, matches[0].Positions)
		})
	}
}

func TestFuzzyOrdersMixedResults(t *testing.T) {
	created := time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name              string
		results           []Result
		query             string
		wantText          []string
		wantKinds         []ResultKind
		wantPositions     [][]int
		wantActiveTargets map[int]protocol.ExactSessionTarget
	}{
		{
			name: "command shortcode precedes exact session names and broader matches",
			results: []Result{
				NewStoppedSessionResult(testExactTarget("work", 1), created),
				NewCommandResult(cmd("WORK", "", "Create a workspace")),
				NewActiveSessionResult(testExactTarget("work", 2), created),
				NewCommandResult(cmd("WQORRK", "", "")),
				NewCommandResult(cmd("ZZZ", "", "work tools")),
			},
			query:         "work",
			wantText:      []string{"WORK", "Switch to session work", "Resume session work", "WQORRK", "ZZZ"},
			wantKinds:     []ResultKind{ResultKindCommand, ResultKindActiveSession, ResultKindStoppedSession, ResultKindCommand, ResultKindCommand},
			wantPositions: [][]int{{0, 1, 2, 3}, {18, 19, 20, 21}, {15, 16, 17, 18}, {0, 2, 3, 5}, nil},
		},
		{
			name: "equivalent sessions sort by normalized text",
			results: []Result{
				NewStoppedSessionResult(testExactTarget("aBravo", 1), time.Time{}),
				NewStoppedSessionResult(testExactTarget("aAlpha", 2), time.Time{}),
			},
			query:         "a",
			wantText:      []string{"Resume session aAlpha", "Resume session aBravo"},
			wantKinds:     []ResultKind{ResultKindStoppedSession, ResultKindStoppedSession},
			wantPositions: [][]int{{15}, {15}},
		},
		{
			name: "semantic session prefixes sort by kind then normalized label",
			results: []Result{
				NewStoppedSessionResult(testExactTarget("aBravo", 1), time.Time{}),
				NewStoppedSessionResult(testExactTarget("aAlpha", 2), time.Time{}),
				NewActiveSessionResult(testExactTarget("aZulu", 3), time.Time{}),
				NewActiveSessionResult(testExactTarget("aEcho", 4), time.Time{}),
				NewActiveSessionResult(testExactTarget("aEcho", 5), time.Time{}),
				NewCommandResult(cmd("AX", "", "")),
			},
			query:         "a",
			wantText:      []string{"AX", "Switch to session aEcho", "Switch to session aEcho", "Switch to session aZulu", "Resume session aAlpha", "Resume session aBravo"},
			wantKinds:     []ResultKind{ResultKindCommand, ResultKindActiveSession, ResultKindActiveSession, ResultKindActiveSession, ResultKindStoppedSession, ResultKindStoppedSession},
			wantPositions: [][]int{{0}, {18}, {18}, {18}, {15}, {15}},
			wantActiveTargets: map[int]protocol.ExactSessionTarget{
				1: testExactTarget("aEcho", 4), 2: testExactTarget("aEcho", 5), 3: testExactTarget("aZulu", 3),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := Fuzzy(tt.results, tt.query)
			require.Equal(t, tt.wantText, matchSearchText(matches))
			require.Equal(t, tt.wantKinds, matchKinds(matches))
			positions := make([][]int, len(matches))
			for i := range matches {
				positions[i] = matches[i].Positions
			}
			require.Equal(t, tt.wantPositions, positions)
			for index, wantTarget := range tt.wantActiveTargets {
				gotTarget, ok := matches[index].Result.SessionTarget()
				require.True(t, ok)
				require.Equal(t, wantTarget, gotTarget)
			}
		})
	}
}

func matchSearchText(matches []Match) []string {
	out := make([]string, len(matches))
	for i, match := range matches {
		out[i] = match.Result.SearchText()
	}
	return out
}

func matchKinds(matches []Match) []ResultKind {
	out := make([]ResultKind, len(matches))
	for i, match := range matches {
		out[i] = match.Result.Kind()
	}
	return out
}
