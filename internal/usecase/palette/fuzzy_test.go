package palette

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/stretchr/testify/require"
)

func cmd(code, name, desc string) command.Command {
	return command.Command{Code: code, Name: name, Desc: desc}
}

func codes(matches []Match) []string {
	out := make([]string, len(matches))
	for i, match := range matches {
		out[i] = match.Command.Code
	}
	return out
}

func TestFuzzyOrdersVisibleFieldScoringTiers(t *testing.T) {
	matches := Fuzzy([]command.Command{
		cmd("ZQQ", "unused", "alpha beta"),
		cmd("XAB", "unused", "subsequence code"),
		cmd("ABX", "unused", "prefix code"),
		cmd("AB", "unused", "exact code"),
	}, "ab")

	require.Equal(t, []string{"AB", "ABX", "XAB", "ZQQ"}, codes(matches))
	require.Empty(t, matches[3].Positions, "description matches do not expose code highlight positions")
}

func TestFuzzyDoesNotMatchCommandNames(t *testing.T) {
	matches := Fuzzy([]command.Command{
		cmd("CPY", "Create Alpha Beta", "Enter copy mode"),
		cmd("ZQQ", "unused", "Alpha Beta tools"),
	}, "ab")

	require.Equal(t, []string{"ZQQ"}, codes(matches))
}

func TestFuzzyDescriptionMatchingIsCaseInsensitiveWithoutCodeHighlights(t *testing.T) {
	matches := Fuzzy([]command.Command{cmd("CPY", "unused", "Enter COPY mode")}, "copy")

	require.Len(t, matches, 1)
	require.Empty(t, matches[0].Positions)
}

func TestFuzzyTieBreaksBySpanFirstThenCode(t *testing.T) {
	matches := Fuzzy([]command.Command{
		cmd("ZAXQB", "", ""), // span 3, first 1
		cmd("BAXQ", "", ""),  // span 3, first 1, code sorts first
		cmd("PXAQ", "", ""),  // span 2, first 2
		cmd("BAQ", "", ""),   // span 2, first 0
	}, "aq")

	require.Equal(t, []string{"BAQ", "PXAQ", "BAXQ", "ZAXQB"}, codes(matches))
}

func TestFuzzyIsCaseInsensitiveAndHighlightsCodeRunePositions(t *testing.T) {
	matches := Fuzzy([]command.Command{cmd("CpY", "Copy mode", "Enter copy mode")}, "cy")

	require.Len(t, matches, 1)
	require.Equal(t, []int{0, 2}, matches[0].Positions)
}

func TestFuzzyEmptyQueryPreservesRegistryOrder(t *testing.T) {
	commands := []command.Command{cmd("B", "", ""), cmd("A", "", "")}
	matches := Fuzzy(commands, "")

	require.Equal(t, []string{"B", "A"}, codes(matches))
}

func TestFuzzyMixedResultsPrioritizeCommandShortcodeAndRankSessions(t *testing.T) {
	created := time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC)
	results := []Result{
		NewStoppedSessionResult("work", created),
		NewCommandResult(cmd("WORK", "", "Create a workspace")),
		NewActiveSessionResult("work", created, domain.SessionID("work-id")),
		NewCommandResult(cmd("WQORRK", "", "")),
		NewCommandResult(cmd("ZZZ", "", "work tools")),
	}

	matches := Fuzzy(results, "work")
	require.Equal(t, []string{"WORK", "work", "work", "WQORRK", "ZZZ"}, matchSearchText(matches))
	require.Equal(t, []ResultKind{
		ResultKindCommand,
		ResultKindActiveSession,
		ResultKindStoppedSession,
		ResultKindCommand,
		ResultKindCommand,
	}, matchKinds(matches))
	require.Equal(t, []int{0, 1, 2, 3}, matches[0].Positions)
	require.Equal(t, []int{0, 1, 2, 3}, matches[1].Positions)
	require.Empty(t, matches[4].Positions)
}

func TestFuzzyMixedResultsUsesKindThenNormalizedTextThenStableOrder(t *testing.T) {
	created := time.Time{}
	results := []Result{
		NewStoppedSessionResult("aBravo", created),
		NewStoppedSessionResult("aAlpha", created),
		NewActiveSessionResult("aZulu", created, "z"),
		NewActiveSessionResult("aEcho", created, "e"),
		NewActiveSessionResult("aEcho", created, "e2"),
		NewCommandResult(cmd("AX", "", "")),
	}

	matches := Fuzzy(results, "a")
	require.Equal(t, []string{"AX", "aEcho", "aEcho", "aZulu", "aAlpha", "aBravo"}, matchSearchText(matches))
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
