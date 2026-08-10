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

func TestFuzzyOrdersMixedResults(t *testing.T) {
	created := time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		results       []Result
		query         string
		wantText      []string
		wantKinds     []ResultKind
		wantPositions [][]int
		wantActiveIDs map[int]domain.SessionID
	}{
		{
			name: "command shortcode precedes sessions and description matches",
			results: []Result{
				NewStoppedSessionResult("work", created),
				NewCommandResult(cmd("WORK", "", "Create a workspace")),
				NewActiveSessionResult("work", created, domain.SessionID("work-id")),
				NewCommandResult(cmd("WQORRK", "", "")),
				NewCommandResult(cmd("ZZZ", "", "work tools")),
			},
			query:         "work",
			wantText:      []string{"WORK", "Resume session work", "WQORRK", "Switch to session work", "ZZZ"},
			wantKinds:     []ResultKind{ResultKindCommand, ResultKindStoppedSession, ResultKindCommand, ResultKindActiveSession, ResultKindCommand},
			wantPositions: [][]int{{0, 1, 2, 3}, {15, 16, 17, 18}, {0, 2, 3, 5}, {1, 8, 20, 21}, nil},
		},
		{
			name: "equivalent sessions sort by normalized text",
			results: []Result{
				NewStoppedSessionResult("aBravo", time.Time{}),
				NewStoppedSessionResult("aAlpha", time.Time{}),
			},
			query:         "a",
			wantText:      []string{"Resume session aAlpha", "Resume session aBravo"},
			wantKinds:     []ResultKind{ResultKindStoppedSession, ResultKindStoppedSession},
			wantPositions: [][]int{{15}, {15}},
		},
		{
			name: "display prefix positions sort before later active names",
			results: []Result{
				NewStoppedSessionResult("aBravo", time.Time{}),
				NewStoppedSessionResult("aAlpha", time.Time{}),
				NewActiveSessionResult("aZulu", time.Time{}, "z"),
				NewActiveSessionResult("aEcho", time.Time{}, "e"),
				NewActiveSessionResult("aEcho", time.Time{}, "e2"),
				NewCommandResult(cmd("AX", "", "")),
			},
			query:         "a",
			wantText:      []string{"AX", "Resume session aAlpha", "Resume session aBravo", "Switch to session aEcho", "Switch to session aEcho", "Switch to session aZulu"},
			wantKinds:     []ResultKind{ResultKindCommand, ResultKindStoppedSession, ResultKindStoppedSession, ResultKindActiveSession, ResultKindActiveSession, ResultKindActiveSession},
			wantPositions: [][]int{{0}, {15}, {15}, {18}, {18}, {18}},
			wantActiveIDs: map[int]domain.SessionID{3: "e", 4: "e2", 5: "z"},
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
			for index, wantID := range tt.wantActiveIDs {
				gotID, ok := matches[index].Result.SessionID()
				require.True(t, ok)
				require.Equal(t, wantID, gotID)
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
