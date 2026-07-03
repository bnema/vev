package palette

import (
	"testing"

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

func TestFuzzyOrdersScoringTiers(t *testing.T) {
	matches := Fuzzy([]command.Command{
		cmd("ZQQ", "alpha beta", "name subsequence"),
		cmd("XAB", "unused", "subsequence code"),
		cmd("ABX", "unused", "prefix code"),
		cmd("AB", "unused", "exact code"),
	}, "ab")

	require.Equal(t, []string{"AB", "ABX", "XAB", "ZQQ"}, codes(matches))
	require.Empty(t, matches[3].Positions, "name/description matches do not expose code highlight positions")
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
