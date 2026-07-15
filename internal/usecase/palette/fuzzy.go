package palette

import (
	"sort"
	"strings"

	"github.com/bnema/vev/internal/usecase/command"
)

// Match is a fuzzy-matched immutable palette result. Command is retained for
// compatibility with command-only callers; session results leave it empty.
type Match struct {
	Result    Result
	Command   command.Command
	Positions []int
	rank      int
	span      int
	first     int
	order     int
}

// Fuzzy searches either typed results or the legacy command-only input. New
// callers should provide []Result so session targets remain typed end-to-end.
func Fuzzy(items any, query string) []Match {
	var results []Result
	switch values := items.(type) {
	case []Result:
		results = append([]Result(nil), values...)
	case []command.Command:
		results = commandResults(values)
	default:
		return nil
	}
	return fuzzyResults(results, query)
}

func fuzzyResults(results []Result, query string) []Match {
	if query == "" {
		out := make([]Match, 0, len(results))
		for i, result := range results {
			if result != nil {
				out = append(out, newMatch(result, i))
			}
		}
		return out
	}

	var out []Match
	for i, result := range results {
		if result == nil {
			continue
		}
		if match, ok := score(result, query, i); ok {
			out = append(out, match)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.rank != b.rank {
			return a.rank < b.rank
		}
		if a.span != b.span {
			return a.span < b.span
		}
		if a.first != b.first {
			return a.first < b.first
		}
		if a.Result.Kind() != b.Result.Kind() {
			return a.Result.Kind() < b.Result.Kind()
		}
		if aText, bText := normalizedSearchText(a.Result), normalizedSearchText(b.Result); aText != bText {
			return aText < bText
		}
		return a.order < b.order
	})
	return out
}

func score(result Result, query string, order int) (Match, bool) {
	text := normalizedSearchText(result)
	needle := strings.ToLower(query)
	textRunes, queryRunes := []rune(text), []rune(needle)
	match := newMatch(result, order)

	// An exact command shortcode always wins over every other match. The
	// remaining results then share exact/prefix/subsequence scoring.
	if result.Kind() == ResultKindCommand && text == needle {
		match.Positions = rangePositions(len(textRunes))
		match.rank, match.span, match.first = 0, len(textRunes), 0
		return match, true
	}
	if text == needle {
		match.Positions = rangePositions(len(textRunes))
		match.rank, match.span, match.first = 1, len(textRunes), 0
		return match, true
	}
	if strings.HasPrefix(text, needle) {
		match.Positions = rangePositions(len(queryRunes))
		match.rank, match.span, match.first = 2, len(queryRunes), 0
		return match, true
	}
	if positions, ok := subsequencePositions(textRunes, queryRunes); ok {
		match.Positions = positions
		match.rank, match.span, match.first = 3, positions[len(positions)-1]-positions[0]+1, positions[0]
		return match, true
	}
	if cmd, ok := resultCommand(result); ok {
		if positions, ok := subsequencePositions([]rune(strings.ToLower(cmd.Desc)), queryRunes); ok {
			match.rank, match.span, match.first = 4, positions[len(positions)-1]-positions[0]+1, positions[0]
			return match, true
		}
	}
	return Match{}, false
}

func newMatch(result Result, order int) Match {
	match := Match{Result: result, order: order}
	if cmd, ok := resultCommand(result); ok {
		match.Command = cmd
	}
	return match
}

func normalizedSearchText(result Result) string { return strings.ToLower(result.SearchText()) }

func resultCommand(result Result) (command.Command, bool) {
	switch result := result.(type) {
	case CommandResult:
		return result.Command(), true
	case *CommandResult:
		return result.Command(), true
	default:
		return command.Command{}, false
	}
}

func rangePositions(n int) []int {
	positions := make([]int, n)
	for i := range positions {
		positions[i] = i
	}
	return positions
}

func subsequencePositions(haystack, needle []rune) ([]int, bool) {
	if len(needle) == 0 {
		return nil, true
	}
	positions := make([]int, 0, len(needle))
	j := 0
	for i, r := range haystack {
		if r == needle[j] {
			positions = append(positions, i)
			j++
			if j == len(needle) {
				return positions, true
			}
		}
	}
	return nil, false
}
