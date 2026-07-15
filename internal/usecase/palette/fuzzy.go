package palette

import (
	"sort"
	"strings"
	"unicode/utf8"
)

// Match is a fuzzy-matched immutable palette result.
type Match struct {
	Result    Result
	Positions []int
	rank      int
	span      int
	first     int
	order     int
	search    string
}

// Fuzzy searches immutable palette results.
func Fuzzy(results []Result, query string) []Match {
	needle := strings.ToLower(query)
	if query == "" {
		out := make([]Match, 0, len(results))
		for i, result := range results {
			out = append(out, newMatch(result, i))
		}
		return out
	}

	needleRunes := []rune(needle)
	var out []Match
	for i, result := range results {
		if match, ok := score(result, needle, needleRunes, i); ok {
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
		if a.search != b.search {
			return a.search < b.search
		}
		return a.order < b.order
	})
	return out
}

func score(result Result, needle string, needleRunes []rune, order int) (Match, bool) {
	match := newMatch(result, order)
	text := match.search

	// An exact command shortcode always wins over every other match. The
	// remaining results then share exact/prefix/subsequence scoring.
	if result.Kind() == ResultKindCommand && text == needle {
		match.Positions = rangePositions(utf8.RuneCountInString(text))
		match.rank, match.span, match.first = 0, len(match.Positions), 0
		return match, true
	}
	if text == needle {
		match.Positions = rangePositions(utf8.RuneCountInString(text))
		match.rank, match.span, match.first = 1, len(match.Positions), 0
		return match, true
	}
	if strings.HasPrefix(text, needle) {
		match.Positions = rangePositions(utf8.RuneCountInString(needle))
		match.rank, match.span, match.first = 2, len(match.Positions), 0
		return match, true
	}
	if positions, ok := subsequencePositions([]rune(text), needleRunes); ok {
		match.Positions = positions
		match.rank, match.span, match.first = 3, positions[len(positions)-1]-positions[0]+1, positions[0]
		return match, true
	}
	if cmd, ok := result.Command(); ok {
		if positions, ok := subsequencePositions([]rune(strings.ToLower(cmd.Desc)), needleRunes); ok {
			match.rank, match.span, match.first = 4, positions[len(positions)-1]-positions[0]+1, positions[0]
			return match, true
		}
	}
	return Match{}, false
}

func newMatch(result Result, order int) Match {
	return Match{Result: result, order: order, search: strings.ToLower(result.SearchText())}
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
