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

	identity, label, offset, hasTerms := result.searchTerms()
	if hasTerms {
		exactRank := 1
		if result.Kind() == ResultKindCommand {
			exactRank = 0
		}
		best, matched := scoreField(identity, needle, needleRunes, offset, exactRank, 2, 3)
		if label != identity {
			if candidate, ok := scoreField(label, needle, needleRunes, offset, exactRank, 2, 3); ok && (!matched || candidate.betterThan(best)) {
				best, matched = candidate, true
			}
		}
		if matched {
			best.apply(&match)
			return match, true
		}
	}

	// Action text remains searchable, but semantic codes and session labels
	// outrank matches caused only by a rendered action prefix.
	if fallback, ok := scoreField(match.search, needle, needleRunes, 0, 4, 4, 4); ok {
		fallback.apply(&match)
		return match, true
	}
	if cmd, ok := result.Command(); ok {
		if positions, ok := subsequencePositions([]rune(strings.ToLower(cmd.Desc)), needleRunes); ok {
			match.rank, match.span, match.first = 5, positions[len(positions)-1]-positions[0]+1, positions[0]
			return match, true
		}
	}
	return Match{}, false
}

type fieldScore struct {
	positions []int
	rank      int
	span      int
	first     int
}

func scoreField(text, needle string, needleRunes []rune, displayOffset, exactRank, prefixRank, subsequenceRank int) (fieldScore, bool) {
	text = strings.ToLower(text)
	var scored fieldScore
	switch {
	case text == needle:
		scored.positions = rangePositions(utf8.RuneCountInString(text))
		scored.rank = exactRank
	case strings.HasPrefix(text, needle):
		scored.positions = rangePositions(utf8.RuneCountInString(needle))
		scored.rank = prefixRank
	default:
		positions, ok := subsequencePositions([]rune(text), needleRunes)
		if !ok {
			return fieldScore{}, false
		}
		scored.positions = positions
		scored.rank = subsequenceRank
	}
	scored.span = scored.positions[len(scored.positions)-1] - scored.positions[0] + 1
	scored.first = scored.positions[0]
	for i := range scored.positions {
		scored.positions[i] += displayOffset
	}
	return scored, true
}

func (s fieldScore) betterThan(other fieldScore) bool {
	if s.rank != other.rank {
		return s.rank < other.rank
	}
	if s.span != other.span {
		return s.span < other.span
	}
	return s.first < other.first
}

func (s fieldScore) apply(match *Match) {
	match.Positions = s.positions
	match.rank = s.rank
	match.span = s.span
	match.first = s.first
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
