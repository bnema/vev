package palette

import (
	"sort"
	"strings"

	"github.com/bnema/vev/internal/usecase/fuzzy"
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
		aScore, bScore := fuzzy.Score{Span: a.span, First: a.first}, fuzzy.Score{Span: b.span, First: b.first}
		if fuzzy.Less(a.rank, aScore, b.rank, bScore) {
			return true
		}
		if fuzzy.Less(b.rank, bScore, a.rank, aScore) {
			return false
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
		// Description matches only rank and never highlight, so Positions
		// stays empty.
		if matched, ok := fuzzy.Match(strings.ToLower(cmd.Desc), needle, needleRunes); ok {
			match.rank, match.span, match.first = 5, matched.Span, matched.First
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
	matched, ok := fuzzy.Match(strings.ToLower(text), needle, needleRunes)
	if !ok {
		return fieldScore{}, false
	}
	scored := fieldScore{positions: matched.Positions, span: matched.Span, first: matched.First}
	switch matched.Kind {
	case fuzzy.Exact:
		scored.rank = exactRank
	case fuzzy.Prefix:
		scored.rank = prefixRank
	default:
		scored.rank = subsequenceRank
	}
	for i := range scored.positions {
		scored.positions[i] += displayOffset
	}
	return scored, true
}

func (s fieldScore) betterThan(other fieldScore) bool {
	return fuzzy.Less(s.rank, fuzzy.Score{Span: s.span, First: s.first}, other.rank, fuzzy.Score{Span: other.span, First: other.first})
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
