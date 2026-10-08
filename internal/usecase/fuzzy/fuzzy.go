// Package fuzzy scores one already case-folded text against a folded query.
//
// A field matches exactly, by prefix, or as an in-order rune subsequence.
// Callers map each Kind to their own rank and order matches with Less.
package fuzzy

import "strings"

// Kind is how a field matched the query.
type Kind uint8

// Kinds are ordered best first, so a caller may use the Kind itself as a rank.
const (
	Exact Kind = iota
	Prefix
	Subsequence
)

// Score is one field match. Positions are rune indexes into the text.
type Score struct {
	Kind      Kind
	Positions []int
	// Span is the rune distance from the first to the last matched position.
	Span  int
	First int
}

// Match scores folded text against query, whose runes are needle. An empty
// needle matches as Exact with no positions.
func Match(text, query string, needle []rune) (Score, bool) {
	if len(needle) == 0 {
		return Score{Kind: Exact}, true
	}
	var score Score
	switch {
	case text == query:
		score = Score{Kind: Exact, Positions: rangePositions(len(needle))}
	case strings.HasPrefix(text, query):
		score = Score{Kind: Prefix, Positions: rangePositions(len(needle))}
	default:
		positions, ok := subsequencePositions([]rune(text), needle)
		if !ok {
			return Score{}, false
		}
		score = Score{Kind: Subsequence, Positions: positions}
	}
	score.Span = score.Positions[len(score.Positions)-1] - score.Positions[0] + 1
	score.First = score.Positions[0]
	return score, true
}

// Less orders two scores whose ranks the caller derived from their Kind:
// lower rank, then tighter span, then earlier first position.
func Less(leftRank int, left Score, rightRank int, right Score) bool {
	if leftRank != rightRank {
		return leftRank < rightRank
	}
	if left.Span != right.Span {
		return left.Span < right.Span
	}
	return left.First < right.First
}

// rangePositions returns the positions 0..n-1.
func rangePositions(n int) []int {
	positions := make([]int, n)
	for i := range positions {
		positions[i] = i
	}
	return positions
}

// subsequencePositions returns the first in-order positions of needle in
// haystack.
func subsequencePositions(haystack, needle []rune) ([]int, bool) {
	if len(needle) == 0 {
		return nil, true
	}
	positions := make([]int, 0, len(needle))
	next := 0
	for idx, r := range haystack {
		if r != needle[next] {
			continue
		}
		positions = append(positions, idx)
		next++
		if next == len(needle) {
			return positions, true
		}
	}
	return nil, false
}
