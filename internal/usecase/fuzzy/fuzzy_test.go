package fuzzy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatch(t *testing.T) {
	tests := []struct {
		name  string
		text  string
		query string
		want  Score
		ok    bool
	}{
		{name: "empty query", text: "work", query: "", want: Score{Kind: Exact}, ok: true},
		{name: "exact", text: "work", query: "work", want: Score{Kind: Exact, Positions: []int{0, 1, 2, 3}, Span: 4}, ok: true},
		{name: "prefix", text: "workspace", query: "wor", want: Score{Kind: Prefix, Positions: []int{0, 1, 2}, Span: 3}, ok: true},
		{name: "subsequence", text: "a-b-c", query: "bc", want: Score{Kind: Subsequence, Positions: []int{2, 4}, Span: 3, First: 2}, ok: true},
		{name: "runes not bytes", text: "été-x", query: "x", want: Score{Kind: Subsequence, Positions: []int{4}, Span: 1, First: 4}, ok: true},
		{name: "no match", text: "work", query: "z", ok: false},
		{name: "out of order", text: "ab", query: "ba", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Match(tt.text, tt.query, []rune(tt.query))
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestLess(t *testing.T) {
	tests := []struct {
		name        string
		leftRank    int
		left        Score
		rightRank   int
		right       Score
		wantLess    bool
		wantGreater bool
	}{
		{name: "rank wins", leftRank: 0, left: Score{Span: 9}, rightRank: 1, right: Score{Span: 1}, wantLess: true},
		{name: "span breaks rank tie", left: Score{Span: 2, First: 5}, right: Score{Span: 3}, wantLess: true},
		{name: "first breaks span tie", left: Score{Span: 2, First: 1}, right: Score{Span: 2, First: 3}, wantLess: true},
		{name: "equal", left: Score{Span: 2}, right: Score{Span: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantLess, Less(tt.leftRank, tt.left, tt.rightRank, tt.right))
			require.Equal(t, tt.wantGreater, Less(tt.rightRank, tt.right, tt.leftRank, tt.left))
		})
	}
}
