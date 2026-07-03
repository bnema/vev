package palette

import (
	"sort"
	"strings"

	"github.com/bnema/vev/internal/usecase/command"
)

type Match struct {
	Command   command.Command
	Positions []int
	rank      int
	span      int
	first     int
	order     int
}

func Fuzzy(commands []command.Command, query string) []Match {
	if query == "" {
		out := make([]Match, len(commands))
		for i, cmd := range commands {
			out[i] = Match{Command: cmd, order: i}
		}
		return out
	}
	var out []Match
	for i, cmd := range commands {
		if m, ok := score(cmd, query, i); ok {
			out = append(out, m)
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
		if a.Command.Code != b.Command.Code {
			return a.Command.Code < b.Command.Code
		}
		return a.order < b.order
	})
	return out
}

func score(cmd command.Command, query string, order int) (Match, bool) {
	code := []rune(cmd.Code)
	q := []rune(query)
	codeLower := strings.ToLower(cmd.Code)
	qLower := strings.ToLower(query)
	if codeLower == qLower {
		return Match{Command: cmd, Positions: rangePositions(len(code)), rank: 0, span: len(code), first: 0, order: order}, true
	}
	if strings.HasPrefix(codeLower, qLower) {
		return Match{Command: cmd, Positions: rangePositions(len(q)), rank: 1, span: len(q), first: 0, order: order}, true
	}
	if positions, ok := subsequencePositions([]rune(codeLower), []rune(qLower)); ok {
		return Match{Command: cmd, Positions: positions, rank: 2, span: positions[len(positions)-1] - positions[0] + 1, first: positions[0], order: order}, true
	}
	text := strings.ToLower(cmd.Name + " " + cmd.Desc)
	if positions, ok := subsequencePositions([]rune(text), []rune(qLower)); ok {
		return Match{Command: cmd, rank: 3, span: positions[len(positions)-1] - positions[0] + 1, first: positions[0], order: order}, true
	}
	return Match{}, false
}

func rangePositions(n int) []int {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	return p
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
