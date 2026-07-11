package palette

import (
	"fmt"

	"github.com/bnema/vev/internal/usecase/command"
)

// RecentSessionHint is the immutable display data for one captured MRU rank.
type RecentSessionHint struct {
	Rank int
	Name string
}

// ContextualHints is immutable contextual palette guidance.
type ContextualHints struct {
	Kind         command.ContextHint
	Recent       []RecentSessionHint
	SelectedRank int
	Feedback     string
}

// BuildRecentSessionHints produces guidance without consulting live daemon state.
func BuildRecentSessionHints(names []string, args []string) ContextualHints {
	h := ContextualHints{Kind: command.ContextHintRecentSessions}
	h.Recent = make([]RecentSessionHint, len(names))
	for i, name := range names {
		h.Recent[i] = RecentSessionHint{Rank: i + 1, Name: name}
	}
	if len(names) == 0 {
		h.Feedback = "no recent sessions"
		return h
	}
	rank, err := command.ParsePositiveDecimal(args)
	if len(args) == 0 {
		h.Feedback = "enter a recent-session rank"
	} else if err != nil {
		h.Feedback = "rank must be one positive decimal"
	} else if rank > len(names) {
		h.Feedback = fmt.Sprintf("rank %d is unavailable", rank)
	} else {
		h.SelectedRank = rank
		h.Feedback = fmt.Sprintf("jump to recent session %d", rank)
	}
	return h
}
