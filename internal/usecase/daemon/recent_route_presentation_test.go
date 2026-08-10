package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/stretchr/testify/require"
)

func TestFormatRecentRoutePresentations(t *testing.T) {
	tests := []struct {
		name  string
		input []recentRoutePresentation
		want  []recentRouteDisplay
	}{
		{
			name: "unique local name stays compact",
			input: []recentRoutePresentation{{
				name: "editor", kind: recentRouteLocal,
			}},
			want: []recentRouteDisplay{{
				name: "editor", kind: recentRouteLocal,
			}},
		},
		{
			name: "remote name is host qualified",
			input: []recentRoutePresentation{{
				name: "logs", hostLabel: "edge", kind: recentRouteRemote,
			}},
			want: []recentRouteDisplay{{
				name: "logs@edge", kind: recentRouteRemote,
			}},
		},
		{
			name: "duplicate local and remote names qualify every entry",
			input: []recentRoutePresentation{
				{name: "logs", kind: recentRouteLocal},
				{name: "logs", hostLabel: "edge", kind: recentRouteRemote},
			},
			want: []recentRouteDisplay{
				{name: "logs@local", kind: recentRouteLocal},
				{name: "logs@edge", kind: recentRouteRemote},
			},
		},
		{
			name: "missing host uses kind fallback",
			input: []recentRoutePresentation{{
				name: "logs", kind: recentRouteRemote,
			}},
			want: []recentRouteDisplay{{
				name: "logs@remote", kind: recentRouteRemote,
			}},
		},
		{
			name: "authoritative markers survive formatting",
			input: []recentRoutePresentation{{
				name: "scratch", kind: recentRouteLocal, ephemeral: true, attention: true,
			}},
			want: []recentRouteDisplay{{
				name: "scratch*", kind: recentRouteLocal, ephemeral: true, attention: true,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := append([]recentRoutePresentation(nil), tt.input...)
			got := formatRecentRoutePresentations(input)

			require.Equal(t, tt.want, got)
			require.Equal(t, input, tt.input, "formatting must not mutate the source snapshot")
		})
	}
}

func TestRecentSessionHintsAndRankedRenderingShareCanonicalLabels(t *testing.T) {
	snapshot := ports.RecentRouteSnapshot{
		Generation: 1,
		Entries: []ports.RecentRouteEntry{
			{Key: 1, Generation: 1, Name: "logs", Kind: ports.RouteKindLocal, Attention: true},
			{Key: 2, Generation: 1, Name: "logs", Kind: ports.RouteKindLocal},
		},
	}

	hints := recentRouteHints(snapshot, nil)
	require.Equal(t, []palette.RecentSessionHint{
		{Rank: 1, Name: "logs@local", SnapshotGeneration: 1, Key: 1, Generation: 1},
		{Rank: 2, Name: "logs@local", SnapshotGeneration: 1, Key: 2, Generation: 1},
	}, hints.Recent)

	hints = palette.ContextualHints{
		Kind:         command.ContextHintRecentSessions,
		SelectedRank: 9,
		Recent: []palette.RecentSessionHint{
			{Rank: 4, Name: "stale-label"},
			{Rank: 9, Name: "stale-label"},
		},
	}
	ranked := rankedRecentForHintsWithSnapshot(&hints, snapshot)

	require.Equal(t, []rankedRecent{
		{rank: 4, name: "logs@local", kind: recentRouteLocal, attention: true},
		{rank: 9, name: "logs@local", kind: recentRouteLocal, selected: true},
	}, ranked)
}

func TestFitRecentRouteEntriesKeepsWholeWideAndAttentionEntries(t *testing.T) {
	tests := []struct {
		name    string
		entries []recentRouteDisplay
		width   int
		want    []string
	}{
		{
			name: "wide rune consumes its full cell width",
			entries: []recentRouteDisplay{
				{name: "界"},
				{name: "b"},
			},
			width: 12,
			want:  []string{"界"},
		},
		{
			name: "attention bell is part of the entry width",
			entries: []recentRouteDisplay{
				{name: "a", attention: true},
				{name: "b"},
			},
			width: 12,
			want:  []string{"a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fitMRU(tt.entries, tt.width, 0, "copy")
			names := make([]string, len(got))
			for i, entry := range got {
				names[i] = entry.name
			}
			require.Equal(t, tt.want, names)
		})
	}
}

func TestRankedRecentKeepsCapturedRanksWhenFitting(t *testing.T) {
	entries := []rankedRecent{
		{rank: 3, name: "wide界"},
		{rank: 7, name: "next"},
	}

	got := fitRankedRecent(entries, 17, 0, "")

	require.Equal(t, []rankedRecent{{rank: 3, name: "wide界"}}, got)
}
