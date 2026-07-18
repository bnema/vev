package theme

import (
	"math/rand"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestResolveAccentThresholds(t *testing.T) {
	tests := []struct {
		name       string
		color      oklab
		background oklab
		want       bool
	}{
		{name: "chroma below", color: oklab{L: accentMinBackgroundDistance, A: accentMinChroma - 0.000001}},
		{name: "chroma at", color: oklab{L: accentMinBackgroundDistance, A: accentMinChroma}, want: true},
		{name: "chroma above", color: oklab{L: accentMinBackgroundDistance, A: accentMinChroma + 0.000001}, want: true},
		{name: "background distance below", color: oklab{L: accentMinBackgroundDistance - 0.000001, A: accentMinChroma}, background: oklab{A: accentMinChroma}},
		{name: "background distance at", color: oklab{L: accentMinBackgroundDistance, A: accentMinChroma}, background: oklab{A: accentMinChroma}, want: true},
		{name: "background distance above", color: oklab{L: accentMinBackgroundDistance + 0.000001, A: accentMinChroma}, background: oklab{A: accentMinChroma}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, accentEligible(tt.color, tt.background))
		})
	}

	a := oklab{}
	require.True(t, accentConnected(a, oklab{L: accentClusterDistance - 0.000001}))
	require.True(t, accentConnected(a, oklab{L: accentClusterDistance}))
	require.False(t, accentConnected(a, oklab{L: accentClusterDistance + 0.000001}))
}

func TestResolveAccentAuto(t *testing.T) {
	teal := renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}
	blue := renderer.RGB{R: 0x6c, G: 0x9b, B: 0xd9}
	tests := []struct {
		name  string
		theme Theme
		want  Accent
	}{
		{
			name:  "repeated teal beats repeated blue",
			theme: paletteTheme(map[int]renderer.RGB{2: teal, 10: teal, 14: teal, 4: blue, 12: blue}),
			want:  Accent{RGB: teal, Slot: 2, Known: true},
		},
		{
			name:  "singleton falls back to slot four",
			theme: paletteTheme(map[int]renderer.RGB{2: teal, 4: blue}),
			want:  Accent{RGB: blue, Slot: 4, Known: true},
		},
		{
			name:  "singleton falls back to slot twelve",
			theme: paletteTheme(map[int]renderer.RGB{2: teal, 12: blue}),
			want:  Accent{RGB: blue, Slot: 12, Known: true},
		},
		{
			name:  "tied repeated groups fall back to blue",
			theme: paletteTheme(map[int]renderer.RGB{2: teal, 10: teal, 4: blue, 12: blue}),
			want:  Accent{RGB: blue, Slot: 4, Known: true},
		},
		{
			name:  "regular bright pair beats equal size runner up",
			theme: paletteTheme(map[int]renderer.RGB{2: teal, 10: teal, 4: blue, 13: blue}),
			want:  Accent{RGB: teal, Slot: 2, Known: true},
		},
		{
			name:  "neutral slots excluded from auto",
			theme: paletteTheme(map[int]renderer.RGB{0: teal, 7: teal, 8: teal, 15: teal}),
			want:  Accent{},
		},
		{
			name: "low chroma is rejected",
			theme: paletteTheme(map[int]renderer.RGB{
				1: {R: 60, G: 61, B: 60}, 9: {R: 60, G: 61, B: 60},
			}),
			want: Accent{},
		},
		{
			name: "near background is rejected",
			theme: paletteTheme(map[int]renderer.RGB{
				1: {R: 9, G: 8, B: 12}, 9: {R: 9, G: 8, B: 12},
			}),
			want: Accent{},
		},
		{
			name:  "missing foreground yields indexed fallback only",
			theme: func() Theme { t := paletteTheme(map[int]renderer.RGB{2: teal, 10: teal}); t.HasFG = false; return t }(),
			want:  Accent{RGB: teal, Slot: 2, Known: true, IndexedOnly: true},
		},
		{
			name: "non truecolor yields indexed fallback only",
			theme: func() Theme {
				t := paletteTheme(map[int]renderer.RGB{2: teal, 10: teal})
				t.TrueColor = false
				return t
			}(),
			want: Accent{RGB: teal, Slot: 2, Known: true, IndexedOnly: true},
		},
		{
			name:  "missing background cannot infer",
			theme: func() Theme { t := paletteTheme(map[int]renderer.RGB{2: teal, 10: teal}); t.HasBG = false; return t }(),
			want:  Accent{},
		},
		{
			name: "palette disabled ignores accent policy",
			theme: func() Theme {
				t := paletteTheme(map[int]renderer.RGB{2: teal, 10: teal})
				t.UsePalette = false
				return t
			}(),
			want: Accent{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ResolveAccent(tt.theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}))
		})
	}
}

func TestResolveAccentExplicitSlot(t *testing.T) {
	teal := renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}
	blue := renderer.RGB{R: 0x6c, G: 0x9b, B: 0xd9}
	tests := []struct {
		name   string
		theme  Theme
		policy domain.ThemeAccent
		want   Accent
	}{
		{
			name:   "known explicit neutral slot bypasses auto exclusions",
			theme:  paletteTheme(map[int]renderer.RGB{0: teal, 2: blue, 10: blue}),
			policy: domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 0},
			want:   Accent{RGB: teal, Slot: 0, Known: true},
		},
		{
			name:   "known explicit slot bypasses eligibility",
			theme:  paletteTheme(map[int]renderer.RGB{3: {R: 9, G: 8, B: 12}, 2: teal, 10: teal}),
			policy: domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 3},
			want:   Accent{RGB: renderer.RGB{R: 9, G: 8, B: 12}, Slot: 3, Known: true},
		},
		{
			name:   "unknown explicit slot keeps its exact indexed decoration",
			theme:  paletteTheme(map[int]renderer.RGB{2: teal, 10: teal, 4: blue}),
			policy: domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 5},
			want:   Accent{Slot: 5, IndexedOnly: true},
		},
		{
			name:   "explicit known slot is indexed only without truecolor prerequisites",
			theme:  func() Theme { t := paletteTheme(map[int]renderer.RGB{3: teal}); t.TrueColor = false; return t }(),
			policy: domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 3},
			want:   Accent{RGB: teal, Slot: 3, Known: true, IndexedOnly: true},
		},
		{
			name:   "palette off ignores explicit slot",
			theme:  func() Theme { t := paletteTheme(map[int]renderer.RGB{3: teal}); t.UsePalette = false; return t }(),
			policy: domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 3},
			want:   Accent{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ResolveAccent(tt.theme, tt.policy))
		})
	}
}

func TestResolveAccentDeterministicAcrossShuffledPaletteConstruction(t *testing.T) {
	entries := []struct {
		slot  int
		color renderer.RGB
	}{
		{2, renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}}, {10, renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}}, {14, renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}},
		{4, renderer.RGB{R: 0x6c, G: 0x9b, B: 0xd9}}, {12, renderer.RGB{R: 0x6c, G: 0x9b, B: 0xd9}},
	}
	want := Accent{RGB: renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}, Slot: 2, Known: true}
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test shuffle.
	for i := 0; i < 50; i++ {
		rng.Shuffle(len(entries), func(a, b int) { entries[a], entries[b] = entries[b], entries[a] })
		palette := make(map[int]renderer.RGB, len(entries))
		for _, entry := range entries {
			palette[entry.slot] = entry.color
		}
		require.Equalf(t, want, ResolveAccent(paletteTheme(palette), domain.ThemeAccent{}), "shuffle %d", i)
	}
}

func TestResolveAccentMedoidAndTieBreak(t *testing.T) {
	group := accentGroup{members: 1<<0 | 1<<1 | 1<<2}
	colors := [len(accentCandidateSlots)]oklab{
		{L: 0.10, A: 0.10}, {L: 0.11, A: 0.10}, {L: 0.12, A: 0.10},
	}
	finalizeAccentGroup(&group, colors)
	require.Equal(t, uint8(1), group.rep, "central member is the medoid")

	color := renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}
	got := ResolveAccent(paletteTheme(map[int]renderer.RGB{3: color, 11: color}), domain.ThemeAccent{})
	require.Equal(t, Accent{RGB: color, Slot: 3, Known: true}, got)
}

func TestResolveAccentAllocs(t *testing.T) {
	theme := paletteTheme(map[int]renderer.RGB{
		2: {R: 0x7d, G: 0xb5, B: 0xb5}, 10: {R: 0x7d, G: 0xb5, B: 0xb5}, 14: {R: 0x7d, G: 0xb5, B: 0xb5},
		4: {R: 0x6c, G: 0x9b, B: 0xd9}, 12: {R: 0x6c, G: 0x9b, B: 0xd9},
	})
	require.Zero(t, testing.AllocsPerRun(1000, func() {
		_ = ResolveAccent(theme, domain.ThemeAccent{})
	}))
}

func paletteTheme(colors map[int]renderer.RGB) Theme {
	theme := Theme{
		Foreground: renderer.RGB{R: 0xe0, G: 0xe0, B: 0xe0},
		Background: renderer.RGB{R: 0x08, G: 0x09, B: 0x0a},
		HasFG:      true,
		HasBG:      true,
		TrueColor:  true,
		Known:      true,
		UsePalette: true,
	}
	for slot, color := range colors {
		theme.Palette[slot] = color
		theme.PaletteKnown |= 1 << slot
	}
	return theme
}
