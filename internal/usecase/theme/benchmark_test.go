package theme

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
)

var (
	benchmarkStylesSink   Styles
	benchmarkResolvedSink ResolvedTheme
)

func benchmarkPaletteTheme() Theme {
	return Theme{
		Foreground: renderer.RGB{R: 0xd8, G: 0xdc, B: 0xe8},
		Background: renderer.RGB{R: 0x08, G: 0x09, B: 0x0a},
		Palette: [16]renderer.RGB{
			2:  {R: 0x7d, G: 0xb5, B: 0xb5},
			4:  {R: 0x6c, G: 0x9b, B: 0xd9},
			10: {R: 0x7d, G: 0xb5, B: 0xb5},
			12: {R: 0x6c, G: 0x9b, B: 0xd9},
			14: {R: 0x7d, G: 0xb5, B: 0xb5},
		},
		PaletteKnown: 1<<2 | 1<<4 | 1<<10 | 1<<12 | 1<<14,
		HasFG:        true,
		HasBG:        true,
		TrueColor:    true,
		Known:        true,
		SchemeKnown:  true,
		UsePalette:   true,
	}
}

func TestNewStylesAllocations(t *testing.T) {
	theme := benchmarkPaletteTheme()
	allocs := testing.AllocsPerRun(1000, func() {
		benchmarkStylesSink = NewStyles(theme)
	})
	if allocs != 0 {
		t.Fatalf("NewStyles allocations/op = %v, want 0", allocs)
	}
}

func BenchmarkNewStyles(b *testing.B) {
	theme := benchmarkPaletteTheme()
	b.ReportAllocs()
	for b.Loop() {
		benchmarkStylesSink = NewStyles(theme)
	}
}

func TestResolveAllocations(t *testing.T) {
	theme := benchmarkPaletteTheme()
	allocs := testing.AllocsPerRun(1000, func() {
		benchmarkResolvedSink = Resolve(theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto})
	})
	if allocs != 0 {
		t.Fatalf("Resolve allocations/op = %v, want 0", allocs)
	}
}

func BenchmarkResolve(b *testing.B) {
	theme := benchmarkPaletteTheme()
	policy := domain.ThemeAccent{Mode: domain.ThemeAccentAuto}
	b.ReportAllocs()
	for b.Loop() {
		benchmarkResolvedSink = Resolve(theme, policy)
	}
}
