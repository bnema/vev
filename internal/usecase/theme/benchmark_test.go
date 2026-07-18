package theme

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

var benchmarkStylesSink Styles

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
