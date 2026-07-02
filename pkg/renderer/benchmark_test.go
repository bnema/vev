package renderer

import "testing"

func markBenchmarkFrame(f *Frame) {
	for y := range f.Height {
		for x := range f.Width {
			f.Set(x, y, Cell{Rune: rune('A' + (y*f.Width+x)%26), Style: DefaultStyle()})
		}
	}
}

func BenchmarkRendererFullFrameDraw(b *testing.B) {
	frame := NewFrame(120, 40)
	markBenchmarkFrame(&frame)
	r := New(Capabilities{})
	damage := []Damage{FullRedraw()}
	b.ReportAllocs()
	for b.Loop() {
		r.Reset()
		out, err := r.Draw(frame, damage)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("expected renderer output")
		}
	}
}

func BenchmarkRendererScrollFastPath(b *testing.B) {
	frame := NewFrame(120, 40)
	markBenchmarkFrame(&frame)
	r := New(Capabilities{})
	if _, err := r.Draw(frame, []Damage{FullRedraw()}); err != nil {
		b.Fatal(err)
	}

	scrolled := NewFrame(120, 40)
	copy(scrolled.Cells[0:39*120], frame.Cells[120:40*120])
	for i := 39 * 120; i < 40*120; i++ {
		scrolled.Cells[i] = BlankCell()
	}
	for x, r := range []rune("new output line") {
		scrolled.Set(x, 39, Cell{Rune: r, Style: DefaultStyle()})
	}
	damage := []Damage{
		{Kind: DamageScrollUp, X: 0, Y: 0, Width: 120, Height: 40, Count: 1},
		{Kind: DamageText, X: 0, Y: 39, Width: 120, Height: 1, Count: 1},
	}

	b.ReportAllocs()
	for b.Loop() {
		r.replaceShadow(frame)
		out, err := r.Draw(scrolled, damage)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("expected renderer output")
		}
	}
}
