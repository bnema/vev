package ansi

import "testing"

func markBenchmarkFrame(f *Frame) {
	for y := range f.Height {
		for x := range f.Width {
			f.Set(x, y, Cell{Rune: rune('A' + (y*f.Width+x)%26), Style: DefaultStyle()})
		}
	}
}

func setBenchmarkShadow(r *Renderer, frame Frame) {
	committed := r.committedFrame()
	replaceFrame(&committed, frame)
	r.setCommittedFrame(committed)
}

func BenchmarkRendererFullFrameDraw(b *testing.B) {
	frame := NewFrame(120, 40)
	markBenchmarkFrame(&frame)
	r := New(Capabilities{})
	damage := []Damage{FullRedraw()}
	b.ReportAllocs()
	var outBytes int64
	for b.Loop() {
		r.Reset()
		out, err := r.Draw(frame, damage)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("expected renderer output")
		}
		outBytes += int64(len(out))
	}
	b.ReportMetric(float64(outBytes)/float64(b.N), "outbytes/op")
}

func BenchmarkRendererFragmentedDamage(b *testing.B) {
	const (
		width  = 120
		height = 40
	)

	baseline := NewFrame(width, height)
	markBenchmarkFrame(&baseline)
	frame := NewFrame(width, height)
	markBenchmarkFrame(&frame)

	// Create 256 overlapping or vertically adjacent text rectangles in reverse
	// row/column order. Canonical damage planning must sort and merge these into
	// stable per-row spans before emission.
	damage := make([]Damage, 0, 256)
	for row := 15; row >= 0; row-- {
		for col := 15; col >= 0; col-- {
			x, y := col*7, row*2
			damage = append(damage, Damage{Kind: DamageText, X: x, Y: y, Width: 8, Height: 2})
			for dy := range 2 {
				for dx := range 8 {
					frame.Set(x+dx, y+dy, Cell{Rune: 'z', Style: DefaultStyle()})
				}
			}
		}
	}

	r := New(Capabilities{})
	b.ReportAllocs()
	var outBytes int64
	for b.Loop() {
		// Each iteration starts with the identical pre-update terminal shadow.
		setBenchmarkShadow(r, baseline)
		out, err := r.Draw(frame, damage)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("expected renderer output")
		}
		outBytes += int64(len(out))
	}
	b.ReportMetric(float64(outBytes)/float64(b.N), "outbytes/op")
}

func BenchmarkRendererIncrementalOneCell(b *testing.B) {
	for _, tt := range []struct {
		name          string
		prepareCommit bool
	}{
		{name: "PrepareCommit", prepareCommit: true},
		{name: "Draw"},
	} {
		b.Run(tt.name, func(b *testing.B) {
			frame := NewFrame(120, 40)
			markBenchmarkFrame(&frame)
			r := New(Capabilities{})
			if tt.prepareCommit {
				initial, err := r.Prepare(frame, []Damage{FullRedraw()}, false)
				if err != nil {
					b.Fatal(err)
				}
				initial.Commit()
			} else if _, err := r.Draw(frame, []Damage{FullRedraw()}); err != nil {
				b.Fatal(err)
			}
			damage := []Damage{{Kind: DamageText, X: 60, Y: 20, Width: 1, Height: 1}}

			b.ReportAllocs()
			var outBytes int64
			changed := false
			for b.Loop() {
				changed = !changed
				cell := Cell{Rune: 'X', Style: DefaultStyle()}
				if changed {
					cell.Rune = 'Y'
				}
				frame.Set(60, 20, cell)
				if tt.prepareCommit {
					prepared, err := r.Prepare(frame, damage, false)
					if err != nil {
						b.Fatal(err)
					}
					if len(prepared.Bytes()) == 0 {
						b.Fatal("expected renderer output")
					}
					outBytes += int64(len(prepared.Bytes()))
					prepared.Commit()
					continue
				}
				out, err := r.Draw(frame, damage)
				if err != nil {
					b.Fatal(err)
				}
				if len(out) == 0 {
					b.Fatal("expected renderer output")
				}
				outBytes += int64(len(out))
			}
			b.ReportMetric(float64(outBytes)/float64(b.N), "outbytes/op")
		})
	}
}

func BenchmarkRendererIncrementalNoBytePrepareCommit(b *testing.B) {
	frame := NewFrame(120, 40)
	markBenchmarkFrame(&frame)
	r := New(Capabilities{})
	initial, err := r.Prepare(frame, []Damage{FullRedraw()}, false)
	if err != nil {
		b.Fatal(err)
	}
	initial.Commit()

	b.ReportAllocs()
	for b.Loop() {
		prepared, err := r.Prepare(frame, nil, false)
		if err != nil {
			b.Fatal(err)
		}
		if len(prepared.Bytes()) != 0 {
			b.Fatal("expected no output")
		}
		prepared.Commit()
	}
}

func BenchmarkRendererBroadRegularDamage(b *testing.B) {
	const (
		width  = 120
		height = 40
	)
	baseline := NewFrame(width, height)
	markBenchmarkFrame(&baseline)
	frame := baseline.Clone()
	for y := range height {
		frame.Set(0, y, Cell{Rune: 'z', Style: DefaultStyle()})
	}
	damage := make([]Damage, height)
	for y := range height {
		damage[y] = Damage{Kind: DamageText, X: 0, Y: y, Width: width, Height: 1}
	}

	r := New(Capabilities{})
	b.ReportAllocs()
	var outBytes int64
	for b.Loop() {
		setBenchmarkShadow(r, baseline)
		out, err := r.Draw(frame, damage)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("expected renderer output")
		}
		outBytes += int64(len(out))
	}
	b.ReportMetric(float64(outBytes)/float64(b.N), "outbytes/op")
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
	var outBytes int64
	for b.Loop() {
		setBenchmarkShadow(r, frame)
		out, err := r.Draw(scrolled, damage)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("expected renderer output")
		}
		outBytes += int64(len(out))
	}
	b.ReportMetric(float64(outBytes)/float64(b.N), "outbytes/op")
}
