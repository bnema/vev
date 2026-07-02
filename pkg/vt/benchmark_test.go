package vt

import "testing"

func BenchmarkScreenShellRedrawBurst(b *testing.B) {
	chunk := []byte("\r\x1b[K❯ abc\x1b[90m autosuggestion\x1b[39m\r\x1b[5C")
	b.ReportAllocs()
	for b.Loop() {
		s := NewScreen(120, 40)
		for range 200 {
			s.Write(chunk)
			s.ClearDamage()
		}
	}
}

func BenchmarkScreenFullscreenScrollRegion(b *testing.B) {
	chunk := []byte("\x1b[2;39r\x1b[39;1Hline\n")
	b.ReportAllocs()
	for b.Loop() {
		s := NewScreen(120, 40)
		for range 500 {
			s.Write(chunk)
			s.ClearDamage()
		}
	}
}
