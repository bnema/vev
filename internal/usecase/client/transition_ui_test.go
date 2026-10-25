package client

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
)

func TestRenderTransitionNoticeAnimatedFrames(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		size := domain.Size{Cols: 80, Rows: 24}
		first := RenderTransitionNotice(size, 0, "Connecting to session…", ui.GlyphSet{})
		second := RenderTransitionNotice(size, 1, "Connecting to session…", ui.GlyphSet{})
		require.Contains(t, string(first), string(transitionSpinnerFrames[0]))
		require.Contains(t, string(second), string(transitionSpinnerFrames[1]))
		require.NotEqual(t, first, second)
	})
}

func TestRenderTransitionNoticeGlyphSets(t *testing.T) {
	size := domain.Size{Cols: 60, Rows: 10}
	tests := []struct {
		name   string
		glyphs ui.GlyphSet
		want   rune
	}{
		{name: "nerd keeps braille", glyphs: ui.NerdGlyphs(), want: transitionSpinnerFrames[1]},
		{name: "unicode", glyphs: ui.UnicodeGlyphs(), want: '◓'},
		{name: "ascii", glyphs: ui.ASCIIGlyphs(), want: '/'},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				require.Contains(t, string(RenderTransitionNotice(size, 1, "Connecting", tt.glyphs)), string(tt.want))
			})
		})
	}
}
