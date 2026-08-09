package copy

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func navigatorDocument() *Document {
	return NewDocument(NewSnapshotFromRows([][]renderer.Cell{
		documentCells("abcdef"),
		documentCells("xy"),
		{{Rune: '界'}, {Continuation: true}, {Rune: 'z'}},
		{},
		documentCells("omega"),
	}, 6, 2), " -_@")
}

func TestNavigatorHorizontalSkipsContinuations(t *testing.T) {
	doc := navigatorDocument()

	tests := []struct {
		name string
		move func(*Navigator, *Document) bool
		from Pos
		want Pos
	}{
		{"right skips wide continuation", (*Navigator).Right, Pos{Row: 2, Col: 0}, Pos{Row: 2, Col: 2}},
		{"left skips wide continuation", (*Navigator).Left, Pos{Row: 2, Col: 2}, Pos{Row: 2, Col: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			navigator := NewNavigator(tt.from)

			require.True(t, tt.move(&navigator, doc))
			require.Equal(t, tt.want, navigator.Pos)
			require.Equal(t, tt.want.Col, navigator.PreferredCol)
		})
	}
}

func TestNavigatorVerticalRetainsPreferredColumn(t *testing.T) {
	doc := navigatorDocument()
	navigator := NewNavigator(Pos{Row: 0, Col: 5})

	require.True(t, navigator.Down(doc))
	require.Equal(t, Pos{Row: 1, Col: 1}, navigator.Pos)
	require.Equal(t, 5, navigator.PreferredCol)

	require.True(t, navigator.Down(doc))
	require.Equal(t, Pos{Row: 2, Col: 2}, navigator.Pos)
	require.Equal(t, 5, navigator.PreferredCol)

	require.True(t, navigator.Down(doc))
	require.Equal(t, Pos{Row: 3, Col: 0}, navigator.Pos)
	require.Equal(t, 5, navigator.PreferredCol)

	require.True(t, navigator.Down(doc))
	require.Equal(t, Pos{Row: 4, Col: 4}, navigator.Pos)
	require.Equal(t, 5, navigator.PreferredCol)

	require.True(t, navigator.Up(doc))
	require.Equal(t, Pos{Row: 3, Col: 0}, navigator.Pos)
	require.Equal(t, 5, navigator.PreferredCol)

	require.True(t, navigator.Up(doc))
	require.Equal(t, Pos{Row: 2, Col: 2}, navigator.Pos)
	require.Equal(t, 5, navigator.PreferredCol)
}

func TestNavigatorWordMotionsCrossRows(t *testing.T) {
	doc := navigatorDocument()

	tests := []struct {
		name string
		move func(*Navigator, *Document) bool
		from Pos
		want Pos
	}{
		{"word next", (*Navigator).WordNext, Pos{Row: 0, Col: 5}, Pos{Row: 1, Col: 0}},
		{"word backward", (*Navigator).WordBackward, Pos{Row: 1, Col: 0}, Pos{Row: 0, Col: 0}},
		{"word end", (*Navigator).WordEnd, Pos{Row: 1, Col: 1}, Pos{Row: 2, Col: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			navigator := NewNavigator(tt.from)

			require.True(t, tt.move(&navigator, doc))
			require.Equal(t, tt.want, navigator.Pos)
			require.Equal(t, tt.want.Col, navigator.PreferredCol)
		})
	}
}

func TestNavigatorEmptyTopBottomPageAndSet(t *testing.T) {
	doc := navigatorDocument()

	t.Run("empty row is stable", func(t *testing.T) {
		navigator := NewNavigator(Pos{Row: 2, Col: 2})
		require.True(t, navigator.Down(doc))
		require.Equal(t, Pos{Row: 3, Col: 0}, navigator.Pos)
		require.False(t, navigator.Right(doc))
		require.Equal(t, Pos{Row: 3, Col: 0}, navigator.Pos)
	})

	t.Run("top and bottom reset the preferred column", func(t *testing.T) {
		navigator := NewNavigator(Pos{Row: 0, Col: 5})
		require.True(t, navigator.Down(doc))
		require.Equal(t, Pos{Row: 1, Col: 1}, navigator.Pos)
		require.Equal(t, 5, navigator.PreferredCol)
		require.True(t, navigator.Top(doc))
		require.Equal(t, Pos{Row: 0, Col: 1}, navigator.Pos)
		require.Equal(t, 1, navigator.PreferredCol)
		require.True(t, navigator.Bottom(doc))
		require.Equal(t, Pos{Row: 4, Col: 1}, navigator.Pos)
		require.Equal(t, 1, navigator.PreferredCol)
	})

	t.Run("page keeps preferred column", func(t *testing.T) {
		navigator := NewNavigator(Pos{Row: 0, Col: 5})
		require.True(t, navigator.Page(doc, 2))
		require.Equal(t, Pos{Row: 2, Col: 2}, navigator.Pos)
		require.Equal(t, 5, navigator.PreferredCol)
		require.True(t, navigator.Page(doc, 2))
		require.Equal(t, Pos{Row: 4, Col: 4}, navigator.Pos)
		require.Equal(t, 5, navigator.PreferredCol)
	})

	t.Run("set normalizes continuation and updates preferred column", func(t *testing.T) {
		navigator := NewNavigator(Pos{Row: 0, Col: 0})
		require.True(t, navigator.Set(doc, Pos{Row: 2, Col: 1}))
		require.Equal(t, Pos{Row: 2, Col: 0}, navigator.Pos)
		require.Equal(t, 0, navigator.PreferredCol)
		require.False(t, navigator.Set(doc, Pos{Row: 9, Col: 0}))
		require.Equal(t, Pos{Row: 2, Col: 0}, navigator.Pos)
	})
}
