package vt

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrimaryVisibleRowsCopiesActivePrimaryScreen(t *testing.T) {
	s := NewScreen(6, 2)
	s.Write([]byte("hello"))

	rows := s.PrimaryVisibleRows()
	require.Len(t, rows, 2)
	require.Equal(t, "hello ", rowString(rows[0]))

	rows[0][0].Rune = 'X'
	rows[0] = nil

	require.Equal(t, "hello ", rowString(s.Frame.Row(0)))
	require.Equal(t, "hello ", rowString(s.PrimaryVisibleRows()[0]))
}

func TestPrimaryVisibleRowsUsesSavedPrimaryScreenWhenAlternateActive(t *testing.T) {
	s := NewScreen(8, 2)
	s.Write([]byte("primary"))
	s.Write([]byte("\x1b[?1049h"))
	s.Write([]byte("alt"))

	rows := s.PrimaryVisibleRows()
	require.Len(t, rows, 2)
	require.Equal(t, "primary ", rowString(rows[0]))
	require.False(t, strings.Contains(rowString(rows[0]), "alt"))

	rows[0][0].Rune = 'X'
	s.Write([]byte("\x1b[?1049l"))
	require.Equal(t, "primary ", rowString(s.Frame.Row(0)))
}
