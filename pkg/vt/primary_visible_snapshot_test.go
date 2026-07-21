package vt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrimaryVisibleSnapshotOwnsSavedPrimaryAcrossLiveMutations(t *testing.T) {
	s := NewScreen(8, 2)
	s.Write([]byte("primary"))
	s.Write([]byte("\x1b[?1049h"))

	captured := s.PrimaryVisibleSnapshot()
	mutated := make(chan struct{})
	go func() {
		s.Write([]byte("alternate"))
		s.Resize(3, 3)
		s.Write([]byte("\x1b[?1049lmutated"))
		close(mutated)
	}()

	blob, err := captured.Marshal()
	<-mutated
	require.NoError(t, err)
	restored := NewScreen(1, 1)
	require.NoError(t, restored.RestorePrimaryVisible(blob))
	require.Equal(t, "primary ", rowString(restored.Frame.Row(0)))
}
