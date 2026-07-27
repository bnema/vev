package vt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecoveryTranscriptSnapshotOwnsSavedPrimaryAcrossLiveMutations(t *testing.T) {
	s := NewScreen(8, 2)
	s.Write([]byte("primary"))
	s.Write([]byte("\x1b[?1049h"))

	captured := s.RecoveryTranscriptSnapshot()
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
	view, err := UnmarshalHistory(blob)
	require.NoError(t, err)
	require.Equal(t, []string{"primary "}, historyViewTexts(view))
}
