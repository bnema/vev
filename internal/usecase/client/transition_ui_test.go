package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestRenderTransitionNoticeAnimatedFrames(t *testing.T) {
	size := domain.Size{Cols: 80, Rows: 24}
	first := RenderTransitionNotice(size, 0, "Connecting to session…")
	second := RenderTransitionNotice(size, 1, "Connecting to session…")
	require.Contains(t, string(first), string(transitionSpinnerFrames[0]))
	require.Contains(t, string(second), string(transitionSpinnerFrames[1]))
	require.NotEqual(t, first, second)
}
