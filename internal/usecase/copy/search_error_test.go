package copy

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindMatchesDiscardsPartialResultsOnHistoryError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		yielded bool
	}{
		{name: "before rows"},
		{name: "after match", yielded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := modeFor([]string{"match"}, 1)
			require.True(t, m.Search("match"))
			matches := findMatches("match", func(yield func(int, []rune, []int, int) bool) error {
				if tc.yielded {
					require.True(t, yield(0, []rune("match"), []int{0, 1, 2, 3, 4}, 5))
				}
				return errors.New("corrupt cold history")
			})
			require.Nil(t, matches)
			require.False(t, m.SetSearchMatches("match", matches, 0))
			require.Empty(t, m.Searches)
			require.Equal(t, -1, m.SearchIndex)
		})
	}
}
