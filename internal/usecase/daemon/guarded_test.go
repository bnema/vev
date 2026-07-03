package daemon

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGuardedGetSetWith(t *testing.T) {
	var g Guarded[int]

	g.Set(10)
	require.Equal(t, 10, g.Get())

	g.With(func(v *int) { *v += 5 })
	require.Equal(t, 15, g.Get())
}

func TestGuardedConcurrentAccess(t *testing.T) {
	var g Guarded[int]
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			for range 1000 {
				g.With(func(v *int) { *v++ })
			}
		})
	}
	wg.Wait()

	require.Equal(t, 20000, g.Get())
}
