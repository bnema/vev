package main

import (
	"image"
	"image/color"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompare(t *testing.T) {
	for _, n := range []int{0, 19, 20} {
		a := image.NewRGBA(image.Rect(0, 0, 300, 300))
		b := image.NewRGBA(a.Bounds())
		for x := 0; x < n; x++ {
			b.Set(100+x, 100, color.White)
		}
		got, err := compare(a, b)
		require.NoError(t, err)
		require.Equal(t, n, got)
	}
	for _, bounds := range []image.Rectangle{image.Rect(0, 0, 200, 300), image.Rect(0, 0, 300, 200)} {
		_, err := compare(image.NewRGBA(bounds), image.NewRGBA(bounds))
		require.Error(t, err)
	}
	_, err := compare(image.NewRGBA(image.Rect(0, 0, 300, 300)), image.NewRGBA(image.Rect(0, 0, 400, 300)))
	require.Error(t, err)
}
