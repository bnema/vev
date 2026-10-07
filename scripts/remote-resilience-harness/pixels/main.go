// Command pixels compares the fixed 1280x720 fixture's terminal content.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/png"
	"os"
)

const fixtureMargin = 100
const minimumChangedPixels = 20

func load(path string) (image.Image, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	im, _, err := image.Decode(f)
	return im, err
}

func compare(a, b image.Image) (int, error) {
	if a.Bounds() != b.Bounds() {
		return 0, errors.New("capture dimensions changed")
	}
	bounds := a.Bounds()
	if bounds.Dx() <= 2*fixtureMargin || bounds.Dy() <= 2*fixtureMargin {
		return 0, errors.New("capture has no comparison region")
	}
	changed := 0
	for y := bounds.Min.Y + fixtureMargin; y < bounds.Max.Y-fixtureMargin; y++ {
		for x := bounds.Min.X + fixtureMargin; x < bounds.Max.X-fixtureMargin; x++ {
			ar, ag, ab, _ := a.At(x, y).RGBA()
			br, bg, bb, _ := b.At(x, y).RGBA()
			if ar != br || ag != bg || ab != bb {
				changed++
			}
		}
	}
	return changed, nil
}

func execute(args []string) (int, error) {
	if len(args) != 2 {
		return 2, errors.New("usage: pixels before.png after.png")
	}
	a, err := load(args[0])
	if err != nil {
		return 2, err
	}
	b, err := load(args[1])
	if err != nil {
		return 2, err
	}
	changed, err := compare(a, b)
	if err != nil {
		return 2, err
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]int{"changed_pixels": changed}); err != nil {
		return 2, err
	}
	if changed < minimumChangedPixels {
		return 1, nil
	}
	return 0, nil
}
func main() {
	code, err := execute(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}
