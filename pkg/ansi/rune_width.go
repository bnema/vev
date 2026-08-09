package ansi

import "github.com/bnema/vev/pkg/vtcore"

// RuneWidth is the terminal cell width function from the frontend-neutral
// model package.
func RuneWidth(r rune) int { return vtcore.RuneWidth(r) }
