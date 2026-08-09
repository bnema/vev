// Package ansi emits ANSI terminal output from the frontend-neutral
// github.com/bnema/vev/pkg/vtcore model.
//
// The package owns output planning, ANSI encoding, and a transactional renderer
// shadow. It does not own Cell, Style, RGB, Frame, Damage, or RuneWidth; those
// values are aliases during this staged extraction and are owned by vtcore.
package ansi
