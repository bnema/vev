// Package vt implements the server-side terminal emulator core.
//
// It tracks the visible cell grid, damage rectangles, scroll regions,
// alternate-screen state, cursor/reporting modes, and common xterm/VT control
// sequences. Wide runes that fit are stored as a head cell plus a continuation
// cell, and resize or edit operations repair row-boundary splits instead of
// leaving orphaned halves.
package vt
