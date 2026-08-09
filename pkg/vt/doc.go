// Package vt implements the frontend-neutral terminal emulator core.
//
// It tracks the visible cell grid, damage rectangles, scroll regions,
// alternate-screen state, cursor/reporting modes, and common xterm/VT control
// sequences. Wide runes that fit are stored as a head cell plus a continuation
// cell, and resize or edit operations repair row-boundary splits instead of
// leaving orphaned halves.
//
// Stateful values are single-owner and are not internally synchronized. The
// owner must serialize Write, Resize, Snapshot, and History mutations. Write
// parses synchronously: every callback runs before Write returns and callbacks
// are delivered in the order their control sequences are encountered. A line
// eviction callback receives a stable row copy; response bytes should be
// consumed or copied during the callback. Other callback arguments are values
// or strings owned by the callback.
//
// Snapshot and HistorySnapshotView capture owned state without sealing or
// mutating the live owner. Row methods return owned copies. BorrowedRow and
// HistoryView.Range expose immutable backing storage: callers must not mutate
// it, and Range callbacks must not retain their row after returning. A
// HistoryChunk pointer has stable identity for the lifetime of the view, so
// consumers can reuse unchanged sealed chunks. Direct Frame.Row access is a
// mutable borrow valid only until the frame scrolls or resizes.
package vt
