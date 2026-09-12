//go:build race

package daemon

// The race detector makes history construction, sealing, and cloning dominate
// this package's runtime, while the allocation and byte budgets these fixtures
// feed are calibrated without it. The race build therefore keeps a smaller but
// still multi-chunk history.
const (
	raceEnabled     = true
	perfHistoryRows = 600
)
