//go:build !race

package daemon

// perfHistoryRows is the history depth every performance fixture builds, and
// the depth the compact-storage budgets are calibrated against.
const (
	raceEnabled     = false
	perfHistoryRows = 10_000
)
