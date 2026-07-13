//go:build !race

package daemon

// copyEnterAllocationBudgetEnabled keeps the allocation gate aligned with the
// non-race parent benchmark, whose allocation baseline defines the budget.
const copyEnterAllocationBudgetEnabled = true
