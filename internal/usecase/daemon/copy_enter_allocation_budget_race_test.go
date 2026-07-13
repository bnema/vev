//go:build race

package daemon

// Race instrumentation allocations are not part of the parent benchmark's
// allocation baseline. TestCopyEnterAllocationBudget still exercises copy
// entry and validates its result under -race.
const copyEnterAllocationBudgetEnabled = false
