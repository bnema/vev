//go:build race

package agentactivity

// The race detector randomly drops sync.Pool items, so regexp machine reuse
// allocates and allocation counts are meaningless.
const raceEnabled = true
