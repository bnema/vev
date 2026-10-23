// Package agentactivity detects what an agent CLI running in a pane is doing
// from its process identity and its rendered screen text.
//
// The package is pure: it performs no I/O, reads no clock, and holds no
// locks. The daemon identifies the foreground process ([Identify]), copies
// the rows ending at the lowest non-blank grid row ([Window]) into an
// [Extractor], classifies the text with [Classify], and feeds the resulting
// [Signal] to a per-pane [Tracker] together with the current time. The
// Tracker debounces weak idle signals and reports state transitions and the
// next deadline at which it must be resolved again.
package agentactivity
