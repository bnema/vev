package ansi

import "github.com/bnema/vev/pkg/vtcore"

// Frame is an alias for the frontend-neutral terminal grid. ANSI rendering
// consumes it but does not own its model or storage policy.
type Frame = vtcore.Frame

var NewFrame = vtcore.NewFrame
