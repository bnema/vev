package client_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/client"
)

// TestCompositionOutsideThePackageCanBuildThePicker proves the composition seam
// works from outside the package. Without it a composition could build the
// supervisor and subscribe to broker publications, but never hand it a picker,
// so it could never resolve a selection into an attachment.
func TestCompositionOutsideThePackageCanBuildThePicker(t *testing.T) {
	geometry := domain.Size{Cols: 80, Rows: 24}
	picker := client.NewPicker(nil, 0)
	require.NotNil(t, picker)

	// The exact assignment an app composition performs.
	cfg := client.SupervisorConfig{Picker: picker}
	require.NotNil(t, cfg.Picker)

	// Rendering belongs to the composition, so both entry points are callable
	// with the geometry the composition reads itself. Nothing has been applied
	// yet, so both are empty rather than a frame.
	require.Empty(t, picker.Render(geometry))
	require.Empty(t, picker.RenderNotice(geometry))

	// A composition that never built a picker renders nothing instead of
	// panicking.
	var absent *client.Picker
	require.Empty(t, absent.Render(geometry))
	require.Empty(t, absent.RenderNotice(geometry))
}
