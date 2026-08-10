package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/ports"
)

// manualPaletteClock applies only declarative coordinator timer actions. Its
// callbacks re-enter the coordinator through generation-tagged events; they
// never perform terminal I/O or protocol sends.
type manualPaletteClock struct {
	drain, completion map[paletteGenerationID]bool
}

func newManualPaletteClock() *manualPaletteClock {
	return &manualPaletteClock{drain: map[paletteGenerationID]bool{}, completion: map[paletteGenerationID]bool{}}
}

func (c *manualPaletteClock) apply(actions []paletteGenerationAction) {
	for _, action := range actions {
		switch action.kind {
		case paletteActionArmDrainDeadline:
			c.drain[action.id] = true
		case paletteActionArmCompletionDeadline:
			c.completion[action.id] = true
		case paletteActionCancelDrainDeadline:
			delete(c.drain, action.id)
		case paletteActionCancelCompletionDeadline:
			delete(c.completion, action.id)
		}
	}
}

func (c *manualPaletteClock) fireDrain(g *paletteGenerationCoordinator, id paletteGenerationID) []paletteGenerationAction {
	if !c.drain[id] {
		return nil
	}
	delete(c.drain, id)
	return g.handle(paletteGenerationEvent{id: id, kind: paletteEventDrainDeadline})
}

func (c *manualPaletteClock) fireCompletion(g *paletteGenerationCoordinator, id paletteGenerationID) []paletteGenerationAction {
	if !c.completion[id] {
		return nil
	}
	delete(c.completion, id)
	return g.handle(paletteGenerationEvent{id: id, kind: paletteEventCompletionDeadline})
}

func generationTheme() ports.Theme {
	return ports.Theme{
		TrueColor:     true,
		HasForeground: true,
		Foreground:    renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true,
		Background:    renderer.RGB{R: 4, G: 5, B: 6},
		SchemeKnown:   true,
		Light:         true,
		PaletteKnown:  1 << 2,
		Palette:       [16]renderer.RGB{2: {R: 7, G: 8, B: 9}},
	}
}

func actionKinds(actions []paletteGenerationAction) []paletteGenerationActionKind {
	out := make([]paletteGenerationActionKind, len(actions))
	for i := range actions {
		out[i] = actions[i].kind
	}
	return out
}

func findAction(t *testing.T, actions []paletteGenerationAction, kind paletteGenerationActionKind) paletteGenerationAction {
	t.Helper()
	for _, action := range actions {
		if action.kind == kind {
			return action
		}
	}
	t.Fatalf("missing action %v in %#v", kind, actions)
	return paletteGenerationAction{}
}

func TestPaletteGenerationInitialPublishesClearedThenDefinitive(t *testing.T) {
	g := newPaletteGenerationCoordinator()
	clock := newManualPaletteClock()
	start := g.start(generationTheme(), false)
	clock.apply(start)

	require.Equal(t, []paletteGenerationActionKind{
		paletteActionPublishCleared,
		paletteActionWriteBatch,
		paletteActionArmCompletionDeadline,
	}, actionKinds(start))
	cleared := findAction(t, start, paletteActionPublishCleared).theme
	require.True(t, cleared.HasForeground)
	require.Equal(t, generationTheme().Foreground, cleared.Foreground)
	require.Zero(t, cleared.PaletteKnown)
	require.Zero(t, cleared.Palette)

	batch := findAction(t, start, paletteActionWriteBatch)
	require.Equal(t, "\x1b]10;?\x07\x1b]11;?\x07"+
		"\x1b]4;0;?;1;?;2;?;3;?;4;?;5;?;6;?;7;?;8;?;9;?;10;?;11;?;12;?;13;?;14;?;15;?\x07"+
		"\x1b[?2031$p", batch.bytes)
	id := batch.id
	g.foreground(id, renderer.RGB{R: 10, G: 11, B: 12})
	g.background(id, renderer.RGB{R: 13, G: 14, B: 15})
	g.palette(id, 3, renderer.RGB{R: 16, G: 17, B: 18})
	final := g.marker(id)
	clock.apply(final)

	require.Equal(t, []paletteGenerationActionKind{
		paletteActionCancelDrainDeadline,
		paletteActionCancelCompletionDeadline,
		paletteActionPublishFinal,
	}, actionKinds(final))
	got := findAction(t, final, paletteActionPublishFinal).theme
	require.True(t, got.TrueColor)
	require.True(t, got.HasForeground)
	require.Equal(t, renderer.RGB{R: 10, G: 11, B: 12}, got.Foreground)
	require.True(t, got.HasBackground)
	require.Equal(t, uint16(1<<3), got.PaletteKnown)
}

func TestPaletteGenerationReplacementWaitsForDrain(t *testing.T) {
	g := newPaletteGenerationCoordinator()
	clock := newManualPaletteClock()
	start := g.start(generationTheme(), true)
	clock.apply(start)

	require.Equal(t, []paletteGenerationActionKind{
		paletteActionPublishCleared,
		paletteActionWriteDrain,
		paletteActionArmDrainDeadline,
	}, actionKinds(start))
	drain := findAction(t, start, paletteActionWriteDrain)
	require.Equal(t, "\x1b[?2031$p", drain.bytes)
	id := drain.id
	batch := g.marker(id)
	clock.apply(batch)
	require.Equal(t, []paletteGenerationActionKind{
		paletteActionCancelDrainDeadline,
		paletteActionWriteBatch,
		paletteActionArmCompletionDeadline,
	}, actionKinds(batch))
}

func TestPaletteGenerationLateDrainBoundaryExcludesStaleOSC(t *testing.T) {
	g := newPaletteGenerationCoordinator()
	clock := newManualPaletteClock()
	start := g.start(generationTheme(), true)
	clock.apply(start)
	id := findAction(t, start, paletteActionWriteDrain).id

	timedOut := clock.fireDrain(g, id)
	clock.apply(timedOut)
	require.Equal(t, []paletteGenerationActionKind{
		paletteActionWriteBatch,
		paletteActionArmCompletionDeadline,
	}, actionKinds(timedOut))

	// The drain reply may be late, so reports received before its boundary are
	// conservatively excluded from this generation's accumulator.
	g.foreground(id, renderer.RGB{R: 1, G: 2, B: 3})
	g.background(id, renderer.RGB{R: 4, G: 5, B: 6})
	g.palette(id, 2, renderer.RGB{R: 7, G: 8, B: 9})

	lateDrain := g.marker(id)
	require.Empty(t, lateDrain, "the late drain response must not finalize the batch")
	g.foreground(id, renderer.RGB{R: 10, G: 11, B: 12})
	g.background(id, renderer.RGB{R: 13, G: 14, B: 15})
	g.palette(id, 3, renderer.RGB{R: 16, G: 17, B: 18})
	complete := g.marker(id)
	clock.apply(complete)
	final := findAction(t, complete, paletteActionPublishFinal).theme
	require.Equal(t, renderer.RGB{R: 10, G: 11, B: 12}, final.Foreground)
	require.Equal(t, renderer.RGB{R: 13, G: 14, B: 15}, final.Background)
	require.Equal(t, uint16(1<<3), final.PaletteKnown)
	require.Equal(t, renderer.RGB{R: 16, G: 17, B: 18}, final.Palette[3])
}

func TestPaletteGenerationCompletionDeadlineFinalizesAfterMissingLateDrain(t *testing.T) {
	g := newPaletteGenerationCoordinator()
	clock := newManualPaletteClock()
	start := g.start(generationTheme(), true)
	clock.apply(start)
	id := findAction(t, start, paletteActionWriteDrain).id
	clock.apply(clock.fireDrain(g, id))

	// One marker may be the completion marker while the timed-out drain reply
	// never arrives, so it cannot finalize by marker accounting alone.
	require.Empty(t, g.marker(id))
	final := clock.fireCompletion(g, id)
	clock.apply(final)
	require.Equal(t, paletteActionPublishFinal, final[len(final)-1].kind)
}

func TestPaletteGenerationStaleTimerAndMarkerAreNoOps(t *testing.T) {
	g := newPaletteGenerationCoordinator()
	clock := newManualPaletteClock()
	first := g.start(generationTheme(), true)
	clock.apply(first)
	oldID := findAction(t, first, paletteActionWriteDrain).id
	second := g.start(generationTheme(), false)
	clock.apply(second)
	newID := findAction(t, second, paletteActionWriteBatch).id

	require.Empty(t, g.marker(oldID))
	require.Empty(t, clock.fireDrain(g, oldID))
	require.Empty(t, clock.fireCompletion(g, oldID))
	final := g.marker(newID)
	require.Equal(t, paletteActionPublishFinal, final[len(final)-1].kind)
}

func TestPaletteMarkerScannerConsumesSplitMarkerAndPreservesInput(t *testing.T) {
	scanner := paletteMarkerScanner{}
	var got []byte
	markers := 0
	emit := func(data []byte) { got = append(got, data...) }
	marker := func() { markers++ }

	scanner.scan([]byte("a\x1b[?2031;"), emit, marker)
	scanner.scan([]byte("2$yb"), emit, marker)
	scanner.flush(emit)

	require.Equal(t, []byte("ab"), got)
	require.Equal(t, 1, markers)
}

func TestPaletteGenerationLateOSCDoesNotMutateFinalizedAccumulator(t *testing.T) {
	g := newPaletteGenerationCoordinator()
	start := g.start(generationTheme(), false)
	id := findAction(t, start, paletteActionWriteBatch).id
	g.palette(id, 2, renderer.RGB{R: 1, G: 2, B: 3})
	final := g.marker(id)
	want := findAction(t, final, paletteActionPublishFinal).theme

	g.palette(id, 2, renderer.RGB{R: 9, G: 9, B: 9})
	require.Equal(t, want, g.finalizedTheme())
}

func TestPaletteGenerationDeadlineIsFixedCapabilityFallback(t *testing.T) {
	require.Equal(t, 200*time.Millisecond, paletteGenerationDeadline)
}
