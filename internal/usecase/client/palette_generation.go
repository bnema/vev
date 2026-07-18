package client

import (
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

// Palette queries always have a DECRQM boundary. The first marker drains
// replies from a replaced generation; the second completes the color batch.
const paletteBoundaryQuery = "\x1b[?2031$p"

const paletteColorBatch = "\x1b]10;?\x07\x1b]11;?\x07" +
	"\x1b]4;0;?;1;?;2;?;3;?;4;?;5;?;6;?;7;?;8;?;9;?;10;?;11;?;12;?;13;?;14;?;15;?\x07" +
	paletteBoundaryQuery

// paletteGenerationDeadline is a local terminal capability fallback, not a
// user-facing timeout. Timer ownership belongs to the attach loop.
const paletteGenerationDeadline = 200 * time.Millisecond

// paletteMarkerAmbiguityDeadline bounds how long an Escape byte may remain
// withheld while it could still become a DECRQM completion response. Match
// the existing local input-prefix bound so a keypress remains prompt.
const paletteMarkerAmbiguityDeadline = pasteFlushDelay

type paletteGenerationID uint64

type paletteGenerationPhase uint8

const (
	generationIdle paletteGenerationPhase = iota
	generationDraining
	generationCollecting
	generationFinalized
)

type paletteGeneration struct {
	id                        paletteGenerationID
	phase                     paletteGenerationPhase
	theme                     ports.Theme
	expectedMarkers           int
	awaitingLateDrainBoundary bool
}

type paletteGenerationEventKind uint8

const (
	paletteEventMarker paletteGenerationEventKind = iota
	paletteEventDrainDeadline
	paletteEventCompletionDeadline
	paletteEventForeground
	paletteEventBackground
	paletteEventPalette
	paletteEventScheme
)

type paletteGenerationEvent struct {
	id    paletteGenerationID
	kind  paletteGenerationEventKind
	rgb   renderer.RGB
	slot  uint8
	light bool
}

// paletteGenerationAction is deliberately declarative. The attach loop is
// the sole terminal writer and protocol sender; timer bridges only enqueue a
// generation-tagged event back to that loop.
type paletteGenerationActionKind uint8

const (
	paletteActionPublishCleared paletteGenerationActionKind = iota
	paletteActionWriteDrain
	paletteActionWriteBatch
	paletteActionArmDrainDeadline
	paletteActionArmCompletionDeadline
	paletteActionCancelDrainDeadline
	paletteActionCancelCompletionDeadline
	paletteActionPublishFinal
)

type paletteGenerationAction struct {
	kind     paletteGenerationActionKind
	id       paletteGenerationID
	bytes    string
	theme    ports.Theme
	deadline time.Duration
}

// paletteGenerationCoordinator has no I/O, clocks, channels, or goroutines.
// All state is scoped to one monotonically increasing generation.
type paletteGenerationCoordinator struct {
	next      paletteGenerationID
	current   paletteGeneration
	active    bool
	finalized ports.Theme
}

func newPaletteGenerationCoordinator() *paletteGenerationCoordinator {
	return &paletteGenerationCoordinator{}
}

// start clears the last definitive palette before issuing either the initial
// batch or a replacement drain. The retained foreground/background and
// capability avoid a full neutral flash, while the accumulator begins with no
// defaults or palette values so a definitive result never mixes generations.
func (c *paletteGenerationCoordinator) start(retained ports.Theme, replacement bool) []paletteGenerationAction {
	actions := c.cancelCurrent()
	c.next++
	id := c.next

	cleared := retained
	cleared.PaletteKnown = 0
	cleared.Palette = [16]renderer.RGB{}
	c.current = paletteGeneration{
		id:    id,
		phase: generationCollecting,
		theme: ports.Theme{
			TrueColor:   retained.TrueColor,
			SchemeKnown: retained.SchemeKnown,
			Light:       retained.Light,
		},
	}
	c.active = true
	actions = append(actions, paletteGenerationAction{kind: paletteActionPublishCleared, id: id, theme: cleared})
	if replacement {
		c.current.phase = generationDraining
		actions = append(actions,
			paletteGenerationAction{kind: paletteActionWriteDrain, id: id, bytes: paletteBoundaryQuery},
			paletteGenerationAction{kind: paletteActionArmDrainDeadline, id: id, deadline: paletteGenerationDeadline},
		)
		return actions
	}
	return append(actions,
		paletteGenerationAction{kind: paletteActionWriteBatch, id: id, bytes: paletteColorBatch},
		paletteGenerationAction{kind: paletteActionArmCompletionDeadline, id: id, deadline: paletteGenerationDeadline},
	)
}

func (c *paletteGenerationCoordinator) cancelCurrent() []paletteGenerationAction {
	if !c.active {
		return nil
	}
	id := c.current.id
	c.active = false
	c.current.phase = generationFinalized
	return []paletteGenerationAction{
		{kind: paletteActionCancelDrainDeadline, id: id},
		{kind: paletteActionCancelCompletionDeadline, id: id},
	}
}

func (c *paletteGenerationCoordinator) handle(event paletteGenerationEvent) []paletteGenerationAction {
	if !c.active || event.id != c.current.id {
		return nil
	}
	switch event.kind {
	case paletteEventMarker:
		return c.onMarker()
	case paletteEventDrainDeadline:
		if c.current.phase != generationDraining {
			return nil
		}
		c.current.phase = generationCollecting
		// A late drain response may still arrive after the batch marker. Exclude
		// OSC reports until its boundary is consumed: otherwise stale drain-era
		// reports could become part of this generation's definitive snapshot.
		c.current.expectedMarkers = 2
		c.current.awaitingLateDrainBoundary = true
		return []paletteGenerationAction{
			{kind: paletteActionWriteBatch, id: event.id, bytes: paletteColorBatch},
			{kind: paletteActionArmCompletionDeadline, id: event.id, deadline: paletteGenerationDeadline},
		}
	case paletteEventCompletionDeadline:
		if c.current.phase != generationCollecting {
			return nil
		}
		return c.finalize()
	case paletteEventForeground:
		if c.current.phase == generationCollecting && !c.current.awaitingLateDrainBoundary {
			c.current.theme.HasForeground = true
			c.current.theme.Foreground = event.rgb
		}
	case paletteEventBackground:
		if c.current.phase == generationCollecting && !c.current.awaitingLateDrainBoundary {
			c.current.theme.HasBackground = true
			c.current.theme.Background = event.rgb
		}
	case paletteEventPalette:
		if c.current.phase == generationCollecting && !c.current.awaitingLateDrainBoundary && event.slot < 16 {
			c.current.theme.PaletteKnown |= uint16(1) << event.slot
			c.current.theme.Palette[event.slot] = event.rgb
		}
	}
	return nil
}

func (c *paletteGenerationCoordinator) onMarker() []paletteGenerationAction {
	switch c.current.phase {
	case generationDraining:
		c.current.phase = generationCollecting
		c.current.expectedMarkers = 1
		return []paletteGenerationAction{
			{kind: paletteActionCancelDrainDeadline, id: c.current.id},
			{kind: paletteActionWriteBatch, id: c.current.id, bytes: paletteColorBatch},
			{kind: paletteActionArmCompletionDeadline, id: c.current.id, deadline: paletteGenerationDeadline},
		}
	case generationCollecting:
		if c.current.expectedMarkers == 0 {
			c.current.expectedMarkers = 1
		}
		if c.current.awaitingLateDrainBoundary {
			c.current.awaitingLateDrainBoundary = false
		}
		c.current.expectedMarkers--
		if c.current.expectedMarkers == 0 {
			return c.finalize()
		}
	}
	return nil
}

func (c *paletteGenerationCoordinator) finalize() []paletteGenerationAction {
	if !c.active || c.current.phase != generationCollecting {
		return nil
	}
	c.current.phase = generationFinalized
	c.active = false
	c.finalized = c.current.theme
	return []paletteGenerationAction{
		{kind: paletteActionCancelDrainDeadline, id: c.current.id},
		{kind: paletteActionCancelCompletionDeadline, id: c.current.id},
		{kind: paletteActionPublishFinal, id: c.current.id, theme: c.finalized},
	}
}

func (c *paletteGenerationCoordinator) marker(id paletteGenerationID) []paletteGenerationAction {
	return c.handle(paletteGenerationEvent{id: id, kind: paletteEventMarker})
}

func (c *paletteGenerationCoordinator) foreground(id paletteGenerationID, rgb renderer.RGB) {
	c.handle(paletteGenerationEvent{id: id, kind: paletteEventForeground, rgb: rgb})
}

func (c *paletteGenerationCoordinator) background(id paletteGenerationID, rgb renderer.RGB) {
	c.handle(paletteGenerationEvent{id: id, kind: paletteEventBackground, rgb: rgb})
}

func (c *paletteGenerationCoordinator) palette(id paletteGenerationID, slot uint8, rgb renderer.RGB) {
	c.handle(paletteGenerationEvent{id: id, kind: paletteEventPalette, slot: slot, rgb: rgb})
}

func (c *paletteGenerationCoordinator) finalizedTheme() ports.Theme { return c.finalized }

// paletteMarkerScanner consumes only complete DECRQM 2031 replies and retains
// possible prefixes across reads, forwarding all ordinary input exactly once.
type paletteMarkerScanner struct{ pending []byte }

var paletteMarkerResponses = [][]byte{
	[]byte("\x1b[?2031;0$y"),
	[]byte("\x1b[?2031;1$y"),
	[]byte("\x1b[?2031;2$y"),
	[]byte("\x1b[?2031;3$y"),
	[]byte("\x1b[?2031;4$y"),
}

func (s *paletteMarkerScanner) scan(data []byte, onBytes func([]byte), onMarker func()) {
	for i, b := range data {
		s.pending = append(s.pending, b)
		matched, possible := false, false
		for _, response := range paletteMarkerResponses {
			if len(s.pending) > len(response) || string(s.pending) != string(response[:len(s.pending)]) {
				continue
			}
			if len(s.pending) == len(response) {
				matched = true
			} else {
				possible = true
			}
		}
		if matched {
			s.pending = nil
			onMarker()
			continue
		}
		if possible {
			continue
		}
		if s.pending[0] != '\x1b' {
			onBytes(s.pending[:1])
			s.pending = s.pending[1:]
			continue
		}
		// This is not a marker. Forward the entire remaining read as one
		// contiguous input run so CSI mouse reports and escape sequences keep
		// their framing.
		s.pending = append(s.pending, data[i+1:]...)
		onBytes(s.pending)
		s.pending = nil
		return
	}
}
func (s *paletteMarkerScanner) flush(onBytes func([]byte)) {
	if len(s.pending) == 0 {
		return
	}
	onBytes(s.pending)
	s.pending = nil
}

func (s *paletteMarkerScanner) hasPendingPrefix() bool { return len(s.pending) != 0 }
