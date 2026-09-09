package webterm

// Wheel quantization converts raw browser wheel deltas into discrete
// terminal wheel reports. Browsers emit wheel events as a scroll distance,
// not as physical notches: high-resolution wheels and trackpads produce a
// burst of micro-events per notch, and forwarding one SGR report per event
// scrolls dozens of lines per notch. The accumulator below merges fractions
// until they form whole notches, so one notch yields one report, like an
// ordinary terminal. The remainder is kept per terminal and bounded.
const (
	// wheelPixelsPerNotch matches the Chromium convention of 100 CSS pixels
	// per wheel notch in pixel (DOM_DELTA_PIXEL) mode.
	wheelPixelsPerNotch = 100
	// wheelLinesPerNotch matches the browser convention of 3 lines per wheel
	// notch in line (DOM_DELTA_LINE) mode.
	wheelLinesPerNotch = 3
	// wheelNotchesPerPage converts page (DOM_DELTA_PAGE) mode into notches.
	wheelNotchesPerPage = 10
	// wheelMaxNotchesPerEvent caps the reports emitted for a single event.
	wheelMaxNotchesPerEvent = 10
	// wheelMaxAccumulated caps the retained remainder in notch units, so a
	// violent momentum gesture cannot bank phantom scrolling.
	wheelMaxAccumulated = 10.0
	// wheelShiftMultiplier is the fast-scroll factor while Shift is held.
	wheelShiftMultiplier = 10
)

// consumeWheel converts one wheel delta into whole-notch SGR reports. It
// returns the report count and the SGR button (64 for up, 65 for down).
// Sub-notch fractions accumulate on the terminal; a zero count means the
// delta was banked for later. Shift multiplies the notch count for
// predictable fast scrolling.
func (t *Terminal) consumeWheel(deltaY float64, deltaMode int, shift bool) (int, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.wheelAcc += wheelNotches(deltaY, deltaMode)
	if t.wheelAcc > wheelMaxAccumulated {
		t.wheelAcc = wheelMaxAccumulated
	} else if t.wheelAcc < -wheelMaxAccumulated {
		t.wheelAcc = -wheelMaxAccumulated
	}
	notches := int(t.wheelAcc)
	t.wheelAcc -= float64(notches)
	if t.wheelAcc < 1e-9 && t.wheelAcc > -1e-9 {
		t.wheelAcc = 0
	}
	if notches == 0 {
		return 0, 0
	}
	magnitude := notches
	if magnitude < 0 {
		magnitude = -magnitude
	}
	if magnitude > wheelMaxNotchesPerEvent {
		magnitude = wheelMaxNotchesPerEvent
		if notches < 0 {
			notches = -magnitude
		} else {
			notches = magnitude
		}
	}
	if shift {
		magnitude *= wheelShiftMultiplier
		if magnitude > wheelMaxNotchesPerEvent*wheelShiftMultiplier {
			magnitude = wheelMaxNotchesPerEvent * wheelShiftMultiplier
		}
	}
	button := 65
	if notches < 0 {
		button = 64
	}
	return magnitude, button
}

// resetWheel drops the banked wheel remainder. It runs when wheel input is
// ignored (mouse tracking off, out-of-bounds cell) so a stale remainder
// cannot leak into a later, unrelated scroll gesture.
func (t *Terminal) resetWheel() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.wheelAcc = 0
}

// wheelNotches converts a raw wheel delta into notch units. Unknown delta
// modes contribute nothing rather than guessing a scale.
func wheelNotches(deltaY float64, deltaMode int) float64 {
	switch deltaMode {
	case 0:
		return deltaY / wheelPixelsPerNotch
	case 1:
		return deltaY / wheelLinesPerNotch
	case 2:
		return deltaY * wheelNotchesPerPage
	default:
		return 0
	}
}
