package theme

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

const (
	accentMinChroma             = 0.025
	accentMinBackgroundDistance = 0.08
	accentClusterDistance       = 0.04
)

// Accent is the single terminal-owned color selected for chrome. IndexedOnly
// means the slot may decorate foregrounds and borders, but cannot fill RGB
// backgrounds because terminal defaults or truecolor support are unavailable.
type Accent struct {
	RGB         renderer.RGB
	Slot        uint8
	Known       bool
	IndexedOnly bool
}

// The resolver only ever considers the chromatic ANSI slots in automatic
// mode. This immutable mapping keeps resolution bounded and independent of
// terminal response order without mutable package-level state.
const accentCandidateCount = 12

func accentCandidateSlot(index int) uint8 {
	if index < 6 {
		return uint8(index + 1)
	}
	return uint8(index + 3)
}

type accentGroup struct {
	members   uint16
	size      uint8
	pairs     uint8
	chromaSum float64
	rep       uint8
}

// ResolveAccent deterministically selects a terminal-owned accent. It uses
// only fixed arrays and bitsets so this pure hot-reload operation makes no
// per-call heap allocations.
func ResolveAccent(t Theme, policy domain.ThemeAccent) Accent {
	if !t.UsePalette {
		return Accent{}
	}
	if policy.Mode == domain.ThemeAccentSlot {
		return resolveExplicitAccent(t, policy.Slot)
	}
	return resolveAutoAccent(t)
}

func resolveExplicitAccent(t Theme, slot uint8) Accent {
	if slot >= uint8(len(t.Palette)) {
		return Accent{}
	}
	if t.PaletteKnown&(uint16(1)<<slot) == 0 {
		// An explicit policy is a strict override. Even without OSC RGB for its
		// slot, it may still safely decorate foregrounds and borders by index.
		return indexedAccent(slot)
	}
	return resolvedAccent(t, t.Palette[slot], slot)
}

func resolveAutoAccent(t Theme) Accent {
	// Background is necessary to reject colors that merely alias the terminal
	// surface. Without it, retain the pre-accent scheme-aware ANSI blue
	// foreground fallback rather than inventing an RGB background.
	if !t.HasBG {
		return schemeIndexedBlue(t)
	}

	var colors [accentCandidateCount]oklab
	var eligible uint16
	background := rgbToOKLab(t.Background)
	for i := range accentCandidateCount {
		slot := accentCandidateSlot(i)
		if t.PaletteKnown&(uint16(1)<<slot) == 0 {
			continue
		}
		colors[i] = rgbToOKLab(t.Palette[slot])
		if accentEligible(colors[i], background) {
			eligible |= uint16(1) << i
		}
	}
	if eligible == 0 {
		return fallbackAutoAccent(t, background)
	}

	var adjacent [accentCandidateCount]uint16
	for i := range accentCandidateCount {
		if eligible&(uint16(1)<<i) == 0 {
			continue
		}
		adjacent[i] = uint16(1) << i
		for j := i + 1; j < accentCandidateCount; j++ {
			if eligible&(uint16(1)<<j) == 0 || !accentConnected(colors[i], colors[j]) {
				continue
			}
			adjacent[i] |= uint16(1) << j
			adjacent[j] |= uint16(1) << i
		}
	}

	var groups [accentCandidateCount]accentGroup
	var visited uint16
	groupCount := 0
	for start := range accentCandidateCount {
		startBit := uint16(1) << start
		if eligible&startBit == 0 || visited&startBit != 0 {
			continue
		}
		var queue [accentCandidateCount]uint8
		head, tail := 0, 1
		queue[0] = uint8(start)
		visited |= startBit
		for head < tail {
			i := queue[head]
			head++
			groups[groupCount].members |= uint16(1) << i
			neighbors := adjacent[i] &^ visited
			for j := range accentCandidateCount {
				bit := uint16(1) << j
				if neighbors&bit == 0 {
					continue
				}
				visited |= bit
				queue[tail] = uint8(j)
				tail++
			}
		}
		finalizeAccentGroup(&groups[groupCount], colors)
		groupCount++
	}

	best, runner := -1, -1
	for i := 0; i < groupCount; i++ {
		if best < 0 || betterAccentGroup(groups[i], groups[best]) {
			runner = best
			best = i
		} else if runner < 0 || betterAccentGroup(groups[i], groups[runner]) {
			runner = i
		}
	}
	winner := groups[best]
	runnerSize, runnerPairs := uint8(0), uint8(0)
	if runner >= 0 {
		runnerSize, runnerPairs = groups[runner].size, groups[runner].pairs
	}
	if winner.size >= 2 && (winner.size > runnerSize || winner.pairs > runnerPairs) {
		slot := accentCandidateSlot(int(winner.rep))
		return resolvedAccent(t, t.Palette[slot], slot)
	}

	return fallbackAutoAccent(t, background)
}

func fallbackAutoAccent(t Theme, background oklab) Accent {
	for _, slot := range [...]uint8{4, 12} {
		if t.PaletteKnown&(uint16(1)<<slot) == 0 {
			continue
		}
		color := rgbToOKLab(t.Palette[slot])
		if accentEligible(color, background) {
			return resolvedAccent(t, t.Palette[slot], slot)
		}
	}
	if t.PaletteKnown&(uint16(1)<<4|uint16(1)<<12) != 0 {
		// A terminal-reported blue that fails eligibility deliberately does not
		// become an unvalidated indexed fallback.
		return Accent{}
	}
	return schemeIndexedBlue(t)
}

func schemeIndexedBlue(t Theme) Accent {
	if !t.SchemeKnown {
		return Accent{}
	}
	if t.Light {
		return indexedAccent(4)
	}
	return indexedAccent(12)
}

func indexedAccent(slot uint8) Accent {
	return Accent{Slot: slot, IndexedOnly: true}
}

func finalizeAccentGroup(group *accentGroup, colors [accentCandidateCount]oklab) {
	var representative uint8
	var representativeDistance float64
	first := true
	for i := range accentCandidateCount {
		slot := accentCandidateSlot(i)
		if group.members&(uint16(1)<<i) == 0 {
			continue
		}
		group.size++
		group.chromaSum += okLabChroma(colors[i])
		var totalDistance float64
		for j := range accentCandidateCount {
			if group.members&(uint16(1)<<j) != 0 {
				totalDistance += okLabDistance(colors[i], colors[j])
			}
		}
		if first || totalDistance < representativeDistance || (totalDistance == representativeDistance && slot < accentCandidateSlot(int(representative))) {
			first = false
			representative = uint8(i)
			representativeDistance = totalDistance
		}
	}
	group.rep = representative
	for slot := uint8(1); slot <= 6; slot++ {
		if accentGroupHasSlot(*group, slot) && accentGroupHasSlot(*group, slot+8) {
			group.pairs++
		}
	}
}

func accentGroupHasSlot(group accentGroup, slot uint8) bool {
	for i := range accentCandidateCount {
		if accentCandidateSlot(i) == slot {
			return group.members&(uint16(1)<<i) != 0
		}
	}
	return false
}

func betterAccentGroup(a, b accentGroup) bool {
	if a.size != b.size {
		return a.size > b.size
	}
	if a.pairs != b.pairs {
		return a.pairs > b.pairs
	}
	averageA := a.chromaSum / float64(a.size)
	averageB := b.chromaSum / float64(b.size)
	if averageA != averageB {
		return averageA > averageB
	}
	aBlue := accentGroupHasSlot(a, 4) || accentGroupHasSlot(a, 12)
	bBlue := accentGroupHasSlot(b, 4) || accentGroupHasSlot(b, 12)
	if aBlue != bBlue {
		return aBlue
	}
	return accentCandidateSlot(int(a.rep)) < accentCandidateSlot(int(b.rep))
}

func resolvedAccent(t Theme, color renderer.RGB, slot uint8) Accent {
	return Accent{
		RGB:         color,
		Slot:        slot,
		Known:       true,
		IndexedOnly: !t.TrueColor || !t.HasFG || !t.HasBG,
	}
}

func accentEligible(candidate, background oklab) bool {
	return okLabChroma(candidate) >= accentMinChroma && okLabDistance(candidate, background) >= accentMinBackgroundDistance
}

func accentConnected(a, b oklab) bool {
	return okLabDistance(a, b) <= accentClusterDistance
}
