package daemon

import (
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/palette"
)

// This file owns the serving-daemon side of navigation inventory: demand
// emission, publication admission, and imported-row selection. All overlay
// fields below are guarded by paletteMu alongside the palette model; no
// lock is held across sends. Helpers never lock internally: callers hold
// paletteMu for state transitions and release it before any sendControl.

// inventoryDemandEnabled reports whether this attachment may receive
// inventory demand. The serving daemon emits demand only for the advertised
// capability; the client independently checks its concrete local source.
func inventoryDemandEnabled(ac *attachedClient) bool {
	return ac != nil && ac.navigationCapabilities&protocol.NavigationCapabilityInventory != 0
}

// openPaletteInventory starts one palette interaction namespace. Admitted
// keys and retirements never cross interactions.
func openPaletteInventory(rt *overlayRuntime) uint64 {
	rt.paletteInventoryOpen = true
	rt.paletteInventoryInteraction++
	rt.paletteInventoryPublication = 0
	rt.paletteInventoryGroups = nil
	rt.paletteInventorySelected = nil
	rt.paletteInventoryDemandSent = false
	return rt.paletteInventoryInteraction
}

// closePaletteInventory ends the interaction and drops relayed state.
// Imported rows vanish with the palette; a reopen starts a new namespace.
func closePaletteInventory(rt *overlayRuntime) {
	rt.paletteInventoryOpen = false
	rt.paletteInventoryGroups = nil
	rt.paletteInventorySelected = nil
	rt.paletteInventoryDemandSent = false
}

// takePaletteInventoryClose snapshots a pending close-demand: the overlay
// must be open for the interaction. Callers send Demand{open:false} after
// releasing paletteMu, then close through clearPaletteLocked.
func takePaletteInventoryClose(rt *overlayRuntime) (uint64, bool) {
	if !rt.paletteInventoryOpen {
		return 0, false
	}
	return rt.paletteInventoryInteraction, true
}

// sendPaletteInventoryDemand emits an open or close demand on the guarded
// serving connection. It locks paletteMu only for the emission check, never
// across the send. A nil effect skips the send while the overlay state still
// transitions; those paths coincide with attachment teardown or replacement
// where the client-side relay is cancelled independently. Stale close
// demands (superseded by a reopen) skip so generations stay ordered.
func (d *Daemon) sendPaletteInventoryDemand(ac *attachedClient, effect *attachmentEffect, open bool, interaction uint64) {
	if !inventoryDemandEnabled(ac) || effect == nil || interaction == 0 {
		return
	}
	ac.overlays.paletteMu.Lock()
	if open {
		if !ac.overlays.paletteInventoryOpen || ac.overlays.paletteInventoryInteraction != interaction || ac.overlays.paletteInventoryDemandSent {
			ac.overlays.paletteMu.Unlock()
			return
		}
		ac.overlays.paletteInventoryDemandSent = true
	} else if ac.overlays.paletteInventoryInteraction != interaction {
		ac.overlays.paletteMu.Unlock()
		return
	}
	ac.overlays.paletteMu.Unlock()
	demand := protocol.NavigationInventoryDemand{InteractionGeneration: interaction, Open: open}
	if protocol.ValidateNavigationInventoryDemand(demand) != nil {
		return
	}
	_ = effect.sendControl(demand)
}

// admitPaletteInventoryPublication validates and stores one client
// publication. Older or duplicate publication generations discard: the
// daemon never steps its display backward. Stale interactions (palette
// closed or reopened since) drop silently.
func admitPaletteInventoryPublication(rt *overlayRuntime, message protocol.NavigationInventoryPublication) bool {
	if protocol.ValidateNavigationInventoryPublication(message) != nil {
		return false
	}
	if !rt.paletteInventoryOpen || message.InteractionGeneration != rt.paletteInventoryInteraction {
		return false
	}
	if message.PublicationGeneration <= rt.paletteInventoryPublication {
		return false
	}
	rt.paletteInventoryPublication = message.PublicationGeneration
	rt.paletteInventoryGroups = append([]protocol.NavigationInventorySourceGroup(nil), message.Groups...)
	return true
}

// copyPaletteInventoryGroups snapshots the admitted relayed groups for
// projection. It copies under paletteMu and releases it before any capture:
// callers must never nest it inside registry locks. A closed palette
// yields nil: imported rows vanish with the overlay.
func copyPaletteInventoryGroups(ac *attachedClient) []protocol.NavigationInventorySourceGroup {
	if ac == nil || ac.overlays == nil {
		return nil
	}
	ac.overlays.paletteMu.Lock()
	defer ac.overlays.paletteMu.Unlock()
	if !ac.overlays.paletteInventoryOpen {
		return nil
	}
	return append([]protocol.NavigationInventorySourceGroup(nil), ac.overlays.paletteInventoryGroups...)
}

// importedPaletteResults projects admitted sanitized groups as palette rows
// with explicit origin qualification. Homonyms across sources stay distinct
// rows; selection resolves through opaque keys, never labels.
func importedPaletteResults(groups []protocol.NavigationInventorySourceGroup) []palette.Result {
	var results []palette.Result
	for _, group := range groups {
		if group.Status != protocol.NavigationInventorySourceOK {
			continue
		}
		for _, entry := range group.Entries {
			origin := entry.DisplayOrigin
			if origin == "" {
				origin = group.SourceKey
			}
			results = append(results, palette.NewImportedSessionResult(group.SourceKey, entry.EntryKey, entry.Name, origin, entry.State, entry.Reason))
		}
	}
	return results
}

// appendImportedResults projects admitted relayed rows after native
// results. Enter starts native-only (fresh namespace); refreshes pick up
// streamed publications. Stable opaque keys preserve cursor and query
// across ReplaceResults.
func appendImportedResults(results []palette.Result, ac *attachedClient) []palette.Result {
	return append(results, importedPaletteResults(copyPaletteInventoryGroups(ac))...)
}

// inventoryFailureNotice maps a bounded relay failure code to palette
// feedback through existing conventions. Native commands stay available:
// only the contextual feedback line changes.
func inventoryFailureNotice(code protocol.NavigationInventoryFailureCode) string {
	switch code {
	case protocol.NavigationInventoryStaleIdentity, protocol.NavigationInventoryInvalidTarget:
		return "that destination is no longer available"
	case protocol.NavigationInventorySourceGone:
		return "local inventory source unavailable"
	case protocol.NavigationInventoryIncompatible:
		return "incompatible session version"
	case protocol.NavigationInventoryNavigationFailed:
		return "couldn't switch sessions"
	case protocol.NavigationInventoryRestoreFailed:
		return "couldn't restore the original session"
	default:
		return "inventory selection unavailable"
	}
}

// freezePaletteInventorySelection records the chosen imported row before its
// Selection crosses the wire. Closing after selection must not cancel the
// client's pending navigation operation.
func freezePaletteInventorySelection(rt *overlayRuntime, selection protocol.NavigationInventorySelection) {
	rt.paletteInventorySelected = &selection
}
