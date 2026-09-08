package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// TestPaletteInventoryNamespaceLifecycle pins the overlay interaction
// namespace: open bumps the generation and resets admit state, close drops
// relayed rows, and a reopen starts a new namespace.
func TestPaletteInventoryNamespaceLifecycle(t *testing.T) {
	rt := &overlayRuntime{}
	first := openPaletteInventory(rt)
	require.Equal(t, uint64(1), first)
	rt.paletteInventoryPublication = 3
	rt.paletteInventoryGroups = []protocol.NavigationInventorySourceGroup{{SourceKey: "local"}}

	interaction, ok := takePaletteInventoryClose(rt)
	require.True(t, ok)
	require.Equal(t, first, interaction)

	closePaletteInventory(rt)
	if _, ok := takePaletteInventoryClose(rt); ok {
		t.Fatal("closed palette must not offer a close-demand")
	}
	require.Empty(t, rt.paletteInventoryGroups)

	second := openPaletteInventory(rt)
	require.Equal(t, first+1, second)
	require.Zero(t, rt.paletteInventoryPublication)
}

// TestAdmitPaletteInventoryPublication pins admission: newer generations
// store, older and duplicate generations discard, stale interactions and
// closed palettes drop, malformed messages never store.
func TestAdmitPaletteInventoryPublication(t *testing.T) {
	groups := func() []protocol.NavigationInventorySourceGroup {
		return []protocol.NavigationInventorySourceGroup{{
			SourceKey: "local", Status: protocol.NavigationInventorySourceOK,
			Entries: []protocol.NavigationInventoryEntry{
				{SourceKey: "local", EntryKey: "aaa/one", Name: "one"},
			},
		}}
	}
	publication := func(interaction, generation uint64) protocol.NavigationInventoryPublication {
		return protocol.NavigationInventoryPublication{InteractionGeneration: interaction, PublicationGeneration: generation, Groups: groups()}
	}

	rt := &overlayRuntime{}
	interaction := openPaletteInventory(rt)

	require.True(t, admitPaletteInventoryPublication(rt, publication(interaction, 1)))
	require.Len(t, rt.paletteInventoryGroups, 1)
	require.False(t, admitPaletteInventoryPublication(rt, publication(interaction, 1)), "duplicate generation must discard")
	require.False(t, admitPaletteInventoryPublication(rt, publication(interaction, 0)), "older generation must discard")
	require.True(t, admitPaletteInventoryPublication(rt, publication(interaction, 2)))
	require.Equal(t, uint64(2), rt.paletteInventoryPublication)
	require.False(t, admitPaletteInventoryPublication(rt, publication(interaction+7, 3)), "stale interaction must drop")

	closePaletteInventory(rt)
	require.False(t, admitPaletteInventoryPublication(rt, publication(interaction, 3)), "closed palette must drop")

	other := &overlayRuntime{}
	openPaletteInventory(other)
	malformed := publication(other.paletteInventoryInteraction, 1)
	malformed.Groups = nil
	malformed.PublicationGeneration = 0
	require.False(t, admitPaletteInventoryPublication(other, malformed))
	require.Empty(t, other.paletteInventoryGroups)
}

// TestPaletteInventoryDemandAndSelectionFlow drives the serving-daemon side
// end to end through a capturing transport: capability-gated open demand,
// publication admission with imported-row projection, Enter resolving
// through opaque keys with bound cause, Selection before close-demand wire
// order, and overlay teardown that preserves the frozen selection.
func TestPaletteInventoryDemandAndSelectionFlow(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, current, ac, sends := newManualSessionWithPTYs(t, p)
	ac.navigationCapabilities = protocol.NavigationCapabilityInventory
	effect := beginRecentRoutePaletteEffect(t, d, current, ac)
	effect.uiActionID = 123

	interaction := d.enterPalette(current, ac)
	d.sendPaletteInventoryDemand(ac, effect, true, interaction)
	demandFrame := awaitFrame(t, sends, wire.MsgNavigationInventoryDemand)
	demand, err := wire.UnmarshalNavigationInventoryDemand(demandFrame.Payload)
	require.NoError(t, err)
	require.True(t, demand.Open)
	require.Equal(t, interaction, demand.InteractionGeneration)

	published := protocol.NavigationInventoryPublication{
		InteractionGeneration: interaction, PublicationGeneration: 1,
		Groups: []protocol.NavigationInventorySourceGroup{{
			SourceKey: "local", Status: protocol.NavigationInventorySourceOK,
			Entries: []protocol.NavigationInventoryEntry{
				{SourceKey: "local", EntryKey: "aaa/zzqimported", Name: "zzqimported", DisplayOrigin: "local", State: "up"},
			},
		}},
	}
	ac.overlays.paletteMu.Lock()
	require.True(t, admitPaletteInventoryPublication(ac.overlays, published))
	ac.overlays.paletteMu.Unlock()
	d.refreshPalette(ac)

	d.handlePaletteInput(ac, []byte("zzqimported\r"), effect)

	selectionFrame := awaitFrame(t, sends, wire.MsgNavigationInventorySelection)
	selection, err := wire.UnmarshalNavigationInventorySelection(selectionFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(123), selection.CauseActionID, "sendControl binds the admitted input cause")
	require.Equal(t, interaction, selection.InteractionGeneration)
	require.Equal(t, uint64(1), selection.PublicationGeneration)
	require.Equal(t, "local", selection.SourceKey)
	require.Equal(t, "aaa/zzqimported", selection.EntryKey)

	closeFrame := awaitFrame(t, sends, wire.MsgNavigationInventoryDemand)
	closeDemand, err := wire.UnmarshalNavigationInventoryDemand(closeFrame.Payload)
	require.NoError(t, err)
	require.False(t, closeDemand.Open)
	require.Equal(t, interaction, closeDemand.InteractionGeneration)

	require.False(t, ac.overlays.paletteActive(), "select closes the overlay")
	ac.overlays.paletteMu.Lock()
	frozen := ac.overlays.paletteInventorySelected
	ac.overlays.paletteMu.Unlock()
	require.Nil(t, frozen, "overlay teardown drops relayed state; the client transition owns recovery")
}

// TestPaletteInventoryRemoteEnterNeverAttaches pins read-only remote
// vision: Enter on a remote-origin row reports feedback, keeps the palette
// open, and emits no Selection frame.
func TestPaletteInventoryRemoteEnterNeverAttaches(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, current, ac, sends := newManualSessionWithPTYs(t, p)
	ac.navigationCapabilities = protocol.NavigationCapabilityInventory
	effect := beginRecentRoutePaletteEffect(t, d, current, ac)
	effect.uiActionID = 123

	interaction := d.enterPalette(current, ac)
	published := protocol.NavigationInventoryPublication{
		InteractionGeneration: interaction, PublicationGeneration: 1,
		Groups: []protocol.NavigationInventorySourceGroup{
			{
				SourceKey: "local", Status: protocol.NavigationInventorySourceOK,
				Entries: []protocol.NavigationInventoryEntry{
					{SourceKey: "local", EntryKey: "aaa/zzqlocal", Name: "zzqlocal", DisplayOrigin: "local", State: "up"},
				},
			},
			{
				SourceKey: "arch", Status: protocol.NavigationInventorySourceOK,
				Entries: []protocol.NavigationInventoryEntry{
					{SourceKey: "arch", EntryKey: "aaa/zzqremote", Name: "zzqremote", DisplayOrigin: "arch", State: "down", Reason: "unreachable"},
				},
			},
		},
	}
	ac.overlays.paletteMu.Lock()
	require.True(t, admitPaletteInventoryPublication(ac.overlays, published))
	ac.overlays.paletteMu.Unlock()
	d.refreshPalette(ac)

	d.handlePaletteInput(ac, []byte("zzqremote\r"), effect)

	ac.overlays.paletteMu.Lock()
	feedback := ac.overlays.paletteFeedback
	ac.overlays.paletteMu.Unlock()
	require.Equal(t, "remote sessions are visible but cannot be attached", feedback)
	require.True(t, ac.overlays.paletteActive(), "refused remote selection keeps the palette open")
	select {
	case frame := <-sends:
		require.NotEqual(t, wire.MsgNavigationInventorySelection, frame.Type, "remote Enter must not emit a selection")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestPaletteInventoryEscapeSendsCloseDemand pins the plain-cancel path: a
// bare Escape cancels the unselected interaction and still stops polling.
func TestPaletteInventoryEscapeSendsCloseDemand(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, current, ac, sends := newManualSessionWithPTYs(t, p)
	ac.navigationCapabilities = protocol.NavigationCapabilityInventory
	effect := beginRecentRoutePaletteEffect(t, d, current, ac)

	interaction := d.enterPalette(current, ac)
	d.sendPaletteInventoryDemand(ac, effect, true, interaction)
	awaitFrame(t, sends, wire.MsgNavigationInventoryDemand)

	d.handlePaletteInput(ac, []byte{0x1b}, effect)
	closeFrame := awaitFrame(t, sends, wire.MsgNavigationInventoryDemand)
	closeDemand, err := wire.UnmarshalNavigationInventoryDemand(closeFrame.Payload)
	require.NoError(t, err)
	require.False(t, closeDemand.Open)
	require.Equal(t, interaction, closeDemand.InteractionGeneration)
	require.False(t, ac.overlays.paletteActive())
}

// TestPaletteInventoryDemandRequiresCapability pins the serving-side gate:
// without the advertised capability bit no demand crosses, even with an
// admitted effect, and the overlay namespace still opens natively.
func TestPaletteInventoryDemandRequiresCapability(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, current, ac, sends := newManualSessionWithPTYs(t, p)
	effect := beginRecentRoutePaletteEffect(t, d, current, ac)

	interaction := d.enterPalette(current, ac)
	d.sendPaletteInventoryDemand(ac, effect, true, interaction)
	deadline := time.After(time.Second)
	for {
		select {
		case frame := <-sends:
			if frame.Type == wire.MsgNavigationInventoryDemand {
				t.Fatalf("demand without capability must not send")
			}
		case <-deadline:
			require.True(t, ac.overlays.paletteActive())
			return
		}
	}
}
