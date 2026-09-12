package daemon

import (
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/notices"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/picker"
	promptui "github.com/bnema/vev/internal/usecase/prompt"
	"github.com/bnema/vev/internal/usecase/visualsearch"
)

type overlayRuntime struct {
	ac *attachedClient

	pickerMu sync.Mutex
	// pickerPublishMu serializes snapshot publication for this attachment. A
	// publisher reserves the sole right to publish, builds, validates, and
	// sends its snapshot, and only then commits the revision it announced. It
	// is held across the transport send so two concurrent publishers cannot
	// interleave or reorder authoritative revisions. pickerMu is still the
	// state lock and is never held across the send; the lock order is
	// pickerPublishMu -> pickerMu, and it is never taken while sendMu is held.
	pickerPublishMu sync.Mutex
	// picker* carries the serving-daemon side of the picker interaction,
	// guarded by pickerMu. Open tracks the current interaction namespace;
	// interaction is the latest admitted open ID (late selections for closed
	// interactions reject); revisions are the per-source published versions
	// (older or duplicate revisions discard); keys maps opaque row keys of the
	// serving source to resolved targets; intent and moverSource are the
	// daemon-side facts captured at open; requestID echoes a client PickerBegin.
	pickerOpen        bool
	pickerInteraction uint64
	pickerRevisions   map[string]uint64
	pickerKeys        map[string]picker.Target
	// pickerSourcePublished distinguishes "never published" from "published an
	// empty set" for the current interaction. pickerRecent and pickerGrouped
	// are the exact projections last published: a refresh whose projections
	// equal them is not authority-relevant, so it must not bump the revision
	// nor send a snapshot the client never needed to see.
	pickerSourcePublished bool
	pickerRecent          protocol.PickerProjection
	pickerGrouped         protocol.PickerProjection
	pickerIntent          protocol.PickerIntent
	pickerMoveSource      moveSourceLocator
	pickerRequestID       uint64
	// pickerPreview* track the row the client asked to preview. The generation
	// supersedes an in-flight capture and names the render subscription, so a
	// delayed preview can never replace the row the user is displaying.
	// pickerPreviewSession pins the recorded subscription to the coordinator
	// that owns it (the target session, or the viewer for a remote row), so
	// teardown removes the exact target subscription.
	pickerPreviewGeneration uint64
	pickerPreviewKey        string
	pickerPreviewSession    *session

	paletteMu            sync.Mutex
	palette              *palette.Model
	paletteRouteSnapshot protocol.RecentRouteSnapshot
	paletteGeneration    uint64
	paletteHints         palette.ContextualHints
	palettePreview       string
	paletteFeedback      string
	palettePending       []byte
	// paletteInventory* carries the serving-daemon side of navigation
	// inventory, guarded by paletteMu alongside the palette model. Open
	// tracks the current interaction namespace; publication is the latest
	// admitted publication generation (older/duplicates discard); groups
	// holds the sanitized relayed rows; selected freezes the chosen row
	// across select-close; demandSent records open-demand emission.
	paletteInventoryOpen        bool
	paletteInventoryInteraction uint64
	paletteInventoryPublication uint64
	paletteInventoryGroups      []protocol.NavigationInventorySourceGroup
	paletteInventorySelected    *protocol.NavigationInventorySelection
	paletteInventoryDemandSent  bool

	promptMu               sync.Mutex
	prompt                 *promptui.Model
	promptSubmit           func(string) error
	promptTransitionSubmit func(string, *attachmentEffect) error
	promptPending          []byte

	resizeMu      sync.Mutex
	resizeActive  bool
	resizePending []byte
	resizeESC     pendingByteTimer

	copyMu            sync.Mutex
	copyMode          *scopy.Mode
	copyCandidate     *scopy.Mode
	copyDocument      *scopy.Document
	copyPane          *pane
	copyPending       []byte
	copyESC           pendingByteTimer
	copyScroll        copyScrollAnimation
	copySearch        *visualsearch.Model
	copySearchPending []byte
	statusFeedback    string
	copyPointer       copyPointerState
	copyClick         copyClickCandidate
	copyPointerEpoch  uint64

	// noticeMu is the innermost overlay lock: it guards the toast fields below
	// and nothing is ever locked, sent, or rendered while it is held. The only
	// permitted nesting is sendMu -> noticeMu (rendering reads the toasts).
	// The notification history overlay's fields share this lock rather than
	// adding one of their own: both concern notice presentation for this
	// client and neither is ever held across a render call.
	noticeMu       sync.Mutex
	noticeToasts   []noticeToast
	noticeOverflow int
	// noticeSeq numbers toast entries so an already-fired expiry timer cannot
	// dismiss the refreshed entry that replaced the one it belonged to.
	noticeSeq uint64

	// noticesOverlay is the `notifications` command's history modal. nil when
	// closed.
	noticesOverlay *notices.Model
	noticesPending []byte
	noticesESC     pendingByteTimer
}

type copyPointerState struct {
	valid    bool
	epoch    uint64
	pane     *pane
	document *scopy.Document
	geometry copyMouseGeometry
	press    scopy.Pos
	dragging bool
	wordDrag bool
}

type copyClickCandidate struct {
	valid   bool
	pane    *pane
	pos     scopy.Pos
	at      time.Time
	dragged bool
}

func newOverlayRuntime(ac *attachedClient) *overlayRuntime {
	return &overlayRuntime{ac: ac}
}

func (rt *overlayRuntime) Active() bool {
	if rt == nil || rt.ac == nil {
		return false
	}
	return rt.promptActive() || rt.paletteActive() || rt.pickerClientActive() || rt.noticesActive() || rt.resizeModeActive() || rt.copyActive()
}

func (rt *overlayRuntime) promptActive() bool {
	if rt == nil {
		return false
	}
	rt.promptMu.Lock()
	defer rt.promptMu.Unlock()
	return rt.prompt != nil
}

func (rt *overlayRuntime) paletteActive() bool {
	if rt == nil {
		return false
	}
	rt.paletteMu.Lock()
	defer rt.paletteMu.Unlock()
	return rt.palette != nil
}

// pickerClientActive reports whether a picker interaction is open on this
// attachment. Such an interaction owns the user input: the daemon must not
// route raw key or mouse events to the session while it is open. Its typed
// PickerSelection/PickerClose messages are the only effect it may have, and
// they arrive on the control path, not here.
func (rt *overlayRuntime) pickerClientActive() bool {
	if rt == nil {
		return false
	}
	rt.pickerMu.Lock()
	defer rt.pickerMu.Unlock()
	return rt.pickerOpen
}

func (rt *overlayRuntime) noticesActive() bool {
	if rt == nil {
		return false
	}
	rt.noticeMu.Lock()
	defer rt.noticeMu.Unlock()
	return rt.noticesOverlay != nil
}

func (rt *overlayRuntime) resizeModeActive() bool {
	if rt == nil {
		return false
	}
	rt.resizeMu.Lock()
	defer rt.resizeMu.Unlock()
	return rt.resizeActive
}

func (rt *overlayRuntime) copyActive() bool {
	if rt == nil {
		return false
	}
	rt.copyMu.Lock()
	defer rt.copyMu.Unlock()
	return rt.copyMode != nil
}

func (rt *overlayRuntime) copySearchActive() bool {
	if rt == nil {
		return false
	}
	rt.copyMu.Lock()
	defer rt.copyMu.Unlock()
	return rt.copySearch != nil
}

func (rt *overlayRuntime) beginCopyPointerLocked(pointer copyPointerState) {
	rt.copyPointerEpoch++
	pointer.epoch = rt.copyPointerEpoch
	pointer.valid = true
	rt.copyPointer = pointer
}

func (rt *overlayRuntime) invalidateCopyPointerLocked(clearClick bool) {
	rt.stopCopyScrollLocked()
	rt.copyPointerEpoch++
	rt.copyPointer = copyPointerState{}
	if clearClick {
		rt.copyClick = copyClickCandidate{}
	}
}

// clearCopyPointerForTransferLocked leaves the epoch unchanged so an input or
// teardown occurring while publication revalidates can invalidate the transfer.
func (rt *overlayRuntime) clearCopyPointerForTransferLocked() {
	rt.copyPointer = copyPointerState{}
	rt.copyClick = copyClickCandidate{}
}

func (rt *overlayRuntime) discardCopyCandidateLocked(candidate *scopy.Mode) {
	if rt.copyCandidate != candidate {
		return
	}
	rt.copyCandidate = nil
	rt.copyDocument = nil
	rt.copyPane = nil
	rt.copySearch = nil
	rt.copySearchPending = nil
}

func (rt *overlayRuntime) clearCopyModeLocked() {
	rt.copyMode = nil
	rt.copyCandidate = nil
	rt.copyDocument = nil
	rt.copyPane = nil
	rt.copySearch = nil
	rt.copySearchPending = nil
	rt.invalidateCopyPointerLocked(true)
}

func (rt *overlayRuntime) clearCopyModeForPane(p *pane) bool {
	if rt == nil || p == nil {
		return false
	}
	rt.copyMu.Lock()
	defer rt.copyMu.Unlock()
	active := rt.copyPane == p && (rt.copyMode != nil || rt.copyCandidate != nil)
	prePublication := rt.copyPointer.pane == p || rt.copyClick.pane == p
	if active {
		rt.clearCopyModeLocked()
	} else if prePublication {
		rt.invalidateCopyPointerLocked(true)
	}
	return active || prePublication
}

func (rt *overlayRuntime) HandleInput(d *Daemon, data []byte, effects ...*attachmentEffect) bool {
	if rt == nil || rt.ac == nil {
		return false
	}
	ac := rt.ac
	rt.copyMu.Lock()
	rt.stopCopyScrollLocked()
	rt.copyMu.Unlock()
	var effect *attachmentEffect
	if len(effects) != 0 {
		effect = effects[0]
	}
	if rt.promptActive() {
		d.handlePromptInput(ac, data, effect)
		return true
	}
	if rt.paletteActive() {
		d.handlePaletteInput(ac, data, effect)
		return true
	}
	if rt.noticesActive() {
		d.handleNoticesInput(ac, data)
		return true
	}
	if rt.resizeModeActive() {
		d.handleResizeInput(ac, data)
		return true
	}
	if rt.copyActive() {
		d.handleCopyInput(ac, data)
		return true
	}
	return false
}

type overlayRenderSnapshot struct {
	rt *overlayRuntime

	copyActive      bool
	copyMode        *scopy.Mode
	copyPane        *pane
	copySearchModel *visualsearch.Model
	statusFeedback  string
	resizeActive    bool

	noticesOverlayActive bool
	noticesOverlayModel  *notices.Model

	paletteActive bool
	paletteModel  *palette.Model
	// paletteHints is a copy captured under paletteMu. Rendering must use this
	// immutable interaction snapshot rather than consult live session state.
	paletteHints         *palette.ContextualHints
	palettePreview       string
	paletteFeedback      string
	paletteRouteSnapshot protocol.RecentRouteSnapshot
	paletteLocked        bool

	promptActive bool
	promptModel  *promptui.Model
	promptLocked bool

	notices        []domain.Notification
	noticeOverflow int
}

// SnapshotForRender captures the overlay state needed by paint.
//
// The snapshot owns any prompt or palette locks it had to keep held so the
// returned model pointers stay stable during composition. Callers must release
// those locks with overlayRenderSnapshot.Unlock, usually with defer immediately
// after acquisition and before any path that may re-enter rendering.
func (rt *overlayRuntime) SnapshotForRender() *overlayRenderSnapshot {
	snap := &overlayRenderSnapshot{rt: rt}
	if rt == nil {
		return snap
	}

	// noticeMu is innermost and never held across render, so it is taken and
	// released here rather than tracked like paletteMu/promptMu below.
	rt.noticeMu.Lock()
	if len(rt.noticeToasts) > 0 {
		snap.notices = make([]domain.Notification, len(rt.noticeToasts))
		for i, t := range rt.noticeToasts {
			snap.notices[i] = t.n
		}
	}
	snap.noticeOverflow = rt.noticeOverflow
	snap.noticesOverlayActive = rt.noticesOverlay != nil
	snap.noticesOverlayModel = rt.noticesOverlay.Clone()
	rt.noticeMu.Unlock()

	rt.copyMu.Lock()
	snap.copyActive = rt.copyMode != nil
	snap.copyPane = rt.copyPane
	if rt.copyMode != nil {
		copyModeValue := *rt.copyMode
		copyModeValue.Searches = append([]scopy.SearchMatch(nil), rt.copyMode.Searches...)
		snap.copyMode = &copyModeValue
	}
	snap.copySearchModel = rt.copySearch.Clone()
	snap.statusFeedback = rt.statusFeedback
	if snap.statusFeedback != "" && !snap.copyActive {
		rt.statusFeedback = ""
	}
	rt.copyMu.Unlock()

	rt.resizeMu.Lock()
	snap.resizeActive = rt.resizeActive
	rt.resizeMu.Unlock()

	rt.paletteMu.Lock()
	snap.paletteModel = rt.palette
	snap.paletteActive = snap.paletteModel != nil
	if snap.paletteActive {
		hints := rt.paletteHints
		hints.Recent = append([]palette.RecentSessionHint(nil), hints.Recent...)
		snap.paletteHints = &hints
		snap.palettePreview = rt.palettePreview
		snap.paletteFeedback = rt.paletteFeedback
		snap.paletteRouteSnapshot = rt.paletteRouteSnapshot
		snap.paletteRouteSnapshot.Entries = append([]protocol.RecentRouteEntry(nil), rt.paletteRouteSnapshot.Entries...)
		snap.paletteLocked = true
	} else {
		rt.paletteMu.Unlock()
	}

	rt.promptMu.Lock()
	snap.promptModel = rt.prompt
	snap.promptActive = snap.promptModel != nil
	if snap.promptActive {
		snap.promptLocked = true
	} else {
		rt.promptMu.Unlock()
	}

	return snap
}

func (rt *overlayRuntime) UnlockRenderSnapshot(snap *overlayRenderSnapshot) {
	if snap == nil {
		return
	}
	snap.Unlock()
}

func (snap *overlayRenderSnapshot) Unlock() {
	if snap == nil || snap.rt == nil {
		return
	}
	if snap.promptLocked {
		snap.rt.promptMu.Unlock()
		snap.promptLocked = false
	}
	if snap.paletteLocked {
		snap.rt.paletteMu.Unlock()
		snap.paletteLocked = false
	}
}
