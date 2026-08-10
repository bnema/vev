package daemon

import (
	"context"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
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
	picker   *picker.Model
	// pickerGeneration identifies one open lifecycle. It advances only when a
	// picker is published, so delayed close and registration work can prove it
	// still owns the exact lifecycle it captured.
	pickerGeneration          uint64
	pickerTitle               string
	pickerIntent              pickerIntent
	pickerSource              moveSourceLocator
	pickerPreview             *tab
	pickerPreviewSession      *session
	pickerRemotePreview       picker.Preview
	pickerRemotePreviewCancel context.CancelFunc
	pickerPreviewGeneration   uint64
	pickerPending             []byte
	pickerESC                 pendingByteTimer

	// Test-only, unsynchronized lifecycle seams. Assign them before picker
	// publication or goroutine startup. Hooks run without pickerMu or
	// remoteCatalog.mu held.
	beforeRemotePickerRegistration func()
	afterPickerRefreshBuild        func(*picker.Model)

	paletteMu            sync.Mutex
	palette              *palette.Model
	paletteRouteSnapshot ports.RecentRouteSnapshot
	paletteGeneration    uint64
	paletteHints         palette.ContextualHints
	paletteFeedback      string
	palettePending       []byte

	promptMu               sync.Mutex
	prompt                 *promptui.Model
	promptSubmit           func(string) error
	promptTransitionSubmit func(string, attachmentConnectionToken) error
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
	return rt.promptActive() || rt.paletteActive() || rt.pickerActive() || rt.noticesActive() || rt.resizeModeActive() || rt.copyActive()
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

func (rt *overlayRuntime) pickerActive() bool {
	if rt == nil {
		return false
	}
	rt.pickerMu.Lock()
	defer rt.pickerMu.Unlock()
	return rt.picker != nil
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

func (rt *overlayRuntime) HandleInput(d *Daemon, data []byte, effects ...*attachmentEffectTicket) bool {
	if rt == nil || rt.ac == nil {
		return false
	}
	ac := rt.ac
	var effect *attachmentEffectTicket
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
	if rt.pickerActive() {
		d.handlePickerInput(ac, data, effect)
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

	pickerActive  bool
	pickerModel   *picker.Model
	pickerTitle   string
	previewTab    *tab
	remotePreview picker.Preview

	noticesOverlayActive bool
	noticesOverlayModel  *notices.Model

	paletteActive bool
	paletteModel  *palette.Model
	// paletteHints is a copy captured under paletteMu. Rendering must use this
	// immutable interaction snapshot rather than consult live session state.
	paletteHints         *palette.ContextualHints
	paletteFeedback      string
	paletteRouteSnapshot ports.RecentRouteSnapshot
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

	rt.pickerMu.Lock()
	snap.pickerActive = rt.picker != nil
	snap.pickerModel = rt.picker.Clone()
	snap.pickerTitle = rt.pickerTitle
	snap.previewTab = rt.pickerPreview
	snap.remotePreview = clonePickerPreview(rt.pickerRemotePreview)
	rt.pickerMu.Unlock()

	rt.paletteMu.Lock()
	snap.paletteModel = rt.palette
	snap.paletteActive = snap.paletteModel != nil
	if snap.paletteActive {
		hints := rt.paletteHints
		hints.Recent = append([]palette.RecentSessionHint(nil), hints.Recent...)
		snap.paletteHints = &hints
		snap.paletteFeedback = rt.paletteFeedback
		snap.paletteRouteSnapshot = rt.paletteRouteSnapshot
		snap.paletteRouteSnapshot.Entries = append([]ports.RecentRouteEntry(nil), rt.paletteRouteSnapshot.Entries...)
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
