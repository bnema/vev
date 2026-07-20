package daemon

import (
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/picker"
	promptui "github.com/bnema/vev/internal/usecase/prompt"
	"github.com/bnema/vev/internal/usecase/visualsearch"
)

type overlayRuntime struct {
	ac *attachedClient

	pickerMu                sync.Mutex
	picker                  *picker.Model
	pickerPreview           *tab
	pickerPreviewSession    *session
	pickerPreviewGeneration uint64
	pickerPending           []byte
	pickerESC               pendingByteTimer

	paletteMu         sync.Mutex
	palette           *palette.Model
	paletteRecent     []recentSession // immutable for this palette interaction
	paletteGeneration uint64
	paletteHints      palette.ContextualHints
	paletteFeedback   string
	palettePending    []byte

	promptMu      sync.Mutex
	prompt        *promptui.Model
	promptSubmit  func(string) error
	promptPending []byte

	copyMu            sync.Mutex
	copyMode          *scopy.Mode
	copyCandidate     *scopy.Mode
	copyDocument      *scopy.Document
	copyPane          *pane
	copyPending       []byte
	copyESC           pendingByteTimer
	copySearch        *visualsearch.Model
	copySearchPending []byte
	copyFeedback      string
	copyPointer       copyPointerState
	copyClick         copyClickCandidate
	copyPointerEpoch  uint64

	// noticeMu is the innermost overlay lock: it guards the toast fields below
	// and nothing is ever locked, sent, or rendered while it is held. The only
	// permitted nesting is sendMu -> noticeMu (rendering reads the toasts).
	noticeMu       sync.Mutex
	noticeToasts   []noticeToast
	noticeOverflow int
	// noticeSeq numbers toast entries so an already-fired expiry timer cannot
	// dismiss the refreshed entry that replaced the one it belonged to.
	noticeSeq uint64
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
	return rt.promptActive() || rt.paletteActive() || rt.pickerActive() || rt.copyActive()
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

func (rt *overlayRuntime) HandleInput(d *Daemon, data []byte) bool {
	if rt == nil || rt.ac == nil {
		return false
	}
	ac := rt.ac
	if rt.promptActive() {
		d.handlePromptInput(ac, data)
		return true
	}
	if rt.paletteActive() {
		d.handlePaletteInput(ac, data)
		return true
	}
	if rt.pickerActive() {
		d.handlePickerInput(ac, data)
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
	copyFeedback    string

	pickerActive bool
	pickerModel  *picker.Model
	previewTab   *tab

	paletteActive bool
	paletteModel  *palette.Model
	// paletteHints is a copy captured under paletteMu. Rendering must use this
	// immutable interaction snapshot rather than consult live session state.
	paletteHints    *palette.ContextualHints
	paletteFeedback string
	paletteRecent   []recentSession
	paletteLocked   bool

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
	snap.copyFeedback = rt.copyFeedback
	if snap.copyFeedback != "" && !snap.copyActive {
		rt.copyFeedback = ""
	}
	rt.copyMu.Unlock()

	rt.pickerMu.Lock()
	snap.pickerActive = rt.picker != nil
	snap.pickerModel = rt.picker.Clone()
	snap.previewTab = rt.pickerPreview
	rt.pickerMu.Unlock()

	rt.paletteMu.Lock()
	snap.paletteModel = rt.palette
	snap.paletteActive = snap.paletteModel != nil
	if snap.paletteActive {
		hints := rt.paletteHints
		hints.Recent = append([]palette.RecentSessionHint(nil), hints.Recent...)
		snap.paletteHints = &hints
		snap.paletteFeedback = rt.paletteFeedback
		snap.paletteRecent = append([]recentSession(nil), rt.paletteRecent...)
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
