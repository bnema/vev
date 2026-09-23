package client

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	pickerusecase "github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/internal/usecase/ui"
)

// Client-owned picker controller (Plan 001 P5.2b, offline and unactivated).
//
// This file is the narrow seam between the autonomous supervisor and the
// client-owned catalogue. The supervisor owns raw mode, the single terminal
// input lifetime, and the broker service; the controller owns the catalogue
// projection, the presentation model (reusing the picker package's cursor,
// search, sort, and rendering machinery), and the input decisions.
//
// Two invariants make the picker autonomous:
//
//   - Zero session bytes. The controller has no session, stream, or PTY
//     reference. A consumed terminal read is decoded into picker events and
//     applied to the model; nothing is ever written toward a daemon. A
//     selection resolves to a ports.BrokerOpenStreamRequest value and stops
//     there.
//   - No serving-daemon catalogue. Rows come from broker snapshots through
//     pickerCatalogue, so navigation needs no serving attachment.
//
// input ownership reuses the existing pickerConsumer, so the supervisor's one
// terminal input lifetime feeds exactly one owner and the picker takes
// precedence: while the controller owns input every decoded batch is consumed
// and there is no fallback to a session pipeline.

// pickerHost is the supervisor's view of the client-owned picker. The
// supervisor applies each broker publication and hands it every terminal read
// from its single input lifetime; the picker decides ownership.
//
// The commit seam (Plan 001 P5.3b) is what lets the supervisor turn a user
// commit into one exact broker stream without ever reading the presentation
// model itself: the picker records the decision and the key it committed
// atomically, the supervisor wakes on OpsReady, and ResolveKey revalidates
// exactly that key. The picker never opens a stream, and the supervisor never
// reconstructs a selection from a display label.
type pickerHost interface {
	pickerInputConsumer
	// ApplySnapshot folds one broker publication into the catalogue and
	// refreshes the presentation immediately.
	ApplySnapshot(ports.BrokerSnapshot)
	// TakeOp returns and clears the accumulated presentation decision together
	// with the catalogue key captured with a commit decision.
	TakeOp() (pickerOp, string)
	// ResolveKey revalidates exactly one committed catalogue key into the
	// exact broker stream request the user committed.
	ResolveKey(string, pickerResolveBase) (ports.BrokerOpenStreamRequest, error)
	// SetOwnsInput releases or re-acquires picker input ownership at an attach
	// boundary, so exactly one owner consumes the shared terminal reader.
	SetOwnsInput(bool)
	// OpsReady wakes the supervisor when a presentation decision was recorded.
	// It is a capacity-one coalescing signal, never closed while the picker
	// lives, so a driver that ignores it only misses a wakeup, never blocks.
	OpsReady() <-chan struct{}
}

// pickerInputConsumer is the supervisor's narrow input seam. ConsumeTerminalRead
// returns true when the picker owned and consumed the read. It returns false for
// bytes the picker declined, which are then dropped: the supervisor keeps no
// ordinary terminal handling alongside the picker, so a declined read has no
// second owner to hand it to.
type pickerInputConsumer interface {
	ConsumeTerminalRead(data []byte) bool
}

const (
	// pickerGeneration identifies the single client-owned picker generation.
	// It never changes while the controller lives; the presentation epoch is
	// the broker epoch, not this value.
	pickerGeneration = uint64(1)
	// pickerNoticeLifetime bounds how long one catalogue notice is displayed.
	pickerNoticeLifetime = 4 * time.Second
)

// pickerController owns one client-owned picker: catalogue, model, renderer,
// bounded notices, and input ownership. It is safe for concurrent use, so the
// supervisor's input lifetime and its run path may both reach it.
type pickerController struct {
	clock     ports.Clock
	catalogue *pickerCatalogue

	mu          sync.Mutex
	loop        *pickerLoop
	consumer    pickerConsumer
	renderer    *pickerRenderer
	notices     ui.ToastManager
	interaction uint64
	ownsInput   bool
	opsReady    chan struct{}
	lastOp      pickerOp
	// lastCommitKey is the catalogue key captured with the pending commit
	// decision, so TakeOp can hand a driver the exact committed row.
	lastCommitKey string
	// lastKillKey is the catalogue key captured with a pending kill decision.
	lastKillKey string
	// noticedFailures records the failure episode already toasted per remote
	// host, so one outage toasts once (main's notifyNewRemoteFailures).
	noticedFailures map[string]uint64
	preview         pickerusecase.Preview
	// flushStop cancels the armed lone-escape flush. It is non-nil exactly
	// while one flush is armed for the currently withheld prefix.
	flushStop chan struct{}
}

// newPickerController builds a picker over an empty catalogue. The picker owns
// input from the start: P5.2b presents it for the whole picker presentation and
// has no session pipeline to defer to.
func newPickerController(clock ports.Clock, freshness time.Duration, trueColor bool) *pickerController {
	if supervisorNil(clock) {
		clock = systemClock{}
	}
	controller := &pickerController{
		clock:     clock,
		catalogue: newPickerCatalogue(pickerCatalogueConfig{Clock: clock, Freshness: freshness}),
		renderer:  newPickerRenderer(pickerColorProfile(trueColor)),
		ownsInput: true,
		opsReady:  make(chan struct{}, 1),
	}
	controller.consumer.setOwned(pickerGeneration, pickerGeneration)
	return controller
}

// Catalogue exposes the projection for a driver that needs to resolve a
// selection or inspect the applied revision.
func (p *pickerController) Catalogue() *pickerCatalogue {
	if p == nil {
		return nil
	}
	return p.catalogue
}

// SetOwnsInput toggles input ownership. It is the future attach slice's release
// boundary; P5.2b keeps it owned for the whole run.
func (p *pickerController) SetOwnsInput(owns bool) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if owns == p.ownsInput {
		return
	}
	p.ownsInput = owns
	if owns {
		p.consumer.setOwned(pickerGeneration, pickerGeneration)
		return
	}
	p.consumer.clear()
}

// ApplySnapshot folds one broker publication into the catalogue and rebuilds
// the presentation synchronously, so the next Render reflects it immediately.
func (p *pickerController) ApplySnapshot(snapshot ports.BrokerSnapshot) {
	if p == nil || p.catalogue == nil {
		return
	}
	if !p.catalogue.Apply(snapshot) {
		// A publication that fails the broker snapshot contract is rejected as
		// a whole (the previous projection is kept) and surfaced as a bounded
		// notice instead of silently vanishing. A merely stale publication
		// validates and is ignored without a notice.
		if snapshot.Validate() != nil {
			p.offerNotice("picker-snapshot-rejected", "broker catalogue update rejected")
		}
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rebuildLocked()
	p.offerDiagnosticsLocked()
}

// ConsumeTerminalRead decodes one terminal read while the picker owns input,
// applies it to the model, and records the presentation decision (with any
// committed key) atomically under the controller lock. It reports whether the
// read was consumed: a false result means the supervisor keeps its ordinary
// handling. It never writes a session.
func (p *pickerController) ConsumeTerminalRead(data []byte) bool {
	if p == nil {
		return false
	}
	return p.handleRead(data)
}

// recordOpLocked accumulates one read's presentation decision. A commit also
// captures the committed row key under the same lock, so the key and the
// decision can never disagree.
func (p *pickerController) recordOpLocked(op pickerOp) {
	if op.commit {
		p.lastCommitKey = p.commitKeyLocked()
	}
	if op.kill {
		p.lastKillKey = p.killKeyLocked()
	}
	p.lastOp = mergePickerOps(p.lastOp, op)
	if p.lastOp.commit || p.lastOp.close || p.lastOp.kill {
		// Coalesce exactly one wakeup: a driver that has not yet run TakeOp
		// re-reads the accumulated decision, so a dropped signal is harmless.
		select {
		case p.opsReady <- struct{}{}:
		default:
		}
	}
}

// OpsReady wakes the supervisor when a commit or close was recorded. It is a
// capacity-one coalescing signal; TakeOp remains the authority on what was
// decided, so a driver never infers the decision from the wakeup alone.
func (p *pickerController) OpsReady() <-chan struct{} {
	if p == nil {
		return nil
	}
	return p.opsReady
}

// commitKeyLocked returns the opaque catalogue key of the row under the cursor
// for a commit, or the empty string when no committable row is selected.
func (p *pickerController) commitKeyLocked() string {
	if p.loop == nil {
		return ""
	}
	selection, ok := commitSelection(p.loop, protocol.PickerActionNavigate, 0)
	if !ok {
		return ""
	}
	return selection.Key
}

// killKeyLocked returns the opaque catalogue key of the row under the cursor
// when it authorises destruction, or the empty string.
func (p *pickerController) killKeyLocked() string {
	if p.loop == nil {
		return ""
	}
	selection, ok := killSelection(p.loop, 0)
	if !ok {
		return ""
	}
	return selection.Key
}

// TakeOp returns and clears the accumulated presentation decision since the
// last call, together with the catalogue key of the row the commit decision
// selected at the moment it was recorded. Resolving that key with ResolveKey
// attaches exactly the committed row: TakeOp cannot observe a cursor that a
// concurrent ApplySnapshot moved afterwards. The key is empty when the
// decision carries no commit or no committable row.
func (p *pickerController) TakeOp() (pickerOp, string) {
	if p == nil {
		return pickerOp{}, ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	op := p.lastOp
	key := p.lastCommitKey
	if !op.commit {
		// A kill carries its own captured row; a commit in the same read wins
		// because it leaves the picker anyway.
		key = p.lastKillKey
	}
	p.lastOp = pickerOp{}
	p.lastCommitKey = ""
	p.lastKillKey = ""
	return op, key
}

// FlushPending resolves a withheld escape or UTF-8 prefix once its
// disambiguation window expired. The bound belongs to the driver; the picker
// only decides what the prefix means.
func (p *pickerController) FlushPending() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.ownsInput {
		return
	}
	outcome, consumed := p.consumer.flushPending()
	if !consumed || p.loop == nil {
		return
	}
	if outcome.interaction != pickerGeneration || outcome.generation != pickerGeneration {
		return
	}
	op, changed := applyPickerBatch(p.loop, outcome.events)
	p.recordOpLocked(op)
	if changed {
		select {
		case p.opsReady <- struct{}{}:
		default:
		}
	}
}

// armFlushLocked bounds a withheld escape or UTF-8 prefix with the picker's
// disambiguation window on the controller clock, so a lone Escape keypress
// resolves without waiting for another key. A later read disarms it first.
// Callers hold p.mu.
func (p *pickerController) armFlushLocked() {
	if p.flushStop != nil || !p.consumer.hasPending() {
		return
	}
	timer := p.clock.NewTimer(pickerEscapeDeadline)
	if supervisorNil(timer) {
		return
	}
	stop := make(chan struct{})
	p.flushStop = stop
	go func() {
		select {
		case <-timer.C():
		case <-stop:
			stopSupervisorTimer(timer)
			return
		}
		p.mu.Lock()
		if p.flushStop != stop {
			p.mu.Unlock()
			return
		}
		p.flushStop = nil
		p.mu.Unlock()
		p.FlushPending()
	}()
}

// disarmFlushLocked cancels an armed flush: the next read resolves the
// withheld prefix itself. Callers hold p.mu.
func (p *pickerController) disarmFlushLocked() {
	if p.flushStop == nil {
		return
	}
	close(p.flushStop)
	p.flushStop = nil
}

// invalidatePresentation makes the next Render redraw the whole box, because
// another owner wrote the terminal since the last picker frame.
func (p *pickerController) invalidatePresentation() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.renderer.invalidate()
	p.mu.Unlock()
}

// OwnsInput reports whether the picker currently owns terminal input.
func (p *pickerController) OwnsInput() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ownsInput
}

// Render composes the current picker modal into terminal bytes for one display
// size. It is a pure read of the current model: it changes no state and writes
// nothing.
func (p *pickerController) Render(size domain.Size) []byte {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.loop == nil {
		return nil
	}
	return p.renderer.render(p.loop, size, p.preview)
}

// SetPreview displays one preview frame in the given state. A frame is shown
// only in the fresh and stale states; stale dims it. The other states show a
// short dim label instead, so the preview pane is never silently blank.
func (p *pickerController) SetPreview(preview protocol.RemotePreview, state previewState) {
	if p == nil {
		return
	}
	view := previewView(preview, state)
	p.mu.Lock()
	p.preview = view
	p.mu.Unlock()
}

// PreviewRequest derives preview authority from the exact catalogue ref behind
// the selected row. Only live exact sessions are previewable; a tab row
// previews its own tab and a session row the active one. Create rows and
// stopped sessions are deliberately refused.
func (p *pickerController) PreviewRequest(size domain.Size) (ports.BrokerPreviewRoute, protocol.RemotePreviewRequest, bool) {
	refuse := func() (ports.BrokerPreviewRoute, protocol.RemotePreviewRequest, bool) {
		return ports.BrokerPreviewRoute{}, protocol.RemotePreviewRequest{}, false
	}
	key, ok := p.CursorKey()
	if !ok || p.catalogue == nil {
		return refuse()
	}
	ref, ok := p.catalogue.Ref(key)
	if !ok || ref.kind != pickerSelectionExact {
		return refuse()
	}
	// Resolving with a provisional stream reuses every refusal the commit path
	// applies (epoch, replaced registration, incompatibility, gone session);
	// the broker allocates the real observation stream itself.
	resolved, err := p.catalogue.ResolveRef(ref, pickerResolveBase{Connection: ports.BrokerConnectionID{1}, Stream: 1})
	if err != nil {
		return refuse()
	}
	p.catalogue.mu.Lock()
	authority := p.catalogue.authorityForRefLocked(ref)
	p.catalogue.mu.Unlock()
	if !authority.found {
		return refuse()
	}
	session, ok := pickerFindSession(authority.observation.Sessions, ref.lifecycle)
	if !ok || session.State != catalogue.RemoteCatalogSessionUp {
		return refuse()
	}
	previewTab := domain.TabStableID(session.ActiveTabID)
	if ref.tab.present {
		previewTab = ref.tab.id
	}
	if previewTab == "" {
		return refuse()
	}
	route := ports.BrokerPreviewRoute{Local: resolved.Local, Policy: resolved.Policy}
	endpoint := ports.BrokerPreviewLocalEndpoint
	if !resolved.Local {
		route.Endpoint, route.Registration = resolved.Endpoint, resolved.Registration
		endpoint = resolved.Endpoint
	}
	viewport := pickerPreviewSize(size)
	target := domain.RemoteSessionTarget{Endpoint: endpoint, DisplayOrigin: authority.observation.DisplayOrigin, LifecycleID: session.LifecycleID, SessionName: session.Name, LiveTabID: previewTab}
	if target.DisplayOrigin == "" {
		target.DisplayOrigin = pickerOriginLabel(authority.observation)
	}
	preview := protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: target, Width: clampPreviewDimension(viewport.Cols, protocol.RemotePreviewMaxWidth), Height: clampPreviewDimension(viewport.Rows, protocol.RemotePreviewMaxHeight)}
	if route.Validate() != nil || protocol.ValidateRemotePreviewRequest(preview) != nil {
		return refuse()
	}
	return route, preview, true
}

func (p *pickerController) ClearPreview() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.preview = pickerusecase.Preview{}
	p.mu.Unlock()
}

// RenderNotice composes the newest bounded catalogue notice as a client-local
// toast. It reuses the client's existing toast rendering and returns nil when
// nothing is displayed.
func (p *pickerController) RenderNotice(size domain.Size) []byte {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	active := p.notices.Active(p.clock.Now())
	p.mu.Unlock()
	if len(active) == 0 {
		return nil
	}
	// ToastManager.Active returns toasts in map-iteration order, so "the last
	// one" is arbitrary and can change between frames. Select the newest by
	// ShownAt, tie-broken deterministically by ID.
	newest := active[0]
	for _, toast := range active[1:] {
		if toast.ShownAt.After(newest.ShownAt) ||
			(toast.ShownAt.Equal(newest.ShownAt) && toast.ID > newest.ID) {
			newest = toast
		}
	}
	if newest.Message == "" {
		return nil
	}
	var buffer bytes.Buffer
	if _, err := drawClientToast(&buffer, size, newest.Message); err != nil {
		return nil
	}
	return buffer.Bytes()
}

// CursorKey reports the opaque key of the row under the cursor, or false when
// nothing qualifies. It is the selection displayed now; a commit decision is
// captured with TakeOp instead so a concurrent publication cannot move it.
func (p *pickerController) CursorKey() (string, bool) {
	if p == nil {
		return "", false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key := p.commitKeyLocked()
	if key == "" {
		return "", false
	}
	return key, true
}

// Resolve revalidates the row displayed now and returns the exact broker
// stream request. A row the picker shows but does not admit an action still
// resolves, so committing an inspectable row yields a typed catalogue refusal
// (stale, incompatible, unavailable) instead of a generic no-selection. A
// refusal is bounded and is also surfaced as a notice.
//
// Resolve reads the mutable presentation model, so a concurrent ApplySnapshot
// can move the cursor between the caller's commit decision and this call. A
// driver that is committing a specific row must capture that row's key with
// the decision (TakeOp) and resolve it with ResolveKey instead.
func (p *pickerController) Resolve(base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	if p == nil {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueNoSelection, Text: "picker is not available"}
	}
	key, ok := p.displayedKey()
	if !ok {
		err := pickerCatalogueError{Code: pickerCatalogueNoSelection, Text: "no row is selected"}
		p.offerNotice("picker-refusal", err.Error())
		return ports.BrokerOpenStreamRequest{}, err
	}
	return p.ResolveKey(key, base)
}

// ResolveKey revalidates exactly the supplied catalogue key against the latest
// applied snapshot and returns the exact broker stream request. It is the
// commit path: the key captured atomically with the commit decision (see
// TakeOp) resolves the row the user committed even if the cursor has since
// moved. A refusal is bounded and is also surfaced as a notice.
func (p *pickerController) ResolveKey(key string, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	if p == nil || p.catalogue == nil {
		err := pickerCatalogueError{Code: pickerCatalogueNoSelection, Text: "picker is not available"}
		if p != nil {
			p.offerNotice("picker-refusal", err.Error())
		}
		return ports.BrokerOpenStreamRequest{}, err
	}
	request, _, err := p.ResolveKeyTarget(key, base)
	return request, err
}

// ResolveKeyTarget is ResolveKey plus the exact tab the committed row names.
func (p *pickerController) ResolveKeyTarget(key string, base pickerResolveBase) (ports.BrokerOpenStreamRequest, attachmentTab, error) {
	if p == nil || p.catalogue == nil {
		err := pickerCatalogueError{Code: pickerCatalogueNoSelection, Text: "picker is not available"}
		if p != nil {
			p.offerNotice("picker-refusal", err.Error())
		}
		return ports.BrokerOpenStreamRequest{}, attachmentTab{}, err
	}
	ref, ok := p.catalogue.Ref(key)
	if !ok {
		err := pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "picker row is not in the catalogue"}
		p.offerNotice("picker-refusal", pickerRefusalNotice(err))
		return ports.BrokerOpenStreamRequest{}, attachmentTab{}, err
	}
	var request ports.BrokerOpenStreamRequest
	var tab attachmentTab
	var err error
	if ref.kind == pickerSelectionCreateNamed || ref.kind == pickerSelectionCreateEphemeral {
		request, err = p.catalogue.Resolve(key, base)
	} else if ref.kind == pickerSelectionExact {
		request, tab, err = p.catalogue.ResolveTarget(key, base)
	} else {
		err = pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "this picker row is not a session destination"}
	}
	if err != nil {
		p.offerNotice("picker-refusal", pickerRefusalNotice(err))
		return ports.BrokerOpenStreamRequest{}, attachmentTab{}, err
	}
	return request, tab, nil
}

// ResolveKill revalidates one kill key; a refusal is surfaced as a notice.
func (p *pickerController) ResolveKill(key string) (pickerKillTarget, error) {
	if p == nil || p.catalogue == nil {
		return pickerKillTarget{}, pickerCatalogueError{Code: pickerCatalogueNoSelection, Text: "picker is not available"}
	}
	target, err := p.catalogue.ResolveKill(key)
	if err != nil {
		p.offerNotice("picker-refusal", pickerRefusalNotice(err))
		return pickerKillTarget{}, err
	}
	return target, nil
}

// SetCurrent names the attachment the picker is about to be presented over
// (or none) and restarts the presentation, so a fresh picker opens with the
// cursor on that session's tab and no leftover search, exactly as each daemon
// interaction did on main.
func (p *pickerController) SetCurrent(current pickerCurrent) {
	if p == nil || p.catalogue == nil {
		return
	}
	p.catalogue.SetCurrent(current)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.catalogue.Epoch() == 0 {
		return
	}
	p.loop = nil
	p.rebuildLocked()
	p.renderer.invalidate()
}

// pickerRefusalNotice is the toast text for one resolution refusal.
func pickerRefusalNotice(err error) string {
	var typed pickerCatalogueError
	if errors.As(err, &typed) {
		return typed.noticeText()
	}
	return err.Error()
}

// displayedKey names the row under the cursor, whether or not it admits an
// action, and maps its presentation key back to the source identity.
func (p *pickerController) displayedKey() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.loop == nil || p.loop.model == nil {
		return "", false
	}
	line, ok := p.loop.model.Cursor()
	if !ok {
		return "", false
	}
	identity, ok := p.loop.rows[line.Key]
	if !ok {
		return "", false
	}
	return identity.key, true
}

func (p *pickerController) handleRead(data []byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.ownsInput {
		return false
	}
	if len(data) != 0 {
		data = append([]byte(nil), data...)
	}
	p.disarmFlushLocked()
	outcome, consumed := p.consumer.consume(terminalReadResult{data: data})
	if !consumed {
		return false
	}
	defer p.armFlushLocked()
	if outcome.interaction != pickerGeneration || outcome.generation != pickerGeneration {
		return true
	}
	if p.loop == nil {
		// Before a catalogue exists only an explicit interrupt is actionable.
		// Use decoded events so a pasted Ctrl-C is not mistaken for an exit.
		for _, event := range outcome.events {
			if event.kind == pickerEventKey && event.key == "Ctrl+C" {
				p.recordOpLocked(pickerOp{close: true, exit: true})
			}
		}
		return true
	}
	op, changed := applyPickerBatch(p.loop, outcome.events)
	p.recordOpLocked(op)
	if changed {
		select {
		case p.opsReady <- struct{}{}:
		default:
		}
	}
	return true
}

func (p *pickerController) rebuildLocked() {
	recent, grouped := p.catalogue.projections()
	revision := uint64(p.catalogue.Revision())
	snapshot := func() protocol.PickerSnapshot {
		return protocol.PickerSnapshot{
			InteractionID:  p.interaction,
			SourceID:       pickerCatalogueSourceID,
			SourceRevision: revision,
			Lines:          grouped.Lines,
			Cursor:         grouped.Cursor,
			Recent:         recent,
			Grouped:        grouped,
		}
	}
	if p.loop == nil {
		p.interaction++
		p.loop = pickerLoopFromSnapshot(snapshot(), protocol.PickerIntentNavigation, defaultPickerSort())
		return
	}
	p.loop.replaceLines(snapshot())
}

// offerDiagnosticsLocked toasts each remote host failure once per failure
// episode (main's notifyNewRemoteFailures). Ordinary observation progress and
// catalogue aging stay on the rows; a recovered host forgets its episode so a
// later outage toasts again.
func (p *pickerController) offerDiagnosticsLocked() {
	failing := make(map[string]struct{})
	for _, failure := range p.catalogue.failures() {
		failing[failure.key] = struct{}{}
		if noticed, ok := p.noticedFailures[failure.key]; ok && noticed == failure.episode {
			continue
		}
		if p.noticedFailures == nil {
			p.noticedFailures = make(map[string]uint64)
		}
		p.noticedFailures[failure.key] = failure.episode
		p.notices.Show(p.clock.Now(), ui.Toast{
			ID:       "picker-host:" + failure.key,
			Message:  failure.message,
			Anchor:   domain.AnchorCenter,
			Duration: pickerNoticeLifetime,
		})
	}
	for key := range p.noticedFailures {
		if _, ok := failing[key]; !ok {
			delete(p.noticedFailures, key)
		}
	}
}

func (p *pickerController) offerNotice(id, message string) {
	if message == "" {
		return
	}
	p.mu.Lock()
	p.notices.Show(p.clock.Now(), ui.Toast{ID: id, Message: message, Anchor: domain.AnchorCenter, Duration: pickerNoticeLifetime})
	p.mu.Unlock()
}

// mergePickerOps accumulates two presentation decisions from one read. Close
// wins because it retires the interaction; commit and kill both remain.
func mergePickerOps(a, b pickerOp) pickerOp {
	return pickerOp{
		commit: a.commit || b.commit,
		kill:   a.kill || b.kill,
		close:  a.close || b.close,
		exit:   a.exit || b.exit,
	}
}
