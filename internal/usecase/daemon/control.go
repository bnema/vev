package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sort"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/layout"
)

// handleCommand serves one one-shot control request. The leading version is
// checked before decoding the versioned body so newer layouts fail cleanly.
func (d *Daemon) handleCommand(tr ports.Transport, f ports.Frame) error {
	defer func() { _ = tr.Close() }()

	if version, ok := ports.PeekCommandVersion(f.Payload); !ok || version != protocol.Version {
		return d.sendCommandResult(tr, protocol.CommandResult{Code: protocol.ErrVersionMismatch, Text: "protocol version mismatch"})
	}
	request, err := ports.UnmarshalCommandRequest(f.Payload)
	if err != nil {
		return d.sendCommandResult(tr, protocol.CommandResult{Code: protocol.ErrInternal, Text: "malformed command request"})
	}
	if request.Attached {
		return d.sendCommandResult(tr, protocol.CommandResult{
			RequestID: request.RequestID,
			Code:      protocol.ErrNotScriptable,
			Text:      "attached command relay is not enabled",
		})
	}

	tracker := NewCommandRequestTracker()
	const generation = uint64(1)
	outcome, _ := tracker.Track(request.RequestID, generation)
	commandCtx := d.serveCtx
	if commandCtx == nil {
		commandCtx = context.Background()
	}
	commandCtx, cancel := context.WithCancel(commandCtx)
	defer cancel()
	go func() {
		result := d.dispatchCommand(commandCtx, request)
		result.RequestID = request.RequestID
		tracker.Complete(generation, result)
	}()
	commandClock := d.clock
	if commandClock == nil {
		commandClock = systemClock{}
	}
	result, waitErr := tracker.Wait(commandCtx, commandClock, request.RequestID, generation, outcome)
	if waitErr != nil {
		return d.sendCommandResult(tr, protocol.CommandResult{
			RequestID: request.RequestID,
			Code:      protocol.ErrInternal,
			Text:      waitErr.Error(),
		})
	}
	return d.sendCommandResult(tr, result)
}

// sendCommandResult gives one-shot control responses the same observable
// transport-failure behavior regardless of which validation path produced it.
func (d *Daemon) sendCommandResult(tr ports.Transport, result protocol.CommandResult) error {
	if err := d.boundedControlSend(tr, frameCommandResult(result)); err != nil {
		d.log.Warn("command response send failed", "err", err)
		return err
	}
	return nil
}

// boundedControlSend keeps one-shot control handlers from waiting forever on a
// client that stopped reading. A timeout closes the exact transport so a
// blocked Send can unwind before the handler returns.
func (d *Daemon) boundedControlSend(tr ports.Transport, frame ports.Frame) error {
	_, err := d.boundedSendWith(tr, func() error { return tr.Send(frame) })
	if errors.Is(err, errSendTimedOut) {
		_ = tr.Close()
	}
	return err
}

func frameCommandResult(result protocol.CommandResult) ports.Frame {
	return ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(result)}
}

func (d *Daemon) dispatchCommand(ctx context.Context, request protocol.CommandRequest) protocol.CommandResult {
	cmd, ok := command.BySlug(request.Slug)
	if !ok {
		return commandFailure(protocol.ErrUnknownCommand, "unknown command: "+request.Slug)
	}
	if !cmd.Scriptable || cmd.Control == nil {
		return commandFailure(protocol.ErrNotScriptable, request.Slug+" requires an attached client")
	}
	if cmd.Target == command.TargetNone && !request.Self {
		return d.runControl(cmd, controlExec{ctx: ctx, d: d, recoveryName: request.TargetSession}, request)
	}

	sess, code, text := d.resolveTargetSession(request)
	if sess == nil {
		return commandFailure(code, text)
	}
	if cmd.Scope == command.CommandScopeCrossSession {
		// Cross-session moves intentionally skip sess.dispatchMu: movePane and
		// moveTab acquire both session dispatch locks through lockMoveDispatch in
		// global order. Pre-locking the source here could recreate the
		// opposite-direction deadlock covered by
		// TestHandleCommandOppositeMoveCommandsDoNotDeadlock.
		tb, pane, code, text := resolveControlTarget(sess, cmd.Target, request)
		if code != 0 {
			return commandFailure(code, text)
		}
		return d.runControl(cmd, controlExec{ctx: ctx, d: d, sess: sess, tab: tb, target: daemonActionTarget{session: sess, tab: tb, pane: pane}}, request)
	}

	for {
		select {
		case <-ctx.Done():
			return commandFailure(protocol.ErrInternal, ctx.Err().Error())
		default:
		}
		if sess.dispatchMu.TryLock() {
			break
		}
		runtime.Gosched()
	}
	defer sess.dispatchMu.Unlock()

	tb, pane, code, text := resolveControlTarget(sess, cmd.Target, request)
	if code != 0 {
		return commandFailure(code, text)
	}
	return d.runControl(cmd, controlExec{ctx: ctx, d: d, sess: sess, tab: tb, target: daemonActionTarget{session: sess, tab: tb, pane: pane}}, request)
}

func (d *Daemon) runControl(cmd command.Command, exec controlExec, request protocol.CommandRequest) protocol.CommandResult {
	result, err := cmd.Control(exec, request.Args, command.ControlOptions{JSON: request.JSON})
	if err == nil {
		return protocol.CommandResult{OK: true, Output: result.Output}
	}
	switch {
	case errors.Is(err, command.ErrInvalidArguments), errors.Is(err, errSessionNameRequired), errors.Is(err, domain.ErrInvalidSessionName):
		return commandFailure(protocol.ErrInvalidCommandArgs, "usage: "+cmd.Usage)
	case errors.Is(err, errSessionNameInUse):
		return commandFailure(protocol.ErrNameTaken, err.Error())
	case isMoveCommandError(err):
		return moveCommandFailure(err)
	case errors.Is(err, layout.ErrNotInSplit):
		return commandFailure(protocol.ErrNoSuchTarget, "pane is not in a split")
	case errors.Is(err, layout.ErrTooSmall):
		return commandFailure(protocol.ErrNoSuchTarget, "pane cannot be resized further")
	default:
		return commandFailure(protocol.ErrInternal, err.Error())
	}
}

func commandFailure(code uint16, text string) protocol.CommandResult {
	return protocol.CommandResult{Code: code, Text: text}
}

// resolveTargetSession applies explicit-name, stable-ID, then unique-session
// resolution. A name paired with complete stable IDs is always advisory because
// a process keeps its original VEV session component after relocation.
func (d *Daemon) resolveTargetSession(request protocol.CommandRequest) (*session, uint16, string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	named := d.findByNameLocked(request.TargetSession)
	if request.Self && (request.TargetTab == "" || request.TargetPane == "") {
		return nil, protocol.ErrNoSuchTarget, "--self requires target tab and pane IDs"
	}
	if request.TargetSession != "" && request.TargetTab == "" && request.TargetPane == "" {
		if named == nil {
			return nil, protocol.ErrNoSuchTarget, "no such session: " + request.TargetSession
		}
		return named, 0, ""
	}
	if request.TargetTab != "" || request.TargetPane != "" {
		if request.TargetTab == "" || request.TargetPane == "" {
			return nil, protocol.ErrNoSuchTarget, "target tab and pane IDs must be provided together"
		}
		var match *session
		for _, sess := range d.sessions {
			if sess == nil || !sess.containsStableIDs(request.TargetTab, request.TargetPane) {
				continue
			}
			if match != nil {
				return nil, protocol.ErrAmbiguousTarget, "target tab and pane IDs are ambiguous"
			}
			match = sess
		}
		if match != nil {
			return match, 0, ""
		}
		return nil, protocol.ErrNoSuchTarget, "no live session contains the target tab/pane"
	}
	locals := sessionsSnapshot(d.sessions)
	if len(locals) == 0 {
		return nil, protocol.ErrNoSuchTarget, "no live sessions"
	}
	if len(locals) != 1 {
		return nil, protocol.ErrAmbiguousTarget, "several sessions are live; use -s <session> or run from inside a pane"
	}
	return locals[0], 0, ""
}

func (s *session) containsStableIDs(tabID, paneID string) bool {
	s.mu.Lock()
	tabs := append([]*tab(nil), s.tabs...)
	s.mu.Unlock()
	for _, tb := range tabs {
		tb.mu.Lock()
		pane := paneByStableIDLocked(tb, paneID)
		match := tb.stableID == tabID && pane != nil
		tb.mu.Unlock()
		if match {
			return true
		}
	}
	return false
}

func resolveControlTarget(sess *session, kind command.TargetKind, request protocol.CommandRequest) (*tab, *pane, uint16, string) {
	sess.mu.Lock()
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	if request.Self {
		for _, tb := range tabs {
			tb.mu.Lock()
			if tb.stableID != request.TargetTab {
				tb.mu.Unlock()
				continue
			}
			target := paneByStableIDLocked(tb, request.TargetPane)
			tb.mu.Unlock()
			if target == nil {
				return nil, nil, protocol.ErrNoSuchTarget, "target pane does not belong to target tab"
			}
			return tb, target, 0, ""
		}
		return nil, nil, protocol.ErrNoSuchTarget, "no such target tab"
	}
	if len(tabs) == 0 {
		return nil, nil, protocol.ErrNoSuchTarget, "target session has no tabs"
	}
	tb := tabs[0]
	if kind != command.TargetPane {
		return tb, nil, 0, ""
	}
	tb.mu.Lock()
	target := tb.focusedPane()
	tb.mu.Unlock()
	if target == nil {
		return nil, nil, protocol.ErrNoSuchTarget, "no such target pane"
	}
	return tb, target, 0, ""
}

func paneByStableIDLocked(tb *tab, stableID string) *pane {
	for _, pane := range tb.panes {
		if pane.stableID == stableID {
			return pane
		}
	}
	return nil
}

type daemonActionKind uint8

const (
	daemonActionCreateTab daemonActionKind = iota
	daemonActionCloseTab
	daemonActionSplitPane
	daemonActionStackPane
	daemonActionToggleStack
	daemonActionClosePane
	daemonActionFocusPane
	daemonActionNextTab
	daemonActionPreviousTab
	daemonActionRenameSession
	daemonActionRenameTab
	daemonActionResizePane
	daemonActionEqualizePanes
	daemonActionConsumeOrExpelPane
)

type daemonActionTarget struct {
	session    *session
	attachment *attachedClient
	tab        *tab
	pane       *pane
}

type daemonActionRequest struct {
	kind      daemonActionKind
	target    daemonActionTarget
	effect    *attachmentEffect
	direction layout.Direction
	axis      layout.Axis
	delta     int
	viewport  domain.Size
	name      string
}

type daemonActionRunner interface {
	Run(daemonActionRequest) error
}

func resolveDaemonActionTargetForAttachment(sess *session, ac *attachedClient) daemonActionTarget {
	if sess == nil {
		return daemonActionTarget{}
	}
	if ac != nil {
		tb, pane := sess.paneForAttachment(ac)
		if tb == nil {
			return daemonActionTarget{session: sess, attachment: ac}
		}
		return daemonActionTarget{session: sess, attachment: ac, tab: tb, pane: pane}
	}
	tb := sess.firstTab()
	if tb == nil {
		return daemonActionTarget{session: sess}
	}
	tb.mu.Lock()
	pane := tb.focusedPane()
	tb.mu.Unlock()
	return daemonActionTarget{session: sess, tab: tb, pane: pane}
}

// daemonActions is the daemon-owned mutation seam shared by palette and
// control. Requests contain already-resolved targets and no attachment state;
// adapters remain responsible for their attached-client UI lifecycle.
type daemonActions struct{ d *Daemon }

func (a daemonActions) Run(request daemonActionRequest) error {
	target := request.target
	switch request.kind {
	case daemonActionCreateTab:
		return a.d.createTabForAttachment(target.session, target.attachment, request.viewport)
	case daemonActionCloseTab:
		return a.d.closeTabLockedWithEffect(target.session, target.tab, true, request.effect)
	case daemonActionSplitPane:
		change, err := a.d.splitPaneAt(target.session, target.tab, target.pane, request.direction)
		if err == nil {
			publishPaneFocusForAttachment(target.session, target.attachment, change)
		}
		return err
	case daemonActionStackPane:
		change, err := a.d.stackPaneAt(target.session, target.tab, target.pane)
		if err == nil {
			publishPaneFocusForAttachment(target.session, target.attachment, change)
		}
		return err
	case daemonActionToggleStack:
		return a.d.toggleStackAt(target.session, target.tab, target.pane)
	case daemonActionClosePane:
		if a.d.ptys == nil {
			return nil
		}
		if !a.d.hasDaemonActionPaneTarget(target) {
			return layout.ErrNotFound
		}
		if err := a.d.closePaneLockedWithEffect(target.session, target.tab, target.pane.id, nil, true, request.effect); err != nil {
			return err
		}
		if a.d.hasDaemonActionPaneTarget(target) {
			return errors.New("pane close did not complete")
		}
		return nil
	case daemonActionFocusPane:
		change, err := a.d.focusDirAt(target.session, target.tab, target.pane, request.direction)
		if err == nil {
			publishPaneFocusForAttachment(target.session, target.attachment, change)
		}
		return err
	case daemonActionNextTab:
		return a.switchRelative(target.session, target.attachment, 1)
	case daemonActionPreviousTab:
		return a.switchRelative(target.session, target.attachment, -1)
	case daemonActionRenameSession:
		return a.d.renameSession(target.session, request.name)
	case daemonActionRenameTab:
		return a.d.renameTab(target.session, target.tab, request.name)
	case daemonActionResizePane:
		return a.d.resizePane(target, request.axis, request.delta)
	case daemonActionEqualizePanes:
		return a.d.equalizePanes(target)
	case daemonActionConsumeOrExpelPane:
		return a.d.consumeOrExpelPane(target, request.direction)
	default:
		return errors.New("daemon: unknown action")
	}
}

// hasDaemonActionPaneTarget verifies that an already-resolved pane still
// belongs to the live tab and session. Close-pane uses it both before mutation
// and as a postcondition because closing a final pane delegates to close-tab.
func (d *Daemon) hasDaemonActionPaneTarget(target daemonActionTarget) bool {
	if target.session == nil || target.tab == nil || target.pane == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sessions[target.session.id] != target.session {
		return false
	}
	target.session.mu.Lock()
	defer target.session.mu.Unlock()
	if !slices.Contains(target.session.tabs, target.tab) {
		return false
	}
	target.tab.mu.Lock()
	defer target.tab.mu.Unlock()
	return target.tab.panes[target.pane.id] == target.pane &&
		target.tab.tree != nil && layout.ContainsLeaf(target.tab.tree.Root, target.pane.id)
}

func (a daemonActions) switchRelative(sess *session, ac *attachedClient, delta int) error {
	if sess.switchAttachmentRelativeForDispatch(ac, delta) {
		a.d.activateTabAfterResizeForLease(sess, sess.tabForAttachment(ac), false, ac, nil)
	}
	return nil
}

// controlExec implements command.ControlContext against resolved daemon-owned
// targets. It deliberately contains no attached-client state.
type controlExec struct {
	ctx          context.Context
	d            *Daemon
	sess         *session
	tab          *tab
	recoveryName string
	target       daemonActionTarget
	actions      daemonActionRunner
}

func (e controlExec) runAction(request daemonActionRequest) error {
	if request.target.session == nil {
		request.target = e.target
	}
	runner := e.actions
	if runner == nil {
		runner = daemonActions{d: e.d}
	}
	err := runner.Run(request)
	if errors.Is(err, errDaemonActionNoChange) {
		return nil
	}
	if err != nil {
		return err
	}
	if e.actions == nil {
		finishDaemonActionForClient(e.d, request, request.target.attachment, "control.go")
	}
	return nil
}

func finishDaemonActionForClient(d *Daemon, request daemonActionRequest, ac *attachedClient, producer string) {
	if ac == nil {
		return
	}
	if request.kind == daemonActionFocusPane && request.target.tab != nil && request.target.pane != nil {
		d.finishPaneFocusForClient(request.target.session, ac, request.target.tab, request.target.pane.id, producer)
		return
	}
	d.invalidateRender(request.target.session, ac, true, producer)
}

func (e controlExec) CreateTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionCreateTab, viewport: e.sess.fullViewportSize()})
}
func (e controlExec) CreateSessionNamed(name string) error {
	if err := domain.ValidateSessionName(name); err != nil {
		return command.ErrInvalidArguments
	}
	e.sess.mu.Lock()
	cwd, env := e.sess.cwd, copyEnvironment(e.sess.env)
	e.sess.mu.Unlock()
	geometry := domain.Geometry{Size: e.sess.fullViewportSize()}
	if source, ok := e.sess.geometry.sourceSnapshot(e.sess); ok {
		geometry = source.geometry
	}
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	if e.d.closing {
		return errors.New("daemon is shutting down")
	}
	if e.d.nameLiveOrStoppedLocked(name) {
		return errSessionNameInUse
	}
	_, err := e.d.createSessionLocked(name, false, cwd, geometry, env)
	return err
}
func (e controlExec) CloseTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionCloseTab})
}
func (e controlExec) ClosePane() error {
	return e.runAction(daemonActionRequest{kind: daemonActionClosePane})
}

func (e controlExec) MovePane(destinationSession, destinationTabID string) error {
	destination := e.d.liveSessionByName(destinationSession)
	if destination == nil {
		return errMoveStaleTarget
	}
	if e.sess == nil || e.tab == nil || e.target.pane == nil {
		return errMovePaneInvalid
	}
	attachment := e.target.attachment
	token := attachmentCapability{}
	if attachment != nil {
		token = e.sess.captureAttachmentCapability(attachment, attachment.transport())
		if token.ac == nil {
			return errMoveStaleTarget
		}
	}
	return e.d.movePane(movePaneRequest{
		Attachment:           attachment,
		AttachmentCapability: token,
		Source:               sessionMoveLocator(e.sess),
		SourceTabID:          domain.TabStableID(e.tab.stableID),
		SourcePaneID:         domain.PaneStableID(e.target.pane.stableID),
		Destination:          sessionMoveLocator(destination),
		DestinationTabID:     domain.TabStableID(destinationTabID),
	})
}

func (e controlExec) MoveTab(destinationSession string) error {
	destination := e.d.liveSessionByName(destinationSession)
	if destination == nil {
		return errMoveStaleTarget
	}
	if e.sess == nil || e.tab == nil {
		return errMovePaneInvalid
	}
	attachment := e.target.attachment
	token := attachmentCapability{}
	if attachment != nil {
		token = e.sess.captureAttachmentCapability(attachment, attachment.transport())
		if token.ac == nil {
			return errMoveStaleTarget
		}
	}
	return e.d.moveTab(moveTabRequest{
		Attachment:           attachment,
		AttachmentCapability: token,
		Source:               sessionMoveLocator(e.sess),
		SourceTabID:          domain.TabStableID(e.tab.stableID),
		Destination:          sessionMoveLocator(destination),
	})
}

func (d *Daemon) liveSessionByName(name string) *session {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.findByNameLocked(name)
}

func sessionMoveLocator(sess *session) moveSessionLocator {
	sess.mu.Lock()
	name := sess.name
	sess.mu.Unlock()
	return moveSessionLocator{ID: sess.id, Incarnation: sess.incarnation, Name: name}
}
func (e controlExec) split(direction layout.Direction) error {
	return e.runAction(daemonActionRequest{kind: daemonActionSplitPane, direction: direction})
}
func (e controlExec) SplitRight() error { return e.split(layout.Right) }
func (e controlExec) SplitLeft() error  { return e.split(layout.Left) }
func (e controlExec) SplitUp() error    { return e.split(layout.Up) }
func (e controlExec) SplitDown() error  { return e.split(layout.Down) }
func (e controlExec) consumeOrExpelPane(direction layout.Direction) error {
	return e.runAction(daemonActionRequest{kind: daemonActionConsumeOrExpelPane, direction: direction})
}
func (e controlExec) ConsumeOrExpelPaneLeft() error {
	return e.consumeOrExpelPane(layout.Left)
}
func (e controlExec) ConsumeOrExpelPaneRight() error {
	return e.consumeOrExpelPane(layout.Right)
}
func (e controlExec) StackPane() error {
	return e.runAction(daemonActionRequest{kind: daemonActionStackPane})
}
func (e controlExec) ToggleStack() error {
	return e.runAction(daemonActionRequest{kind: daemonActionToggleStack})
}
func (e controlExec) GrowPaneWidth() error    { return e.resize(layout.Width, resizeStepCols) }
func (e controlExec) ShrinkPaneWidth() error  { return e.resize(layout.Width, -resizeStepCols) }
func (e controlExec) GrowPaneHeight() error   { return e.resize(layout.Height, resizeStepRows) }
func (e controlExec) ShrinkPaneHeight() error { return e.resize(layout.Height, -resizeStepRows) }
func (e controlExec) resize(axis layout.Axis, delta int) error {
	return e.runAction(daemonActionRequest{kind: daemonActionResizePane, axis: axis, delta: delta})
}
func (e controlExec) EqualizePanes() error {
	return resizeUserError(e.runAction(daemonActionRequest{kind: daemonActionEqualizePanes}))
}
func (e controlExec) focus(direction layout.Direction) error {
	return e.runAction(daemonActionRequest{kind: daemonActionFocusPane, direction: direction})
}
func (e controlExec) FocusPaneLeft() error  { return e.focus(layout.Left) }
func (e controlExec) FocusPaneRight() error { return e.focus(layout.Right) }
func (e controlExec) FocusPaneUp() error    { return e.focus(layout.Up) }
func (e controlExec) FocusPaneDown() error  { return e.focus(layout.Down) }
func (e controlExec) NextTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionNextTab})
}
func (e controlExec) PrevTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionPreviousTab})
}
func (e controlExec) RenameSessionTo(name string) error {
	return e.runAction(daemonActionRequest{kind: daemonActionRenameSession, name: name})
}
func (e controlExec) RenameTabTo(name string) error {
	return e.runAction(daemonActionRequest{kind: daemonActionRenameTab, name: name})
}
func (e controlExec) Toast(severity, message string) error {
	var level domain.NoticeSeverity
	switch severity {
	case "info":
		level = domain.NoticeInfo
	case "warn":
		level = domain.NoticeWarn
	case "error":
		level = domain.NoticeError
	default:
		return command.ErrInvalidArguments
	}
	if strings.TrimSpace(message) == "" {
		return command.ErrInvalidArguments
	}
	e.d.notify(e.sess, level, domain.NoticeUser, message, nil)
	return nil
}

func (e controlExec) SessionRecovery(action string) (string, error) {
	if e.d == nil || e.d.recovery == nil || e.recoveryName == "" || action != "discard" {
		return "", command.ErrInvalidArguments
	}
	ctx := e.ctx
	if ctx == nil {
		ctx = e.d.serveCtx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := e.d.recovery.Discard(ctx, e.recoveryName); err != nil {
		return "", err
	}
	record, ok, err := e.d.catalogue.Record(e.recoveryName)
	if err != nil {
		return "", err
	}
	if ok {
		e.d.setStoppedRecovery(record, protocol.SessionDown)
	}
	return "", nil
}

func (e controlExec) ListSessions(asJSON bool) (string, error) {
	type row struct {
		Name      string `json:"name"`
		Ephemeral bool   `json:"ephemeral"`
		Tabs      int    `json:"tabs"`
		Attached  bool   `json:"attached"`
		Active    bool   `json:"active"`
	}
	e.d.mu.Lock()
	sessions := sessionsSnapshot(e.d.sessions)
	e.d.mu.Unlock()
	snaps := make([]sessionView, 0, len(sessions))
	for _, sess := range sessions {
		snaps = append(snaps, sess.snapshotView(viewOptions{}))
	}
	activeIdx := -1
	for i, snap := range snaps {
		if activeIdx == -1 || snap.mruAt > snaps[activeIdx].mruAt {
			activeIdx = i
		}
	}
	rows := make([]row, 0, len(snaps))
	for i, snap := range snaps {
		rows = append(rows, row{Name: snap.name, Ephemeral: snap.ephemeral, Tabs: snap.tabCount, Attached: snap.attached, Active: i == activeIdx})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	if asJSON {
		return marshalListing(rows)
	}
	var out strings.Builder
	out.WriteString("NAME\tSTATE\tTABS\tATTACHED\tACTIVE\n")
	for _, row := range rows {
		state := "named"
		if row.Ephemeral {
			state = "ephemeral"
		}
		fmt.Fprintf(&out, "%s\t%s\t%d\t%t\t%t\n", row.Name, state, row.Tabs, row.Attached, row.Active)
	}
	return out.String(), nil
}

func (e controlExec) RemoteCatalog(asJSON bool) (string, error) {
	if !asJSON {
		return "", command.ErrInvalidArguments
	}
	e.d.mu.Lock()
	sessions := sessionsSnapshot(e.d.sessions)
	stopped := make([]inactiveSession, 0, len(e.d.inactive))
	for _, entry := range e.d.inactive {
		if entry.visible() {
			stopped = append(stopped, entry)
		}
	}
	e.d.mu.Unlock()

	rows := make([]ports.RemoteCatalogSession, 0, len(sessions)+len(stopped))
	liveNames := make(map[string]struct{}, len(sessions))
	for _, sess := range sessions {
		e.d.refreshSessionFocusedTitles(sess)
		snap := sess.snapshotView(viewOptions{tabDetails: true, focusedTitles: true, terminalTitle: false})
		tabs, err := remoteCatalogTabs(snap)
		if err != nil {
			return "", err
		}
		row := ports.RemoteCatalogSession{
			LifecycleID: snap.incarnation,
			Name:        snap.name,
			State:       ports.RemoteCatalogSessionUp,
			Ephemeral:   snap.ephemeral,
			Tabs:        tabs,
			Attached:    snap.attached,
			LastUsedSeq: snap.mruAt,
		}
		if len(tabs) != 0 {
			row.ActiveTabID = tabs[min(snap.defaultTab, len(tabs)-1)].ID
		}
		rows = append(rows, row)
		liveNames[row.Name] = struct{}{}
	}
	for _, entry := range stopped {
		if _, live := liveNames[entry.name]; live {
			continue
		}
		tabs, err := remoteCatalogStoppedTabs(entry)
		if err != nil {
			return "", err
		}
		state := ports.RemoteCatalogSessionDown
		reason := ""
		if entry.broken() {
			state = ports.RemoteCatalogSessionBroken
			reason = "session_broken"
		}
		rows = append(rows, ports.RemoteCatalogSession{
			LifecycleID: entry.incarnation,
			Name:        entry.name,
			State:       state,
			Ephemeral:   false,
			Tabs:        tabs,
			LastUsedSeq: entry.lastUsedSeq,
			Reason:      reason,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Name != rows[j].Name {
			return rows[i].Name < rows[j].Name
		}
		return rows[i].LifecycleID.String() < rows[j].LifecycleID.String()
	})
	catalog := ports.RemoteCatalog{
		ProtocolVersion: protocol.Version,
		SchemaVersion:   ports.RemoteCatalogSchemaVersion,
		Sessions:        rows,
	}
	if err := ports.ValidateRemoteCatalog(catalog); err != nil {
		return "", err
	}
	return marshalListing(catalog)
}

func remoteCatalogTabs(view sessionView) ([]ports.RemoteCatalogTab, error) {
	if len(view.tabs) > ports.RemoteCatalogMaxTabsPerSess {
		return nil, fmt.Errorf("remote catalog: session %q has too many tabs", view.name)
	}
	tabs := make([]ports.RemoteCatalogTab, 0, len(view.tabs))
	for i, tab := range view.tabs {
		tabs = append(tabs, ports.RemoteCatalogTab{
			ID:        string(tab.id),
			Index:     uint16(i),
			Name:      tab.name,
			Detail:    tabTitleDetail(tab.name, tab.focusedTitle),
			Attention: tab.attention,
		})
	}
	return tabs, nil
}

func remoteCatalogStoppedTabs(entry inactiveSession) ([]ports.RemoteCatalogTab, error) {
	records := entry.tabRecords
	if len(records) == 0 && len(entry.tabNames) != 0 {
		records = make([]domain.CatalogueTabRecord, len(entry.tabNames))
		for i, name := range entry.tabNames {
			records[i].Name = name
		}
	}
	if len(records) > ports.RemoteCatalogMaxTabsPerSess {
		return nil, fmt.Errorf("remote catalog: session %q has too many tabs", entry.name)
	}
	tabs := make([]ports.RemoteCatalogTab, 0, len(records))
	for i, record := range records {
		tabs = append(tabs, ports.RemoteCatalogTab{ID: string(record.StableID), Index: uint16(i), Name: record.Name})
	}
	return tabs, nil
}

func (e controlExec) ListTabs(asJSON bool) (string, error) {
	type row struct {
		Index  int    `json:"index"`
		ID     string `json:"id"`
		Name   string `json:"name"`
		Panes  int    `json:"panes"`
		Active bool   `json:"active"`
	}
	e.sess.mu.Lock()
	tabs := append([]*tab(nil), e.sess.tabs...)
	e.sess.mu.Unlock()
	active, _ := e.sess.tabIndexForAttachment(e.target.attachment)
	rows := make([]row, 0, len(tabs))
	for i, tb := range tabs {
		tb.mu.Lock()
		rows = append(rows, row{Index: i, ID: tb.stableID, Name: tb.name, Panes: len(tb.panes), Active: i == active})
		tb.mu.Unlock()
	}
	if asJSON {
		return marshalListing(rows)
	}
	var out strings.Builder
	out.WriteString("INDEX\tID\tNAME\tPANES\tACTIVE\n")
	for _, row := range rows {
		fmt.Fprintf(&out, "%d\t%s\t%s\t%d\t%t\n", row.Index, row.ID, row.Name, row.Panes, row.Active)
	}
	return out.String(), nil
}

func (e controlExec) ListPanes(asJSON bool) (string, error) {
	type row struct {
		ID      string `json:"id"`
		Pane    string `json:"pane"`
		Size    string `json:"size"`
		CWD     string `json:"cwd"`
		Focused bool   `json:"focused"`
	}
	tb := e.tab
	if tb == nil {
		if e.target.attachment == nil {
			tb = e.sess.firstTab()
		} else {
			tb = e.sess.tabForAttachment(e.target.attachment)
		}
	}
	if tb == nil {
		return "", errors.New("target session has no active tab")
	}
	e.sess.mu.Lock()
	fallbackCWD := e.sess.cwd
	e.sess.mu.Unlock()
	var focus layout.PaneID
	var focusStableID domain.PaneStableID
	if e.target.attachment != nil {
		focusStableID = e.target.attachment.viewSnapshot().paneID
	}
	tb.mu.Lock()
	if e.target.attachment == nil {
		if focused := tb.focusedPane(); focused != nil {
			focus = focused.id
		}
	}
	panes := tb.panesSnapshot()
	tb.mu.Unlock()
	rows := make([]row, 0, len(panes))
	for _, pane := range panes {
		pane.mu.Lock()
		size := domain.Size{Cols: pane.rect.Width, Rows: pane.rect.Height}
		pid := 0
		if pane.pty != nil {
			pid = pane.pty.Pid()
		}
		pane.mu.Unlock()
		cwd := fallbackCWD
		if e.d.procCwd != nil && pid > 0 {
			if live, err := e.d.procCwd(pid); err == nil && live != "" {
				cwd = live
			}
		}
		focused := pane.id == focus
		if e.target.attachment != nil {
			focused = domain.PaneStableID(pane.stableID) == focusStableID
		}
		rows = append(rows, row{ID: pane.stableID, Pane: string(pane.id), Size: fmt.Sprintf("%dx%d", size.Cols, size.Rows), CWD: cwd, Focused: focused})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Pane < rows[j].Pane })
	if asJSON {
		return marshalListing(rows)
	}
	var out strings.Builder
	out.WriteString("ID\tPANE\tSIZE\tCWD\tFOCUSED\n")
	for _, row := range rows {
		fmt.Fprintf(&out, "%s\t%s\t%s\t%s\t%t\n", row.ID, row.Pane, row.Size, row.CWD, row.Focused)
	}
	return out.String(), nil
}

func marshalListing(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded) + "\n", nil
}
