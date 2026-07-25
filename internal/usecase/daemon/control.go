package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/layout"
)

// handleCommand serves one one-shot control request. The leading version is
// checked before decoding the versioned body so newer layouts fail cleanly.
func (d *Daemon) handleCommand(tr ports.Transport, f ports.Frame) {
	defer func() { _ = tr.Close() }()

	if version, ok := ports.PeekCommandVersion(f.Payload); !ok || version != ports.ProtocolVersion {
		_ = tr.Send(frameCommandResult(ports.CommandResult{Code: ports.ErrVersionMismatch, Text: "protocol version mismatch"}))
		return
	}
	request, err := ports.UnmarshalCommandRequest(f.Payload)
	if err != nil {
		_ = tr.Send(frameCommandResult(ports.CommandResult{Code: ports.ErrInternal, Text: "malformed command request"}))
		return
	}
	_ = tr.Send(frameCommandResult(d.dispatchCommand(request)))
}

func frameCommandResult(result ports.CommandResult) ports.Frame {
	return ports.Frame{Type: ports.MsgCommandResult, Payload: ports.MarshalCommandResult(result)}
}

func (d *Daemon) dispatchCommand(request ports.CommandRequest) ports.CommandResult {
	cmd, ok := command.BySlug(request.Slug)
	if !ok {
		return commandFailure(ports.ErrUnknownCommand, "unknown command: "+request.Slug)
	}
	if !cmd.Scriptable || cmd.Control == nil {
		return commandFailure(ports.ErrNotScriptable, request.Slug+" requires an attached client")
	}
	if cmd.Target == command.TargetNone && !request.Self {
		return d.runControl(cmd, controlExec{d: d}, request)
	}

	sess, code, text := d.resolveTargetSession(request)
	if sess == nil {
		return commandFailure(code, text)
	}
	sess.dispatchMu.Lock()
	defer sess.dispatchMu.Unlock()

	tb, pane, code, text := resolveControlTarget(sess, cmd.Target, request)
	if code != 0 {
		return commandFailure(code, text)
	}
	return d.runControl(cmd, controlExec{d: d, sess: sess, tab: tb, target: daemonActionTarget{session: sess, tab: tb, pane: pane}}, request)
}

func (d *Daemon) runControl(cmd command.Command, exec controlExec, request ports.CommandRequest) ports.CommandResult {
	result, err := cmd.Control(exec, request.Args, command.ControlOptions{JSON: request.JSON})
	if err == nil {
		return ports.CommandResult{OK: true, Output: result.Output}
	}
	switch {
	case errors.Is(err, command.ErrInvalidArguments), errors.Is(err, errSessionNameRequired), errors.Is(err, domain.ErrInvalidSessionName):
		return commandFailure(ports.ErrInvalidCommandArgs, "usage: "+cmd.Usage)
	case errors.Is(err, errSessionNameInUse):
		return commandFailure(ports.ErrNameTaken, err.Error())
	case errors.Is(err, layout.ErrNotInSplit):
		return commandFailure(ports.ErrNoSuchTarget, "pane is not in a split")
	case errors.Is(err, layout.ErrTooSmall):
		return commandFailure(ports.ErrNoSuchTarget, "pane cannot be resized further")
	default:
		return commandFailure(ports.ErrInternal, err.Error())
	}
}

func commandFailure(code uint16, text string) ports.CommandResult {
	return ports.CommandResult{Code: code, Text: text}
}

// resolveTargetSession applies explicit-name, stable-ID, then unique-session
// resolution. A name paired with IDs is advisory only when no live session has
// that name (the VEV value can survive a rename).
func (d *Daemon) resolveTargetSession(request ports.CommandRequest) (*session, uint16, string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	named := d.findByNameLocked(request.TargetSession)
	if request.Self && (request.TargetTab == "" || request.TargetPane == "") {
		return nil, ports.ErrNoSuchTarget, "--self requires target tab and pane IDs"
	}
	if request.TargetSession != "" && request.TargetTab == "" && request.TargetPane == "" {
		if named == nil {
			return nil, ports.ErrNoSuchTarget, "no such session: " + request.TargetSession
		}
		return named, 0, ""
	}
	if request.TargetTab != "" || request.TargetPane != "" {
		if request.TargetTab == "" || request.TargetPane == "" {
			return nil, ports.ErrNoSuchTarget, "target tab and pane IDs must be provided together"
		}
		for _, sess := range d.sessions {
			if sess.containsStableIDs(request.TargetTab, request.TargetPane) {
				if named != nil && named != sess {
					return nil, ports.ErrNoSuchTarget, "tab/pane IDs belong to another session"
				}
				return sess, 0, ""
			}
		}
		return nil, ports.ErrNoSuchTarget, "no live session contains the target tab/pane"
	}
	if len(d.sessions) == 0 {
		return nil, ports.ErrNoSuchTarget, "no live sessions"
	}
	if len(d.sessions) != 1 {
		return nil, ports.ErrAmbiguousTarget, "several sessions are live; use -s <session> or run from inside a pane"
	}
	for _, sess := range d.sessions {
		return sess, 0, ""
	}
	panic("unreachable")
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

func resolveControlTarget(sess *session, kind command.TargetKind, request ports.CommandRequest) (*tab, *pane, uint16, string) {
	sess.mu.Lock()
	tabs := append([]*tab(nil), sess.tabs...)
	active := sess.active
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
				return nil, nil, ports.ErrNoSuchTarget, "target pane does not belong to target tab"
			}
			return tb, target, 0, ""
		}
		return nil, nil, ports.ErrNoSuchTarget, "no such target tab"
	}
	if len(tabs) == 0 || active < 0 || active >= len(tabs) {
		return nil, nil, ports.ErrNoSuchTarget, "target session has no active tab"
	}
	tb := tabs[active]
	if kind != command.TargetPane {
		return tb, nil, 0, ""
	}
	tb.mu.Lock()
	target := tb.focusedPane()
	tb.mu.Unlock()
	if target == nil {
		return nil, nil, ports.ErrNoSuchTarget, "no such target pane"
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
)

type daemonActionTarget struct {
	session *session
	tab     *tab
	pane    *pane
}

type daemonActionRequest struct {
	kind      daemonActionKind
	target    daemonActionTarget
	direction layout.Direction
	axis      layout.Axis
	delta     int
	viewport  domain.Size
	name      string
}

type daemonActionRunner interface {
	Run(daemonActionRequest) error
}

func resolveDaemonActionTarget(sess *session) daemonActionTarget {
	if sess == nil {
		return daemonActionTarget{}
	}
	tb := sess.activeTab()
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
		return a.d.createTab(target.session, request.viewport)
	case daemonActionCloseTab:
		return a.d.closeTab(target.session, target.tab, true)
	case daemonActionSplitPane:
		return a.d.splitPaneAt(target.session, target.tab, target.pane, request.direction)
	case daemonActionStackPane:
		return a.d.stackPaneAt(target.session, target.tab, target.pane)
	case daemonActionToggleStack:
		return a.d.toggleStackAt(target.session, target.tab, target.pane)
	case daemonActionClosePane:
		if a.d.ptys == nil {
			return nil
		}
		if !a.d.hasDaemonActionPaneTarget(target) {
			return layout.ErrNotFound
		}
		if err := a.d.closePane(target.session, target.tab, target.pane.id, nil, true); err != nil {
			return err
		}
		if a.d.hasDaemonActionPaneTarget(target) {
			return errors.New("pane close did not complete")
		}
		return nil
	case daemonActionFocusPane:
		return a.d.focusDirAt(target.session, target.tab, target.pane, request.direction)
	case daemonActionNextTab:
		return a.switchRelative(target.session, 1)
	case daemonActionPreviousTab:
		return a.switchRelative(target.session, -1)
	case daemonActionRenameSession:
		return a.d.renameSession(target.session, request.name)
	case daemonActionRenameTab:
		return a.d.renameTab(target.session, target.tab, request.name)
	case daemonActionResizePane:
		return a.d.resizePane(target, request.axis, request.delta)
	case daemonActionEqualizePanes:
		return a.d.equalizePanes(target)
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
	foundTab := false
	for _, tb := range target.session.tabs {
		if tb == target.tab {
			foundTab = true
			break
		}
	}
	if !foundTab {
		return false
	}
	target.tab.mu.Lock()
	defer target.tab.mu.Unlock()
	return target.tab.panes[target.pane.id] == target.pane &&
		target.tab.tree != nil && layout.ContainsLeaf(target.tab.tree.Root, target.pane.id)
}

func (a daemonActions) switchRelative(sess *session, delta int) error {
	if sess.switchRelative(delta) {
		a.d.activateTab(sess, sess.activeTab())
	}
	return nil
}

// controlExec implements command.ControlContext against resolved daemon-owned
// targets. It deliberately contains no attached-client state.
type controlExec struct {
	d       *Daemon
	sess    *session
	tab     *tab
	target  daemonActionTarget
	actions daemonActionRunner
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
	if err == nil && e.actions == nil {
		e.sess.mu.Lock()
		ac := e.sess.client
		e.sess.mu.Unlock()
		finishDaemonActionForClient(e.d, request, ac, "control.go")
	}
	return err
}

func finishDaemonActionForClient(d *Daemon, request daemonActionRequest, ac *attachedClient, producer string) {
	if ac == nil {
		return
	}
	if request.kind == daemonActionFocusPane && request.target.tab != nil && request.target.pane != nil {
		tb := request.target.tab
		tb.mu.Lock()
		newFocus := tb.tree.Focus
		pl, hasPlacement := focusedPlacementLocked(tb)
		tb.mu.Unlock()
		if newFocus != request.target.pane.id {
			d.exitCopyMode(ac)
			if hasPlacement && pl.InStack {
				d.refreshPaneTitleOnFocus(request.target.session, newFocus)
			}
		}
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
	cwd, term, env := e.sess.cwd, e.sess.terminal, copyEnvironment(e.sess.env)
	e.sess.mu.Unlock()
	size := e.sess.fullViewportSize()
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	if e.d.closing {
		return errors.New("daemon is shutting down")
	}
	if e.d.nameLiveOrStoppedLocked(name) {
		return errSessionNameInUse
	}
	_, err := e.d.createSessionLocked(name, false, cwd, size, term, env)
	return err
}
func (e controlExec) CloseTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionCloseTab})
}
func (e controlExec) ClosePane() error {
	return e.runAction(daemonActionRequest{kind: daemonActionClosePane})
}
func (e controlExec) split(direction layout.Direction) error {
	return e.runAction(daemonActionRequest{kind: daemonActionSplitPane, direction: direction})
}
func (e controlExec) SplitRight() error { return e.split(layout.Right) }
func (e controlExec) SplitLeft() error  { return e.split(layout.Left) }
func (e controlExec) SplitUp() error    { return e.split(layout.Up) }
func (e controlExec) SplitDown() error  { return e.split(layout.Down) }
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

func (e controlExec) ListSessions(asJSON bool) (string, error) {
	type row struct {
		Name      string `json:"name"`
		Ephemeral bool   `json:"ephemeral"`
		Tabs      int    `json:"tabs"`
		Attached  bool   `json:"attached"`
		Active    bool   `json:"active"`
	}
	e.d.mu.Lock()
	sessions := make([]*session, 0, len(e.d.sessions))
	for _, sess := range e.d.sessions {
		sessions = append(sessions, sess)
	}
	e.d.mu.Unlock()
	var active *session
	for _, sess := range sessions {
		if active == nil || sess.mruAt.Load() > active.mruAt.Load() {
			active = sess
		}
	}
	rows := make([]row, 0, len(sessions))
	for _, sess := range sessions {
		sess.mu.Lock()
		rows = append(rows, row{Name: sess.name, Ephemeral: sess.ephemeral, Tabs: len(sess.tabs), Attached: sess.client != nil, Active: sess == active})
		sess.mu.Unlock()
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
	active := e.sess.active
	e.sess.mu.Unlock()
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
		tb = e.sess.activeTab()
	}
	if tb == nil {
		return "", errors.New("target session has no active tab")
	}
	e.sess.mu.Lock()
	fallbackCWD := e.sess.cwd
	e.sess.mu.Unlock()
	tb.mu.Lock()
	focus := tb.tree.Focus
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
		rows = append(rows, row{ID: pane.stableID, Pane: string(pane.id), Size: fmt.Sprintf("%dx%d", size.Cols, size.Rows), CWD: cwd, Focused: pane.id == focus})
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
