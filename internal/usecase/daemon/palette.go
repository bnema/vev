package daemon

import (
	"errors"
	"sort"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/picker"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
)

const (
	paletteRailBreakpoint = 96
	paletteRailWidth      = 64
)

var (
	paletteModal                    = ui.Modal{WidthPct: 100, MinWidth: 32, FixedHeight: 11, Title: " Commands ", Anchor: domain.AnchorBottom, Margins: ui.Margins{Top: 1, Right: 1, Bottom: 1, Left: 1}}
	errCreateDestinationUnavailable = errors.New("that destination is no longer available")
)

func paletteModalFor(size domain.Size, cfg domain.PaletteConfig) ui.Modal {
	modal := paletteModal
	if size.Cols >= paletteRailBreakpoint {
		modal.FixedWidth = paletteRailWidth
	}
	if cfg.AnchorSet {
		modal.Anchor = cfg.Anchor
	} else if size.Cols >= paletteRailBreakpoint {
		modal.Anchor = domain.AnchorBottomRight
	}
	return modal
}

func (d *Daemon) enterPalette(sess *session, ac *attachedClient) {
	// Capture the client-owned route snapshot before taking paletteMu. The
	// snapshot is immutable for this palette interaction and the daemon never
	// consults its own session history for recent-route commands.
	routeSnapshot := ac.routeSnapshotCopy()
	commands := d.paletteCommands()
	results := d.paletteResults(sess, commands, routeSnapshot)
	model := palette.New(results)
	ac.overlays.paletteMu.Lock()
	ac.overlays.paletteGeneration++
	ac.overlays.palette = model
	ac.overlays.paletteRouteSnapshot = routeSnapshot
	ac.overlays.paletteHints = palette.ContextualHints{}
	ac.overlays.palettePreview = ""
	ac.overlays.paletteFeedback = ""
	ac.overlays.palettePending = nil
	generation := ac.overlays.paletteGeneration
	ac.overlays.paletteMu.Unlock()
	d.invalidateRender(sess, ac, true, "palette.go")
	d.remoteDiscoveryOpened(remoteDiscoveryInstance{
		ac: ac, kind: remoteDiscoveryPalette, generation: generation, palette: model,
	})
}

type paletteSessionIdentity struct {
	kind      protocol.RouteKind
	endpoint  string
	lifecycle domain.SessionLifecycleID
}

type remotePalettePresentationIdentity struct {
	origin    string
	name      string
	lifecycle domain.SessionLifecycleID
}

func localPaletteSessionIdentity(lifecycle domain.SessionLifecycleID) paletteSessionIdentity {
	return paletteSessionIdentity{kind: protocol.RouteKindLocal, endpoint: "local", lifecycle: lifecycle}
}

func remotePaletteSessionIdentity(endpoint string, lifecycle domain.SessionLifecycleID) paletteSessionIdentity {
	return paletteSessionIdentity{kind: protocol.RouteKindRemote, endpoint: endpoint, lifecycle: lifecycle}
}

func paletteRemoteDisplayOrigin(hostLabel string) string {
	origin := domain.RemoteDisplayOrigin(hostLabel)
	if origin == "" {
		return "remote"
	}
	return origin
}

func paletteDaemonDisplayOrigin(snapshot protocol.RecentRouteSnapshot, lifecycle domain.SessionLifecycleID) string {
	active, ok := activeRouteEntryForLifecycle(snapshot, lifecycle)
	if !ok || active.Kind != protocol.RouteKindRemote {
		return ""
	}
	return paletteRemoteDisplayOrigin(active.HostLabel)
}

func routeEntryForRef(snapshot protocol.RecentRouteSnapshot, ref protocol.RouteRef) (protocol.RecentRouteEntry, bool) {
	if ref.IsZero() {
		return protocol.RecentRouteEntry{}, false
	}
	if snapshot.Active == ref {
		entryRef := protocol.RouteRef{Key: snapshot.ActiveEntry.Key, Generation: snapshot.ActiveEntry.Generation}
		if entryRef == ref {
			return snapshot.ActiveEntry, true
		}
	}
	for _, entry := range snapshot.Entries {
		if entry.Key == ref.Key && entry.Generation == ref.Generation {
			return entry, true
		}
	}
	return protocol.RecentRouteEntry{}, false
}

func createSessionDestinationResults(snapshot protocol.RecentRouteSnapshot, currentLifecycle domain.SessionLifecycleID, remoteCatalog []remoteCatalogPresentationEntry, hostRanks map[string]int) []palette.Result {
	results := make([]palette.Result, 0, len(remoteCatalog)+2)
	servingOrigin := ""
	if home, ok := routeEntryForRef(snapshot, snapshot.Home); ok && home.Kind == protocol.RouteKindLocal {
		results = append(results, palette.NewCreateSessionDestination(
			palette.CreateSessionOnLocalRoute, "", "", snapshot.Home, snapshot.Generation,
		))
	}
	if active, ok := activeRouteEntryForLifecycle(snapshot, currentLifecycle); ok && active.Kind == protocol.RouteKindRemote {
		servingOrigin = paletteRemoteDisplayOrigin(active.HostLabel)
		results = append(results, palette.NewCreateSessionDestination(
			palette.CreateSessionOnServingDaemon, servingOrigin, "", protocol.RouteRef{}, 0,
		))
	}
	for _, host := range remoteCatalog {
		if host.status != remoteHostFresh || host.entry.Host == "" {
			continue
		}
		if _, configured := hostRanks[host.entry.Host]; !configured {
			continue
		}
		displayOrigin := paletteRemoteDisplayOrigin(host.entry.Host)
		if servingOrigin != "" && displayOrigin == servingOrigin {
			continue
		}
		results = append(results, palette.NewCreateSessionDestination(
			palette.CreateSessionOnRemoteHost, displayOrigin, host.entry.Host, protocol.RouteRef{}, 0,
		))
	}
	return results
}

func paletteRouteRepresentsDaemonSession(entry protocol.RecentRouteEntry, daemonDisplayOrigin string) bool {
	if entry.Kind == protocol.RouteKindLocal {
		return true
	}
	return entry.Kind == protocol.RouteKindRemote && daemonDisplayOrigin != "" &&
		paletteRemoteDisplayOrigin(entry.HostLabel) == daemonDisplayOrigin
}

func remotePaletteUnavailableReason(status remoteHostStatus, session catalogue.RemoteCatalogSession) string {
	if session.State == catalogue.RemoteCatalogSessionBroken {
		return "session_broken"
	}
	return remoteReasonForStatus(status)
}

// paletteResults captures eligible named sessions before paletteMu so the
// palette has an immutable lifecycle target without violating lock ordering.
func (d *Daemon) paletteResults(current *session, commands []command.Command, routeSnapshot protocol.RecentRouteSnapshot) []palette.Result {
	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	stopped := make([]inactiveSession, 0, len(d.inactive))
	represented := make(map[paletteSessionIdentity]struct{}, len(sessions)+len(d.inactive)+len(routeSnapshot.Entries))
	for _, entry := range d.inactive {
		represented[localPaletteSessionIdentity(entry.incarnation)] = struct{}{}
		if entry.canResume() {
			stopped = append(stopped, entry)
		}
	}
	d.mu.Unlock()
	var currentLifecycle domain.SessionLifecycleID
	if current != nil {
		current.mu.Lock()
		currentLifecycle = current.incarnation
		current.mu.Unlock()
	}
	daemonDisplayOrigin := paletteDaemonDisplayOrigin(routeSnapshot, currentLifecycle)

	remoteCatalog := d.remoteCatalogSnapshot()
	hostRanks := d.remoteHostRanks()
	sortRemoteCatalog(remoteCatalog, hostRanks)
	remoteSessionCount := 0
	for _, host := range remoteCatalog {
		remoteSessionCount += len(host.entry.Sessions)
	}

	destinations := createSessionDestinationResults(routeSnapshot, currentLifecycle, remoteCatalog, hostRanks)
	results := make([]palette.Result, 0, len(commands)+len(destinations)+len(sessions)+len(stopped)+remoteSessionCount+len(routeSnapshot.Entries))
	for _, cmd := range commands {
		results = append(results, palette.NewCommandResult(cmd))
	}
	results = append(results, destinations...)
	active := make([]palette.Result, 0, len(sessions))
	for _, candidate := range sessions {
		snap := candidate.snapshotView(viewOptions{})
		if snap.name == "" || snap.ephemeral {
			continue
		}
		represented[localPaletteSessionIdentity(snap.incarnation)] = struct{}{}
		if candidate == current {
			continue
		}
		target := protocol.ExactSessionTarget{LifecycleID: snap.incarnation, SessionName: snap.name}
		active = append(active, palette.NewActiveSessionResultWithDisplayOrigin(target, time.Unix(0, snap.createdAt), daemonDisplayOrigin))
	}
	sort.Slice(active, func(i, j int) bool {
		left, _ := active[i].SessionName()
		right, _ := active[j].SessionName()
		return left < right
	})
	sort.Slice(stopped, func(i, j int) bool { return stopped[i].name < stopped[j].name })
	results = append(results, active...)
	for _, candidate := range stopped {
		target := protocol.ExactSessionTarget{LifecycleID: candidate.incarnation, SessionName: candidate.name}
		results = append(results, palette.NewStoppedSessionResultWithDisplayOrigin(target, time.Unix(0, candidate.createdAt), daemonDisplayOrigin))
		represented[localPaletteSessionIdentity(target.LifecycleID)] = struct{}{}
	}
	type discoveredRemote struct {
		identity paletteSessionIdentity
		result   palette.Result
	}
	discovered := make([]discoveredRemote, 0, remoteSessionCount)
	discoveredByPresentation := make(map[remotePalettePresentationIdentity][]paletteSessionIdentity, remoteSessionCount)
	for _, host := range remoteCatalog {
		for _, session := range host.entry.Sessions {
			if session.Ephemeral {
				continue
			}
			key, target := remoteCatalogSessionTarget(domain.RemoteSessionKey{
				Host: host.entry.Host, Name: session.Name,
			}, session)
			if key.Validate() != nil || target.Validate() != nil {
				continue
			}
			identity := remotePaletteSessionIdentity(key.Host, session.LifecycleID)
			presentation := remotePalettePresentationIdentity{
				origin: key.DisplayOrigin, name: session.Name, lifecycle: session.LifecycleID,
			}
			discovered = append(discovered, discoveredRemote{
				identity: identity,
				result:   palette.NewRemoteSessionResult(key, target, remotePaletteUnavailableReason(host.status, session)),
			})
			discoveredByPresentation[presentation] = append(discoveredByPresentation[presentation], identity)
		}
	}
	formattedRoutes := formatRecentRouteSnapshot(routeSnapshot)
	for i, entry := range routeSnapshot.Entries {
		if i >= len(formattedRoutes) {
			break
		}
		label := formattedRoutes[i].name
		if label == "" {
			continue
		}
		identity := localPaletteSessionIdentity(entry.Target.LifecycleID)
		if _, exists := represented[identity]; exists && paletteRouteRepresentsDaemonSession(entry, daemonDisplayOrigin) {
			continue
		}
		results = append(results, palette.NewRecentRouteResult(label, protocol.RouteNavigationAction{
			SnapshotGeneration: routeSnapshot.Generation,
			Key:                entry.Key,
			Generation:         entry.Generation,
		}))
		if entry.Kind == protocol.RouteKindLocal {
			represented[identity] = struct{}{}
			continue
		}
		presentation := remotePalettePresentationIdentity{
			origin:    domain.RemoteDisplayOrigin(entry.HostLabel),
			name:      entry.Target.SessionName,
			lifecycle: entry.Target.LifecycleID,
		}
		if matches := discoveredByPresentation[presentation]; len(matches) == 1 {
			represented[matches[0]] = struct{}{}
		}
	}
	for _, remote := range discovered {
		if _, exists := represented[remote.identity]; exists {
			continue
		}
		results = append(results, remote.result)
		represented[remote.identity] = struct{}{}
	}
	return results
}

func (d *Daemon) refreshPalette(ac *attachedClient) {
	if d == nil || ac == nil || ac.overlays == nil {
		return
	}
	sess := ac.currentAttachmentSession()
	if sess == nil {
		return
	}
	rt := ac.overlays
	rt.paletteMu.Lock()
	model := rt.palette
	generation := rt.paletteGeneration
	routeSnapshot := rt.paletteRouteSnapshot
	routeSnapshot.Entries = append([]protocol.RecentRouteEntry(nil), routeSnapshot.Entries...)
	rt.paletteMu.Unlock()
	if model == nil {
		return
	}

	results := d.paletteResults(sess, d.paletteCommands(), routeSnapshot)
	rt.paletteMu.Lock()
	defer rt.paletteMu.Unlock()
	if rt.paletteGeneration != generation || rt.palette != model {
		return
	}
	model.ReplaceResults(results)
}

func recentRouteHints(snapshot protocol.RecentRouteSnapshot, args []string) palette.ContextualHints {
	formatted := formatRecentRouteSnapshot(snapshot)
	names := make([]string, len(formatted))
	for i, entry := range formatted {
		names[i] = entry.name
	}
	hints := palette.BuildRecentSessionHints(names, args)
	for i, entry := range snapshot.Entries {
		if i >= len(hints.Recent) {
			break
		}
		hints.Recent[i].SnapshotGeneration = snapshot.Generation
		hints.Recent[i].Key = entry.Key
		hints.Recent[i].Generation = entry.Generation
	}
	return hints
}

func (d *Daemon) paletteCommands() []command.Command {
	commands := command.PaletteRegistry()
	d.paletteRecentMu.Lock()
	recent := append([]string(nil), d.paletteRecent...)
	d.paletteRecentMu.Unlock()
	overrides := d.codeOverrideSnapshot()

	byCode := make(map[string]command.Command, len(commands))
	for _, cmd := range commands {
		cmd = commandWithOverrides(cmd, overrides)
		byCode[cmd.Code] = cmd
	}
	out := make([]command.Command, 0, len(commands))
	used := make(map[string]bool, len(recent))
	for _, code := range recent {
		cmd, ok := byCode[code]
		if !ok || used[code] {
			continue
		}
		out = append(out, cmd)
		used[code] = true
	}
	for _, cmd := range commands {
		cmd = commandWithOverrides(cmd, overrides)
		if !used[cmd.Code] {
			out = append(out, cmd)
		}
	}
	return out
}

func (d *Daemon) recordPaletteUse(code string) {
	d.paletteRecentMu.Lock()
	defer d.paletteRecentMu.Unlock()

	recent := make([]string, 0, len(d.paletteRecent)+1)
	recent = append(recent, code)
	for _, existing := range d.paletteRecent {
		if existing != code {
			recent = append(recent, existing)
		}
	}
	d.paletteRecent = recent
}

func (d *Daemon) handlePaletteInput(ac *attachedClient, data []byte, effects ...*attachmentEffect) {
	entry := ac.currentAttachmentSession()
	if entry == nil {
		return
	}
	sess := entry
	var cmd command.Command
	var sessionTarget palette.Result
	var hasSessionTarget bool
	var remoteTarget domain.RemoteSessionTarget
	var remoteKey domain.RemoteSessionKey
	var remoteUnavailableReason string
	var hasRemoteTarget bool
	var routeTarget protocol.RouteNavigationAction
	var hasRouteTarget bool
	var createDestination palette.Result
	var hasCreateDestination bool
	var args []string
	var routeSnapshot protocol.RecentRouteSnapshot
	var generation uint64
	var rawQuery string
	changed, cancel, execute, chooseDestination := false, false, false, false
	var closedDiscovery remoteDiscoveryInstance
	var effect *attachmentEffect
	if len(effects) != 0 {
		effect = effects[0]
	}

	ac.overlays.paletteMu.Lock()
	if ac.overlays.palette == nil {
		ac.overlays.palettePending = nil
		ac.overlays.paletteMu.Unlock()
		return
	}
	routeOverlayBytes(data, &ac.overlays.palettePending, overlayEvents{
		rune: func(r rune) {
			if !execute {
				ac.overlays.palette.Insert(r)
				changed = true
			}
		},
		backspace: func() {
			if !execute {
				ac.overlays.palette.Backspace()
				changed = true
			}
		},
		tab: func() {
			if !execute {
				changed = ac.overlays.palette.CompleteSelected() || changed
			}
		},
		up: func() {
			if !execute {
				ac.overlays.palette.Up()
				changed = true
			}
		},
		down: func() {
			if !execute {
				ac.overlays.palette.Down()
				changed = true
			}
		},
		cancel: func() {
			if !execute {
				cancel = true
			}
		},
		enter: func() {
			if execute {
				return
			}
			selected, ok := ac.overlays.palette.Selected()
			if !ok {
				changed = true
				chooseDestination = true
				return
			}
			rawQuery = ac.overlays.palette.Query()
			if selectedCommand, ok := selected.Command(); ok {
				cmd = selectedCommand
				if cmd.Arguments != command.ArgumentsNone {
					action, valid := palette.ParseAction([]palette.Result{selected}, rawQuery)
					if valid {
						args = action.Args
					} else if cmd.Arguments == command.ArgumentsRequired {
						changed = true
						return
					}
				}
				if cmd.ContextHint == command.ContextHintRecentSessions {
					routeSnapshot = ac.overlays.paletteRouteSnapshot
					routeSnapshot.Entries = append([]protocol.RecentRouteEntry(nil), routeSnapshot.Entries...)
				}
			} else if target, ok := selected.RemoteSessionTarget(); ok {
				key, keyOK := selected.RemoteSessionKey()
				if !keyOK {
					changed = true
					return
				}
				remoteTarget = target
				remoteKey = key
				remoteUnavailableReason, _ = selected.RemoteSessionUnavailableReason()
				hasRemoteTarget = true
			} else if action, ok := selected.RouteNavigationAction(); ok {
				routeTarget = action
				hasRouteTarget = true
			} else if _, _, _, _, _, ok := selected.CreateSessionDestination(); ok {
				createDestination = selected
				hasCreateDestination = true
			} else if _, ok := selected.SessionTarget(); ok {
				sessionTarget = selected
				hasSessionTarget = true
			} else {
				changed = true
				return
			}
			generation = ac.overlays.paletteGeneration
			execute = true
		},
	})
	if changed {
		ac.overlays.paletteFeedback = ""
		if chooseDestination {
			ac.overlays.paletteFeedback = "Choose a destination"
		}
		ac.overlays.palettePreview = ""
		active, ok := ac.overlays.palette.ArgumentCommand()
		if ok {
			ac.overlays.palettePreview = palette.Preview(active, ac.overlays.palette.Query())
		}
		if ok && active.Slug == "jump-recent-session" {
			args := paletteArgs(ac.overlays.palette.Query(), active)
			if ac.overlays.paletteRouteSnapshot.Generation != 0 {
				ac.overlays.paletteHints = recentRouteHints(ac.overlays.paletteRouteSnapshot, args)
			} else {
				ac.overlays.paletteHints = palette.ContextualHints{}
			}
		} else {
			ac.overlays.paletteHints = palette.ContextualHints{}
		}
	}
	if cancel {
		closedDiscovery = ac.clearPaletteLocked()
	}
	ac.overlays.paletteMu.Unlock()
	if cancel {
		d.remoteDiscoveryClosed(closedDiscovery)
		d.invalidateRender(entry, ac, true, "palette.go")
		return
	}
	if !execute {
		if changed {
			d.invalidateRender(entry, ac, true, "palette.go")
		}
		return
	}

	if hasCreateDestination {
		name, _, _, _, _, _ := createDestination.CreateSessionDestination()
		exec := paletteExec{d: d, sess: sess, attachment: entry, ac: ac, effect: effect}
		if err := exec.validateCreateSessionDestination(effect, createDestination); err != nil {
			ac.paletteFailure(generation, rawQuery, errCreateDestinationUnavailable.Error())
			d.invalidateRender(entry, ac, true, "palette.go")
			return
		}
		if !d.closeExecutedPalette(ac, generation, rawQuery) {
			return
		}
		if name == "" {
			d.enterTransitionPrompt(entry, ac, " Create session ", "", func(name string, promptEffect *attachmentEffect) error {
				return exec.createSessionOnDestination(promptEffect, createDestination, name)
			})
			d.invalidateRender(entry, ac, true, "palette.go")
			return
		}
		err := exec.createSessionOnValidatedDestination(effect, createDestination, name)
		if err != nil && !errors.Is(err, errAttachmentTransition) {
			d.reportAttachmentError(entry, domain.UserErr(domain.NoticeSessionUnavailable, "couldn't create session", err))
			return
		}
		if current := ac.currentAttachmentSession(); current != nil {
			d.invalidateRender(current, ac, true, "palette.go")
		}
		return
	}

	if hasRemoteTarget {
		if effect == nil {
			ac.paletteFailure(generation, rawQuery, "requested remote session is unavailable")
			d.invalidateRender(entry, ac, true, "palette.go")
			return
		}
		target := picker.Target{
			Session: remoteKey.ID(), Incarnation: remoteKey.LifecycleID, Name: remoteKey.Display(),
			RemoteKey: &remoteKey, RemoteTarget: &remoteTarget, RemoteHost: remoteKey.Host,
			UnavailableReason: remoteUnavailableReason, TabIndex: -1,
		}
		err := d.switchToTargetForAttachment(effect, target, sessionHandoffGuard{}, "palette-remote-session")
		if err == nil {
			d.closeExecutedPalette(ac, generation, rawQuery)
			return
		}
		if !errors.Is(err, errAttachmentTransition) {
			ac.paletteFailure(generation, rawQuery, "requested remote session is unavailable")
			d.invalidateRender(entry, ac, true, "palette.go")
		}
		return
	}

	if hasRouteTarget {
		exec := paletteExec{d: d, sess: sess, attachment: entry, ac: ac, effect: effect}
		if err := exec.NavigateRecentRoute(routeTarget); err != nil {
			if errors.Is(err, errAttachmentTransition) {
				return
			}
			ac.paletteFailure(generation, rawQuery, "requested recent session is unavailable")
			d.invalidateRender(entry, ac, true, "palette.go")
			return
		}
		if d.closeExecutedPalette(ac, generation, rawQuery) {
			d.invalidateRender(entry, ac, true, "palette.go")
		}
		return
	}

	if hasSessionTarget {
		createdAt, _ := sessionTarget.SessionCreatedAt()
		expectedCreatedAt := createdAt.UnixNano()
		name, _ := sessionTarget.SessionName()
		exactTarget, _ := sessionTarget.SessionTarget()
		target := picker.Target{
			Incarnation: exactTarget.LifecycleID, Name: name, TabIndex: -1,
			ExpectedCreatedAt: &expectedCreatedAt, Stopped: sessionTarget.Kind() == palette.ResultKindStoppedSession,
		}
		var err error
		if effect != nil {
			err = d.switchToTargetForAttachment(effect, target, sessionHandoffGuard{allowSamePeer: true}, "palette-session")
		} else {
			err = d.switchToTarget(sess, ac, target)
		}
		if errors.Is(err, errAttachmentTransition) {
			return
		}
		if err != nil {
			ac.paletteFailure(generation, rawQuery, "requested session is unavailable")
			d.invalidateRender(entry, ac, true, "palette.go")
			return
		}
		if d.closeExecutedPalette(ac, generation, rawQuery) {
			if current := ac.currentAttachmentSession(); current != nil {
				d.invalidateRender(current, ac, true, "palette.go")
			}
		}
		return
	}

	if cmd.Slug == "jump-recent-session" {
		rank, err := command.ParsePositiveDecimal(args)
		if err != nil {
			ac.paletteFailure(generation, rawQuery, "rank must be one positive decimal")
			d.invalidateRender(entry, ac, true, "palette.go")
			return
		}
		exec := paletteExec{d: d, sess: sess, attachment: entry, ac: ac, routeSnapshot: routeSnapshot, effect: effect}
		if err := exec.JumpRecentSession(rank); err != nil {
			if errors.Is(err, errAttachmentTransition) {
				return
			}
			ac.paletteFailure(generation, rawQuery, "requested recent session is unavailable")
			d.invalidateRender(entry, ac, true, "palette.go")
			return
		}
		if d.closeExecutedPalette(ac, generation, rawQuery) {
			d.recordPaletteUse(cmd.Code)
			if current := ac.currentAttachmentSession(); current != nil {
				d.invalidateRender(current, ac, true, "palette.go")
			}
		}
		return
	}
	attachmentHandoff := cmd.Slug == "back-session" || cmd.Slug == "detach"
	if !attachmentHandoff && !d.closeExecutedPalette(ac, generation, rawQuery) {
		return
	}
	sess.dispatchMu.Lock()
	err := cmd.Run(paletteExec{d: d, sess: sess, ac: ac, effect: effect, redrawClosedPalette: true}, args)
	sess.dispatchMu.Unlock()
	if attachmentHandoff {
		if current := ac.currentSession(); current != nil {
			currentToken := current.captureAttachmentCapability(ac, ac.transport())
			fresh, admitted := ac.beginAttachmentEffect(currentToken)
			if d.closeExecutedPalette(ac, generation, rawQuery) {
				d.invalidateRender(current, ac, true, "palette.go")
			}
			if admitted {
				fresh.End()
			}
		} else {
			d.closeExecutedPalette(ac, generation, rawQuery)
		}
	}
	if errors.Is(err, errAttachmentTransition) {
		return
	}
	if err != nil {
		d.log.Error("palette command failed", "err", err, "command", cmd.Code)
		d.reportError(sess, paletteCommandNoticeError(cmd, err))
	} else {
		d.recordPaletteUse(cmd.Code)
	}
}

func paletteCommandNoticeError(cmd command.Command, err error) error {
	if cmd.Scope == command.CommandScopeCrossSession {
		return movePickerUserError(err)
	}
	return err
}

func paletteArgs(query string, cmd command.Command) []string {
	action, ok := palette.ParseAction(palette.CommandResults([]command.Command{cmd}), query)
	if !ok {
		return nil
	}
	return action.Args
}

func (ac *attachedClient) clearPaletteLocked() remoteDiscoveryInstance {
	model := ac.overlays.palette
	ac.overlays.paletteGeneration++
	ac.overlays.palette = nil
	ac.overlays.paletteRouteSnapshot = protocol.RecentRouteSnapshot{}
	ac.overlays.paletteHints = palette.ContextualHints{}
	ac.overlays.palettePreview = ""
	ac.overlays.paletteFeedback = ""
	ac.overlays.palettePending = nil
	return remoteDiscoveryInstance{
		ac: ac, kind: remoteDiscoveryPalette, generation: ac.overlays.paletteGeneration, palette: model,
	}
}

func (ac *attachedClient) paletteFailure(generation uint64, rawQuery, feedback string) {
	ac.overlays.paletteMu.Lock()
	defer ac.overlays.paletteMu.Unlock()
	if ac.overlays.palette == nil || ac.overlays.paletteGeneration != generation || ac.overlays.palette.Query() != rawQuery {
		return
	}
	ac.overlays.paletteFeedback = feedback
}

func (d *Daemon) closeExecutedPalette(ac *attachedClient, generation uint64, rawQuery string) bool {
	ac.overlays.paletteMu.Lock()
	if ac.overlays.palette == nil || ac.overlays.paletteGeneration != generation || ac.overlays.palette.Query() != rawQuery {
		ac.overlays.paletteMu.Unlock()
		return false
	}
	instance := ac.clearPaletteLocked()
	ac.overlays.paletteMu.Unlock()
	d.remoteDiscoveryClosed(instance)
	return true
}

func (d *Daemon) closePalette(ac *attachedClient) {
	d.closePaletteIfCurrent(ac, 0)
}

func (d *Daemon) closePaletteIfCurrent(ac *attachedClient, generation uint64) bool {
	if d == nil || ac == nil || ac.overlays == nil {
		return false
	}
	ac.overlays.paletteMu.Lock()
	if ac.overlays.palette == nil || generation != 0 && ac.overlays.paletteGeneration != generation {
		ac.overlays.paletteMu.Unlock()
		return false
	}
	instance := ac.clearPaletteLocked()
	ac.overlays.paletteMu.Unlock()
	d.remoteDiscoveryClosed(instance)
	return true
}

type paletteExec struct {
	d                   *Daemon
	sess                *session
	attachment          *session
	ac                  *attachedClient
	routeSnapshot       protocol.RecentRouteSnapshot
	actions             daemonActionRunner
	effect              *attachmentEffect
	redrawClosedPalette bool
}

func (e paletteExec) runAction(request daemonActionRequest) error {
	request.effect = e.effect
	if request.target.session == nil {
		request.target = resolveDaemonActionTargetForAttachment(e.sess, e.ac)
	}
	runner := e.actions
	if runner == nil {
		runner = daemonActions{d: e.d}
	}
	err := runner.Run(request)
	if errors.Is(err, errDaemonActionNoChange) {
		if e.redrawClosedPalette {
			e.d.invalidateRender(e.sess, e.ac, true, "palette.go")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if e.actions == nil {
		finishDaemonActionForClient(e.d, request, e.ac, "palette.go")
	}
	return nil
}

func (e paletteExec) CreateTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionCreateTab, viewport: e.sess.fullViewportSize()})
}

func (e paletteExec) CreateSession() error {
	entry := e.attachment
	if entry == nil {
		entry = e.sess
	}
	e.d.enterTransitionPrompt(entry, e.ac, " Create session ", "", func(name string, effect *attachmentEffect) error {
		if effect == nil || effect.ac == nil {
			if e.sess == nil {
				return errAttachmentTransition
			}
			return e.d.createSessionAndSwitch(e.sess, e.ac, name)
		}
		return e.d.createSessionAndSwitchForAttachment(effect, name)
	})
	return nil
}

func (e paletteExec) CreateSessionNamed(name string) error {
	if e.effect == nil {
		return e.d.createSessionAndSwitch(e.sess, e.ac, name)
	}
	return e.d.createSessionAndSwitchForAttachment(e.effect, name)
}

func (e paletteExec) validateCreateSessionDestination(effect *attachmentEffect, result palette.Result) error {
	_, kind, _, endpoint, route, ok := result.CreateSessionDestination()
	if !ok {
		return errCreateDestinationUnavailable
	}
	switch kind {
	case palette.CreateSessionOnServingDaemon:
		return nil
	case palette.CreateSessionOnLocalRoute:
		if e.ac == nil {
			return errCreateDestinationUnavailable
		}
		snapshotGeneration, ok := result.CreateSessionSnapshotGeneration()
		if !ok || snapshotGeneration == 0 {
			return errCreateDestinationUnavailable
		}
		snapshot := e.ac.routeSnapshotCopy()
		entry, valid := routeEntryForRef(snapshot, route)
		if !valid || snapshot.Generation != snapshotGeneration || entry.Kind != protocol.RouteKindLocal {
			return errCreateDestinationUnavailable
		}
		if effect == nil && snapshot.Active != route {
			return errCreateDestinationUnavailable
		}
		return nil
	case palette.CreateSessionOnRemoteHost:
		if effect == nil || !e.d.remoteCreateHostReady(endpoint) {
			return errCreateDestinationUnavailable
		}
		return nil
	default:
		return errCreateDestinationUnavailable
	}
}

func (e paletteExec) createSessionOnDestination(effect *attachmentEffect, result palette.Result, name string) error {
	if err := e.validateCreateSessionDestination(effect, result); err != nil {
		return err
	}
	return e.createSessionOnValidatedDestination(effect, result, name)
}

func (e paletteExec) createSessionOnValidatedDestination(effect *attachmentEffect, result palette.Result, name string) error {
	if err := domain.ValidateSessionName(name); err != nil {
		return err
	}
	_, kind, _, endpoint, route, _ := result.CreateSessionDestination()
	if kind == palette.CreateSessionOnServingDaemon ||
		(kind == palette.CreateSessionOnLocalRoute && e.ac != nil && e.ac.routeSnapshotCopy().Active == route) {
		if effect == nil {
			return e.d.createSessionAndSwitch(e.sess, e.ac, name)
		}
		return e.d.createSessionAndSwitchForAttachment(effect, name)
	}
	if effect == nil {
		return errAttachmentTransition
	}
	requestID := e.d.creationRequestSeq.Add(1)
	if requestID == 0 {
		requestID = e.d.creationRequestSeq.Add(1)
	}
	switch kind {
	case palette.CreateSessionOnLocalRoute:
		snapshotGeneration, _ := result.CreateSessionSnapshotGeneration()
		action := protocol.RouteCreateSessionAction{
			RequestID: requestID, SnapshotGeneration: snapshotGeneration,
			Key: route.Key, Generation: route.Generation, SessionName: name,
		}
		if err := effect.sendControl(action); err != nil {
			return err
		}
		return nil
	case palette.CreateSessionOnRemoteHost:
		handoff := protocol.AttachTarget{
			RequestID: requestID, Endpoint: endpoint, Session: name, Intent: protocol.IntentNew,
			EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned,
		}
		if err := protocol.ValidateAttachTarget(handoff); err != nil {
			return errAttachmentTransition
		}
		if err := effect.sendControl(handoff); err != nil {
			return err
		}
		e.d.clientGoneForAttachment(effect, false)
		return nil
	default:
		return errAttachmentTransition
	}
}

func (d *Daemon) remoteCreateHostReady(endpoint string) bool {
	if d == nil || domain.ValidateRemoteHostTarget(endpoint) != nil {
		return false
	}
	ranks := d.remoteHostRanks()
	if _, ok := ranks[endpoint]; !ok {
		return false
	}
	d.remoteCatalog.mu.Lock()
	defer d.remoteCatalog.mu.Unlock()
	_, ok := d.remoteCatalogEntryLocked(endpoint)
	return ok
}

func (e paletteExec) CreateEphemeralSession() error {
	if e.effect == nil {
		return e.d.createEphemeralSessionAndSwitch(e.sess, e.ac)
	}
	return e.d.createEphemeralSessionAndSwitchForAttachment(e.effect)
}

func (e paletteExec) CloseTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionCloseTab})
}

func (e paletteExec) split(direction layout.Direction) error {
	return e.runAction(daemonActionRequest{kind: daemonActionSplitPane, direction: direction})
}

func (e paletteExec) SplitRight() error { return e.split(layout.Right) }
func (e paletteExec) SplitLeft() error  { return e.split(layout.Left) }
func (e paletteExec) SplitUp() error    { return e.split(layout.Up) }
func (e paletteExec) SplitDown() error  { return e.split(layout.Down) }
func (e paletteExec) consumeOrExpelPane(direction layout.Direction) error {
	return e.runAction(daemonActionRequest{kind: daemonActionConsumeOrExpelPane, direction: direction})
}
func (e paletteExec) ConsumeOrExpelPaneLeft() error {
	return e.consumeOrExpelPane(layout.Left)
}
func (e paletteExec) ConsumeOrExpelPaneRight() error {
	return e.consumeOrExpelPane(layout.Right)
}

func (e paletteExec) StackPane() error {
	return e.runAction(daemonActionRequest{kind: daemonActionStackPane})
}

func (e paletteExec) ToggleStack() error {
	return e.runAction(daemonActionRequest{kind: daemonActionToggleStack})
}

func (e paletteExec) ClosePane() error {
	return e.runAction(daemonActionRequest{kind: daemonActionClosePane})
}

func (e paletteExec) OpenMovePanePicker() error {
	target := resolveDaemonActionTargetForAttachment(e.sess, e.ac)
	if target.tab == nil || target.pane == nil {
		return errMovePaneInvalid
	}
	return e.d.enterPickerForIntent(e.sess, e.ac, pickerMovePane, moveSourceLocator{
		Session:    sessionMoveLocator(e.sess),
		TabID:      domain.TabStableID(target.tab.stableID),
		PaneID:     domain.PaneStableID(target.pane.stableID),
		Attachment: e.ac,
	})
}

func (e paletteExec) OpenMoveTabPicker() error {
	target := resolveDaemonActionTargetForAttachment(e.sess, e.ac)
	if target.tab == nil {
		return errMovePaneInvalid
	}
	return e.d.enterPickerForIntent(e.sess, e.ac, pickerMoveTab, moveSourceLocator{
		Session:    sessionMoveLocator(e.sess),
		TabID:      domain.TabStableID(target.tab.stableID),
		Attachment: e.ac,
	})
}

func (e paletteExec) focus(direction layout.Direction) error {
	return e.runAction(daemonActionRequest{kind: daemonActionFocusPane, direction: direction})
}

func (e paletteExec) FocusPaneLeft() error  { return e.focus(layout.Left) }
func (e paletteExec) FocusPaneRight() error { return e.focus(layout.Right) }
func (e paletteExec) FocusPaneUp() error    { return e.focus(layout.Up) }
func (e paletteExec) FocusPaneDown() error  { return e.focus(layout.Down) }

func (e paletteExec) EnterResizeMode() error { return e.d.enterResizeMode(e.sess, e.ac) }
func (e paletteExec) resize(axis layout.Axis, delta int) error {
	return resizeUserError(e.runAction(daemonActionRequest{kind: daemonActionResizePane, axis: axis, delta: delta}))
}
func (e paletteExec) GrowPaneWidth() error    { return e.resize(layout.Width, resizeStepCols) }
func (e paletteExec) ShrinkPaneWidth() error  { return e.resize(layout.Width, -resizeStepCols) }
func (e paletteExec) GrowPaneHeight() error   { return e.resize(layout.Height, resizeStepRows) }
func (e paletteExec) ShrinkPaneHeight() error { return e.resize(layout.Height, -resizeStepRows) }
func (e paletteExec) EqualizePanes() error {
	return resizeUserError(e.runAction(daemonActionRequest{kind: daemonActionEqualizePanes}))
}

func (e paletteExec) NextTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionNextTab})
}

func (e paletteExec) PrevTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionPreviousTab})
}

func (e paletteExec) ToggleFloatingPane() error {
	return e.d.toggleFloating(e.sess, e.ac)
}

func (e paletteExec) BackSession() error {
	if e.effect != nil {
		return e.d.backSessionForAttachment(e.effect)
	}
	e.d.backSession(e.sess, e.ac)
	return nil
}

func (e paletteExec) Detach() error {
	if e.effect != nil {
		if !e.d.clientGoneForAttachment(e.effect, true) {
			return errAttachmentTransition
		}
		return nil
	}
	e.d.clientGone(e.sess, e.ac, e.ac.transport(), true)
	return nil
}

func (e paletteExec) EnterVisualMode() error {
	e.d.enterCopyMode(e.sess, e.ac)
	return nil
}

func (e paletteExec) RenameSession() error {
	e.sess.mu.Lock()
	currentName := e.sess.name
	e.sess.mu.Unlock()
	e.d.enterPrompt(e.sess, e.ac, " Rename session ", currentName, func(name string) error {
		return e.runAction(daemonActionRequest{kind: daemonActionRenameSession, name: name})
	})
	return nil
}

func (e paletteExec) RenameSessionTo(name string) error {
	return e.runAction(daemonActionRequest{kind: daemonActionRenameSession, name: name})
}

func (e paletteExec) RenameTab() error {
	tb := e.sess.tabForAttachment(e.ac)
	if e.ac == nil {
		tb = e.sess.firstTab()
	}
	if tb == nil {
		return nil
	}
	e.sess.mu.Lock()
	index := 0
	for i, candidate := range e.sess.tabs {
		if candidate == tb {
			index = i
			break
		}
	}
	currentName := tabDisplayName(tb, index)
	e.sess.mu.Unlock()
	e.d.enterPrompt(e.sess, e.ac, " Rename tab ", currentName, func(name string) error {
		return e.runAction(daemonActionRequest{kind: daemonActionRenameTab, target: daemonActionTarget{session: e.sess, tab: tb}, name: name})
	})
	return nil
}

func (e paletteExec) RenameTabTo(name string) error {
	tb := e.sess.tabForAttachment(e.ac)
	if e.ac == nil {
		tb = e.sess.firstTab()
	}
	if tb == nil {
		return nil
	}
	return e.runAction(daemonActionRequest{kind: daemonActionRenameTab, target: daemonActionTarget{session: e.sess, tab: tb}, name: name})
}

func (e paletteExec) OpenSessionPicker() error {
	if e.ac != nil && e.ac.navigationCapabilities&protocol.NavigationCapabilityHomePicker != 0 && e.effect != nil {
		return e.d.sendNavigationActionForAttachment(e.effect, protocol.NavigationOpenHomePicker)
	}
	e.d.enterPicker(e.sess, e.ac)
	return nil
}

func (e paletteExec) OpenNotifications() error {
	e.d.enterNotices(e.sess, e.ac)
	return nil
}

func (e paletteExec) YankLastNotification() error {
	entry := e.attachment
	if entry == nil {
		entry = e.sess
	}
	n, ok := e.d.notices.latest()
	if !ok {
		// Reported directly rather than returned: the generic cmd.Run error
		// path only logs, so a returned error here would never reach the
		// user as the warn toast this no-op is supposed to produce.
		e.d.reportAttachmentError(entry, domain.UserWarn(domain.NoticeClipboard, "no notifications yet", nil))
		return nil
	}
	e.d.yankNotice(entry, e.ac, n)
	return nil
}

func (e paletteExec) JumpRecentSession(rank int) error {
	if e.routeSnapshot.Generation == 0 || rank < 1 || rank > len(e.routeSnapshot.Entries) {
		return command.ErrInvalidArguments
	}
	entry := e.routeSnapshot.Entries[rank-1]
	return e.NavigateRecentRoute(protocol.RouteNavigationAction{
		SnapshotGeneration: e.routeSnapshot.Generation,
		Key:                entry.Key,
		Generation:         entry.Generation,
	})
}

func (e paletteExec) NavigateRecentRoute(action protocol.RouteNavigationAction) error {
	if e.d.beforeRecentSessionHandoff != nil {
		e.d.beforeRecentSessionHandoff()
	}
	if e.effect != nil {
		return e.d.sendRecentRouteNavigationActionForAttachment(e.effect, action)
	}
	if e.ac == nil || e.attachment == nil {
		return errAttachmentTransition
	}
	capability := e.attachment.captureAttachmentCapability(e.ac, e.ac.transport())
	effect, admitted := e.ac.beginAttachmentEffect(capability)
	if !admitted {
		return errAttachmentTransition
	}
	defer effect.End()
	return e.d.sendRecentRouteNavigationActionForAttachment(effect, action)
}

func composePaletteClientFrame(model *palette.Model, base renderer.Frame, cfg domain.PaletteConfig, guidance string, styles ...themeui.Styles) (renderer.Frame, []renderer.Damage) {
	styleSet := resolveStyles(styles)
	modal := paletteModalFor(domain.Size{Cols: base.Width, Rows: base.Height}, cfg)
	return composeModalClientFrame(base, modal, styleSet, func(size domain.Size) renderer.Frame {
		return model.Render(size, palette.RenderOptions{Styles: palette.RenderStyles{Base: styleSet.PickerBase, Row: styleSet.PickerBase, Selection: styleSet.PickerSelection, Description: styleSet.PickerDescription, SelectionDescription: styleSet.PickerSelectionMuted}, Guidance: guidance})
	})
}
