package client

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Picker projections, ported from the daemon-built picker
// on main (picker.go, picker_lines.go).
//
// The catalogue publishes two projections of the same rows, keys, and
// actions:
//
//   - Recent is flat: live local sessions by recency (LastUsedSeq), then each
//     remote host's sessions in catalogue order, then stopped local sessions
//     by recency. Remote rows carry their origin in the detail because no
//     section names it.
//   - Grouped is sectioned by daemon: the local section (live by recency,
//     then stopped), then one section per remote host.
//
// A session with tabs is a non-focusable header; its tab rows are the
// destinations, each resolving to its exact tab. A session without tab
// metadata is its own destination. A host status row is published only when
// it adds information: the host lists no session and is either not reachable
// or known to have no daemon.
//
// The cursor starts on the attachment the picker is presented over (its tab,
// else its session), otherwise on the most recent session's active tab.

// pickerCurrent names the attachment the picker is presented over. Only its
// presence changes the cursor hint; it is never a selection authority.
type pickerCurrent struct {
	known     bool
	local     bool
	endpoint  string
	lifecycle domain.SessionLifecycleID
	tab       domain.TabStableID
}

// pickerRow is one projected line plus the facts its cursor hint needs.
type pickerRow struct {
	line          protocol.PickerLine
	ref           pickerSelectionRef
	hasRef        bool
	defaultCursor bool
}

// pickerSessionBlock is one session header and its tab rows.
type pickerSessionBlock struct {
	rows        []pickerRow
	lastUsedSeq uint64
	name        string
	lifecycle   domain.SessionLifecycleID
}

// projectLocked rebuilds both projections and their opaque keys from the
// current host state. It is deterministic: the same inputs always produce the
// same keys, order, and actions.
func (c *pickerCatalogue) projectLocked() {
	now := c.clock.Now()
	refs := make(map[string]pickerSelectionRef)
	var localLive, localStopped []pickerSessionBlock
	localOrigin := ""
	localStatus := protocol.PickerLineStatusNone
	hasLocal := false
	type remoteGroup struct {
		origin string
		status protocol.PickerLineStatus
		rows   []pickerRow
	}
	var remotes []remoteGroup
	var localHost []pickerRow
	for _, key := range c.order {
		host, ok := c.hosts[key]
		if !ok {
			continue
		}
		observation := host.observation
		fresh := c.freshLocked(observation, now)
		origin := pickerOriginLabel(observation)
		var hostRows []pickerRow
		for i := range observation.Sessions {
			session := observation.Sessions[i]
			if session.LifecycleID == (domain.SessionLifecycleID{}) {
				// A session without a lifecycle cannot be addressed exactly and
				// is never invented into one.
				continue
			}
			block := c.sessionBlockLocked(observation, session, fresh, refs)
			switch {
			case !observation.Local:
				hostRows = append(hostRows, block.rows...)
			case session.State == catalogue.RemoteCatalogSessionUp:
				localLive = append(localLive, block)
			default:
				localStopped = append(localStopped, block)
			}
		}
		hasHostRow := len(observation.Sessions) == 0 && observation.Availability != domain.RemoteAvailabilityReachable
		if hasHostRow {
			hostRef := pickerSelectionRef{
				kind: pickerSelectionCreateEphemeral, epoch: c.snapshot.Epoch,
				local: observation.Local, endpoint: observation.Endpoint, registration: observation.Registration,
			}
			hostRows = append(hostRows, pickerRow{line: protocol.PickerLine{
				Key:          pickerHostRowKey(observation),
				Kind:         protocol.PickerLineHost,
				Label:        origin,
				Detail:       pickerHostDetail(observation),
				Status:       pickerHostProblem(observation, fresh),
				StatusDetail: pickerObservationReason(observation, fresh),
				Dim:          true,
				// A host row is focusable and offers creation only when no daemon
				// exists. The endpoint registration is revalidated at commit.
				Focusable: true,
				Actions: func() protocol.PickerLineActions {
					if observation.Availability == domain.RemoteAvailabilityNoDaemon {
						return protocol.PickerCanNavigate
					}
					return 0
				}(),
			}, ref: hostRef, hasRef: observation.Availability == domain.RemoteAvailabilityNoDaemon})
		}
		// The section carries the host's problem dot unless a host row below
		// it already does. The local host never shows a transient stale dot:
		// no toast would explain it.
		sectionStatus := pickerHostProblem(observation, fresh)
		if hasHostRow || observation.Local && sectionStatus == protocol.PickerLineStatusStale {
			sectionStatus = protocol.PickerLineStatusNone
		}
		if observation.Local {
			hasLocal = true
			localOrigin = origin
			localStatus = sectionStatus
			localHost = hostRows
			continue
		}
		if len(hostRows) != 0 {
			remotes = append(remotes, remoteGroup{origin: origin, status: sectionStatus, rows: hostRows})
		}
	}
	sortPickerBlocks(localLive)
	sortPickerBlocks(localStopped)

	var recent, grouped []pickerRow
	localRows := make([]pickerRow, 0)
	for _, block := range localLive {
		localRows = append(localRows, block.rows...)
	}
	localRows = append(localRows, localHost...)
	for _, row := range localHost {
		if row.hasRef {
			refs[row.line.Key] = row.ref
		}
	}
	recent = append(recent, localRows...)
	for _, remote := range remotes {
		for _, row := range remote.rows {
			if row.line.Kind == protocol.PickerLineHost && row.hasRef {
				refs[row.line.Key] = row.ref
			}
			if row.line.Kind == protocol.PickerLineSession && row.line.Detail == "" {
				row.line.Detail = "@" + remote.origin
			} else if row.line.Kind == protocol.PickerLineSession {
				row.line.Detail = "@" + remote.origin + " · " + row.line.Detail
			}
			recent = append(recent, row)
		}
	}
	stoppedRows := make([]pickerRow, 0)
	for _, block := range localStopped {
		stoppedRows = append(stoppedRows, block.rows...)
	}
	recent = append(recent, stoppedRows...)

	localRows = append(localRows, stoppedRows...)
	if hasLocal && len(localRows) != 0 {
		grouped = append(grouped, pickerRow{line: protocol.PickerLine{Kind: protocol.PickerLineSection, Label: localOrigin, Status: localStatus, Dim: true}})
		grouped = append(grouped, localRows...)
	}
	for _, remote := range remotes {
		grouped = append(grouped, pickerRow{line: protocol.PickerLine{Kind: protocol.PickerLineSection, Label: remote.origin, Status: remote.status, Dim: true}})
		grouped = append(grouped, remote.rows...)
	}

	c.moveRetiredLocked(refs)
	c.refs = refs
	c.lines, c.cursor = c.pickerProjectionLocked(grouped)
	c.recent, c.recentCursor = c.pickerProjectionLocked(recent)
}

// sessionBlockLocked projects one session into its header and tab rows.
func (c *pickerCatalogue) sessionBlockLocked(observation ports.BrokerDaemonObservation, session catalogue.RemoteCatalogSession, fresh bool, refs map[string]pickerSelectionRef) pickerSessionBlock {
	compatible := pickerObservationCompatible(observation)
	available := observation.Availability == domain.RemoteAvailabilityReachable
	eligible := compatible && available && fresh
	// Admission is deliberately more permissive than eligibility: a stale or
	// not-yet-observed daemon is still selectable, because only the
	// destination revalidates the exact identity and only it may start a
	// stopped target under the resolved policy. A known incompatibility and a
	// broken session refuse immediately; a remote host known to be failing
	// refuses at resolution with a notice instead of a slow attempt.
	admissible := !pickerObservationKnownIncompatible(observation)
	selectable := admissible && session.State != catalogue.RemoteCatalogSessionBroken
	stopped := session.State == catalogue.RemoteCatalogSessionDown
	reason := pickerObservationReason(observation, fresh)
	if reason == "" {
		reason = session.Reason
	}
	ref := pickerSelectionRef{
		kind:         pickerSelectionExact,
		epoch:        c.snapshot.Epoch,
		local:        observation.Local,
		endpoint:     observation.Endpoint,
		registration: observation.Registration,
		lifecycle:    session.LifecycleID,
		name:         session.Name,
	}
	actions := pickerRowActions(selectable, observation.Local)
	attention := false
	for _, tab := range session.Tabs {
		attention = attention || tab.Attention
	}
	headerKey := pickerSessionRowKey(ref)
	refs[headerKey] = ref
	header := pickerRow{
		line: protocol.PickerLine{
			Key:          headerKey,
			Kind:         protocol.PickerLineSession,
			Label:        session.Name,
			Detail:       pickerSessionDetail(session),
			Status:       pickerSessionStatus(session),
			StatusDetail: reason,
			Attention:    attention,
			Stopped:      stopped,
			// Dim keeps the informational eligibility signal (fresh,
			// compatible, reachable) and the hard non-committable state.
			Dim:       !eligible || !selectable,
			Ephemeral: session.Ephemeral,
		},
		ref:    ref,
		hasRef: true,
	}
	block := pickerSessionBlock{lastUsedSeq: session.LastUsedSeq, name: session.Name, lifecycle: session.LifecycleID}
	if len(session.Tabs) == 0 {
		// Without tab metadata the session is its own destination.
		header.line.Focusable = true
		header.line.Actions = actions
		header.defaultCursor = true
		block.rows = append(block.rows, header)
		return block
	}
	// A session with tabs names a group: its tab rows are the destinations.
	block.rows = append(block.rows, header)
	active := pickerActiveTabIndex(session)
	for i, tab := range session.Tabs {
		tabRef := ref
		tabRef.tab = pickerTabRef{present: true, id: domain.TabStableID(tab.ID), index: i, name: tab.Name, count: len(session.Tabs)}
		key := pickerTabRowKey(tabRef)
		refs[key] = tabRef
		tabActions := actions
		if !stopped && tab.ID == "" {
			// A live tab without a stable identity cannot be targeted exactly.
			tabActions &^= protocol.PickerCanNavigate
		}
		label := tab.Name
		if label == "" {
			label = strconv.Itoa(int(tab.Index) + 1)
		}
		block.rows = append(block.rows, pickerRow{
			line: protocol.PickerLine{
				Key:          key,
				Kind:         protocol.PickerLineTab,
				Label:        label,
				Detail:       tab.Detail,
				StatusDetail: reason,
				Attention:    tab.Attention,
				Stopped:      stopped,
				Dim:          header.line.Dim,
				Focusable:    true,
				Actions:      tabActions,
				Ephemeral:    session.Ephemeral,
			},
			ref:           tabRef,
			hasRef:        true,
			defaultCursor: i == active,
		})
	}
	return block
}

// pickerRowActions maps eligibility to the actions a row authorises. Kill is
// offered on local rows only, exactly as on main; the daemon revalidates the
// name, and a stopped row deletes its history record.
func pickerRowActions(selectable, local bool) protocol.PickerLineActions {
	var actions protocol.PickerLineActions
	if selectable {
		actions |= protocol.PickerCanNavigate
	}
	if local {
		actions |= protocol.PickerCanKill
	}
	return actions
}

// pickerActiveTabIndex is the session's active tab, or its first tab.
func pickerActiveTabIndex(session catalogue.RemoteCatalogSession) int {
	for i, tab := range session.Tabs {
		if session.ActiveTabID != "" && tab.ID == session.ActiveTabID {
			return i
		}
	}
	return 0
}

// sortPickerBlocks orders sessions by recency, then name, then lifecycle.
func sortPickerBlocks(blocks []pickerSessionBlock) {
	sort.SliceStable(blocks, func(i, j int) bool {
		if blocks[i].lastUsedSeq != blocks[j].lastUsedSeq {
			return blocks[i].lastUsedSeq > blocks[j].lastUsedSeq
		}
		if blocks[i].name != blocks[j].name {
			return blocks[i].name < blocks[j].name
		}
		return blocks[i].lifecycle.String() < blocks[j].lifecycle.String()
	})
}

// pickerProjectionLocked flattens one projection and computes its cursor:
// the current attachment's tab, else its session, else the first default row
// (the most recent session's active tab), else the first actionable row.
func (c *pickerCatalogue) pickerProjectionLocked(rows []pickerRow) ([]protocol.PickerLine, protocol.PickerCursor) {
	lines := make([]protocol.PickerLine, len(rows))
	for i := range rows {
		lines[i] = rows[i].line
	}
	if cursor, ok := c.currentCursorLocked(rows); ok {
		return lines, cursor
	}
	for i, row := range rows {
		if row.defaultCursor && row.line.Focusable && row.line.Actions&protocol.PickerCanNavigate != 0 {
			return lines, protocol.PickerCursor{Key: row.line.Key, Index: i}
		}
	}
	return lines, pickerCatalogueCursor(lines)
}

func (c *pickerCatalogue) currentCursorLocked(rows []pickerRow) (protocol.PickerCursor, bool) {
	current := c.current
	if !current.known {
		return protocol.PickerCursor{}, false
	}
	matches := func(ref pickerSelectionRef) bool {
		if ref.lifecycle != current.lifecycle || ref.local != current.local {
			return false
		}
		return ref.local || ref.endpoint == current.endpoint
	}
	// Exact tab first, then the session's own row (no tab metadata), then its
	// active tab.
	session, active := -1, -1
	for i, row := range rows {
		if !row.hasRef || !row.line.Focusable || !matches(row.ref) {
			continue
		}
		if !row.ref.tab.present {
			session = i
			continue
		}
		if current.tab != "" && row.ref.tab.id == current.tab {
			return protocol.PickerCursor{Key: row.line.Key, Index: i}, true
		}
		if row.defaultCursor && active < 0 {
			active = i
		}
	}
	for _, index := range []int{session, active} {
		if index >= 0 {
			return protocol.PickerCursor{Key: rows[index].line.Key, Index: index}, true
		}
	}
	return protocol.PickerCursor{}, false
}

// pickerTabRowKey is the stable, opaque key of one tab row: the session
// identity plus the tab's stable ID, or its ordinal and raw name when the
// catalogue carries no ID.
func pickerTabRowKey(ref pickerSelectionRef) string {
	tab := string(ref.tab.id)
	if tab == "" {
		tab = fmt.Sprintf("#%d:%s", ref.tab.index, ref.tab.name)
	}
	lifecycle := append(append([]byte(nil), ref.lifecycle[:]...), 0)
	lifecycle = append(lifecycle, tab...)
	return pickerRowKey("t", ref.local, ref.endpoint, ref.registration.Incarnation[:], ref.registration.Generation, lifecycle)
}

// attachmentTab is the client-owned tab selection an attachment Hello
// carries: a stable tab to prefer on a live or stopped-local session, or an
// exact stopped-remote selector. Its zero value attaches at session level.
type attachmentTab struct {
	preferred domain.TabStableID
	stopped   *protocol.SessionAttachTarget
}

// pickerResolveTab turns a tab ref into the attachment tab it names, checked
// against the latest session metadata. A tab that left the session is gone.
func pickerResolveTab(ref pickerSelectionRef, session catalogue.RemoteCatalogSession) (attachmentTab, error) {
	if !ref.tab.present {
		return attachmentTab{}, nil
	}
	gone := pickerCatalogueError{Code: pickerCatalogueGone, Text: "tab is no longer in the catalogue"}
	stopped := session.State == catalogue.RemoteCatalogSessionDown
	if ref.tab.id != "" {
		found := false
		for _, tab := range session.Tabs {
			if domain.TabStableID(tab.ID) == ref.tab.id {
				found = true
				break
			}
		}
		if !found {
			return attachmentTab{}, gone
		}
		if stopped && !ref.local {
			target := protocol.SessionAttachTarget{LifecycleID: session.LifecycleID, SessionName: session.Name, TabID: ref.tab.id, TabIndex: protocol.NoTabIndex, Stopped: true}
			if target.Validate() != nil {
				return attachmentTab{}, pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "stopped tab cannot be addressed"}
			}
			return attachmentTab{stopped: &target}, nil
		}
		return attachmentTab{preferred: ref.tab.id}, nil
	}
	if ref.tab.index >= len(session.Tabs) || session.Tabs[ref.tab.index].Name != ref.tab.name || len(session.Tabs) != ref.tab.count {
		return attachmentTab{}, gone
	}
	if !stopped {
		return attachmentTab{}, pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "tab has no stable identity"}
	}
	if ref.local || ref.tab.count > int(^uint16(0)) {
		// A local restore without a stable tab falls back to the session.
		return attachmentTab{}, nil
	}
	target := protocol.SessionAttachTarget{
		LifecycleID: session.LifecycleID, SessionName: session.Name, Stopped: true,
		TabIndex: int32(ref.tab.index), TabRawName: ref.tab.name, TabExpectedCount: uint16(ref.tab.count),
	}
	if target.Validate() != nil {
		return attachmentTab{}, pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "stopped tab cannot be addressed"}
	}
	return attachmentTab{stopped: &target}, nil
}

// pickerObservationFailing reports a host the broker observed failing: its
// last probe could not reach it, authenticate, or read its catalogue.
func pickerObservationFailing(observation ports.BrokerDaemonObservation) bool {
	switch observation.Availability {
	case domain.RemoteAvailabilityUnreachable, domain.RemoteAvailabilityAuthFailed, domain.RemoteAvailabilityInvalidResponse, domain.RemoteAvailabilityNoDaemon:
		return true
	default:
		return false
	}
}

// pickerUnavailableNotice is main's remote-unavailable notice: the session
// and its origin, then the reason in words.
func pickerUnavailableNotice(ref pickerSelectionRef, observation ports.BrokerDaemonObservation, reason string) string {
	message := "Remote session unavailable"
	origin := pickerOriginLabel(observation)
	if ref.name != "" {
		message += ": " + ref.name + "@" + origin
	} else {
		message += ": " + origin
	}
	return message + " — " + pickerReasonText(reason)
}

// pickerReasonText renders one domain.RemoteReason in words (main's
// remotePickerReasonText).
func pickerReasonText(reason string) string {
	switch reason {
	case domain.RemoteReasonCatalogStale:
		return "catalog stale"
	case domain.RemoteReasonHostUnreachable:
		return "host unreachable"
	case domain.RemoteReasonVersionMismatch:
		return "version mismatch"
	case domain.RemoteReasonSessionStopped:
		return "session stopped"
	case domain.RemoteReasonSessionBroken:
		return "session broken"
	case domain.RemoteReasonMalformed:
		return "catalog malformed"
	case domain.RemoteReasonAuthFailure:
		return "authentication failed"
	case domain.RemoteReasonNoDaemon:
		return "no daemon — Enter to create a session"
	case domain.RemoteReasonRefreshing:
		return "refreshing"
	case domain.RemoteReasonIdentityChanged:
		return "session identity changed"
	default:
		return "unavailable"
	}
}

// pickerKillTarget is one resolved kill: the exact local control route and
// the session name the daemon kills (live) or deletes from history
// (stopped).
type pickerKillTarget struct {
	route   BrokerOperationRoute
	name    string
	stopped bool
}

// ResolveKill revalidates one row key for destruction against the latest
// publication. Only local sessions are killable, exactly as on main; the row
// must still name the same lifecycle and name.
func (c *pickerCatalogue) ResolveKill(key string) (pickerKillTarget, error) {
	if c == nil {
		return pickerKillTarget{}, pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "no catalogue"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ref, ok := c.refs[key]
	if !ok {
		ref, ok = c.retired[key]
	}
	if !ok || ref.kind != pickerSelectionExact {
		return pickerKillTarget{}, pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "picker row is not in the catalogue"}
	}
	if !ref.local {
		return pickerKillTarget{}, pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "remote sessions cannot be killed from the picker"}
	}
	epoch := c.snapshot.Epoch
	if epoch == 0 {
		return pickerKillTarget{}, pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "broker catalogue is unavailable"}
	}
	if ref.epoch != 0 && ref.epoch != epoch {
		return pickerKillTarget{}, pickerCatalogueError{Code: pickerCatalogueEpochStale, Text: "broker epoch changed"}
	}
	authority := c.authorityForRefLocked(ref)
	if !authority.found {
		return pickerKillTarget{}, pickerCatalogueError{Code: pickerCatalogueGone, Text: "host is no longer in the catalogue"}
	}
	session, found := pickerFindSession(authority.observation.Sessions, ref.lifecycle)
	if !found {
		if pickerSessionReplaced(authority.observation.Sessions, ref) {
			return pickerKillTarget{}, pickerCatalogueError{Code: pickerCatalogueReplaced, Text: "session identity was replaced"}
		}
		return pickerKillTarget{}, pickerCatalogueError{Code: pickerCatalogueGone, Text: "session is no longer in the catalogue"}
	}
	if session.Name != ref.name {
		return pickerKillTarget{}, pickerCatalogueError{Code: pickerCatalogueReplaced, Text: "session identity was replaced"}
	}
	if domain.ValidateSessionName(session.Name) != nil {
		return pickerKillTarget{}, pickerCatalogueError{Code: pickerCatalogueInvalidName, Text: "session name is not valid"}
	}
	return pickerKillTarget{
		route:   BrokerOperationRoute{Epoch: epoch, Destination: ports.BrokerEndpointFence{Local: true}},
		name:    session.Name,
		stopped: session.State != catalogue.RemoteCatalogSessionUp,
	}, nil
}

// remoteHealth reports the notice-relevant state of every remote host, in
// catalogue order. Transition policy lives in domain.RemoteHealthNotice.
func (c *pickerCatalogue) remoteHealth() []domain.RemoteHealth {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	health := make([]domain.RemoteHealth, 0, len(c.order))
	for _, key := range c.order {
		host, ok := c.hosts[key]
		if !ok || host.observation.Local {
			continue
		}
		observation := host.observation
		health = append(health, domain.RemoteHealth{
			Key:             key,
			Origin:          pickerOriginLabel(observation),
			Availability:    observation.Availability,
			Failure:         observation.LastFailure.Kind,
			VersionMismatch: pickerObservationVersionMismatch(observation),
			Episode:         observation.FailureEpisode,
		})
	}
	return health
}
