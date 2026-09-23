package client

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Client-owned picker catalogue.
//
// The autonomous supervisor owns the broker connection and the single terminal
// input lifetime; this file owns the catalogue the picker presents, projected
// from the broker's own observations rather than from a serving-daemon
// catalogue. It projects broker observations instead of the daemon-served
// protocol.PickerSnapshot: rows are derived from ports.BrokerSnapshot, the
// opaque keys stay client-local, and a selection resolves to an exact
// ports.BrokerOpenStreamRequest instead of a daemon-side key handoff.
//
// The projection preserves configured authority and observed state separately,
// exactly as ports.BrokerDaemonObservation does:
//
//   - daemon identity is the local flag or the remote endpoint plus the
//     registration incarnation; a re-registered endpoint is an identity
//     replacement and its rows never survive.
//   - session identity is the catalogue lifecycle ID; a same-name replacement
//     resolves as "replaced", never by name.
//   - freshness is the LastSuccess window against the injected clock.
//   - compatibility is the observed protocol version against the policy's
//     required version.
//   - availability is the observed RemoteAvailability.
//
// Updates are monotonic: a publication that does not supersede the applied
// epoch/revision is ignored, and a per-daemon observation that regresses
// (older LastSuccess and no newer failure episode, same incarnation) keeps the
// newer observed state while still refreshing configured authority. Row order
// is stable: hosts keep their first-seen position and unrelated updates never
// reshuffle them. Nothing here attaches to a daemon, starts one, or writes a
// session.

const (
	// pickerCatalogueSourceID names the single client-owned picker source.
	pickerCatalogueSourceID = "broker"
	// pickerCatalogueDefaultFreshness bounds how old a LastSuccess may be before
	// a row is displayed as stale. Freshness is presentation information and
	// never a resolution lock: a stale row is dimmed and badged, but an explicit
	// attach or creation may still be attempted, because only the destination
	// revalidates the exact identity.
	pickerCatalogueDefaultFreshness = 30 * time.Second
	// pickerCatalogueMaxRetiredRefs bounds the retired-key fence used to
	// distinguish "gone" from "replaced" after a row leaves the projection.
	pickerCatalogueMaxRetiredRefs = 128
)

// pickerCatalogueErrorCode is the closed refusal taxonomy for a client-owned
// picker selection. The zero value is never produced: a refusal always names
// why the exact selection cannot be resolved.
type pickerCatalogueErrorCode uint8

const (
	pickerCatalogueUnknown pickerCatalogueErrorCode = iota + 1
	pickerCatalogueGone
	pickerCatalogueReplaced
	pickerCatalogueIncompatible
	pickerCatalogueUnavailable
	pickerCatalogueEpochStale
	pickerCatalogueInvalidName
	pickerCatalogueNoSelection
)

func (c pickerCatalogueErrorCode) String() string {
	switch c {
	case pickerCatalogueUnknown:
		return "unknown"
	case pickerCatalogueGone:
		return "gone"
	case pickerCatalogueReplaced:
		return "replaced"
	case pickerCatalogueIncompatible:
		return "incompatible"
	case pickerCatalogueUnavailable:
		return "unavailable"
	case pickerCatalogueEpochStale:
		return "stale_epoch"
	case pickerCatalogueInvalidName:
		return "invalid_name"
	case pickerCatalogueNoSelection:
		return "no_selection"
	default:
		return "unknown"
	}
}

// pickerCatalogueError is a typed, bounded refusal. It never carries a raw
// endpoint, credential, or adapter diagnostic as display text.
type pickerCatalogueError struct {
	Code pickerCatalogueErrorCode
	Text string
	// Notice is the bounded user-facing sentence shown for this refusal when
	// the generic "picker selection <code>" wording would name the wrong
	// thing, as for a remote host known to be failing.
	Notice string
}

// noticeText is the sentence a picker toast shows for this refusal.
func (e pickerCatalogueError) noticeText() string {
	if e.Notice != "" {
		return e.Notice
	}
	return e.Error()
}

func (e pickerCatalogueError) Error() string {
	if e.Text == "" {
		return "vev: picker selection " + e.Code.String()
	}
	return "vev: picker selection " + e.Code.String() + ": " + e.Text
}

// pickerSelectionKind names one closed admission variant a resolved selection
// can carry.
type pickerSelectionKind uint8

const (
	// pickerSelectionExact attaches to one exact session lifecycle.
	pickerSelectionExact pickerSelectionKind = iota + 1
	// pickerSelectionCreateNamed creates one named session.
	pickerSelectionCreateNamed
	// pickerSelectionCreateEphemeral creates one ephemeral session.
	pickerSelectionCreateEphemeral
)

// pickerSelectionRef is the client's exact selection identity, captured when a
// row was projected. Resolution revalidates it against the latest snapshot; it
// is never reconstructed from a display label.
type pickerSelectionRef struct {
	kind         pickerSelectionKind
	epoch        ports.BrokerEpoch
	local        bool
	endpoint     string
	registration domain.RemoteRegistration
	lifecycle    domain.SessionLifecycleID
	name         string
	createName   string
	// tab names one tab row inside the session; its zero value is a
	// session-level selection.
	tab pickerTabRef
}

// pickerTabRef is the exact tab a tab row names, captured when it was
// projected: the stable ID when the catalogue carries one, and the ordinal,
// raw name, and tab count a stopped remote selector needs otherwise.
type pickerTabRef struct {
	present bool
	id      domain.TabStableID
	index   int
	name    string
	count   int
}

// pickerResolveBase carries the live broker connection identity a resolved
// request must be fenced to. The catalogue supplies everything else.
type pickerResolveBase struct {
	Connection ports.BrokerConnectionID
	Stream     ports.BrokerStreamID
	Env        []string
}

// pickerHostState is one projected daemon and its newest accepted
// observation.
type pickerHostState struct {
	observation ports.BrokerDaemonObservation
}

// pickerCatalogueConfig supplies the fresh-window clock. Clock is optional and
// falls back to the system clock; Freshness defaults to
// pickerCatalogueDefaultFreshness.
type pickerCatalogueConfig struct {
	Clock     ports.Clock
	Freshness time.Duration
}

// pickerCatalogue is the client-owned, monotonic projection of broker daemon
// observations into ordered picker rows. It is safe for concurrent use: the
// supervisor applies publications from its run path while the picker loop
// resolves selections.
type pickerCatalogue struct {
	clock     ports.Clock
	freshness time.Duration

	mu           sync.Mutex
	snapshot     ports.BrokerSnapshot
	hosts        map[string]*pickerHostState
	order        []string
	refs         map[string]pickerSelectionRef
	retired      map[string]pickerSelectionRef
	retiredOrder []string
	// lines/cursor are the grouped projection; recent/recentCursor the flat
	// recency projection. Both carry the same keys and actions.
	lines        []protocol.PickerLine
	cursor       protocol.PickerCursor
	recent       []protocol.PickerLine
	recentCursor protocol.PickerCursor
	// current is the attachment the picker is presented over; it only
	// decides where a fresh cursor starts.
	current pickerCurrent
}

// newPickerCatalogue builds an empty projection.
func newPickerCatalogue(cfg pickerCatalogueConfig) *pickerCatalogue {
	clock := cfg.Clock
	if supervisorNil(clock) {
		clock = systemClock{}
	}
	freshness := cfg.Freshness
	if freshness <= 0 {
		freshness = pickerCatalogueDefaultFreshness
	}
	return &pickerCatalogue{
		clock:     clock,
		freshness: freshness,
		hosts:     make(map[string]*pickerHostState),
		refs:      make(map[string]pickerSelectionRef),
		retired:   make(map[string]pickerSelectionRef),
	}
}

// Apply folds one broker publication into the projection. It reports whether
// the projection changed. The publication is the projection's trust boundary:
// a snapshot that fails its own contract is rejected outright and the previous
// projection is kept. A valid publication that does not supersede the applied
// epoch/revision is also ignored, so a slow or out-of-order snapshot can never
// regress a row.
func (c *pickerCatalogue) Apply(snapshot ports.BrokerSnapshot) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if snapshot.Epoch == 0 || snapshot.Revision == 0 {
		return false
	}
	if err := snapshot.Validate(); err != nil {
		return false
	}
	if !snapshot.Supersedes(c.snapshot) {
		return false
	}
	if c.snapshot.Epoch != 0 && snapshot.Epoch != c.snapshot.Epoch {
		// A new broker process is a full resynchronization: every key, row, and
		// retired fence from the previous epoch is invalid.
		c.resetLocked()
	}
	c.snapshot = snapshot.Clone()
	c.mergeHostsLocked()
	c.projectLocked()
	return true
}

// Epoch reports the applied broker epoch, or zero before the first publication.
func (c *pickerCatalogue) Epoch() ports.BrokerEpoch {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshot.Epoch
}

// Revision reports the applied broker revision, or zero before the first
// publication.
func (c *pickerCatalogue) Revision() ports.BrokerRevision {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshot.Revision
}

// Snapshot returns a defensive copy of the applied broker snapshot.
func (c *pickerCatalogue) Snapshot() ports.BrokerSnapshot {
	if c == nil {
		return ports.BrokerSnapshot{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshot.Clone()
}

// Lines returns a copy of the current ordered rows.
func (c *pickerCatalogue) Lines() []protocol.PickerLine {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.PickerLine(nil), c.lines...)
}

// Cursor returns the projection's cursor hint.
func (c *pickerCatalogue) Cursor() protocol.PickerCursor {
	if c == nil {
		return protocol.PickerCursor{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursor
}

// projections returns the recent and grouped projections together, so both
// always describe the same publication.
func (c *pickerCatalogue) projections() (recent, grouped protocol.PickerProjection) {
	if c == nil {
		return protocol.PickerProjection{}, protocol.PickerProjection{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	recent = protocol.PickerProjection{Lines: append([]protocol.PickerLine(nil), c.recent...), Cursor: c.recentCursor}
	grouped = protocol.PickerProjection{Lines: append([]protocol.PickerLine(nil), c.lines...), Cursor: c.cursor}
	return recent, grouped
}

// RecentLines returns a copy of the flat recency projection.
func (c *pickerCatalogue) RecentLines() []protocol.PickerLine {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.PickerLine(nil), c.recent...)
}

// SetCurrent records the attachment the picker is presented over (or none)
// and recomputes the cursor hints.
func (c *pickerCatalogue) SetCurrent(current pickerCurrent) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == current {
		return
	}
	c.current = current
	if c.snapshot.Epoch != 0 {
		c.projectLocked()
	}
}

// Ref returns the exact selection identity recorded for one row key, including
// a retired key that has left the current projection.
func (c *pickerCatalogue) Ref(key string) (pickerSelectionRef, bool) {
	if c == nil {
		return pickerSelectionRef{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ref, ok := c.refs[key]; ok {
		return ref, true
	}
	ref, ok := c.retired[key]
	return ref, ok
}

// Resolve revalidates one row key against the LATEST applied snapshot and
// returns the exact broker stream request it names. It refuses with a typed
// refusal when the identity is gone, replaced, stale, incompatible, or
// unavailable, and it never invents identity.
func (c *pickerCatalogue) Resolve(key string, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	if c == nil {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "no catalogue"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ref, ok := c.refs[key]
	if !ok {
		ref, ok = c.retired[key]
	}
	if !ok {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "picker row is not in the catalogue"}
	}
	return c.resolveRefLocked(ref, base)
}

// ResolveTarget is Resolve plus the exact tab the row names, both revalidated
// under one lock against the same publication.
func (c *pickerCatalogue) ResolveTarget(key string, base pickerResolveBase) (ports.BrokerOpenStreamRequest, attachmentTab, error) {
	if c == nil {
		return ports.BrokerOpenStreamRequest{}, attachmentTab{}, pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "no catalogue"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ref, ok := c.refs[key]
	if !ok {
		ref, ok = c.retired[key]
	}
	if !ok {
		return ports.BrokerOpenStreamRequest{}, attachmentTab{}, pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "picker row is not in the catalogue"}
	}
	return resolvePickerTarget(c.snapshot.Epoch, c.authorityForRefLocked(ref), ref, base, true)
}

// ResolveRef revalidates an exact selection identity a driver captured when it
// displayed the row. It is the entry point for a selection held across a
// broker epoch or an equivalent output boundary, where the key alone would be
// re-bound to a newer identity.
func (c *pickerCatalogue) ResolveRef(ref pickerSelectionRef, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	if c == nil {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "no catalogue"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resolveRefLocked(ref, base)
}

func (c *pickerCatalogue) resetLocked() {
	c.hosts = make(map[string]*pickerHostState)
	c.order = nil
	c.refs = make(map[string]pickerSelectionRef)
	c.retired = make(map[string]pickerSelectionRef)
	c.retiredOrder = nil
	c.lines = nil
	c.cursor = protocol.PickerCursor{}
	c.recent = nil
	c.recentCursor = protocol.PickerCursor{}
}

// mergeHostsLocked folds the newest publication into the per-daemon state. A
// daemon keeps its observed state when the incoming observation regresses, and
// an incarnation change replaces the daemon's identity wholesale.
func (c *pickerCatalogue) mergeHostsLocked() {
	next := make(map[string]*pickerHostState, len(c.snapshot.Daemons))
	for i := range c.snapshot.Daemons {
		observation := c.snapshot.Daemons[i]
		key := pickerDaemonKey(observation.Local, observation.Endpoint)
		current, ok := c.hosts[key]
		if ok && pickerSameIncarnation(current.observation, observation) && !pickerObservationNewer(observation, current.observation) {
			// The publication is newer but this daemon's own observation is not:
			// keep the newer observed state and still apply configured authority.
			next[key] = &pickerHostState{
				observation: pickerMergeAuthority(current.observation, observation),
			}
			continue
		}
		next[key] = &pickerHostState{observation: observation}
	}
	c.hosts = next
	c.order = pickerStableHostOrder(c.order, c.snapshot.Daemons)
	// Tombstones fence a retired registration even if a publication still
	// carries it; a re-added endpoint with a fresh incarnation is never fenced.
	for _, tombstone := range c.snapshot.Removed {
		for key, host := range c.hosts {
			if !host.observation.Local && host.observation.Endpoint == tombstone.Endpoint && tombstone.Fences(host.observation.Registration) {
				delete(c.hosts, key)
			}
		}
	}
}

// moveRetiredLocked keeps the last known ref of a key that left the current
// projection so a late selection can still be classified as gone or replaced.
func (c *pickerCatalogue) moveRetiredLocked(next map[string]pickerSelectionRef) {
	for key, ref := range c.refs {
		if _, ok := next[key]; ok {
			continue
		}
		if _, exists := c.retired[key]; !exists {
			c.retiredOrder = append(c.retiredOrder, key)
		}
		c.retired[key] = ref
	}
	for len(c.retiredOrder) > pickerCatalogueMaxRetiredRefs {
		oldest := c.retiredOrder[0]
		c.retiredOrder = c.retiredOrder[1:]
		delete(c.retired, oldest)
	}
}

// resolveRefLocked revalidates exactly one captured selection against the
// latest applied publication.
func (c *pickerCatalogue) resolveRefLocked(ref pickerSelectionRef, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	request, _, err := resolvePickerTarget(c.snapshot.Epoch, c.authorityForRefLocked(ref), ref, base, true)
	return request, err
}

// authorityForRefLocked selects the merged observation one selection names. The
// merged host state is used rather than the raw publication, so a slow snapshot
// can never regress the authority a selection resolves against.
func (c *pickerCatalogue) authorityForRefLocked(ref pickerSelectionRef) pickerResolveAuthority {
	for _, key := range c.order {
		host, ok := c.hosts[key]
		if !ok {
			continue
		}
		if pickerAuthorityMatches(host.observation, ref) {
			return pickerResolveAuthority{observation: host.observation, found: true}
		}
	}
	return pickerResolveAuthority{}
}

// resolveCatalogueRequest resolves one selection against a raw broker
// publication. It is the entry point of the closed InitialNavigation union,
// which carries its own destination fence rather than a projected row key.
func resolveCatalogueRequest(snapshot ports.BrokerSnapshot, ref pickerSelectionRef, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	return resolvePickerRequest(snapshot.Epoch, pickerAuthorityInSnapshot(snapshot, ref), ref, base)
}

// pickerResolveAuthority is the single daemon authority one selection names in
// one publication, merged or raw.
type pickerResolveAuthority struct {
	observation ports.BrokerDaemonObservation
	found       bool
}

// pickerAuthorityMatches reports whether one observation is the authority a
// selection names: the local daemon for a local selection, or the exact remote
// endpoint otherwise.
func pickerAuthorityMatches(observation ports.BrokerDaemonObservation, ref pickerSelectionRef) bool {
	if ref.local {
		return observation.Local
	}
	return !observation.Local && observation.Endpoint == ref.endpoint
}

// pickerAuthorityInSnapshot selects the observation one selection names from a
// raw broker publication.
func pickerAuthorityInSnapshot(snapshot ports.BrokerSnapshot, ref pickerSelectionRef) pickerResolveAuthority {
	for i := range snapshot.Daemons {
		if pickerAuthorityMatches(snapshot.Daemons[i], ref) {
			return pickerResolveAuthority{observation: snapshot.Daemons[i], found: true}
		}
	}
	return pickerResolveAuthority{}
}

// resolvePickerRequest turns one validated selection into the exact broker
// stream request it names. Connection and Stream come from the caller's own
// live connection; Policy comes exclusively from the selected observation and
// the process environment is never read.
//
// Refusal rules are deliberately narrow:
//
//   - the applied publication must be valid (nonzero epoch);
//   - a captured epoch must equal the current one, never compare numerically
//     across broker processes;
//   - the selected daemon must still be in the publication, and a remote
//     selection must still match its complete registration (endpoint,
//     incarnation, and generation), not merely its incarnation, so a
//     re-registered endpoint or an advanced generation is a replacement;
//   - a known incompatibility refuses immediately, because no bounded attempt
//     can succeed against a daemon whose version or availability says so.
//
// Freshness is presentation information, not a lock: a stale, unavailable, or
// not-yet-observed daemon may still be attempted explicitly, because only the
// daemon revalidates the exact identity and only it may start a stopped target
// under the resolved policy. The request therefore always carries the explicit
// start-if-needed authorization; a policy that forbids launching still refuses
// visibly at the destination.
func resolvePickerRequest(epoch ports.BrokerEpoch, authority pickerResolveAuthority, ref pickerSelectionRef, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	request, _, err := resolvePickerTarget(epoch, authority, ref, base, false)
	return request, err
}

// resolvePickerTarget is resolvePickerRequest plus the exact tab a tab row
// names. refuseFailing is the picker's instant refusal: a
// remote host the broker observed failing (unreachable, authentication, or an
// invalid response) refuses with a notice instead of a slow attempt that
// would fail the same way. An explicit CLI target keeps the attempt.
func resolvePickerTarget(epoch ports.BrokerEpoch, authority pickerResolveAuthority, ref pickerSelectionRef, base pickerResolveBase, refuseFailing bool) (ports.BrokerOpenStreamRequest, attachmentTab, error) {
	fail := func(err error) (ports.BrokerOpenStreamRequest, attachmentTab, error) {
		return ports.BrokerOpenStreamRequest{}, attachmentTab{}, err
	}
	if epoch == 0 {
		return fail(pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "broker catalogue is unavailable"})
	}
	if ref.epoch != 0 && ref.epoch != epoch {
		return fail(pickerCatalogueError{Code: pickerCatalogueEpochStale, Text: "broker epoch changed"})
	}
	if !authority.found {
		return fail(pickerCatalogueError{Code: pickerCatalogueGone, Text: "host is no longer in the catalogue"})
	}
	observation := authority.observation
	if !ref.local && !observation.Registration.Equal(ref.registration) {
		return fail(pickerCatalogueError{Code: pickerCatalogueReplaced, Text: "host identity was replaced"})
	}
	if observation.Availability == domain.RemoteAvailabilityIncompatible || pickerObservationVersionMismatch(observation) {
		return fail(pickerCatalogueError{Code: pickerCatalogueIncompatible, Text: "host protocol is incompatible"})
	}
	if observation.Availability == domain.RemoteAvailabilityNoDaemon && ref.kind == pickerSelectionExact {
		return fail(pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "remote host has no vev daemon; choose create session"})
	}
	if observation.Availability == domain.RemoteAvailabilityNoDaemon && ref.kind != pickerSelectionCreateNamed && ref.kind != pickerSelectionCreateEphemeral {
		return fail(pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "no daemon exists on this host; choose create session"})
	}
	if refuseFailing && !ref.local && pickerObservationFailing(observation) && observation.Availability != domain.RemoteAvailabilityNoDaemon {
		reason := pickerObservationReason(observation, false)
		return fail(pickerCatalogueError{
			Code:   pickerCatalogueUnavailable,
			Text:   "host " + reason,
			Notice: pickerUnavailableNotice(ref, observation, reason),
		})
	}
	request := ports.BrokerOpenStreamRequest{
		Epoch:        epoch,
		Purpose:      ports.BrokerStreamAttachment,
		Local:        ref.local,
		Connection:   base.Connection,
		Stream:       base.Stream,
		Registration: observation.Registration,
		Policy:       observation.Policy,
		Env:          append([]string(nil), base.Env...),
		// An attachment or creation may need to start a stopped daemon, so it
		// carries the explicit start-if-needed authorization; the resolved
		// policy still decides whether launching is permitted.
		StartMode: ports.BrokerDaemonStartIfNeeded,
	}
	if !ref.local {
		request.Endpoint = observation.Endpoint
	}
	var tab attachmentTab
	switch ref.kind {
	case pickerSelectionExact:
		session, ok := pickerFindSession(observation.Sessions, ref.lifecycle)
		if !ok {
			if pickerSessionReplaced(observation.Sessions, ref) {
				return fail(pickerCatalogueError{Code: pickerCatalogueReplaced, Text: "session identity was replaced"})
			}
			return fail(pickerCatalogueError{Code: pickerCatalogueGone, Text: "session is no longer in the catalogue"})
		}
		if session.Name != ref.name {
			// The exact identity is the lifecycle/name pair the daemon
			// revalidates: a renamed lifecycle is a different selection.
			return fail(pickerCatalogueError{Code: pickerCatalogueReplaced, Text: "session identity was replaced"})
		}
		if session.State == catalogue.RemoteCatalogSessionBroken {
			return fail(pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "session is broken"})
		}
		resolved, err := pickerResolveTab(ref, session)
		if err != nil {
			return fail(err)
		}
		tab = resolved
		request.Admission = ports.BrokerAdmissionExact
		request.Target = protocol.ExactSessionTarget{LifecycleID: session.LifecycleID, SessionName: session.Name}
	case pickerSelectionCreateNamed:
		if err := domain.ValidateSessionName(ref.createName); err != nil {
			return fail(pickerCatalogueError{Code: pickerCatalogueInvalidName, Text: "session name is not valid"})
		}
		request.Admission = ports.BrokerAdmissionCreateNamed
		request.Name = ref.createName
	case pickerSelectionCreateEphemeral:
		request.Admission = ports.BrokerAdmissionCreateEphemeral
	default:
		return fail(pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "picker selection kind is unknown"})
	}
	if err := request.Validate(); err != nil {
		return fail(pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "picker stream request is not valid"})
	}
	return request, tab, nil
}

// freshLocked reports whether LastSuccess is inside the freshness window. An
// observation that never succeeded is never fresh.
func (c *pickerCatalogue) freshLocked(observation ports.BrokerDaemonObservation, now time.Time) bool {
	if observation.LastSuccess.IsZero() {
		return false
	}
	return now.Sub(observation.LastSuccess) <= c.freshness
}

func pickerDaemonKey(local bool, endpoint string) string {
	if local {
		return "local"
	}
	return "remote:" + endpoint
}

// pickerCatalogueOriginFallback is the fixed, bounded label shown when an
// observation carries no display origin. A raw endpoint is never a display
// label.
const pickerCatalogueOriginFallback = "remote"

func pickerOriginLabel(observation ports.BrokerDaemonObservation) string {
	if observation.DisplayOrigin != "" {
		return observation.DisplayOrigin
	}
	if observation.Local {
		return "local"
	}
	return pickerCatalogueOriginFallback
}

// pickerHostRowKey is the stable, opaque key of a daemon's status row. It is
// never a selection: a host row is focusable but not committable.
func pickerHostRowKey(observation ports.BrokerDaemonObservation) string {
	return pickerRowKey("h", observation.Local, observation.Endpoint, observation.Registration.Incarnation[:], observation.Registration.Generation, nil)
}

// pickerSessionRowKey is the stable, opaque key of one exact session. It is
// derived from the complete identity (endpoint, registration incarnation and
// generation, session lifecycle): a rename never changes it, a replaced
// lifecycle always does, and an advanced registration generation does too. The
// generation is part of the key precisely so that changed authority requires a
// fresh explicit selection instead of silently re-binding a displayed row.
func pickerSessionRowKey(ref pickerSelectionRef) string {
	return pickerRowKey("s", ref.local, ref.endpoint, ref.registration.Incarnation[:], ref.registration.Generation, ref.lifecycle[:])
}

func pickerRowKey(prefix string, local bool, endpoint string, incarnation []byte, generation domain.RemoteGeneration, lifecycle []byte) string {
	digest := sha256.New()
	digest.Write([]byte("vev.picker."))
	digest.Write([]byte(prefix))
	digest.Write([]byte{0})
	if local {
		digest.Write([]byte("true"))
	} else {
		digest.Write([]byte("false"))
	}
	digest.Write([]byte{0})
	digest.Write([]byte(endpoint))
	digest.Write([]byte{0})
	digest.Write(incarnation)
	digest.Write([]byte{0})
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(generation))
	digest.Write(encoded[:])
	digest.Write([]byte{0})
	digest.Write(lifecycle)
	return prefix + base64.RawURLEncoding.EncodeToString(digest.Sum(nil)[:16])
}

// pickerStableHostOrder keeps each host's first-seen position and appends new
// hosts in publication order, so an unrelated update never reshuffles rows.
func pickerStableHostOrder(previous []string, daemons []ports.BrokerDaemonObservation) []string {
	present := make(map[string]struct{}, len(daemons))
	published := make([]string, 0, len(daemons))
	for i := range daemons {
		key := pickerDaemonKey(daemons[i].Local, daemons[i].Endpoint)
		if _, ok := present[key]; ok {
			continue
		}
		present[key] = struct{}{}
		published = append(published, key)
	}
	order := make([]string, 0, len(published))
	seen := make(map[string]struct{}, len(published))
	for _, key := range previous {
		if _, ok := present[key]; !ok {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		order = append(order, key)
	}
	for _, key := range published {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		order = append(order, key)
	}
	return order
}

// pickerObservationCompatible reports whether the observed daemon can accept an
// attachment under its own configured policy. A version mismatch is derived by
// comparing the observation's ProtocolVersion against its Policy.ProtocolVersion,
// and only when the daemon was actually observed (ProtocolVersion != 0). An
// unobserved daemon carries no version and is never compatible; reachability is
// classified separately so an unobserved or unreachable daemon is never
// reported as a version mismatch.
func pickerObservationCompatible(observation ports.BrokerDaemonObservation) bool {
	return observation.ProtocolVersion != 0 && observation.ProtocolVersion == observation.Policy.ProtocolVersion
}

// pickerObservationKnownIncompatible reports whether a daemon is known to be
// unable to accept an attachment: either its availability was classified as
// incompatible by the observation itself, or it was observed with a protocol
// version that differs from its policy's required version. An unobserved or
// unreachable daemon is not known-incompatible, so an explicit bounded attempt
// stays possible; that distinction is what lets the picker start a stopped
// daemon instead of blocking behind observation.
func pickerObservationKnownIncompatible(observation ports.BrokerDaemonObservation) bool {
	return observation.Availability == domain.RemoteAvailabilityIncompatible || pickerObservationVersionMismatch(observation)
}

// pickerObservationVersionMismatch reports whether a daemon that was actually
// observed (ProtocolVersion != 0) reports a protocol version that differs from
// its policy's required version. Reachability and observation are classified
// separately, so an unobserved daemon is never a version mismatch.
func pickerObservationVersionMismatch(observation ports.BrokerDaemonObservation) bool {
	return observation.ProtocolVersion != 0 && observation.ProtocolVersion != observation.Policy.ProtocolVersion
}

// pickerSameIncarnation reports whether two observations describe the same
// daemon identity, so their observed state may be compared for monotonicity.
func pickerSameIncarnation(current, incoming ports.BrokerDaemonObservation) bool {
	return current.Incarnation == incoming.Incarnation
}

// pickerObservationNewer reports whether an incoming observation is at least as
// new as the current one for the same daemon identity. A different incarnation
// is an identity replacement and always wins.
func pickerObservationNewer(incoming, current ports.BrokerDaemonObservation) bool {
	if incoming.Incarnation != current.Incarnation {
		return true
	}
	if incoming.FailureEpisode != current.FailureEpisode {
		return incoming.FailureEpisode > current.FailureEpisode
	}
	return !incoming.LastSuccess.Before(current.LastSuccess)
}

// pickerMergeAuthority keeps the newer observed state while refreshing every
// field the registry stamps as configured authority.
func pickerMergeAuthority(current, incoming ports.BrokerDaemonObservation) ports.BrokerDaemonObservation {
	merged := current
	merged.Local = incoming.Local
	merged.Endpoint = incoming.Endpoint
	merged.DisplayOrigin = incoming.DisplayOrigin
	merged.Rank = incoming.Rank
	merged.Registration = incoming.Registration
	merged.Policy = incoming.Policy
	return merged
}

// pickerObservationStatus classifies one observation into a row badge using
// the shared domain.RemoteReason taxonomy: availability is decided first, so
// an unobserved (Availability Unknown) or unreachable daemon is never reported
// as a version mismatch. A version mismatch is derived only for a reachable
// observation whose observed ProtocolVersion differs from policy.
func pickerObservationStatus(observation ports.BrokerDaemonObservation, fresh bool) protocol.PickerLineStatus {
	switch observation.Availability {
	case domain.RemoteAvailabilityUnreachable, domain.RemoteAvailabilityAuthFailed:
		return protocol.PickerLineStatusDown
	case domain.RemoteAvailabilityInvalidResponse:
		return protocol.PickerLineStatusError
	case domain.RemoteAvailabilityIncompatible:
		return protocol.PickerLineStatusVersion
	case domain.RemoteAvailabilityNoDaemon:
		return protocol.PickerLineStatusNoDaemon
	case domain.RemoteAvailabilityReachable:
		if pickerObservationVersionMismatch(observation) {
			return protocol.PickerLineStatusVersion
		}
		if observation.ProtocolVersion == 0 || !fresh {
			return protocol.PickerLineStatusStale
		}
		return protocol.PickerLineStatusUp
	default:
		// Availability Unknown (unobserved) and any out-of-range value are
		// never treated as compatible.
		return protocol.PickerLineStatusStale
	}
}

// pickerObservationReason classifies one observation into a bounded display
// reason from the shared domain.RemoteReason taxonomy. Availability is decided
// first; the version branch applies only to a reachable observation whose
// observed version differs from policy. Unobserved and unknown availability
// render as refreshing.
func pickerObservationReason(observation ports.BrokerDaemonObservation, fresh bool) string {
	switch observation.Availability {
	case domain.RemoteAvailabilityUnreachable:
		return domain.RemoteReasonHostUnreachable
	case domain.RemoteAvailabilityAuthFailed:
		return domain.RemoteReasonAuthFailure
	case domain.RemoteAvailabilityInvalidResponse:
		return domain.RemoteReasonMalformed
	case domain.RemoteAvailabilityIncompatible:
		return domain.RemoteReasonVersionMismatch
	case domain.RemoteAvailabilityNoDaemon:
		return domain.RemoteReasonNoDaemon
	case domain.RemoteAvailabilityReachable:
		if pickerObservationVersionMismatch(observation) {
			return domain.RemoteReasonVersionMismatch
		}
		if observation.ProtocolVersion == 0 {
			return domain.RemoteReasonRefreshing
		}
		if observation.Checking {
			return domain.RemoteReasonRefreshing
		}
		if !fresh {
			return domain.RemoteReasonCatalogStale
		}
		return ""
	default:
		// Availability Unknown is the unobserved state: it is still being
		// discovered, so it renders as refreshing rather than as a failure.
		return domain.RemoteReasonRefreshing
	}
}

func pickerHostDetail(observation ports.BrokerDaemonObservation) string {
	if !observation.InventoryKnown {
		return "catalogue pending"
	}
	return fmt.Sprintf("%d sessions", len(observation.Sessions))
}

func pickerSessionStatus(session catalogue.RemoteCatalogSession) protocol.PickerLineStatus {
	switch session.State {
	case catalogue.RemoteCatalogSessionUp:
		return protocol.PickerLineStatusUp
	case catalogue.RemoteCatalogSessionDown:
		return protocol.PickerLineStatusStopped
	case catalogue.RemoteCatalogSessionBroken:
		return protocol.PickerLineStatusError
	default:
		return protocol.PickerLineStatusNone
	}
}

func pickerSessionDetail(session catalogue.RemoteCatalogSession) string {
	parts := make([]string, 0, 2)
	if session.Attached {
		parts = append(parts, "attached")
	}
	if count := len(session.Tabs); count > 0 {
		parts = append(parts, fmt.Sprintf("%d tabs", count))
	}
	return strings.Join(parts, " · ")
}

// pickerCatalogueCursor hints the first actionable row, then the first
// inspectable row, so a fresh picker always has a cursor destination when one
// exists.
func pickerCatalogueCursor(lines []protocol.PickerLine) protocol.PickerCursor {
	for i, line := range lines {
		if line.Key != "" && line.Focusable && line.Actions != 0 {
			return protocol.PickerCursor{Key: line.Key, Index: i}
		}
	}
	for i, line := range lines {
		if line.Key != "" && line.Focusable {
			return protocol.PickerCursor{Key: line.Key, Index: i}
		}
	}
	return protocol.PickerCursor{Index: -1}
}

func pickerFindSession(sessions []catalogue.RemoteCatalogSession, lifecycle domain.SessionLifecycleID) (catalogue.RemoteCatalogSession, bool) {
	for _, session := range sessions {
		if session.LifecycleID == lifecycle {
			return session, true
		}
	}
	return catalogue.RemoteCatalogSession{}, false
}

// pickerSessionReplaced reports whether the exact lifecycle vanished but a
// same-name session remains, which is a replacement rather than a removal.
func pickerSessionReplaced(sessions []catalogue.RemoteCatalogSession, ref pickerSelectionRef) bool {
	if ref.name == "" {
		return false
	}
	for _, session := range sessions {
		if session.LifecycleID != ref.lifecycle && session.Name == ref.name {
			return true
		}
	}
	return false
}
