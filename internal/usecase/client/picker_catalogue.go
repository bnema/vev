package client

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Client-owned picker catalogue (Plan 001 P5.2b, offline and unactivated).
//
// The autonomous supervisor owns the broker connection and the single terminal
// input lifetime; this file owns the catalogue the picker presents, projected
// from the broker's own observations rather than from a serving-daemon
// catalogue. It is the P5.2b replacement source for the daemon-served
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
	// pickerCatalogueDefaultFreshness bounds how old a LastSuccess may be
	// before a row is displayed as stale and refused at resolution.
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
	pickerCatalogueStale
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
	case pickerCatalogueStale:
		return "stale"
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
}

func (e pickerCatalogueError) Error() string {
	if e.Text == "" {
		return "vev: picker selection " + e.Code.String()
	}
	return "vev: picker selection " + e.Code.String() + ": " + e.Text
}

// pickerCatalogueErrorIs reports whether err is a typed catalogue refusal with
// the supplied code, so callers classify without matching message text.
func pickerCatalogueErrorIs(err error, code pickerCatalogueErrorCode) bool {
	var typed pickerCatalogueError
	return errors.As(err, &typed) && typed.Code == code
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
	lines        []protocol.PickerLine
	cursor       protocol.PickerCursor
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

// projection returns the current rows and cursor hint together.
func (c *pickerCatalogue) projection() ([]protocol.PickerLine, protocol.PickerCursor) {
	if c == nil {
		return nil, protocol.PickerCursor{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]protocol.PickerLine(nil), c.lines...), c.cursor
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

// ResolveCreation revalidates a named or ephemeral creation against the latest
// snapshot. The created identity is not invented: the daemon must be present,
// compatible, available, and fresh, and a named creation needs a valid session
// name.
func (c *pickerCatalogue) ResolveCreation(local bool, endpoint string, kind pickerSelectionKind, name string, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	if c == nil {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "no catalogue"}
	}
	if kind != pickerSelectionCreateNamed && kind != pickerSelectionCreateEphemeral {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "not a creation selection"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ref := pickerSelectionRef{kind: kind, epoch: c.snapshot.Epoch, local: local, endpoint: endpoint, createName: name}
	if !local {
		host := c.hostForEndpointLocked(endpoint)
		if host == nil {
			return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueGone, Text: "host is no longer in the catalogue"}
		}
		ref.registration = host.observation.Registration
	}
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

// projectLocked rebuilds the ordered rows and their opaque keys from the
// current host state. It is deterministic: the same inputs always produce the
// same keys, order, and actions.
func (c *pickerCatalogue) projectLocked() {
	now := c.clock.Now()
	refs := make(map[string]pickerSelectionRef)
	lines := make([]protocol.PickerLine, 0, len(c.hosts)*2)
	for _, key := range c.order {
		host, ok := c.hosts[key]
		if !ok {
			continue
		}
		observation := host.observation
		fresh := c.freshLocked(observation, now)
		compatible := pickerObservationCompatible(observation)
		available := observation.Availability == domain.RemoteAvailabilityReachable
		eligible := compatible && available && fresh
		origin := pickerOriginLabel(observation)

		lines = append(lines, protocol.PickerLine{Kind: protocol.PickerLineSection, Label: origin, Dim: true})
		lines = append(lines, protocol.PickerLine{
			Key:          pickerHostRowKey(observation),
			Kind:         protocol.PickerLineHost,
			Label:        origin,
			Detail:       pickerHostDetail(observation),
			Status:       pickerObservationStatus(observation, fresh),
			StatusDetail: pickerObservationReason(observation, fresh),
			Focusable:    true,
		})

		for i := range observation.Sessions {
			session := observation.Sessions[i]
			if session.LifecycleID == (domain.SessionLifecycleID{}) {
				// A session without a lifecycle cannot be addressed exactly and
				// is never invented into one.
				continue
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
			rowKey := pickerSessionRowKey(ref)
			refs[rowKey] = ref
			selectable := eligible && session.State != catalogue.RemoteCatalogSessionBroken
			actions := protocol.PickerLineActions(0)
			if selectable {
				actions = protocol.PickerCanNavigate
			}
			reason := pickerObservationReason(observation, fresh)
			if reason == "" {
				reason = session.Reason
			}
			lines = append(lines, protocol.PickerLine{
				Key:          rowKey,
				Kind:         protocol.PickerLineSession,
				Label:        session.Name,
				Detail:       pickerSessionDetail(session),
				Status:       pickerSessionStatus(session),
				StatusDetail: reason,
				Stopped:      session.State == catalogue.RemoteCatalogSessionDown,
				Dim:          !selectable,
				Focusable:    true,
				Actions:      actions,
				Ephemeral:    session.Ephemeral,
			})
		}
	}
	c.moveRetiredLocked(refs)
	c.refs = refs
	c.lines = lines
	c.cursor = pickerCatalogueCursor(lines)
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

func (c *pickerCatalogue) resolveRefLocked(ref pickerSelectionRef, base pickerResolveBase) (ports.BrokerOpenStreamRequest, error) {
	if c.snapshot.Epoch == 0 {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "broker catalogue is unavailable"}
	}
	if ref.epoch != 0 && ref.epoch != c.snapshot.Epoch {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueEpochStale, Text: "broker epoch changed"}
	}
	host := c.hostForRefLocked(ref)
	if host == nil {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueGone, Text: "host is no longer in the catalogue"}
	}
	observation := host.observation
	if !ref.local && observation.Registration.Incarnation != ref.registration.Incarnation {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueReplaced, Text: "host identity was replaced"}
	}
	// Availability is decided before compatibility: an unobserved or
	// unreachable daemon refuses as unavailable, never as a version mismatch.
	// Only the legacy incompatible availability state, or a reachable daemon
	// whose observed version differs from the policy, is incompatible.
	switch {
	case observation.Availability == domain.RemoteAvailabilityIncompatible:
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueIncompatible, Text: "host protocol is incompatible"}
	case observation.Availability != domain.RemoteAvailabilityReachable:
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "host is unavailable"}
	case observation.ProtocolVersion == 0:
		// Reachable but unobserved: still never reported as a version mismatch.
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "host is unavailable"}
	case !pickerObservationCompatible(observation):
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueIncompatible, Text: "host protocol is incompatible"}
	}
	if !c.freshLocked(observation, c.clock.Now()) {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueStale, Text: "host observation is stale"}
	}

	request := ports.BrokerOpenStreamRequest{
		Epoch:        c.snapshot.Epoch,
		Purpose:      ports.BrokerStreamAttachment,
		Local:        ref.local,
		Connection:   base.Connection,
		Stream:       base.Stream,
		Registration: observation.Registration,
		Policy:       observation.Policy,
		Env:          append([]string(nil), base.Env...),
	}
	if !ref.local {
		request.Endpoint = observation.Endpoint
	}
	switch ref.kind {
	case pickerSelectionExact:
		session, ok := pickerFindSession(observation.Sessions, ref.lifecycle)
		if !ok {
			if pickerSessionReplaced(observation.Sessions, ref) {
				return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueReplaced, Text: "session identity was replaced"}
			}
			return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueGone, Text: "session is no longer in the catalogue"}
		}
		if session.State == catalogue.RemoteCatalogSessionBroken {
			return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnavailable, Text: "session is broken"}
		}
		request.Admission = ports.BrokerAdmissionExact
		request.Target = protocol.ExactSessionTarget{LifecycleID: session.LifecycleID, SessionName: session.Name}
	case pickerSelectionCreateNamed:
		if err := domain.ValidateSessionName(ref.createName); err != nil {
			return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueInvalidName, Text: "session name is not valid"}
		}
		request.Admission = ports.BrokerAdmissionCreateNamed
		request.Name = ref.createName
	case pickerSelectionCreateEphemeral:
		request.Admission = ports.BrokerAdmissionCreateEphemeral
	default:
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "picker selection kind is unknown"}
	}
	if err := request.Validate(); err != nil {
		return ports.BrokerOpenStreamRequest{}, pickerCatalogueError{Code: pickerCatalogueUnknown, Text: "picker stream request is not valid"}
	}
	return request, nil
}

func (c *pickerCatalogue) hostForRefLocked(ref pickerSelectionRef) *pickerHostState {
	if ref.local {
		for _, host := range c.hosts {
			if host.observation.Local {
				return host
			}
		}
		return nil
	}
	return c.hostForEndpointLocked(ref.endpoint)
}

func (c *pickerCatalogue) hostForEndpointLocked(endpoint string) *pickerHostState {
	for _, host := range c.hosts {
		if !host.observation.Local && host.observation.Endpoint == endpoint {
			return host
		}
	}
	return nil
}

// freshLocked reports whether LastSuccess is inside the freshness window. An
// observation that never succeeded is never fresh.
func (c *pickerCatalogue) freshLocked(observation ports.BrokerDaemonObservation, now time.Time) bool {
	if observation.LastSuccess.IsZero() {
		return false
	}
	return now.Sub(observation.LastSuccess) <= c.freshness
}

// pickerHostDiagnostic is one bounded, presentation-safe catalogue observation
// the controller surfaces as a toast.
type pickerHostDiagnostic struct {
	Key     string
	Origin  string
	Status  protocol.PickerLineStatus
	Message string
}

// diagnostics reports every projected host that is not a clean, fresh,
// compatible, reachable daemon, so the controller can surface it.
func (c *pickerCatalogue) diagnostics() []pickerHostDiagnostic {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	diagnostics := make([]pickerHostDiagnostic, 0, len(c.hosts))
	for _, key := range c.order {
		host, ok := c.hosts[key]
		if !ok {
			continue
		}
		observation := host.observation
		fresh := c.freshLocked(observation, now)
		if pickerObservationCompatible(observation) && observation.Availability == domain.RemoteAvailabilityReachable && fresh && !observation.Checking {
			continue
		}
		diagnostics = append(diagnostics, pickerHostDiagnostic{
			Key:     pickerHostRowKey(observation),
			Origin:  pickerOriginLabel(observation),
			Status:  pickerObservationStatus(observation, fresh),
			Message: pickerObservationReason(observation, fresh),
		})
	}
	return diagnostics
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
	return pickerRowKey("h", observation.Local, observation.Endpoint, observation.Registration.Incarnation[:], nil)
}

// pickerSessionRowKey is the stable, opaque key of one exact session. It is
// derived only from identity (endpoint, registration incarnation, session
// lifecycle), so a rename never changes it and a replaced lifecycle always
// does.
func pickerSessionRowKey(ref pickerSelectionRef) string {
	return pickerRowKey("s", ref.local, ref.endpoint, ref.registration.Incarnation[:], ref.lifecycle[:])
}

func pickerRowKey(prefix string, local bool, endpoint string, incarnation, lifecycle []byte) string {
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

// pickerObservationStatus classifies one observation into a row badge. It
// mirrors the legacy daemon-served classifier (directorySessionReason):
// availability is decided first, so an unobserved (Availability Unknown) or
// unreachable daemon is never reported as a version mismatch. A version
// mismatch is derived only for a reachable observation whose observed
// ProtocolVersion differs from its policy's required version.
func pickerObservationStatus(observation ports.BrokerDaemonObservation, fresh bool) protocol.PickerLineStatus {
	switch observation.Availability {
	case domain.RemoteAvailabilityUnreachable, domain.RemoteAvailabilityAuthFailed:
		return protocol.PickerLineStatusDown
	case domain.RemoteAvailabilityInvalidResponse:
		return protocol.PickerLineStatusError
	case domain.RemoteAvailabilityIncompatible:
		return protocol.PickerLineStatusVersion
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
// reason. As with the badge, availability is decided first; the version branch
// applies only to a reachable observation whose observed version differs from
// policy. Unobserved and unknown availability render as refreshing, matching
// the legacy daemon-served classifier.
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
