// Stateful multipart snapshot assembly.
//
// SnapshotAssembler stages one multipart SnapshotPart publication and
// publishes it atomically only after full validation. It builds on the
// stateless codec: parts are already strict-scanned and semantically
// converted, so this file owns only transfer state: exact indexes, counts,
// order, staged bounds, revision fencing, generation fencing, and atomic
// publication.
//
// Transfer layout (indexes are exact positions):
//
//	Begin(0) Daemon/Session... Tombstone... End(N-1)
//
// Begin carries the total daemon, session, and tombstone counts plus whether
// the local daemon is present. Daemon parts arrive in host_index order, with
// the local daemon (when present) first at index 0 and remotes following in
// registration order; each Daemon advertises how many Session parts follow it
// immediately, with per-daemon session_index order. A session of the local
// daemon carries Local=true and host_index 0. Tombstones arrive in
// tombstone_index order after all daemons and sessions. End closes the
// transfer. Total parts are 1 + daemons + sessions + tombstones + 1, bounded
// by MaxSnapshotParts (65 daemons, 65*256 sessions, 64 tombstones).
//
// Bounds: at most MaxSnapshotParts parts and MaxSnapshotStagedBytes staged
// estimate per transfer. The staged estimate is a deterministic function of
// endpoint, origin, policy, identity, session, tab, and tombstone text lengths
// plus fixed per-part overhead; it bounds hostile memory without measuring
// wire bytes.
//
// Publication is atomic: the committed snapshot is replaced only when End
// arrives with exact indexes and the assembled BrokerSnapshot passes
// ports.BrokerSnapshot.Validate plus, for every remote daemon,
// ports.ValidateDurableHostProjection. Any malformed index, count, order, or
// validation failure aborts staging and retains the old snapshot. A new Begin
// for the same generation with a newer revision coalesces by replacing
// staging; an older revision is rejected and retains both the committed
// snapshot and any staging in flight. Equal revisions are allowed so a resync
// may republish the committed revision.
//
// Generation fencing: the assembler belongs to exactly one nonzero
// subscription generation, adopted from the first Begin or pinned by
// WithGeneration. Parts with a stale generation are ignored without touching
// staging; parts with a future generation are rejected, whether or not a
// transfer is in flight. Scope fencing is exact: every part must carry the
// assembler's epoch and connection.
//
// There is no I/O, socket, pump, or transport here. The owning
// Connection discards staging on generation advance, unsubscribe, or close.
package brokerwire

import (
	"errors"
	"sync"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

const (
	// MaxSnapshotParts bounds one multipart publication: 1 Begin + 65 daemons
	// + 65*256 sessions + 64 tombstones + 1 End.
	MaxSnapshotParts = 1 + ports.BrokerMaxDaemonsPerSnapshot + ports.BrokerMaxDaemonsPerSnapshot*ports.BrokerMaxSessionsPerHost + ports.BrokerMaxTombstones + 1
	// MaxSnapshotStagedBytes bounds the deterministic staged estimate of
	// one transfer (80 MiB).
	MaxSnapshotStagedBytes = uint64(80 << 20)
)

var (
	// ErrSnapshotInvalid reports a malformed transfer: wrong index, count,
	// order, payload type, or assembled validation failure. Staging is
	// aborted and the committed snapshot is retained.
	ErrSnapshotInvalid = errors.New("brokerwire: invalid snapshot transfer")
	// ErrSnapshotStale reports an older revision: below the committed
	// revision or below staging. Nothing changes.
	ErrSnapshotStale = errors.New("brokerwire: stale snapshot revision")
	// ErrStaleGeneration reports a part bound to a superseded
	// subscription. It is ignored without touching staging.
	ErrStaleGeneration = errors.New("brokerwire: stale subscription generation")
	// ErrFutureGeneration reports a part bound to a generation past the
	// active one. It is rejected.
	ErrFutureGeneration = errors.New("brokerwire: future subscription generation")
	// ErrInvalidGeneration reports a zero subscription generation.
	ErrInvalidGeneration = errors.New("brokerwire: invalid subscription generation")
	// ErrScopeMismatch reports a part whose epoch or connection does not
	// exactly match the assigned scope. It is rejected.
	ErrScopeMismatch = errors.New("brokerwire: scope mismatch")
)

// SnapshotOption tunes a SnapshotAssembler. Tests use a small staged cap
// to prove the bound without allocating 80 MiB; production uses defaults.
type SnapshotOption func(*SnapshotAssembler)

// WithGeneration pins the subscription generation this assembler belongs to,
// so a part of any other generation is fenced from the first one. The zero
// default adopts the generation from the first Begin.
func WithGeneration(generation uint64) SnapshotOption {
	return func(a *SnapshotAssembler) {
		a.generation = generation
	}
}

// SnapshotAssembler stages one multipart publication per generation and
// holds the last committed snapshot. It is safe for concurrent use.
type SnapshotAssembler struct {
	mu             sync.Mutex
	epoch          ports.BrokerEpoch
	connection     ports.BrokerConnectionID
	generation     SubscriptionGeneration
	committed      ports.BrokerSnapshot
	hasCommitted   bool
	staging        *snapshotStaging
	maxStagedBytes uint64
}

type snapshotStaging struct {
	generation     SubscriptionGeneration
	revision       ports.BrokerRevision
	hostCount      uint32
	sessionTotal   uint32
	tombstoneCount uint32
	localPresent   bool
	totalParts     uint32
	nextIndex      uint32
	daemons        []ports.BrokerDaemonObservation
	daemonExpected []uint32
	daemonSeen     []uint32
	daemonsSeen    uint32
	sessionsSeen   uint32
	tombstones     []ports.BrokerHostTombstone
	tombstonesSeen uint32
	stagedBytes    uint64
}

// newSnapshotStaging allocates one transfer's staging from a validated Begin.
func newSnapshotStaging(generation SubscriptionGeneration, revision ports.BrokerRevision, begin SnapshotBegin) *snapshotStaging {
	total := 1 + uint64(begin.HostCount) + uint64(begin.SessionCount) + uint64(begin.TombstoneCount) + 1
	return &snapshotStaging{
		generation:     generation,
		revision:       revision,
		hostCount:      begin.HostCount,
		sessionTotal:   begin.SessionCount,
		tombstoneCount: begin.TombstoneCount,
		localPresent:   begin.LocalPresent,
		totalParts:     uint32(total),
		nextIndex:      1,
		daemons:        make([]ports.BrokerDaemonObservation, begin.HostCount),
		daemonExpected: make([]uint32, begin.HostCount),
		daemonSeen:     make([]uint32, begin.HostCount),
		tombstones:     make([]ports.BrokerHostTombstone, begin.TombstoneCount),
	}
}

// NewSnapshotAssembler binds an assembler to one exact scope assigned by
// Registered. Every part must carry that scope.
func NewSnapshotAssembler(epoch ports.BrokerEpoch, connection ports.BrokerConnectionID, opts ...SnapshotOption) *SnapshotAssembler {
	a := &SnapshotAssembler{epoch: epoch, connection: connection, maxStagedBytes: MaxSnapshotStagedBytes}
	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}
	if a.maxStagedBytes == 0 {
		a.maxStagedBytes = MaxSnapshotStagedBytes
	}
	return a
}

// Snapshot returns the last committed snapshot and whether one exists.
// The returned value is a defensive clone.
func (a *SnapshotAssembler) Snapshot() (ports.BrokerSnapshot, bool) {
	if a == nil {
		return ports.BrokerSnapshot{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.hasCommitted {
		return ports.BrokerSnapshot{}, false
	}
	return a.committed.Clone(), true
}

// AdoptCommitted seeds the assembler with a snapshot already committed for
// the same scope, so a subscription generation advance keeps publishing the
// last committed revision until a newer transfer commits. An older revision
// or a foreign epoch is ignored, and no transfer in flight is disturbed.
func (a *SnapshotAssembler) adoptCommitted(snapshot ports.BrokerSnapshot) {
	if a == nil || snapshot.Epoch != a.epoch || snapshot.Revision == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.hasCommitted && snapshot.Revision < a.committed.Revision {
		return
	}
	a.committed = snapshot.Clone()
	a.hasCommitted = true
}

// StagingActive reports whether a transfer is in flight.
func (a *SnapshotAssembler) StagingActive() bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.staging != nil
}

// DiscardStaging abandons any transfer in flight and retains the committed
// snapshot.
func (a *SnapshotAssembler) DiscardStaging() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.staging = nil
}

// Add stages one SnapshotPart. It returns completed=true with the newly
// committed clone only when End validates and publishes. Stale generations
// are ignored, future generations rejected, older revisions rejected, and
// malformed transfers aborted; all retain the committed snapshot.
func (a *SnapshotAssembler) Add(part SnapshotPart) (bool, ports.BrokerSnapshot, error) {
	if a == nil {
		return false, ports.BrokerSnapshot{}, ErrSnapshotInvalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if part.Epoch != a.epoch || part.Connection != a.connection {
		return false, ports.BrokerSnapshot{}, ErrScopeMismatch
	}
	if part.Generation == 0 {
		return false, ports.BrokerSnapshot{}, ErrInvalidGeneration
	}
	// Generation fencing holds for the assembler's whole lifetime: the
	// first Begin adopts the generation, and every later part must carry
	// it even when no transfer is staged.
	if a.generation != 0 && part.Generation != a.generation {
		if part.Generation < a.generation {
			return false, ports.BrokerSnapshot{}, ErrStaleGeneration
		}
		return false, ports.BrokerSnapshot{}, ErrFutureGeneration
	}
	if part.Revision == 0 {
		return false, ports.BrokerSnapshot{}, ErrSnapshotInvalid
	}
	if a.staging == nil {
		begin, ok := normalizeBegin(part.Part)
		if !ok {
			return false, ports.BrokerSnapshot{}, ErrSnapshotInvalid
		}
		if part.Index != 0 {
			return false, ports.BrokerSnapshot{}, ErrSnapshotInvalid
		}
		if err := checkBeginCounts(begin); err != nil {
			return false, ports.BrokerSnapshot{}, err
		}
		if a.hasCommitted && part.Revision < a.committed.Revision {
			return false, ports.BrokerSnapshot{}, ErrSnapshotStale
		}
		staging := newSnapshotStaging(part.Generation, part.Revision, begin)
		staged := estimatePartBytes(part)
		if staged > a.maxStagedBytes {
			return false, ports.BrokerSnapshot{}, ErrTooLarge
		}
		staging.stagedBytes = staged
		a.generation = part.Generation
		a.staging = staging
		return false, ports.BrokerSnapshot{}, nil
	}
	staging := a.staging
	if part.Generation != staging.generation {
		if part.Generation < staging.generation {
			return false, ports.BrokerSnapshot{}, ErrStaleGeneration
		}
		return false, ports.BrokerSnapshot{}, ErrFutureGeneration
	}
	if begin, ok := normalizeBegin(part.Part); ok {
		if part.Index != 0 {
			a.staging = nil
			return false, ports.BrokerSnapshot{}, ErrSnapshotInvalid
		}
		if err := checkBeginCounts(begin); err != nil {
			a.staging = nil
			return false, ports.BrokerSnapshot{}, err
		}
		switch {
		case part.Revision == staging.revision:
			// Restart of the same transfer: discard progress.
		case part.Revision > staging.revision:
			if a.hasCommitted && part.Revision < a.committed.Revision {
				return false, ports.BrokerSnapshot{}, ErrSnapshotStale
			}
		default:
			return false, ports.BrokerSnapshot{}, ErrSnapshotStale
		}
		fresh := newSnapshotStaging(part.Generation, part.Revision, begin)
		staged := estimatePartBytes(part)
		if staged > a.maxStagedBytes {
			a.staging = nil
			return false, ports.BrokerSnapshot{}, ErrTooLarge
		}
		fresh.stagedBytes = staged
		a.staging = fresh
		return false, ports.BrokerSnapshot{}, nil
	}
	if part.Revision != staging.revision {
		if part.Revision < staging.revision {
			return false, ports.BrokerSnapshot{}, ErrSnapshotStale
		}
		a.staging = nil
		return false, ports.BrokerSnapshot{}, ErrSnapshotInvalid
	}
	if part.Index != staging.nextIndex {
		a.staging = nil
		return false, ports.BrokerSnapshot{}, ErrSnapshotInvalid
	}
	if uint64(part.Index) >= uint64(staging.totalParts) {
		a.staging = nil
		return false, ports.BrokerSnapshot{}, ErrSnapshotInvalid
	}
	staged := staging.stagedBytes + estimatePartBytes(part)
	if staged > a.maxStagedBytes {
		a.staging = nil
		return false, ports.BrokerSnapshot{}, ErrTooLarge
	}
	switch payload := part.Part.(type) {
	case SnapshotDaemonPart, *SnapshotDaemonPart:
		daemonPart := normalizeDaemonPart(payload)
		if err := a.stageDaemon(staging, daemonPart); err != nil {
			a.staging = nil
			return false, ports.BrokerSnapshot{}, err
		}
	case SnapshotSessionPart, *SnapshotSessionPart:
		sessionPart := normalizeSessionPart(payload)
		if err := a.stageSession(staging, sessionPart); err != nil {
			a.staging = nil
			return false, ports.BrokerSnapshot{}, err
		}
	case SnapshotTombstonePart, *SnapshotTombstonePart:
		tombPart := normalizeTombstonePart(payload)
		if err := a.stageTombstone(staging, tombPart); err != nil {
			a.staging = nil
			return false, ports.BrokerSnapshot{}, err
		}
	case SnapshotEnd, *SnapshotEnd:
		snapshot, err := a.commitStaging(staging)
		if err != nil {
			a.staging = nil
			return false, ports.BrokerSnapshot{}, err
		}
		staging.stagedBytes = staged
		a.committed = snapshot
		a.hasCommitted = true
		a.staging = nil
		return true, snapshot.Clone(), nil
	default:
		a.staging = nil
		return false, ports.BrokerSnapshot{}, ErrSnapshotInvalid
	}
	staging.stagedBytes = staged
	staging.nextIndex++
	return false, ports.BrokerSnapshot{}, nil
}

func normalizeBegin(payload SnapshotPartPayload) (SnapshotBegin, bool) {
	switch p := payload.(type) {
	case SnapshotBegin:
		return p, true
	case *SnapshotBegin:
		if p == nil {
			return SnapshotBegin{}, false
		}
		return *p, true
	default:
		return SnapshotBegin{}, false
	}
}

func normalizeDaemonPart(payload SnapshotPartPayload) SnapshotDaemonPart {
	switch p := payload.(type) {
	case SnapshotDaemonPart:
		return p
	case *SnapshotDaemonPart:
		if p == nil {
			return SnapshotDaemonPart{HostIndex: ^uint32(0)}
		}
		return *p
	default:
		return SnapshotDaemonPart{HostIndex: ^uint32(0)}
	}
}

func normalizeSessionPart(payload SnapshotPartPayload) SnapshotSessionPart {
	switch p := payload.(type) {
	case SnapshotSessionPart:
		return p
	case *SnapshotSessionPart:
		if p == nil {
			return SnapshotSessionPart{HostIndex: ^uint32(0)}
		}
		return *p
	default:
		return SnapshotSessionPart{HostIndex: ^uint32(0)}
	}
}

func normalizeTombstonePart(payload SnapshotPartPayload) SnapshotTombstonePart {
	switch p := payload.(type) {
	case SnapshotTombstonePart:
		return p
	case *SnapshotTombstonePart:
		if p == nil {
			return SnapshotTombstonePart{TombstoneIndex: ^uint32(0)}
		}
		return *p
	default:
		return SnapshotTombstonePart{TombstoneIndex: ^uint32(0)}
	}
}

func checkBeginCounts(begin SnapshotBegin) error {
	if begin.HostCount > uint32(ports.BrokerMaxDaemonsPerSnapshot) ||
		begin.SessionCount > uint32(ports.BrokerMaxDaemonsPerSnapshot*ports.BrokerMaxSessionsPerHost) ||
		begin.TombstoneCount > uint32(ports.BrokerMaxTombstones) {
		return ErrTooLarge
	}
	// A local daemon occupies daemon index 0, so it can only be present when
	// at least one daemon part follows.
	if begin.LocalPresent && begin.HostCount == 0 {
		return ErrSnapshotInvalid
	}
	return nil
}

func (a *SnapshotAssembler) stageDaemon(staging *snapshotStaging, part SnapshotDaemonPart) error {
	if staging.daemonsSeen >= staging.hostCount {
		return ErrSnapshotInvalid
	}
	if part.HostIndex != staging.daemonsSeen {
		return ErrSnapshotInvalid
	}
	if part.HostIndex >= staging.hostCount {
		return ErrSnapshotInvalid
	}
	if part.SessionCount > uint32(ports.BrokerMaxSessionsPerHost) {
		return ErrTooLarge
	}
	if len(part.Daemon.Sessions) != 0 {
		return ErrSnapshotInvalid
	}
	// The local daemon is always first: daemon index 0 is local exactly when
	// Begin advertised it, and no later daemon may be local.
	if part.HostIndex == 0 {
		if part.Daemon.Local != staging.localPresent {
			return ErrSnapshotInvalid
		}
	} else if part.Daemon.Local {
		return ErrSnapshotInvalid
	}
	var seen uint64
	for _, n := range staging.daemonExpected[:staging.daemonsSeen] {
		seen += uint64(n)
	}
	if seen+uint64(part.SessionCount) > uint64(staging.sessionTotal) {
		return ErrSnapshotInvalid
	}
	// Remaining daemons must still be able to fill the advertised total:
	// even if every remaining daemon carried the per-daemon maximum, the
	// total must stay reachable. This is checked exactly at End; here we
	// only refuse an already-exceeded sum.
	staging.daemons[part.HostIndex] = part.Daemon.Clone()
	staging.daemonExpected[part.HostIndex] = part.SessionCount
	staging.daemonSeen[part.HostIndex] = 0
	staging.daemonsSeen++
	return nil
}

func (a *SnapshotAssembler) stageSession(staging *snapshotStaging, part SnapshotSessionPart) error {
	if staging.daemonsSeen == 0 || staging.daemonsSeen > staging.hostCount {
		return ErrSnapshotInvalid
	}
	current := staging.daemonsSeen - 1
	if part.HostIndex != current {
		return ErrSnapshotInvalid
	}
	if current >= staging.hostCount {
		return ErrSnapshotInvalid
	}
	expected := staging.daemonExpected[current]
	if staging.daemonSeen[current] >= expected {
		return ErrSnapshotInvalid
	}
	if part.SessionIndex != staging.daemonSeen[current] {
		return ErrSnapshotInvalid
	}
	// A session belongs to the daemon it follows: a local session binds to
	// the local daemon at index 0, and a remote session to a remote daemon.
	if part.Local != staging.daemons[current].Local {
		return ErrSnapshotInvalid
	}
	if part.HostIndex >= uint32(ports.BrokerMaxDaemonsPerSnapshot) || part.SessionIndex >= uint32(ports.BrokerMaxSessionsPerHost) {
		return ErrTooLarge
	}
	if staging.sessionsSeen >= staging.sessionTotal {
		return ErrSnapshotInvalid
	}
	session := part.Session
	if session.Tabs == nil {
		return ErrSnapshotInvalid
	}
	tabs := make([]catalogue.RemoteCatalogTab, len(session.Tabs))
	copy(tabs, session.Tabs)
	session.Tabs = tabs
	daemon := staging.daemons[current]
	daemon.Sessions = append(daemon.Sessions, session)
	staging.daemons[current] = daemon
	staging.daemonSeen[current]++
	staging.sessionsSeen++
	return nil
}

func (a *SnapshotAssembler) stageTombstone(staging *snapshotStaging, part SnapshotTombstonePart) error {
	if staging.daemonsSeen != staging.hostCount || staging.sessionsSeen != staging.sessionTotal {
		return ErrSnapshotInvalid
	}
	if staging.tombstonesSeen >= staging.tombstoneCount {
		return ErrSnapshotInvalid
	}
	if part.TombstoneIndex != staging.tombstonesSeen {
		return ErrSnapshotInvalid
	}
	if part.RetiredRevision == 0 {
		return ErrSnapshotInvalid
	}
	staging.tombstones[part.TombstoneIndex] = ports.BrokerHostTombstone{
		Endpoint:        part.Registration.Endpoint,
		Registration:    part.Registration,
		RetiredRevision: part.RetiredRevision,
	}
	staging.tombstonesSeen++
	return nil
}

func (a *SnapshotAssembler) commitStaging(staging *snapshotStaging) (ports.BrokerSnapshot, error) {
	if staging.daemonsSeen != staging.hostCount || staging.sessionsSeen != staging.sessionTotal || staging.tombstonesSeen != staging.tombstoneCount {
		return ports.BrokerSnapshot{}, ErrSnapshotInvalid
	}
	if staging.nextIndex != staging.totalParts-1 {
		return ports.BrokerSnapshot{}, ErrSnapshotInvalid
	}
	var sum uint64
	for _, n := range staging.daemonExpected {
		sum += uint64(n)
	}
	if sum != uint64(staging.sessionTotal) {
		return ports.BrokerSnapshot{}, ErrSnapshotInvalid
	}
	daemons := make([]ports.BrokerDaemonObservation, len(staging.daemons))
	for i, daemon := range staging.daemons {
		daemons[i] = daemon.Clone()
		if daemons[i].Sessions == nil {
			daemons[i].Sessions = []catalogue.RemoteCatalogSession{}
		}
	}
	removed := append([]ports.BrokerHostTombstone(nil), staging.tombstones...)
	snapshot := ports.BrokerSnapshot{Epoch: a.epoch, Revision: staging.revision, Daemons: daemons, Removed: removed}
	if err := snapshot.Validate(); err != nil {
		return ports.BrokerSnapshot{}, ErrSnapshotInvalid
	}
	for _, daemon := range daemons {
		// The local daemon is process-local state and is never durable.
		if daemon.Local {
			continue
		}
		if err := ports.ValidateDurableHostProjection(daemon); err != nil {
			return ports.BrokerSnapshot{}, ErrSnapshotInvalid
		}
	}
	return snapshot, nil
}

func estimatePartBytes(part SnapshotPart) uint64 {
	const overhead = 32
	switch payload := part.Part.(type) {
	case SnapshotBegin, *SnapshotBegin:
		return overhead
	case SnapshotDaemonPart:
		return estimateDaemonBytes(payload.Daemon) + overhead
	case *SnapshotDaemonPart:
		if payload == nil {
			return overhead
		}
		return estimateDaemonBytes(payload.Daemon) + overhead
	case SnapshotSessionPart:
		return estimateCatalogSessionBytes(payload.Session) + overhead
	case *SnapshotSessionPart:
		if payload == nil {
			return overhead
		}
		return estimateCatalogSessionBytes(payload.Session) + overhead
	case SnapshotTombstonePart:
		return uint64(len(payload.Registration.Endpoint)+32) + overhead
	case *SnapshotTombstonePart:
		if payload == nil {
			return overhead
		}
		return uint64(len(payload.Registration.Endpoint)+32) + overhead
	case SnapshotEnd, *SnapshotEnd:
		return overhead
	default:
		return overhead
	}
}

// estimateDaemonBytes bounds one daemon projection's staged memory: every
// configured-authority and observed-identity string plus a fixed per-daemon
// charge.
func estimateDaemonBytes(daemon ports.BrokerDaemonObservation) uint64 {
	text := len(daemon.Endpoint) + len(daemon.DisplayOrigin) + len(daemon.Registration.Endpoint) +
		len(daemon.Policy.Transport) + len(daemon.Policy.Trust) + len(daemon.Policy.Launch) + len(daemon.Policy.Isolation) +
		len(daemon.Identity)
	return uint64(text + 64)
}

// estimateCatalogSessionBytes bounds one session's staged memory. It charges
// every tab at least estimateTabBytes (covering the tab struct, its string
// headers, and its slice slot) plus the variable-length text, and adds one
// slice-header charge when the tab list is present. This deliberately
// over-charges minimal tabs so a hostile max-tab publication cannot hide
// behind tiny strings and slip under MaxSnapshotStagedBytes.
const (
	estimateTabBytes      = 64
	estimateTabSliceBytes = 24
	estimateSessionBase   = 32
)

func estimateCatalogSessionBytes(session catalogue.RemoteCatalogSession) uint64 {
	n := len(session.Name) + len(session.Reason) + len(session.ActiveTabID) + estimateSessionBase
	if session.Tabs != nil {
		n += estimateTabSliceBytes
	}
	for _, tab := range session.Tabs {
		n += len(tab.ID) + len(tab.Name) + len(tab.Detail) + estimateTabBytes
	}
	if n < 0 {
		return 0
	}
	return uint64(n)
}
