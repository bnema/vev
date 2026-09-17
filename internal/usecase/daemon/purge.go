package daemon

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/bnema/vev/internal/domain"
)

// errPurgeAdmissionClosed is returned to command-path creation/resume that
// already holds architecture locks and therefore cannot wait for a daemon-wide
// purge. Route-level creation waits on the purge admission gate instead.
var errPurgeAdmissionClosed = errors.New("daemon is purging sessions")

// purgeAdmissionClosedLocked reports whether a KillAll purge currently owns
// admission. Caller holds mu. A command-path create/resume that cannot wait
// rejects with errPurgeAdmissionClosed instead of inserting into the exact set
// being removed.
func (d *Daemon) purgeAdmissionClosedLocked() bool { return d.purgeAdmissionClosing }

// acquirePurgeAdmission reserves one create/restore transition against the
// transient KillAll gate. It waits without holding any other lock while a purge
// owns the gate, so a Hello arriving mid-purge is admitted as a create or
// restore after the purge rather than inserted into the set being removed.
func (d *Daemon) acquirePurgeAdmission(ctx context.Context) error {
	for {
		d.mu.Lock()
		if !d.purgeAdmissionClosing {
			d.purgeAdmissionActive++
			d.mu.Unlock()
			return nil
		}
		changed := d.purgeAdmissionChangeLocked()
		d.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// releasePurgeAdmission drops a reservation taken by acquirePurgeAdmission or
// an equivalent move reservation.
func (d *Daemon) releasePurgeAdmission() {
	d.mu.Lock()
	d.purgeAdmissionActive--
	d.signalPurgeAdmissionChangedLocked()
	d.mu.Unlock()
}

// purgeAdmissionStatus reports how a KillAll's attempt to close the transient
// admission gate resolved.
type purgeAdmissionStatus uint8

const (
	// purgeAdmissionGranted means the gate closed and every admitted transition
	// drained within the bound, so the purge owns the exact lifecycle set.
	purgeAdmissionGranted purgeAdmissionStatus = iota
	// purgeAdmissionSuperseded means an explicit daemon shutdown began, or the
	// caller's context ended, before the drain completed. The purge removes
	// nothing and defers to the stop.
	purgeAdmissionSuperseded
	// purgeAdmissionTimedOut means an admitted transition did not release the
	// gate before the bounded purge budget expired. The purge removes nothing so
	// a transition stuck in repository I/O that ignores cancellation can never
	// wedge Serve.
	purgeAdmissionTimedOut
)

// beginPurgeAdmission closes the KillAll admission gate and waits, bounded, for
// every admitted transition to drain before the caller snapshots the exact
// lifecycle set. It mirrors closeMoveLifecycles but is re-openable: the caller
// must always call endPurgeAdmission to clear it and leave the daemon serving.
//
// The wait ends as soon as the drain completes. It also ends early, without the
// purge owning anything, when ctx is done, an explicit shutdown begins
// (closing), or deadline expires — so an admitted create, restore, or move that
// ignores cancellation (for example a repository call that never returns) can
// never extend Serve past the bounded purge budget. On success purgeEpoch is
// incremented so a restoration worker that waited on the gate can detect that
// the record set it captured was superseded.
func (d *Daemon) beginPurgeAdmission(ctx context.Context, deadline *snapshotShutdownDeadline) purgeAdmissionStatus {
	if ctx == nil {
		ctx = context.Background()
	}
	d.mu.Lock()
	d.purgeAdmissionClosing = true
	d.signalPurgeAdmissionChangedLocked()
	for {
		if d.closing {
			d.mu.Unlock()
			return purgeAdmissionSuperseded
		}
		if d.purgeAdmissionActive == 0 {
			d.purgeEpoch++
			d.signalPurgeAdmissionChangedLocked()
			d.mu.Unlock()
			return purgeAdmissionGranted
		}
		changed := d.purgeAdmissionChangeLocked()
		d.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return purgeAdmissionSuperseded
		case <-deadline.Done():
			// An explicit stop that began as the budget expired wins: classify it
			// as superseded so the purge defers and reports stop semantics instead
			// of a drain timeout. terminateAllForShutdown also signals changed, but
			// a ready select may still pick the deadline, so recheck closing here.
			d.mu.Lock()
			closing := d.closing
			d.mu.Unlock()
			if closing {
				return purgeAdmissionSuperseded
			}
			return purgeAdmissionTimedOut
		}
		d.mu.Lock()
	}
}

func (d *Daemon) endPurgeAdmission() {
	d.mu.Lock()
	d.purgeAdmissionClosing = false
	d.signalPurgeAdmissionChangedLocked()
	d.mu.Unlock()
}

// purgeEpochSnapshot returns the current purge boundary. Restoration captures
// it before reading catalogue records so a purge that completes while its
// workers wait for admission invalidates those records.
func (d *Daemon) purgeEpochSnapshot() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.purgeEpoch
}

// purgeSupersededLocked reports whether an admitted transition captured at
// epoch must abandon its work: an explicit shutdown has begun, a purge currently
// owns and waits on admission, or a purge completed after epoch was captured.
// Caller holds mu.
func (d *Daemon) purgeSupersededLocked(epoch uint64) bool {
	return d.closing || d.purgeAdmissionClosing || d.purgeEpoch != epoch
}

// purgeSuperseded is purgeSupersededLocked without the caller-held lock.
func (d *Daemon) purgeSuperseded(epoch uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.purgeSupersededLocked(epoch)
}

// purgeSupersededForShutdown reports whether an explicit daemon shutdown has
// begun. A KillAll purge defers to it so explicit stop semantics (preserving
// live sessions as stopped authority) are never overwritten by a concurrent
// purge.
func (d *Daemon) purgeSupersededForShutdown() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.closing
}

// purgeAdmissionChangeLocked returns the channel closed when either purge
// admission field changes. Caller holds mu.
func (d *Daemon) purgeAdmissionChangeLocked() chan struct{} {
	if d.purgeAdmissionChanged == nil {
		d.purgeAdmissionChanged = make(chan struct{})
	}
	return d.purgeAdmissionChanged
}

// signalPurgeAdmissionChangedLocked wakes every admission waiter. Caller holds
// mu.
func (d *Daemon) signalPurgeAdmissionChangedLocked() {
	close(d.purgeAdmissionChangeLocked())
	d.purgeAdmissionChanged = make(chan struct{})
}

// purgeRecordClass identifies which lifecycle record class a failed purge unit
// belongs to, so callers can report a typed partial failure instead of one
// opaque error.
type purgeRecordClass uint8

const (
	purgeRecordLive purgeRecordClass = iota + 1
	purgeRecordStopped
	purgeRecordBroken
	purgeRecordDurable
)

func (c purgeRecordClass) String() string {
	switch c {
	case purgeRecordLive:
		return "live"
	case purgeRecordStopped:
		return "stopped"
	case purgeRecordBroken:
		return "broken"
	case purgeRecordDurable:
		return "durable"
	default:
		return "unknown"
	}
}

// inactivePurgeClass resolves the record class of a captured stopped/broken
// registry entry.
func inactivePurgeClass(entry inactiveSession) purgeRecordClass {
	if entry.broken() {
		return purgeRecordBroken
	}
	return purgeRecordStopped
}

// purgeFailure is one structured partial purge failure: the record class, the
// affected name when known, and the underlying error.
type purgeFailure struct {
	Class purgeRecordClass
	Name  string
	Err   error
}

func (f purgeFailure) Error() string {
	if f.Name == "" {
		return fmt.Sprintf("%s: %v", f.Class, f.Err)
	}
	return fmt.Sprintf("%s %q: %v", f.Class, f.Name, f.Err)
}

func (f purgeFailure) Unwrap() error { return f.Err }

// purgeResult reports every record that could not be fully purged. An empty
// Failures slice means the purge removed the entire exact lifecycle set. A
// superseded result means an explicit daemon shutdown won the race: the purge
// deferred and removed nothing, or removed only the units whose teardown it had
// already owned when the stop began. A drain timeout result means an admitted
// transition refused to drain within the bounded admission budget, so the purge
// removed nothing and reopened admission. A typed live failure means a live unit
// was left untouched because the shared live-teardown budget expired before its
// destructive boundary or because its participants kept changing. A typed
// stopped/broken failure means the bounded stopped/broken sweep did not finish
// the record's durable purge within its own budget.
type purgeResult struct {
	Failures      []purgeFailure
	superseded    bool
	drainTimedOut bool
}

func (r purgeResult) Failed() bool { return len(r.Failures) > 0 || r.drainTimedOut }

// Superseded reports that an explicit daemon shutdown owned the lifecycle set
// before this purge could finish destroying it, so the purge deferred to it.
// Any Failures it carries describe units the stop still owns; a superseded
// result never means the purge silently reported success.
func (r purgeResult) Superseded() bool { return r.superseded }

// purgeSummaryFailureLimit bounds the wire-safe failure detail. The result may
// hold arbitrarily many failures (one per record), but a control response must
// stay bounded: report the total count and only the first N failures, then note
// how many were omitted.
const purgeSummaryFailureLimit = 5

func (r purgeResult) summary() string {
	if r.drainTimedOut {
		return "session purge aborted: purge admission did not drain within the bounded timeout; no records were removed"
	}
	if !r.Failed() {
		return ""
	}
	shown := min(len(r.Failures), purgeSummaryFailureLimit)
	parts := make([]string, 0, shown)
	for _, failure := range r.Failures[:shown] {
		parts = append(parts, failure.Error())
	}
	if omitted := len(r.Failures) - shown; omitted > 0 {
		return fmt.Sprintf("%d record(s) could not be purged; first %d: %s; %d more omitted", len(r.Failures), shown, strings.Join(parts, "; "), omitted)
	}
	return fmt.Sprintf("%d record(s) could not be purged: %s", len(r.Failures), strings.Join(parts, "; "))
}

// purgeAllSessions is the KillAll operation: it removes every live session and
// every stopped/broken/durable record while leaving the daemon active. It does
// not set closing, close move admission, cancel daemon-wide pane/process
// contexts, close done, or consume shutdown signals, and it never makes Serve
// return.
//
// Linearization: beginPurgeAdmission closes the transient admission gate and
// drains in-flight create/restore/move reservations, so the exact lifecycle set
// captured below cannot be mutated by a concurrent transition. Purge then runs
// against that exact set, and endPurgeAdmission reopens admission for later
// creations, which are ordered after the purge.
//
// Budgets: the admission drain, the stopped/broken durable sweep, and
// live-session teardown spend separate bounded budgets, each matching the
// daemon shutdown budget. A transition that ignores
// cancellation and burns the whole drain budget therefore cannot leave teardown
// with no time, and teardown cannot extend the drain. A repository deletion
// that ignores cancellation cannot extend the stopped/broken sweep past its own
// budget either. The live budget is armed
// when the live phase begins and is shared by every live unit: once it expires,
// each remaining live unit is skipped and reported as a typed live failure
// rather than reported as purged. Every unit keeps its own accurate name.
func (d *Daemon) purgeAllSessions(reason uint8) purgeResult {
	d.purgeAllMu.Lock()
	defer d.purgeAllMu.Unlock()

	// One deadline bounds the drain of every admitted transition. It is stopped
	// before the destructive phase so teardown spends a fresh budget.
	admissionDeadline := newSnapshotShutdownDeadline(d.clock)
	defer admissionDeadline.stop()

	ctx := d.serveCtx
	if ctx == nil {
		ctx = context.Background()
	}
	switch d.beginPurgeAdmission(ctx, admissionDeadline) {
	case purgeAdmissionSuperseded:
		d.log.Info("session purge superseded before admission", "reason", reason)
		d.endPurgeAdmission()
		return purgeResult{superseded: true}
	case purgeAdmissionTimedOut:
		d.log.Warn("session purge aborted: purge admission did not drain", "reason", reason)
		d.endPurgeAdmission()
		return purgeResult{drainTimedOut: true}
	}
	admissionDeadline.stop()
	defer d.endPurgeAdmission()

	d.mu.Lock()
	// A shutdown that began after admission was granted still wins: no record
	// has been removed yet, so defer without touching the preserved set.
	if d.closing {
		d.mu.Unlock()
		d.log.Info("session purge superseded by daemon shutdown", "reason", reason, "phase", "pre-snapshot")
		return purgeResult{superseded: true}
	}
	d.purgeAllParkingLocked()
	parkedRetirements := d.purgeAllParkedLocked()
	d.purgeAllSuspendedLocked()
	live := d.sessionsSnapshotLocked()
	inactive := make([]inactiveSession, 0, len(d.inactive))
	exact := make(map[string]struct{}, len(live)+len(d.inactive))
	for _, entry := range d.inactive {
		inactive = append(inactive, entry)
		exact[entry.name] = struct{}{}
	}
	for _, sess := range live {
		sess.mu.Lock()
		exact[sess.name] = struct{}{}
		sess.mu.Unlock()
	}
	d.mu.Unlock()
	if d.afterPurgeAdmission != nil {
		d.afterPurgeAdmission()
	}
	d.finishParkedAttachmentRetirements(parkedRetirements)
	sort.Slice(inactive, func(i, j int) bool { return inactive[i].name < inactive[j].name })

	// A shutdown that began while the snapshot was taken still wins: no record
	// has been removed yet, so defer without touching the preserved set.
	if d.purgeSupersededForShutdown() {
		d.log.Info("session purge superseded by daemon shutdown", "reason", reason, "phase", "pre-destructive")
		return purgeResult{superseded: true}
	}

	result := purgeResult{}
	d.log.Info("session purge begin", "reason", reason, "live_sessions", len(live), "inactive_sessions", len(inactive))

	// Stopped authority first (mirroring the shutdown purge order), then live
	// sessions. Each unit is independent: one failure never cancels a sibling.
	// Explicit stop wins per unit: a shutdown that begins mid-purge stops further
	// removals so it can preserve every not-yet-purged lifecycle as stopped
	// authority. A unit the purge already owns (its teardown is in progress)
	// keeps purge authority and completes as a purge; only units not yet started
	// are handed back to the stop.
	//
	// The stopped/broken sweep spends its own deadline and context, separate from
	// the admission drain and the live-teardown budget. A repository deletion
	// that ignores cancellation therefore cannot extend the purge: at the budget
	// every record the sweep did not finish is still stopped/broken authority and
	// is reported as a typed failure, and admission is released promptly.
	if len(inactive) > 0 {
		stoppedDeadline := newSnapshotShutdownDeadline(d.clock)
		defer stoppedDeadline.stop()
		stoppedCtx, cancelStopped := snapshotStopContext(stoppedDeadline)
		defer cancelStopped()
		sweep := d.purgeInactiveSweep(inactive, stoppedCtx, stoppedDeadline)
		result.Failures = append(result.Failures, sweep.failures...)
		if sweep.superseded {
			d.log.Info("session purge superseded by daemon shutdown", "reason", reason, "phase", "stopped")
			return purgeResult{Failures: result.Failures, superseded: true}
		}
	}
	// The live phase gets its own bounded budget, armed here so neither the
	// admission drain nor the stopped-record purge consumes it. It is shared by
	// every live unit.
	destructiveDeadline := newSnapshotShutdownDeadline(d.clock)
	defer destructiveDeadline.stop()
	for _, sess := range live {
		if d.purgeSupersededForShutdown() {
			d.log.Info("session purge superseded by daemon shutdown", "reason", reason, "phase", "live")
			return purgeResult{Failures: result.Failures, superseded: true}
		}
		sess.mu.Lock()
		name := sess.name
		sess.mu.Unlock()
		if d.beforePurgeLiveSessionKill != nil {
			d.beforePurgeLiveSessionKill(sess)
		}
		sess.stopInMemoryLifecycle()
		err := d.purgeLiveSession(sess, reason, destructiveDeadline)
		if err == nil {
			// A stop that wins this unit after the precheck above makes
			// purgeLiveSession a no-op: the stop already removed the session and
			// now owns it. Recheck so the purge never counts a stop-owned unit as a
			// successful purge. A unit whose teardown the purge already owned keeps
			// purge authority and is reported as removed, but the remaining units
			// still belong to the stop.
			if d.purgeSupersededForShutdown() {
				d.log.Info("session purge superseded by daemon shutdown", "reason", reason, "phase", "live-owned")
				return purgeResult{Failures: result.Failures, superseded: true}
			}
			continue
		}
		if d.purgeSupersededForShutdown() &&
			(errors.Is(err, errSessionKillDeadline) || errors.Is(err, errSessionKillParticipantsChanged)) {
			// The explicit stop owns the remaining lifecycles: either the live budget
			// expired as it began, or a participant change aborted a kill that the
			// stop now classifies as preservation rather than a purge failure.
			d.log.Info("session purge superseded by daemon shutdown", "reason", reason, "phase", "live-deadline")
			return purgeResult{Failures: result.Failures, superseded: true}
		}
		result.Failures = append(result.Failures, purgeFailure{Class: purgeRecordLive, Name: name, Err: err})
		d.log.Error("purging live session failed", "err", err, "session", name)
	}
	result.Failures = append(result.Failures, d.purgeDurableResidue(exact, result.Failures)...)
	sort.SliceStable(result.Failures, func(i, j int) bool {
		if result.Failures[i].Class != result.Failures[j].Class {
			return result.Failures[i].Class < result.Failures[j].Class
		}
		return result.Failures[i].Name < result.Failures[j].Name
	})
	if result.Failed() {
		d.log.Warn("session purge incomplete", "reason", reason, "failures", len(result.Failures))
	}
	return result
}

// purgeDurableResidue removes durable records that have no live/stopped/broken
// registry entry, so a KillAll still accounts for every catalogue record. A
// record already represented by a reported live/stopped/broken failure is that
// failure's durable residue and is not double-reported here.
func (d *Daemon) purgeDurableResidue(exact map[string]struct{}, existing []purgeFailure) []purgeFailure {
	if !d.persistEnabled || d.catalogue == nil {
		return nil
	}
	records, err := d.catalogue.Records()
	if err != nil {
		return []purgeFailure{{Class: purgeRecordDurable, Err: err}}
	}
	reported := make(map[string]struct{}, len(existing))
	for _, failure := range existing {
		reported[failure.Name] = struct{}{}
	}
	var out []purgeFailure
	for _, record := range records {
		if _, accounted := exact[record.Name]; accounted {
			continue
		}
		if _, already := reported[record.Name]; already {
			continue
		}
		if err := d.purgeDurableRecord(record); err != nil {
			out = append(out, purgeFailure{Class: purgeRecordDurable, Name: record.Name, Err: err})
		}
	}
	return out
}

// errSessionPurgeSweepDeadline reports that the stopped/broken sweep's own
// bounded budget expired before a record's durable purge finished. The record
// keeps its stopped/broken authority and is reported as a typed failure rather
// than silently counted as purged.
var errSessionPurgeSweepDeadline = errors.New("stopped session purge sweep deadline exceeded")

// errSessionPurgeDeletePending reports that a purge-owned live unit's exact
// durable delete did not finish within the shared live-teardown budget. The
// live identity is already removed and its stopped authority still reserves the
// name; a detached worker finishes the fenced DeleteExact and removes that
// authority once the repository returns. It is a typed failure so a bounded
// purge never reports a skipped durable delete as success, and a repository
// delete that ignores cancellation can never hold purgeAllMu, the admission
// gate, or control responses.
var errSessionPurgeDeletePending = errors.New("session durable purge pending")

// finishLiveDurablePurge runs the exact durable delete for a live unit the purge
// already removed from the live registry. With a bounded deadline it runs the
// delete on a detached worker: the caller returns errSessionPurgeDeletePending
// at the deadline while the worker finishes and removes the matching stopped
// authority. That removal is fenced by the captured name, incarnation, and
// createdAt, so a same-name or re-incarnated replacement is never touched. A
// nil deadline (a command-path kill) keeps the previous synchronous behavior.
func (d *Daemon) finishLiveDurablePurge(deadline *snapshotShutdownDeadline, record domain.CatalogueRecord) error {
	expected := inactiveSession{name: record.Name, createdAt: record.CreatedAt, incarnation: record.IncarnationID, purging: true}
	finish := func() error {
		if err := d.finishSnapshotPurge(d.serveCtx, record.Name, record.IncarnationID, record.CreatedAt); err != nil {
			return err
		}
		d.mu.Lock()
		if stopped, ok := d.inactive[record.Name]; ok && stopped.purging && stopped.sameLifecycle(expected) {
			delete(d.inactive, record.Name)
		}
		d.mu.Unlock()
		d.reconcileAllRouteHistories()
		return nil
	}
	if deadline == nil {
		return finish()
	}
	done := make(chan error, 1)
	go func() { done <- finish() }()
	select {
	case err := <-done:
		return err
	case <-deadline.Done():
		// Recheck completion so a delete that finished as the budget expired is
		// reported as success rather than pending.
		select {
		case err := <-done:
			return err
		default:
			// The caller already reported a typed pending failure and stopped
			// waiting. Wait for the detached delete on its own goroutine so a late
			// failure is still recorded with bounded, non-sensitive fields (record
			// class, incarnation, created-time) instead of the user-chosen session
			// name.
			go func() {
				if err := <-done; err != nil {
					d.log.Warn("detached session purge delete failed after deadline",
						"class", purgeRecordLive, "incarnation", record.IncarnationID, "created_at", record.CreatedAt, "err", err)
				}
			}()
			return errSessionPurgeDeletePending
		}
	}
}

// purgeInactiveSweepResult reports the bounded stopped/broken sweep outcome.
// failures holds every record the purge could not finish; superseded means an
// explicit stop began mid-sweep and owns the remaining records.
type purgeInactiveSweepResult struct {
	failures   []purgeFailure
	superseded bool
}

// purgeInactiveSweep removes the captured stopped/broken records under its own
// deadline and context, separate from the admission drain and the
// live-teardown budget. Because a repository deletion may ignore cancellation,
// the sweep runs on a detached worker and the caller returns at the deadline:
// every record the worker did not finish keeps its stopped/broken authority and
// is reported as a typed failure, so a bounded purge releases admission
// promptly instead of waiting out an uncooperative delete. The detached worker
// stops contributing to the returned result at that point; its registry removal
// stays lifecycle-fenced, so a late completion cannot delete a same-name
// replacement.
func (d *Daemon) purgeInactiveSweep(inactive []inactiveSession, ctx context.Context, deadline *snapshotShutdownDeadline) purgeInactiveSweepResult {
	var (
		mu         sync.Mutex
		failures   []purgeFailure
		completed  int
		superseded bool
		detached   bool
	)
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		for _, entry := range inactive {
			mu.Lock()
			abandoned := detached
			mu.Unlock()
			if abandoned {
				return
			}
			if d.purgeSupersededForShutdown() {
				mu.Lock()
				superseded = true
				mu.Unlock()
				return
			}
			class := inactivePurgeClass(entry)
			err := d.retryStoppedPurgeContextExact(ctx, entry.name, entry.incarnation, &entry.createdAt)
			mu.Lock()
			if detached {
				// The bounded caller already reported this unit; a late result must
				// not mutate the returned result.
				mu.Unlock()
				return
			}
			if err != nil {
				failures = append(failures, purgeFailure{Class: class, Name: entry.name, Err: err})
				d.log.Error("purging stopped session failed", "err", err, "session", entry.name)
			}
			completed++
			mu.Unlock()
		}
	}()

	select {
	case <-sweepDone:
	case <-deadline.Done():
		mu.Lock()
		detached = true
		done := completed
		reported := append([]purgeFailure(nil), failures...)
		mu.Unlock()
		if d.purgeSupersededForShutdown() {
			// An explicit stop that began as the budget expired owns the remaining
			// records, so the sweep is supersession rather than a purge failure.
			return purgeInactiveSweepResult{failures: reported, superseded: true}
		}
		for _, entry := range inactive[done:] {
			reported = append(reported, purgeFailure{Class: inactivePurgeClass(entry), Name: entry.name, Err: errSessionPurgeSweepDeadline})
			d.log.Error("purging stopped session failed", "err", errSessionPurgeSweepDeadline, "session", entry.name)
		}
		return purgeInactiveSweepResult{failures: reported}
	}

	mu.Lock()
	defer mu.Unlock()
	return purgeInactiveSweepResult{failures: failures, superseded: superseded}
}

// purgeLiveSession removes one live session under the shared live-teardown
// budget. A post-freeze participant change aborts the kill before it touches the
// session. A session a racing path already removed owns nothing and counts as
// purged; an explicit stop that began meanwhile owns the rest and is never
// retried against; otherwise the purge retries once against a fresh snapshot,
// and a session whose participants changed again stays a typed live failure for
// the caller.
func (d *Daemon) purgeLiveSession(sess *session, reason uint8, deadline *snapshotShutdownDeadline) error {
	err := d.killSessionWithSnapshotDeadline(sess, reason, true, deadline, nil)
	if !errors.Is(err, errSessionKillParticipantsChanged) {
		return err
	}
	if d.purgeSupersededForShutdown() {
		// An explicit stop has begun: it owns the session's remaining lifecycle, so
		// the purge must not retry against it. The caller classifies the abort as
		// supersession instead of a live failure.
		return err
	}
	if !d.sessionRegistered(sess) {
		return nil
	}
	return d.killSessionWithSnapshotDeadline(sess, reason, true, deadline, nil)
}

// sessionRegistered reports whether sess is still the exact live registry entry.
func (d *Daemon) sessionRegistered(sess *session) bool {
	if sess == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sessions[sess.id] == sess
}

func (d *Daemon) purgeDurableRecord(record domain.CatalogueRecord) error {
	if !d.persistEnabled {
		return nil
	}
	if d.recovery == nil {
		return errors.New("durable session authority is not configured")
	}
	return d.finishSnapshotPurge(d.serveCtx, record.Name, record.IncarnationID, record.CreatedAt)
}
