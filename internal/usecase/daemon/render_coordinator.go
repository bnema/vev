package daemon

import (
	"runtime"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

type renderCoordinator struct {
	mu   sync.Mutex
	opts renderCoordinatorOptions

	// Coalesced pending state: latest wins, reset stays sticky, and at most
	// one deadline wake may be armed at a time (cap-1).
	pending       bool
	pendingReset  bool
	pendingUrgent bool
	// queueMarked spans the coordinator's actual pending interval, from first
	// enqueue until fire consumes the coalesced work.
	queueMarked      bool
	queueCorrelation ports.RuntimeCorrelation
	// pendingPreview is accounted separately from the target primary wake:
	// viewers may consume a coalesced target snapshot while that target is
	// still ACK-blocked.
	pendingPreview bool
	// ackDeferred records that an expired deadline or watchdog was blocked by
	// output-window capacity. ACK notifications may flush only this state.
	ackDeferred bool
	// ackBlocked retains the first rejected ACK-capacity probe until the
	// matching consume or lifecycle closure releases that exact interval.
	ackBlocked *ackBlockedSpan
	// deadlineDue closes the handoff between a timer worker receiving its tick
	// and fire observing unavailable ACK capacity.
	deadlineDue bool
	coalesced   int
	// outputPressure carries recent bulk-burst pressure between completed
	// batches. It selects a bounded 8–16ms deadline without letting a busy
	// batch perpetually reset its own timer.
	outputPressure int

	// previewWake receives the same coalesced wakes for the legacy single
	// subscriber. previewWakes tracks picker subscriptions by viewer, so one
	// inactive session cannot replace another viewer's live preview.
	previewWake  func(renderWake)
	previewWakes map[*attachedClient]previewSubscription

	// lease is the sole primary attachment lifecycle state. A revoked lease is
	// retained after detach so headless preview delivery remains distinguishable
	// from an unbound test coordinator.
	lease    *attachmentLease
	torndown bool

	resize resizeRequestMetadata
	// Each lane owns one complete immutable worker token at a time.
	resizeLane timerLane
	retryLane  timerLane
	metrics    renderCoordinatorBurstMetrics
	// normalLane owns render invalidation deadlines. The supervisor is only
	// waited by the session owner after terminal teardown has rejected future
	// registrations.
	normalLane timerLane
	armed      bool
	supervisor timerSupervisor
	// syncBatches is keyed by the stable pane owner. Each pane owns an
	// independent watchdog; composition remains gated until every batch ends.
	syncBatches map[*pane]*syncBatch
	// syncRegistryVersion changes for every batch identity mutation. fire
	// snapshots it before evaluating visibility and retries if a producer
	// publishes or completes a batch during that unlocked evaluation.
	syncRegistryVersion uint64
}

// newRenderCoordinator constructs a coordinator bound to opts. It starts no
// goroutine; deadline scheduling is armed lazily by invalidate.
func newRenderCoordinator(opts renderCoordinatorOptions) *renderCoordinator {
	return &renderCoordinator{opts: opts}
}

// detachNormalTimerLocked removes the complete current token. The caller may
// stop it only after dropping c.mu and must never wait for its worker.
func (c *renderCoordinator) detachNormalTimerLocked() *timerToken {
	return c.normalLane.detachLocked()
}

type syncBatch struct {
	generation uint64
	// renderable is evaluated outside c.mu: tab/session visibility locks may be
	// acquired by this predicate.
	renderable func() bool
	force      func()
	lane       timerLane
}

func detachSyncBatchLocked(batch *syncBatch) *timerToken { return batch.lane.detachLocked() }

func (c *renderCoordinator) detachSyncTimersLocked() []*timerToken {
	workers := make([]*timerToken, 0, len(c.syncBatches))
	for pane, batch := range c.syncBatches {
		workers = append(workers, detachSyncBatchLocked(batch))
		delete(c.syncBatches, pane)
		c.syncRegistryVersion++
	}
	return workers
}

// invalidate publishes a coordinator-owned producer transition. Callers that
// carry an attachment identity must use invalidateForAttachment instead.
func (c *renderCoordinator) invalidate(inv renderInvalidation) {
	c.invalidateForAttachment(nil, inv)
}

// invalidateForAttachment publishes only while source is still the bound
// attachment. This check shares c.mu with replacement, making a stale
// attachment's timer callback unable to enqueue work for its replacement.
// A nil source represents an internal pane/session invalidation.
func (c *renderCoordinator) invalidateForAttachment(source *attachedClient, inv renderInvalidation) bool {
	return c.invalidateForAttachmentAtResizeEpoch(source, 0, inv)
}

// invalidateForLease publishes an attachment-owned transition only for the
// exact lifecycle incarnation that captured it.
func (c *renderCoordinator) invalidateForLease(source *attachedClient, lease *attachmentLease, inv renderInvalidation) bool {
	return c.invalidateForLeaseAtResizeEpoch(source, lease, 0, inv)
}

// invalidateForAttachmentAtResizeEpoch additionally requires epoch to remain
// current when nonzero, so a completed resize cannot publish after a newer
// request supersedes it.
func (c *renderCoordinator) invalidateForAttachmentAtResizeEpoch(source *attachedClient, epoch uint64, inv renderInvalidation) bool {
	return c.invalidateForLeaseAtResizeEpoch(source, nil, epoch, inv)
}

// invalidateForLeaseAtResizeEpoch is the attachment-callback variant: it
// rejects a same-object replacement whose source pointer still matches but
// whose lifecycle lease has been revoked.
func (c *renderCoordinator) invalidateForLeaseAtResizeEpoch(source *attachedClient, lease *attachmentLease, epoch uint64, inv renderInvalidation) bool {
	c.mu.Lock()
	// An unbound coordinator is the manual/headless harness state; there is no
	// published production attachment to protect there. A parked target may
	// still publish its own PTY/session mutations to picker observers, but no
	// attachment-owned callback may revive that target's render path.
	detachedPreviewOnly := c.primaryDetachedLocked() && source == nil && (c.previewWake != nil || len(c.previewWakes) != 0)
	if c.torndown || (lease != nil && (!c.leaseCurrentLocked(lease, true) || lease.attachment != source)) ||
		(c.primaryDetachedLocked() && !detachedPreviewOnly) || (source != nil && c.lease != nil && c.lease.attachment != source) ||
		(epoch != 0 && (c.resize.epoch != epoch || c.resize.source != source || (lease != nil && c.resize.lease != lease))) {
		c.mu.Unlock()
		return false
	}
	onInvalidate := c.opts.onInvalidate
	c.metrics.invalidations.Add(1)
	wasPending, wasUrgent, wasPreviewPending := c.pending, c.pendingUrgent, c.pendingPreview
	queueStart := false
	var queueCorrelation ports.RuntimeCorrelation
	if !wasPending {
		c.ackDeferred = false
		c.deadlineDue = false
		if c.opts.observer != nil {
			c.queueMarked = true
			c.queueCorrelation = ports.NewRuntimeCorrelation()
			queueCorrelation = c.queueCorrelation
			queueStart = true
		}
	}
	c.pending = true
	c.pendingReset = c.pendingReset || inv.reset
	c.pendingUrgent = c.pendingUrgent || inv.class == invalidateUrgent
	c.pendingPreview = c.pendingPreview || c.previewWake != nil || len(c.previewWakes) != 0
	c.coalesced++
	// A deadline may have expired while synchronized output still gated the
	// pending work. Completion republishes urgently and must reserve a fresh
	// deadline rather than leaving that already-fired timer as the only arm.
	arm := !wasPending || (!wasUrgent && c.pendingUrgent) || (!wasPreviewPending && c.pendingPreview) || c.deadlineDue
	var old *timerToken
	if arm {
		_, old = c.normalLane.replaceLocked()
	}
	gen := c.normalLane.generation
	delay := minOutputRenderDeadline + time.Duration(c.outputPressure)*time.Millisecond
	if c.pendingUrgent {
		delay = urgentRenderDeadline
	}
	clock := c.opts.clock
	c.armed = c.armed || arm
	observer := c.opts.observer
	c.mu.Unlock()
	if queueStart && observer != nil {
		observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("daemon", queueCorrelation, ports.RuntimeQueueEnqueued, 0, true))
	}
	stopDetachedTimer(old)
	if !arm || clock == nil {
		if onInvalidate != nil {
			onInvalidate(inv)
		}
		return true
	}

	// NewTimer and C may re-enter the coordinator. Publish only if this
	// generation is still the reserved deadline after those external calls.
	timer := clock.NewTimer(delay)
	timerC := timer.C()
	c.mu.Lock()
	valid := !c.torndown && c.pending && c.normalLane.generation == gen
	var token *timerToken
	if valid && timerC != nil {
		token = c.normalLane.publishLocked(gen, timer)
		valid = token != nil
		if valid {
			c.supervisor.startLocked(token, timerC, func() {
				c.markDeadlineDue(token)
				c.fireFromTimer(token, false, true)
			})
		}
	}
	c.mu.Unlock()
	if !valid {
		timer.Stop()
		if onInvalidate != nil {
			onInvalidate(inv)
		}
		return true
	}
	if timerC == nil {
		timer.Stop()
		c.fire(gen, false, true)
		if onInvalidate != nil {
			onInvalidate(inv)
		}
		return true
	}
	// Test-visible observation follows deadline publication, so a producer
	// callback cannot observe a successful invalidation before its timer owns
	// a worker (and race tests cannot finish while a mock expectation remains).
	if onInvalidate != nil {
		onInvalidate(inv)
	}
	runtime.Gosched()
	return true
}

// notifyAck reports that the client acknowledged an output state, releasing
// at most one deadline/watchdog-deferred wake. It never bypasses an unexpired
// normal or urgent deadline.
func (c *renderCoordinator) notifyAck() { c.notifyAckForLease(nil) }

// notifyAckForLease prevents an acknowledgement captured by an obsolete active
// frame from flushing the replacement attachment's render queue. A nil lease is
// reserved for coordinator-owned/direct test callers.
func (c *renderCoordinator) notifyAckForLease(lease *attachmentLease) {
	c.mu.Lock()
	if lease != nil && !c.leaseCurrentLocked(lease, true) {
		c.mu.Unlock()
		return
	}
	if (!c.ackDeferred && !c.deadlineDue) || !c.pending {
		c.mu.Unlock()
		return
	}
	gen := c.normalLane.generation
	blocked := c.ackBlocked
	c.ackBlocked = nil
	c.mu.Unlock()
	if blocked != nil {
		blocked.finish(true)
	}
	c.fire(gen, false, false)
}

// noteSyncBegin records a synchronized-output batch for its stable pane and
// arms that pane's watchdog. Overlapping pane batches are independent.
// markDeadlineDue records a received timer tick before the worker probes ACK
// readiness, so a concurrent ACK cannot miss an already-expired deadline.
func (c *renderCoordinator) markDeadlineDue(token *timerToken) {
	c.mu.Lock()
	if c.fireValidLocked(token, token.generation, false) {
		c.deadlineDue = true
	}
	c.mu.Unlock()
}

func (c *renderCoordinator) noteSyncBegin(p *pane, gen uint64, force ...func()) {
	var renderable func() bool
	if c.opts.syncRenderable != nil {
		renderable = func() bool { return c.opts.syncRenderable(p) }
	}
	c.noteSyncBeginWithRenderability(p, gen, renderable, force...)
}

// noteSyncBeginWithRenderability records lifecycle unconditionally while the
// supplied predicate decides dynamically whether this batch gates composition.
func (c *renderCoordinator) noteSyncBeginWithRenderability(p *pane, gen uint64, renderable func() bool, force ...func()) {
	c.mu.Lock()
	if c.torndown {
		c.mu.Unlock()
		return
	}
	if c.syncBatches == nil {
		c.syncBatches = make(map[*pane]*syncBatch)
	}
	var old *timerToken
	if previous := c.syncBatches[p]; previous != nil {
		old = detachSyncBatchLocked(previous)
	}
	batch := &syncBatch{generation: gen, renderable: renderable}
	if len(force) != 0 {
		batch.force = force[0]
	}
	c.syncBatches[p] = batch
	c.syncRegistryVersion++
	clock := c.opts.clock
	c.mu.Unlock()
	stopDetachedTimer(old)
	if clock == nil {
		return
	}

	// Both timer operations are external. A reentrant callback may replace or
	// remove batch; identity validation below rejects this stale timer.
	timer := clock.NewTimer(maxSyncUpdateDuration)
	timerC := timer.C()
	c.mu.Lock()
	valid := !c.torndown && c.syncBatches[p] == batch && batch.generation == gen && batch.lane.generation == 0
	var token *timerToken
	if valid && timerC != nil {
		batch.lane.generation = gen
		token = batch.lane.publishLocked(gen, timer)
		valid = token != nil
		if valid {
			c.supervisor.startLocked(token, timerC, func() { c.runSyncWatchdog(p, batch, token) })
		}
	}
	c.mu.Unlock()
	if !valid || timerC == nil {
		stopDetachedTimer(&timerToken{timer: timer})
		return
	}
}

func (c *renderCoordinator) runSyncWatchdog(p *pane, batch *syncBatch, token *timerToken) {
	// Keep the batch registered while force runs. A concurrent render must
	// continue to observe the synchronized-output gate until the VT has
	// authoritatively closed the batch.
	c.mu.Lock()
	current := c.syncBatches[p]
	valid := !c.torndown && current == batch && current.generation == token.generation && current.lane.token == token
	c.mu.Unlock()
	if !valid {
		return
	}
	if batch.force != nil {
		batch.force()
	}

	// force may synchronously end this batch, replace it with a newer
	// generation, or tear down the coordinator. Remove only the exact
	// snapshot that expired, then publish one urgent completion wake.
	c.mu.Lock()
	current = c.syncBatches[p]
	valid = !c.torndown && current == batch && current.generation == token.generation && current.lane.token == token
	var stopped *timerToken
	if valid {
		delete(c.syncBatches, p)
		c.syncRegistryVersion++
		stopped = detachSyncBatchLocked(batch)
		if len(c.syncBatches) == 0 && c.pending {
			c.pendingUrgent = true
		}
	}
	c.mu.Unlock()
	if valid {
		stopDetachedTimer(stopped)
		// Reevaluate the aggregate gate only after forcing this pane.
		c.fireCurrent(true)
	}
}

// noteSyncEnd records completion for exactly one pane batch. It only flushes
// after the aggregate gate opens.
func (c *renderCoordinator) noteSyncEnd(p *pane, gen uint64) {
	// Visibility may acquire session/tab locks, so evaluate it outside c.mu.
	c.mu.Lock()
	batch := c.syncBatches[p]
	if batch == nil || batch.generation != gen {
		c.mu.Unlock()
		return
	}
	renderable := batch.renderable
	c.mu.Unlock()
	wasRenderable := renderable == nil || renderable()
	if c.removeSyncEnd(p, gen, wasRenderable) {
		c.fireCurrent(false)
	}
}

// removeSyncEnd removes a batch without invoking a predicate or callback. PTY
// processing calls it while pane.mu is still held, so a deadline can never
// compose the post-Write partial screen between mutation and publication.
// The caller must invoke fireCurrent only after releasing pane.mu.
func (c *renderCoordinator) removeSyncEnd(p *pane, gen uint64, renderable bool) bool {
	c.mu.Lock()
	batch := c.syncBatches[p]
	if batch == nil || batch.generation != gen {
		c.mu.Unlock()
		return false
	}
	delete(c.syncBatches, p)
	c.syncRegistryVersion++
	worker := detachSyncBatchLocked(batch)
	if renderable && len(c.syncBatches) == 0 && c.pending {
		c.pendingUrgent = true
	}
	c.mu.Unlock()
	stopDetachedTimer(worker)
	return renderable
}

// noteSyncPaneRemoved releases a pane watchdog when pane lifecycle ends.
func (c *renderCoordinator) noteSyncPaneRemoved(p *pane) {
	c.mu.Lock()
	var worker *timerToken
	if batch := c.syncBatches[p]; batch != nil {
		delete(c.syncBatches, p)
		c.syncRegistryVersion++
		worker = detachSyncBatchLocked(batch)
	}
	c.mu.Unlock()
	stopDetachedTimer(worker)
}

// subscribePreview installs fn as the preview observer for coalesced wakes.
func (c *renderCoordinator) subscribePreview(fn func(renderWake)) {
	c.mu.Lock()
	c.previewWake = fn
	c.mu.Unlock()
}

// teardownPreview removes the legacy preview subscription.
func (c *renderCoordinator) teardownPreview() { c.mu.Lock(); c.previewWake = nil; c.mu.Unlock() }

type previewSubscription struct {
	generation uint64
	fn         func(renderWake)
}

// subscribePreviewFor installs the dynamic picker observer owned by viewer.
// A newer picker generation wins, so a delayed subscription cannot replace it.
func (c *renderCoordinator) subscribePreviewFor(viewer *attachedClient, generation uint64, fn func(renderWake)) bool {
	if viewer == nil || fn == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.torndown {
		return false
	}
	if current, ok := c.previewWakes[viewer]; ok && current.generation > generation {
		return false
	}
	if c.previewWakes == nil {
		c.previewWakes = make(map[*attachedClient]previewSubscription)
	}
	c.previewWakes[viewer] = previewSubscription{generation: generation, fn: fn}
	return true
}

// teardownPreviewFor removes viewer's observer only when it still belongs to
// generation. A delayed teardown cannot clear a replacement subscription.
func (c *renderCoordinator) teardownPreviewFor(viewer *attachedClient, generation uint64) {
	c.mu.Lock()
	if current, ok := c.previewWakes[viewer]; ok && current.generation == generation {
		delete(c.previewWakes, viewer)
	}
	c.mu.Unlock()
}

// primaryDetachedLocked distinguishes an explicitly detached primary from an
// unbound coordinator used by headless tests. Preview subscribers remain live
// for the former, but it can never regain a primary transport without a lease.
func (c *renderCoordinator) primaryDetachedLocked() bool { return c.lease != nil && !c.lease.active }

func (c *renderCoordinator) installLeaseLocked(ac *attachedClient, ready bool) *attachmentLease {
	if c.lease != nil {
		c.lease.active = false
	}
	c.lease = &attachmentLease{attachment: ac, ready: ready, active: ac != nil}
	return c.lease
}

// attachmentLease returns the exact currently published lease for ac. Welcome
// continuations capture this before their transport write and cannot later
// bless a same-object park/resume replacement.
func (c *renderCoordinator) attachmentLease(ac *attachedClient) *attachmentLease {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.torndown || c.lease == nil || !c.lease.active || c.lease.attachment != ac {
		return nil
	}
	return c.lease
}

func (c *renderCoordinator) leaseCurrent(lease *attachmentLease, ready bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaseCurrentLocked(lease, ready)
}

// leaseCurrentLocked is a short, non-reentrant effect admission check. It
// must not be used to retain c.mu across input routing, transport operations,
// or other arbitrary handler work.
func (c *renderCoordinator) leaseCurrentLocked(lease *attachmentLease, ready bool) bool {
	return !c.torndown && lease != nil && c.lease == lease && lease.active && (!ready || lease.ready)
}

// attach binds an already-handshaken internal attachment. Route/resume use
// attachWithReadiness(..., false) until their Welcome frame is accepted.
func (c *renderCoordinator) attach(ac *attachedClient) { c.attachWithReadiness(ac, true) }

func (c *renderCoordinator) attachWithReadiness(ac *attachedClient, ready bool) {
	c.mu.Lock()
	if !c.torndown && (c.lease == nil || !c.lease.active || c.lease.attachment != ac) {
		c.installLeaseLocked(ac, ready)
	}
	c.mu.Unlock()
}

// markAttachmentReady completes only the captured attachment incarnation.
// A stale Welcome must never bless a replacement that reused its client object.
func (c *renderCoordinator) markAttachmentReady(lease *attachmentLease) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.leaseCurrentLocked(lease, false) {
		return false
	}
	lease.ready = true
	return true
}

// wakeCurrent validates a dispatched wake. paint repeats this check after
// sendMu is acquired, so a reused attachedClient cannot revive an old wake.
func (c *renderCoordinator) wakeCurrent(w renderWake) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leaseCurrentLocked(w.lease, true)
}

type renderLifecycleCleanup struct {
	tokens           []*timerToken
	observer         ports.RuntimeObserver
	queueCorrelation ports.RuntimeCorrelation
	queueMarked      bool
	ackBlocked       *ackBlockedSpan
}

func (cleanup renderLifecycleCleanup) finish() {
	for _, token := range cleanup.tokens {
		stopDetachedTimer(token)
	}
	if cleanup.queueMarked && cleanup.observer != nil {
		cleanup.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("daemon", cleanup.queueCorrelation, ports.RuntimeQueueDequeued, 0, false))
	}
	if cleanup.ackBlocked != nil {
		cleanup.ackBlocked.finish(false)
	}
}

// beginDetach invalidates pending work. It never waits for detached workers.
func (c *renderCoordinator) beginDetach(ac *attachedClient) renderLifecycleCleanup {
	c.mu.Lock()
	cleanup := c.beginDetachLocked(ac)
	c.mu.Unlock()
	return cleanup
}

// beginDetachLocked is the prelocked lifecycle seam used when registry,
// current-session, and lease publication must share one commit boundary.
func (c *renderCoordinator) beginDetachLocked(ac *attachedClient) renderLifecycleCleanup {
	var cleanup renderLifecycleCleanup
	if c.lease != nil && c.lease.active && c.lease.attachment == ac {
		c.lease.active = false
		cleanup = c.resetAttachmentLifecycleLocked()
	}
	return cleanup
}

func (c *renderCoordinator) resetAttachmentLifecycleLocked() renderLifecycleCleanup {
	c.pending = false
	c.pendingReset = false
	c.pendingUrgent = false
	c.ackDeferred = false
	c.pendingPreview = false
	c.coalesced = 0
	_, timer := c.normalLane.replaceLocked()
	c.armed = false
	_, resizeTimer := c.resizeLane.replaceLocked()
	_, retryTimer := c.retryLane.replaceLocked()
	cleanup := renderLifecycleCleanup{
		tokens:           []*timerToken{timer, resizeTimer, retryTimer},
		observer:         c.opts.observer,
		queueCorrelation: c.queueCorrelation,
		queueMarked:      c.queueMarked,
		ackBlocked:       c.ackBlocked,
	}
	c.queueMarked, c.ackBlocked = false, nil
	return cleanup
}

// noteDetach is for callers that hold no outer lifecycle locks.
func (c *renderCoordinator) noteDetach(ac *attachedClient) {
	c.beginDetach(ac).finish()
}

// beginReplace hands the coordinator from old to replacement without waiting
// for detached workers.
func (c *renderCoordinator) beginReplace(old, replacement *attachedClient, ready bool) renderLifecycleCleanup {
	c.mu.Lock()
	cleanup, _ := c.beginReplaceLocked(old, replacement, ready)
	c.mu.Unlock()
	return cleanup
}

// beginReplaceLocked requires c.mu. Callers validate canReplaceLocked before
// their ownership commit, making lease installation non-failing afterward.
func (c *renderCoordinator) beginReplaceLocked(old, replacement *attachedClient, ready bool) (renderLifecycleCleanup, *attachmentLease) {
	if !c.canReplaceLocked(old, replacement) {
		return renderLifecycleCleanup{}, nil
	}
	lease := c.installLeaseLocked(replacement, ready)
	return c.resetAttachmentLifecycleLocked(), lease
}

func (c *renderCoordinator) canReplaceLocked(old, replacement *attachedClient) bool {
	// A coordinator may be installed while replacing a legacy attachment which
	// predated coordinator ownership. Any inactive lease is detached historical
	// state; a resume may also replace the same client object because registry and
	// transport validation proved a new role incarnation.
	return !c.torndown && (c.lease == nil || !c.lease.active || c.lease.attachment == old || c.lease.attachment == replacement)
}

// noteReplace is for callers that hold no outer lifecycle locks.
func (c *renderCoordinator) noteReplace(old, replacement *attachedClient, readiness ...bool) {
	ready := true
	if len(readiness) != 0 {
		ready = readiness[0]
	}
	c.beginReplace(old, replacement, ready).finish()
}

// notePark invalidates pending wakes when the attachment parks for resume.
func (c *renderCoordinator) notePark(ac *attachedClient) { c.noteDetach(ac) }

// beginSessionTeardown is terminal phase one. It rejects future worker
// registration and atomically detaches every complete lane token.
func (c *renderCoordinator) beginSessionTeardown() renderLifecycleCleanup {
	c.mu.Lock()
	if c.torndown {
		c.mu.Unlock()
		return renderLifecycleCleanup{}
	}
	c.torndown = true
	if c.lease != nil {
		c.lease.active = false
	}
	c.previewWake = nil
	c.previewWakes = nil
	c.pending = false
	_, resizeTimer := c.resizeLane.replaceLocked()
	_, retryTimer := c.retryLane.replaceLocked()
	c.ackDeferred = false
	c.pendingPreview = false
	_, timer := c.normalLane.replaceLocked()
	c.armed = false
	workers := c.detachSyncTimersLocked()
	observer, queueCorrelation, queueMarked := c.opts.observer, c.queueCorrelation, c.queueMarked
	ackBlocked := c.ackBlocked
	c.queueMarked, c.ackBlocked = false, nil
	c.mu.Unlock()
	return renderLifecycleCleanup{
		tokens:           append([]*timerToken{timer, resizeTimer, retryTimer}, workers...),
		observer:         observer,
		queueCorrelation: queueCorrelation,
		queueMarked:      queueMarked,
		ackBlocked:       ackBlocked,
	}
}

// waitForTimerWorkers is terminal phase two. Only the session owner invokes it,
// after beginSessionTeardown has made future supervisor registration impossible.
func (c *renderCoordinator) waitForTimerWorkers() { c.supervisor.wait() }

// invalidateRender is the sole producer fan-in. In tests and transitional
// headless paths without an attached coordinator it retains the old private
// compositor; attached sessions always schedule through their coordinator.
func (d *Daemon) invalidateRender(sess *session, ac *attachedClient, reset bool, producer string) {
	if sess != nil {
		if rc := sess.renderCoordinator(); rc != nil {
			rc.invalidateForAttachment(ac, renderInvalidation{class: invalidateUrgent, reset: reset, producer: producer})
			return
		}
	}
	if ac != nil {
		d.paint(sess, ac, reset, nil)
	}
}

// invalidateRenderNow publishes through the coordinator but immediately
// flushes the wake when the client can accept state. Attach uses this path so
// the required first full frame never depends on a debounce timer.
func (d *Daemon) invalidateRenderNow(sess *session, ac *attachedClient, reset bool, producer string) {
	if sess != nil {
		if rc := sess.renderCoordinator(); rc != nil {
			rc.invalidateForAttachment(ac, renderInvalidation{class: invalidateUrgent, reset: reset, producer: producer})
			rc.fireCurrent(false)
			return
		}
	}
	if ac != nil {
		d.paint(sess, ac, reset, nil)
	}
}

func (c *renderCoordinator) fire(gen uint64, watchdog, deadline bool) {
	c.mu.Lock()
	if !watchdog && c.normalLane.generation != gen {
		c.mu.Unlock()
		return
	}
	token := c.normalLane.token
	c.mu.Unlock()
	c.fireWithTimerToken(token, gen, watchdog, deadline)
}

// fireFromTimer consumes a normal deadline using its exact immutable token.
func (c *renderCoordinator) fireFromTimer(token *timerToken, watchdog, deadline bool) {
	c.fireWithTimerToken(token, token.generation, watchdog, deadline)
}

func (c *renderCoordinator) fireWithTimerToken(token *timerToken, gen uint64, watchdog, deadline bool) {
	// Readiness probes enter the attachment send path, which may in turn read
	// coordinator metadata. Never hold c.mu across that external callback.
	c.mu.Lock()
	if !c.fireValidLocked(token, gen, watchdog) {
		c.mu.Unlock()
		return
	}
	// Welcome holds attachment.sendMu while its transport Send is in flight.
	// Do not probe ACK capacity through that lock before this incarnation is
	// ready: a deadline that expires during the handshake must complete and
	// leave work pending, rather than wedging behind Welcome.
	handshakePending := c.lease != nil && c.lease.active && !c.lease.ready
	ackReady := c.opts.ackReady
	c.mu.Unlock()
	ready := !handshakePending && (ackReady == nil || ackReady())

	// The readiness callback can detach, replace, or publish a sync batch.
	// syncGateOpenLocked retries predicate evaluation on every registry change
	// and returns holding c.mu, so pending consumption has no intervening gap.
	c.mu.Lock()
	if !c.fireValidLocked(token, gen, watchdog) {
		c.mu.Unlock()
		return
	}
	if !c.syncGateOpenLocked() {
		c.mu.Unlock()
		return
	}
	if !c.fireValidLocked(token, gen, watchdog) {
		c.mu.Unlock()
		return
	}
	// A detached target has no primary transport or ACK window. Its pending
	// state belongs exclusively to picker previews and must be consumed after
	// notification rather than accumulating ackDeferred forever. Do not pass
	// its revoked primary lease to preview consumers.
	headlessPreviewOnly := c.primaryDetachedLocked()
	lease := c.lease
	if headlessPreviewOnly {
		lease = nil
	}
	w := renderWake{reset: c.pendingReset, urgent: c.pendingUrgent, coalesced: c.coalesced, watchdog: watchdog, lease: lease}
	if !headlessPreviewOnly && (!ready || (c.lease != nil && c.lease.active && !c.lease.ready)) {
		var ackStart *ackBlockedSpan
		if !ready && deadline {
			c.ackDeferred = true
			if c.ackBlocked == nil && c.opts.observer != nil {
				// Install the span while coordinator state is owned. Its Start
				// publication runs unlocked, and its own handoff defers End until
				// that call has returned.
				ackStart = newACKBlockedSpan(c.opts.observer)
				c.ackBlocked = ackStart
			}
		}
		preview, previews := c.takePendingPreviewsLocked()
		c.mu.Unlock()
		if ackStart != nil {
			ackStart.publishStart()
		}
		c.notifyPreviews(w, preview, previews)
		return
	}
	preview, previews := c.takePendingPreviewsLocked()
	c.metrics.wakes.Add(1)
	c.metrics.coalesced.Add(uint64(w.coalesced))
	if !w.urgent {
		if w.coalesced > 1 {
			c.outputPressure += w.coalesced - 1
			if c.outputPressure > int((maxOutputRenderDeadline-minOutputRenderDeadline)/time.Millisecond) {
				c.outputPressure = int((maxOutputRenderDeadline - minOutputRenderDeadline) / time.Millisecond)
			}
		} else if c.outputPressure > 0 {
			c.outputPressure--
		}
	}
	worker := c.detachNormalTimerLocked()
	c.pending, c.pendingReset, c.pendingUrgent, c.ackDeferred, c.deadlineDue, c.coalesced, c.armed = false, false, false, false, false, 0, false
	observer, queueCorrelation, queueMarked := c.opts.observer, c.queueCorrelation, c.queueMarked
	ackBlocked := c.ackBlocked
	c.queueMarked, c.ackBlocked = false, nil
	wake := c.opts.wake
	c.mu.Unlock()
	stopDetachedTimer(worker)
	if queueMarked && observer != nil {
		observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("daemon", queueCorrelation, ports.RuntimeQueueDequeued, 0, true))
	}
	if ackBlocked != nil {
		ackBlocked.finish(true)
	}
	if wake != nil && !headlessPreviewOnly {
		wake(w)
	}
	c.notifyPreviews(w, preview, previews)
}

func (c *renderCoordinator) fireValidLocked(token *timerToken, gen uint64, watchdog bool) bool {
	if c.torndown || !c.pending || watchdog {
		return !c.torndown && c.pending
	}
	return c.normalLane.generation == gen && (token == nil || c.normalLane.token == token)
}

// syncGateOpen evaluates pane visibility without c.mu, then reacquires and
// validates the complete batch identity set before accepting that result.
// syncGateOpen evaluates visibility outside c.mu and returns with c.mu held
// only when the registry version still matches the snapshot. The caller must
// consume pending state before unlocking, eliminating the gate-to-consume gap.
func (c *renderCoordinator) syncGateOpenLocked() bool {
	for {
		version := c.syncRegistryVersion
		batches := make([]*syncBatch, 0, len(c.syncBatches))
		for _, batch := range c.syncBatches {
			batches = append(batches, batch)
		}
		c.mu.Unlock()

		gated := false
		for _, batch := range batches {
			if batch.renderable == nil || batch.renderable() {
				gated = true
				break
			}
		}
		if hook := c.opts.afterSyncGateEvaluated; hook != nil {
			hook()
		}

		c.mu.Lock()
		if version == c.syncRegistryVersion {
			return !gated
		}
	}
}

// takePendingPreviewsLocked snapshots and consumes the preview delivery for
// this target generation. Unlike the target primary, this is never ACK-gated.
func (c *renderCoordinator) takePendingPreviewsLocked() (func(renderWake), []func(renderWake)) {
	if !c.pendingPreview {
		return nil, nil
	}
	c.pendingPreview = false
	previews := make([]func(renderWake), 0, len(c.previewWakes))
	for _, subscription := range c.previewWakes {
		previews = append(previews, subscription.fn)
	}
	return c.previewWake, previews
}

func (c *renderCoordinator) notifyPreviews(w renderWake, preview func(renderWake), previews []func(renderWake)) {
	if preview != nil {
		preview(w)
	}
	for _, fn := range previews {
		fn(w)
	}
}
