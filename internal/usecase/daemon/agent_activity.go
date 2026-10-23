package daemon

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/layout"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/agentactivity"
)

// Agent activity scheduling.
//
// The PTY read path only bumps a per-pane sequence and, on the first output
// since the last scan, enqueues the pane and wakes one shared scheduler
// goroutine. The scheduler coalesces output bursts per pane (a 40 ms quiet
// window, at most 120 ms of latency under continuous output), identifies the
// foreground agent, extracts the bottom screen rows under pane.mu, classifies
// them outside every lock, and publishes the result to the owning tab. All time
// comes from ports.Clock and one ports.Timer holding the nearest deadline.
const (
	agentScanQuiet        = 40 * time.Millisecond
	agentScanMaxLatency   = 120 * time.Millisecond
	agentReverifyInterval = 5 * time.Second
	// A pgid identified as no agent is re-read after agentNoneReverifyFirst,
	// then with a doubling interval from agentReverifyInterval up to
	// agentNoneReverifyMax, so an exec into an agent is still detected.
	agentNoneReverifyFirst = time.Second
	agentNoneReverifyMax   = 8 * agentReverifyInterval
	// agentBellAbsorbWindow bounds how long after a Done publish a deferred
	// BEL still belongs to the finished turn; the first settled publish
	// after the Done publish closes the window earlier.
	agentBellAbsorbWindow = time.Second
)

// agentPaneStatus is the activity of one pane, kept in the scheduler-owned
// record of the tab that owns the pane.
type agentPaneStatus struct {
	state agentactivity.State
	// done marks a completed turn no attachment that may be seen has shown.
	done bool
	// doneGen is the first published generation that carries done. It orders
	// completions for jump-attention.
	doneGen uint64
	// hidden records that the pane was not drawn in its tab when last
	// checked: a hidden floating pane or a collapsed stack member. Its Done
	// is unseen even in the viewed tab.
	hidden bool
	// shownGen is the first published generation that carries done with the
	// pane drawn, or 0 while hidden. A paint acknowledgement clears done only
	// when it rendered that generation, so the paint showed the pane.
	shownGen uint64
	// blockedRaised records that this Blocked episode already raised one
	// attention event.
	blockedRaised bool
}

// agentTabState is the immutable activity snapshot of one tab. The scheduler
// goroutine is its only writer; render and status code read it lock-free.
type agentTabState struct {
	indicator agentactivity.Indicator
	// done reports that some pane carries an unseen completion, even when a
	// higher-priority state hides it.
	done bool
	// working reports that some pane is Working, even when Done or Blocked
	// outranks it. The seen active tab, which never shows the Done of a
	// drawn pane, falls back to the spinner.
	working bool
	// doneHidden reports that some unseen completion is on a pane its tab
	// does not draw, so the seen active tab still shows Done.
	doneHidden bool
	// doneSeq is the oldest doneGen among the tab's unseen completions, a
	// monotonic scheduler counter that orders finished tabs for
	// jump-attention. Zero when done is false.
	doneSeq uint64
	// gen is the scheduler-wide generation of this snapshot.
	gen uint64
}

// agentAck is a paint's Done acknowledgement for one tab: the published
// generation it rendered and the tab's paneVisibilitySeq captured with the
// layout it drew.
type agentAck struct {
	gen           uint64
	visibilitySeq uint64
}

// agentTabRecord is the scheduler-owned activity of one tab.
type agentTabRecord struct {
	tab   *tab
	sess  *session
	panes map[*pane]agentPaneStatus
}

// agentIndicator returns the tab's published indicator.
func (tb *tab) agentIndicator() agentactivity.Indicator {
	if tb == nil {
		return agentactivity.IndicatorNone
	}
	if state := tb.agentState.Load(); state != nil {
		return state.indicator
	}
	return agentactivity.IndicatorNone
}

// agentDoneHidden reports whether one of the tab's unseen completions is on
// a pane the tab does not draw.
func (tb *tab) agentDoneHidden() bool {
	if tb == nil {
		return false
	}
	state := tb.agentState.Load()
	return state != nil && state.doneHidden
}

// agentDoneSeq returns the completion order of the tab's visible Done check,
// or false when the tab does not show one.
func (tb *tab) agentDoneSeq() (uint64, bool) {
	if tb == nil {
		return 0, false
	}
	state := tb.agentState.Load()
	if state == nil || state.indicator != agentactivity.IndicatorDone || !state.done {
		return 0, false
	}
	return state.doneSeq, true
}

func aggregateAgentPanes(panes map[*pane]agentPaneStatus) agentTabState {
	var out agentTabState
	for _, status := range panes {
		out.indicator = agentactivity.MaxIndicator(out.indicator, agentactivity.IndicatorFor(status.state, status.done))
		if status.done && (!out.done || status.doneGen < out.doneSeq) {
			out.doneSeq = status.doneGen
		}
		out.done = out.done || status.done
		out.doneHidden = out.doneHidden || status.done && status.hidden
		out.working = out.working || status.state == agentactivity.Working
	}
	return out
}

// agentPaneRecord is the scheduler-owned state of one observed pane. Only the
// scheduler goroutine touches it.
type agentPaneRecord struct {
	pane      *pane
	tracker   agentactivity.Tracker
	extractor agentactivity.Extractor

	// pending is set by output; checkAt is the next coalescing check and
	// pendingSince bounds the latency. seenSeq is the output sequence seen
	// at the last check.
	pending      bool
	pendingSince time.Time
	checkAt      time.Time
	seenSeq      uint64
	// deadline is the tracker's pending decision time.
	deadline time.Time

	// Identification cache: /proc is read only when the foreground pgid
	// changes or the identification is due for re-verification, after
	// agentReverifyInterval for an agent and noneRetry for no agent.
	pgid       int
	pgidKnown  bool
	agent      agentactivity.Agent
	verifiedAt time.Time
	noneRetry  time.Duration
	missed     bool

	// titleAtIdentify is the title seen when the agent was identified; the
	// title counts as agent-set once it differs.
	titleAtIdentify string
	titleKnown      bool
	titleFromAgent  bool

	// pubTab is the tab whose record holds this pane's status.
	pubTab   *tab
	stopGone func() bool

	// absorbUntil closes the window in which a deferred BEL belongs to the
	// turn that just published Done, so an end-of-turn BEL scanned after
	// the Done publish does not ring. Zero when no window is open.
	absorbUntil time.Time
}

// Inbox capacities. Each pane or tab sits in its channel at most once while
// its queued flag is set, so a send only fails with more queued items than
// capacity; that falls back to an atomic sweep flag and never blocks.
const (
	agentDirtyInbox = 256
	agentGoneInbox  = 64
	agentAckInbox   = 64
)

// agentActivityScheduler owns the shared scan goroutine and every piece of
// agent activity state. It uses no mutex: producers (the PTY read path, pane
// teardown, config reload, and paint) only flip atomic flags and make
// non-blocking channel sends; the scheduler goroutine is the only writer of
// the records below and of each tab's published agentState snapshot.
type agentActivityScheduler struct {
	d *Daemon

	// Lock-free inbox.
	dirty chan *pane
	gone  chan *pane
	acks  chan *tab
	// rescanAll, goneSweep, and ackSweep record a failed send: the scheduler
	// then sweeps every live pane, every record, or every tab record.
	rescanAll   atomic.Bool
	goneSweep   atomic.Bool
	ackSweep    atomic.Bool
	reconfigure atomic.Bool
	// reindex asks for a session index rebuild after a session's lifecycle
	// changed (an ephemeral session was named).
	reindex atomic.Bool
	wake    chan struct{}

	// sessions is the immutable per-session aggregate of every published tab
	// snapshot, keyed by lifecycle. The scheduler goroutine is its only
	// writer; status composition and the animator Load it.
	sessions atomic.Pointer[agentSessionIndex]

	// Owned by the scheduler goroutine.
	records map[*pane]*agentPaneRecord
	tabs    map[*tab]*agentTabRecord
	// indexStale records that a tab record changed session, so the next
	// republish or step rebuilds the session index.
	indexStale bool
	// hiddenScratch is the reused per-ack set of undrawn panes (see
	// doneHiddenPanes).
	hiddenScratch map[*pane]bool
	gen           uint64
	enabled       bool
	configured    bool
}

func newAgentActivityScheduler(d *Daemon) *agentActivityScheduler {
	return &agentActivityScheduler{
		d:             d,
		dirty:         make(chan *pane, agentDirtyInbox),
		gone:          make(chan *pane, agentGoneInbox),
		acks:          make(chan *tab, agentAckInbox),
		wake:          make(chan struct{}, 1),
		records:       make(map[*pane]*agentPaneRecord),
		tabs:          make(map[*tab]*agentTabRecord),
		hiddenScratch: make(map[*pane]bool),
	}
}

func (s *agentActivityScheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// markDirty records output for p. It costs one atomic add when a scan is
// already pending, never blocks, and takes no lock.
func (s *agentActivityScheduler) markDirty(p *pane) {
	p.activitySeq.Add(1)
	if p.activityDirty.Swap(true) {
		return
	}
	select {
	case s.dirty <- p:
	default:
		s.rescanAll.Store(true)
	}
	s.signal()
}

func (s *agentActivityScheduler) markGone(p *pane) {
	select {
	case s.gone <- p:
	default:
		s.goneSweep.Store(true)
	}
	s.signal()
}

// requestReconfigure makes the scheduler re-read agents.activity.
func (s *agentActivityScheduler) requestReconfigure() {
	s.reconfigure.Store(true)
	s.signal()
}

// requestAck asks the scheduler to clear the completions of tb published at
// or before gen, drawn by a paint that captured tb's paneVisibilitySeq as
// visibilitySeq. The highest requested generation (then the latest
// visibility) wins, so a coalesced or overflowed request is never lost; the
// scheduler consumes it once.
func (s *agentActivityScheduler) requestAck(tb *tab, gen, visibilitySeq uint64) {
	next := &agentAck{gen: gen, visibilitySeq: visibilitySeq}
	for {
		cur := tb.agentAck.Load()
		if cur != nil && (cur.gen > gen || cur.gen == gen && cur.visibilitySeq >= visibilitySeq) {
			break
		}
		if tb.agentAck.CompareAndSwap(cur, next) {
			break
		}
	}
	s.queueAck(tb)
}

// queueAck hands tb to the scheduler for applyAck.
func (s *agentActivityScheduler) queueAck(tb *tab) {
	if tb.agentAckQueued.Swap(true) {
		return
	}
	select {
	case s.acks <- tb:
	default:
		s.ackSweep.Store(true)
	}
	s.signal()
}

// noteAgentPaneVisibility asks the scheduler to re-check which of tb's
// unseen completions are on a drawn pane, after tb showed or hid a pane
// (see applyAck). It acknowledges nothing. Call with every lock released.
func (d *Daemon) noteAgentPaneVisibility(tb *tab) {
	if d == nil || d.agents == nil || tb == nil {
		return
	}
	d.agents.queueAck(tb)
}

// noteAgentOutput is called by the PTY read path after pane.mu is released.
func (d *Daemon) noteAgentOutput(p *pane) {
	if d == nil || d.agents == nil || p == nil || !d.currentRuntimeConfig().agents.Activity {
		return
	}
	d.agents.markDirty(p)
}

// deferAgentBell hands a terminal BEL from p to the scheduler when an
// identified agent owns the pane's attention cell, and reports whether it
// did. The scheduler resolves the bell once the turn settles (see
// resolvePendingBell): a Done or Blocked indicator replaces it, and a turn
// that ends without either rings it. agentOwned is set at the first scan
// that identifies the agent, so a BEL during agent startup rings at once; it
// is ignored once agents.activity is off, before the scheduler cleared it.
func (d *Daemon) deferAgentBell(p *pane) bool {
	if d == nil || d.agents == nil || p == nil || !p.agentOwned.Load() || !d.currentRuntimeConfig().agents.Activity {
		return false
	}
	p.pendingBell.Store(true)
	// Mark the pane dirty after the flag: a scan of the output that carried
	// the BEL may already have run, and a quiet pane must still resolve it.
	d.agents.markDirty(p)
	return true
}

// run is the scheduler goroutine. It parks on its wake channel when no pane
// has a pending deadline.
func (s *agentActivityScheduler) run(ctx context.Context) {
	var timer ports.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		for _, rec := range s.records {
			if rec.stopGone != nil {
				rec.stopGone()
			}
		}
	}()
	s.step(s.d.daemonNow())
	for {
		var timerC <-chan time.Time
		if next := s.nextDeadline(); !next.IsZero() {
			delay := max(next.Sub(s.d.daemonNow()), 0)
			if timer == nil {
				timer = s.d.clock.NewTimer(delay)
			} else {
				stopAndDrainTimer(timer)
				timer.Reset(delay)
			}
			timerC = timer.C()
		} else if timer != nil {
			stopAndDrainTimer(timer)
		}
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		case <-timerC:
		}
		s.step(s.d.daemonNow())
	}
}

func stopAndDrainTimer(timer ports.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C():
		default:
		}
	}
}

// step drains the inbox and runs every due scan at now.
func (s *agentActivityScheduler) step(now time.Time) {
	if s.reindex.Swap(false) {
		s.indexStale = true
	}
	s.drain(now)
	s.runDue(now)
	if s.indexStale {
		s.refreshSessionIndex(nil)
	}
}

// requestReindex makes the scheduler rebuild the session index, after a
// session's lifecycle changed.
func (s *agentActivityScheduler) requestReindex() {
	if s == nil {
		return
	}
	s.reindex.Store(true)
	s.signal()
}

// drain consumes the inbox. Each sweep flag is cleared before its channel is
// drained, so a send that fails afterwards is seen at the next wake.
func (s *agentActivityScheduler) drain(now time.Time) {
	sweepLive := false
	if s.reconfigure.Swap(false) || !s.configured {
		wasEnabled := s.configured && s.enabled
		s.configured = true
		s.enabled = s.d.currentRuntimeConfig().agents.Activity
		if !s.enabled {
			s.disable()
		}
		// Static screens (a pending dialog, an idle prompt) produce no
		// output: scan every live pane once when detection starts.
		sweepLive = s.enabled && !wasEnabled
	}

	goneSweep := s.goneSweep.Swap(false)
	for drained := false; !drained; {
		select {
		case p := <-s.gone:
			s.forget(p)
		default:
			drained = true
		}
	}
	if goneSweep {
		for p := range s.records {
			if paneGone(p) {
				s.forget(p)
			}
		}
	}

	rescanAll := s.rescanAll.Swap(false) || sweepLive
	for drained := false; !drained; {
		select {
		case p := <-s.dirty:
			s.noteDirty(p, now)
		default:
			drained = true
		}
	}
	if rescanAll {
		for _, p := range s.d.liveAgentPanes() {
			s.noteDirty(p, now)
		}
	}

	ackSweep := s.ackSweep.Swap(false)
	for drained := false; !drained; {
		select {
		case tb := <-s.acks:
			s.applyAck(tb)
		default:
			drained = true
		}
	}
	if ackSweep {
		for tb := range s.tabs {
			s.applyAck(tb)
		}
	}
}

func paneGone(p *pane) bool { return p.ctx != nil && p.ctx.Err() != nil }

// noteDirty starts or extends the coalescing window of p.
func (s *agentActivityScheduler) noteDirty(p *pane, now time.Time) {
	if !s.enabled || paneGone(p) {
		p.activityDirty.Store(false)
		s.ringPendingBell(p)
		return
	}
	rec := s.record(p)
	if !rec.pending {
		rec.pending = true
		rec.pendingSince = now
		rec.seenSeq = p.activitySeq.Load()
		rec.checkAt = now.Add(agentScanQuiet)
	}
}

// liveAgentPanes lists every pane of every session. It takes d.mu, each
// session.mu, and each tab.mu in turn, never two at once.
func (d *Daemon) liveAgentPanes() []*pane {
	d.mu.Lock()
	sessions := sessionsSnapshot(d.sessions)
	d.mu.Unlock()
	var out []*pane
	for _, sess := range sessions {
		sess.mu.Lock()
		tabs := append([]*tab(nil), sess.tabs...)
		sess.mu.Unlock()
		for _, tb := range tabs {
			tb.mu.Lock()
			out = append(out, tb.panesSnapshot()...)
			if fp := tb.floating.pane; fp != nil {
				out = append(out, fp)
			}
			tb.mu.Unlock()
		}
	}
	return out
}

// ringPendingBell rings the deferred BEL of p as a terminal bell on its
// current owner, for a pane the scheduler stops tracking. A gone pane drops
// it. Call with every lock released: noteAttention takes session.mu.
func (s *agentActivityScheduler) ringPendingBell(p *pane) {
	if !p.pendingBell.Swap(false) || paneGone(p) {
		return
	}
	if owner := p.ownerSnapshot(); owner != nil {
		s.d.noteAttention(owner.session, owner.tab)
	}
}

// applyAck consumes tb's pending acknowledgement, if any, and clears the
// completions the acknowledging paint showed. It also re-checks which of the
// tab's unseen completions are on a drawn pane: one that became drawn gets a
// fresh shownGen, acknowledged by the following paint, and one that became
// hidden stays unseen. An acknowledgement whose paint captured another
// paneVisibilitySeq than the current one may not have drawn a pane the tab
// draws now, so it clears nothing and leaves its completions to the next
// paint.
func (s *agentActivityScheduler) applyAck(tb *tab) {
	// Clear the queued flag before taking the acknowledgement: a later
	// request re-queues the tab.
	tb.agentAckQueued.Store(false)
	ack := tb.agentAck.Swap(nil)
	tr := s.tabs[tb]
	if tr == nil {
		return
	}
	hidden, visibilitySeq := s.doneHiddenPanes(tb, tr.panes)
	current := ack != nil && ack.visibilitySeq == visibilitySeq
	changed, redraw := false, false
	for p, status := range tr.panes {
		if !status.done {
			continue
		}
		switch {
		case hidden[p]:
			if status.hidden {
				continue
			}
			status.hidden, status.shownGen = true, 0
		case status.hidden:
			// The generation republish is about to store.
			status.hidden, status.shownGen = false, s.gen+1
			redraw = true
		case ack == nil || status.shownGen > ack.gen:
			continue
		case !current:
			status.shownGen = s.gen + 1
			redraw = true
		default:
			status.done = false
			if status.state == agentactivity.Unknown {
				delete(tr.panes, p)
				changed = true
				continue
			}
		}
		changed = true
		tr.panes[p] = status
	}
	if changed {
		s.republish(tr)
	}
	if redraw {
		// The next paint draws the fresh shownGen and acknowledges it.
		s.d.requestAgentRepaint(tr.sess)
	}
}

// doneHiddenPanes reports, for each pane of panes carrying an unseen
// completion, whether tb does not draw it, and tb's paneVisibilitySeq read in
// the same critical section. It takes tb.mu alone. The returned set is the
// scheduler's scratch map, valid until the next call.
func (s *agentActivityScheduler) doneHiddenPanes(tb *tab, panes map[*pane]agentPaneStatus) (map[*pane]bool, uint64) {
	out := s.hiddenScratch
	clear(out)
	tb.mu.Lock()
	defer tb.mu.Unlock()
	for p, status := range panes {
		if status.done {
			out[p] = paneHiddenLocked(tb, p)
		}
	}
	return out, tb.paneVisibilitySeq
}

// paneHiddenLocked reports whether tb does not draw p: p is tb's hidden
// floating pane or a collapsed stack member. The caller holds tb.mu.
func paneHiddenLocked(tb *tab, p *pane) bool {
	if tb.floating.pane == p {
		return tb.floating.state != floatingVisible
	}
	return tb.tree != nil && tb.panes[p.id] == p && layout.CollapsedLeaf(tb.tree.Root, p.id)
}

// disable forgets every pane, ringing its pending bell, and clears every
// published tab state.
func (s *agentActivityScheduler) disable() {
	for p := range s.records {
		s.forget(p)
	}
	for _, tr := range s.tabs {
		clear(tr.panes)
		s.republish(tr)
	}
}

func (s *agentActivityScheduler) record(p *pane) *agentPaneRecord {
	if rec := s.records[p]; rec != nil {
		return rec
	}
	rec := &agentPaneRecord{pane: p}
	if p.ctx != nil {
		rec.stopGone = context.AfterFunc(p.ctx, func() { s.markGone(p) })
	}
	s.records[p] = rec
	return rec
}

// tabRecord returns the record of tb, now owned by sess.
func (s *agentActivityScheduler) tabRecord(tb *tab, sess *session) *agentTabRecord {
	tr := s.tabs[tb]
	if tr == nil {
		tr = &agentTabRecord{tab: tb, panes: make(map[*pane]agentPaneStatus)}
		s.tabs[tb] = tr
	}
	if tr.sess != sess {
		s.indexStale = true
	}
	tr.sess = sess
	return tr
}

// republish stores a fresh snapshot of tr and, when the indicator changed,
// requests repaints of its session and of the sessions whose bars list it,
// and pokes the animator. An empty record is dropped.
func (s *agentActivityScheduler) republish(tr *agentTabRecord) {
	d, tb := s.d, tr.tab
	before := tb.agentIndicator()
	wasHidden := tb.agentDoneHidden()
	s.gen++
	if len(tr.panes) == 0 {
		delete(s.tabs, tb)
		tb.agentState.Store(nil)
	} else {
		state := aggregateAgentPanes(tr.panes)
		state.gen = s.gen
		tb.agentState.Store(&state)
	}
	if before != tb.agentIndicator() {
		d.requestAgentRepaint(tr.sess)
		s.indexStale = true
	} else if wasHidden != tb.agentDoneHidden() {
		// The viewed tab's cell shows Done only for undrawn panes.
		d.requestAgentRepaint(tr.sess)
	}
	if s.indexStale {
		s.refreshSessionIndex(tr.sess)
	}
	if before != tb.agentIndicator() {
		d.pokeAttentionTicker()
	}
}

// refreshSessionIndex rebuilds the session index and, for each changed
// session aggregate, repaints the other sessions whose bars may list it and
// asks the animator to refresh open pickers. skip was already repainted.
func (s *agentActivityScheduler) refreshSessionIndex(skip *session) {
	s.indexStale = false
	changed := s.publishSessionIndex()
	if len(changed) == 0 {
		return
	}
	s.d.requestRouteViewerRepaint(changed, skip)
	s.d.agentPickerRefresh.Store(true)
	s.d.pokeAttentionTicker()
}

// agentSessionIndex maps a session lifecycle to the highest-priority
// indicator of its tabs (Blocked > Done > Working). Sessions whose tabs are
// all Idle or unobserved are absent.
type agentSessionIndex map[domain.SessionLifecycleID]agentactivity.Indicator

// publishSessionIndex rebuilds the per-session index from the tab records and
// returns the lifecycles whose aggregate changed. It takes each session.mu
// once, alone, to read its lifecycle.
func (s *agentActivityScheduler) publishSessionIndex() map[domain.SessionLifecycleID]struct{} {
	lifecycles := make(map[*session]domain.SessionLifecycleID)
	next := make(agentSessionIndex)
	for tb, tr := range s.tabs {
		if tr.sess == nil {
			continue
		}
		indicator := tb.agentIndicator()
		if indicator <= agentactivity.IndicatorIdle {
			continue
		}
		lifecycle, ok := lifecycles[tr.sess]
		if !ok {
			tr.sess.mu.Lock()
			lifecycle = tr.sess.incarnation
			tr.sess.mu.Unlock()
			lifecycles[tr.sess] = lifecycle
		}
		if lifecycle == (domain.SessionLifecycleID{}) {
			continue
		}
		next[lifecycle] = agentactivity.MaxIndicator(next[lifecycle], indicator)
	}
	var prev agentSessionIndex
	if p := s.sessions.Load(); p != nil {
		prev = *p
	}
	changed := make(map[domain.SessionLifecycleID]struct{})
	for lifecycle, indicator := range next {
		if prev[lifecycle] != indicator {
			changed[lifecycle] = struct{}{}
		}
	}
	for lifecycle := range prev {
		if _, ok := next[lifecycle]; !ok {
			changed[lifecycle] = struct{}{}
		}
	}
	if len(changed) > 0 {
		s.sessions.Store(&next)
	}
	return changed
}

// sessionIndicator returns the published aggregate of the session with
// lifecycle, or IndicatorNone.
func (s *agentActivityScheduler) sessionIndicator(lifecycle domain.SessionLifecycleID) agentactivity.Indicator {
	if s == nil {
		return agentactivity.IndicatorNone
	}
	if p := s.sessions.Load(); p != nil {
		return (*p)[lifecycle]
	}
	return agentactivity.IndicatorNone
}

// agentSessionLookup returns the session aggregate reader the bars use, or
// nil when agent activity is off or nothing is published.
func (d *Daemon) agentSessionLookup() func(domain.SessionLifecycleID) agentactivity.Indicator {
	if d == nil || d.agents == nil || !d.currentRuntimeConfig().agents.Activity {
		return nil
	}
	p := d.agents.sessions.Load()
	if p == nil || len(*p) == 0 {
		return nil
	}
	index := *p
	return func(lifecycle domain.SessionLifecycleID) agentactivity.Indicator { return index[lifecycle] }
}

// routeShowsLifecycle reports whether ac's route snapshot lists one of
// lifecycles, so its bottom bar may draw that session's entry.
func routeShowsLifecycle(ac *attachedClient, lifecycles map[domain.SessionLifecycleID]struct{}) bool {
	if ac == nil || len(lifecycles) == 0 {
		return false
	}
	ac.routeMu.RLock()
	defer ac.routeMu.RUnlock()
	for _, entry := range ac.routeSnapshot.Entries {
		if _, ok := lifecycles[entry.Target.LifecycleID]; ok {
			return true
		}
	}
	return false
}

// requestRouteViewerRepaint schedules a repaint of every other session with
// an attachment whose bar lists one of lifecycles. It takes d.mu, then each
// session.mu and attachment routeMu in turn, and never paints synchronously.
func (d *Daemon) requestRouteViewerRepaint(lifecycles map[domain.SessionLifecycleID]struct{}, skip *session) {
	for _, sess := range d.sessionsSnapshot() {
		if sess == skip {
			continue
		}
		for _, ac := range sess.snapshotAttachments() {
			if routeShowsLifecycle(ac, lifecycles) {
				d.requestAgentRepaint(sess)
				break
			}
		}
	}
}

// requestAgentRepaint schedules a repaint of sess through its render
// coordinator and drops it when none is installed (nothing is attached). It
// never paints synchronously, so a slow client cannot stall the scheduler.
func (d *Daemon) requestAgentRepaint(sess *session) {
	if rc := attachmentRenderCoordinator(sess); rc != nil {
		rc.invalidate(renderInvalidation{class: invalidateUrgent, producer: "agent_activity.go"})
	}
}

// noteAgentPanesMoved re-scans panes whose owner changed, so their status is
// republished on the tab that owns them now. Call with every lock released.
func (d *Daemon) noteAgentPanesMoved(panes []*pane) {
	for _, p := range panes {
		d.noteAgentOutput(p)
	}
}

// forget drops p's record and its status. A pending bell rings unless the
// pane is gone.
func (s *agentActivityScheduler) forget(p *pane) {
	rec := s.records[p]
	if rec == nil {
		return
	}
	delete(s.records, p)
	p.activityDirty.Store(false)
	// Clear agentOwned before resolving the bell: a BEL deferred after this
	// point marks the pane dirty, and noteDirty rings or re-tracks it.
	p.agentOwned.Store(false)
	s.ringPendingBell(p)
	if rec.stopGone != nil {
		rec.stopGone()
	}
	if tr := s.tabs[rec.pubTab]; tr != nil {
		if _, ok := tr.panes[p]; ok {
			delete(tr.panes, p)
			s.republish(tr)
		}
	}
}
func (s *agentActivityScheduler) runDue(now time.Time) {
	for p, rec := range s.records {
		switch {
		case rec.pending && !now.Before(rec.checkAt):
			// Output since the last check extends the quiet window up to
			// the latency bound.
			latest := rec.pendingSince.Add(agentScanMaxLatency)
			if seq := p.activitySeq.Load(); seq != rec.seenSeq && now.Before(latest) {
				rec.seenSeq = seq
				rec.checkAt = minTime(now.Add(agentScanQuiet), latest)
				continue
			}
			s.scan(rec, now)
		case !rec.pending && !rec.deadline.IsZero() && !now.Before(rec.deadline):
			s.scan(rec, now)
		}
	}
}

// nextDeadline returns the nearest coalescing or tracker deadline, or the
// zero time when the scheduler can park.
func (s *agentActivityScheduler) nextDeadline() time.Time {
	var next time.Time
	for _, rec := range s.records {
		at := rec.deadline
		if rec.pending {
			at = rec.checkAt
		}
		if !at.IsZero() && (next.IsZero() || at.Before(next)) {
			next = at
		}
	}
	return next
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// scan observes p once: identify, extract, classify, publish.
func (s *agentActivityScheduler) scan(rec *agentPaneRecord, now time.Time) {
	p := rec.pane
	rec.pending = false
	rec.deadline = time.Time{}
	// Clear before reading the screen so output during the scan re-arms.
	p.activityDirty.Store(false)
	owner := p.ownerSnapshot()
	if owner == nil || owner.session == nil || owner.tab == nil {
		s.forget(p)
		return
	}

	agent := s.identify(rec, now)
	u := rec.tracker.Identify(agent, now)
	p.agentOwned.Store(rec.tracker.Agent() != agentactivity.None)
	if u.AgentChanged {
		rec.extractor.Forget()
		rec.titleKnown = false
		rec.titleFromAgent = false
	}
	if agent != agentactivity.None {
		title, ok := s.extract(rec)
		if !ok {
			s.forget(p)
			return
		}
		if !rec.titleKnown {
			rec.titleAtIdentify, rec.titleKnown = title, true
		} else if title != rec.titleAtIdentify {
			rec.titleFromAgent = true
		}
		var observed agentactivity.Update
		if rec.extractor.Commit(title) {
			signal := agentactivity.Classify(agent, rec.extractor.Input(title, rec.titleFromAgent))
			observed = rec.tracker.Observe(signal, now)
		} else {
			observed = rec.tracker.Tick(now)
		}
		u = mergeAgentUpdates(u, observed)
	}
	rec.deadline = u.Deadline
	s.publish(rec, owner, u, now)
}

// mergeAgentUpdates folds an identification update and the observation that
// followed it in the same scan into one transition.
func mergeAgentUpdates(identified, observed agentactivity.Update) agentactivity.Update {
	if !identified.AgentChanged {
		return observed
	}
	out := observed
	out.AgentChanged = true
	out.From = identified.From
	if out.Rule == "" {
		out.Rule = identified.Rule
	}
	return out
}

// identify returns the pane's foreground agent. A plain shell costs one
// ioctl; /proc is read only on a pgid change or a due re-verification.
func (s *agentActivityScheduler) identify(rec *agentPaneRecord, now time.Time) agentactivity.Agent {
	p, proc := rec.pane, s.d.proc
	if p.pty == nil || proc == nil {
		return agentactivity.None
	}
	pgid, err := p.pty.ForegroundPgid()
	if err != nil || pgid <= 0 {
		rec.pgidKnown = false
		return agentactivity.None
	}
	shellPid := p.pty.Pid()
	if pgid == shellPid {
		rec.pgid, rec.pgidKnown, rec.agent, rec.missed = pgid, true, agentactivity.None, false
		return agentactivity.None
	}
	samePgid := rec.pgidKnown && pgid == rec.pgid
	if samePgid {
		interval := agentReverifyInterval
		if rec.agent == agentactivity.None {
			interval = rec.noneRetry
		}
		if now.Sub(rec.verifiedAt) < interval {
			return rec.agent
		}
	}
	agent, ok := agentactivity.None, false
	// The group leader is the job the shell started; GroupArgv's member
	// search is the fallback when the leader is gone or has no argv.
	argv, err := proc.Argv(pgid)
	if err != nil || len(argv) == 0 {
		argv, err = proc.GroupArgv(pgid, shellPid)
	}
	if err == nil && len(argv) > 0 {
		agent, ok = agentactivity.Identify(argv, ""), true
	} else if comm, err := proc.Comm(pgid); err == nil && comm != "" {
		agent, ok = agentactivity.Identify(nil, comm), true
	}
	if !ok && !rec.missed {
		// The group may be exiting between the ioctl and /proc; keep the
		// previous identity for one scan and retry.
		rec.missed = true
		rec.pgidKnown = false
		return rec.agent
	}
	switch {
	case agent != agentactivity.None:
		rec.noneRetry = 0
	case samePgid && rec.agent == agentactivity.None && rec.noneRetry > 0:
		rec.noneRetry = min(max(2*rec.noneRetry, agentReverifyInterval), agentNoneReverifyMax)
	default:
		rec.noneRetry = agentNoneReverifyFirst
	}
	rec.pgid, rec.pgidKnown, rec.agent, rec.verifiedAt, rec.missed = pgid, true, agent, now, false
	return agent
}

// extract copies the classification window of p's visible grid, the rows
// ending at its lowest non-blank row, into the record's extractor under
// pane.mu and returns the terminal title.
func (s *agentActivityScheduler) extract(rec *agentPaneRecord) (string, bool) {
	p := rec.pane
	p.mu.Lock()
	defer p.mu.Unlock()
	screen := p.screen
	if screen == nil {
		return "", false
	}
	height, width := screen.Rows(), screen.Columns()
	rec.extractor.Reset()
	start, end := agentactivity.Window(height, func(y int) bool {
		for x := range width {
			if cell := screen.Cell(x, y); !agentactivity.BlankCell(cell.Rune, cell.Continuation) {
				return false
			}
		}
		return true
	})
	for y := start; y < end; y++ {
		rec.extractor.BeginRow()
		for x := range width {
			cell := screen.Cell(x, y)
			rec.extractor.WriteCell(cell.Rune, cell.Continuation)
		}
		rec.extractor.EndRow()
	}
	return p.title.terminalTitle, true
}

// publish projects the tracker state onto the record of the owning tab,
// republishes it on change, and raises attention with no lock held.
func (s *agentActivityScheduler) publish(rec *agentPaneRecord, owner *paneOwner, u agentactivity.Update, now time.Time) {
	d, p := s.d, rec.pane
	sess, tb := owner.session, owner.tab
	if u.Changed() || u.Done {
		d.log.Debug("agent activity", "pane", p.stableID, "agent", u.Agent.String(),
			"rule", u.Rule, "from", u.From.String(), "to", u.To.String(), "done", u.Done)
	}

	var carried agentPaneStatus
	hasCarried := false
	if rec.pubTab != nil && rec.pubTab != tb {
		if old := s.tabs[rec.pubTab]; old != nil {
			if carried, hasCarried = old.panes[p]; hasCarried {
				delete(old.panes, p)
				s.republish(old)
			}
		}
	}
	rec.pubTab = tb

	seen, hidden := false, false
	if u.Done {
		seen, hidden = paneMaySeen(sess, tb, p)
	}
	agent, state := rec.tracker.Agent(), rec.tracker.State()

	tr := s.tabRecord(tb, sess)
	status, ok := tr.panes[p]
	if !ok && hasCarried {
		status = carried
		if status.done {
			// The old tab's visibility and generations mean nothing here:
			// the new tab acknowledges the Done only once it draws the pane.
			tb.mu.Lock()
			status.hidden = paneHiddenLocked(tb, p)
			tb.mu.Unlock()
			status.shownGen = 0
			if !status.hidden {
				// The generation republish is about to store.
				status.shownGen = s.gen + 1
			}
		}
	}
	before := status
	status.state = state
	if agent == agentactivity.None {
		status.state = agentactivity.Unknown
	}
	if status.state == agentactivity.Working {
		status.done = false
	}
	if u.Done && !seen {
		// The generation republish is about to store.
		next := s.gen + 1
		if !status.done {
			status.doneGen = next
			status.shownGen = 0
		}
		status.done = true
		status.hidden = hidden
		switch {
		case hidden:
			status.shownGen = 0
		case status.shownGen == 0:
			status.shownGen = next
		}
	}
	raise := false
	if status.state == agentactivity.Blocked {
		raise = !status.blockedRaised
		status.blockedRaised = true
	} else {
		status.blockedRaised = false
	}
	keep := status.state != agentactivity.Unknown || status.done
	switch {
	case !keep && ok:
		delete(tr.panes, p)
		s.republish(tr)
	case !keep:
		if len(tr.panes) == 0 {
			delete(s.tabs, tb)
		}
	case !ok || status != before:
		tr.panes[p] = status
		s.republish(tr)
	}

	// One attention event covers both a Blocked raise and a rung bell.
	if rec.resolvePendingBell(u, status.state, now) {
		raise = true
	}
	if raise {
		d.noteAttention(sess, tb)
	}
}

// resolvePendingBell settles a BEL the PTY path deferred for the record's
// pane at a publish at now whose pane state is state, and reports whether it
// rings. While the agent is Working, or the tracker holds a pending decision
// (an idle hold or the startup grace), the turn is unsettled and the bell
// stays pending. A BEL resolved on a settled Blocked prompt belongs to it,
// and the Blocked attention replaces it. A turn that published Done, seen or
// not, absorbs one BEL resolved at its Done publish or at the next settled
// publish within agentBellAbsorbWindow. Any other settled bell (a short turn,
// Blocked then Idle, or a later bell) rings as a terminal bell. A new turn
// closes the absorb window. An agent change or exit settles the previous
// agent's bell at once and rings it, whatever the new agent's state: a
// Blocked new agent's prompt does not own the old agent's bell.
func (rec *agentPaneRecord) resolvePendingBell(u agentactivity.Update, state agentactivity.State, now time.Time) bool {
	if u.AgentChanged || state == agentactivity.Working {
		rec.absorbUntil = time.Time{}
	}
	if u.Done {
		rec.absorbUntil = now.Add(agentBellAbsorbWindow)
	}
	if !u.AgentChanged && (state == agentactivity.Working || !u.Deadline.IsZero()) {
		return false
	}
	absorb := now.Before(rec.absorbUntil)
	pending := rec.pane.pendingBell.Swap(false)
	if pending || !u.Done {
		// One BEL is absorbed, and the first settled publish after the
		// Done publish ends the window.
		rec.absorbUntil = time.Time{}
	}
	if u.AgentChanged {
		return pending
	}
	return pending && state != agentactivity.Blocked && !absorb
}

// tabMaySeen reports whether an attachment that may be seen shows tb.
func tabMaySeen(sess *session, tb *tab) bool {
	sess.mu.Lock()
	attachments := sess.snapshotAttachmentsLocked()
	id := domain.TabStableID(tb.stableID)
	sess.mu.Unlock()
	for _, ac := range attachments {
		if ac.viewSnapshot().tabID == id && ac.terminalFocus().MaySee() {
			return true
		}
	}
	return false
}

// paneMaySeen reports whether p may be seen, and whether tb does not draw
// p (see paneHiddenLocked). p may be seen when tb is shown to an attachment
// that may be seen and draws p: a hidden floating pane or a collapsed stack
// member is unseen even in the viewed tab. tb.mu is taken alone, after
// tabMaySeen released session.mu.
func paneMaySeen(sess *session, tb *tab, p *pane) (seen, hidden bool) {
	tb.mu.Lock()
	hidden = paneHiddenLocked(tb, p)
	tb.mu.Unlock()
	return !hidden && tabMaySeen(sess, tb), hidden
}

// ackAgentDone asks the scheduler to clear the unseen completions of the tab
// ac shows, up to the generation gen the paint rendered, when ac may be seen.
// It mirrors ackAttention without the one-visible-pulse rule: the Done of a
// drawn pane is never drawn for the tab the attachment is looking at. The
// scheduler keeps the Done of a pane the tab does not draw (see applyAck).
// Paint mutates nothing: the scheduler clears Done, republishes, and
// repaints a few ms later.
func (s *session) ackAgentDone(d *Daemon, tb *tab, ac *attachedClient, gen, visibilitySeq uint64) bool {
	if d == nil || d.agents == nil || tb == nil || ac == nil || gen == 0 || !ac.terminalFocus().MaySee() {
		return false
	}
	s.mu.Lock()
	shown := domain.TabStableID(tb.stableID) == ac.viewSnapshot().tabID
	s.mu.Unlock()
	if !shown {
		return false
	}
	d.agents.requestAck(tb, gen, visibilitySeq)
	return true
}

// agentAnimating reports whether sess has a tab showing a pulsing Blocked
// bell, a pulsing Done check to an attachment, or, with spinner set, a
// Working spinner. A Done check lasts until it is viewed, so a session no
// attachment displays keeps the animator parked.
func (sess *session) agentAnimating(spinner bool) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, tb := range sess.tabs {
		switch tb.agentIndicator() {
		case agentactivity.IndicatorBlocked:
			return true
		case agentactivity.IndicatorDone:
			if len(sess.attachments) > 0 {
				return true
			}
		case agentactivity.IndicatorWorking:
			if spinner {
				return true
			}
		}
	}
	return false
}
