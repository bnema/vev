package daemon

import (
	"sync"
	"sync/atomic"

	"github.com/bnema/vev/internal/domain"
)

// sharedPTYClaim is one immutable attach, resume, or resize claim for shared
// Session PTY geometry. A Session retains only the latest claim per registered
// Attachment; historical claims from the same Attachment are never fallback
// candidates.
type sharedPTYClaim struct {
	attachment *attachedClient
	geometry   domain.Geometry
	sequence   uint64
}

// sharedPTYGeometry is the concrete Session Module for shared PTY geometry.
// It owns claim publication, latest-valid selection, and accepted geometry.
// Attachment, render-coordinator, Tab, Pane, floating, and Move state retain
// their independent owners and are consumed through their existing fences.
type sharedPTYGeometry struct {
	mu       sync.Mutex
	latest   atomic.Pointer[sharedPTYClaim]
	claims   map[*attachedClient]*sharedPTYClaim // session.mu -> mu
	sequence uint64                              // session.mu -> mu
	applied  domain.Geometry                     // mu
}

// claimOutranks reports whether claim a wins over b. A claim from an
// attachment whose terminal reported losing focus never wins over one that may
// be seen, so a background window cannot take geometry from the focused one.
// Otherwise the newest claim wins, as when no terminal reports focus.
func claimOutranks(a *sharedPTYClaim, aOrder uint64, b *sharedPTYClaim, bOrder uint64) bool {
	if b == nil {
		return true
	}
	aSeen, bSeen := a.attachment.terminalFocus().MaySee(), b.attachment.terminalFocus().MaySee()
	if aSeen != bSeen {
		return aSeen
	}
	return a.sequence > b.sequence || (a.sequence == b.sequence && aOrder > bOrder)
}

// latestValidClaimLocked selects the winning valid remaining Attachment claim
// (see claimOutranks). The caller holds both session.mu and g.mu.
func (g *sharedPTYGeometry) latestValidClaimLocked(core *sessionCore, exclude *attachedClient) *sharedPTYClaim {
	if g == nil || core == nil {
		return nil
	}
	var latest *sharedPTYClaim
	var latestOrder uint64
	for attachment := range core.attachments {
		if attachment == nil || attachment == exclude || !attachment.geometrySnapshot().Valid() {
			continue
		}
		claim := g.claims[attachment]
		if claim == nil || !claim.geometry.Valid() {
			continue
		}
		order := core.attachmentOrder[attachment]
		if claimOutranks(claim, order, latest, latestOrder) {
			latest = claim
			latestOrder = order
		}
	}
	return latest
}

// refreshLocked repairs an invalid publication without creating a newer
// claim. The caller holds session.mu.
func (g *sharedPTYGeometry) refreshLocked(core *sessionCore) {
	if g == nil || core == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	latest := g.latest.Load()
	if latest != nil && latest.attachment != nil && g.claims[latest.attachment] == latest &&
		latest.geometry.Valid() && latest.attachment.geometrySnapshot().Valid() {
		if _, registered := core.attachments[latest.attachment]; registered {
			return
		}
	}
	g.latest.Store(g.latestValidClaimLocked(core, nil))
}

// claimLocked records a new immutable claim and publishes the winning claim,
// which is the new one unless an unfocused attachment made it while another
// may be seen. The caller holds session.mu; claim publication then follows the
// session.mu -> geometry.mu order.
func (g *sharedPTYGeometry) claimLocked(core *sessionCore, attachment *attachedClient, geometry domain.Geometry) *sharedPTYClaim {
	if g == nil || core == nil || attachment == nil || !geometry.Valid() {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.claims == nil {
		g.claims = make(map[*attachedClient]*sharedPTYClaim)
	}
	g.sequence++
	claim := &sharedPTYClaim{
		attachment: attachment,
		geometry:   geometry.NormalizePixels(),
		sequence:   g.sequence,
	}
	g.claims[attachment] = claim
	if !g.currentHidesLocked(core, claim) {
		g.latest.Store(claim)
	}
	return claim
}

// currentHidesLocked reports whether the published claim must stay ahead of a
// newer claim: the newer one comes from an unfocused window while the current
// one is live and may be seen. The caller holds session.mu and g.mu.
func (g *sharedPTYGeometry) currentHidesLocked(core *sessionCore, claim *sharedPTYClaim) bool {
	current := g.latest.Load()
	if current == nil || current.attachment == claim.attachment {
		return false
	}
	if g.claims[current.attachment] != current {
		return false // the current claim was superseded or released
	}
	if _, registered := core.attachments[current.attachment]; !registered {
		return false
	}
	return !claim.attachment.terminalFocus().MaySee() && current.attachment.terminalFocus().MaySee()
}

// removeAttachmentLocked removes the exact Attachment claim and republishes
// the most recent valid remaining claimant. The caller holds session.mu.
func (g *sharedPTYGeometry) removeAttachmentLocked(core *sessionCore, attachment *attachedClient) {
	if g == nil || core == nil || attachment == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.claims, attachment)
	latest := g.latest.Load()
	if latest == nil || latest.attachment == attachment {
		g.latest.Store(g.latestValidClaimLocked(core, attachment))
	}
}

func (g *sharedPTYGeometry) claimAttachment(sess *session, attachment *attachedClient) (*sharedPTYClaim, bool) {
	if attachment == nil {
		return nil, false
	}
	return g.claimGeometry(sess, attachment, attachment.geometrySnapshot())
}

func (g *sharedPTYGeometry) claimSize(sess *session, attachment *attachedClient, size domain.Size) (*sharedPTYClaim, bool) {
	if attachment == nil {
		return nil, false
	}
	// Layout and floating requests carry only shared cells. Preserve the
	// claimant's complete terminal pixel pair for pane mapping and fallback.
	geometry := attachment.geometrySnapshot()
	geometry.Size = size
	return g.claimGeometry(sess, attachment, geometry)
}

func (g *sharedPTYGeometry) claimGeometry(sess *session, attachment *attachedClient, geometry domain.Geometry) (*sharedPTYClaim, bool) {
	if g == nil || sess == nil || attachment == nil || !geometry.Valid() {
		return nil, false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if _, registered := sess.attachments[attachment]; !registered {
		return nil, false
	}
	claim := g.claimLocked(sess.core(), attachment, geometry)
	return claim, claim != nil
}

// release removes a claim that did not reach coordinator admission. It removes
// only the exact current claim and never restores an older claim from the same
// Attachment.
func (g *sharedPTYGeometry) release(sess *session, claim *sharedPTYClaim) {
	if g == nil || sess == nil || claim == nil || claim.attachment == nil {
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.latest.Load() != claim || g.claims[claim.attachment] != claim {
		return
	}
	delete(g.claims, claim.attachment)
	g.latest.Store(g.latestValidClaimLocked(sess.core(), claim.attachment))
}

// sourceSnapshot returns the latest live immutable claim. Refresh takes
// session.mu only before Tab locks are involved; final transaction validation
// uses current and remains lock-free.
func (g *sharedPTYGeometry) sourceSnapshot(sess *session) (*sharedPTYClaim, bool) {
	if g == nil || sess == nil {
		return nil, false
	}
	sess.mu.Lock()
	g.refreshLocked(sess.core())
	claim := g.latest.Load()
	sess.mu.Unlock()
	if claim == nil || claim.attachment == nil || !claim.geometry.Valid() {
		return nil, false
	}
	return claim, true
}

// current is deliberately one lock-free immutable-token comparison. Session
// layout publication calls it while Tab locks and geometry.mu are held.
func (g *sharedPTYGeometry) current(claim *sharedPTYClaim) bool {
	return g != nil && claim != nil && g.latest.Load() == claim
}

func (g *sharedPTYGeometry) appliedSnapshot() domain.Geometry {
	if g == nil {
		return domain.Geometry{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.applied
}

func (g *sharedPTYGeometry) publishAppliedIfCurrent(claim *sharedPTYClaim) {
	if g == nil || claim == nil {
		return
	}
	g.mu.Lock()
	if g.latest.Load() == claim {
		g.applied = claim.geometry
	}
	g.mu.Unlock()
}

// scaleSharedPTYGeometry is the sole cell/pixel mapping policy. Integer
// division deliberately truncates because kernel winsize and CSI reports
// cannot represent fractional pixels. Session construction uses it before a
// claim can be published; live paths call paneGeometry below.
func scaleSharedPTYGeometry(full domain.Geometry, size domain.Size) domain.Geometry {
	geometry := domain.Geometry{Size: size}
	if !full.Valid() || !size.Valid() || !full.PixelsKnown() {
		return geometry
	}
	geometry.PixelWidth = int(int64(full.PixelWidth) * int64(size.Cols) / int64(full.Cols))
	geometry.PixelHeight = int(int64(full.PixelHeight) * int64(size.Rows) / int64(full.Rows))
	return geometry.NormalizePixels()
}

func (g *sharedPTYGeometry) paneGeometry(size domain.Size) domain.Geometry {
	if g == nil {
		return domain.Geometry{Size: size}
	}
	claim := g.latest.Load()
	if claim == nil {
		return domain.Geometry{Size: size}
	}
	return scaleSharedPTYGeometry(claim.geometry, size)
}

// viewport returns false when no valid Attachment claim exists.
func (g *sharedPTYGeometry) viewport(sess *session) (domain.Size, *sharedPTYClaim, bool) {
	claim, ok := g.sourceSnapshot(sess)
	if !ok {
		return domain.Size{}, nil, false
	}
	content := contentSize(claim.geometry.Size)
	return domain.Size{Cols: content.Cols, Rows: content.Rows + tabChromeRows}, claim, true
}

// reconcile applies the latest live Attachment claim to shared Session
// PTY/layout geometry. An explicit source records a new claim first.
func (g *sharedPTYGeometry) reconcile(d *Daemon, sess *session, source *attachedClient) bool {
	if sess == nil {
		return false
	}
	if source != nil {
		if _, ok := g.claimAttachment(sess, source); !ok {
			return false
		}
	}
	want, claim, ok := g.viewport(sess)
	if !ok {
		return false
	}
	current := func() bool { return g.current(claim) }
	applied := g.appliedSnapshot()
	pixelGeometryChanged := (applied.PixelsKnown() || claim.geometry.PixelsKnown()) && applied != claim.geometry
	if !pixelGeometryChanged && contentSize(sess.fullViewportSize()) == contentSize(want) {
		return false
	}
	failed, ok := g.applySessionLayout(d, sess, want, current, nil)
	if ok {
		g.publishAppliedIfCurrent(claim)
	}
	if ok && len(failed) != 0 {
		seen := make(map[*tab]struct{}, len(failed))
		for _, member := range failed {
			if member.tab == nil {
				continue
			}
			if _, exists := seen[member.tab]; exists {
				continue
			}
			seen[member.tab] = struct{}{}
			g.scheduleAcceptedTabLayoutRetry(d, sess, member.tab)
		}
	}
	return ok
}

func (g *sharedPTYGeometry) reconcileAndInvalidate(d *Daemon, sess *session, source *attachedClient, producer string) bool {
	if !g.reconcile(d, sess, source) {
		return false
	}
	d.invalidateRender(sess, nil, true, producer)
	return true
}

func (g *sharedPTYGeometry) reconcileAndInvalidateAsync(d *Daemon, sess *session, producer string) {
	if g == nil || d == nil {
		return
	}
	d.sessWg.Go(func() {
		g.reconcileAndInvalidate(d, sess, nil, producer)
	})
}
