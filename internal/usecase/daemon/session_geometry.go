package daemon

import "github.com/bnema/vev/internal/domain"

// sessionGeometrySource identifies the attachment whose latest attach,
// resume, or resize claim controls shared PTY/layout geometry. claim is zero
// only for legacy/headless test fixtures that construct attachment maps
// directly instead of publishing them through the attachment registry.
type sessionGeometrySource struct {
	attachment *attachedClient
	geometry   domain.Geometry
	claim      uint64
}

// geometrySourceSnapshot returns the latest live attachment claim. The
// attachment registry publishes geometryOwner under the session lock, and this
// function reads the owner, claim, and claim size under that same lock. Later
// revalidation at the layout publication fence is lock-free so a stale claim
// can be rejected without taking session.mu.
func (s *session) geometrySourceSnapshot() (sessionGeometrySource, bool) {
	if s == nil {
		return sessionGeometrySource{}, false
	}

	s.mu.Lock()
	s.refreshGeometryOwnerLocked()
	owner := s.geometryOwner.Load()
	var geometry domain.Geometry
	var claim uint64
	if owner != nil {
		geometry = owner.geometryClaimSnapshot()
		claim = owner.geometryClaim.Load()
	}
	s.mu.Unlock()

	if owner == nil || !geometry.Valid() {
		return sessionGeometrySource{}, false
	}
	return sessionGeometrySource{attachment: owner, geometry: geometry, claim: claim}, true
}

// geometryClaimTokenCurrent is deliberately lock-free. applySessionLayout invokes
// its current callback while tab locks are held, so taking session.mu here
// would reverse the session -> tab ordering used by layout mutation paths.
func (s *session) geometryClaimTokenCurrent(attachment *attachedClient, claim uint64) bool {
	if s == nil || attachment == nil || claim == 0 {
		return true
	}
	return s.geometryOwner.Load() == attachment && attachment.geometryClaim.Load() == claim
}

func (s *session) geometryClaimCurrent(source sessionGeometrySource) bool {
	return s.geometryClaimTokenCurrent(source.attachment, source.claim) &&
		(source.attachment == nil || source.attachment.geometryClaimSnapshot() == source.geometry)
}

// paneGeometry maps the controlling terminal's optional pixel dimensions to a
// pane's cell rectangle. Integer division deliberately truncates: the kernel
// winsize and CSI reports cannot represent fractional pixels.
// scalePaneGeometry maps a full terminal claim to one pane rectangle. Pixel
// dimensions are deliberately scaled with integer truncation, matching the
// mapping used by paneGeometry, but it is also usable while a session is still
// private and has not published its attachment owner.
func scalePaneGeometry(full domain.Geometry, size domain.Size) domain.Geometry {
	geometry := domain.Geometry{Size: size}
	if !full.Valid() || !size.Valid() || !full.PixelsKnown() {
		return geometry
	}
	geometry.PixelWidth = int(int64(full.PixelWidth) * int64(size.Cols) / int64(full.Cols))
	geometry.PixelHeight = int(int64(full.PixelHeight) * int64(size.Rows) / int64(full.Rows))
	return geometry.NormalizePixels()
}

func (s *session) paneGeometry(size domain.Size) domain.Geometry {
	geometry := domain.Geometry{Size: size}
	if s == nil || !size.Valid() {
		return geometry
	}
	owner := s.geometryOwner.Load()
	if owner == nil {
		return geometry
	}
	return scalePaneGeometry(owner.geometryClaimSnapshot(), size)
}

// sessionGeometryViewport is called with a non-nil session by the daemon
// geometry paths; it returns ok == false when no valid claim exists.
func sessionGeometryViewport(sess *session) (domain.Size, sessionGeometrySource, bool) {
	source, ok := sess.geometrySourceSnapshot()
	if !ok {
		return domain.Size{}, sessionGeometrySource{}, false
	}
	content := contentSize(source.geometry.Size)
	return domain.Size{Cols: content.Cols, Rows: content.Rows + tabChromeRows}, source, true
}

// recalculateSessionGeometry reconciles shared PTY/layout geometry with the
// latest live attachment claim. An explicit source records a new claim before
// the snapshot; attach and resume paths already claimed ownership when they
// entered the attachment registry. If the owner detaches, unregistering the
// attachment selects the most recently claimed remaining attachment.
func (d *Daemon) recalculateSessionGeometry(sess *session, source *attachedClient) bool {
	if sess == nil {
		return false
	}
	if source != nil {
		if _, ok := sess.claimGeometryOwner(source); !ok {
			return false
		}
	}
	want, owner, ok := sessionGeometryViewport(sess)
	if !ok {
		return false
	}
	current := func() bool { return sess.geometryClaimCurrent(owner) }
	sess.geometryMu.Lock()
	appliedGeometry := sess.appliedGeometry
	sess.geometryMu.Unlock()
	pixelGeometryChanged := (appliedGeometry.PixelsKnown() || owner.geometry.PixelsKnown()) && appliedGeometry != owner.geometry
	if !pixelGeometryChanged && contentSize(sess.fullViewportSize()) == contentSize(want) {
		return false
	}
	failed, ok := d.applySessionLayout(sess, want, current, nil)
	if ok {
		sess.geometryMu.Lock()
		if current() {
			sess.appliedGeometry = owner.geometry
		}
		sess.geometryMu.Unlock()
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
			d.scheduleAcceptedTabLayoutRetry(sess, member.tab)
		}
	}
	return ok
}

// recalculateSessionGeometryAndInvalidate publishes a shared render wake after
// canonical geometry changes. A nil source uses the latest attach/resume/resize
// claim already published by the attachment registry. The wake is asynchronous
// so a slow peer cannot block a resize or detach.
func (d *Daemon) recalculateSessionGeometryAndInvalidate(sess *session, source *attachedClient, producer string) bool {
	if !d.recalculateSessionGeometry(sess, source) {
		return false
	}
	d.invalidateRender(sess, nil, true, producer)
	return true
}

func (d *Daemon) recalculateSessionGeometryAndInvalidateAsync(sess *session, producer string) {
	if d == nil {
		return
	}
	d.sessWg.Go(func() {
		d.recalculateSessionGeometryAndInvalidate(sess, nil, producer)
	})
}
