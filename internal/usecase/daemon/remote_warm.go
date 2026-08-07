package daemon

import (
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

const remoteViewWarmTTL = 5 * time.Minute

// remoteViewWarm is a single generation-fenced retention timer. It owns no
// view state: callers snapshot it while holding view.mu, then stop it after
// every architecture lock is released.
type remoteViewWarm struct {
	generation uint64
	timer      ports.Timer
	done       chan struct{}
	doneOnce   sync.Once
}

func (warm *remoteViewWarm) stop() {
	if warm == nil {
		return
	}
	if warm.timer != nil {
		warm.timer.Stop()
	}
	warm.doneOnce.Do(func() { close(warm.done) })
}

// parkRemoteViewWarm retains an exact registered remote view for a bounded
// time after its final local attachment leaves. It never makes a remote view a
// daemon-liveness root: daemon shutdown revokes this timer with the registry.
func (d *Daemon) parkRemoteViewWarm(view *remoteView) {
	if d == nil || view == nil {
		return
	}
	warm := &remoteViewWarm{
		timer: d.clock.NewTimer(remoteViewWarmTTL),
		done:  make(chan struct{}),
	}

	d.mu.Lock()
	if d.closing || !d.attachmentOwnerRegisteredByDaemonLocked(view) {
		d.mu.Unlock()
		warm.stop()
		return
	}
	view.mu.Lock()
	if view.closed || len(view.attachments) != 0 {
		view.mu.Unlock()
		d.mu.Unlock()
		warm.stop()
		return
	}
	view.warmGeneration++
	warm.generation = view.warmGeneration
	old := view.warm
	view.warm = warm
	view.mu.Unlock()
	d.mu.Unlock()

	old.stop()
	d.watchRemoteViewWarm(view, warm)
}

// activateRemoteView revokes a previously scheduled expiry when an attachment
// becomes visible again. The exact-pointer/generation checks in the timer
// callback make a racing stale expiry harmless.
func (d *Daemon) activateRemoteView(view *remoteView) {
	if d == nil || view == nil {
		return
	}
	d.mu.Lock()
	if !d.attachmentOwnerRegisteredByDaemonLocked(view) {
		d.mu.Unlock()
		return
	}
	view.mu.Lock()
	warm := view.warm
	if warm != nil {
		view.warm = nil
		view.warmGeneration++
	}
	view.mu.Unlock()
	d.mu.Unlock()
	warm.stop()
}

func (d *Daemon) watchRemoteViewWarm(view *remoteView, warm *remoteViewWarm) {
	if d == nil || view == nil || warm == nil || warm.timer == nil {
		return
	}
	go func() {
		select {
		case <-warm.timer.C():
			d.expireRemoteViewWarm(view, warm)
		case <-warm.done:
		}
	}()
}

// expireRemoteViewWarm removes only the exact, still-detached registered view.
// It marks the candidate closed under registry/view locks, then interrupts and
// joins its exact remote link without holding either lock.
func (d *Daemon) expireRemoteViewWarm(view *remoteView, warm *remoteViewWarm) {
	if d == nil || view == nil || warm == nil {
		return
	}

	d.mu.Lock()
	if d.closing || !d.attachmentOwnerRegisteredByDaemonLocked(view) {
		d.mu.Unlock()
		return
	}
	view.mu.Lock()
	current := !view.closed && view.warm == warm && view.warmGeneration == warm.generation && len(view.attachments) == 0
	if !current {
		view.mu.Unlock()
		d.mu.Unlock()
		return
	}
	view.warm = nil
	view.warmGeneration++
	view.closed = true
	retirements := d.purgeParkedForRemoteViewLocked(view)
	d.purgeParkingForRemoteViewLocked(view)
	link := view.link
	view.link = nil
	view.linkGeneration++
	signalRemoteViewMetadataChangedLocked(view)
	if link != nil {
		link.active = false
	}
	_ = d.unregisterRemoteViewLocked(view)
	view.mu.Unlock()
	d.mu.Unlock()

	warm.stop()
	d.finishParkedAttachmentRetirements(retirements)
	if link == nil {
		return
	}
	interruptRemoteLink(link)
	joinRemoteLink(link)
}

// stopRemoteViewWarm clears and returns only the current timer. Its caller is
// responsible for stopping it after all architecture locks are released.
func (d *Daemon) stopRemoteViewWarm(view *remoteView) *remoteViewWarm {
	if view == nil {
		return nil
	}
	view.mu.Lock()
	warm := view.warm
	if warm != nil {
		view.warm = nil
		view.warmGeneration++
	}
	view.mu.Unlock()
	return warm
}
