package daemon

// retireTerminalRemoteView invalidates one exact remote-link generation after
// the remote daemon reports that its session detached. It removes the local
// view before interrupting transports, so stale receive callbacks, warm timers,
// and resume credentials cannot resurrect it.
func (d *Daemon) retireTerminalRemoteView(link *remoteLink, reason uint8) {
	if d == nil || link == nil || link.view == nil {
		return
	}
	view := link.view
	d.mu.Lock()
	if d.closing || !d.attachmentOwnerRegisteredByDaemonLocked(view) {
		d.mu.Unlock()
		return
	}
	view.mu.Lock()
	if view.closed || view.link != link || view.linkGeneration != link.generation {
		view.mu.Unlock()
		d.mu.Unlock()
		return
	}
	view.closed = true
	attachments := make([]*attachedClient, 0, len(view.attachments))
	for attachment := range view.attachments {
		attachments = append(attachments, attachment)
	}
	clear(view.attachments)
	warm := view.warm
	view.warm = nil
	view.warmGeneration++
	retirements := d.purgeParkedForRemoteViewLocked(view)
	d.purgeParkingForRemoteViewLocked(view)
	view.link = nil
	view.linkGeneration++
	link.active = false
	signalRemoteViewMetadataChangedLocked(view)
	_ = d.unregisterRemoteViewLocked(view)
	view.mu.Unlock()
	d.mu.Unlock()

	warm.stop()
	d.finishParkedAttachmentRetirements(retirements)
	for _, attachment := range attachments {
		d.retireShutdownRemoteAttachment(view, attachment, reason)
	}
	link.commands.FailGeneration(link.generation, errRemoteViewUnavailable)
	link.cancel()
	if link.transport != nil {
		_ = link.transport.Close()
	}
}
