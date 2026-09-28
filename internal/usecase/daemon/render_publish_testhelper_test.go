package daemon

// publishCapturedFrameForTest publishes a frame captured by a test that already
// holds ac.sendMu. Like paint, it releases sendMu before settling the
// publication and flushing runtime marks. It reports false only when the frame
// was rejected before preparation.
func (d *Daemon) publishCapturedFrameForTest(entry *session, ac *attachedClient, state *capturedRenderState, composed composedRenderFrame, batches ...*runtimeMarkBatch) bool {
	marks := &runtimeMarkBatch{}
	if len(batches) != 0 {
		marks = batches[0]
	} else {
		owned := d.newRuntimeMarkBatch()
		marks = &owned
	}
	followUp, published := d.publishFrameLocked(entry, ac, state, composed, marks)
	if ac != nil {
		ac.sendMu.Unlock()
	}
	if published {
		d.settleFrame(entry, ac, marks, followUp)
	}
	if len(batches) == 0 {
		marks.flush()
	}
	return published
}
