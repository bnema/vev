package client

import (
	"fmt"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// echoTickInterval re-culls pending guesses while a timing trigger may still
// fire without output, as mosh's PredictionEngine::wait_time.
const echoTickInterval = 50 * time.Millisecond

// attachmentEcho connects the prediction engine to one attached loop: it feeds
// sent input and applied output to the engine and writes the guesses through
// the foreground. It is nil when the attachment does not predict (local
// attachments, scripted foregrounds, echo.predict = never), and every method
// is a no-op on nil.
type attachmentEcho struct {
	predictor *echoPredictor
	out       attachmentPredictionForeground
	clock     ports.Clock
	tick      ports.Timer
}

func newAttachmentEcho(w *sessionAttachmentWorker, fg AttachmentForeground) *attachmentEcho {
	if w.cfg.Request.Local || w.cfg.EchoPredict == domain.EchoPredictNever {
		return nil
	}
	out, ok := fg.(attachmentPredictionForeground)
	if !ok {
		return nil
	}
	return &attachmentEcho{predictor: newEchoPredictor(w.cfg.EchoPredict), out: out, clock: w.cfg.Clock}
}

// seed mirrors output written before the loop started (the initial frame).
func (e *attachmentEcho) seed(output protocol.Output) {
	if e == nil {
		return
	}
	e.predictor.applyOutput(output, e.clock.Now())
}

// input records one session Input sent with seq.
func (e *attachmentEcho) input(seq uint64, data []byte) error {
	if e == nil {
		return nil
	}
	e.predictor.input(seq, data, e.clock.Now())
	return e.draw()
}

// output mirrors one Output after the foreground wrote it.
func (e *attachmentEcho) output(output protocol.Output) error {
	if e == nil {
		return nil
	}
	e.predictor.applyOutput(output, e.clock.Now())
	return e.draw()
}

// resized drops every guess: the next frame repaints the new geometry.
func (e *attachmentEcho) resized() {
	if e == nil {
		return
	}
	e.predictor.forget()
}

// tickC fires while a pending guess waits on a timing trigger.
func (e *attachmentEcho) tickC() <-chan time.Time {
	if e == nil || e.tick == nil {
		return nil
	}
	return e.tick.C()
}

func (e *attachmentEcho) ticked() error {
	if e == nil {
		return nil
	}
	e.tick = nil
	e.predictor.cull(e.clock.Now())
	return e.draw()
}

func (e *attachmentEcho) close() {
	if e != nil && e.tick != nil {
		e.tick.Stop()
		e.tick = nil
	}
}

func (e *attachmentEcho) draw() error {
	defer e.armTick()
	data := e.predictor.render()
	if len(data) == 0 {
		return nil
	}
	written, err := e.out.writePrediction(data)
	if err != nil {
		return fmt.Errorf("writing predicted echo: %w", err)
	}
	if !written {
		// An overlay owns the terminal: its box covers the guesses and the
		// daemon repaints everything once it closes.
		e.predictor.forget()
	}
	return nil
}

func (e *attachmentEcho) armTick() {
	if e.tick != nil || !e.predictor.needsTick() {
		return
	}
	e.tick = e.clock.NewTimer(echoTickInterval)
}
