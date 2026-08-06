package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/vt"
)

const (
	defaultProbeCols = 80
	defaultProbeRows = 24

	maxProbeEvents      = 128
	maxProbeCheckpoints = 16
	maxProbeTraces      = 128
	maxArtifactBytes    = 64 << 10

	probeEventOutput       = "output"
	probeEventWelcome      = "welcome"
	probeEventAttachTarget = "attach_target"
	probeEventOutputReset  = "output_reset_request"
)

// visualOutputState mirrors the client's output dependency chain. A probe may
// inspect output only after the frame's epoch, base, and view revision continue
// the last accepted state.
type visualOutputState struct {
	epoch        uint64
	state        uint64
	viewRevision uint64
	initialized  bool
}

func (s visualOutputState) next(output ports.Output) (visualOutputState, bool) {
	if output.Epoch == 0 {
		return visualOutputState{}, false
	}
	if !s.initialized {
		if output.New == 0 {
			return s, true
		}
		if output.Base != 0 || !output.Full {
			return visualOutputState{}, false
		}
		return visualOutputState{epoch: output.Epoch, state: output.New, viewRevision: output.ViewRevision, initialized: true}, true
	}
	if output.Epoch < s.epoch {
		return visualOutputState{}, false
	}
	if output.Epoch == s.epoch {
		if output.ViewRevision != s.viewRevision {
			return visualOutputState{}, false
		}
		if output.New == 0 {
			if output.Base != 0 || output.Full {
				return visualOutputState{}, false
			}
			return s, true
		}
		if output.Full || output.Base != s.state || output.New != output.Base+1 {
			return visualOutputState{}, false
		}
		return visualOutputState{epoch: s.epoch, state: output.New, viewRevision: s.viewRevision, initialized: true}, true
	}
	if !output.Full || output.Base != 0 || output.New == 0 {
		return visualOutputState{}, false
	}
	return visualOutputState{epoch: output.Epoch, state: output.New, viewRevision: output.ViewRevision, initialized: true}, true
}

type probeEvent struct {
	Sequence     uint64
	Transport    string
	Kind         string
	Accepted     bool
	StateBearing bool
	Acked        bool
	Epoch        uint64
	Base         uint64
	New          uint64
	ViewRevision uint64
	Full         bool
	DataBytes    int
}

type probeTrace struct {
	events []*probeEvent
}

func (t *probeTrace) record(event *probeEvent) {
	if t == nil || event == nil {
		return
	}
	if len(t.events) == maxProbeTraces {
		t.events = t.events[1:]
	}
	t.events = append(t.events, event)
}

type visualCheckpoint struct {
	event    *probeEvent
	state    visualOutputState
	snapshot vt.ScreenSnapshot
}

type visualOutputResult struct {
	Accepted     bool
	StateBearing bool
	Ack          ports.Ack
	event        *probeEvent
}

// visualProbe is deliberately transport-local. A new physical transport gets
// one screen and one output state; waiting for a particular marker never
// creates a replacement screen and therefore cannot lose prior terminal state.
type visualProbe struct {
	label string

	screen *vt.Screen
	state  visualOutputState

	events      []*probeEvent
	checkpoints []visualCheckpoint
	trace       *probeTrace
	nextEvent   uint64

	outputFrames int
	outputBytes  int
	resetPending bool
}

type harnessArtifactContextKey struct{}

func withHarnessArtifact(ctx context.Context, artifact *harnessArtifact) context.Context {
	return context.WithValue(ctx, harnessArtifactContextKey{}, artifact)
}

func harnessArtifactFromContext(ctx context.Context) *harnessArtifact {
	if ctx == nil {
		return nil
	}
	artifact, _ := ctx.Value(harnessArtifactContextKey{}).(*harnessArtifact)
	return artifact
}

func newVisualProbe(size domain.Size) *visualProbe {
	width, height := size.Cols, size.Rows
	if width <= 0 {
		width = defaultProbeCols
	}
	if height <= 0 {
		height = defaultProbeRows
	}
	return &visualProbe{screen: vt.NewScreen(width, height)}
}

func (p *visualProbe) configure(label string, trace *probeTrace) {
	if p == nil {
		return
	}
	p.label = label
	p.trace = trace
}

func (p *visualProbe) record(event probeEvent) *probeEvent {
	if p == nil {
		return nil
	}
	p.nextEvent++
	event.Sequence = p.nextEvent
	event.Transport = p.label
	owned := &event
	if len(p.events) == maxProbeEvents {
		p.events = p.events[1:]
	}
	p.events = append(p.events, owned)
	if p.trace != nil {
		p.trace.record(owned)
	}
	return owned
}

func (p *visualProbe) recordControl(kind string) {
	if p == nil {
		return
	}
	p.record(probeEvent{Kind: kind, Accepted: true})
}

func (p *visualProbe) apply(output ports.Output) visualOutputResult {
	if p == nil {
		return visualOutputResult{}
	}
	p.outputFrames++
	p.outputBytes += len(output.Data)
	accepted := ports.ValidateOutput(output) == nil
	next := visualOutputState{}
	if accepted {
		next, accepted = p.state.next(output)
	}
	event := p.record(probeEvent{
		Kind:         probeEventOutput,
		Accepted:     accepted,
		StateBearing: output.New != 0,
		Epoch:        output.Epoch,
		Base:         output.Base,
		New:          output.New,
		ViewRevision: output.ViewRevision,
		Full:         output.Full,
		DataBytes:    len(output.Data),
	})
	if !accepted {
		return visualOutputResult{event: event}
	}

	// The client writes side effects as well as state-bearing frames, but only
	// state-bearing frames advance the dependency chain and receive an ACK.
	p.screen.Write(output.Data)
	p.state = next
	checkpoint := visualCheckpoint{event: event, state: p.state, snapshot: p.screen.Snapshot()}
	if len(p.checkpoints) == maxProbeCheckpoints {
		p.checkpoints = p.checkpoints[1:]
	}
	p.checkpoints = append(p.checkpoints, checkpoint)
	result := visualOutputResult{Accepted: true, StateBearing: output.New != 0, event: event}
	if result.StateBearing {
		result.Ack = ports.Ack{Epoch: output.Epoch, State: output.New}
	}
	return result
}

func (p *visualProbe) text() string {
	if p == nil || p.screen == nil {
		return ""
	}
	return visualScreenText(p.screen)
}

func (p *visualProbe) contains(want string) bool {
	return strings.Contains(p.text(), want)
}

func (p *visualProbe) markAcked(result visualOutputResult) {
	if result.event != nil && result.StateBearing && result.Accepted {
		result.event.Acked = true
	}
}

func visualScreenText(screen *vt.Screen) string {
	if screen == nil {
		return ""
	}
	var text strings.Builder
	for y := range screen.Frame.Height {
		for x := range screen.Frame.Width {
			r := screen.Frame.At(x, y).Rune
			if r == 0 {
				r = ' '
			}
			text.WriteRune(r)
		}
		text.WriteByte('\n')
	}
	return text.String()
}

func validateHandoffEventOrdering(events []*probeEvent) error {
	acceptedState := func(event *probeEvent) bool {
		return event != nil && event.Kind == probeEventOutput && event.Accepted && event.StateBearing && event.Acked
	}
	localOutput := eventIndex(events, func(event *probeEvent) bool {
		return event.Transport == "local-picker" && acceptedState(event)
	})
	handoff := eventIndex(events, func(event *probeEvent) bool {
		return event.Transport == "local-picker" && event.Kind == probeEventAttachTarget
	})
	selectedWelcome := eventIndex(events, func(event *probeEvent) bool {
		return event.Transport == "selected-remote" && event.Kind == probeEventWelcome
	})
	selectedOutput := eventIndex(events, func(event *probeEvent) bool {
		return event.Transport == "selected-remote" && acceptedState(event)
	})
	if localOutput < 0 || handoff < 0 || selectedWelcome < 0 || selectedOutput < 0 {
		return fmt.Errorf("handoff trace omitted local output, direct handoff, selected welcome, or selected output")
	}
	if !(localOutput < handoff && handoff < selectedWelcome && selectedWelcome < selectedOutput) {
		return fmt.Errorf("handoff trace order was not local output, direct handoff, selected welcome, selected output")
	}
	return nil
}

func eventIndex(events []*probeEvent, match func(*probeEvent) bool) int {
	for i, event := range events {
		if event != nil && match(event) {
			return i
		}
	}
	return -1
}

type harnessArtifact struct {
	dir    string
	probes []*visualProbe
	traces []*probeTrace
}

func newHarnessArtifact(dir string) *harnessArtifact {
	if dir == "" {
		return nil
	}
	return &harnessArtifact{dir: dir}
}

func (a *harnessArtifact) registerProbe(probe *visualProbe) {
	if a == nil || probe == nil {
		return
	}
	a.probes = append(a.probes, probe)
}

func (a *harnessArtifact) registerTrace(trace *probeTrace) {
	if a == nil || trace == nil {
		return
	}
	a.traces = append(a.traces, trace)
}

type artifactEvent struct {
	Sequence     uint64 `json:"sequence"`
	Transport    string `json:"transport"`
	Kind         string `json:"kind"`
	Accepted     bool   `json:"accepted"`
	StateBearing bool   `json:"state_bearing"`
	Acked        bool   `json:"acked"`
	Epoch        uint64 `json:"epoch,omitempty"`
	Base         uint64 `json:"base,omitempty"`
	New          uint64 `json:"new,omitempty"`
	ViewRevision uint64 `json:"view_revision,omitempty"`
	Full         bool   `json:"full,omitempty"`
	DataBytes    int    `json:"data_bytes,omitempty"`
}

type artifactCheckpoint struct {
	Sequence           uint64 `json:"sequence"`
	Epoch              uint64 `json:"epoch,omitempty"`
	State              uint64 `json:"state,omitempty"`
	ViewRevision       uint64 `json:"view_revision,omitempty"`
	Columns            int    `json:"columns"`
	Rows               int    `json:"rows"`
	CursorRow          int    `json:"cursor_row"`
	CursorColumn       int    `json:"cursor_column"`
	AlternateScreen    bool   `json:"alternate_screen"`
	BracketedPaste     bool   `json:"bracketed_paste"`
	SynchronizedUpdate bool   `json:"synchronized_update"`
}

type artifactProbe struct {
	Transport   string               `json:"transport"`
	Events      []artifactEvent      `json:"events"`
	Checkpoints []artifactCheckpoint `json:"checkpoints"`
}

type artifactDocument struct {
	Version int               `json:"version"`
	Passed  bool              `json:"passed"`
	Probes  []artifactProbe   `json:"probes"`
	Traces  [][]artifactEvent `json:"traces,omitempty"`
}

func (a *harnessArtifact) document(passed bool) artifactDocument {
	document := artifactDocument{Version: 1, Passed: passed}
	for _, probe := range a.probes {
		if probe == nil {
			continue
		}
		item := artifactProbe{Transport: probe.label}
		for _, event := range probe.events {
			if event == nil {
				continue
			}
			item.Events = append(item.Events, artifactEventFrom(event))
		}
		for _, checkpoint := range probe.checkpoints {
			snapshot := checkpoint.snapshot
			cursor := snapshot.Cursor()
			modes := snapshot.Modes()
			item.Checkpoints = append(item.Checkpoints, artifactCheckpoint{
				Sequence:           checkpoint.event.Sequence,
				Epoch:              checkpoint.state.epoch,
				State:              checkpoint.state.state,
				ViewRevision:       checkpoint.state.viewRevision,
				Columns:            snapshot.Columns(),
				Rows:               snapshot.Rows(),
				CursorRow:          cursor.Row,
				CursorColumn:       cursor.Col,
				AlternateScreen:    modes.AlternateScreen,
				BracketedPaste:     modes.BracketedPaste,
				SynchronizedUpdate: modes.SynchronizedUpdate,
			})
		}
		document.Probes = append(document.Probes, item)
	}
	for _, trace := range a.traces {
		if trace == nil {
			continue
		}
		var events []artifactEvent
		for _, event := range trace.events {
			if event != nil {
				events = append(events, artifactEventFrom(event))
			}
		}
		document.Traces = append(document.Traces, events)
	}
	return document
}

func artifactEventFrom(event *probeEvent) artifactEvent {
	return artifactEvent{
		Sequence:     event.Sequence,
		Transport:    event.Transport,
		Kind:         event.Kind,
		Accepted:     event.Accepted,
		StateBearing: event.StateBearing,
		Acked:        event.Acked,
		Epoch:        event.Epoch,
		Base:         event.Base,
		New:          event.New,
		ViewRevision: event.ViewRevision,
		Full:         event.Full,
		DataBytes:    event.DataBytes,
	}
}

func (a *harnessArtifact) write(passed bool) error {
	if a == nil || a.dir == "" {
		return nil
	}
	data, err := json.MarshalIndent(a.document(passed), "", "  ")
	if err != nil {
		return fmt.Errorf("marshal harness artifact: %w", err)
	}
	if len(data) > maxArtifactBytes {
		return fmt.Errorf("harness artifact exceeds %d bytes", maxArtifactBytes)
	}
	if err := os.MkdirAll(a.dir, 0o700); err != nil {
		return fmt.Errorf("create harness artifact directory: %w", err)
	}
	tmp, err := os.CreateTemp(a.dir, ".remote-picker-harness-*.tmp")
	if err != nil {
		return fmt.Errorf("create harness artifact: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("protect harness artifact: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write harness artifact: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close harness artifact: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(a.dir, "remote-picker-harness.json")); err != nil {
		return fmt.Errorf("publish harness artifact: %w", err)
	}
	return nil
}
