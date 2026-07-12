package daemon

import (
	"errors"
	"io"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

type cacheFailTransport struct{}

func (cacheFailTransport) Send(ports.Frame) error     { return errors.New("send failed") }
func (cacheFailTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (cacheFailTransport) Close() error               { return nil }

func TestPipelineCachePublishesOnlyAfterEmission(t *testing.T) {
	for _, failure := range []struct {
		name  string
		apply func(*composedRenderFrame, *attachedClient)
	}{
		{
			name: "prepare",
			apply: func(composed *composedRenderFrame, _ *attachedClient) {
				// Frame validation fails in output.prepare after composition.
				composed.frame = renderer.Frame{Width: 1}
			},
		},
		{
			name:  "send",
			apply: func(_ *composedRenderFrame, ac *attachedClient) { ac.replaceTransport(cacheFailTransport{}) },
		},
	} {
		t.Run(failure.name, func(t *testing.T) {
			d, sess, ac, sends := newManualSessionWithPTYs(t)
			healthy := ac.transport()
			committed := composeFrame(cacheState("committed", 1), composeCacheInput{})
			ac.pipelineCache = committed.cache
			before := cloneComposeCache(ac.pipelineCache)
			pending := composeFrame(cacheState("next", 2), ac.pipelineCache, ac.pipelineScratch)
			failure.apply(&pending, ac)

			state := cacheState("next", 2)
			state.attachment = ac
			ac.sendMu.Lock()
			require.True(t, d.emitFrame(sess, ac, &state, pending))
			require.Equal(t, before, cloneComposeCache(ac.pipelineCache), "failed emission must not publish any composed cache backing storage")

			// A send error detaches the failed link. Re-own this test attachment with
			// its original healthy transport, then retry the pending state.
			if failure.name == "send" {
				sess.mu.Lock()
				sess.client = ac
				sess.mu.Unlock()
				ac.setSession(sess)
				ac.replaceTransport(healthy)
			}
			pending = composeFrame(cacheState("next", 2), ac.pipelineCache, ac.pipelineScratch)
			state = cacheState("next", 2)
			state.attachment = ac
			ac.sendMu.Lock()
			require.True(t, d.emitFrame(sess, ac, &state, pending))
			require.Equal(t, pending.cache, ac.pipelineCache)
			frame := <-sends
			output, err := ports.UnmarshalOutput(frame.Payload)
			require.NoError(t, err)
			require.Contains(t, string(output.Data), "next", "the retry must emit state retained after the failed emission")
		})
	}
}

func cacheState(title string, generation uint64) capturedRenderState {
	pane := renderer.NewFrame(6, 1)
	for x, r := range title[:min(len(title), 6)] {
		pane.Set(x, 0, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
	}
	floating := renderer.NewFrame(2, 1)
	floating.Set(0, 0, renderer.Cell{Rune: 'F', Style: renderer.DefaultStyle()})
	return capturedRenderState{
		reset:    false,
		layout:   capturedTabLayout{area: domain.Rect{Width: 6, Height: 3}, valid: true, fingerprint: "layout"},
		panes:    []capturedPaneRenderState{{id: "pane", frame: pane, placement: layout.Placement{ID: "pane", Content: domain.Rect{Width: 6, Height: 1}, TitleBar: domain.Rect{Width: 6, Height: 1}}, title: title, titleGeneration: generation, focused: true, damage: []renderer.Damage{renderer.FullRedraw()}}},
		floating: capturedFloatingRenderState{visible: true, pane: capturedPaneRenderState{frame: floating, damage: []renderer.Damage{renderer.FullRedraw()}}, geometry: floatingGeometry{Bounds: domain.Rect{X: 2, Y: 1, Width: 2, Height: 1}, Inner: domain.Rect{X: 2, Y: 1, Width: 2, Height: 1}}, generation: generation, titleGeneration: generation},
		bars:     barState{topRight: title, bottomRight: title},
	}
}

func cloneComposeCache(in composeCacheInput) composeCacheInput {
	out := in
	out.frame = in.frame.Clone()
	out.titleGenerations = make(map[layout.PaneID]uint64, len(in.titleGenerations))
	for id, generation := range in.titleGenerations {
		out.titleGenerations[id] = generation
	}
	out.damage = append([]renderer.Damage(nil), in.damage...)
	out.bars.top = append([]renderer.Cell(nil), in.bars.top...)
	out.bars.bottom = append([]renderer.Cell(nil), in.bars.bottom...)
	return out
}
