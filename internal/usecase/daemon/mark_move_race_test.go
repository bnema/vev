package daemon

import (
	"sync"
	"testing"
	"testing/synctest"

	"bnema.dev/vev/internal/domain"

	"bnema.dev/vev/internal/usecase/keys"
	"github.com/stretchr/testify/require"
)

func TestMarkPromptRejectsMovedTab(t *testing.T) {
	for _, edit := range []bool{false, true} {
		name := "mark"
		if edit {
			name = "edit"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, source, ac, _, releases := newManualTabSession(t, 2)
				for _, release := range releases {
					defer release()
				}
				stopOverlayTimersAtCleanup(t, ac)
				destination, _ := addAgentTestSession(t, d, "destination")
				destination.name = "destination"
				moved := source.tabs[0]
				moved.marked = edit
				moved.markNote = "original"
				exec := paletteExec{d: d, sess: source, ac: ac}
				if edit {
					require.NoError(t, exec.EditMarkNote())
				} else {
					require.NoError(t, exec.ToggleMark())
				}
				require.NotNil(t, ac.overlays.prompt)
				require.NoError(t, d.moveTab(moveTabRequest{Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(moved.stableID), Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation}}))
				// Mutate under the new owner's lock while the old prompt submits.
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range 100 {
						destination.mu.Lock()
						moved.markNote = "destination"
						destination.mu.Unlock()
					}
				}()
				d.handlePromptInput(ac, []byte("late\r"))
				wg.Wait()
				destination.mu.Lock()
				require.Equal(t, edit, moved.marked)
				require.Equal(t, "destination", moved.markNote)
				destination.mu.Unlock()
			})
		})
	}
}

func TestConcurrentMarkToggleAndMove(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, source, ac, _, releases := newManualTabSession(t, 2)
		for _, release := range releases {
			defer release()
		}
		stopOverlayTimersAtCleanup(t, ac)
		destination, _ := addAgentTestSession(t, d, "destination")
		destination.name = "destination"
		moved := source.tabs[0]
		moved.marked = true
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); (daemonKeyHandler{d: d, ac: ac}).Action(keys.ActionToggleMark, nil) }()
		err := d.moveTab(moveTabRequest{Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(moved.stableID), Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation}})
		wg.Wait()
		require.NoError(t, err)
	})
}
