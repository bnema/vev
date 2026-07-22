package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

type snapshotAcceptanceRepository struct {
	mu          sync.Mutex
	names       []string
	generations map[string]ports.SnapshotGeneration
	publishErr  error
	listErr     error
	loadErr     error
	publishes   []ports.SnapshotPublication
	loadMutate  func(ports.SnapshotGeneration, ports.SnapshotPublication) ports.SnapshotGeneration
}

func (r *snapshotAcceptanceRepository) Publish(ctx context.Context, p ports.SnapshotPublication) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.publishes = append(r.publishes, p)
	if r.publishErr != nil {
		return r.publishErr
	}
	objects := make(map[ports.SnapshotDigest][]byte, len(p.Objects))
	for _, object := range p.Objects {
		objects[object.Digest] = append([]byte(nil), object.Data...)
	}
	r.generations[p.Name] = ports.SnapshotGeneration{Name: p.Name, Generation: p.Generation, Manifest: append([]byte(nil), p.Manifest...), Objects: objects}
	found := false
	for _, name := range r.names {
		found = found || name == p.Name
	}
	if !found {
		r.names = append(r.names, p.Name)
	}
	return nil
}

func (r *snapshotAcceptanceRepository) List(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listErr != nil {
		return nil, r.listErr
	}
	return append([]string(nil), r.names...), nil
}

func (r *snapshotAcceptanceRepository) Load(ctx context.Context, name string) (ports.SnapshotGeneration, error) {
	if err := ctx.Err(); err != nil {
		return ports.SnapshotGeneration{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.loadErr != nil {
		return ports.SnapshotGeneration{}, r.loadErr
	}
	generation, ok := r.generations[name]
	if !ok {
		return ports.SnapshotGeneration{}, errors.New("missing generation")
	}
	if r.loadMutate != nil && len(r.publishes) > 0 {
		generation = r.loadMutate(generation, r.publishes[len(r.publishes)-1])
	}
	return cloneAcceptanceGeneration(generation), nil
}

func (*snapshotAcceptanceRepository) Delete(context.Context, string) error          { return nil }
func (*snapshotAcceptanceRepository) Tombstone(context.Context, string) error       { return nil }
func (*snapshotAcceptanceRepository) DeleteTombstone(context.Context, string) error { return nil }
func (*snapshotAcceptanceRepository) Maintain(context.Context) error                { return nil }

func cloneAcceptanceGeneration(g ports.SnapshotGeneration) ports.SnapshotGeneration {
	out := g
	out.Manifest = append([]byte(nil), g.Manifest...)
	out.Objects = make(map[ports.SnapshotDigest][]byte, len(g.Objects))
	for digest, data := range g.Objects {
		out.Objects[digest] = append([]byte(nil), data...)
	}
	return out
}

func restoreAcceptanceSession(t *testing.T, name string) snapcodec.Session {
	t.Helper()
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: 8, MaxCells: 8 * 293, ChunkRows: 2})
	for _, ch := range []rune{'a', 'b', 'c', 'd', 'e'} {
		row := make([]renderer.Cell, 293)
		for i := range row {
			row[i] = renderer.Cell{Rune: ch}
		}
		require.NoError(t, history.Append(row))
	}
	view := history.SnapshotView()
	sealed := make([][]byte, view.ChunkCount())
	for i := range sealed {
		var err error
		sealed[i], err = vt.MarshalHistoryChunk(view.Chunk(i))
		require.NoError(t, err)
	}
	tail, err := vt.MarshalHistoryTail(view)
	require.NoError(t, err)

	screen := vt.NewScreen(293, 4)
	screen.Write([]byte("primary-visible"))
	screen.Write([]byte("\x1b[?1049h"))
	screen.Write([]byte("alternate-active"))
	visible, err := screen.PrimaryVisibleSnapshot().Marshal()
	require.NoError(t, err)

	paneID := layout.PaneID("pane-7")
	return snapcodec.Session{
		Name: name, CreatedAt: 77, Active: 0,
		Tabs: []snapcodec.Tab{{
			StableID: "tab-stable", Cols: 293, Rows: 4, NextPaneID: 8, Focus: paneID,
			Tree: &layout.Tree{Root: layout.NewLeaf(paneID), Focus: paneID},
			Panes: []snapcodec.Pane{{
				ID: paneID, StableID: "pane-stable", Cwd: "/snapshot/cwd", SealedChunks: sealed, Tail: tail, Visible: visible,
				Process: &snapcodec.Process{Argv: []string{"sleep", "99"}, Strategy: "argv"},
			}},
		}},
	}
}

func acceptanceGeneration(t *testing.T, snapshot snapcodec.Session, generation uint64) ports.SnapshotGeneration {
	t.Helper()
	publication, err := legacyPublication(snapshot)
	require.NoError(t, err)
	manifest, err := snapcodec.UnmarshalManifest(publication.Manifest)
	require.NoError(t, err)
	manifest.Generation = generation
	encoded, err := snapcodec.MarshalManifest(manifest)
	require.NoError(t, err)
	objects := make(map[ports.SnapshotDigest][]byte, len(publication.Objects))
	for _, object := range publication.Objects {
		objects[object.Digest] = append([]byte(nil), object.Data...)
	}
	return ports.SnapshotGeneration{Name: snapshot.Name, Generation: generation, Manifest: encoded, Objects: objects}
}

func TestRestoreIncrementalGenerationAcceptance(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "restored")
	generation := acceptanceGeneration(t, snapshot, 9)
	repository := &snapshotAcceptanceRepository{names: []string{snapshot.Name}, generations: map[string]ports.SnapshotGeneration{snapshot.Name: generation}}
	pty, release := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	store, _ := newMockStore(t)
	WithStore(store)(d)
	WithSnapshotRepository(repository, nil)(d)
	t.Cleanup(func() { release(); d.sessWg.Wait() })

	d.restoreIncrementalSnapshots(context.Background())

	d.mu.Lock()
	restored := d.findByNameLocked(snapshot.Name)
	d.mu.Unlock()
	require.NotNil(t, restored)
	require.False(t, restored.snapDirty.Load(), "a loaded generation must begin clean")
	require.Equal(t, 0, restored.active)
	require.Equal(t, "/snapshot/cwd", restored.cwd)
	require.Len(t, restored.tabs, 1)
	tab := restored.tabs[0]
	require.Equal(t, 293, tab.size.Cols)
	require.Equal(t, layout.PaneID("pane-7"), tab.tree.Focus)
	pane := tab.panes[layout.PaneID("pane-7")]
	pane.mu.Lock()
	require.Equal(t, "primary-visible", string([]rune{pane.screen.Frame.Row(0)[0].Rune, pane.screen.Frame.Row(0)[1].Rune, pane.screen.Frame.Row(0)[2].Rune, pane.screen.Frame.Row(0)[3].Rune, pane.screen.Frame.Row(0)[4].Rune, pane.screen.Frame.Row(0)[5].Rune, pane.screen.Frame.Row(0)[6].Rune, pane.screen.Frame.Row(0)[7].Rune, pane.screen.Frame.Row(0)[8].Rune, pane.screen.Frame.Row(0)[9].Rune, pane.screen.Frame.Row(0)[10].Rune, pane.screen.Frame.Row(0)[11].Rune, pane.screen.Frame.Row(0)[12].Rune, pane.screen.Frame.Row(0)[13].Rune, pane.screen.Frame.Row(0)[14].Rune}))
	require.Equal(t, 5, pane.history.Len())
	require.Equal(t, 5*293, pane.history.Cells())
	require.Equal(t, defaultScrollbackRows, pane.history.Cap())
	require.Equal(t, defaultScrollbackCells, pane.history.CellCap())
	pane.mu.Unlock()
}

func TestRestoreIncrementalFallbackAndInvalidObjectMappings(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "fallback")
	valid := acceptanceGeneration(t, snapshot, 3)
	valid.Fallback = true
	pty, release := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	store, _ := newMockStore(t)
	WithStore(store)(d)
	repository := &snapshotAcceptanceRepository{names: []string{snapshot.Name}, generations: map[string]ports.SnapshotGeneration{snapshot.Name: valid}}
	WithSnapshotRepository(repository, nil)(d)
	t.Cleanup(func() { release(); d.sessWg.Wait() })
	d.restoreIncrementalSnapshots(context.Background())
	require.Len(t, d.notices.history(), 1)
	require.Equal(t, domain.NoticeWarn, d.notices.history()[0].Severity)

	for _, mutate := range []struct {
		name string
		fn   func(*ports.SnapshotGeneration)
	}{
		{"missing", func(g *ports.SnapshotGeneration) {
			for digest := range g.Objects {
				delete(g.Objects, digest)
				break
			}
		}},
		{"wrong kind", func(g *ports.SnapshotGeneration) {
			for digest, data := range g.Objects {
				kind, payload, err := snapcodec.UnmarshalObject(data)
				require.NoError(t, err)
				if kind == snapcodec.HistoryTail {
					replacement, err := snapcodec.MarshalObject(snapcodec.Visible, payload)
					require.NoError(t, err)
					delete(g.Objects, digest)
					g.Objects[digest] = replacement.Data
					break
				}
			}
		}},
		{"wrong digest", func(g *ports.SnapshotGeneration) {
			for digest, data := range g.Objects {
				kind, payload, err := snapcodec.UnmarshalObject(data)
				require.NoError(t, err)
				payload = append([]byte(nil), payload...)
				payload[len(payload)-1] ^= 1
				replacement, err := snapcodec.MarshalObject(kind, payload)
				require.NoError(t, err)
				require.Equal(t, len(data), len(replacement.Data))
				g.Objects[digest] = replacement.Data
				break
			}
		}},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			generation := cloneAcceptanceGeneration(valid)
			generation.Fallback = false
			mutate.fn(&generation)
			_, err := sessionFromGeneration(generation)
			require.Error(t, err)
		})
	}
}
