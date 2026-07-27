package daemon

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

type snapshotAcceptanceRepository struct {
	noOpSnapshotRepository
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
	r.generations[p.Name] = ports.SnapshotGeneration{IncarnationID: p.IncarnationID, Name: p.Name, Generation: p.Generation, ParentCheckpoint: p.ParentCheckpoint, Manifest: append([]byte(nil), p.Manifest...), Objects: objects}
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
	generation = cloneAcceptanceGeneration(generation)
	if r.loadMutate != nil && len(r.publishes) > 0 {
		generation = r.loadMutate(generation, r.publishes[len(r.publishes)-1])
	}
	return cloneAcceptanceGeneration(generation), nil
}

func (r *snapshotAcceptanceRepository) LoadCheckpoint(ctx context.Context, id domain.IncarnationID, name string, ref ports.CheckpointRef) (ports.SnapshotGeneration, error) {
	generation, err := r.Load(ctx, name)
	if err != nil {
		return ports.SnapshotGeneration{}, err
	}
	if generation.IncarnationID != id || generation.Generation != ref.Generation || snapcodec.ManifestDigest(generation.Manifest) != ref.ManifestDigest {
		return ports.SnapshotGeneration{}, errors.New("checkpoint unavailable")
	}
	return generation, nil
}

func TestSnapshotAcceptanceRepositoryLoadMutationDoesNotChangeStoredGeneration(t *testing.T) {
	digest := ports.SnapshotDigest{1}
	stored := ports.SnapshotGeneration{
		Name:     "work",
		Manifest: []byte("manifest"),
		Objects:  map[ports.SnapshotDigest][]byte{digest: []byte("object")},
	}
	repository := &snapshotAcceptanceRepository{
		generations: map[string]ports.SnapshotGeneration{"work": stored},
		publishes:   []ports.SnapshotPublication{{Name: "work"}},
		loadMutate: func(g ports.SnapshotGeneration, _ ports.SnapshotPublication) ports.SnapshotGeneration {
			g.Manifest[0] = 'M'
			g.Objects[digest][0] = 'O'
			delete(g.Objects, digest)
			return g
		},
	}

	_, err := repository.Load(context.Background(), "work")
	require.NoError(t, err)
	require.Equal(t, []byte("manifest"), repository.generations["work"].Manifest)
	require.Equal(t, []byte("object"), repository.generations["work"].Objects[digest])
}

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
		appendHistoryRow(t, history, row)
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
	publication, err := acceptancePublication(snapshot)
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
	return ports.SnapshotGeneration{IncarnationID: publication.IncarnationID, Name: snapshot.Name, Generation: generation, ParentCheckpoint: publication.ParentCheckpoint, Manifest: encoded, Objects: objects}
}

func TestValidateRestoreSessionSnapshot(t *testing.T) {
	valid := restoreAcceptanceSession(t, "restored")
	for _, test := range []struct {
		name string
		snap snapcodec.Session
	}{
		{"valid", valid},
		{"empty name", func() snapcodec.Session { snap := valid; snap.Name = ""; return snap }()},
		{"active without tabs", func() snapcodec.Session { snap := valid; snap.Tabs = nil; snap.Active = 1; return snap }()},
		{"active beyond tabs", func() snapcodec.Session { snap := valid; snap.Active = 1; return snap }()},
		{"created at overflow", func() snapcodec.Session { snap := valid; snap.CreatedAt = math.MaxInt64 + 1; return snap }()},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateRestoreSessionSnapshot(test.snap)
			if test.name == "valid" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
		})
	}
}

func TestRestoreIncrementalGenerationAcceptance(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "restored")
	generation := acceptanceGeneration(t, snapshot, 9)
	repository := &snapshotAcceptanceRepository{names: []string{snapshot.Name}, generations: map[string]ports.SnapshotGeneration{snapshot.Name: generation}}
	pty, release := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	checkpoint := domain.CheckpointRef{Generation: generation.Generation, ManifestDigest: snapcodec.ManifestDigest(generation.Manifest)}
	record := domain.CatalogueRecord{Name: snapshot.Name, IncarnationID: generation.IncarnationID, Cwd: "/snapshot/cwd", CreatedAt: int64(snapshot.CreatedAt), Committed: &checkpoint}
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})
	WithCatalogue(catalogue, []domain.CatalogueRecord{record})(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)
	t.Cleanup(func() { release(); d.sessWg.Wait() })

	d.restoreIncrementalSnapshots(context.Background())

	d.mu.Lock()
	restored := d.findByNameLocked(snapshot.Name)
	d.mu.Unlock()
	require.NotNil(t, restored)
	require.False(t, restored.snapDirty.Load(), "a loaded generation must begin clean")
	require.Equal(t, uint64(9), restored.snapshotPublishedGeneration, "the loaded manifest is the repository generation head")
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

	startSnapshotEncodeWorker(t, d)
	markSnapshotDirty(restored)
	require.True(t, d.scheduleSnapshot(restored))
	awaitSnapshotClean(t, restored)
	require.Len(t, repository.publishes, 1)
	require.Equal(t, uint64(10), repository.publishes[0].Generation, "a restored session must continue the concrete repository generation stream")
}

func TestSnapshotCaptureRestorePreservesConsumedAndExpelledPaneLayout(t *testing.T) {
	initial := &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Weight: 9, Children: []*layout.Node{
		{Kind: layout.Leaf, Leaf: "pane-1", Weight: 40},
		{Kind: layout.Split, Dir: layout.Vertical, Weight: 80, Children: []*layout.Node{
			{Kind: layout.Leaf, Leaf: "pane-2", Weight: 10},
			{Kind: layout.Leaf, Leaf: "pane-3", Weight: 20},
		}},
		{Kind: layout.Stack, Weight: 40, Expanded: "pane-5", Children: []*layout.Node{
			{Kind: layout.Leaf, Leaf: "pane-4", Weight: 7},
			{Kind: layout.Leaf, Leaf: "pane-5", Weight: 3},
		}},
	}}, Focus: "pane-4"}
	h := newPaneRearrangeHarness(t, domain.Size{Cols: 200, Rows: 30}, initial)
	h.session.createdAt = 77
	h.session.incarnation = domain.IncarnationID{7}
	h.tab.mu.Lock()
	h.tab.stableID = "tab-stable-rearranged"
	h.tab.nextPaneID = 6
	paneStableIDs := map[layout.PaneID]string{
		"pane-1": "pane-stable-1",
		"pane-2": "pane-stable-2",
		"pane-3": "pane-stable-3",
		"pane-4": "pane-stable-4",
		"pane-5": "pane-stable-5",
	}
	for id, stableID := range paneStableIDs {
		h.tab.panes[id].mu.Lock()
		h.tab.panes[id].stableID = stableID
		h.tab.panes[id].mu.Unlock()
	}
	h.tab.mu.Unlock()

	operations := []struct {
		name      string
		pane      layout.PaneID
		direction layout.Direction
	}{
		{name: "consume singleton into vertical column", pane: "pane-1", direction: layout.Right},
		{name: "expel vertical member as singleton column", pane: "pane-3", direction: layout.Right},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			err := (daemonActions{d: h.daemon}).Run(daemonActionRequest{
				kind: daemonActionConsumeOrExpelPane, target: h.target(operation.pane), direction: operation.direction,
			})
			require.NoError(t, err)
		})
	}

	h.tab.mu.Lock()
	wantTree := h.tab.tree.Clone()
	h.tab.mu.Unlock()
	require.Equal(t, layout.Horizontal, wantTree.Root.Dir)
	require.Len(t, wantTree.Root.Children, 3)
	require.Equal(t, layout.Vertical, wantTree.Root.Children[0].Dir)
	require.Equal(t, layout.Leaf, wantTree.Root.Children[1].Kind)
	require.Equal(t, layout.Stack, wantTree.Root.Children[2].Kind)
	require.Equal(t, layout.PaneID("pane-5"), wantTree.Root.Children[2].Expanded)
	require.Equal(t, layout.PaneID("pane-3"), wantTree.Focus)

	capture, ok := h.daemon.captureSnapshotState(h.session, 7)
	require.True(t, ok)
	publication, err := h.daemon.incrementalPublication(capture)
	require.NoError(t, err)
	objects := make(map[ports.SnapshotDigest][]byte, len(publication.Objects))
	for _, object := range publication.Objects {
		objects[object.Digest] = append([]byte(nil), object.Data...)
	}
	generation := ports.SnapshotGeneration{
		IncarnationID: publication.IncarnationID,
		Name:          publication.Name,
		Generation:    publication.Generation,
		Manifest:      append([]byte(nil), publication.Manifest...),
		Objects:       objects,
	}
	decoded, err := sessionFromGeneration(generation)
	require.NoError(t, err)
	require.Len(t, decoded.Tabs, 1)
	decodedTab := decoded.Tabs[0]
	require.Equal(t, "tab-stable-rearranged", decodedTab.StableID)
	require.Equal(t, layout.PaneID("pane-3"), decodedTab.Focus)
	require.Equal(t, wantTree, decodedTab.Tree, "manifest codec must preserve the complete rearranged tree")
	require.Equal(t, snapshotLayoutWeights(wantTree.Root), snapshotLayoutWeights(decodedTab.Tree.Root))
	require.Equal(t, layout.PaneID("pane-5"), decodedTab.Tree.Root.Children[2].Expanded)
	decodedStableIDs := make(map[layout.PaneID]string, len(decodedTab.Panes))
	for _, restoredPane := range decodedTab.Panes {
		decodedStableIDs[restoredPane.ID] = restoredPane.StableID
	}
	require.Equal(t, paneStableIDs, decodedStableIDs)

	restorePTY := &paneRearrangePTY{}
	restoreDaemon := newTestDaemon(t, newFactory(t, restorePTY), stubClock{})
	restoredTabs, err := restoreDaemon.restoreSnapshotTabs(context.Background(), restoreDaemon.serveCtx, decoded)
	require.NoError(t, err)
	require.Len(t, restoredTabs, 1)
	defer closeRestoredTabs(restoredTabs)
	restoredTab := restoredTabs[0]
	require.Equal(t, "tab-stable-rearranged", restoredTab.stableID)
	require.Equal(t, wantTree, restoredTab.tree, "runtime restore must preserve the complete rearranged tree")
	require.Equal(t, snapshotLayoutWeights(wantTree.Root), snapshotLayoutWeights(restoredTab.tree.Root))
	require.Equal(t, layout.PaneID("pane-3"), restoredTab.tree.Focus)
	require.Equal(t, layout.PaneID("pane-5"), restoredTab.tree.Root.Children[2].Expanded)
	for id, stableID := range paneStableIDs {
		require.Equal(t, stableID, restoredTab.panes[id].stableID, "pane %s stable ID", id)
	}
}

func snapshotLayoutWeights(node *layout.Node) []float64 {
	if node == nil {
		return nil
	}
	weights := []float64{node.Weight}
	for _, child := range node.Children {
		weights = append(weights, snapshotLayoutWeights(child)...)
	}
	return weights
}

func TestRestoredSessionMetadataUpdatePreservesCheckpointLineage(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "restored")
	generation := acceptanceGeneration(t, snapshot, 9)
	repository := &snapshotAcceptanceRepository{names: []string{snapshot.Name}, generations: map[string]ports.SnapshotGeneration{snapshot.Name: generation}}
	committed := domain.CheckpointRef{Generation: generation.Generation, ManifestDigest: snapcodec.ManifestDigest(generation.Manifest)}
	record := domain.CatalogueRecord{
		Name: snapshot.Name, IncarnationID: generation.IncarnationID, Cwd: "/snapshot/cwd",
		CreatedAt: int64(snapshot.CreatedAt), UpdatedAt: 81, LastUsedSeq: 17,
		TabNames: []string{"before"}, Committed: &committed,
	}
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})

	pty, release := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	coordinator := recoveryusecase.NewCoordinator(catalogue, repository, nil)
	WithCatalogue(catalogue, []domain.CatalogueRecord{record})(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(coordinator)(d)
	t.Cleanup(func() { release(); d.sessWg.Wait() })

	d.restoreIncrementalSnapshots(context.Background())
	d.mu.Lock()
	restored := d.findByNameLocked(snapshot.Name)
	d.mu.Unlock()
	require.NotNil(t, restored)
	require.NoError(t, d.renameTab(restored, restored.tabs[0], "after"))

	updates := catalogue.MetadataUpdates()
	require.Len(t, updates, 1, "the authoritative Catalogue port must receive restored-session metadata updates")
	require.Equal(t, record.IncarnationID, updates[0].IncarnationID)
	updated, ok, err := catalogue.Record(record.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, record.IncarnationID, updated.IncarnationID)
	require.Equal(t, record.Committed, updated.Committed)
	require.Equal(t, record.DegradedReason, updated.DegradedReason)
	require.Equal(t, []string{"after"}, updated.TabNames)

	startSnapshotEncodeWorker(t, d)
	markSnapshotDirty(restored)
	require.True(t, d.scheduleSnapshot(restored))
	awaitSnapshotIdle(t, restored)
	d.snapshotNoticeMu.Lock()
	failure := d.snapshotActiveFailureSignature
	d.snapshotNoticeMu.Unlock()
	require.Empty(t, failure)
	require.Len(t, repository.publishes, 1)
	require.Equal(t, record.Committed, repository.publishes[0].ParentCheckpoint)
	published, ok, err := catalogue.Record(record.Name)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(10), published.Committed.Generation)
}

func TestRestoreIncrementalFallbackAndInvalidObjectMappings(t *testing.T) {
	snapshot := restoreAcceptanceSession(t, "fallback")
	valid := acceptanceGeneration(t, snapshot, 3)

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
			mutate.fn(&generation)
			_, err := sessionFromGeneration(generation)
			require.Error(t, err)
		})
	}
}
