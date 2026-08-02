package daemon

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
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
	r.generations[p.Name] = cloneAcceptanceGeneration(snapshotGeneration(p))
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
	transcript, err := screen.RecoveryTranscriptSnapshot().Marshal()
	require.NoError(t, err)

	paneID := layout.PaneID("pane-7")
	return snapcodec.Session{
		Name: name, CreatedAt: 77, Active: 0,
		Tabs: []snapcodec.Tab{{
			StableID: "tab-stable", Cols: 293, Rows: 4, NextPaneID: 8, Focus: paneID,
			Tree: &layout.Tree{Root: layout.NewLeaf(paneID), Focus: paneID},
			Panes: []snapcodec.Pane{{
				ID: paneID, StableID: "pane-stable", Cwd: "/snapshot/cwd", SealedChunks: sealed, Tail: tail, Transcript: transcript,
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
	publication.Generation = generation
	publication.Manifest = encoded
	return cloneAcceptanceGeneration(snapshotGeneration(publication))
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
	d.baseEnv = []string{"TERM=xterm-kitty", "COLORTERM=truecolor"}
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
	require.True(t, restored.terminal.TrueColor, "restored sessions must retain startup terminal capability for future panes")
	require.Equal(t, 0, restored.active)
	require.Equal(t, "/snapshot/cwd", restored.cwd)
	require.Len(t, restored.tabs, 1)
	tab := restored.tabs[0]
	require.Equal(t, 293, tab.size.Cols)
	require.Equal(t, layout.PaneID("pane-7"), tab.tree.Focus)
	pane := tab.panes[layout.PaneID("pane-7")]
	pane.mu.Lock()
	wantHistory := []string{strings.Repeat("a", 293), strings.Repeat("b", 293), strings.Repeat("c", 293), strings.Repeat("d", 293), strings.Repeat("e", 293), "primary-visible", "alternate-active"}
	require.Equal(t, wantHistory, snapshotHistoryTexts(pane.history.View()), "bounded history must precede primary and active alternate transcript rows")
	for y := range pane.screen.Frame.Height {
		for x := range pane.screen.Frame.Width {
			require.Truef(t, pane.screen.Frame.At(x, y).Equal(renderer.BlankCell()), "restored frame cell (%d,%d) must be blank", x, y)
		}
	}
	require.Equal(t, 7, pane.history.Len())
	require.Equal(t, 7*293, pane.history.Cells())
	require.Equal(t, defaultScrollbackRows, pane.history.Cap())
	require.Equal(t, defaultScrollbackCells, pane.history.CellCap())
	pane.screen.Write([]byte("new-output"))
	pane.screen.Write([]byte("\x1b[?1049h\x1b[?1049l"))
	require.Equal(t, wantHistory, snapshotHistoryTexts(pane.history.View()), "new output and redraw must not remove recovered history")
	pane.mu.Unlock()

	startSnapshotEncodeWorker(t, d)
	markSnapshotDirty(restored)
	require.True(t, d.scheduleSnapshot(restored))
	awaitSnapshotClean(t, restored)
	require.Len(t, repository.publishes, 1)
	require.Equal(t, uint64(10), repository.publishes[0].Generation, "a restored session must continue the concrete repository generation stream")
}

type paneLayoutSnapshotFixture struct {
	snapshot      snapcodec.Session
	tree          *layout.Tree
	tabStableID   string
	paneStableIDs map[layout.PaneID]string
}

type restoredPaneLayout struct {
	tree          *layout.Tree
	tabStableID   string
	paneStableIDs map[layout.PaneID]string
}

func TestSnapshotCodecPreservesConsumedAndExpelledPaneLayout(t *testing.T) {
	fixture := captureConsumedAndExpelledPaneLayout(t)
	require.Len(t, fixture.snapshot.Tabs, 1)
	decodedTab := fixture.snapshot.Tabs[0]

	require.Equal(t, fixture.tabStableID, decodedTab.StableID)
	require.Equal(t, layout.PaneID("pane-3"), decodedTab.Focus, "snapshot tab focus must be preserved")
	assertConsumedAndExpelledColumns(t, decodedTab.Tree)
	require.Equal(t, fixture.tree, decodedTab.Tree, "manifest codec must preserve the complete rearranged tree")
	require.Equal(t, fixture.paneStableIDs, snapshotPaneStableIDs(decodedTab.Panes), "manifest codec must preserve every pane stable ID")
}

func TestSnapshotRuntimeRestorePreservesConsumedAndExpelledPaneLayout(t *testing.T) {
	fixture := captureConsumedAndExpelledPaneLayout(t)
	restoreDaemon := newTestDaemon(t, newFactory(t, &paneRearrangePTY{}), stubClock{})
	restoredTabs, err := restoreDaemon.restoreSnapshotTabs(context.Background(), restoreDaemon.serveCtx, fixture.snapshot)
	require.NoError(t, err)
	defer closeRestoredTabs(restoredTabs)
	require.Len(t, restoredTabs, 1)

	restored := captureRestoredPaneLayout(restoredTabs[0])
	require.Equal(t, fixture.tabStableID, restored.tabStableID)
	assertConsumedAndExpelledColumns(t, restored.tree)
	require.Equal(t, fixture.tree, restored.tree, "runtime restore must preserve the complete rearranged tree")
	require.Equal(t, fixture.paneStableIDs, restored.paneStableIDs, "runtime restore must preserve every pane stable ID")
}

func TestSnapshotRestoreUsesDaemonTerminalCapabilities(t *testing.T) {
	tests := []struct {
		name           string
		baseEnv        []string
		wantTerm       string
		wantColorTerm  string
		notWantEntries []string
	}{
		{
			name: "truecolor",
			baseEnv: []string{
				"TERM=xterm-kitty",
				"COLORTERM=truecolor",
				"XDG_RUNTIME_DIR=/run/user/1000",
				"WAYLAND_DISPLAY=wayland-1",
			},
			wantTerm:       "TERM=xterm-direct",
			wantColorTerm:  "COLORTERM=truecolor",
			notWantEntries: []string{"TERM=xterm-kitty"},
		},
		{
			name: "256-color fallback",
			baseEnv: []string{
				"TERM=xterm-256color",
				"COLORTERM=old",
				"XDG_RUNTIME_DIR=/run/user/1000",
				"WAYLAND_DISPLAY=wayland-1",
			},
			wantTerm:       "TERM=xterm-256color",
			notWantEntries: []string{"COLORTERM=old", "COLORTERM=truecolor"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := restoreAcceptanceSession(t, "restored")
			pty, release := newBlockingPTY(t)
			defer release()

			var gotEnv []string
			factory := portsmocks.NewMockPTYFactory(t)
			factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, _ string, _ []string, env []string, _ string, _ domain.Size) (ports.PTY, error) {
					gotEnv = append([]string(nil), env...)
					return pty, nil
				}).Once()

			d := newTestDaemon(t, factory, stubClock{})
			d.baseEnv = tt.baseEnv
			tabs, err := d.restoreSnapshotTabs(context.Background(), d.serveCtx, snapshot)
			require.NoError(t, err)
			defer closeRestoredTabs(tabs)

			require.Contains(t, gotEnv, tt.wantTerm)
			if tt.wantColorTerm != "" {
				require.Contains(t, gotEnv, tt.wantColorTerm)
			}
			require.Contains(t, gotEnv, "XDG_RUNTIME_DIR=/run/user/1000")
			require.Contains(t, gotEnv, "WAYLAND_DISPLAY=wayland-1")
			for _, notWant := range tt.notWantEntries {
				require.NotContains(t, gotEnv, notWant)
			}
		})
	}
}

func captureConsumedAndExpelledPaneLayout(t *testing.T) paneLayoutSnapshotFixture {
	t.Helper()
	h := newPaneRearrangeHarness(t, domain.Size{Cols: 200, Rows: 30}, paneLayoutSnapshotTree())
	h.session.createdAt = 77
	h.session.incarnation = domain.IncarnationID{7}
	tabStableID := "tab-stable-rearranged"
	paneStableIDs := map[layout.PaneID]string{
		"pane-1": "pane-stable-1",
		"pane-2": "pane-stable-2",
		"pane-3": "pane-stable-3",
		"pane-4": "pane-stable-4",
		"pane-5": "pane-stable-5",
	}
	setPaneLayoutSnapshotIDs(h.tab, tabStableID, paneStableIDs)
	applyPaneLayoutSnapshotOperations(t, h)

	h.tab.mu.Lock()
	wantTree := h.tab.tree.Clone()
	h.tab.mu.Unlock()
	capture, ok := h.daemon.captureSnapshotState(h.session, 7)
	require.True(t, ok)
	publication, err := h.daemon.incrementalPublication(capture)
	require.NoError(t, err)
	decoded, err := sessionFromGeneration(snapshotGeneration(publication))
	require.NoError(t, err)

	return paneLayoutSnapshotFixture{
		snapshot:      decoded,
		tree:          wantTree,
		tabStableID:   tabStableID,
		paneStableIDs: paneStableIDs,
	}
}

func paneLayoutSnapshotTree() *layout.Tree {
	return &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Weight: 9, Children: []*layout.Node{
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
}

func setPaneLayoutSnapshotIDs(tb *tab, tabStableID string, paneStableIDs map[layout.PaneID]string) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.stableID = tabStableID
	tb.nextPaneID = 6
	for id, stableID := range paneStableIDs {
		tb.panes[id].mu.Lock()
		tb.panes[id].stableID = stableID
		tb.panes[id].mu.Unlock()
	}
}

func applyPaneLayoutSnapshotOperations(t *testing.T, h *paneRearrangeHarness) {
	t.Helper()
	for _, operation := range []struct {
		name      string
		pane      layout.PaneID
		direction layout.Direction
	}{
		{name: "consume singleton into vertical column", pane: "pane-1", direction: layout.Right},
		{name: "expel vertical member as singleton column", pane: "pane-3", direction: layout.Right},
	} {
		err := (daemonActions{d: h.daemon}).Run(daemonActionRequest{
			kind: daemonActionConsumeOrExpelPane, target: h.target(operation.pane), direction: operation.direction,
		})
		require.NoError(t, err, operation.name)
	}
}

func assertConsumedAndExpelledColumns(t *testing.T, tree *layout.Tree) {
	t.Helper()
	require.NotNil(t, tree)
	require.NotNil(t, tree.Root)
	require.Equal(t, layout.Horizontal, tree.Root.Dir)
	require.Len(t, tree.Root.Children, 3)
	require.Equal(t, layout.Vertical, tree.Root.Children[0].Dir, "consumed panes must remain a vertical column")
	require.Equal(t, layout.Leaf, tree.Root.Children[1].Kind, "expelled pane must remain a singleton column")
	require.Equal(t, layout.Stack, tree.Root.Children[2].Kind, "stack column must remain present")
	require.Equal(t, layout.PaneID("pane-5"), tree.Root.Children[2].Expanded, "stack expansion must be preserved")
	require.Equal(t, layout.PaneID("pane-3"), tree.Focus, "expelled pane focus must be preserved")
}

func snapshotPaneStableIDs(panes []snapcodec.Pane) map[layout.PaneID]string {
	stableIDs := make(map[layout.PaneID]string, len(panes))
	for _, p := range panes {
		stableIDs[p.ID] = p.StableID
	}
	return stableIDs
}

func captureRestoredPaneLayout(tb *tab) restoredPaneLayout {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	stableIDs := make(map[layout.PaneID]string, len(tb.panes))
	for id, p := range tb.panes {
		p.mu.Lock()
		stableIDs[id] = p.stableID
		p.mu.Unlock()
	}
	return restoredPaneLayout{tree: tb.tree.Clone(), tabStableID: tb.stableID, paneStableIDs: stableIDs}
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

func TestRestorePaneTerminalRequiresCompleteRecoveryAndInstallsAtomically(t *testing.T) {
	base := restoreAcceptanceSession(t, "restored").Tabs[0].Panes[0]
	for _, tt := range []struct {
		name   string
		mutate func(*snapcodec.Pane)
	}{
		{name: "missing tail", mutate: func(snap *snapcodec.Pane) { snap.Tail = nil }},
		{name: "missing transcript", mutate: func(snap *snapcodec.Pane) { snap.Transcript = nil }},
		{name: "invalid transcript", mutate: func(snap *snapcodec.Pane) { snap.Transcript = []byte("invalid") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			snap := base
			tt.mutate(&snap)
			p := newPane("pane", nil, domain.Size{Cols: 293, Rows: 4})
			p.screen.Write([]byte("old-frame"))
			require.NoError(t, p.history.Append(testRow("old-history"), vt.LineBound{End: len("old-history")}))
			oldScreen, oldHistory := p.screen, p.history

			err := restorePaneTerminal(p, snap)

			require.Error(t, err)
			require.Same(t, oldScreen, p.screen)
			require.Same(t, oldHistory, p.history)
			require.Equal(t, 'o', p.screen.Frame.At(0, 0).Rune, "a failed restore must never install a partial or old persisted frame")
			require.Equal(t, "old-history", cellsString(p.history.View().Row(0)))
		})
	}
}

func snapshotHistoryTexts(view vt.HistoryView) []string {
	rows := make([]string, view.Len())
	for i := range rows {
		rows[i] = strings.TrimRight(cellsString(view.Row(i)), " ")
	}
	return rows
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
		{"wrong role after digest verification", func(g *ports.SnapshotGeneration) {
			manifest, err := snapcodec.UnmarshalManifest(g.Manifest)
			require.NoError(t, err)
			tailRef := manifest.Tabs[0].Panes[0].Tail
			_, payload, err := snapcodec.UnmarshalObject(g.Objects[tailRef.Digest])
			require.NoError(t, err)
			replacement, err := snapcodec.MarshalObject(snapcodec.RecoveryTranscript, payload)
			require.NoError(t, err)
			manifest.Tabs[0].Panes[0].Tail.Digest = replacement.Digest
			manifest.Tabs[0].Panes[0].Tail.Size = uint32(len(replacement.Data))
			g.Manifest, err = snapcodec.MarshalManifest(manifest)
			require.NoError(t, err)
			delete(g.Objects, tailRef.Digest)
			g.Objects[replacement.Digest] = replacement.Data
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
