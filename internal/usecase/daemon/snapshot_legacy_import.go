package daemon

import (
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

func legacyPublication(snapshot snapcodec.Session) (ports.SnapshotPublication, error) {
	// Legacy payloads have no durable identity; this test-only conversion uses
	// the fixture's stable synthetic identity.
	incarnation := domain.IncarnationID{1}
	manifest := snapcodec.Manifest{Generation: 1, IncarnationID: incarnation, Name: snapshot.Name, CreatedAt: snapshot.CreatedAt, Active: snapshot.Active, Tabs: make([]snapcodec.ManifestTab, 0, len(snapshot.Tabs))}
	objects := make([]ports.SnapshotObject, 0)
	for _, tab := range snapshot.Tabs {
		outTab := snapcodec.ManifestTab{StableID: tab.StableID, Cols: tab.Cols, Rows: tab.Rows, NextPaneID: tab.NextPaneID, Focus: tab.Focus, Tree: tab.Tree, Panes: make([]snapcodec.ManifestPane, 0, len(tab.Panes))}
		for _, pane := range tab.Panes {
			outPane := snapcodec.ManifestPane{ID: pane.ID, StableID: pane.StableID, Cwd: pane.Cwd, Process: pane.Process}
			for i, payload := range pane.SealedChunks {
				object, err := snapcodec.MarshalObject(snapcodec.HistoryChunk, payload)
				if err != nil {
					return ports.SnapshotPublication{}, fmt.Errorf("snapshot legacy import: tab %q pane %q sealed chunk %d: marshal object: %w", tab.StableID, pane.ID, i, err)
				}
				outPane.Sealed = append(outPane.Sealed, objectRef(snapcodec.HistoryChunk, object))
				objects = append(objects, object)
			}
			tail, err := snapcodec.MarshalObject(snapcodec.HistoryTail, pane.Tail)
			if err != nil {
				return ports.SnapshotPublication{}, fmt.Errorf("snapshot legacy import: tab %q pane %q tail: marshal object: %w", tab.StableID, pane.ID, err)
			}
			visible, err := snapcodec.MarshalObject(snapcodec.Visible, pane.Visible)
			if err != nil {
				return ports.SnapshotPublication{}, fmt.Errorf("snapshot legacy import: tab %q pane %q visible: marshal object: %w", tab.StableID, pane.ID, err)
			}
			outPane.Tail, outPane.Visible = objectRef(snapcodec.HistoryTail, tail), objectRef(snapcodec.Visible, visible)
			objects = append(objects, tail, visible)
			outTab.Panes = append(outTab.Panes, outPane)
		}
		manifest.Tabs = append(manifest.Tabs, outTab)
	}
	encoded, err := snapcodec.MarshalManifest(manifest)
	if err != nil {
		return ports.SnapshotPublication{}, fmt.Errorf("snapshot: marshal legacy import manifest: %w", err)
	}
	return ports.SnapshotPublication{IncarnationID: incarnation, Name: snapshot.Name, Generation: 1, Manifest: encoded, Objects: objects}, nil
}
