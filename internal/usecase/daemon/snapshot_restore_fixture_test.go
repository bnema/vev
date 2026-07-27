package daemon

import (
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

func acceptancePublication(snapshot snapcodec.Session) (ports.SnapshotPublication, error) {
	// acceptancePublication converts a restore fixture to repository form.
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
					return ports.SnapshotPublication{}, fmt.Errorf("snapshot fixture conversion: tab %q pane %q sealed chunk %d: marshal object: %w", tab.StableID, pane.ID, i, err)
				}
				outPane.Sealed = append(outPane.Sealed, objectRef(snapcodec.HistoryChunk, object))
				objects = append(objects, object)
			}
			tail, err := snapcodec.MarshalObject(snapcodec.HistoryTail, pane.Tail)
			if err != nil {
				return ports.SnapshotPublication{}, fmt.Errorf("snapshot fixture conversion: tab %q pane %q tail: marshal object: %w", tab.StableID, pane.ID, err)
			}
			transcript, err := snapcodec.MarshalObject(snapcodec.RecoveryTranscript, pane.Transcript)
			if err != nil {
				return ports.SnapshotPublication{}, fmt.Errorf("snapshot fixture conversion: tab %q pane %q recovery transcript: marshal object: %w", tab.StableID, pane.ID, err)
			}
			outPane.Tail, outPane.Transcript = objectRef(snapcodec.HistoryTail, tail), objectRef(snapcodec.RecoveryTranscript, transcript)
			objects = append(objects, tail, transcript)
			outTab.Panes = append(outTab.Panes, outPane)
		}
		manifest.Tabs = append(manifest.Tabs, outTab)
	}
	encoded, err := snapcodec.MarshalManifest(manifest)
	if err != nil {
		return ports.SnapshotPublication{}, fmt.Errorf("snapshot fixture conversion: marshal manifest: %w", err)
	}
	return ports.SnapshotPublication{IncarnationID: incarnation, Name: snapshot.Name, Generation: 1, Manifest: encoded, Objects: objects}, nil
}
