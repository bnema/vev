package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

// restoreIncrementalSnapshots imports v3 only before listing repository
// sessions, then restores each independently. A corrupt session therefore
// cannot prevent an unrelated saved session from starting.
func (d *Daemon) restoreIncrementalSnapshots(ctx context.Context) {
	d.importLegacySnapshots(ctx)
	names, err := d.snapshotRepository.List(ctx)
	if err != nil {
		d.log.Warn("listing session snapshots failed", "err", err)
		d.NotifyGlobal(domain.NoticeError, domain.NoticeSnapshotRestore, "couldn't load saved sessions after restart", err)
		return
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return
		}
		generation, err := d.snapshotRepository.Load(ctx, name)
		if err != nil {
			d.snapshotRestoreFailure(name, err)
			continue
		}
		if generation.Fallback {
			d.NotifyGlobal(domain.NoticeWarn, domain.NoticeSnapshotRestore, "restored an older saved checkpoint for session "+name, nil)
		}
		snapshot, err := sessionFromGeneration(generation)
		if err != nil {
			d.snapshotRestoreFailure(name, err)
			continue
		}
		if err := d.restoreSession(ctx, snapshot); err != nil {
			if ctx.Err() == nil {
				d.snapshotRestoreFailure(name, err)
			}
		}
	}
}

func (d *Daemon) snapshotRestoreFailure(name string, err error) {
	d.log.Warn("restoring session snapshot failed", "err", err, "session", name)
	d.NotifyGlobal(domain.NoticeError, domain.NoticeSnapshotRestore, "couldn't restore session "+name+" after restart", err)
}

func sessionFromGeneration(generation ports.SnapshotGeneration) (snapcodec.Session, error) {
	manifest, err := snapcodec.UnmarshalManifest(generation.Manifest)
	if err != nil {
		return snapcodec.Session{}, err
	}
	if manifest.Name != generation.Name || manifest.Generation != generation.Generation {
		return snapcodec.Session{}, fmt.Errorf("snapshot: generation identity mismatch")
	}
	result := snapcodec.Session{Name: manifest.Name, CreatedAt: manifest.CreatedAt, Active: manifest.Active, Tabs: make([]snapcodec.Tab, 0, len(manifest.Tabs))}
	for _, tab := range manifest.Tabs {
		outTab := snapcodec.Tab{StableID: tab.StableID, Cols: tab.Cols, Rows: tab.Rows, NextPaneID: tab.NextPaneID, Focus: tab.Focus, Tree: tab.Tree, Panes: make([]snapcodec.Pane, 0, len(tab.Panes))}
		for _, pane := range tab.Panes {
			outPane := snapcodec.Pane{ID: pane.ID, StableID: pane.StableID, Cwd: pane.Cwd, Process: pane.Process}
			for _, ref := range pane.Sealed {
				data, err := generationObject(generation, ref, snapcodec.HistoryChunk)
				if err != nil {
					return snapcodec.Session{}, err
				}
				outPane.SealedChunks = append(outPane.SealedChunks, data)
			}
			if outPane.Tail, err = generationObject(generation, pane.Tail, snapcodec.HistoryTail); err != nil {
				return snapcodec.Session{}, err
			}
			if outPane.Visible, err = generationObject(generation, pane.Visible, snapcodec.Visible); err != nil {
				return snapcodec.Session{}, err
			}
			outTab.Panes = append(outTab.Panes, outPane)
		}
		result.Tabs = append(result.Tabs, outTab)
	}
	return result, nil
}

func generationObject(generation ports.SnapshotGeneration, ref snapcodec.ObjectRef, kind snapcodec.ObjectKind) ([]byte, error) {
	if ref.Kind != kind {
		return nil, fmt.Errorf("snapshot: object kind mismatch")
	}
	data, ok := generation.Objects[ref.Digest]
	if !ok || uint32(len(data)) != ref.Size || sha256.Sum256(data) != ref.Digest {
		return nil, fmt.Errorf("snapshot: missing object")
	}
	gotKind, payload, err := snapcodec.UnmarshalObject(data)
	if err != nil || gotKind != kind {
		return nil, fmt.Errorf("snapshot: invalid object")
	}
	return append([]byte(nil), payload...), nil
}

// importLegacySnapshots is intentionally best effort per session. It deletes
// a legacy blob only after publication and a full repository reload verify the
// exact converted session, leaving failures retryable on the next startup.
func (d *Daemon) importLegacySnapshots(ctx context.Context) {
	if d.legacySnapshots == nil || d.snapshotRepository == nil || ctx.Err() != nil {
		return
	}
	existing, err := d.snapshotRepository.List(ctx)
	if err != nil {
		d.log.Warn("listing incremental snapshots before import failed", "err", err)
		return
	}
	present := make(map[string]struct{}, len(existing))
	for _, name := range existing {
		present[name] = struct{}{}
	}
	legacy, err := d.legacySnapshots.LoadLegacy(ctx)
	if err != nil {
		d.log.Warn("loading legacy snapshots failed", "err", err)
		return
	}
	for _, blob := range legacy {
		if ctx.Err() != nil {
			return
		}
		if _, ok := present[blob.Name]; ok {
			d.retryLegacyDelete(ctx, blob)
			continue
		}
		snapshot, err := snapcodec.Unmarshal(blob.Data)
		if err != nil || snapshot.Name != blob.Name {
			d.log.Warn("importing legacy snapshot failed", "session", blob.Name, "err", err)
			continue
		}
		publication, err := legacyPublication(snapshot)
		if err == nil {
			err = d.snapshotRepository.Publish(ctx, publication)
		}
		if err == nil {
			var generation ports.SnapshotGeneration
			generation, err = d.snapshotRepository.Load(ctx, snapshot.Name)
			if err == nil {
				err = verifyLegacyImportGeneration(generation, publication)
			}
		}
		if err != nil {
			d.log.Warn("importing legacy snapshot failed", "session", blob.Name, "err", err)
			continue
		}
		d.rememberLegacyDelete(blob)
		if err := d.legacySnapshots.DeleteLegacy(ctx, blob.Name); err != nil {
			d.log.Warn("deleting imported legacy snapshot failed", "session", blob.Name, "err", err)
			continue
		}
		d.forgetLegacyDelete(blob.Name)
		present[blob.Name] = struct{}{}
	}
}

func verifyLegacyImportGeneration(generation ports.SnapshotGeneration, publication ports.SnapshotPublication) error {
	if generation.Name != publication.Name || generation.Generation != publication.Generation || !bytes.Equal(generation.Manifest, publication.Manifest) {
		return fmt.Errorf("snapshot: legacy import verification mismatch")
	}
	expected := make(map[ports.SnapshotDigest][]byte, len(publication.Objects))
	for _, object := range publication.Objects {
		if sha256.Sum256(object.Data) != object.Digest {
			return fmt.Errorf("snapshot: legacy import publication digest mismatch")
		}
		expected[object.Digest] = object.Data
	}
	if len(generation.Objects) != len(expected) {
		return fmt.Errorf("snapshot: legacy import verification mismatch")
	}
	for digest, expectedData := range expected {
		actual, ok := generation.Objects[digest]
		if !ok || !bytes.Equal(actual, expectedData) || sha256.Sum256(actual) != digest {
			return fmt.Errorf("snapshot: legacy import verification mismatch")
		}
	}
	return nil
}

func (d *Daemon) rememberLegacyDelete(blob ports.LegacySnapshot) {
	d.legacyImportMu.Lock()
	if d.legacyImportPending == nil {
		d.legacyImportPending = make(map[string][]byte)
	}
	d.legacyImportPending[blob.Name] = append([]byte(nil), blob.Data...)
	d.legacyImportMu.Unlock()
}

func (d *Daemon) forgetLegacyDelete(name string) {
	d.legacyImportMu.Lock()
	delete(d.legacyImportPending, name)
	d.legacyImportMu.Unlock()
}

// retryLegacyDelete only acts on a blob which this daemon has already
// published and exactly reloaded. An unrelated pre-existing generation must
// continue to leave its legacy counterpart untouched.
func (d *Daemon) retryLegacyDelete(ctx context.Context, blob ports.LegacySnapshot) {
	d.legacyImportMu.Lock()
	pending, ok := d.legacyImportPending[blob.Name]
	d.legacyImportMu.Unlock()
	if !ok || !bytes.Equal(pending, blob.Data) {
		return
	}
	if err := d.legacySnapshots.DeleteLegacy(ctx, blob.Name); err != nil {
		d.log.Warn("deleting imported legacy snapshot failed", "session", blob.Name, "err", err)
		return
	}
	d.forgetLegacyDelete(blob.Name)
}
