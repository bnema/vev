package snapshot

import (
	"context"
	"fmt"
	"os"
	"testing"
)

func TestRepositoryMaintainDoesNotCollectBeyondRetainedMetadataBudget(t *testing.T) {
	repo := NewRepository(privateDir(t))
	for i := range maxMaintenanceMarkedGenerations + 1 {
		generation := uint64(i + 1)
		if err := repo.Publish(context.Background(), repositoryPublicationAfter(t, repo, "named", generation, fmt.Appendf(nil, "state-%d", generation))); err != nil {
			t.Fatal(err)
		}
	}
	for pass := range maxMaintenanceMarkedGenerations * 3 {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if repo.maintenanceSessions[legacyIncarnationID("named").String()] == nil {
			break
		}
		if pass == maxMaintenanceMarkedGenerations*3-1 {
			t.Fatal("maintenance did not complete after bounded object-shard traversal")
		}
	}
	if _, err := os.Lstat(repo.legacyManifestPath(legacyIncarnationID("named").String(), 1)); err != nil {
		t.Fatalf("old manifest collected after mark budget overflow: %v", err)
	}
	if state := repo.maintenanceSessions[legacyIncarnationID("named").String()]; state != nil {
		t.Fatalf("overflow maintenance state retained = %#v, want reset", state)
	}
}

func TestMaintenanceMarkStateIsCapped(t *testing.T) {
	references := &sessionMaintenance{marked: make(map[uint64]manifestMaintenance)}
	if !references.canRetainManifest(maxMaintenanceReferences) {
		t.Fatal("state rejected references at its documented ceiling")
	}
	if references.canRetainManifest(1) {
		t.Fatal("state accepted references beyond its documented ceiling")
	}
	generations := &sessionMaintenance{marked: make(map[uint64]manifestMaintenance)}
	for generation := uint64(1); generation <= maxMaintenanceMarkedGenerations; generation++ {
		generations.marked[generation] = manifestMaintenance{}
	}
	if generations.canRetainManifest(0) {
		t.Fatal("state accepted generations beyond its documented ceiling")
	}
}
