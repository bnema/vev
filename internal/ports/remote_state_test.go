package ports

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

func TestRemoteJobKindString(t *testing.T) {
	tests := []struct {
		name  string
		value RemoteJobKind
		want  string
	}{
		{name: "registry read", value: RemoteJobRegistryRead, want: "registry_read"},
		{name: "cache load", value: RemoteJobCacheLoad, want: "cache_load"},
		{name: "catalog observe", value: RemoteJobCatalogObserve, want: "catalog_observe"},
		{name: "cache store", value: RemoteJobCacheStore, want: "cache_store"},
		{name: "zero is unknown", value: RemoteJobKind(0), want: "unknown"},
		{name: "out of range is unknown", value: RemoteJobKind(99), want: "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.value.String(); got != test.want {
				t.Fatalf("String() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRemoteDirectorySnapshotCloneIsDefensive(t *testing.T) {
	original := RemoteDirectorySnapshot{
		Revision: 7, Initialized: true,
		Hosts: []RemoteHostSnapshot{{
			Endpoint: "user@arch", DisplayOrigin: "arch", Rank: 0,
			Registration:   domain.RemoteRegistration{Endpoint: "user@arch", Incarnation: [16]byte{3}, Generation: 1},
			Availability:   domain.RemoteAvailabilityReachable,
			Checking:       true,
			InventoryKnown: true,
			Sessions: []catalogue.RemoteCatalogSession{{
				Name: "work", State: catalogue.RemoteCatalogSessionUp,
				Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "shell"}},
			}},
		}},
	}
	cloned := original.Clone()
	if found, ok := cloned.Find("user@arch"); !ok || found.Availability != domain.RemoteAvailabilityReachable {
		t.Fatalf("Find() = %+v, %t, want reachable host", found, ok)
	}
	if _, ok := cloned.Find("user@mule"); ok {
		t.Fatal("Find() must miss unknown endpoints")
	}

	cloned.Hosts[0].Endpoint = "mutated"
	cloned.Hosts[0].Sessions[0].Name = "mutated"
	cloned.Hosts[0].Sessions[0].Tabs[0].ID = "mutated"
	if original.Hosts[0].Endpoint != "user@arch" || original.Hosts[0].Sessions[0].Name != "work" ||
		original.Hosts[0].Sessions[0].Tabs[0].ID != "tab-1" {
		t.Fatalf("Clone() shares memory with the original: %+v", original.Hosts[0])
	}

	empty := RemoteDirectorySnapshot{}.Clone()
	if empty.Hosts == nil {
		t.Fatal("Clone() of an empty snapshot must keep a non-nil host slice")
	}
	if _, ok := empty.Find("user@arch"); ok {
		t.Fatal("Find() on an empty snapshot must miss")
	}
}

func TestRemoteHostSnapshotCloneIsDefensive(t *testing.T) {
	original := RemoteHostSnapshot{
		Endpoint: "user@arch",
		Sessions: []catalogue.RemoteCatalogSession{{Name: "work"}},
	}
	cloned := original.Clone()
	cloned.Sessions[0].Name = "mutated"
	if original.Sessions[0].Name != "work" {
		t.Fatal("Clone() shares session memory with the original")
	}
	if (RemoteHostSnapshot{}).Clone().Sessions != nil {
		t.Fatal("Clone() of a session-less host must stay nil")
	}
}
