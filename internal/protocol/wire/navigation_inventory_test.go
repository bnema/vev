package wire

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func inventoryRegistrationForTest() domain.RemoteRegistration {
	registration, err := domain.NewRemoteRegistration("user@arch", [16]byte{1, 2, 3})
	if err != nil {
		panic(err)
	}
	return registration
}

func TestNavigationInventoryRequestRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		value protocol.NavigationInventoryRequest
	}{
		{name: "snapshot", value: protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 1, Operation: protocol.NavigationInventorySnapshot}},
		{name: "resolve", value: protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 2, Operation: protocol.NavigationInventoryResolve, SourceKey: "remote-a", EntryKey: "abc", Registration: inventoryRegistrationForTest()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := MarshalNavigationInventoryRequest(tt.value)
			if payload == nil {
				t.Fatal("MarshalNavigationInventoryRequest returned nil")
			}
			if version, ok := PeekNavigationInventoryVersion(payload); !ok || version != tt.value.Version {
				t.Fatalf("PeekNavigationInventoryVersion() = %d, %v; want %d, true", version, ok, tt.value.Version)
			}
			if id, ok := PeekNavigationInventoryRequestID(payload); !ok || id != tt.value.RequestID {
				t.Fatalf("PeekNavigationInventoryRequestID() = %d, %v; want %d, true", id, ok, tt.value.RequestID)
			}
			decoded, err := UnmarshalNavigationInventoryRequest(payload)
			if err != nil {
				t.Fatalf("UnmarshalNavigationInventoryRequest() error = %v", err)
			}
			if decoded != tt.value {
				t.Fatalf("UnmarshalNavigationInventoryRequest() = %#v, want %#v", decoded, tt.value)
			}
			assertAllPrefixesFail(t, payload, UnmarshalNavigationInventoryRequest)
			assertTrailingGarbageFails(t, payload, UnmarshalNavigationInventoryRequest)
		})
	}
	if MarshalNavigationInventoryRequest(protocol.NavigationInventoryRequest{}) != nil {
		t.Fatal("MarshalNavigationInventoryRequest accepted zero request")
	}
	if MarshalNavigationInventoryRequest(protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: 1, Operation: protocol.NavigationInventorySnapshot, SourceKey: "local"}) != nil {
		t.Fatal("MarshalNavigationInventoryRequest accepted snapshot with keys")
	}
}

func TestNavigationInventoryResponseRoundTrip(t *testing.T) {
	group := protocol.NavigationInventorySourceGroup{SourceKey: "local", Status: protocol.NavigationInventorySourceOK, Entries: []protocol.NavigationInventoryEntry{{SourceKey: "local", EntryKey: "a", Name: "one", DisplayOrigin: "local", State: "up"}}}
	target := protocol.AttachTarget{Session: "work", Intent: protocol.IntentAttach}
	tests := []struct {
		name  string
		value protocol.NavigationInventoryResponse
	}{
		{name: "snapshot", value: protocol.NavigationInventoryResponse{RequestID: 1, Operation: protocol.NavigationInventorySnapshot, Status: protocol.NavigationInventoryOK, Groups: []protocol.NavigationInventorySourceGroup{group}}},
		{name: "unavailable", value: protocol.NavigationInventoryResponse{RequestID: 2, Operation: protocol.NavigationInventorySnapshot, Status: protocol.NavigationInventoryUnavailable, Groups: []protocol.NavigationInventorySourceGroup{{SourceKey: "remote", Status: protocol.NavigationInventorySourceUnavailable}}}},
		{name: "resolve", value: protocol.NavigationInventoryResponse{RequestID: 3, Operation: protocol.NavigationInventoryResolve, Status: protocol.NavigationInventoryOK, Resolved: &target}},
		{name: "version mismatch", value: protocol.NavigationInventoryResponse{RequestID: 4, Operation: protocol.NavigationInventorySnapshot, Status: protocol.NavigationInventoryVersionMismatch}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := MarshalNavigationInventoryResponse(tt.value)
			if payload == nil {
				t.Fatal("MarshalNavigationInventoryResponse returned nil")
			}
			decoded, err := UnmarshalNavigationInventoryResponse(payload)
			if err != nil {
				t.Fatalf("UnmarshalNavigationInventoryResponse() error = %v", err)
			}
			if decoded.RequestID != tt.value.RequestID || decoded.Operation != tt.value.Operation || decoded.Status != tt.value.Status {
				t.Fatalf("UnmarshalNavigationInventoryResponse() = %#v, want %#v", decoded, tt.value)
			}
			assertAllPrefixesFail(t, payload, UnmarshalNavigationInventoryResponse)
			assertTrailingGarbageFails(t, payload, UnmarshalNavigationInventoryResponse)
		})
	}
	bad := protocol.NavigationInventoryResponse{RequestID: 1, Operation: protocol.NavigationInventorySnapshot, Status: protocol.NavigationInventoryOK, Groups: []protocol.NavigationInventorySourceGroup{{SourceKey: "local", Status: protocol.NavigationInventorySourceOK, Entries: []protocol.NavigationInventoryEntry{{SourceKey: "local", EntryKey: "a", Name: "bad\x1b"}}}}}
	if MarshalNavigationInventoryResponse(bad) != nil {
		t.Fatal("MarshalNavigationInventoryResponse accepted control character")
	}
	oversize := protocol.NavigationInventoryResponse{RequestID: 1, Operation: protocol.NavigationInventorySnapshot, Status: protocol.NavigationInventoryOK, Groups: []protocol.NavigationInventorySourceGroup{{SourceKey: "local", Status: protocol.NavigationInventorySourceOK, Entries: []protocol.NavigationInventoryEntry{{SourceKey: "local", EntryKey: "a", Name: strings.Repeat("x", protocol.NavigationInventoryMaxDisplayBytes+1)}}}}}
	if MarshalNavigationInventoryResponse(oversize) != nil {
		t.Fatal("MarshalNavigationInventoryResponse accepted oversize label")
	}
}

func TestNavigationInventoryDemandPublicationSelectionFailureRoundTrip(t *testing.T) {
	group := protocol.NavigationInventorySourceGroup{SourceKey: "local", Status: protocol.NavigationInventorySourceOK, Entries: []protocol.NavigationInventoryEntry{{SourceKey: "local", EntryKey: "a", Name: "one"}}}
	demand := protocol.NavigationInventoryDemand{InteractionGeneration: 3, Open: true}
	if payload := MarshalNavigationInventoryDemand(demand); payload == nil {
		t.Fatal("MarshalNavigationInventoryDemand returned nil")
	} else {
		decoded, err := UnmarshalNavigationInventoryDemand(payload)
		if err != nil || decoded != demand {
			t.Fatalf("UnmarshalNavigationInventoryDemand() = %#v, %v; want %#v", decoded, err, demand)
		}
		assertAllPrefixesFail(t, payload, UnmarshalNavigationInventoryDemand)
		assertTrailingGarbageFails(t, payload, UnmarshalNavigationInventoryDemand)
	}
	publication := protocol.NavigationInventoryPublication{InteractionGeneration: 3, PublicationGeneration: 1, Groups: []protocol.NavigationInventorySourceGroup{group}}
	if payload := MarshalNavigationInventoryPublication(publication); payload == nil {
		t.Fatal("MarshalNavigationInventoryPublication returned nil")
	} else {
		decoded, err := UnmarshalNavigationInventoryPublication(payload)
		if err != nil {
			t.Fatalf("UnmarshalNavigationInventoryPublication() error = %v", err)
		}
		if decoded.InteractionGeneration != publication.InteractionGeneration || decoded.PublicationGeneration != publication.PublicationGeneration {
			t.Fatalf("UnmarshalNavigationInventoryPublication() = %#v, want %#v", decoded, publication)
		}
		assertAllPrefixesFail(t, payload, UnmarshalNavigationInventoryPublication)
		assertTrailingGarbageFails(t, payload, UnmarshalNavigationInventoryPublication)
	}
	selection := protocol.NavigationInventorySelection{CauseActionID: 9, InteractionGeneration: 3, PublicationGeneration: 1, SourceKey: "local", EntryKey: "a"}
	if payload := MarshalNavigationInventorySelection(selection); payload == nil {
		t.Fatal("MarshalNavigationInventorySelection returned nil")
	} else {
		decoded, err := UnmarshalNavigationInventorySelection(payload)
		if err != nil || decoded != selection {
			t.Fatalf("UnmarshalNavigationInventorySelection() = %#v, %v; want %#v", decoded, err, selection)
		}
		assertAllPrefixesFail(t, payload, UnmarshalNavigationInventorySelection)
		assertTrailingGarbageFails(t, payload, UnmarshalNavigationInventorySelection)
	}
	failure := protocol.NavigationInventoryFailure{CauseActionID: 9, InteractionGeneration: 3, SourceKey: "local", EntryKey: "a", Code: protocol.NavigationInventoryStaleIdentity}
	if payload := MarshalNavigationInventoryFailure(failure); payload == nil {
		t.Fatal("MarshalNavigationInventoryFailure returned nil")
	} else {
		decoded, err := UnmarshalNavigationInventoryFailure(payload)
		if err != nil || decoded != failure {
			t.Fatalf("UnmarshalNavigationInventoryFailure() = %#v, %v; want %#v", decoded, err, failure)
		}
		assertAllPrefixesFail(t, payload, UnmarshalNavigationInventoryFailure)
		assertTrailingGarbageFails(t, payload, UnmarshalNavigationInventoryFailure)
	}
}
