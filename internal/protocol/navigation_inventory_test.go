package protocol

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
)

func TestValidateNavigationInventoryRequest(t *testing.T) {
	registration, err := domain.NewRemoteRegistration("user@arch", [16]byte{1})
	if err != nil {
		t.Fatalf("NewRemoteRegistration: %v", err)
	}
	tests := []struct {
		name    string
		request NavigationInventoryRequest
		wantErr bool
	}{
		{name: "snapshot ok", request: NavigationInventoryRequest{Version: Version, RequestID: 1, Operation: NavigationInventorySnapshot}},
		{name: "zero request", request: NavigationInventoryRequest{Operation: NavigationInventorySnapshot}, wantErr: true},
		{name: "bad operation", request: NavigationInventoryRequest{Version: Version, RequestID: 1, Operation: 9}, wantErr: true},
		{name: "snapshot with keys", request: NavigationInventoryRequest{Version: Version, RequestID: 1, Operation: NavigationInventorySnapshot, SourceKey: "local"}, wantErr: true},
		{name: "resolve ok", request: NavigationInventoryRequest{Version: Version, RequestID: 2, Operation: NavigationInventoryResolve, SourceKey: "remote-a", EntryKey: "abc", Registration: registration}},
		{name: "resolve local ok", request: NavigationInventoryRequest{Version: Version, RequestID: 3, Operation: NavigationInventoryResolve, SourceKey: NavigationInventoryLocalSourceKey, EntryKey: "abc"}},
		{name: "resolve local with registration", request: NavigationInventoryRequest{Version: Version, RequestID: 3, Operation: NavigationInventoryResolve, SourceKey: NavigationInventoryLocalSourceKey, EntryKey: "abc", Registration: registration}, wantErr: true},
		{name: "resolve missing registration", request: NavigationInventoryRequest{Version: Version, RequestID: 2, Operation: NavigationInventoryResolve, SourceKey: "remote-a", EntryKey: "abc"}, wantErr: true},
		{name: "resolve bad key", request: NavigationInventoryRequest{Version: Version, RequestID: 2, Operation: NavigationInventoryResolve, SourceKey: "a\x1b", EntryKey: "abc", Registration: registration}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNavigationInventoryRequest(tt.request)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateNavigationInventoryRequest() = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func TestValidateNavigationInventoryResponseUnions(t *testing.T) {
	group := NavigationInventorySourceGroup{SourceKey: "local", Status: NavigationInventorySourceOK, Entries: []NavigationInventoryEntry{{SourceKey: "local", EntryKey: "a", Name: "one"}}}
	tests := []struct {
		name     string
		response NavigationInventoryResponse
		wantErr  bool
	}{
		{name: "snapshot ok", response: NavigationInventoryResponse{RequestID: 1, Operation: NavigationInventorySnapshot, Status: NavigationInventoryOK, Groups: []NavigationInventorySourceGroup{group}}},
		{name: "snapshot with target", response: NavigationInventoryResponse{RequestID: 1, Operation: NavigationInventorySnapshot, Status: NavigationInventoryOK, Groups: []NavigationInventorySourceGroup{group}, Resolved: &AttachTarget{}}, wantErr: true},
		{name: "duplicate source", response: NavigationInventoryResponse{RequestID: 1, Operation: NavigationInventorySnapshot, Status: NavigationInventoryOK, Groups: []NavigationInventorySourceGroup{group, group}}, wantErr: true},
		{name: "too many groups", response: NavigationInventoryResponse{RequestID: 1, Operation: NavigationInventorySnapshot, Status: NavigationInventoryOK, Groups: make([]NavigationInventorySourceGroup, NavigationInventoryMaxSourceGroups+1)}, wantErr: true},
		{name: "control char label", response: NavigationInventoryResponse{RequestID: 1, Operation: NavigationInventorySnapshot, Status: NavigationInventoryOK, Groups: []NavigationInventorySourceGroup{{SourceKey: "local", Status: NavigationInventorySourceOK, Entries: []NavigationInventoryEntry{{SourceKey: "local", EntryKey: "a", Name: "bad\x1b"}}}}}, wantErr: true},
		{name: "oversize label", response: NavigationInventoryResponse{RequestID: 1, Operation: NavigationInventorySnapshot, Status: NavigationInventoryOK, Groups: []NavigationInventorySourceGroup{{SourceKey: "local", Status: NavigationInventorySourceOK, Entries: []NavigationInventoryEntry{{SourceKey: "local", EntryKey: "a", Name: strings.Repeat("x", NavigationInventoryMaxDisplayBytes+1)}}}}}, wantErr: true},
		{name: "version mismatch clean", response: NavigationInventoryResponse{RequestID: 1, Operation: NavigationInventorySnapshot, Status: NavigationInventoryVersionMismatch}},
		{name: "version mismatch with target", response: NavigationInventoryResponse{RequestID: 1, Operation: NavigationInventorySnapshot, Status: NavigationInventoryVersionMismatch, Groups: []NavigationInventorySourceGroup{group}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNavigationInventoryResponse(tt.response)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateNavigationInventoryResponse() = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func TestNavigationCapabilityInventoryValid(t *testing.T) {
	if err := ValidateNavigation(NavigationCapabilityInventory); err != nil {
		t.Fatalf("ValidateNavigation(inventory) = %v", err)
	}
}
