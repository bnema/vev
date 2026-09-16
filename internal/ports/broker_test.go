package ports

import (
	"reflect"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

func testBrokerRegistration(endpoint string, incarnation byte, generation domain.RemoteGeneration) domain.RemoteRegistration {
	return domain.RemoteRegistration{
		Endpoint:    endpoint,
		Incarnation: [16]byte{incarnation, 7, 9},
		Generation:  generation,
	}
}

func testBrokerLifecycle() domain.SessionLifecycleID {
	return domain.SessionLifecycleID{1, 2, 3}
}

func testBrokerTarget() protocol.ExactSessionTarget {
	return protocol.ExactSessionTarget{LifecycleID: testBrokerLifecycle(), SessionName: "work"}
}

func testBrokerPolicy() BrokerPolicy {
	return BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion,
		EnvironmentPolicy:    protocol.EnvironmentPolicyDaemonOwned,
		Transport:            "quic",
	}
}

func testBrokerHost(endpoint string) RemoteHostSnapshot {
	return RemoteHostSnapshot{
		Endpoint:      endpoint,
		DisplayOrigin: endpoint,
		Registration:  testBrokerRegistration(endpoint, 1, 1),
		Availability:  domain.RemoteAvailabilityReachable,
	}
}

func validBrokerSnapshot() BrokerSnapshot {
	return BrokerSnapshot{
		Epoch:    3,
		Revision: 9,
		Hosts:    []RemoteHostSnapshot{testBrokerHost("user@arch")},
	}
}

func TestBrokerSnapshotValidateBounds(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*BrokerSnapshot)
		wantErr bool
	}{
		{name: "valid", mutate: func(*BrokerSnapshot) {}, wantErr: false},
		{name: "zero epoch", mutate: func(s *BrokerSnapshot) { s.Epoch = 0 }, wantErr: true},
		{name: "zero revision", mutate: func(s *BrokerSnapshot) { s.Revision = 0 }, wantErr: true},
		{name: "too many hosts", mutate: func(s *BrokerSnapshot) {
			s.Hosts = make([]RemoteHostSnapshot, BrokerMaxHosts+1)
			for i := range s.Hosts {
				s.Hosts[i] = testBrokerHost("user@arch")
				s.Hosts[i].Registration = testBrokerRegistration("user@arch", 1, 1)
			}
		}, wantErr: true},
		{name: "duplicate host", mutate: func(s *BrokerSnapshot) {
			s.Hosts = append(s.Hosts, testBrokerHost("user@arch"))
		}, wantErr: true},
		{name: "endpoint registration mismatch", mutate: func(s *BrokerSnapshot) {
			s.Hosts[0].Registration = testBrokerRegistration("user@other", 1, 1)
		}, wantErr: true},
		{name: "zero registration incarnation", mutate: func(s *BrokerSnapshot) {
			s.Hosts[0].Registration = domain.RemoteRegistration{Endpoint: "user@arch", Generation: 1}
		}, wantErr: true},
		{name: "live host as tombstone", mutate: func(s *BrokerSnapshot) {
			s.Removed = []BrokerHostTombstone{{
				Endpoint:        "user@arch",
				Registration:    testBrokerRegistration("user@arch", 1, 1),
				RetiredRevision: 9,
			}}
		}, wantErr: true},
		{name: "too many tombstones", mutate: func(s *BrokerSnapshot) {
			s.Removed = make([]BrokerHostTombstone, BrokerMaxTombstones+1)
			for i := range s.Removed {
				s.Removed[i] = BrokerHostTombstone{
					Endpoint:        "user@gone",
					Registration:    testBrokerRegistration("user@gone", 1, 1),
					RetiredRevision: 9,
				}
			}
		}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := validBrokerSnapshot()
			tt.mutate(&snapshot)
			if err := snapshot.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate() = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

func TestBrokerSnapshotEpochRevisionOrdering(t *testing.T) {
	tests := []struct {
		name        string
		current     BrokerSnapshot
		candidate   BrokerSnapshot
		wantNewer   bool
		description string
	}{
		{
			name:        "higher revision wins in epoch",
			current:     BrokerSnapshot{Epoch: 3, Revision: 9},
			candidate:   BrokerSnapshot{Epoch: 3, Revision: 10},
			wantNewer:   true,
			description: "monotonic revision within an epoch",
		},
		{
			name:        "lower revision loses in epoch",
			current:     BrokerSnapshot{Epoch: 3, Revision: 10},
			candidate:   BrokerSnapshot{Epoch: 3, Revision: 9},
			wantNewer:   false,
			description: "stale revision never applies",
		},
		{
			name:        "equal revision loses",
			current:     BrokerSnapshot{Epoch: 3, Revision: 9},
			candidate:   BrokerSnapshot{Epoch: 3, Revision: 9},
			wantNewer:   false,
			description: "replay of the same revision is not newer",
		},
		{
			name:        "epoch change forces resync",
			current:     BrokerSnapshot{Epoch: 3, Revision: 99},
			candidate:   BrokerSnapshot{Epoch: 4, Revision: 1},
			wantNewer:   true,
			description: "any valid epoch change supersedes regardless of revision",
		},
		{
			name:        "lower epoch still forces resync",
			current:     BrokerSnapshot{Epoch: 9, Revision: 1},
			candidate:   BrokerSnapshot{Epoch: 4, Revision: 50},
			wantNewer:   true,
			description: "epochs are fresh per process, never numerically ordered",
		},
		{
			name:        "zero epoch never supersedes",
			current:     BrokerSnapshot{Epoch: 3, Revision: 9},
			candidate:   BrokerSnapshot{},
			wantNewer:   false,
			description: "invalid snapshots never apply",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.candidate.Supersedes(tt.current); got != tt.wantNewer {
				t.Fatalf("Supersedes() = %t, want %t (%s)", got, tt.wantNewer, tt.description)
			}
		})
	}
}

func TestBrokerSnapshotCloneAndFind(t *testing.T) {
	original := validBrokerSnapshot()
	original.Hosts[0].Sessions = []catalogue.RemoteCatalogSession{{
		LifecycleID: testBrokerLifecycle(),
		Name:        "work",
		State:       catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "shell"}},
	}}
	cloned := original.Clone()
	if found, ok := cloned.Find("user@arch"); !ok || found.Endpoint != "user@arch" {
		t.Fatalf("Find() = %+v, %t, want the host", found, ok)
	}
	if _, ok := cloned.Find("user@mule"); ok {
		t.Fatal("Find() must miss unknown endpoints")
	}
	cloned.Hosts[0].Sessions[0].Name = "mutated"
	cloned.Hosts[0].Sessions[0].Tabs[0].ID = "mutated"
	if original.Hosts[0].Sessions[0].Name != "work" || original.Hosts[0].Sessions[0].Tabs[0].ID != "tab-1" {
		t.Fatal("Clone() shares memory with the original")
	}
}

func TestBrokerTombstoneFencing(t *testing.T) {
	registration := testBrokerRegistration("user@arch", 4, 2)
	tombstone := BrokerHostTombstone{
		Endpoint:        "user@arch",
		Registration:    registration,
		RetiredRevision: 12,
	}
	if err := tombstone.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	tests := []struct {
		name      string
		candidate domain.RemoteRegistration
		wantFence bool
	}{
		{name: "same registration fenced", candidate: registration, wantFence: true},
		{name: "older generation fenced", candidate: testBrokerRegistration("user@arch", 4, 1), wantFence: true},
		{name: "fresh incarnation not fenced", candidate: testBrokerRegistration("user@arch", 5, 1), wantFence: false},
		{name: "other endpoint not fenced", candidate: testBrokerRegistration("user@mule", 4, 2), wantFence: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tombstone.Fences(tt.candidate); got != tt.wantFence {
				t.Fatalf("Fences() = %t, want %t", got, tt.wantFence)
			}
		})
	}

	invalid := []BrokerHostTombstone{
		{Registration: registration, RetiredRevision: 1},
		{Endpoint: "user@arch", Registration: testBrokerRegistration("user@other", 4, 2), RetiredRevision: 1},
		{Endpoint: "user@arch", Registration: registration},
	}
	for i, tomb := range invalid {
		if err := tomb.Validate(); err == nil {
			t.Fatalf("invalid tombstone %d accepted", i)
		}
	}
}

func TestBrokerCompletionStaleRejection(t *testing.T) {
	current := testBrokerRegistration("user@arch", 4, 2)
	tests := []struct {
		name         string
		currentEpoch BrokerEpoch
		completion   domain.RemoteRegistration
		completionEp BrokerEpoch
		wantStale    bool
	}{
		{name: "exact match applies", currentEpoch: 3, completion: current, completionEp: 3, wantStale: false},
		{name: "epoch change stale", currentEpoch: 4, completion: current, completionEp: 3, wantStale: true},
		{name: "generation bump stalls old", currentEpoch: 3, completion: testBrokerRegistration("user@arch", 4, 1), completionEp: 3, wantStale: true},
		{name: "re-add incarnation stalls old", currentEpoch: 3, completion: testBrokerRegistration("user@arch", 5, 1), completionEp: 3, wantStale: true},
		{name: "zero epoch stale", currentEpoch: 0, completion: current, completionEp: 3, wantStale: true},
		{name: "zero completion epoch stale", currentEpoch: 3, completion: current, completionEp: 0, wantStale: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BrokerCompletionIsStale(current, tt.currentEpoch, tt.completion, tt.completionEp); got != tt.wantStale {
				t.Fatalf("BrokerCompletionIsStale() = %t, want %t", got, tt.wantStale)
			}
		})
	}
}

func TestBrokerPolicyCompatibility(t *testing.T) {
	base := testBrokerPolicy()
	if err := base.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	if !base.Compatible(base) {
		t.Fatal("identical policy must be compatible")
	}
	conflicts := map[string]BrokerPolicy{
		"protocol version": withBrokerPolicy(base, func(p *BrokerPolicy) { p.ProtocolVersion++ }),
		"catalog schema":   withBrokerPolicy(base, func(p *BrokerPolicy) { p.CatalogSchemaVersion++ }),
		"environment":      withBrokerPolicy(base, func(p *BrokerPolicy) { p.EnvironmentPolicy = protocol.EnvironmentPolicyClientOwned }),
		"transport":        withBrokerPolicy(base, func(p *BrokerPolicy) { p.Transport = "stdio" }),
	}
	for name, conflict := range conflicts {
		if base.Compatible(conflict) {
			t.Fatalf("conflicting policy %q must be rejected, never merged", name)
		}
	}
	invalid := []BrokerPolicy{
		{},
		{ProtocolVersion: protocol.Version},
		{ProtocolVersion: protocol.Version, CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion},
		{ProtocolVersion: protocol.Version, CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion, EnvironmentPolicy: protocol.EnvironmentPolicy(9), Transport: "quic"},
		{ProtocolVersion: protocol.Version, CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned},
	}
	for i, policy := range invalid {
		if err := policy.Validate(); err == nil {
			t.Fatalf("invalid policy %d accepted", i)
		}
	}
}

func TestBrokerOpenStreamRequestValidate(t *testing.T) {
	valid := func() BrokerOpenStreamRequest {
		return BrokerOpenStreamRequest{
			Connection:   BrokerConnectionID{1},
			Stream:       2,
			Endpoint:     "user@arch",
			Registration: testBrokerRegistration("user@arch", 4, 2),
			Target:       testBrokerTarget(),
			Env:          []string{"TERM=xterm-256color"},
			Policy:       testBrokerPolicy(),
		}
	}
	if err := valid().Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	tests := []struct {
		name    string
		mutate  func(*BrokerOpenStreamRequest)
		wantErr string
	}{
		{name: "zero connection", mutate: func(r *BrokerOpenStreamRequest) { r.Connection = BrokerConnectionID{} }},
		{name: "zero stream", mutate: func(r *BrokerOpenStreamRequest) { r.Stream = 0 }},
		{name: "empty endpoint", mutate: func(r *BrokerOpenStreamRequest) { r.Endpoint = "" }},
		{name: "endpoint registration mismatch", mutate: func(r *BrokerOpenStreamRequest) { r.Registration = testBrokerRegistration("user@other", 4, 2) }},
		{name: "zero target lifecycle", mutate: func(r *BrokerOpenStreamRequest) {
			r.Target = protocol.ExactSessionTarget{SessionName: "work"}
		}},
		{name: "bad session name", mutate: func(r *BrokerOpenStreamRequest) {
			r.Target = protocol.ExactSessionTarget{LifecycleID: testBrokerLifecycle(), SessionName: "bad name!"}
		}},
		{name: "too many env entries", mutate: func(r *BrokerOpenStreamRequest) {
			r.Env = make([]string, BrokerMaxEnvEntries+1)
		}},
		{name: "oversize env entry", mutate: func(r *BrokerOpenStreamRequest) {
			r.Env = []string{strings.Repeat("A", BrokerMaxEnvEntryBytes+1)}
		}},
		{name: "invalid env encoding", mutate: func(r *BrokerOpenStreamRequest) {
			r.Env = []string{"TERM=\xff"}
		}},
		{name: "invalid policy", mutate: func(r *BrokerOpenStreamRequest) { r.Policy = BrokerPolicy{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := valid()
			tt.mutate(&request)
			if err := request.Validate(); err == nil {
				t.Fatal("invalid open-stream request accepted")
			}
		})
	}
}

func TestBrokerMutationOutcomeUncertainty(t *testing.T) {
	for _, outcome := range []BrokerMutationOutcome{BrokerOutcomeOK, BrokerOutcomeFailed, BrokerOutcomeUnknown} {
		if err := outcome.Validate(); err != nil {
			t.Fatalf("Validate(%v) = %v", outcome, err)
		}
	}
	if err := BrokerMutationOutcome(0).Validate(); err == nil {
		t.Fatal("zero mutation outcome accepted")
	}
	if BrokerOutcomeOK.String() != "ok" || BrokerOutcomeUnknown.String() != "outcome_unknown" {
		t.Fatal("mutation outcome tokens changed")
	}

	valid := BrokerOperationResult{Operation: BrokerOperationID{1}, Outcome: BrokerOutcomeFailed, Text: "dial refused"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	unknown := BrokerOperationResult{Operation: BrokerOperationID{1}, Outcome: BrokerOutcomeUnknown, Text: "lost after commit"}
	if err := unknown.Validate(); err != nil {
		t.Fatalf("outcome-unknown result must stay valid: %v", err)
	}
	okWithText := BrokerOperationResult{Operation: BrokerOperationID{1}, Outcome: BrokerOutcomeOK, Text: "extra"}
	if err := okWithText.Validate(); err == nil {
		t.Fatal("successful result must not carry text")
	}
	oversize := BrokerOperationResult{Operation: BrokerOperationID{1}, Outcome: BrokerOutcomeFailed, Text: strings.Repeat("e", BrokerMaxErrorBytes+1)}
	if err := oversize.Validate(); err == nil {
		t.Fatal("oversize result text accepted")
	}
	unsafe := BrokerOperationResult{Operation: BrokerOperationID{1}, Outcome: BrokerOutcomeFailed, Text: "dial\x1b[31m refused"}
	if err := unsafe.Validate(); err == nil {
		t.Fatal("terminal control sequence in operation result accepted")
	}
}

func TestBrokerErrorTerminalOutcomes(t *testing.T) {
	tests := []struct {
		code         BrokerErrorCode
		wantTerminal bool
	}{
		{code: BrokerErrorUnavailable, wantTerminal: false},
		{code: BrokerErrorIncompatible, wantTerminal: false},
		{code: BrokerErrorTimeout, wantTerminal: false},
		{code: BrokerErrorCancelled, wantTerminal: false},
		{code: BrokerErrorOutcomeUnknown, wantTerminal: false},
		{code: BrokerErrorAttachmentLost, wantTerminal: false},
		{code: BrokerErrorStaleEpoch, wantTerminal: false},
		{code: BrokerErrorConflictingPolicy, wantTerminal: false},
		{code: BrokerErrorExplicitExit, wantTerminal: true},
		{code: BrokerErrorFatalTerminal, wantTerminal: true},
	}
	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			failure := BrokerError{Code: tt.code, Text: "visible typed error"}
			if err := failure.Validate(); err != nil {
				t.Fatalf("Validate() = %v", err)
			}
			if got := failure.Terminal(); got != tt.wantTerminal {
				t.Fatalf("Terminal() = %t, want %t", got, tt.wantTerminal)
			}
			if failure.Error() == "" {
				t.Fatal("Error() must render a message")
			}
		})
	}
	if err := (BrokerError{Code: BrokerErrorCode(0)}).Validate(); err == nil {
		t.Fatal("zero broker error code accepted")
	}
	oversize := BrokerError{Code: BrokerErrorTimeout, Text: strings.Repeat("e", BrokerMaxErrorBytes+1)}
	if err := oversize.Validate(); err == nil {
		t.Fatal("oversize broker error text accepted")
	}
	unsafe := BrokerError{Code: BrokerErrorTimeout, Text: "timeout\u202erorrE"}
	if err := unsafe.Validate(); err == nil {
		t.Fatal("bidirectional control in broker error accepted")
	}
}

func TestBrokerIdentityScopesStayDistinct(t *testing.T) {
	// Every identity level validates independently and zero values never
	// identify anything: connection, stream, operation, daemon incarnation,
	// registration, session lifecycle, and broker epoch are distinct types.
	if err := (BrokerConnectionID{}).Validate(); err == nil {
		t.Fatal("zero connection ID accepted")
	}
	if err := BrokerStreamID(0).Validate(); err == nil {
		t.Fatal("zero stream ID accepted")
	}
	if err := (BrokerOperationID{}).Validate(); err == nil {
		t.Fatal("zero operation ID accepted")
	}
	if err := (BrokerDaemonIncarnation{}).Validate(); err == nil {
		t.Fatal("zero daemon incarnation accepted")
	}
	if err := (domain.RemoteRegistration{}).Validate(); err == nil {
		t.Fatal("zero host registration accepted")
	}
	if err := (protocol.ExactSessionTarget{}).Validate(); err == nil {
		t.Fatal("zero session lifecycle target accepted")
	}
	if err := BrokerDaemonIdentity("").Validate(); err == nil {
		t.Fatal("empty daemon identity accepted")
	}
	if err := (BrokerStreamLost{}).Validate(); err == nil {
		t.Fatal("zero stream loss accepted")
	}
	loss := BrokerStreamLost{
		Connection: BrokerConnectionID{1},
		Stream:     2,
		Epoch:      3,
		Cause:      domain.RemoteFailureTimeout,
	}
	if err := loss.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
}

func withBrokerPolicy(base BrokerPolicy, mutate func(*BrokerPolicy)) BrokerPolicy {
	mutate(&base)
	return base
}

func TestBrokerInterfacesArePorts(t *testing.T) {
	// Broker, client, and daemon cross-layer contracts are ports: the
	// seams must stay interfaces so Mockery can generate mocks and
	// adapters can implement them without importing use cases.
	for name, iface := range map[string]reflect.Type{
		"BrokerService":            reflect.TypeOf((*BrokerService)(nil)).Elem(),
		"BrokerSnapshotStore":      reflect.TypeOf((*BrokerSnapshotStore)(nil)).Elem(),
		"BrokerHostProbe":          reflect.TypeOf((*BrokerHostProbe)(nil)).Elem(),
		"BrokerPhysicalConnection": reflect.TypeOf((*BrokerPhysicalConnection)(nil)).Elem(),
		"BrokerEndpointConnector":  reflect.TypeOf((*BrokerEndpointConnector)(nil)).Elem(),
		"BrokerListener":           reflect.TypeOf((*BrokerListener)(nil)).Elem(),
		"BrokerSubscription":       reflect.TypeOf((*BrokerSubscription)(nil)).Elem(),
	} {
		if iface.Kind() != reflect.Interface {
			t.Fatalf("%s must stay an interface, got %v", name, iface.Kind())
		}
	}
}
