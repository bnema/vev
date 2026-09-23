package ports

import (
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/stretchr/testify/require"
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
		Trust:                "trust", Launch: "launch", Isolation: "user",
	}
}

func testBrokerObservation(endpoint string) BrokerDaemonObservation {
	return BrokerDaemonObservation{
		Endpoint:      endpoint,
		DisplayOrigin: endpoint,
		Registration:  testBrokerRegistration(endpoint, 1, 1),
		Policy:        testBrokerPolicy(),
		Availability:  domain.RemoteAvailabilityReachable,
	}
}

func testLocalBrokerObservation() BrokerDaemonObservation {
	return BrokerDaemonObservation{
		Local:         true,
		DisplayOrigin: "local",
		Policy:        testBrokerPolicy(),
		Availability:  domain.RemoteAvailabilityReachable,
	}
}

func validBrokerSnapshot() BrokerSnapshot {
	return BrokerSnapshot{
		Epoch:    3,
		Revision: 9,
		Daemons:  []BrokerDaemonObservation{testBrokerObservation("user@arch")},
	}
}

// TestBrokerHostStoreIsASnapshotStore pins the interface guard: the exclusive
// owner of durable membership also owns the advisory snapshot, so a
// BrokerHostStore must stay usable wherever a BrokerSnapshotStore is required.
func TestBrokerHostStoreIsASnapshotStore(t *testing.T) {
	host := reflect.TypeOf((*BrokerHostStore)(nil)).Elem()
	snapshot := reflect.TypeOf((*BrokerSnapshotStore)(nil)).Elem()
	if !host.Implements(snapshot) {
		t.Fatal("BrokerHostStore must implement BrokerSnapshotStore")
	}
}

// TestValidateDurableHostProjection matches the durable rules the offline store
// enforces and the registry re-checks before adopting an observation.
func TestValidateDurableHostProjection(t *testing.T) {
	session := catalogue.RemoteCatalogSession{
		LifecycleID: testBrokerLifecycle(),
		Name:        "work",
		State:       catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{},
	}
	tests := []struct {
		name    string
		mutate  func(*BrokerDaemonObservation)
		wantErr bool
	}{
		{name: "empty inventory", mutate: func(*BrokerDaemonObservation) {}},
		{name: "valid inventory", mutate: func(o *BrokerDaemonObservation) {
			o.InventoryKnown = true
			o.Sessions = []catalogue.RemoteCatalogSession{session}
		}},
		{name: "local observation", mutate: func(o *BrokerDaemonObservation) {
			*o = testLocalBrokerObservation()
		}, wantErr: true},
		{name: "zero availability", mutate: func(o *BrokerDaemonObservation) { o.Availability = 0 }, wantErr: true},
		{name: "unknown availability", mutate: func(o *BrokerDaemonObservation) { o.Availability = domain.RemoteAvailabilityUnknown }},
		{name: "availability past the closed range", mutate: func(o *BrokerDaemonObservation) { o.Availability = domain.RemoteAvailabilityNoDaemon + 1 }, wantErr: true},
		{name: "failure kind past the closed range", mutate: func(o *BrokerDaemonObservation) { o.LastFailure.Kind = domain.RemoteFailureInvalidResponse + 1 }, wantErr: true},
		{name: "missing lifecycle identity", mutate: func(o *BrokerDaemonObservation) {
			o.InventoryKnown = true
			o.Sessions = []catalogue.RemoteCatalogSession{{Name: "work", State: catalogue.RemoteCatalogSessionUp, Tabs: []catalogue.RemoteCatalogTab{}}}
		}, wantErr: true},
		{name: "absent tab list", mutate: func(o *BrokerDaemonObservation) {
			broken := session
			broken.Tabs = nil
			o.InventoryKnown = true
			o.Sessions = []catalogue.RemoteCatalogSession{broken}
		}, wantErr: true},
		{name: "sessions past the catalogue bound", mutate: func(o *BrokerDaemonObservation) {
			o.InventoryKnown = true
			o.Sessions = make([]catalogue.RemoteCatalogSession, catalogue.RemoteCatalogMaxSessions+1)
			for i := range o.Sessions {
				o.Sessions[i] = session
			}
		}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obs := testBrokerObservation("user@arch")
			obs.LastSuccess = time.Unix(50, 0)
			tc.mutate(&obs)
			if gotErr := ValidateDurableHostProjection(obs); (gotErr != nil) != tc.wantErr {
				t.Fatalf("ValidateDurableHostProjection() = %v, wantErr %t", gotErr, tc.wantErr)
			}
		})
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
		{name: "too many daemons", mutate: func(s *BrokerSnapshot) {
			s.Daemons = make([]BrokerDaemonObservation, BrokerMaxDaemonsPerSnapshot+1)
			for i := range s.Daemons {
				s.Daemons[i] = testBrokerObservation("user@arch")
				s.Daemons[i].Registration = testBrokerRegistration("user@arch", 1, 1)
			}
		}, wantErr: true},
		{name: "local daemon plus remote hosts is valid", mutate: func(s *BrokerSnapshot) {
			s.Daemons = append([]BrokerDaemonObservation{testLocalBrokerObservation()}, s.Daemons...)
		}},
		{name: "two local daemons", mutate: func(s *BrokerSnapshot) {
			s.Daemons = append(s.Daemons, testLocalBrokerObservation(), testLocalBrokerObservation())
		}, wantErr: true},
		{name: "duplicate host", mutate: func(s *BrokerSnapshot) {
			s.Daemons = append(s.Daemons, testBrokerObservation("user@arch"))
		}, wantErr: true},
		{name: "endpoint registration mismatch", mutate: func(s *BrokerSnapshot) {
			s.Daemons[0].Registration = testBrokerRegistration("user@other", 1, 1)
		}, wantErr: true},
		{name: "zero registration incarnation", mutate: func(s *BrokerSnapshot) {
			s.Daemons[0].Registration = domain.RemoteRegistration{Endpoint: "user@arch", Generation: 1}
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
	original.Daemons = append([]BrokerDaemonObservation{testLocalBrokerObservation()}, original.Daemons...)
	original.Daemons[1].InventoryKnown = true
	original.Daemons[1].Sessions = []catalogue.RemoteCatalogSession{{
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
	local := 0
	for _, daemon := range cloned.Daemons {
		if daemon.Local {
			local++
		}
	}
	if local != 1 {
		t.Fatalf("Clone() lost the local daemon entry: %d local entries", local)
	}
	cloned.Daemons[1].Sessions[0].Name = "mutated"
	cloned.Daemons[1].Sessions[0].Tabs[0].ID = "mutated"
	if original.Daemons[1].Sessions[0].Name != "work" || original.Daemons[1].Sessions[0].Tabs[0].ID != "tab-1" {
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
		"trust":            withBrokerPolicy(base, func(p *BrokerPolicy) { p.Trust = "other" }),
		"launch":           withBrokerPolicy(base, func(p *BrokerPolicy) { p.Launch = "other" }),
		"isolation":        withBrokerPolicy(base, func(p *BrokerPolicy) { p.Isolation = "other" }),
	}
	for name, conflict := range conflicts {
		if base.Compatible(conflict) {
			t.Fatalf("conflicting policy %q must be rejected, never merged", name)
		}
	}
	invalid := []BrokerPolicy{
		withBrokerPolicy(base, func(p *BrokerPolicy) { p.ProtocolVersion = 0 }),
		withBrokerPolicy(base, func(p *BrokerPolicy) { p.CatalogSchemaVersion = 0 }),
		withBrokerPolicy(base, func(p *BrokerPolicy) { p.EnvironmentPolicy = protocol.EnvironmentPolicy(9) }),
		withBrokerPolicy(base, func(p *BrokerPolicy) { p.Transport = "" }),
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
			Epoch: 1, Purpose: BrokerStreamAttachment,
			Admission:    BrokerAdmissionExact,
			Connection:   BrokerConnectionID{1},
			Stream:       2,
			Endpoint:     "user@arch",
			Registration: testBrokerRegistration("user@arch", 4, 2),
			Target:       testBrokerTarget(),
			Env:          []string{"TERM=xterm-256color"},
			Policy:       testBrokerPolicy(),
			StartMode:    BrokerDaemonStartIfNeeded,
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
		{name: "exact admission carries a creation name", mutate: func(r *BrokerOpenStreamRequest) {
			r.Name = "work"
		}},
		{name: "named creation carries an exact target", mutate: func(r *BrokerOpenStreamRequest) {
			r.Admission = BrokerAdmissionCreateNamed
			r.Name = "work"
		}},
		{name: "named creation with invalid name", mutate: func(r *BrokerOpenStreamRequest) {
			r.Admission = BrokerAdmissionCreateNamed
			r.Name = "bad name!"
			r.Target = protocol.ExactSessionTarget{}
		}},
		{name: "ephemeral creation carries a name", mutate: func(r *BrokerOpenStreamRequest) {
			r.Admission = BrokerAdmissionCreateEphemeral
			r.Target = protocol.ExactSessionTarget{}
			r.Name = "work"
		}},
		{name: "ephemeral creation carries a target", mutate: func(r *BrokerOpenStreamRequest) {
			r.Admission = BrokerAdmissionCreateEphemeral
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
		{name: "zero start mode", mutate: func(r *BrokerOpenStreamRequest) { r.StartMode = 0 }},
		{name: "unknown start mode", mutate: func(r *BrokerOpenStreamRequest) { r.StartMode = BrokerDaemonStartMode(9) }},
		{name: "attachment existing only", mutate: func(r *BrokerOpenStreamRequest) { r.StartMode = BrokerDaemonExistingOnly }},
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
		{code: BrokerErrorHostConflict, wantTerminal: false},
		{code: BrokerErrorMembershipImmutable, wantTerminal: false},
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
		"BrokerRouteAuthority":     reflect.TypeOf((*BrokerRouteAuthority)(nil)).Elem(),
		"BrokerIdentityBinder":     reflect.TypeOf((*BrokerIdentityBinder)(nil)).Elem(),
		"BrokerLogicalConnection":  reflect.TypeOf((*BrokerLogicalConnection)(nil)).Elem(),
		"BrokerSnapshotStore":      reflect.TypeOf((*BrokerSnapshotStore)(nil)).Elem(),
		"BrokerHostProbe":          reflect.TypeOf((*BrokerHostProbe)(nil)).Elem(),
		"BrokerPhysicalConnection": reflect.TypeOf((*BrokerPhysicalConnection)(nil)).Elem(),
		"BrokerEndpointConnector":  reflect.TypeOf((*BrokerEndpointConnector)(nil)).Elem(),
		"BrokerConnector":          reflect.TypeOf((*BrokerConnector)(nil)).Elem(),
		"BrokerAuthority":          reflect.TypeOf((*BrokerAuthority)(nil)).Elem(),
		"BrokerListener":           reflect.TypeOf((*BrokerListener)(nil)).Elem(),
		"BrokerSubscription":       reflect.TypeOf((*BrokerSubscription)(nil)).Elem(),
	} {
		if iface.Kind() != reflect.Interface {
			t.Fatalf("%s must stay an interface, got %v", name, iface.Kind())
		}
	}
}

// TestSanitizeBrokerDisplayText pins the sanitizer to the display validator:
// every result is valid UTF-8 and carries no rune validateBrokerDisplayText
// refuses, so a producer can sanitize a derived presentation hint and never
// publish a value its own contract rejects.
func TestSanitizeBrokerDisplayText(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "ordinary text is returned unchanged", input: "user@arch", want: "user@arch"},
		{name: "login-prefixed origin is returned unchanged", input: "user@host.example:2222", want: "user@host.example:2222"},
		{name: "accented and emoji text is kept intact", input: "caf\u00e9 \U0001f5a5", want: "caf\u00e9 \U0001f5a5"},
		{name: "right-to-left override is dropped", input: "sh\u202eell", want: "shell"},
		{name: "left-to-right embedding is dropped", input: "a\u202ab", want: "ab"},
		{name: "C0 control is dropped", input: "a\x00b\x1bc", want: "abc"},
		{name: "line separator is dropped", input: "a\u2028b", want: "ab"},
		{name: "paragraph separator is dropped", input: "a\u2029b", want: "ab"},
		{name: "invalid utf-8 is replaced", input: "ok\xff\xfe", want: "ok\ufffd\ufffd"},
		{name: "every rune disallowed yields empty", input: "\u202e\u2028\x00", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SanitizeBrokerDisplayText(tc.input)
			require.Equal(t, tc.want, got)
			require.True(t, utf8.ValidString(got), "sanitized text must always be valid UTF-8")
			require.Equal(t, -1, strings.IndexFunc(got, func(r rune) bool { return !brokerDisplayRuneAllowed(r) }), "sanitized text must carry no disallowed rune")
			require.NoError(t, validateBrokerDisplayText(got, BrokerMaxDisplayOriginBytes, "display origin"))
		})
	}
}

// TestBrokerSnapshotValidateLocalDaemonPlacement pins the wire placement rule:
// the local daemon observation is carried at index zero only, a local
// observation anywhere else is refused, and a local daemon is still refused
// when it appears twice.
func TestBrokerSnapshotValidateLocalDaemonPlacement(t *testing.T) {
	tests := []struct {
		name            string
		daemons         []BrokerDaemonObservation
		wantErrContains string
	}{
		{
			name:    "local daemon first",
			daemons: []BrokerDaemonObservation{testLocalBrokerObservation(), testBrokerObservation("user@arch")},
		},
		{
			name:            "local daemon at index one is refused",
			daemons:         []BrokerDaemonObservation{testBrokerObservation("user@arch"), testLocalBrokerObservation()},
			wantErrContains: "local daemon at index 1",
		},
		{
			name:            "local daemon after two remotes is refused",
			daemons:         []BrokerDaemonObservation{testBrokerObservation("user@arch"), testBrokerObservation("user@other"), testLocalBrokerObservation()},
			wantErrContains: "local daemon at index 2",
		},
		{
			name:            "two local daemons are refused",
			daemons:         []BrokerDaemonObservation{testLocalBrokerObservation(), testLocalBrokerObservation()},
			wantErrContains: "more than one local daemon",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := validBrokerSnapshot()
			snapshot.Daemons = tc.daemons
			err := snapshot.Validate()
			if tc.wantErrContains != "" {
				require.ErrorContains(t, err, tc.wantErrContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestBrokerStreamPurposeShapes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		purpose    BrokerStreamPurpose
		admission  BrokerStreamAdmission
		createName string
		target     bool
		env        bool
		startMode  BrokerDaemonStartMode
		valid      bool
	}{
		{"attachment exact", BrokerStreamAttachment, BrokerAdmissionExact, "", true, true, BrokerDaemonStartIfNeeded, true},
		{"attachment missing target", BrokerStreamAttachment, BrokerAdmissionExact, "", false, false, BrokerDaemonStartIfNeeded, false},
		{"attachment missing admission", BrokerStreamAttachment, 0, "", true, false, BrokerDaemonStartIfNeeded, false},
		{"attachment named creation", BrokerStreamAttachment, BrokerAdmissionCreateNamed, "work", false, true, BrokerDaemonStartIfNeeded, true},
		{"attachment ephemeral creation", BrokerStreamAttachment, BrokerAdmissionCreateEphemeral, "", false, true, BrokerDaemonStartIfNeeded, true},
		{"attachment existing only", BrokerStreamAttachment, BrokerAdmissionCreateEphemeral, "", false, true, BrokerDaemonExistingOnly, false},
		{"attachment zero start mode", BrokerStreamAttachment, BrokerAdmissionCreateEphemeral, "", false, true, 0, false},
		{"control", BrokerStreamControl, 0, "", false, false, BrokerDaemonStartIfNeeded, true},
		{"control existing only", BrokerStreamControl, 0, "", false, false, BrokerDaemonExistingOnly, true},
		{"observation", BrokerStreamObservation, 0, "", false, false, BrokerDaemonExistingOnly, true},
		{"observation start if needed", BrokerStreamObservation, 0, "", false, false, BrokerDaemonStartIfNeeded, false},
		{"observation zero start mode", BrokerStreamObservation, 0, "", false, false, 0, false},
		{"control target", BrokerStreamControl, 0, "", true, false, BrokerDaemonStartIfNeeded, false},
		{"control admission", BrokerStreamControl, BrokerAdmissionExact, "", false, false, BrokerDaemonStartIfNeeded, false},
		{"observation env", BrokerStreamObservation, 0, "", false, true, BrokerDaemonExistingOnly, false},
		{"observation admission", BrokerStreamObservation, BrokerAdmissionCreateEphemeral, "", false, false, BrokerDaemonExistingOnly, false},
		{"unknown", 0, 0, "", false, false, BrokerDaemonStartIfNeeded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := BrokerOpenStreamRequest{Epoch: 1, Connection: BrokerConnectionID{1}, Stream: 1, Local: true, Purpose: tc.purpose, Admission: tc.admission, Name: tc.createName, Policy: testBrokerPolicy(), StartMode: tc.startMode}
			if tc.target {
				r.Target = testBrokerTarget()
			}
			if tc.env {
				r.Env = []string{"TERM=xterm"}
			}
			if (r.Validate() == nil) != tc.valid {
				t.Fatalf("Validate()=%v", r.Validate())
			}
		})
	}
}
