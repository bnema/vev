package wire

import (
	"bytes"
	"errors"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The broker conversation is separate from the session conversation: its
// tags are disjoint from every session tag, both directions are closed
// oneof unions, and the strict scanner treats them exactly like the
// session envelopes.

// brokerClientTags is the frozen client tag inventory.
var brokerClientTags = map[string]protoreflect.FieldNumber{
	"register": 101, "subscribe": 102, "resync": 103, "unsubscribe": 104,
	"add_host": 105, "remove_host": 106, "reconcile": 107, "open_stream": 108,
	"client_stream_data": 109, "close_stream": 110,
}

// brokerServerTags is the frozen server tag inventory.
var brokerServerTags = map[string]protoreflect.FieldNumber{
	"registered": 201, "snapshot_part": 202, "operation_result": 203,
	"stream_opened": 204, "server_stream_data": 205, "stream_closed": 206,
	"progress": 207, "broker_error_message": 208, "shutdown": 209,
}

func oneofTags(t *testing.T, message proto.Message) map[string]protoreflect.FieldNumber {
	t.Helper()
	descriptor := message.ProtoReflect().Descriptor()
	if descriptor.Oneofs().Len() != 1 {
		t.Fatalf("%v has %d oneofs, want exactly the payload oneof", descriptor.FullName(), descriptor.Oneofs().Len())
	}
	oneof := descriptor.Oneofs().Get(0)
	if string(oneof.Name()) != "payload" {
		t.Fatalf("%v oneof %q is not the payload oneof", descriptor.FullName(), oneof.Name())
	}
	tags := make(map[string]protoreflect.FieldNumber, oneof.Fields().Len())
	for i := range oneof.Fields().Len() {
		field := oneof.Fields().Get(i)
		tags[string(field.Name())] = field.Number()
	}
	return tags
}

// TestBrokerEnvelopeInventory proves both broker envelopes carry exactly
// the frozen tag inventory and share no tag with the session envelopes or
// with each other.
func TestBrokerEnvelopeInventory(t *testing.T) {
	client := oneofTags(t, &BrokerClientEnvelope{})
	server := oneofTags(t, &BrokerServerEnvelope{})

	if len(client) != len(brokerClientTags) {
		t.Fatalf("broker client envelope has %d variants, want %d (%v)", len(client), len(brokerClientTags), client)
	}
	for name, number := range brokerClientTags {
		if got, ok := client[name]; !ok || got != number {
			t.Fatalf("broker client variant %q = %d (present %t), want %d", name, got, ok, number)
		}
	}
	if len(server) != len(brokerServerTags) {
		t.Fatalf("broker server envelope has %d variants, want %d (%v)", len(server), len(brokerServerTags), server)
	}
	for name, number := range brokerServerTags {
		if got, ok := server[name]; !ok || got != number {
			t.Fatalf("broker server variant %q = %d (present %t), want %d", name, got, ok, number)
		}
	}

	sessionTags := map[protoreflect.FieldNumber]string{}
	for _, envelope := range []proto.Message{&ClientEnvelope{}, &ServerEnvelope{}} {
		for name, number := range oneofTags(t, envelope) {
			sessionTags[number] = name
		}
	}
	for name, number := range brokerClientTags {
		if session, taken := sessionTags[number]; taken {
			t.Fatalf("broker client tag %d (%s) collides with session variant %s", number, name, session)
		}
	}
	clientByTag := map[protoreflect.FieldNumber]string{}
	for name, number := range brokerClientTags {
		clientByTag[number] = name
	}
	for name, number := range brokerServerTags {
		if session, taken := sessionTags[number]; taken {
			t.Fatalf("broker server tag %d (%s) collides with session variant %s", number, name, session)
		}
		if peer, taken := clientByTag[number]; taken {
			t.Fatalf("broker server tag %d (%s) collides with broker client variant %s", number, name, peer)
		}
	}

	for _, envelope := range []proto.Message{&BrokerClientEnvelope{}, &BrokerServerEnvelope{}} {
		if !isEnvelopeDescriptor(envelope.ProtoReflect().Descriptor()) {
			t.Fatalf("%v is not recognized as a directional envelope", envelope.ProtoReflect().Descriptor().FullName())
		}
	}
	// Payload messages are never envelopes: only the two directions require
	// exactly one variant.
	for _, payload := range []proto.Message{&Subscribe{}, &SnapshotPart{}, &Registered{}} {
		if isEnvelopeDescriptor(payload.ProtoReflect().Descriptor()) {
			t.Fatalf("payload %v is wrongly recognized as a directional envelope", payload.ProtoReflect().Descriptor().FullName())
		}
	}
}

func brokerScope() *BrokerScope {
	return &BrokerScope{BrokerEpoch: 7, ConnectionId: bytes.Repeat([]byte{0x11}, 16)}
}

func brokerRef() *BrokerStreamRef {
	return &BrokerStreamRef{Scope: brokerScope(), StreamId: 3}
}

func brokerRegistration() *RemoteRegistration {
	return &RemoteRegistration{Endpoint: "dev@host:22", Incarnation: bytes.Repeat([]byte{0x22}, 16), Generation: 9}
}

func brokerPolicy() *BrokerWirePolicy {
	return &BrokerWirePolicy{
		ProtocolVersion: 53, CatalogueVersion: 3, EnvironmentPolicy: 1,
		Transport: "quic", Trust: "pinned", Launch: "agent", Isolation: "user",
	}
}

func brokerCatalogTab() *BrokerCatalogTab {
	return &BrokerCatalogTab{Id: "tab-1", Index: 0, Name: "work", Detail: "1 pane", Attention: true}
}

func brokerCatalogSession() *BrokerCatalogSession {
	return &BrokerCatalogSession{
		LifecycleId: bytes.Repeat([]byte{0x33}, 16), Name: "work", State: 1,
		Ephemeral: true, Tabs: []*BrokerCatalogTab{brokerCatalogTab()}, Attached: true,
		LastUsedSeq: 4, ActiveTabId: "tab-1", Reason: "refreshing",
	}
}

// brokerClientSamples returns one populated envelope per client variant.
// Every sample's final serialized field is deliberately multi-byte so a
// one-byte truncation always lands inside a field.
func brokerClientSamples() map[string]*BrokerClientEnvelope {
	return map[string]*BrokerClientEnvelope{
		"register":    {Payload: &BrokerClientEnvelope_Register{Register: &Register{}}},
		"subscribe":   {Payload: &BrokerClientEnvelope_Subscribe{Subscribe: &Subscribe{Scope: brokerScope(), Generation: 1 << 40}}},
		"resync":      {Payload: &BrokerClientEnvelope_Resync{Resync: &Resync{Scope: brokerScope(), Generation: 1 << 41}}},
		"unsubscribe": {Payload: &BrokerClientEnvelope_Unsubscribe{Unsubscribe: &Unsubscribe{Scope: brokerScope(), Generation: 1 << 42}}},
		"add_host": {Payload: &BrokerClientEnvelope_AddHost{AddHost: &AddHost{
			Scope: brokerScope(), OperationId: bytes.Repeat([]byte{0x44}, 16), Endpoint: "dev@host:22",
		}}},
		"remove_host": {Payload: &BrokerClientEnvelope_RemoveHost{RemoveHost: &RemoveHost{
			Scope: brokerScope(), OperationId: bytes.Repeat([]byte{0x45}, 16), Endpoint: "dev@host:23",
		}}},
		"reconcile": {Payload: &BrokerClientEnvelope_Reconcile{Reconcile: &Reconcile{
			Scope: brokerScope(), Registration: brokerRegistration(),
		}}},
		"open_stream": {Payload: &BrokerClientEnvelope_OpenStream{OpenStream: &OpenStream{
			Ref: brokerRef(), Purpose: 1, Local: false, Endpoint: "dev@host:22",
			Registration: brokerRegistration(),
			Target:       &ExactTarget{LifecycleId: &LifecycleID{Value: bytes.Repeat([]byte{0x55}, 16)}, SessionName: "work"},
			Env:          []string{"TERM=xterm-256color", "LANG=C.UTF-8"},
			Policy:       brokerPolicy(),
		}}},
		"client_stream_data": {Payload: &BrokerClientEnvelope_ClientStreamData{ClientStreamData: &ClientStreamData{
			Ref: brokerRef(), Data: bytes.Repeat([]byte{0x66}, 24),
		}}},
		"close_stream": {Payload: &BrokerClientEnvelope_CloseStream{CloseStream: &CloseStream{Ref: brokerRef()}}},
	}
}

// brokerServerSamples returns one populated envelope per server variant.
func brokerServerSamples() map[string]*BrokerServerEnvelope {
	return map[string]*BrokerServerEnvelope{
		"registered": {Payload: &BrokerServerEnvelope_Registered{Registered: &Registered{Scope: brokerScope()}}},
		"snapshot_part": {Payload: &BrokerServerEnvelope_SnapshotPart{SnapshotPart: &SnapshotPart{
			Scope: brokerScope(), Generation: 2, Revision: 5, Index: 9,
			Part: &SnapshotPart_Host{Host: &SnapshotHost{
				HostIndex: 1, Endpoint: "dev@host:22", DisplayOrigin: "dev@host",
				Rank: 2, Registration: brokerRegistration(), Availability: 1,
				Checking:            true,
				LastAttempt:         &BrokerTimestamp{Seconds: 1700000000, Nanos: 1},
				LastSuccess:         &BrokerTimestamp{Seconds: 1700000001, Nanos: 2},
				NextDue:             &BrokerTimestamp{Seconds: 1700000002, Nanos: 3},
				ConsecutiveFailures: 4, FailureEpisode: 6, FailureKind: 1,
				InventoryKnown: true, SessionCount: 1,
			}},
		}}},
		"operation_result": {Payload: &BrokerServerEnvelope_OperationResult{OperationResult: &OperationResult{
			Scope: brokerScope(), OperationId: bytes.Repeat([]byte{0x77}, 16), Outcome: 2, Removed: true,
			Error: &BrokerErrorDetail{Code: 2, Text: "incompatible", AdmissionCode: 1, FailureKind: 3},
		}}},
		"stream_opened": {Payload: &BrokerServerEnvelope_StreamOpened{StreamOpened: &StreamOpened{Ref: brokerRef()}}},
		"server_stream_data": {Payload: &BrokerServerEnvelope_ServerStreamData{ServerStreamData: &ServerStreamData{
			Ref: brokerRef(), Data: bytes.Repeat([]byte{0x88}, 24),
		}}},
		"stream_closed": {Payload: &BrokerServerEnvelope_StreamClosed{StreamClosed: &StreamClosed{
			Ref: brokerRef(), Error: &BrokerErrorDetail{Code: 6, Text: "attachment_lost"},
		}}},
		"progress": {Payload: &BrokerServerEnvelope_Progress{Progress: &Progress{
			Ref: brokerRef(), Phase: 1, Text: "probing",
		}}},
		"broker_error_message": {Payload: &BrokerServerEnvelope_BrokerErrorMessage{BrokerErrorMessage: &BrokerErrorMessage{
			Scope: brokerScope(), Error: &BrokerErrorDetail{Code: 1, Text: "unavailable"},
		}}},
		"shutdown": {Payload: &BrokerServerEnvelope_Shutdown{Shutdown: &Shutdown{
			Scope: brokerScope(), Reason: 1, Text: "idle exit",
		}}},
	}
}

func mustMarshalBroker(t *testing.T, message proto.Message) []byte {
	t.Helper()
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestBrokerEnvelopeRoundTrip proves every variant survives strict scan,
// generated unmarshal, and equality, including nested snapshot payloads.
func TestBrokerEnvelopeRoundTrip(t *testing.T) {
	for name, sample := range brokerClientSamples() {
		t.Run("client/"+name, func(t *testing.T) {
			raw := mustMarshalBroker(t, sample)
			decoded := &BrokerClientEnvelope{}
			if err := validatedUnmarshal(decoded, raw); err != nil {
				t.Fatalf("validatedUnmarshal() = %v", err)
			}
			if !proto.Equal(sample, decoded) {
				t.Fatalf("roundtrip mismatch:\n got %v\nwant %v", decoded, sample)
			}
		})
	}
	for name, sample := range brokerServerSamples() {
		t.Run("server/"+name, func(t *testing.T) {
			raw := mustMarshalBroker(t, sample)
			decoded := &BrokerServerEnvelope{}
			if err := validatedUnmarshal(decoded, raw); err != nil {
				t.Fatalf("validatedUnmarshal() = %v", err)
			}
			if !proto.Equal(sample, decoded) {
				t.Fatalf("roundtrip mismatch:\n got %v\nwant %v", decoded, sample)
			}
		})
	}

	// The snapshot nested unions round-trip their non-host parts too, so
	// every part tag is exercised.
	for name, part := range map[string]*SnapshotPart{
		"begin":     {Part: &SnapshotPart_Begin{Begin: &SnapshotBegin{HostCount: 1, SessionCount: 2, TombstoneCount: 3}}},
		"session":   {Part: &SnapshotPart_Session{Session: &SnapshotSession{HostIndex: 1, SessionIndex: 2, Session: brokerCatalogSession()}}},
		"tombstone": {Part: &SnapshotPart_Tombstone{Tombstone: &SnapshotTombstone{TombstoneIndex: 3, Registration: brokerRegistration(), RetiredRevision: 4}}},
		"end":       {Part: &SnapshotPart_End{End: &SnapshotEnd{}}},
	} {
		t.Run("snapshot/"+name, func(t *testing.T) {
			raw := mustMarshalBroker(t, &BrokerServerEnvelope{Payload: &BrokerServerEnvelope_SnapshotPart{
				SnapshotPart: &SnapshotPart{
					Scope: brokerScope(), Generation: 1, Revision: 2, Index: 3, Part: part.Part,
				},
			}})
			decoded := &BrokerServerEnvelope{}
			if err := validatedUnmarshal(decoded, raw); err != nil {
				t.Fatalf("validatedUnmarshal() = %v", err)
			}
			got := decoded.GetSnapshotPart()
			if got == nil || got.GetIndex() != 3 || got.Part == nil {
				t.Fatalf("snapshot part did not round-trip: %v", decoded)
			}
		})
	}
}

// TestBrokerEnvelopeRequiredVariant proves both broker envelopes demand
// exactly one payload variant, like the session envelopes.
func TestBrokerEnvelopeRequiredVariant(t *testing.T) {
	for _, message := range []proto.Message{&BrokerClientEnvelope{}, &BrokerServerEnvelope{}} {
		if err := ScanEnvelope(message, mustMarshalBroker(t, message)); !errors.Is(err, ErrScanEmpty) {
			t.Fatalf("ScanEnvelope(%T empty) = %v, want ErrScanEmpty", message, err)
		}
	}
}

// TestBrokerEnvelopeWrongDirection proves a payload serialized in one
// direction is rejected when scanned as the other direction.
func TestBrokerEnvelopeWrongDirection(t *testing.T) {
	client := mustMarshalBroker(t, brokerClientSamples()["subscribe"])
	if err := ScanEnvelope(&BrokerServerEnvelope{}, client); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("client payload as server envelope = %v, want ErrScanUnknown", err)
	}
	server := mustMarshalBroker(t, brokerServerSamples()["progress"])
	if err := ScanEnvelope(&BrokerClientEnvelope{}, server); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("server payload as client envelope = %v, want ErrScanUnknown", err)
	}
	// A session payload is unknown on the broker conversation too.
	session := mustMarshalBroker(t, &ClientEnvelope{Payload: &ClientEnvelope_Ping{Ping: &Ping{}}})
	if err := ScanEnvelope(&BrokerClientEnvelope{}, session); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("session payload as broker envelope = %v, want ErrScanUnknown", err)
	}
}

// TestBrokerEnvelopeDuplicateAlternatives proves a second oneof occurrence
// is rejected whether it repeats the same variant or switches alternatives.
func TestBrokerEnvelopeDuplicateAlternatives(t *testing.T) {
	register := mustMarshalBroker(t, brokerClientSamples()["register"])
	subscribe := mustMarshalBroker(t, brokerClientSamples()["subscribe"])
	concatenated := append(append([]byte(nil), register...), subscribe...)
	if err := ScanEnvelope(&BrokerClientEnvelope{}, concatenated); !errors.Is(err, ErrScanDuplicate) {
		t.Fatalf("alternate alternatives = %v, want ErrScanDuplicate", err)
	}
	repeated := append(append([]byte(nil), subscribe...), subscribe...)
	if err := ScanEnvelope(&BrokerClientEnvelope{}, repeated); !errors.Is(err, ErrScanDuplicate) {
		t.Fatalf("duplicate variant = %v, want ErrScanDuplicate", err)
	}

	// The nested snapshot part union is exclusive as well.
	host := mustMarshalBroker(t, &SnapshotPart{Part: &SnapshotPart_Host{Host: &SnapshotHost{Endpoint: "dev@host:22"}}})
	end := mustMarshalBroker(t, &SnapshotPart{Part: &SnapshotPart_End{End: &SnapshotEnd{}}})
	if err := ScanEnvelope(&SnapshotPart{}, append(append([]byte(nil), host...), end...)); !errors.Is(err, ErrScanDuplicate) {
		t.Fatalf("duplicate snapshot part = %v, want ErrScanDuplicate", err)
	}
}

// TestBrokerEnvelopeRejectsUnknownAndWrongTypes proves unknown and
// mistyped fields are rejected recursively, at the envelope level and
// several levels into a nested snapshot payload.
func TestBrokerEnvelopeRejectsUnknownAndWrongTypes(t *testing.T) {
	// Unknown field on the envelope itself.
	raw := mustMarshalBroker(t, brokerClientSamples()["close_stream"])
	raw = protowire.AppendTag(raw, 199, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 1)
	if err := ScanEnvelope(&BrokerClientEnvelope{}, raw); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("unknown envelope field = %v, want ErrScanUnknown", err)
	}

	// Unknown field two levels down: BrokerClientEnvelope → OpenStream → BrokerStreamRef.
	ref := protowire.AppendTag(nil, 1, protowire.BytesType) // BrokerStreamRef.scope
	ref = protowire.AppendBytes(ref, mustMarshalBroker(t, brokerScope()))
	ref = protowire.AppendTag(ref, 2, protowire.VarintType) // BrokerStreamRef.stream_id
	ref = protowire.AppendVarint(ref, 3)
	ref = protowire.AppendTag(ref, 9, protowire.VarintType) // unknown in BrokerStreamRef
	ref = protowire.AppendVarint(ref, 1)
	open := protowire.AppendTag(nil, 1, protowire.BytesType) // OpenStream.ref
	open = protowire.AppendBytes(open, ref)
	outer := protowire.AppendTag(nil, 108, protowire.BytesType) // BrokerClientEnvelope.open_stream
	outer = protowire.AppendBytes(outer, open)
	if err := ScanEnvelope(&BrokerClientEnvelope{}, outer); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("nested unknown field = %v, want ErrScanUnknown", err)
	}

	// Unknown field inside a nested snapshot part message.
	part := protowire.AppendTag(nil, 11, protowire.BytesType) // SnapshotPart.host
	host := protowire.AppendTag(nil, 2, protowire.BytesType)  // SnapshotHost.endpoint
	host = protowire.AppendBytes(host, []byte("dev@host:22"))
	host = protowire.AppendTag(host, 99, protowire.VarintType)
	host = protowire.AppendVarint(host, 1)
	part = protowire.AppendBytes(part, host)
	raw = protowire.AppendTag(nil, 202, protowire.BytesType) // BrokerServerEnvelope.snapshot_part
	raw = protowire.AppendBytes(raw, part)
	if err := ScanEnvelope(&BrokerServerEnvelope{}, raw); !errors.Is(err, ErrScanUnknown) {
		t.Fatalf("unknown nested snapshot field = %v, want ErrScanUnknown", err)
	}

	// Wrong wire type: BrokerClientEnvelope.close_stream (bytes) as a varint.
	wrongType := protowire.AppendTag(nil, 110, protowire.VarintType)
	wrongType = protowire.AppendVarint(wrongType, 1)
	if err := ScanEnvelope(&BrokerClientEnvelope{}, wrongType); !errors.Is(err, ErrScanWireType) {
		t.Fatalf("wrong wire type = %v, want ErrScanWireType", err)
	}

	// A raw varint wider than the declared uint32 is rejected before
	// generated unmarshal can truncate its high bits.
	open = protowire.AppendTag(nil, 2, protowire.VarintType) // OpenStream.purpose
	open = protowire.AppendVarint(open, 1<<33)
	outer = protowire.AppendTag(nil, 108, protowire.BytesType)
	outer = protowire.AppendBytes(outer, open)
	if err := ScanEnvelope(&BrokerClientEnvelope{}, outer); !errors.Is(err, ErrScanVarintRange) {
		t.Fatalf("uint32 overflow = %v, want ErrScanVarintRange", err)
	}
}

// TestBrokerEnvelopeTruncation proves truncated tags, varints, lengths, and
// every strict prefix of a valid envelope are rejected.
func TestBrokerEnvelopeTruncation(t *testing.T) {
	if err := ScanEnvelope(&BrokerClientEnvelope{}, []byte{0xFF}); !errors.Is(err, ErrScanTruncated) {
		t.Fatalf("truncated tag = %v, want ErrScanTruncated", err)
	}
	overlong := protowire.AppendTag(nil, 101, protowire.BytesType)
	overlong = protowire.AppendBytes(overlong, make([]byte, 8))
	overlong = overlong[:len(overlong)-4]
	if err := ScanEnvelope(&BrokerClientEnvelope{}, overlong); !errors.Is(err, ErrScanTruncated) {
		t.Fatalf("overlong length = %v, want ErrScanTruncated", err)
	}
	partialVarint := protowire.AppendTag(nil, 102, protowire.BytesType) // BrokerClientEnvelope.subscribe
	subscribe := protowire.AppendTag(nil, 2, protowire.VarintType)      // Subscribe.generation
	partialVarint = protowire.AppendBytes(partialVarint, append(subscribe, 0x80))
	if err := ScanEnvelope(&BrokerClientEnvelope{}, partialVarint); !errors.Is(err, ErrScanTruncated) {
		t.Fatalf("truncated varint = %v, want ErrScanTruncated", err)
	}

	// Every strict prefix of a valid envelope must be refused: a top-level
	// envelope requires its complete variant.
	for name, sample := range brokerClientSamples() {
		raw := mustMarshalBroker(t, sample)
		for size := range len(raw) {
			if err := ScanEnvelope(&BrokerClientEnvelope{}, raw[:size]); err == nil {
				t.Fatalf("client prefix %s[:%d] accepted", name, size)
			}
		}
	}
	for name, sample := range brokerServerSamples() {
		raw := mustMarshalBroker(t, sample)
		for size := range len(raw) {
			if err := ScanEnvelope(&BrokerServerEnvelope{}, raw[:size]); err == nil {
				t.Fatalf("server prefix %s[:%d] accepted", name, size)
			}
		}
	}
}

// TestBrokerEnvelopeTrailingGarbage proves trailing bytes and a second
// concatenated envelope are rejected.
func TestBrokerEnvelopeTrailingGarbage(t *testing.T) {
	raw := mustMarshalBroker(t, brokerServerSamples()["stream_opened"])
	for _, tail := range [][]byte{
		{0x00},
		protowire.AppendTag(nil, 209, protowire.VarintType),
		mustMarshalBroker(t, brokerServerSamples()["shutdown"]),
	} {
		payload := append(append([]byte(nil), raw...), tail...)
		if err := ScanEnvelope(&BrokerServerEnvelope{}, payload); err == nil {
			t.Fatalf("trailing %x accepted", tail)
		}
	}
}
