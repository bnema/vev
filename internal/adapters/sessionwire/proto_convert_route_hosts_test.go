package sessionwire

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func routeHostsTestSnapshot() protocol.RecentRouteSnapshot {
	target := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{7}, SessionName: "work"}
	return protocol.RecentRouteSnapshot{
		Generation: 3,
		Entries: []protocol.RecentRouteEntry{{
			Key: 1, Generation: 1, Target: target, Name: "work", HostLabel: "box",
			Kind: protocol.RouteKindRemote, Attention: true, AttentionSeq: 5, Visited: true,
		}},
		Hosts: []protocol.RouteHost{
			{Key: 2, Generation: 1, Label: "a", Kind: protocol.RouteKindRemote},
			{Key: 3, Generation: 1, Label: "local", Kind: protocol.RouteKindLocal},
		},
	}
}

// TestProtoRouteHostsWire pins the new route snapshot fields byte for byte
// and checks the envelope rejects a truncated prefix and trailing garbage.
func TestProtoRouteHostsWire(t *testing.T) {
	hostBytes, err := proto.Marshal(routeHostToWire(protocol.RouteHost{Key: 2, Generation: 1, Label: "a", Kind: protocol.RouteKindRemote}))
	require.NoError(t, err)
	require.Equal(t, []byte{0x08, 0x02, 0x10, 0x01, 0x1A, 0x01, 'a', 0x20, 0x02}, hostBytes)
	seqBytes, err := proto.Marshal(&wire.RecentRouteEntry{AttentionSeq: 5})
	require.NoError(t, err)
	require.Equal(t, []byte{0x50, 0x05}, seqBytes)
	visitedBytes, err := proto.Marshal(&wire.RecentRouteEntry{Visited: true})
	require.NoError(t, err)
	require.Equal(t, []byte{0x58, 0x01}, visitedBytes)

	snapshot := routeHostsTestSnapshot()
	envelope, err := encodeProtoClient(snapshot)
	require.NoError(t, err)
	raw, err := proto.Marshal(envelope)
	require.NoError(t, err)

	tests := []struct {
		name    string
		raw     []byte
		wantErr bool
	}{
		{name: "exact", raw: raw},
		{name: "truncated prefix", raw: raw[:len(raw)-1], wantErr: true},
		{name: "trailing garbage", raw: append(append([]byte(nil), raw...), 0xFF), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := wire.ScanEnvelope(&wire.ClientEnvelope{}, tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			decoded := &wire.ClientEnvelope{}
			require.NoError(t, proto.Unmarshal(tt.raw, decoded))
			got, err := decodeProtoClient(decoded)
			require.NoError(t, err)
			require.Equal(t, snapshot, got)
		})
	}
}

func TestProtoRouteHostsRejectInvalid(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*wire.RecentRouteSnapshot)
	}{
		{name: "zero host identity", mutate: func(s *wire.RecentRouteSnapshot) { s.Hosts[0].Key = 0 }},
		{name: "empty host label", mutate: func(s *wire.RecentRouteSnapshot) { s.Hosts[0].Label = "" }},
		{name: "unknown host kind", mutate: func(s *wire.RecentRouteSnapshot) { s.Hosts[0].Kind = 9 }},
		{name: "host collides with entry", mutate: func(s *wire.RecentRouteSnapshot) { s.Hosts[0].Key = 1 }},
		{name: "duplicate host", mutate: func(s *wire.RecentRouteSnapshot) { s.Hosts[1].Key = s.Hosts[0].Key }},
		{name: "attention order without attention", mutate: func(s *wire.RecentRouteSnapshot) { s.Entries[0].Attention = false }},
		{name: "nil host", mutate: func(s *wire.RecentRouteSnapshot) { s.Hosts[0] = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message, err := recentRouteSnapshotToWire(routeHostsTestSnapshot())
			require.NoError(t, err)
			tt.mutate(message)
			_, err = recentRouteSnapshotFromWire(message)
			require.Error(t, err)
		})
	}
}
