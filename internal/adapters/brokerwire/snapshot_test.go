package brokerwire

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

func testSnapshotScope() (ports.BrokerEpoch, ports.BrokerConnectionID) {
	return 7, testConnectionID(0x21)
}

func testSnapshotAssembler(t *testing.T, opts ...SnapshotOption) *SnapshotAssembler {
	t.Helper()
	epoch, connection := testSnapshotScope()
	return NewSnapshotAssembler(epoch, connection, opts...)
}

// testHostAt builds one catalogue-valid host projection for host index i.
func testHostAt(i int) ports.RemoteHostSnapshot {
	endpoint := fmt.Sprintf("user%d@host%d:22", i, i)
	return ports.RemoteHostSnapshot{
		Endpoint:       endpoint,
		DisplayOrigin:  endpoint,
		Registration:   domain.RemoteRegistration{Endpoint: endpoint, Incarnation: [16]byte{0x10, byte(i + 1), 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xf0, 0x11}, Generation: domain.RemoteGeneration(i + 1)},
		Availability:   domain.RemoteAvailabilityReachable,
		LastSuccess:    time.Unix(1700000000+int64(i), 0).UTC(),
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{},
	}
}

// testSessionAt builds one catalogue-valid session for a host and index.
func testSessionAt(host, index int) catalogue.RemoteCatalogSession {
	return catalogue.RemoteCatalogSession{
		LifecycleID: domain.SessionLifecycleID{byte(host + 1), byte(index >> 8), byte(index), 0x77},
		Name:        fmt.Sprintf("s%02d-%03d", host, index),
		State:       catalogue.RemoteCatalogSessionDown,
		Tabs:        []catalogue.RemoteCatalogTab{},
	}
}

// testTombstoneAt builds one valid retired registration for index i.
func testTombstoneAt(i int) ports.BrokerHostTombstone {
	endpoint := fmt.Sprintf("retired%d@old%d:22", i, i)
	return ports.BrokerHostTombstone{
		Endpoint:        endpoint,
		Registration:    domain.RemoteRegistration{Endpoint: endpoint, Incarnation: [16]byte{0x90, byte(i + 1), 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e}, Generation: domain.RemoteGeneration(i + 1)},
		RetiredRevision: ports.BrokerRevision(i + 1),
	}
}

func sampleSnapshotHosts() []ports.RemoteHostSnapshot {
	host0 := testHostAt(0)
	host0.Sessions = []catalogue.RemoteCatalogSession{testSessionAt(0, 0), testSessionAt(0, 1)}
	host1 := testHostAt(1)
	host1.Sessions = []catalogue.RemoteCatalogSession{testSessionAt(1, 0)}
	return []ports.RemoteHostSnapshot{host0, host1}
}

func sampleSnapshotTombstones() []ports.BrokerHostTombstone {
	return []ports.BrokerHostTombstone{testTombstoneAt(0)}
}

func snapshotSessionTotal(hosts []ports.RemoteHostSnapshot) int {
	total := 0
	for _, host := range hosts {
		total += len(host.Sessions)
	}
	return total
}

// snapshotPartsFor lays out one exact transfer: Begin(0), hosts with their
// sessions, tombstones, End. Indexes are exact sequential positions.
func snapshotPartsFor(gen uint64, rev ports.BrokerRevision, hosts []ports.RemoteHostSnapshot, tombstones []ports.BrokerHostTombstone) []SnapshotPart {
	epoch, connection := testSnapshotScope()
	next := uint32(0)
	part := func(payload SnapshotPartPayload) SnapshotPart {
		out := SnapshotPart{Epoch: epoch, Connection: connection, Generation: gen, Revision: rev, Index: next, Part: payload}
		next++
		return out
	}
	parts := []SnapshotPart{part(SnapshotBegin{
		HostCount:      uint32(len(hosts)),
		SessionCount:   uint32(snapshotSessionTotal(hosts)),
		TombstoneCount: uint32(len(tombstones)),
	})}
	for hostIndex, host := range hosts {
		// Host parts carry no inline sessions: the advertised count is
		// followed by that many session parts.
		projection := host.Clone()
		projection.Sessions = []catalogue.RemoteCatalogSession{}
		parts = append(parts, part(SnapshotHostPart{HostIndex: uint32(hostIndex), Host: projection, SessionCount: uint32(len(host.Sessions))}))
		for sessionIndex, session := range host.Sessions {
			parts = append(parts, part(SnapshotSessionPart{HostIndex: uint32(hostIndex), SessionIndex: uint32(sessionIndex), Session: session}))
		}
	}
	for tombstoneIndex, tombstone := range tombstones {
		parts = append(parts, part(SnapshotTombstonePart{
			TombstoneIndex:  uint32(tombstoneIndex),
			Registration:    tombstone.Registration,
			RetiredRevision: tombstone.RetiredRevision,
		}))
	}
	return append(parts, part(SnapshotEnd{}))
}

func cloneSnapshotParts(parts []SnapshotPart) []SnapshotPart {
	return append([]SnapshotPart(nil), parts...)
}

// stagePart stages one part and requires acceptance.
func stagePart(t *testing.T, a *SnapshotAssembler, part SnapshotPart) {
	t.Helper()
	_, _, err := a.Add(part)
	require.NoError(t, err)
}

// runSnapshotParts stages parts in order and returns the first staging error.
func runSnapshotParts(a *SnapshotAssembler, parts []SnapshotPart) error {
	for _, part := range parts {
		if _, _, err := a.Add(part); err != nil {
			return err
		}
	}
	return nil
}

func TestSnapshotAssemblerBoundsConstants(t *testing.T) {
	require.Equal(t, 16514, MaxSnapshotParts)
	require.Equal(t, 1+64+64*256+64+1, MaxSnapshotParts)
	require.Equal(t, uint64(80<<20), MaxSnapshotStagedBytes)
}

// TestSnapshotAssemblerExactTransfer proves exact order, count, and index
// acceptance, and that committed order matches the staged order.
func TestSnapshotAssemblerExactTransfer(t *testing.T) {
	hosts := sampleSnapshotHosts()
	tombstones := sampleSnapshotTombstones()
	parts := snapshotPartsFor(5, 3, hosts, tombstones)
	require.Len(t, parts, 1+2+3+1+1)

	a := testSnapshotAssembler(t)
	for i, part := range parts {
		completed, snapshot, err := a.Add(part)
		require.NoError(t, err, "part %d", i)
		if i == len(parts)-1 {
			require.True(t, completed)
			require.Equal(t, ports.BrokerRevision(3), snapshot.Revision)
		} else {
			require.False(t, completed, "part %d", i)
			_, committed := a.Snapshot()
			require.False(t, committed, "part %d must not publish", i)
			require.True(t, a.StagingActive())
		}
	}
	committed, ok := a.Snapshot()
	require.True(t, ok)
	require.False(t, a.StagingActive())
	epoch, connection := testSnapshotScope()
	require.Equal(t, epoch, committed.Epoch)
	require.Equal(t, ports.BrokerRevision(3), committed.Revision)
	require.Len(t, committed.Hosts, 2)
	require.Equal(t, "user0@host0:22", committed.Hosts[0].Endpoint)
	require.Equal(t, "user1@host1:22", committed.Hosts[1].Endpoint)
	require.Equal(t, 2, len(committed.Hosts[0].Sessions))
	require.Equal(t, "s00-000", committed.Hosts[0].Sessions[0].Name)
	require.Equal(t, "s00-001", committed.Hosts[0].Sessions[1].Name)
	require.Equal(t, "s01-000", committed.Hosts[1].Sessions[0].Name)
	require.Len(t, committed.Removed, 1)
	require.Equal(t, "retired0@old0:22", committed.Removed[0].Endpoint)
	_ = connection
}

func TestSnapshotAssemblerEmptyTransfer(t *testing.T) {
	parts := snapshotPartsFor(1, 1, nil, nil)
	require.Len(t, parts, 2)
	a := testSnapshotAssembler(t)
	completed, _, err := a.Add(parts[0])
	require.NoError(t, err)
	require.False(t, completed)
	require.True(t, a.StagingActive())
	completed, snapshot, err := a.Add(parts[1])
	require.NoError(t, err)
	require.True(t, completed)
	require.Empty(t, snapshot.Hosts)
	require.Empty(t, snapshot.Removed)
}

// TestSnapshotAssemblerTransferRejections proves malformed indexes, counts,
// order, payloads, and assembled validation failures abort staging and
// retain the committed snapshot.
func TestSnapshotAssemblerTransferRejections(t *testing.T) {
	base := func() []SnapshotPart {
		return snapshotPartsFor(5, 3, sampleSnapshotHosts(), sampleSnapshotTombstones())
	}
	mutate := func(parts []SnapshotPart, index int, apply func(*SnapshotPart)) []SnapshotPart {
		out := cloneSnapshotParts(parts)
		apply(&out[index])
		return out
	}
	cases := []struct {
		name    string
		parts   []SnapshotPart
		wantErr error
	}{
		{"begin index not zero", mutate(base(), 0, func(p *SnapshotPart) { p.Index = 1 }), ErrSnapshotInvalid},
		{"non-begin first", base()[1:], ErrSnapshotInvalid},
		{"missing session part", append(cloneSnapshotParts(base()[:3]), base()[4:]...), ErrSnapshotInvalid},
		{"duplicate host index", mutate(base(), 4, func(p *SnapshotPart) {
			p.Part = SnapshotHostPart{HostIndex: 0, Host: testHostAt(1), SessionCount: 1}
		}), ErrSnapshotInvalid},
		{"host index skips ahead", mutate(base(), 4, func(p *SnapshotPart) {
			p.Part = SnapshotHostPart{HostIndex: 5, Host: testHostAt(1), SessionCount: 1}
		}), ErrSnapshotInvalid},
		{"session host index mismatch", mutate(base(), 3, func(p *SnapshotPart) {
			p.Part = SnapshotSessionPart{HostIndex: 1, SessionIndex: 1, Session: testSessionAt(0, 1)}
		}), ErrSnapshotInvalid},
		{"session index gap", mutate(base(), 3, func(p *SnapshotPart) {
			p.Part = SnapshotSessionPart{HostIndex: 0, SessionIndex: 2, Session: testSessionAt(0, 1)}
		}), ErrSnapshotInvalid},
		{"session tabs absent", mutate(base(), 2, func(p *SnapshotPart) {
			session := testSessionAt(0, 0)
			session.Tabs = nil
			p.Part = SnapshotSessionPart{HostIndex: 0, SessionIndex: 0, Session: session}
		}), ErrSnapshotInvalid},
		{"host carries inline sessions", mutate(base(), 1, func(p *SnapshotPart) {
			host := testHostAt(0)
			host.Sessions = []catalogue.RemoteCatalogSession{testSessionAt(0, 0)}
			p.Part = SnapshotHostPart{HostIndex: 0, Host: host, SessionCount: 1}
		}), ErrSnapshotInvalid},
		{"host session count over maximum", mutate(base(), 1, func(p *SnapshotPart) {
			p.Part = SnapshotHostPart{HostIndex: 0, Host: testHostAt(0), SessionCount: uint32(ports.BrokerMaxSessionsPerHost) + 1}
		}), ErrTooLarge},
		{"host session count exceeds advertised total", mutate(base(), 1, func(p *SnapshotPart) {
			p.Part = SnapshotHostPart{HostIndex: 0, Host: testHostAt(0), SessionCount: 3}
		}), ErrSnapshotInvalid},
		{"tombstone before all sessions", func() []SnapshotPart {
			parts := base()
			reordered := []SnapshotPart{parts[0], parts[1], parts[2], parts[3], parts[6], parts[4], parts[5], parts[7]}
			for i := range reordered {
				reordered[i].Index = uint32(i)
			}
			return reordered
		}(), ErrSnapshotInvalid},
		{"tombstone index out of order", mutate(base(), 6, func(p *SnapshotPart) {
			p.Part = SnapshotTombstonePart{TombstoneIndex: 1, Registration: testRegistration(), RetiredRevision: 1}
		}), ErrSnapshotInvalid},
		{"tombstone missing retired revision", mutate(base(), 6, func(p *SnapshotPart) {
			p.Part = SnapshotTombstonePart{TombstoneIndex: 0, Registration: testRegistration(), RetiredRevision: 0}
		}), ErrSnapshotInvalid},
		{"end before completion", append(cloneSnapshotParts(base()[:5]), SnapshotPart{
			Epoch: 7, Connection: testConnectionID(0x21), Generation: 5, Revision: 3, Index: 5, Part: SnapshotEnd{},
		}), ErrSnapshotInvalid},
		{"begin session count mismatch", mutate(base(), 0, func(p *SnapshotPart) {
			p.Part = SnapshotBegin{HostCount: 2, SessionCount: 4, TombstoneCount: 1}
		}), ErrSnapshotInvalid},
		{"begin host count over maximum", mutate(base(), 0, func(p *SnapshotPart) {
			p.Part = SnapshotBegin{HostCount: uint32(ports.BrokerMaxHosts) + 1}
		}), ErrTooLarge},
		{"begin session count over maximum", mutate(base(), 0, func(p *SnapshotPart) {
			p.Part = SnapshotBegin{HostCount: 1, SessionCount: uint32(ports.BrokerMaxHosts*ports.BrokerMaxSessionsPerHost) + 1}
		}), ErrTooLarge},
		{"begin tombstone count over maximum", mutate(base(), 0, func(p *SnapshotPart) {
			p.Part = SnapshotBegin{HostCount: 1, TombstoneCount: uint32(ports.BrokerMaxTombstones) + 1}
		}), ErrTooLarge},
		{"commit duplicate host endpoint", func() []SnapshotPart {
			hosts := sampleSnapshotHosts()
			hosts[1].Endpoint = hosts[0].Endpoint
			hosts[1].Registration.Endpoint = hosts[0].Endpoint
			return snapshotPartsFor(5, 3, hosts, nil)
		}(), ErrSnapshotInvalid},
		{"commit host endpoint registration mismatch", func() []SnapshotPart {
			hosts := sampleSnapshotHosts()
			hosts[0].Endpoint = "other@elsewhere:22"
			return snapshotPartsFor(5, 3, hosts, nil)
		}(), ErrSnapshotInvalid},
		{"commit invalid projection", func() []SnapshotPart {
			hosts := sampleSnapshotHosts()
			hosts[0].Availability = 0
			return snapshotPartsFor(5, 3, hosts, nil)
		}(), ErrSnapshotInvalid},
		{"commit duplicate session name", func() []SnapshotPart {
			hosts := sampleSnapshotHosts()
			hosts[0].Sessions[1].Name = hosts[0].Sessions[0].Name
			return snapshotPartsFor(5, 3, hosts, nil)
		}(), ErrSnapshotInvalid},
		{"commit tombstone for live host", func() []SnapshotPart {
			hosts := sampleSnapshotHosts()
			tombstone := testTombstoneAt(0)
			tombstone.Endpoint = hosts[0].Endpoint
			tombstone.Registration.Endpoint = hosts[0].Endpoint
			return snapshotPartsFor(5, 3, hosts, []ports.BrokerHostTombstone{tombstone})
		}(), ErrSnapshotInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := testSnapshotAssembler(t)
			err := runSnapshotParts(a, tc.parts)
			require.ErrorIs(t, err, tc.wantErr)
			require.False(t, a.StagingActive())
			_, committed := a.Snapshot()
			require.False(t, committed)
		})
	}
}

// TestSnapshotAssemblerLatePartAfterCommit proves a part that follows a
// committed transfer is refused as malformed while the committed snapshot
// survives.
func TestSnapshotAssemblerLatePartAfterCommit(t *testing.T) {
	a := testSnapshotAssembler(t)
	parts := snapshotPartsFor(5, 3, sampleSnapshotHosts(), sampleSnapshotTombstones())
	require.NoError(t, runSnapshotParts(a, parts))
	late := parts[len(parts)-1]
	late.Index = uint32(len(parts))
	_, _, err := a.Add(late)
	require.ErrorIs(t, err, ErrSnapshotInvalid)
	require.False(t, a.StagingActive())
	committed, ok := a.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(3), committed.Revision)
}

// TestSnapshotAssemblerBounds proves the 16,514-part and 80 MiB staged
// ceilings: maximum counts commit, one over is refused.
func TestSnapshotAssemblerBounds(t *testing.T) {
	t.Run("maximum counts commit", func(t *testing.T) {
		hosts := make([]ports.RemoteHostSnapshot, ports.BrokerMaxHosts)
		for i := range hosts {
			host := testHostAt(i)
			sessions := make([]catalogue.RemoteCatalogSession, ports.BrokerMaxSessionsPerHost)
			for j := range sessions {
				sessions[j] = testSessionAt(i, j)
			}
			host.Sessions = sessions
			hosts[i] = host
		}
		tombstones := make([]ports.BrokerHostTombstone, ports.BrokerMaxTombstones)
		for i := range tombstones {
			tombstones[i] = testTombstoneAt(i)
		}
		parts := snapshotPartsFor(9, 12, hosts, tombstones)
		require.Len(t, parts, MaxSnapshotParts)
		a := testSnapshotAssembler(t)
		completed := false
		var snapshot ports.BrokerSnapshot
		for _, part := range parts {
			published, committed, err := a.Add(part)
			require.NoError(t, err)
			if published {
				completed = true
				snapshot = committed
			}
		}
		require.True(t, completed)
		require.Len(t, snapshot.Hosts, ports.BrokerMaxHosts)
		require.Len(t, snapshot.Removed, ports.BrokerMaxTombstones)
		for _, host := range snapshot.Hosts {
			require.Len(t, host.Sessions, ports.BrokerMaxSessionsPerHost)
		}
		require.Equal(t, ports.BrokerRevision(12), snapshot.Revision)
	})

	t.Run("one part over maximum refused", func(t *testing.T) {
		a := testSnapshotAssembler(t)
		_, _, err := a.Add(SnapshotPart{
			Epoch: 7, Connection: testConnectionID(0x21), Generation: 1, Revision: 1, Index: 0,
			Part: SnapshotBegin{HostCount: 1, SessionCount: uint32(ports.BrokerMaxHosts*ports.BrokerMaxSessionsPerHost) + 1},
		})
		require.ErrorIs(t, err, ErrTooLarge)
		require.False(t, a.StagingActive())
	})

	t.Run("staged byte ceiling aborts and retains", func(t *testing.T) {
		a := testSnapshotAssembler(t, WithMaxStagedBytes(200))
		parts := snapshotPartsFor(5, 3, sampleSnapshotHosts(), nil)
		// Begin (32) + host part stay within the cap; the first session part
		// crosses it.
		require.NoError(t, runSnapshotParts(a, parts[:2]))
		require.True(t, a.StagingActive())
		_, _, err := a.Add(parts[2])
		require.ErrorIs(t, err, ErrTooLarge)
		require.False(t, a.StagingActive())
		_, committed := a.Snapshot()
		require.False(t, committed)
	})

	t.Run("zero staged cap falls back to default", func(t *testing.T) {
		a := testSnapshotAssembler(t, WithMaxStagedBytes(0))
		require.Equal(t, MaxSnapshotStagedBytes, a.maxStagedBytes)
	})
}

// TestSnapshotStagedEstimateTabCharge proves the staged-memory estimate
// charges at least 64 bytes of overhead per tab plus the tab-list slice
// header, so a minimal tab cannot hide behind tiny strings.
func TestSnapshotStagedEstimateTabCharge(t *testing.T) {
	base := testSessionAt(0, 0)
	base.Tabs = []catalogue.RemoteCatalogTab{}
	oneTab := base
	oneTab.Tabs = []catalogue.RemoteCatalogTab{{Name: "t"}}
	delta := estimateCatalogSessionBytes(oneTab) - estimateCatalogSessionBytes(base)
	require.GreaterOrEqual(t, delta, uint64(64), "each tab must charge at least 64 bytes")

	maxTabs := make([]catalogue.RemoteCatalogTab, catalogue.RemoteCatalogMaxTabsPerSess)
	for i := range maxTabs {
		maxTabs[i] = catalogue.RemoteCatalogTab{Index: uint16(i), Name: "t"}
	}
	hostile := base
	hostile.Tabs = maxTabs
	require.GreaterOrEqual(t, estimateCatalogSessionBytes(hostile), uint64(catalogue.RemoteCatalogMaxTabsPerSess*64))
	// The slice header is charged when the tab list is present.
	present := base
	present.Tabs = []catalogue.RemoteCatalogTab{}
	absent := base
	absent.Tabs = nil
	require.Greater(t, estimateCatalogSessionBytes(present), estimateCatalogSessionBytes(absent))
}

// TestSnapshotHostileMaxTabsBound proves the 80 MiB staged ceiling against
// hostile max tabs: the worst-case arithmetic crosses the ceiling, a max-tab
// session crosses a ceiling that fits exactly one session, and a ceiling
// that admits it still commits.
func TestSnapshotHostileMaxTabsBound(t *testing.T) {
	require.Equal(t, uint64(80<<20), MaxSnapshotStagedBytes)

	maxTabs := make([]catalogue.RemoteCatalogTab, catalogue.RemoteCatalogMaxTabsPerSess)
	for i := range maxTabs {
		maxTabs[i] = catalogue.RemoteCatalogTab{Index: uint16(i), Name: "t"}
	}
	session := testSessionAt(0, 0)
	session.Tabs = maxTabs
	perSession := estimateCatalogSessionBytes(session)
	worst := perSession * uint64(ports.BrokerMaxHosts*ports.BrokerMaxSessionsPerHost)
	require.Greater(t, worst, MaxSnapshotStagedBytes, "hostile max tabs must cross the 80 MiB ceiling")

	host := testHostAt(0)
	host.Sessions = []catalogue.RemoteCatalogSession{session}
	parts := snapshotPartsFor(9, 1, []ports.RemoteHostSnapshot{host}, nil)

	tight := testSnapshotAssembler(t, WithMaxStagedBytes(perSession))
	require.NoError(t, runSnapshotParts(tight, parts[:2]))
	require.True(t, tight.StagingActive())
	_, _, err := tight.Add(parts[2])
	require.ErrorIs(t, err, ErrTooLarge)
	require.False(t, tight.StagingActive())

	generous := testSnapshotAssembler(t, WithMaxStagedBytes(perSession*4))
	require.NoError(t, runSnapshotParts(generous, parts))
	committed, ok := generous.Snapshot()
	require.True(t, ok)
	require.Len(t, committed.Hosts, 1)
	require.Len(t, committed.Hosts[0].Sessions, 1)
	require.Len(t, committed.Hosts[0].Sessions[0].Tabs, catalogue.RemoteCatalogMaxTabsPerSess)
}

// TestSnapshotAssemblerAtomicCommit proves publication happens only at End
// and that the returned snapshot is a defensive clone.
func TestSnapshotAssemblerAtomicCommit(t *testing.T) {
	a := testSnapshotAssembler(t)
	first := snapshotPartsFor(5, 1, sampleSnapshotHosts(), sampleSnapshotTombstones())
	require.NoError(t, runSnapshotParts(a, first))
	committed, ok := a.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(1), committed.Revision)

	second := snapshotPartsFor(5, 2, sampleSnapshotHosts(), nil)
	for _, part := range second[:len(second)-1] {
		completed, _, err := a.Add(part)
		require.NoError(t, err)
		require.False(t, completed)
	}
	// Staging a newer revision does not disturb the committed snapshot.
	committed, ok = a.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(1), committed.Revision)
	require.Len(t, committed.Removed, 1)

	completed, published, err := a.Add(second[len(second)-1])
	require.NoError(t, err)
	require.True(t, completed)
	require.Equal(t, ports.BrokerRevision(2), published.Revision)
	require.Empty(t, published.Removed)

	// The committed snapshot is immutable to the caller: mutating a returned
	// value never reaches the assembler.
	published.Hosts[0].Endpoint = "mutated@nowhere:22"
	published.Hosts[0].Sessions[0].Tabs = []catalogue.RemoteCatalogTab{{ID: "injected", Index: 0, Name: "injected"}}
	published.Hosts[0].Sessions[0].Name = "mutated"
	again, ok := a.Snapshot()
	require.True(t, ok)
	require.Equal(t, "user0@host0:22", again.Hosts[0].Endpoint)
	require.Equal(t, "s00-000", again.Hosts[0].Sessions[0].Name)
	require.Empty(t, again.Hosts[0].Sessions[0].Tabs)

	// An aborted transfer retains the committed snapshot.
	require.NoError(t, runSnapshotParts(a, snapshotPartsFor(5, 3, sampleSnapshotHosts(), nil)[:2]))
	_, _, err = a.Add(snapshotPartsFor(5, 3, sampleSnapshotHosts(), nil)[4])
	require.ErrorIs(t, err, ErrSnapshotInvalid)
	again, ok = a.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(2), again.Revision)
}

// TestSnapshotAssemblerInterruptedRetention proves an interrupted transfer is
// discarded while the committed snapshot survives, and that a restarted
// transfer starts from index 0.
func TestSnapshotAssemblerInterruptedRetention(t *testing.T) {
	a := testSnapshotAssembler(t)
	parts := snapshotPartsFor(5, 1, sampleSnapshotHosts(), sampleSnapshotTombstones())
	require.NoError(t, runSnapshotParts(a, parts))
	committed, ok := a.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(1), committed.Revision)

	restart := snapshotPartsFor(5, 2, sampleSnapshotHosts(), sampleSnapshotTombstones())
	require.NoError(t, runSnapshotParts(a, restart[:3]))
	require.True(t, a.StagingActive())
	a.DiscardStaging()
	require.False(t, a.StagingActive())
	committed, ok = a.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(1), committed.Revision)

	// A restarted transfer must begin again at index 0, and re-sent parts are
	// accepted from the start.
	stagePart(t, a, restart[0])
	require.True(t, a.StagingActive())
	_, _, err := a.Add(restart[2])
	require.ErrorIs(t, err, ErrSnapshotInvalid)
	require.False(t, a.StagingActive())

	stagePart(t, a, restart[0])
	for _, part := range restart[1:] {
		_, _, err := a.Add(part)
		require.NoError(t, err)
	}
	committed, ok = a.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(2), committed.Revision)
}

// TestSnapshotAssemblerRevisionFencing proves older revisions are refused,
// equal revisions are allowed, and a newer revision coalesces by replacing
// staging.
func TestSnapshotAssemblerRevisionFencing(t *testing.T) {
	a := testSnapshotAssembler(t)
	hosts := sampleSnapshotHosts()
	require.NoError(t, runSnapshotParts(a, snapshotPartsFor(5, 5, hosts, nil)))

	t.Run("older revision refused", func(t *testing.T) {
		_, _, err := a.Add(snapshotPartsFor(5, 4, hosts, nil)[0])
		require.ErrorIs(t, err, ErrSnapshotStale)
		require.False(t, a.StagingActive())
		committed, ok := a.Snapshot()
		require.True(t, ok)
		require.Equal(t, ports.BrokerRevision(5), committed.Revision)
	})

	t.Run("equal revision republishes", func(t *testing.T) {
		require.NoError(t, runSnapshotParts(a, snapshotPartsFor(5, 5, hosts, sampleSnapshotTombstones())))
		committed, ok := a.Snapshot()
		require.True(t, ok)
		require.Equal(t, ports.BrokerRevision(5), committed.Revision)
		require.Len(t, committed.Removed, 1)
	})

	t.Run("newer revision coalesces", func(t *testing.T) {
		newer := snapshotPartsFor(5, 6, hosts, nil)
		stagePart(t, a, newer[0])
		require.True(t, a.StagingActive())
		// A part of the superseded staging revision is refused and staging is
		// retained intact.
		_, _, err := a.Add(snapshotPartsFor(5, 5, hosts, nil)[1])
		require.ErrorIs(t, err, ErrSnapshotStale)
		require.True(t, a.StagingActive())
		// The fresh transfer restarts at index 1.
		for _, part := range newer[1:] {
			_, _, err := a.Add(part)
			require.NoError(t, err)
		}
		committed, ok := a.Snapshot()
		require.True(t, ok)
		require.Equal(t, ports.BrokerRevision(6), committed.Revision)
	})

	t.Run("revision below committed refused while staging", func(t *testing.T) {
		stagePart(t, a, snapshotPartsFor(5, 6, hosts, nil)[0])
		require.True(t, a.StagingActive())
		_, _, err := a.Add(snapshotPartsFor(5, 5, hosts, nil)[0])
		require.ErrorIs(t, err, ErrSnapshotStale)
		require.True(t, a.StagingActive())
	})
}

// TestSnapshotAssemblerCoalescing proves a Begin of the same generation
// replaces staging: equal revisions restart progress, newer revisions
// coalesce.
func TestSnapshotAssemblerCoalescing(t *testing.T) {
	hosts := sampleSnapshotHosts()
	base := snapshotPartsFor(5, 3, hosts, nil)

	a := testSnapshotAssembler(t)
	stagePart(t, a, base[0])
	stagePart(t, a, base[1])
	require.True(t, a.StagingActive())
	// Same revision restarts the transfer: progress is discarded.
	stagePart(t, a, base[0])
	require.True(t, a.StagingActive())
	for _, part := range base[1:] {
		_, _, err := a.Add(part)
		require.NoError(t, err)
	}
	committed, ok := a.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(3), committed.Revision)

	// A newer revision coalesces and also resets progress.
	newer := snapshotPartsFor(5, 4, hosts, nil)
	require.NoError(t, runSnapshotParts(a, newer[:3]))
	require.True(t, a.StagingActive())
	stagePart(t, a, newer[0])
	require.True(t, a.StagingActive())
	for _, part := range newer[1:] {
		_, _, err := a.Add(part)
		require.NoError(t, err)
	}
	committed, ok = a.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(4), committed.Revision)
}

// TestSnapshotAssemblerGenerationFencing proves the assembler adopts exactly
// one generation, ignores stale parts, rejects future parts, and refuses a
// zero generation.
func TestSnapshotAssemblerGenerationFencing(t *testing.T) {
	hosts := sampleSnapshotHosts()
	base := snapshotPartsFor(5, 3, hosts, nil)

	t.Run("zero generation refused", func(t *testing.T) {
		a := testSnapshotAssembler(t)
		part := base[0]
		part.Generation = 0
		_, _, err := a.Add(part)
		require.ErrorIs(t, err, ErrInvalidGeneration)
		require.False(t, a.StagingActive())
	})

	t.Run("stale ignored and future rejected while staging", func(t *testing.T) {
		a := testSnapshotAssembler(t)
		stagePart(t, a, base[0])
		stale := base[1]
		stale.Generation = 4
		_, _, err := a.Add(stale)
		require.ErrorIs(t, err, ErrStaleGeneration)
		require.True(t, a.StagingActive())
		future := base[1]
		future.Generation = 6
		_, _, err = a.Add(future)
		require.ErrorIs(t, err, ErrFutureGeneration)
		require.True(t, a.StagingActive())
		for _, part := range base[1:] {
			_, _, err := a.Add(part)
			require.NoError(t, err)
		}
		committed, ok := a.Snapshot()
		require.True(t, ok)
		require.Equal(t, ports.BrokerRevision(3), committed.Revision)
	})

	t.Run("generation is adopted for the assembler lifetime", func(t *testing.T) {
		a := testSnapshotAssembler(t)
		require.NoError(t, runSnapshotParts(a, base))
		stale := base[1]
		stale.Generation = 4
		_, _, err := a.Add(stale)
		require.ErrorIs(t, err, ErrStaleGeneration)
		future := base[0]
		future.Generation = 6
		_, _, err = a.Add(future)
		require.ErrorIs(t, err, ErrFutureGeneration)
		committed, ok := a.Snapshot()
		require.True(t, ok)
		require.Equal(t, ports.BrokerRevision(3), committed.Revision)
		// The same generation may republish a newer revision.
		require.NoError(t, runSnapshotParts(a, snapshotPartsFor(5, 4, hosts, nil)))
		committed, ok = a.Snapshot()
		require.True(t, ok)
		require.Equal(t, ports.BrokerRevision(4), committed.Revision)
	})
}

// TestSnapshotAssemblerScopeFencing proves every part must carry the exact
// assigned epoch and connection, whether or not a transfer is in flight.
func TestSnapshotAssemblerScopeFencing(t *testing.T) {
	base := snapshotPartsFor(5, 3, sampleSnapshotHosts(), nil)
	cases := []struct {
		name  string
		apply func(*SnapshotPart)
	}{
		{"epoch", func(p *SnapshotPart) { p.Epoch = 8 }},
		{"epoch zero", func(p *SnapshotPart) { p.Epoch = 0 }},
		{"connection", func(p *SnapshotPart) { p.Connection = testConnectionID(0x99) }},
		{"connection zero", func(p *SnapshotPart) { p.Connection = ports.BrokerConnectionID{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := testSnapshotAssembler(t)
			part := base[0]
			tc.apply(&part)
			_, _, err := a.Add(part)
			require.ErrorIs(t, err, ErrScopeMismatch)
			require.False(t, a.StagingActive())

			// With staging in flight the mismatched part is still refused and
			// staging is retained.
			stagePart(t, a, base[0])
			part = base[1]
			tc.apply(&part)
			_, _, err = a.Add(part)
			require.ErrorIs(t, err, ErrScopeMismatch)
			require.True(t, a.StagingActive())
		})
	}
}

// TestSnapshotAssemblerZeroRevision proves a zero revision is refused.
func TestSnapshotAssemblerZeroRevision(t *testing.T) {
	a := testSnapshotAssembler(t)
	part := snapshotPartsFor(5, 3, sampleSnapshotHosts(), nil)[0]
	part.Revision = 0
	_, _, err := a.Add(part)
	require.ErrorIs(t, err, ErrSnapshotInvalid)
	require.False(t, a.StagingActive())
}

// TestSnapshotAssemblerNilAndEmptySafety proves the nil-receiver and
// no-transfer paths.
func TestSnapshotAssemblerNilAndEmptySafety(t *testing.T) {
	var a *SnapshotAssembler
	_, ok := a.Snapshot()
	require.False(t, ok)
	require.False(t, a.StagingActive())
	a.DiscardStaging()
	_, _, err := a.Add(snapshotPartsFor(5, 3, nil, nil)[0])
	require.ErrorIs(t, err, ErrSnapshotInvalid)

	fresh := testSnapshotAssembler(t)
	_, ok = fresh.Snapshot()
	require.False(t, ok)
	extra := SnapshotPart{Epoch: 7, Connection: testConnectionID(0x21), Generation: 5, Revision: 3, Index: 1, Part: SnapshotEnd{}}
	_, _, err = fresh.Add(extra)
	require.ErrorIs(t, err, ErrSnapshotInvalid)
	_, _, err = fresh.Add(SnapshotPart{Epoch: 7, Connection: testConnectionID(0x21), Generation: 5, Revision: 3, Index: 0, Part: nil})
	require.ErrorIs(t, err, ErrSnapshotInvalid)
}

// TestSnapshotAssemblerPinnedGeneration proves WithGeneration fences every
// part from the first one.
func TestSnapshotAssemblerPinnedGeneration(t *testing.T) {
	a := testSnapshotAssembler(t, WithGeneration(5))
	stale := snapshotPartsFor(4, 1, sampleSnapshotHosts(), nil)[0]
	_, _, err := a.Add(stale)
	require.ErrorIs(t, err, ErrStaleGeneration)
	future := snapshotPartsFor(6, 1, sampleSnapshotHosts(), nil)[0]
	_, _, err = a.Add(future)
	require.ErrorIs(t, err, ErrFutureGeneration)
	require.NoError(t, runSnapshotParts(a, snapshotPartsFor(5, 1, sampleSnapshotHosts(), nil)))
	committed, ok := a.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(1), committed.Revision)
}

// TestSnapshotAssemblerAdoptCommitted proves a committed snapshot can be
// handed to a fresh assembler of the same scope, so a generation advance
// keeps publishing it until a newer transfer commits.
func TestSnapshotAssemblerAdoptCommitted(t *testing.T) {
	source := testSnapshotAssembler(t)
	require.NoError(t, runSnapshotParts(source, snapshotPartsFor(5, 3, sampleSnapshotHosts(), sampleSnapshotTombstones())))
	committed, ok := source.Snapshot()
	require.True(t, ok)

	target := testSnapshotAssembler(t, WithGeneration(6))
	require.False(t, target.StagingActive())
	_, ok = target.Snapshot()
	require.False(t, ok)
	target.adoptCommitted(committed)
	adopted, ok := target.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(3), adopted.Revision)
	require.Len(t, adopted.Removed, 1)

	// An older revision and a foreign epoch are ignored.
	target.adoptCommitted(ports.BrokerSnapshot{Epoch: committed.Epoch, Revision: 2, Hosts: committed.Hosts})
	target.adoptCommitted(ports.BrokerSnapshot{Epoch: committed.Epoch + 1, Revision: 9, Hosts: committed.Hosts})
	adopted, ok = target.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(3), adopted.Revision)

	// A newer transfer commits over the adopted snapshot.
	require.NoError(t, runSnapshotParts(target, snapshotPartsFor(6, 4, sampleSnapshotHosts(), nil)))
	adopted, ok = target.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(4), adopted.Revision)
	require.Empty(t, adopted.Removed)

	// The adopted value is independent of the source.
	adopted.Hosts[0].Endpoint = "mutated@nowhere:22"
	again, ok := source.Snapshot()
	require.True(t, ok)
	require.Equal(t, "user0@host0:22", again.Hosts[0].Endpoint)
}

// TestSnapshotAssemblerConcurrentReaders is a deterministic race test: one
// writer publishes sequential revisions while readers observe the committed
// snapshot and staging state.
func TestSnapshotAssemblerConcurrentReaders(t *testing.T) {
	a := testSnapshotAssembler(t)
	const revisions = 40
	const readers = 4
	var wg sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if snapshot, ok := a.Snapshot(); ok {
					assert.NotZero(t, snapshot.Revision)
					assert.NotZero(t, snapshot.Epoch)
				}
				_ = a.StagingActive()
			}
		}()
	}
	for revision := 1; revision <= revisions; revision++ {
		parts := snapshotPartsFor(5, ports.BrokerRevision(revision), sampleSnapshotHosts(), nil)
		for _, part := range parts {
			_, _, err := a.Add(part)
			require.NoError(t, err)
		}
	}
	close(done)
	wg.Wait()
	snapshot, ok := a.Snapshot()
	require.True(t, ok)
	require.Equal(t, ports.BrokerRevision(revisions), snapshot.Revision)
	require.False(t, a.StagingActive())
}

// assemblerFrames concatenates 4-byte big-endian length-prefixed frames, so one
// fuzz input can drive a whole part sequence.
func assemblerFrames(frames ...[]byte) []byte {
	out := make([]byte, 0)
	for _, frame := range frames {
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(frame)))
		out = append(out, header[:]...)
		out = append(out, frame...)
	}
	return out
}

// fuzzSnapshotAssemblerSeeds is the starting corpus: one complete valid
// transfer, a valid staged prefix, empty input, and corrupt variants (a flipped
// byte in one frame and a frame whose length prefix overruns the payload).
func fuzzSnapshotAssemblerSeeds(tb testing.TB) [][]byte {
	tb.Helper()
	encode := func(part SnapshotPart) []byte {
		raw, err := EncodeServer(part, testEnvelopeCeiling, testChunkCeiling)
		require.NoError(tb, err)
		return raw
	}
	parts := snapshotPartsFor(4, 2, sampleSnapshotHosts(), sampleSnapshotTombstones())
	frames := make([][]byte, 0, len(parts))
	for _, part := range parts {
		frames = append(frames, encode(part))
	}
	corrupt := append([]byte(nil), frames[1]...)
	corrupt[len(corrupt)-1] ^= 0xFF
	truncated := append([]byte(nil), frames[2][:len(frames[2])-1]...)
	return [][]byte{
		assemblerFrames(frames...),
		assemblerFrames(frames[:len(frames)-1]...),
		assemblerFrames(frames[0], corrupt),
		assemblerFrames(frames[0], truncated),
		nil,
	}
}

// FuzzSnapshotAssembler proves arbitrary part sequences never panic the
// assembler, never publish an invalid snapshot, and never retain an invalid
// committed snapshot. Input is a sequence of 4-byte big-endian length-prefixed
// frames, each a serialized SnapshotPart envelope; frames that fail strict
// scanning or semantic conversion are dropped, and every accepted part is
// staged in order.
func FuzzSnapshotAssembler(f *testing.F) {
	for _, seed := range fuzzSnapshotAssemblerSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		assembler := testSnapshotAssembler(t, WithMaxStagedBytes(1<<20))
		for len(payload) >= 4 {
			size := int(binary.BigEndian.Uint32(payload[:4]))
			payload = payload[4:]
			if size <= 0 || size > len(payload) {
				return
			}
			frame := payload[:size]
			payload = payload[size:]
			message, err := DecodeServer(frame, testEnvelopeCeiling, testChunkCeiling)
			if err != nil {
				continue
			}
			part, ok := message.(SnapshotPart)
			if !ok {
				continue
			}
			completed, snapshot, err := assembler.Add(part)
			if err != nil {
				continue
			}
			if completed {
				requireValidBrokerSnapshot(t, snapshot)
			}
		}
		if snapshot, ok := assembler.Snapshot(); ok {
			requireValidBrokerSnapshot(t, snapshot)
		}
	})
}

// requireValidBrokerSnapshot asserts one published snapshot is exactly what the
// assembler promises: a validated snapshot with durable host projections.
func requireValidBrokerSnapshot(t *testing.T, snapshot ports.BrokerSnapshot) {
	t.Helper()
	require.NoError(t, snapshot.Validate())
	for _, host := range snapshot.Hosts {
		require.NoError(t, ports.ValidateDurableHostProjection(host))
	}
}
