package brokeripc

import (
	"errors"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Snapshot publication (P3.3).
//
// A broker publication travels as the exact multipart transfer the P3.1
// assembler accepts: Begin(index 0, element counts), then every daemon part in
// daemon-index order immediately followed by that daemon's session parts in
// session-index order, then the tombstones, then End at the final index. The
// local daemon, when present, is daemon index 0 and its sessions carry
// Local=true. Daemon parts carry no inline sessions; sessions travel as their
// own parts. Both sides agree on one deterministic layout, so the receiving
// assembler commits only a transfer whose indexes, counts, and order are exact.
//
// The emitter refuses a transfer the peer's assembler would refuse: more parts
// than MaxSnapshotParts, or a total encoded payload above the staged ceiling.
// The staged ceiling check is deliberately conservative - the encoded total is
// an upper bound of the deterministic staged estimate - so a publication this
// side accepts is never one the peer silently aborts.

// snapshotParts splits one published snapshot into its ordered multipart
// transfer for one connection scope and subscription generation.
func snapshotParts(snapshot ports.BrokerSnapshot, epoch ports.BrokerEpoch, connection ports.BrokerConnectionID, generation brokerwire.SubscriptionGeneration) ([]brokerwire.SnapshotPart, error) {
	if epoch == 0 || connection.IsZero() {
		return nil, ErrScopeMismatch
	}
	if snapshot.Revision == 0 {
		return nil, errors.New("brokeripc: snapshot publication has no revision")
	}
	if len(snapshot.Daemons) > ports.BrokerMaxDaemonsPerSnapshot || len(snapshot.Removed) > ports.BrokerMaxTombstones {
		return nil, errors.New("brokeripc: snapshot publication exceeds daemon bound")
	}
	sessions := 0
	for _, daemon := range snapshot.Daemons {
		if len(daemon.Sessions) > ports.BrokerMaxSessionsPerHost {
			return nil, errors.New("brokeripc: snapshot publication exceeds session bound")
		}
		sessions += len(daemon.Sessions)
	}
	total := 1 + len(snapshot.Daemons) + sessions + len(snapshot.Removed) + 1
	if total > brokerwire.MaxSnapshotParts {
		return nil, errors.New("brokeripc: snapshot publication exceeds part bound")
	}
	localPresent := len(snapshot.Daemons) > 0 && snapshot.Daemons[0].Local

	parts := make([]brokerwire.SnapshotPart, 0, total)
	base := func(part brokerwire.SnapshotPartPayload) brokerwire.SnapshotPart {
		return brokerwire.SnapshotPart{
			Epoch: epoch, Connection: connection, Generation: generation,
			Revision: snapshot.Revision, Index: uint32(len(parts)), Part: part,
		}
	}
	parts = append(parts, base(brokerwire.SnapshotBegin{
		HostCount:      uint32(len(snapshot.Daemons)),
		SessionCount:   uint32(sessions),
		TombstoneCount: uint32(len(snapshot.Removed)),
		LocalPresent:   localPresent,
	}))
	for daemonIndex, daemon := range snapshot.Daemons {
		projection := daemon.Clone()
		sessionCount := len(projection.Sessions)
		projection.Sessions = nil
		parts = append(parts, base(brokerwire.SnapshotDaemonPart{
			HostIndex:    uint32(daemonIndex),
			Daemon:       projection,
			SessionCount: uint32(sessionCount),
		}))
		for sessionIndex, session := range daemon.Sessions {
			parts = append(parts, base(brokerwire.SnapshotSessionPart{
				HostIndex:    uint32(daemonIndex),
				SessionIndex: uint32(sessionIndex),
				Local:        daemon.Local,
				Session:      cloneCatalogSession(session),
			}))
		}
	}
	for tombstoneIndex, tombstone := range snapshot.Removed {
		parts = append(parts, base(brokerwire.SnapshotTombstonePart{
			TombstoneIndex:  uint32(tombstoneIndex),
			Registration:    tombstone.Registration,
			RetiredRevision: tombstone.RetiredRevision,
		}))
	}
	parts = append(parts, base(brokerwire.SnapshotEnd{}))
	return parts, nil
}

// cloneCatalogSession copies one session and guarantees its tabs slice is
// non-nil: the assembler refuses a session part whose tabs are absent, so an
// empty inventory publishes an empty list rather than a missing one.
func cloneCatalogSession(session catalogue.RemoteCatalogSession) catalogue.RemoteCatalogSession {
	out := session
	if session.Tabs == nil {
		out.Tabs = []catalogue.RemoteCatalogTab{}
		return out
	}
	tabs := make([]catalogue.RemoteCatalogTab, len(session.Tabs))
	copy(tabs, session.Tabs)
	out.Tabs = tabs
	return out
}

// snapshotTransferBytes sums the encoded size of one prepared transfer so the
// publisher can refuse a publication above the staged ceiling before the peer
// aborts it.
func snapshotTransferBytes(parts []brokerwire.SnapshotPart, maxEnvelopeBytes, maxChunkBytes uint64) (uint64, error) {
	var total uint64
	for _, part := range parts {
		payload, err := brokerwire.EncodeServer(part, maxEnvelopeBytes, maxChunkBytes)
		if err != nil {
			return 0, err
		}
		total += uint64(len(payload))
	}
	return total, nil
}
