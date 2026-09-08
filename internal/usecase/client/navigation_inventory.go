package client

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// inventoryPollInterval bounds control-source refresh while the palette is
// open. inventoryQueryTimeout bounds each complete dial/query; the exact
// connection closes on cancellation so blocked operations unwind.
const (
	inventoryPollInterval = time.Second
	inventoryQueryTimeout = 5 * time.Second
)

// inventoryRelayEnabled gates the concrete local source independently of
// server demand: only after this client has a committed explicitly local
// route and while its serving attachment is remote. Home labels and remote
// home routes are not proof. Local-only and direct remote-only perform zero
// control dials, including when a remote daemon sends an unsolicited demand.
func inventoryRelayEnabled(homeCommitted, servingRemote bool, dialer ports.ClientDialer) bool {
	return homeCommitted && servingRemote && dialer != nil
}

// inventoryEntryID is the opaque selection identity: source plus entry key.
type inventoryEntryID struct {
	source string
	entry  string
}

// inventoryRelay is the per-attachment navigation-inventory relay state. It
// owns no goroutine, cache, or persistent publication: the owning attempt
// drives polling through pollDue/beginPoll/endPoll and publishes through
// preparePublication. All work binds to attachment/transport identity plus
// the interaction generation; late results cannot affect a new attachment.
type inventoryRelay struct {
	clock  ports.Clock
	dialer ports.ClientDialer

	open        bool
	interaction uint64
	publication uint64
	lastPoll    time.Time
	// inFlight is the outstanding query identity, or zero when the slot
	// is free. Identities bind completions to their originating
	// interaction: a late result from a closed interaction drops instead
	// of polluting the new namespace or clearing its slot.
	inFlight uint64
	pollSeq  uint64

	published []protocol.NavigationInventorySourceGroup
	admitted  map[inventoryEntryID]uint64
	retired   map[inventoryEntryID]struct{}
}

func newInventoryRelay(clock ports.Clock, dialer ports.ClientDialer) *inventoryRelay {
	return &inventoryRelay{clock: clock, dialer: dialer, admitted: make(map[inventoryEntryID]uint64), retired: make(map[inventoryEntryID]struct{})}
}

// setOpen starts or stops polling for one palette interaction. Opening keeps
// a fresh interaction namespace: admitted keys and retirements never cross
// interactions, and reappearing targets earn new keys through new lifecycles.
func (r *inventoryRelay) setOpen(open bool, interaction uint64) {
	if r == nil {
		return
	}
	r.open = open
	if open && interaction != r.interaction {
		r.lastPoll = time.Time{}
		r.interaction = interaction
		r.publication = 0
		r.published = nil
		r.admitted = make(map[inventoryEntryID]uint64)
		r.retired = make(map[inventoryEntryID]struct{})
	}
}

// pollDue reports whether a refresh query may start: open, none in flight,
// and at least one interval since the last poll.
func (r *inventoryRelay) pollDue() bool {
	if r == nil || !r.open || r.inFlight != 0 || r.clock == nil {
		return false
	}
	return !r.clock.Now().Before(r.lastPoll.Add(inventoryPollInterval))
}

// beginPoll claims the single in-flight query slot, returning the query
// identity the completion must present to endPoll.
func (r *inventoryRelay) beginPoll() (uint64, bool) {
	if r == nil || !r.pollDue() {
		return 0, false
	}
	r.pollSeq++
	r.inFlight = r.pollSeq
	r.lastPoll = r.clock.Now()
	return r.inFlight, true
}

// endPoll releases the in-flight slot, but only for the query that owns
// it. Stale completions from a closed interaction drop without touching
// the new namespace.
func (r *inventoryRelay) endPoll(query uint64) {
	if r == nil || query == 0 || r.inFlight != query {
		return
	}
	r.inFlight = 0
}

// cancel stops polling and drops queued updates on close, reconnect,
// handoff, attachment replacement, or process cancellation.
func (r *inventoryRelay) cancel() {
	if r == nil {
		return
	}
	r.open = false
	r.inFlight = 0
}

// canonicalInventoryGroups sorts groups and entries by key so change
// comparison ignores transport order. Poll timestamps and request IDs never
// reach this form, so idle unchanged inventory compares equal.
func canonicalInventoryGroups(groups []protocol.NavigationInventorySourceGroup) []protocol.NavigationInventorySourceGroup {
	out := make([]protocol.NavigationInventorySourceGroup, 0, len(groups))
	for _, group := range groups {
		entries := make([]protocol.NavigationInventoryEntry, len(group.Entries))
		copy(entries, group.Entries)
		sort.Slice(entries, func(i, j int) bool { return entries[i].EntryKey < entries[j].EntryKey })
		out = append(out, protocol.NavigationInventorySourceGroup{SourceKey: group.SourceKey, Status: group.Status, Entries: entries})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SourceKey < out[j].SourceKey })
	return out
}

func inventoryGroupsEqual(a, b []protocol.NavigationInventorySourceGroup) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].SourceKey != b[i].SourceKey || a[i].Status != b[i].Status || len(a[i].Entries) != len(b[i].Entries) {
			return false
		}
		for j := range a[i].Entries {
			if a[i].Entries[j] != b[i].Entries[j] {
				return false
			}
		}
	}
	return true
}

// preparePublication admits a fresh snapshot and reports whether it carries
// a semantic change worth publishing remotely. Unchanged polls produce no
// new publication. Removed targets retire their keys for the interaction and
// never reuse them; admitted keys record the publication generation that
// first carried them so older displayed selections stay valid while current.
func (r *inventoryRelay) preparePublication(groups []protocol.NavigationInventorySourceGroup) ([]protocol.NavigationInventorySourceGroup, uint64, bool) {
	if r == nil {
		return nil, 0, false
	}
	canonical := canonicalInventoryGroups(groups)
	if inventoryGroupsEqual(canonical, r.published) {
		return nil, 0, false
	}
	r.publication++
	seen := make(map[inventoryEntryID]struct{}, len(canonical))
	for _, group := range canonical {
		for _, entry := range group.Entries {
			id := inventoryEntryID{source: entry.SourceKey, entry: entry.EntryKey}
			seen[id] = struct{}{}
			if _, ok := r.admitted[id]; !ok {
				r.admitted[id] = r.publication
			}
		}
	}
	for id := range r.admitted {
		if _, ok := seen[id]; !ok {
			if _, retired := r.retired[id]; !retired {
				r.retired[id] = struct{}{}
			}
		}
	}
	r.published = canonical
	return canonical, r.publication, true
}

// validateSelection admits a serving-daemon selection against published
// state. Generations are monotonic: future generations, keys not yet
// published at the selected generation, and retired or unknown keys reject.
// An unchanged key survives refresh lag: selection from an older displayed
// publication stays valid while the key is still current.
func (r *inventoryRelay) validateSelection(selection protocol.NavigationInventorySelection) error {
	if r == nil {
		return errors.New("vev: inventory relay unavailable")
	}
	if err := protocol.ValidateNavigationInventorySelection(selection); err != nil {
		return err
	}
	// Remote-source resolution needs source registration the relay never
	// carries; remote vision stays off until the E4 slice wires it. Local
	// sources resolve registration-free on the control connection.
	if selection.SourceKey != protocol.NavigationInventoryLocalSourceKey {
		return errors.New("vev: inventory selection for a remote source is unavailable")
	}
	if selection.InteractionGeneration != r.interaction {
		return errors.New("vev: inventory selection for a closed interaction")
	}
	if selection.PublicationGeneration > r.publication {
		return errors.New("vev: inventory selection from a future publication")
	}
	id := inventoryEntryID{source: selection.SourceKey, entry: selection.EntryKey}
	if _, retired := r.retired[id]; retired {
		return errors.New("vev: inventory selection for a retired entry")
	}
	admittedAt, ok := r.admitted[id]
	if !ok || admittedAt > selection.PublicationGeneration {
		return errors.New("vev: inventory selection for an unpublished entry")
	}
	return nil
}

var errInventorySourceUnavailable = errors.New("vev: local inventory source unavailable")

// watchQueryConn closes the exact connection when the attempt cancels or
// the clock-driven query budget expires. Established IPC connections do
// not retain the dial context, so without this a wedged source parks the
// worker past teardown. The returned stop func releases the watcher.
func (r *inventoryRelay) watchQueryConn(ctx context.Context, closeConn func()) func() {
	noop := func() {}
	if r == nil || closeConn == nil {
		return noop
	}
	done := make(chan struct{})
	var timer ports.Timer
	var timerC <-chan time.Time
	if r.clock != nil {
		timer = r.clock.NewTimer(inventoryQueryTimeout)
		timerC = timer.C()
	}
	go func() {
		select {
		case <-ctx.Done():
			closeConn()
		case <-timerC:
			closeConn()
		case <-done:
		}
	}()
	return func() {
		close(done)
		if timer != nil {
			timer.Stop()
		}
	}
}

// querySnapshot performs one bounded dial-only snapshot query without Hello,
// session creation, attachment, or daemon startup. A nil dialer or dial
// failure reports source unavailability; the serving attachment is
// unaffected and native commands remain usable.
func (r *inventoryRelay) querySnapshot(ctx context.Context, requestID uint64) (protocol.NavigationInventoryResponse, error) {
	if r == nil || r.dialer == nil {
		return protocol.NavigationInventoryResponse{}, errInventorySourceUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, inventoryQueryTimeout)
	defer cancel()
	conn, err := r.dialer.Dial(ctx)
	if err != nil {
		return protocol.NavigationInventoryResponse{}, errInventorySourceUnavailable
	}
	var closeOnce sync.Once
	closeConn := func() { closeOnce.Do(func() { _ = conn.Close() }) }
	defer closeConn()
	stopWatch := r.watchQueryConn(ctx, closeConn)
	defer stopWatch()
	request := protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: requestID, Operation: protocol.NavigationInventorySnapshot}
	if err := protocol.ValidateNavigationInventoryRequest(request); err != nil {
		return protocol.NavigationInventoryResponse{}, err
	}
	if err := conn.SendClient(request); err != nil {
		return protocol.NavigationInventoryResponse{}, err
	}
	message, err := conn.ReceiveServer()
	if err != nil {
		return protocol.NavigationInventoryResponse{}, err
	}
	response, ok := message.(protocol.NavigationInventoryResponse)
	if !ok {
		return protocol.NavigationInventoryResponse{}, errors.New("vev: unexpected inventory response type")
	}
	if err := protocol.ValidateNavigationInventoryResponse(response); err != nil {
		return protocol.NavigationInventoryResponse{}, err
	}
	if response.RequestID != requestID || response.Operation != protocol.NavigationInventorySnapshot {
		return protocol.NavigationInventoryResponse{}, errors.New("vev: inventory response identity mismatch")
	}
	return response, nil
}

// resolveEntry revalidates one selected key against the source and returns
// the non-mutating attach target. Registration fencing happens source-side;
// the relay only checks response identity before admitting any data.
func (r *inventoryRelay) resolveEntry(ctx context.Context, requestID uint64, source, entry string, registration domain.RemoteRegistration) (protocol.AttachTarget, error) {
	if r == nil || r.dialer == nil {
		return protocol.AttachTarget{}, errInventorySourceUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, inventoryQueryTimeout)
	defer cancel()
	conn, err := r.dialer.Dial(ctx)
	if err != nil {
		return protocol.AttachTarget{}, errInventorySourceUnavailable
	}
	var closeOnce sync.Once
	closeConn := func() { closeOnce.Do(func() { _ = conn.Close() }) }
	defer closeConn()
	stopWatch := r.watchQueryConn(ctx, closeConn)
	defer stopWatch()
	request := protocol.NavigationInventoryRequest{Version: protocol.Version, RequestID: requestID, Operation: protocol.NavigationInventoryResolve, SourceKey: source, EntryKey: entry, Registration: registration}
	if err := protocol.ValidateNavigationInventoryRequest(request); err != nil {
		return protocol.AttachTarget{}, err
	}
	if err := conn.SendClient(request); err != nil {
		return protocol.AttachTarget{}, err
	}
	message, err := conn.ReceiveServer()
	if err != nil {
		return protocol.AttachTarget{}, err
	}
	response, ok := message.(protocol.NavigationInventoryResponse)
	if !ok {
		return protocol.AttachTarget{}, errors.New("vev: unexpected inventory response type")
	}
	if err := protocol.ValidateNavigationInventoryResponse(response); err != nil {
		return protocol.AttachTarget{}, err
	}
	if response.RequestID != requestID || response.Operation != protocol.NavigationInventoryResolve || response.Status != protocol.NavigationInventoryOK || response.Resolved == nil {
		return protocol.AttachTarget{}, errors.New("vev: inventory resolve rejected")
	}
	return *response.Resolved, nil
}
