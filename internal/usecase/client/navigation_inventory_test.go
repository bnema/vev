package client

import (
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// manualClock is a fake ports.Clock with explicit time control: no sleeps.
type manualClock struct {
	now time.Time
}

func (c *manualClock) Now() time.Time { return c.now }

func (c *manualClock) NewTimer(d time.Duration) ports.Timer { return manualTimer{at: c.now.Add(d)} }

type manualTimer struct{ at time.Time }

func (t manualTimer) C() <-chan time.Time { return nil }

func (t manualTimer) Reset(time.Duration) bool { return false }

func (t manualTimer) Stop() bool { return false }

// inventoryFakeConnection scripts one control query: records sends, replays
// queued server messages, counts closes.
type inventoryFakeConnection struct {
	sent    []protocol.ClientMessage
	replies []protocol.ServerMessage
	recvErr error
	closes  int
}

func (c *inventoryFakeConnection) SendClient(message protocol.ClientMessage) error {
	c.sent = append(c.sent, message)
	return nil
}

func (c *inventoryFakeConnection) ReceiveServer() (protocol.ServerMessage, error) {
	if c.recvErr != nil {
		return nil, c.recvErr
	}
	if len(c.replies) == 0 {
		return nil, context.DeadlineExceeded
	}
	message := c.replies[0]
	c.replies = c.replies[1:]
	return message, nil
}

func (c *inventoryFakeConnection) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{}
}

func (c *inventoryFakeConnection) LinkState() ports.LinkState { return ports.LinkStateConnected }

func (c *inventoryFakeConnection) LinkEvents() <-chan ports.LinkEvent { return nil }

func (c *inventoryFakeConnection) Close() error {
	c.closes++
	return nil
}

// inventoryFakeDialer counts dials and hands out one scripted connection.
// Zero dials is asserted directly for local-only and remote-only modes.
type inventoryFakeDialer struct {
	dials   int
	conn    *inventoryFakeConnection
	dialErr error
}

func (d *inventoryFakeDialer) Dial(context.Context) (ports.ClientConnection, error) {
	d.dials++
	if d.dialErr != nil {
		return nil, d.dialErr
	}
	return d.conn, nil
}

func inventoryTestGroups() []protocol.NavigationInventorySourceGroup {
	return []protocol.NavigationInventorySourceGroup{{
		SourceKey: protocol.NavigationInventoryLocalSourceKey,
		Status:    protocol.NavigationInventorySourceOK,
		Entries: []protocol.NavigationInventoryEntry{
			{SourceKey: protocol.NavigationInventoryLocalSourceKey, EntryKey: "aaa/one", Name: "one"},
			{SourceKey: protocol.NavigationInventoryLocalSourceKey, EntryKey: "bbb/two", Name: "two"},
		},
	}}
}

func inventoryTestResponse(requestID uint64) protocol.NavigationInventoryResponse {
	return protocol.NavigationInventoryResponse{
		RequestID: requestID, Operation: protocol.NavigationInventorySnapshot,
		Status: protocol.NavigationInventoryOK, Groups: inventoryTestGroups(),
	}
}

func TestInventoryRelayEnabledModes(t *testing.T) {
	dialer := &inventoryFakeDialer{conn: &inventoryFakeConnection{}}
	tests := []struct {
		name          string
		homeCommitted bool
		servingRemote bool
		dialer        ports.ClientDialer
		want          bool
	}{
		{name: "hybrid remote serving", homeCommitted: true, servingRemote: true, dialer: dialer, want: true},
		{name: "local only never dials", homeCommitted: true, servingRemote: false, dialer: dialer},
		{name: "direct remote only never dials", servingRemote: true, dialer: dialer},
		{name: "no dialer never dials", homeCommitted: true, servingRemote: true},
		{name: "unsolicited demand without source never dials", servingRemote: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := inventoryRelayEnabled(tt.homeCommitted, tt.servingRemote, tt.dialer); got != tt.want {
				t.Fatalf("inventoryRelayEnabled() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestInventoryRelayZeroDialsWithoutSource(t *testing.T) {
	for _, dialer := range []ports.ClientDialer{nil, &inventoryFakeDialer{conn: &inventoryFakeConnection{}}} {
		relay := newInventoryRelay(&manualClock{now: time.Unix(1_000, 0)}, nil)
		if _, err := relay.querySnapshot(context.Background(), 1); err == nil {
			t.Fatal("nil-dialer query must report source unavailability")
		}
		_ = dialer
	}
	relay := newInventoryRelay(&manualClock{now: time.Unix(1_000, 0)}, nil)
	relay.setOpen(true, 1)
	query, ok := relay.beginPoll()
	if !ok || query == 0 {
		t.Fatal("poll slot should open without a source")
	}
	relay.endPoll(query)
}

func TestInventoryRelaySnapshotAdmitsAndSuppressesUnchanged(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_000, 0)}
	conn := &inventoryFakeConnection{replies: []protocol.ServerMessage{inventoryTestResponse(7), inventoryTestResponse(8)}}
	dialer := &inventoryFakeDialer{conn: conn}
	relay := newInventoryRelay(clock, dialer)
	relay.setOpen(true, 3)
	first, ok := relay.beginPoll()
	if !ok || first == 0 {
		t.Fatal("first poll should start")
	}
	response, err := relay.querySnapshot(context.Background(), 7)
	if err != nil {
		t.Fatalf("querySnapshot() error = %v", err)
	}
	relay.endPoll(first)
	if dialer.dials != 1 || conn.closes != 1 {
		t.Fatalf("dials = %d, closes = %d; want exactly one bounded query", dialer.dials, conn.closes)
	}
	groups, generation, changed := relay.preparePublication(response.Groups)
	if !changed || generation != 1 || len(groups) != 1 {
		t.Fatalf("first snapshot must publish once, got changed=%t generation=%d", changed, generation)
	}

	clock.now = clock.now.Add(2 * time.Second)
	second, ok := relay.beginPoll()
	if !ok || second == 0 || second == first {
		t.Fatal("second poll should start after the interval with a new identity")
	}
	response, err = relay.querySnapshot(context.Background(), 8)
	if err != nil {
		t.Fatalf("second querySnapshot() error = %v", err)
	}
	relay.endPoll(second)
	if _, _, changed := relay.preparePublication(response.Groups); changed {
		t.Fatal("unchanged inventory must not republish remotely")
	}
}

func TestInventoryRelaySelectionSurvivesRefreshLag(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_000, 0)}
	groups := inventoryTestGroups()
	relay := newInventoryRelay(clock, &inventoryFakeDialer{})
	relay.setOpen(true, 5)
	if _, _, changed := relay.preparePublication(groups); !changed {
		t.Fatal("initial publication must report changed")
	}
	// Two further unchanged refreshes: no new publication, same keys.
	clock.now = clock.now.Add(3 * time.Second)
	if _, _, changed := relay.preparePublication(groups); changed {
		t.Fatal("unchanged refresh must stay silent")
	}
	clock.now = clock.now.Add(3 * time.Second)
	if _, _, changed := relay.preparePublication(groups); changed {
		t.Fatal("unchanged refresh must stay silent")
	}
	// Selection from the older displayed publication stays valid: the key is
	// still current and was already published at that generation.
	selection := protocol.NavigationInventorySelection{CauseActionID: 9, InteractionGeneration: 5, PublicationGeneration: 1, SourceKey: protocol.NavigationInventoryLocalSourceKey, EntryKey: "aaa/one"}
	if err := relay.validateSelection(selection); err != nil {
		t.Fatalf("lagged selection for an unchanged key must stay valid: %v", err)
	}
}

func TestInventoryRelaySelectionRejects(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_000, 0)}
	relay := newInventoryRelay(clock, &inventoryFakeDialer{})
	relay.setOpen(true, 5)
	groups, generation, changed := relay.preparePublication(inventoryTestGroups())
	if !changed || generation != 1 {
		t.Fatal("initial publication must report changed")
	}
	_ = groups
	valid := protocol.NavigationInventorySelection{CauseActionID: 9, InteractionGeneration: 5, PublicationGeneration: 1, SourceKey: protocol.NavigationInventoryLocalSourceKey, EntryKey: "aaa/one"}
	if err := relay.validateSelection(valid); err != nil {
		t.Fatalf("current selection must validate: %v", err)
	}
	tests := []struct {
		name      string
		mutate    func(*protocol.NavigationInventorySelection)
		wantError bool
	}{
		{name: "future generation", mutate: func(s *protocol.NavigationInventorySelection) { s.PublicationGeneration = 2 }, wantError: true},
		{name: "unknown key", mutate: func(s *protocol.NavigationInventorySelection) { s.EntryKey = "zzz/nope" }, wantError: true},
		{name: "closed interaction", mutate: func(s *protocol.NavigationInventorySelection) { s.InteractionGeneration = 4 }, wantError: true},
		{name: "zero publication", mutate: func(s *protocol.NavigationInventorySelection) { s.PublicationGeneration = 0 }, wantError: true},
		{name: "zero cause stays valid for keyboard input", mutate: func(s *protocol.NavigationInventorySelection) { s.CauseActionID = 0 }, wantError: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selection := valid
			tt.mutate(&selection)
			if err := relay.validateSelection(selection); (err != nil) != tt.wantError {
				t.Fatalf("validateSelection() = %v, wantError %t", err, tt.wantError)
			}
		})
	}
	// Retire the key by removing it, then check rejection.
	thinned := []protocol.NavigationInventorySourceGroup{{
		SourceKey: protocol.NavigationInventoryLocalSourceKey, Status: protocol.NavigationInventorySourceOK,
		Entries: []protocol.NavigationInventoryEntry{
			{SourceKey: protocol.NavigationInventoryLocalSourceKey, EntryKey: "bbb/two", Name: "two"},
		},
	}}
	if _, _, changed := relay.preparePublication(thinned); !changed {
		t.Fatal("removal must publish")
	}
	if err := relay.validateSelection(valid); err == nil {
		t.Fatal("retired key must reject without same-name substitution")
	}
}

func TestInventoryRelayCancelAndPollGating(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_000, 0)}
	relay := newInventoryRelay(clock, &inventoryFakeDialer{})
	if relay.pollDue() {
		t.Fatal("closed relay must not poll")
	}
	if _, ok := relay.beginPoll(); ok {
		t.Fatal("closed relay must not claim the slot")
	}
	relay.setOpen(true, 2)
	if !relay.pollDue() {
		t.Fatal("open relay must poll")
	}
	first, ok := relay.beginPoll()
	if !ok {
		t.Fatal("open relay must claim the slot")
	}
	if _, ok := relay.beginPoll(); ok {
		t.Fatal("at most one query in flight")
	}
	relay.endPoll(first)
	clock.now = clock.now.Add(inventoryPollInterval)
	if !relay.pollDue() {
		t.Fatal("poll due again after the interval")
	}
	relay.cancel()
	if relay.pollDue() {
		t.Fatal("cancelled relay must not poll")
	}
	if _, ok := relay.beginPoll(); ok {
		t.Fatal("cancelled relay must not claim the slot")
	}
}

func TestInventoryRelayStaleCompletionDropsWithoutTouchingSlot(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_000, 0)}
	relay := newInventoryRelay(clock, &inventoryFakeDialer{})
	relay.setOpen(true, 2)
	stale, ok := relay.beginPoll()
	if !ok {
		t.Fatal("first poll should start")
	}
	// Closing for a new interaction releases the slot without waiting
	// for the outstanding worker.
	relay.setOpen(true, 3)
	relay.endPoll(stale)
	if relay.inFlight != 0 {
		t.Fatal("stale completion must not clear the new namespace slot")
	}
	clock.now = clock.now.Add(inventoryPollInterval)
	fresh, ok := relay.beginPoll()
	if !ok || fresh == stale {
		t.Fatal("new interaction must claim a fresh slot identity")
	}
	relay.endPoll(fresh + 99)
	if relay.inFlight != fresh {
		t.Fatal("foreign identity must not release the slot")
	}
	relay.endPoll(fresh)
	if relay.inFlight != 0 {
		t.Fatal("owning identity must release the slot")
	}
}

func TestInventoryRelayQueryRejectsIdentityMismatch(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_000, 0)}
	mismatched := inventoryTestResponse(999)
	conn := &inventoryFakeConnection{replies: []protocol.ServerMessage{mismatched}}
	dialer := &inventoryFakeDialer{conn: conn}
	relay := newInventoryRelay(clock, dialer)
	if _, err := relay.querySnapshot(context.Background(), 7); err == nil {
		t.Fatal("response ID mismatch must not admit data")
	}
	if len(relay.admitted) != 0 {
		t.Fatal("mismatched response must admit nothing")
	}
}
