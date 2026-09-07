package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/palette"
)

// findRemoteCNSDestination returns the palette CNS destination result for an
// exact remote endpoint, mirroring what the overlay offers the user.
func findRemoteCNSDestination(results []palette.Result, endpoint string) (palette.Result, bool) {
	for _, result := range results {
		_, kind, _, resultEndpoint, _, ok := result.CreateSessionDestination()
		if ok && kind == palette.CreateSessionOnRemoteHost && resultEndpoint == endpoint {
			return result, true
		}
	}
	return palette.Result{}, false
}

// seedDirectoryRemoteCatalog installs one reachable directory host for
// endpoint with a single live session at the daemon clock's current time.
func seedDirectoryRemoteCatalog(t *testing.T, d *Daemon, endpoint string) {
	t.Helper()
	var now time.Time
	if d.clock != nil {
		now = d.clock.Now()
	} else {
		now = time.Unix(1_000, 0)
	}
	seedRemoteDirectory(t, d, reachableDirectoryHost(endpoint, now, catalogue.RemoteCatalogSession{
		LifecycleID: domain.SessionLifecycleID{77}, Name: "work", State: catalogue.RemoteCatalogSessionUp,
		Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-work", Index: 0, Name: "shell"}}, ActiveTabID: "tab-work",
	}))
}

// TestPaletteRemoteCNSDestinationSurvivesStaleOrCheckingCatalog is the P1.1
// regression for the reported symptom: palette CNS creation targeting a
// configured remote fails with "that destination is no longer available"
// (errCreateDestinationUnavailable) whenever catalogue freshness lapses or an
// observation is in flight.
//
// Product requirement under test: a registered host remains a valid,
// attemptable CNS destination while its inventory is stale or checking.
// Freshness is presentation information, not creation authority; exact
// destination validation still applies at the destination itself.
func TestPaletteRemoteCNSDestinationSurvivesStaleOrCheckingCatalog(t *testing.T) {
	const endpoint = "user@arch"

	t.Run("listed while fresh", func(t *testing.T) {
		p, release := newBlockingPTY(t)
		defer release()
		d, sess, _, _ := newManualSessionWithPTYs(t, p)
		d.clock = &remotePickerClock{now: time.Unix(1_000, 0)}
		seedDirectoryRemoteCatalog(t, d, endpoint)

		results := d.paletteResults(sess, nil, testRecentRouteSnapshot())
		_, found := findRemoteCNSDestination(results, endpoint)
		require.True(t, found, "fresh registered destination must be offered")
	})

	t.Run("stale age keeps registered destination", func(t *testing.T) {
		p, release := newBlockingPTY(t)
		defer release()
		d, sess, _, _ := newManualSessionWithPTYs(t, p)
		clock := &remotePickerClock{now: time.Unix(1_000, 0)}
		d.clock = clock
		seedDirectoryRemoteCatalog(t, d, endpoint)

		// Let the 30s inventory TTL lapse with no new observation. The
		// seeded snapshot keeps its original success time, so the daemon
		// clock now observes it as stale presentation.
		clock.Advance(31 * time.Second)

		results := d.paletteResults(sess, nil, testRecentRouteSnapshot())
		_, found := findRemoteCNSDestination(results, endpoint)
		require.True(t, found, "stale registered destination must remain offered, not hidden by age alone")
	})

	t.Run("in-flight observation keeps registered destination", func(t *testing.T) {
		p, release := newBlockingPTY(t)
		defer release()
		d, sess, _, _ := newManualSessionWithPTYs(t, p)
		d.clock = &remotePickerClock{now: time.Unix(1_000, 0)}
		seedDirectoryRemoteCatalog(t, d, endpoint)

		// Mark the seeded host checking: an observation is in flight, but
		// the last-known inventory and registration remain published.
		stub, ok := d.remoteDirectory.(*stubRemoteDirectory)
		require.True(t, ok, "regression harness seeds a stub directory")
		stub.mu.Lock()
		stub.snapshot.Hosts[0].Checking = true
		stub.mu.Unlock()

		results := d.paletteResults(sess, nil, testRecentRouteSnapshot())
		_, found := findRemoteCNSDestination(results, endpoint)
		require.True(t, found, "checking registered destination must remain offered while observation is in flight")
	})

	t.Run("prompt submission after state change is accepted", func(t *testing.T) {
		const second = "user@mule"
		p, release := newBlockingPTY(t)
		defer release()
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		clock := &remotePickerClock{now: time.Unix(1_000, 0)}
		d.clock = clock
		seedDirectoryRemoteCatalog(t, d, endpoint)
		stub, ok := d.remoteDirectory.(*stubRemoteDirectory)
		require.True(t, ok, "regression harness seeds a stub directory")
		stub.mu.Lock()
		stub.snapshot.Hosts = append(stub.snapshot.Hosts, reachableDirectoryHost(second, clock.Now(), catalogue.RemoteCatalogSession{
			LifecycleID: domain.SessionLifecycleID{78}, Name: "other", State: catalogue.RemoteCatalogSessionUp,
			Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-other", Index: 0, Name: "shell"}}, ActiveTabID: "tab-other",
		}))
		for i := range stub.snapshot.Hosts {
			stub.snapshot.Hosts[i].Rank = i
			if stub.snapshot.Hosts[i].DisplayOrigin == "" {
				stub.snapshot.Hosts[i].DisplayOrigin = domain.RemoteDisplayOrigin(stub.snapshot.Hosts[i].Endpoint)
			}
		}
		stub.mu.Unlock()

		// Real handler flow with an attachment effect: open the palette
		// and type an explicit CNS request. Destination mode needs at
		// least two destinations; the first-ranked row is the target
		// endpoint with the name bound.
		token := beginRecentRoutePaletteEffect(t, d, sess, ac)
		d.handleInputForAttachment(token, []byte("\x1b "))
		awaitFrame(t, sends, wire.MsgOutput)
		d.handleInputForAttachment(token, []byte("CNS workremote"))
		// Destination mode lists candidates without a selection until the
		// user moves; Down selects the first-ranked row (the target).
		d.handleInputForAttachment(token, []byte("\x1b[B"))
		selected, ok := ac.overlays.palette.Selected()
		require.True(t, ok, "palette must offer a selection")
		name, kind, _, selectedEndpoint, _, isDestination := selected.CreateSessionDestination()
		if !(isDestination && kind == palette.CreateSessionOnRemoteHost && selectedEndpoint == endpoint) {
			matches := ac.overlays.palette.Matches()
			texts := make([]string, 0, len(matches))
			for _, match := range matches {
				texts = append(texts, match.Result.DisplayText())
			}
			t.Fatalf("query must select the remote CNS destination, got %q (query %q, matches %q)", selected.DisplayText(), ac.overlays.palette.Query(), texts)
		}
		require.Equal(t, "workremote", name)

		// State changes after selection: the inventory TTL lapses before
		// the destination is submitted. Registration still authorizes the
		// submit; the destination validates the exact identity itself.
		clock.Advance(31 * time.Second)

		d.handleInputForAttachment(token, []byte("\r"))
		deadline := time.After(2 * time.Second)
		for {
			select {
			case frame := <-sends:
				if frame.Type == wire.MsgAttachTarget {
					target, err := wire.UnmarshalAttachTarget(frame.Payload)
					require.NoError(t, err)
					require.Equal(t, endpoint, target.Endpoint)
					require.Equal(t, "workremote", target.Session)
					return
				}
			case <-deadline:
				t.Fatalf("registered destination submit after TTL lapse must still reach %s, not fail with %q (feedback %q, palette open=%v)",
					endpoint, errCreateDestinationUnavailable, ac.overlays.paletteFeedback, ac.overlays.paletteActive())
			}
		}
	})
}
