package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func setupMovePickerSessions(t *testing.T, extraDestinationTabs int) (*Daemon, *session, *attachedClient, *session, *tab, []func()) {
	t.Helper()
	return setupMovePickerSessionsWithClock(t, stubClock{}, extraDestinationTabs)
}

func setupMovePickerSessionsWithClock(t *testing.T, clock ports.Clock, extraDestinationTabs int) (*Daemon, *session, *attachedClient, *session, *tab, []func()) {
	t.Helper()
	d, source, ac, destination, destinationTab, releases, sends := setupMovePickerSessionsCore(t, clock, extraDestinationTabs)
	// The move suites drive typed interactions, so the daemon publishes offers
	// and snapshots to this attachment: drain them in the background so a
	// guarded send never blocks the fixture.
	go func() {
		for range sends {
		}
	}()
	return d, source, ac, destination, destinationTab, releases
}

// setupMovePickerSessionsCore builds the move fixture without draining the
// attachment transport, so a regression can read exactly what the daemon sent.
func setupMovePickerSessionsCore(t *testing.T, clock ports.Clock, extraDestinationTabs int) (*Daemon, *session, *attachedClient, *session, *tab, []func(), chan wire.Frame) {
	t.Helper()
	sourcePTY, releaseSource := newBlockingPTY(t)
	d, source, ac, sends := newManualSessionWithPTYsClock(t, clock, sourcePTY)
	source.id, source.name, source.incarnation = "source", "source", domain.IncarnationID{1}
	d.mu.Lock()
	delete(d.sessions, domain.SessionID("manual"))
	d.sessions[source.id] = source
	d.mu.Unlock()
	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	sourcePane := sourceTab.focusedPane()
	sourcePane.stableID = "source-pane"

	releases := []func(){releaseSource}
	destPTY, releaseDest := newBlockingPTY(t)
	releases = append(releases, releaseDest)
	destinationTab := newTabWithStableID("destination-tab", "destination-pane", destPTY, domain.Size{Cols: 80, Rows: 23})
	publishTiledPaneOwners(source, sourceTab)
	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", incarnation: domain.IncarnationID{2}, ephemeral: true}, ctx: source.ctx, cancel: func() {}, tabs: []*tab{destinationTab}}
	publishTiledPaneOwners(destination, destinationTab)
	for range extraDestinationTabs {
		extraPTY, releaseExtra := newBlockingPTY(t)
		releases = append(releases, releaseExtra)
		extraTab := newTabWithStableID("extra-tab", "extra-pane", extraPTY, domain.Size{Cols: 80, Rows: 23})
		destination.tabs = append(destination.tabs, extraTab)
		publishTiledPaneOwners(destination, extraTab)
	}
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()
	return d, source, ac, destination, destinationTab, releases, sends
}

// moveSourceForSession names the exact source a move command captures. The
// attachment carrying the command travels with it, exactly like the palette
// entry: the move transaction must freeze every other effect but this one.
func moveSourceForSession(sess *session, ac *attachedClient, tabID domain.TabStableID, paneID domain.PaneStableID) moveSourceLocator {
	return moveSourceLocator{
		Session:    moveSessionLocator{ID: sess.id, Incarnation: sess.incarnation, Name: sess.name},
		TabID:      tabID,
		PaneID:     paneID,
		Attachment: ac,
	}
}

// openMovePickerForTest opens one move interaction, exactly like the palette
// command does, and releases the effect that carried the offer: the
// interaction namespace lives on the attachment, and a later move must be able
// to freeze every other effect on the source.
func openMovePickerForTest(t *testing.T, d *Daemon, ac *attachedClient, sess *session, intent protocol.PickerIntent, source moveSourceLocator) {
	t.Helper()
	current := ac.transportSnapshot()
	_, effect, admitted := ac.beginCurrentAttachmentEffect(sess, current.transport)
	if !admitted {
		// A test may deliberately drop the source attachment before opening;
		// the captured capability still admits the open.
		effect, admitted = ac.beginAttachmentEffect(captureAttachmentCapability(sess, ac, ac.transport()))
	}
	require.True(t, admitted)
	require.NoError(t, d.openPickerForAttachment(ac, effect, intent, source, 0))
	effect.End()
}

// pickActionEffectForTest admits one effect for a typed action. A move drains
// every live effect on its source, so the caller ends it right before the
// commit, exactly like a client frame that has already been consumed.
func pickActionEffectForTest(t *testing.T, sess *session, ac *attachedClient) *attachmentEffect {
	t.Helper()
	effect, admitted := ac.beginAttachmentEffect(captureAttachmentCapability(sess, ac, ac.transport()))
	require.True(t, admitted)
	return effect
}

// moveSelectionForDestination builds the typed move the client would send for
// the destination session's first selectable line.
func moveSelectionForDestination(t *testing.T, ac *attachedClient, destination domain.SessionID) protocol.PickerSelection {
	t.Helper()
	ac.overlays.pickerMu.Lock()
	interaction := ac.overlays.pickerInteraction
	revision := ac.overlays.pickerRevisions[servingPickerSourceID]
	key := ""
	for candidate, target := range ac.overlays.pickerKeys {
		if target.Session == destination {
			key = candidate
			break
		}
	}
	ac.overlays.pickerMu.Unlock()
	require.NotEmpty(t, key, "no move destination resolves to session %s", destination)
	return protocol.PickerSelection{
		InteractionID: interaction, SourceID: servingPickerSourceID,
		SourceRevision: revision, Key: key, Action: protocol.PickerActionMove,
	}
}

func TestPaletteMovePaneCapturesSourceAndOpensPicker(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)

	d.handleInput(source, ac, []byte("\x1b "))
	require.True(t, ac.overlays.paletteActive())
	d.handleInput(source, ac, []byte("MFP\r"))
	require.False(t, ac.overlays.paletteActive())

	ac.overlays.pickerMu.Lock()
	open := ac.overlays.pickerOpen
	intent, captured := ac.overlays.pickerIntent, ac.overlays.pickerMoveSource
	ac.overlays.pickerMu.Unlock()
	require.True(t, open)
	require.Equal(t, protocol.PickerIntentMovePane, intent)
	require.Equal(t, moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name}, captured.Session)
	require.Equal(t, domain.TabStableID("source-tab"), captured.TabID)
	require.Equal(t, domain.PaneStableID("source-pane"), captured.PaneID)
	require.Same(t, ac, captured.Attachment)
}

func TestPaletteMoveTabCapturesActiveTabAndOpensPicker(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	sourceTab := source.tabs[0]
	sourceTab.stableID = "active-tab"

	d.handleInput(source, ac, []byte("\x1b "))
	d.handleInput(source, ac, []byte("MAT\r"))

	ac.overlays.pickerMu.Lock()
	open := ac.overlays.pickerOpen
	intent, captured := ac.overlays.pickerIntent, ac.overlays.pickerMoveSource
	ac.overlays.pickerMu.Unlock()
	require.True(t, open)
	require.Equal(t, protocol.PickerIntentMoveTab, intent)
	require.Equal(t, domain.TabStableID("active-tab"), captured.TabID)
	require.Equal(t, domain.PaneStableID(""), captured.PaneID)
	require.Same(t, ac, captured.Attachment)
}

func TestPaletteMoveWithoutDestinationShowsToastAndNoPicker(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	d.mu.Lock()
	delete(d.sessions, domain.SessionID("destination"))
	d.mu.Unlock()

	d.handleInput(source, ac, []byte("\x1b "))
	d.handleInput(source, ac, []byte("MFP\r"))

	require.False(t, ac.overlays.pickerClientActive())
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "No destination available.", history[0].Message)
}

func TestMovePickerMoveCommitsPane(t *testing.T) {
	d, source, ac, destination, destinationTab, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	sourceTab := source.tabs[0]
	moved := sourceTab.focusedPane()

	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, moveSourceForSession(source, ac, "source-tab", "source-pane"))
	effect := pickActionEffectForTest(t, source, ac)
	selection := moveSelectionForDestination(t, ac, destination.id)
	effect.End()
	d.resolvePickerSelection(effect, selection)

	require.Nil(t, source.tabs)
	require.Same(t, moved, destinationTab.panes[moved.id])
	require.Same(t, destination, moved.ownerSnapshot().session)
}

func TestMovePickerCommitMovePaneViaSharedAPI(t *testing.T) {
	d, source, ac, destination, destinationTab, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	sourceTab := source.tabs[0]
	moved := sourceTab.focusedPane()

	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, moveSourceForSession(source, ac, "source-tab", "source-pane"))
	selection := moveSelectionForDestination(t, ac, destination.id)
	ac.overlays.pickerMu.Lock()
	target, ok := ac.overlays.pickerKeys[selection.Key]
	ac.overlays.pickerMu.Unlock()
	require.True(t, ok)
	require.NoError(t, d.commitMovePickerSelection(protocol.PickerIntentMovePane, moveSourceForSession(source, ac, "source-tab", "source-pane"), target, nil, nil))

	require.Nil(t, source.tabs)
	require.Same(t, moved, destinationTab.panes[moved.id])
	require.Same(t, destination, moved.ownerSnapshot().session)
}

func TestMovePickerMoveCommitsTab(t *testing.T) {
	d, source, ac, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	movedTab := source.tabs[0]
	movedTab.stableID = "moved-tab"

	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMoveTab, moveSourceForSession(source, ac, "moved-tab", ""))
	effect := pickActionEffectForTest(t, source, ac)
	selection := moveSelectionForDestination(t, ac, destination.id)
	completed := make(chan struct{})
	go func() {
		d.resolvePickerSelection(effect, selection)
		close(completed)
	}()
	awaitTestCompletion(t, completed, "move-tab picker selection deadlocked on its own attachment effect")

	require.Nil(t, source.tabs)
	require.Len(t, destination.tabs, 2)
	require.Same(t, movedTab, destination.tabs[1])
}

func TestMovePickerCancelPerformsNoMutation(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	before := len(source.tabs)

	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, moveSourceForSession(source, ac, "source-tab", "source-pane"))
	effect := pickActionEffectForTest(t, source, ac)
	ac.overlays.pickerMu.Lock()
	interaction := ac.overlays.pickerInteraction
	ac.overlays.pickerMu.Unlock()
	require.True(t, d.closePickerForAttachment(ac, effect, interaction))

	require.False(t, ac.overlays.pickerClientActive())
	require.Len(t, source.tabs, before)
}

func TestMovePickerStaleDestinationReportsNotice(t *testing.T) {
	d, source, ac, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)

	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, moveSourceForSession(source, ac, "source-tab", "source-pane"))
	effect := pickActionEffectForTest(t, source, ac)
	selection := moveSelectionForDestination(t, ac, destination.id)
	d.mu.Lock()
	delete(d.sessions, destination.id)
	d.mu.Unlock()

	effect.End()
	d.resolvePickerSelection(effect, selection)

	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "Destination is no longer available.", history[0].Message)
	require.Len(t, source.tabs, 1)
}

func TestMovePickerStaleSourcePaneReportsPreciseFeedback(t *testing.T) {
	d, source, ac, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	sourceTab := source.tabs[0]

	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, moveSourceForSession(source, ac, "source-tab", "source-pane"))
	effect := pickActionEffectForTest(t, source, ac)
	selection := moveSelectionForDestination(t, ac, destination.id)
	sourceTab.mu.Lock()
	delete(sourceTab.panes, sourceTab.tree.Focus)
	sourceTab.mu.Unlock()

	effect.End()
	d.resolvePickerSelection(effect, selection)

	require.True(t, ac.overlays.pickerClientActive())
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "Pane no longer exists.", history[0].Message)
}

func TestMovePickerStaleSourceTabReportsPreciseFeedback(t *testing.T) {
	d, source, ac, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)

	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMoveTab, moveSourceForSession(source, ac, "source-tab", ""))
	effect := pickActionEffectForTest(t, source, ac)
	selection := moveSelectionForDestination(t, ac, destination.id)
	source.mu.Lock()
	source.tabs = nil
	source.mu.Unlock()

	effect.End()
	d.resolvePickerSelection(effect, selection)

	require.True(t, ac.overlays.pickerClientActive())
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "Tab no longer exists.", history[0].Message)
}

// TestMovePickerSupersedingOpenRetiresTheEarlierNamespace pins that a second
// open moves the interaction on, so rows published for the retired namespace
// can never be committed again.
func TestMovePickerSupersedingOpenRetiresTheEarlierNamespace(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	sourceSnapshot := moveSourceForSession(source, ac, "source-tab", "source-pane")
	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, sourceSnapshot)
	effect := pickActionEffectForTest(t, source, ac)
	ac.overlays.pickerMu.Lock()
	firstInteraction := ac.overlays.pickerInteraction
	firstRevision := ac.overlays.pickerRevisions[servingPickerSourceID]
	firstKey := ""
	for candidate := range ac.overlays.pickerKeys {
		firstKey = candidate
		break
	}
	ac.overlays.pickerMu.Unlock()
	require.NotEmpty(t, firstKey)

	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, sourceSnapshot)
	ac.overlays.pickerMu.Lock()
	secondInteraction := ac.overlays.pickerInteraction
	ac.overlays.pickerMu.Unlock()
	require.Greater(t, secondInteraction, firstInteraction)

	// A commit from the retired namespace is rejected and leaves the open
	// interaction untouched.
	d.resolvePickerSelection(effect, protocol.PickerSelection{
		InteractionID: firstInteraction, SourceID: servingPickerSourceID,
		SourceRevision: firstRevision, Key: firstKey, Action: protocol.PickerActionMove,
	})
	ac.overlays.pickerMu.Lock()
	require.True(t, ac.overlays.pickerOpen)
	require.Equal(t, secondInteraction, ac.overlays.pickerInteraction)
	ac.overlays.pickerMu.Unlock()
}

// Exercise the real selection admission (not a pre-ended test effect), and
// observe the publication boundary without sleeps or concurrent state reads.
func TestMovePickerCompositeFollow(t *testing.T) {
	for _, tt := range []struct {
		name         string
		intent       protocol.PickerIntent
		retainSource bool
	}{
		{"final-tab", protocol.PickerIntentMoveTab, false},
		{"retained-source-tab", protocol.PickerIntentMoveTab, true},
		{"final-pane", protocol.PickerIntentMovePane, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, source, ac, destination, destinationTab, releases := setupMovePickerSessions(t, 0)
			defer releaseAll(releases)
			movedTab := source.tabs[0]
			if tt.retainSource {
				pty, release := newBlockingPTY(t)
				defer release()
				source.tabs = append(source.tabs, newTabWithStableID("remaining", "remaining-pane", pty, movedTab.size))
				publishTiledPaneOwners(source, source.tabs[1])
			}
			peer := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: ac.size}
			peer.setSession(source)
			source.registerAttachment(peer)
			destinationPeer := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: ac.size}
			destinationPeer.setSession(destination)
			destination.registerAttachment(destinationPeer)
			destination.selectAttachmentTabLocked(destinationPeer, domain.TabStableID(destinationTab.stableID))
			old := captureAttachmentCapability(source, ac, ac.transport())
			locator := moveSourceForSession(source, ac, "source-tab", "")
			locator.AttachmentCapability = old
			if tt.intent == protocol.PickerIntentMovePane {
				locator.PaneID = "source-pane"
			}
			openMovePickerForTest(t, d, ac, source, tt.intent, locator)
			effect := pickActionEffectForTest(t, source, ac)
			selection := moveSelectionForDestination(t, ac, destination.id)
			closedAtCommit := false
			rebased := make(chan struct{}, 1)
			ac.renderStages.handoffRebase = func() { rebased <- struct{}{} }
			hook := func() { closedAtCommit = !ac.overlays.pickerClientActive() }
			d.beforeMoveTabCommit = hook
			d.beforeMovePaneCommit = hook
			done := make(chan struct{})
			go func() {
				d.resolvePickerSelection(effect, selection)
				close(done)
			}()
			awaitTestCompletion(t, done, "composite move waited on its own effect")
			require.True(t, closedAtCommit)
			select {
			case <-rebased:
			default:
				t.Fatal("composite move skipped transition first-paint rebase")
			}
			require.True(t, destination.attachmentRegistered(destinationPeer))
			require.Same(t, destinationTab, destination.tabForAttachment(destinationPeer))
			require.False(t, ac.overlays.pickerClientActive())
			require.Same(t, destination, ac.currentSession())
			require.True(t, destination.attachmentRegistered(ac))
			require.False(t, source.attachmentRegistered(ac))
			require.False(t, old.current())
			if tt.intent == protocol.PickerIntentMoveTab {
				require.Same(t, movedTab, destination.tabForAttachment(ac))
			} else {
				require.Same(t, destinationTab, destination.tabForAttachment(ac))
			}
			if tt.retainSource {
				require.True(t, source.attachmentRegistered(peer))
				require.Same(t, source, d.sessionByID(source.id))
			} else {
				require.False(t, source.attachmentRegistered(peer))
				require.Nil(t, d.sessionByID(source.id))
			}
		})
	}
}

// A composite follow ends the committing effect so the move can drain its
// source. Its completion must still reach the client: the postcommit admits a
// fresh effect on the published destination capability and reports the
// correlated PickerResult the ui-driver action waits for.
func TestMovePickerCompositeFollowReportsCorrelatedResult(t *testing.T) {
	d, source, ac, destination, _, releases, sends := setupMovePickerSessionsCore(t, stubClock{}, 0)
	defer releaseAll(releases)
	old := captureAttachmentCapability(source, ac, ac.transport())
	locator := moveSourceForSession(source, ac, "source-tab", "")
	locator.AttachmentCapability = old
	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMoveTab, locator)
	effect := pickActionEffectForTest(t, source, ac)
	selection := moveSelectionForDestination(t, ac, destination.id)
	selection.CauseActionID = 4242
	done := make(chan struct{})
	go func() {
		d.resolvePickerSelection(effect, selection)
		close(done)
	}()
	awaitTestCompletion(t, done, "composite follow never reported completion")

	result := awaitSentPickerResult(t, sends)
	require.Equal(t, selection.CauseActionID, result.CauseActionID)
	require.Equal(t, selection.InteractionID, result.InteractionID)
	require.Equal(t, selection.Key, result.Key)
	require.Equal(t, protocol.PickerActionMove, result.Action)
	assertNoFurtherFrame(t, sends, wire.MsgPickerResult, "composite follow reported its result more than once")
}

// TestMovePickerCompositeFollowCompletionReadmitsSupersededCapability pins the
// non-admission window: when the exact capability the follow published stops
// being admissible between its identity send and the postcommit completion (a
// concurrent link replacement while the attachment stays live), the completion
// must be re-admitted on the attachment's current capability and reported
// exactly once, instead of only logging while the ui-driver action runs to its
// deadline.
func TestMovePickerCompositeFollowCompletionReadmitsSupersededCapability(t *testing.T) {
	d, source, ac, destination, _, releases, sends := setupMovePickerSessionsCore(t, stubClock{}, 0)
	defer releaseAll(releases)
	base, ok := ac.transport().(*mockServerConnection)
	require.True(t, ok, "fixture transport is not the capturing mock")
	old := captureAttachmentCapability(source, ac, ac.transport())
	locator := moveSourceForSession(source, ac, "source-tab", "")
	locator.AttachmentCapability = old
	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMoveTab, locator)
	effect := pickActionEffectForTest(t, source, ac)
	selection := moveSelectionForDestination(t, ac, destination.id)
	selection.CauseActionID = 5252

	// Supersede the exact published capability after the follow's identity send
	// and first paint but before completion admission. The replacement keeps the
	// attachment live on its current session while retiring the published link.
	// A render effect may still be in flight, so this reproduces the transition's
	// end state (a newer published capability for the replacement link) rather
	// than requiring a drained gate.
	replaced := make(chan attachmentCapability, 1)
	d.beforeMoveFollowCompletion = func(published attachmentCapability) {
		supersedeAttachmentCapabilityForTest(published, base)
		replaced <- published
	}
	done := make(chan struct{})
	go func() {
		d.resolvePickerSelection(effect, selection)
		close(done)
	}()
	awaitTestCompletion(t, done, "composite follow never reported completion")
	published := <-replaced
	require.False(t, published.current(), "the superseded published capability was still admissible")

	// The completion must arrive through the current capability, exactly once.
	result := awaitSentPickerResult(t, sends)
	require.Equal(t, selection.CauseActionID, result.CauseActionID)
	require.Equal(t, selection.InteractionID, result.InteractionID)
	require.Equal(t, protocol.PickerActionMove, result.Action)
	assertNoFurtherFrame(t, sends, wire.MsgPickerResult, "composite follow reported its result more than once")
}

// supersedeAttachmentCapabilityForTest retires an attachment's exact published
// capability for a replacement link that keeps the attachment live, modelling a
// concurrent reconnect or transition that won the gate after publication. It
// republishes the current capability directly so it also works while a render
// effect may still be in flight.
func supersedeAttachmentCapabilityForTest(published attachmentCapability, base *mockServerConnection) {
	wrapper := &pickerSnapshotFailTransport{mockServerConnection: base}
	published.ac.replaceTransport(wrapper)
	current := captureAttachmentCapability(published.ac.currentAttachmentSession(), published.ac, wrapper)
	gate := &published.ac.lifecycle
	gate.mu.Lock()
	gate.initLocked()
	current.generation = gate.generation.Add(1)
	gate.capability = current
	gate.failedTransport = transportSnapshot{}
	gate.mu.Unlock()
}

// TestMovePickerCompositeFollowReadmitsSupersededCapabilityBeforeIdentity pins
// the earlier non-admission window: when the exact capability the follow
// published is superseded before finishAttachmentTransition admits its
// committed route identity (a concurrent link replacement while the attachment
// stays live), the transition must still send the committed identity, perform
// the authoritative first paint, and report completion exactly once on the
// attachment's current capability. Aborting instead would leave the post-close
// drain and the ui-driver action pending forever.
func TestMovePickerCompositeFollowReadmitsSupersededCapabilityBeforeIdentity(t *testing.T) {
	d, source, ac, destination, _, releases, sends := setupMovePickerSessionsCore(t, stubClock{}, 0)
	defer releaseAll(releases)
	base, ok := ac.transport().(*mockServerConnection)
	require.True(t, ok, "fixture transport is not the capturing mock")
	// A published route ledger is what makes a ready transition owe its
	// committed identity to the client.
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 1})
	old := captureAttachmentCapability(source, ac, ac.transport())
	locator := moveSourceForSession(source, ac, "source-tab", "")
	locator.AttachmentCapability = old
	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMoveTab, locator)
	effect := pickActionEffectForTest(t, source, ac)
	selection := moveSelectionForDestination(t, ac, destination.id)
	selection.CauseActionID = 6161

	rebased := make(chan struct{}, 1)
	ac.renderStages.handoffRebase = func() { rebased <- struct{}{} }

	// Supersede the published capability before the transition admits its
	// committed route identity, exactly in the window final review flagged.
	superseded := make(chan attachmentCapability, 1)
	d.beforeAttachmentTransitionIdentityAdmission = func(published attachmentCapability) {
		supersedeAttachmentCapabilityForTest(published, base)
		superseded <- published
	}
	done := make(chan struct{})
	go func() {
		d.resolvePickerSelection(effect, selection)
		close(done)
	}()
	awaitTestCompletion(t, done, "superseded composite follow never completed")
	published := <-superseded
	require.False(t, published.current(), "the superseded published capability was still admissible")

	select {
	case <-rebased:
	default:
		t.Fatal("superseded composite follow skipped its authoritative first paint")
	}

	// The committed identity must reach the live replacement link for the
	// session the attachment actually holds, exactly once, and the correlated
	// completion must follow exactly once.
	identity := awaitSentCommittedIdentity(t, sends)
	require.Equal(t, destination.name, identity.Target.SessionName)
	require.Equal(t, destination.incarnation, identity.Target.LifecycleID)
	result := awaitSentPickerResult(t, sends)
	require.Equal(t, selection.CauseActionID, result.CauseActionID)
	require.Equal(t, selection.InteractionID, result.InteractionID)
	require.Equal(t, protocol.PickerActionMove, result.Action)
	assertNoFurtherFrame(t, sends, wire.MsgCommittedRouteIdentity, "composite follow published its committed identity more than once")
	assertNoFurtherFrame(t, sends, wire.MsgPickerResult, "composite follow reported its result more than once")
}

// awaitSentCommittedIdentity reads the next committed route identity the daemon
// published, skipping unrelated frames.
func awaitSentCommittedIdentity(t *testing.T, sends <-chan wire.Frame) protocol.CommittedRouteIdentity {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case frame := <-sends:
			if frame.Type != wire.MsgCommittedRouteIdentity {
				continue
			}
			identity, err := wire.UnmarshalCommittedRouteIdentity(frame.Payload)
			require.NoError(t, err)
			return identity
		case <-deadline:
			t.Fatal("composite follow sent no committed route identity")
			return protocol.CommittedRouteIdentity{}
		}
	}
}

// assertNoFurtherFrame proves a frame was produced exactly once: no later frame
// of that type may follow the one the caller already observed.
func assertNoFurtherFrame(t *testing.T, sends <-chan wire.Frame, frameType wire.MsgType, failure string) {
	t.Helper()
	deadline := time.After(200 * time.Millisecond)
	for {
		select {
		case frame := <-sends:
			require.NotEqual(t, frameType, frame.Type, failure)
		case <-deadline:
			return
		}
	}
}

// A rejection after the picker closed must not touch the ended effect. It
// delivers bounded feedback and a repaint through the attachment's current
// capability so the client's post-close drain can release the terminal.
func TestMovePickerClosedRejectionUsesFreshEffect(t *testing.T) {
	d, source, ac, destination, destinationTab, releases, sends := setupMovePickerSessionsCore(t, stubClock{}, 0)
	defer releaseAll(releases)
	old := captureAttachmentCapability(source, ac, ac.transport())
	locator := moveSourceForSession(source, ac, "source-tab", "")
	locator.AttachmentCapability = old
	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMoveTab, locator)
	effect := pickActionEffectForTest(t, source, ac)
	// Publication must reject after the picker closed: changing the destination
	// tab geometry re-admits the already-validated topology as stale.
	d.afterMoveTabSourceSnapshot = func() {
		destinationTab.mu.Lock()
		destinationTab.size = domain.Size{Cols: 79, Rows: 22}
		destinationTab.mu.Unlock()
	}
	selection := moveSelectionForDestination(t, ac, destination.id)
	selection.CauseActionID = 99
	done := make(chan struct{})
	go func() {
		d.resolvePickerSelection(effect, selection)
		close(done)
	}()
	awaitTestCompletion(t, done, "closed rejection never returned")

	failure, result := collectSentPickerOutcome(t, sends)
	require.Nil(t, result, "rejected composite move reported success")
	require.Equal(t, selection.CauseActionID, failure.CauseActionID)
	require.Equal(t, protocol.PickerActionFailed, failure.Code)
}

// awaitSentPickerResult blocks until the daemon reports a completed mutation.
func awaitSentPickerResult(t *testing.T, sends <-chan wire.Frame) protocol.PickerResult {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case frame := <-sends:
			if frame.Type != wire.MsgPickerResult {
				continue
			}
			result, err := wire.UnmarshalPickerResult(frame.Payload)
			require.NoError(t, err)
			return result
		case <-deadline:
			t.Fatal("composite follow sent no correlated PickerResult")
			return protocol.PickerResult{}
		}
	}
}

// collectSentPickerOutcome drains the bounded set of frames one rejection can
// produce and returns the PickerFailure, plus any PickerResult that must not
// exist on the rejection path.
func collectSentPickerOutcome(t *testing.T, sends <-chan wire.Frame) (failure protocol.PickerFailure, result *protocol.PickerResult) {
	t.Helper()
	found := false
	deadline := time.After(500 * time.Millisecond)
	for {
		select {
		case frame := <-sends:
			switch frame.Type {
			case wire.MsgPickerFailure:
				decoded, err := wire.UnmarshalPickerFailure(frame.Payload)
				require.NoError(t, err)
				failure, found = decoded, true
			case wire.MsgPickerResult:
				decoded, err := wire.UnmarshalPickerResult(frame.Payload)
				require.NoError(t, err)
				result = &decoded
			}
		case <-deadline:
			require.True(t, found, "closed rejection sent no bounded failure")
			return failure, result
		}
	}
}
