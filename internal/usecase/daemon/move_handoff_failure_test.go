package daemon

import (
	"context"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

type moveTabState struct {
	ptr                *tab
	stableID           string
	tree               *layout.Tree
	panes              map[layout.PaneID]*pane
	nextPaneID         int
	layoutGeneration   uint64
	floatingState      floatingState
	floatingPane       *pane
	floatingVisible    bool
	floatingGeneration uint64
	ctxDone            bool
}

type moveSessionState struct {
	ptr                *session
	id                 domain.SessionID
	name               string
	incarnation        domain.IncarnationID
	tabs               []*tab
	active             int
	client             *attachedClient
	snatched           map[*attachedClient]struct{}
	moveReservations   uint
	snapshotGeneration uint64
	snapshotPublished  uint64
	snapshotDirty      bool
	snapshotEligible   bool
	ctxDone            bool
}

type moveLeaseState struct {
	lease      *attachmentLease
	attachment *attachedClient
	ready      bool
	active     bool
}

type moveClientState struct {
	transport        transportSnapshot
	roleGeneration   uint64
	currentSession   *session
	sourceRole       attachmentRole
	destinationRole  attachmentRole
	sourceLease      moveLeaseState
	destinationLease moveLeaseState
	roleEffectPhase  roleEffectPhase
}

type movePaneState struct {
	id              layout.PaneID
	stableID        string
	owner           *paneOwner
	ownerGeneration uint64
	ctxDone         bool
}

type moveState struct {
	closing        bool
	registry       map[domain.SessionID]*session
	source         moveSessionState
	destination    moveSessionState
	sourceTab      moveTabState
	destinationTab moveTabState
	movedPane      movePaneState
	client         moveClientState
}

func captureMoveState(d *Daemon, source, destination *session, sourceTab, destinationTab *tab, movedPane *pane, client *attachedClient) moveState {
	d.mu.Lock()
	d.notices.routingMu.Lock()
	unlockSessions := lockAttachmentSessions(source, destination)
	unlockTabs := lockMoveTabs(sourceTab, destinationTab)
	movedPane.mu.Lock()

	registry := make(map[domain.SessionID]*session, len(d.sessions))
	maps.Copy(registry, d.sessions)
	state := moveState{
		closing:        d.closing,
		registry:       registry,
		source:         captureMoveSessionStateLocked(source),
		destination:    captureMoveSessionStateLocked(destination),
		sourceTab:      captureMoveTabStateLocked(sourceTab),
		destinationTab: captureMoveTabStateLocked(destinationTab),
		movedPane: movePaneState{
			id:              movedPane.id,
			stableID:        movedPane.stableID,
			owner:           movedPane.ownerSnapshot(),
			ownerGeneration: movedPane.ownerGeneration,
			ctxDone:         moveContextDone(movedPane.ctx),
		},
		client: captureMoveClientStateLocked(source, destination, client),
	}

	movedPane.mu.Unlock()
	unlockTabs()
	unlockSessions()
	d.notices.routingMu.Unlock()
	d.mu.Unlock()
	return state
}

func captureMoveSessionStateLocked(sess *session) moveSessionState {
	snatched := make(map[*attachedClient]struct{}, len(sess.snatched))
	maps.Copy(snatched, sess.snatched)
	return moveSessionState{
		ptr:                sess,
		id:                 sess.id,
		name:               sess.name,
		incarnation:        sess.incarnation,
		tabs:               append([]*tab(nil), sess.tabs...),
		active:             sess.active,
		client:             sess.client,
		snatched:           snatched,
		moveReservations:   sess.moveReservations,
		snapshotGeneration: sess.snapshotGeneration,
		snapshotPublished:  sess.snapshotPublishedGeneration,
		snapshotDirty:      sess.snapDirty.Load(),
		snapshotEligible:   sess.snapEligible.Load(),
		ctxDone:            moveContextDone(sess.ctx),
	}
}

func captureMoveTabStateLocked(tb *tab) moveTabState {
	panes := make(map[layout.PaneID]*pane, len(tb.panes))
	maps.Copy(panes, tb.panes)
	return moveTabState{
		ptr:                tb,
		stableID:           tb.stableID,
		tree:               tb.tree.Clone(),
		panes:              panes,
		nextPaneID:         tb.nextPaneID,
		layoutGeneration:   tb.layoutGeneration,
		floatingState:      tb.floating.state,
		floatingPane:       tb.floating.pane,
		floatingVisible:    tb.floating.desiredVisible,
		floatingGeneration: tb.floating.generation,
		ctxDone:            moveContextDone(tb.ctx),
	}
}

func captureMoveClientStateLocked(source, destination *session, client *attachedClient) moveClientState {
	return moveClientState{
		transport:        client.transportSnapshot(),
		roleGeneration:   client.roleGeneration.Load(),
		currentSession:   client.currentSession(),
		sourceRole:       source.attachmentRoleLocked(client),
		destinationRole:  destination.attachmentRoleLocked(client),
		sourceLease:      captureMoveLeaseState(source, client),
		destinationLease: captureMoveLeaseState(destination, client),
		roleEffectPhase:  captureMoveRoleEffectPhase(client),
	}
}

func captureMoveLeaseState(sess *session, client *attachedClient) moveLeaseState {
	coordinator := sess.renderCoordinator()
	if coordinator == nil {
		return moveLeaseState{}
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	lease := coordinator.lease
	if lease == nil || lease.attachment != client {
		return moveLeaseState{lease: lease}
	}
	return moveLeaseState{lease: lease, attachment: lease.attachment, ready: lease.ready, active: lease.active}
}

func captureMoveRoleEffectPhase(client *attachedClient) roleEffectPhase {
	client.roleEffects.mu.Lock()
	defer client.roleEffects.mu.Unlock()
	return client.roleEffects.phase
}

func moveContextDone(ctx context.Context) bool {
	return ctx != nil && ctx.Err() != nil
}

func TestMovePaneFinalSourceHandoffValidationFailureIsAtomic(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	defer release1()
	defer release2()

	d, source, sourceClient, _ := newManualSessionWithPTYs(t, p1)
	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	movedPane := sourceTab.focusedPane()

	destination := &session{
		id:        "destination",
		name:      "destination",
		ephemeral: true,
		tabs:      []*tab{newTabWithStableID("destination-tab", "destination-pane", p2, domain.Size{Cols: 80, Rows: 23})},
		active:    0,
	}
	destinationTab := destination.tabs[0]
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()
	require.NotNil(t, d.attachCoordinator(source, nil, sourceClient, true))

	before := captureMoveState(d, source, destination, sourceTab, destinationTab, movedPane, sourceClient)
	originalTransport := before.client.transport
	staleTransport := &closeTrackingTransport{}
	staleInjected := make(chan struct{})
	d.afterRoleEffectGateFrozen = func(action string, client *attachedClient) {
		if action != "move-pane" || client != sourceClient {
			return
		}
		client.replaceTransport(staleTransport)
		close(staleInjected)
	}

	moveDone := make(chan error, 1)
	go func() {
		moveDone <- d.movePane(movePaneRequest{
			Source:           moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
			SourceTabID:      domain.TabStableID(sourceTab.stableID),
			SourcePaneID:     domain.PaneStableID(movedPane.stableID),
			Destination:      moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
			DestinationTabID: domain.TabStableID(destinationTab.stableID),
		})
	}()
	<-staleInjected
	err := <-moveDone

	// Restore the exact test-injected transport incarnation before comparing the
	// complete state. The move itself must not be able to alter this state.
	sourceClient.linkMu.Lock()
	sourceClient.tr = originalTransport.transport
	sourceClient.transportIncarnation = originalTransport.incarnation
	sourceClient.linkMu.Unlock()
	d.afterRoleEffectGateFrozen = nil

	require.ErrorIs(t, err, errAttachmentTransition)
	after := captureMoveState(d, source, destination, sourceTab, destinationTab, movedPane, sourceClient)
	require.Equal(t, before, after, "stale handoff validation must reject before any move publication")
}
