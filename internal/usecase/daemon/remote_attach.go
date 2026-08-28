package daemon

import (
	"context"
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// routeRemoteTargetWithContext is the exact-target branch for picker handoffs.
// It is deliberately separate from legacy name routing: a lifecycle mismatch,
// missing tab, broken record, or stopped-record race is a no-such-target error,
// never an invitation to attach a same-name replacement.
func (d *Daemon) routeRemoteTargetWithContext(ctx context.Context, h protocol.Hello, tr ports.ServerConnection) (*session, *attachedClient, error) {
	if h.RemoteTarget == nil {
		return nil, nil, errors.New("daemon: missing remote target")
	}
	target := *h.RemoteTarget
	if err := target.Validate(); err != nil {
		return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "invalid remote target"}
	}
	if h.EnvironmentPolicy != protocol.EnvironmentPolicyDaemonOwned {
		return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "remote target requires daemon-owned environment"}
	}
	if h.Intent != protocol.IntentAttach && h.Intent != protocol.IntentResume {
		return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "remote target requires attach or resume"}
	}
	if err := d.waitForTargetRestore(ctx, target.SessionName); err != nil {
		var protocolErr *protoErr
		if errors.As(err, &protocolErr) && protocolErr.code == protocol.ErrInternal {
			return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "remote session is unavailable"}
		}
		return nil, nil, remoteTargetError(err)
	}

	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		return nil, nil, &protoErr{protocol.ErrServerShutdown, "daemon is shutting down"}
	}
	if live := d.findByNameLocked(target.SessionName); live != nil {
		if live.incarnation != target.LifecycleID {
			d.mu.Unlock()
			return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "remote session lifecycle has changed"}
		}
		if target.Stopped && target.LiveTabID != "" {
			d.mu.Unlock()
			return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "stopped target has a live tab selector"}
		}
		if _, ok := remoteTargetTabIndexLocked(live, target); !ok {
			d.mu.Unlock()
			return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "remote tab no longer exists"}
		}
		ac, err := d.finishRouteAttach(live, tr, h.Size, h, false, false)
		return live, ac, err
	}

	if !target.Stopped {
		d.mu.Unlock()
		return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "live remote target has no active runtime"}
	}
	inactive, ok := d.inactive[target.SessionName]
	if !ok || inactive.incarnation != target.LifecycleID {
		d.mu.Unlock()
		return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "remote session lifecycle no longer exists"}
	}
	if !inactive.canResume() {
		d.mu.Unlock()
		return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "remote session is unavailable"}
	}
	if _, ok := remoteTargetTabIndexInactive(inactive, target); !ok {
		d.mu.Unlock()
		return nil, nil, &protoErr{protocol.ErrNoSuchTarget, "remote stopped tab no longer exists"}
	}

	// Picker handoffs use the daemon's own environment and persisted CWD. The
	// client's Env/Cwd fields remain present for wire compatibility but are not
	// trusted for this branch.
	env := copyEnvironment(d.baseEnv)
	cwd := d.dirOrHome(inactive.cwd)
	sess, err := d.resumeRemoteInactiveSessionLocked(target, cwd, h.Geometry(), env, inactive)
	if err != nil {
		d.mu.Unlock()
		if errors.Is(err, errAttachmentTransition) || errors.Is(err, errSessionNameInUse) {
			return nil, nil, remoteTargetError(err)
		}
		return nil, nil, err
	}
	ac, err := d.finishRouteAttach(sess, tr, h.Size, h, true, false)
	return sess, ac, err
}

func (d *Daemon) resumeRemoteInactiveSessionLocked(target domain.RemoteSessionTarget, cwd string, geometry domain.Geometry, env []string, expected inactiveSession) (*session, error) {
	validate := func(current inactiveSession, _ domain.CatalogueRecord, authoritativeExists bool) bool {
		if !target.Stopped || d.persistEnabled && !authoritativeExists {
			return false
		}
		_, ok := target.ResolveTab(stoppedTabMetadata(current))
		return ok
	}
	return d.createSessionLockedWithModeAndInactiveFence(target.SessionName, false, cwd, geometry, env, &expected, validate, expected.tabNames)
}

func (d *Daemon) sendNavigationActionForAttachment(effect *attachmentEffect, action protocol.NavigationAction) error {
	directive := protocol.NavigationDirective{Action: action}
	armed := false
	if action == protocol.NavigationOpenHomePicker {
		leaseID, err := d.armParkedRoute(effect)
		if err != nil {
			return err
		}
		directive.LeaseID = leaseID
		armed = true
	}
	rollback := func() {
		if armed {
			effect.ac.clearParkedRoute()
		}
	}
	if err := effect.sendControl(directive); err != nil {
		rollback()
		return err
	}
	return nil
}

func (d *Daemon) sendRecentRouteNavigationActionForAttachment(effect *attachmentEffect, action protocol.RouteNavigationAction) error {
	if action.SnapshotGeneration == 0 || action.Key == 0 || action.Generation == 0 {
		return errAttachmentTransition
	}
	return effect.sendControl(action)
}

func (d *Daemon) sendCommittedRouteIdentityForAttachment(effect *attachmentEffect) error {
	if !effect.current() || effect.sess == nil {
		return errAttachmentTransition
	}
	effect.sess.mu.Lock()
	identity := protocol.CommittedRouteIdentity{
		Target:    protocol.ExactSessionTarget{LifecycleID: effect.sess.incarnation, SessionName: effect.sess.name},
		Ephemeral: effect.sess.ephemeral,
	}
	effect.sess.mu.Unlock()
	if err := identity.Validate(); err != nil {
		return errAttachmentTransition
	}
	return effect.sendControl(identity)
}

func (d *Daemon) finishRouteAttach(sess *session, tr ports.ServerConnection, sz domain.Size, h protocol.Hello, routeCreated, purge bool) (*attachedClient, error) {
	ac, err := d.finishAttach(sess, tr, sz, h)
	if err != nil && routeCreated {
		if cleanupErr := d.killSessionIfEmpty(sess, protocol.ReasonSessionKilled, purge); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
		return nil, err
	}
	if err == nil && routeCreated && ac != nil {
		ac.routeCreatedSession = true
		ac.routeSessionPurge = purge
	}
	return ac, err
}

// remoteTargetMatchesSessionLocked validates the exact lifecycle and tab
// selector while the daemon registry lock is held. Callers use this before
// changing resume ownership so a stale target cannot partially claim a session.
func (d *Daemon) remoteTargetMatchesSessionLocked(sess *session, target domain.RemoteSessionTarget) bool {
	if sess == nil || d.sessions[sess.id] != sess {
		return false
	}
	sess.mu.Lock()
	matches := sess.incarnation == target.LifecycleID && sess.name == target.SessionName
	sess.mu.Unlock()
	if !matches {
		return false
	}
	_, ok := remoteTargetTabIndexLocked(sess, target)
	return ok
}

func remoteTargetError(err error) error {
	var protocolErr *protoErr
	if errors.As(err, &protocolErr) {
		return protocolErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &protoErr{protocol.ErrNoSuchTarget, "remote target is unavailable"}
}

func stoppedTabMetadata(stopped inactiveSession) []domain.TabSelectorTab {
	records := stopped.tabRecords
	if len(records) == 0 {
		records = make([]domain.CatalogueTabRecord, len(stopped.tabNames))
		for i, name := range stopped.tabNames {
			records[i].Name = name
		}
	}
	metadata := make([]domain.TabSelectorTab, 0, len(records))
	for _, record := range records {
		metadata = append(metadata, domain.TabSelectorTab{ID: record.StableID, Name: record.Name})
	}
	return metadata
}

func remoteTargetTabIndexInactive(inactive inactiveSession, target domain.RemoteSessionTarget) (int, bool) {
	return target.ResolveTab(stoppedTabMetadata(inactive))
}

// remoteTargetTabIndexLocked resolves the selector against the live session's
// current ordered tabs. Caller holds d.mu; this function takes only the
// session/tab locks needed for a coherent metadata snapshot.
func remoteTargetTabIndexLocked(sess *session, target domain.RemoteSessionTarget) (int, bool) {
	if sess == nil {
		return 0, false
	}
	sess.mu.Lock()
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	metadata := make([]domain.TabSelectorTab, 0, len(tabs))
	for _, tab := range tabs {
		if tab == nil {
			return 0, false
		}
		tab.mu.Lock()
		metadata = append(metadata, domain.TabSelectorTab{ID: domain.TabStableID(tab.stableID), Name: tab.name})
		tab.mu.Unlock()
	}
	return target.ResolveTab(metadata)
}
