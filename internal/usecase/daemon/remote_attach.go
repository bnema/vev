package daemon

import (
	"context"
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// routeRemoteTargetWithContext is the exact-target branch for picker handoffs.
// It is deliberately separate from legacy name routing: a lifecycle mismatch,
// missing tab, broken record, or stopped-record race is a no-such-target error,
// never an invitation to attach a same-name replacement.
func (d *Daemon) routeRemoteTargetWithContext(ctx context.Context, h ports.Hello, tr ports.Transport) (*session, *attachedClient, error) {
	if h.RemoteTarget == nil {
		return nil, nil, errors.New("daemon: missing remote target")
	}
	target := *h.RemoteTarget
	if err := target.Validate(); err != nil {
		return nil, nil, &protoErr{ports.ErrNoSuchTarget, "invalid remote target"}
	}
	if h.EnvironmentPolicy != ports.EnvironmentPolicyDaemonOwned {
		return nil, nil, &protoErr{ports.ErrNoSuchTarget, "remote target requires daemon-owned environment"}
	}
	if h.Intent != ports.IntentAttach && h.Intent != ports.IntentResume {
		return nil, nil, &protoErr{ports.ErrNoSuchTarget, "remote target requires attach or resume"}
	}
	if err := d.waitForTargetRestore(ctx, target.SessionName); err != nil {
		var protocol *protoErr
		if errors.As(err, &protocol) && protocol.code == ports.ErrInternal {
			return nil, nil, &protoErr{ports.ErrNoSuchTarget, "remote session is unavailable"}
		}
		return nil, nil, remoteTargetError(err)
	}

	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrServerShutdown, "daemon is shutting down"}
	}
	if live := d.findByNameLocked(target.SessionName); live != nil {
		if live.incarnation != target.LifecycleID {
			d.mu.Unlock()
			return nil, nil, &protoErr{ports.ErrNoSuchTarget, "remote session lifecycle has changed"}
		}
		if target.Stopped && target.LiveTabID != "" {
			d.mu.Unlock()
			return nil, nil, &protoErr{ports.ErrNoSuchTarget, "stopped target has a live tab selector"}
		}
		if _, ok := remoteTargetTabIndexLocked(live, target); !ok {
			d.mu.Unlock()
			return nil, nil, &protoErr{ports.ErrNoSuchTarget, "remote tab no longer exists"}
		}
		ac, err := d.finishRouteAttach(live, tr, h.Size, h, false, false)
		return live, ac, err
	}

	if !target.Stopped {
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrNoSuchTarget, "live remote target has no active runtime"}
	}
	inactive, ok := d.inactive[target.SessionName]
	if !ok || inactive.incarnation != target.LifecycleID {
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrNoSuchTarget, "remote session lifecycle no longer exists"}
	}
	if !inactive.canResume() {
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrNoSuchTarget, "remote session is unavailable"}
	}
	if _, ok := remoteTargetTabIndexInactive(inactive, target); !ok {
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrNoSuchTarget, "remote stopped tab no longer exists"}
	}

	// Picker handoffs use the daemon's own environment and persisted CWD. The
	// client's Env/Cwd fields remain present for wire compatibility but are not
	// trusted for this branch.
	env := copyEnvironment(d.baseEnv)
	cwd := d.dirOrHome(inactive.cwd)
	sess, err := d.resumeRemoteInactiveSessionLocked(target, cwd, h.Size, env, inactive)
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

func (d *Daemon) resumeRemoteInactiveSessionLocked(target domain.RemoteSessionTarget, cwd string, size domain.Size, env []string, expected inactiveSession) (*session, error) {
	validate := func(current inactiveSession, _ domain.CatalogueRecord, authoritativeExists bool) bool {
		if !target.Stopped || d.persistEnabled && !authoritativeExists {
			return false
		}
		_, ok := target.ResolveTab(stoppedTabMetadata(current))
		return ok
	}
	return d.createSessionLockedWithModeAndInactiveFence(target.SessionName, false, cwd, size, env, &expected, validate, expected.tabNames)
}

func (d *Daemon) sendNavigationActionForAttachment(token attachmentConnectionToken, action ports.NavigationAction) error {
	payload := ports.MarshalNavigationAction(action)
	if payload == nil {
		return errAttachmentTransition
	}
	return token.sendControl(ports.Frame{Type: ports.MsgNavigationAction, Payload: payload})
}

func (d *Daemon) sendRecentRouteNavigationActionForAttachment(token attachmentConnectionToken, action ports.RouteNavigationAction) error {
	payload, err := ports.MarshalRouteNavigationAction(action)
	if err != nil {
		return errAttachmentTransition
	}
	return token.sendControl(ports.Frame{Type: ports.MsgNavigateRecentRoute, Payload: payload})
}

func (d *Daemon) sendCommittedRouteIdentityForAttachment(token attachmentConnectionToken) error {
	if token.sess == nil {
		return errAttachmentTransition
	}
	token.sess.mu.Lock()
	identity := ports.CommittedRouteIdentity{
		Target:    ports.ExactSessionTarget{LifecycleID: token.sess.incarnation, SessionName: token.sess.name},
		Ephemeral: token.sess.ephemeral,
	}
	token.sess.mu.Unlock()
	payload, err := ports.MarshalCommittedRouteIdentity(identity)
	if err != nil {
		return errAttachmentTransition
	}
	return token.sendControl(ports.Frame{Type: ports.MsgCommittedRouteIdentity, Payload: payload})
}

func (d *Daemon) finishRouteAttach(sess *session, tr ports.Transport, sz domain.Size, h ports.Hello, routeCreated, purge bool) (*attachedClient, error) {
	ac, err := d.finishAttach(sess, tr, sz, h)
	if err != nil && routeCreated {
		if cleanupErr := d.killSessionIfEmpty(sess, ports.ReasonSessionKilled, purge); cleanupErr != nil {
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
	var protocol *protoErr
	if errors.As(err, &protocol) {
		return protocol
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &protoErr{ports.ErrNoSuchTarget, "remote target is unavailable"}
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
