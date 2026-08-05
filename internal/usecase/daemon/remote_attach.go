package daemon

import (
	"context"
	"errors"
	"os"

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
		ac, err := d.finishAttach(live, tr, h.Size, terminalEnv{TrueColor: h.TrueColor}, h)
		return live, ac, err
	}

	stopped, ok := d.stopped[target.SessionName]
	if !ok || stopped.purging || stopped.incarnation != target.LifecycleID {
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrNoSuchTarget, "remote session lifecycle no longer exists"}
	}
	if stopped.state == ports.SessionBroken || stopped.record.DegradedReason != "" {
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrNoSuchTarget, "remote session is broken"}
	}
	if _, ok := remoteTargetTabIndexStopped(stopped, target); !ok {
		d.mu.Unlock()
		return nil, nil, &protoErr{ports.ErrNoSuchTarget, "remote stopped tab no longer exists"}
	}

	// Picker handoffs use the daemon's own environment and persisted CWD. The
	// client's Env/Cwd fields remain present for wire compatibility but are not
	// trusted for this branch.
	env := copyEnvironment(os.Environ())
	cwd := d.dirOrHome(stopped.cwd)
	sess, err := d.createSessionLockedWithMode(target.SessionName, false, cwd, h.Size, terminalEnv{TrueColor: h.TrueColor}, env, stopped.tabNames)
	if err != nil {
		d.mu.Unlock()
		return nil, nil, err
	}
	ac, err := d.finishAttach(sess, tr, h.Size, terminalEnv{TrueColor: h.TrueColor}, h)
	if err != nil {
		// The route-created session has no published client if finishAttach
		// fails. Retire it immediately rather than leaving a shadow live row.
		_ = d.killSession(sess, ports.ReasonSessionKilled, false)
		return nil, nil, err
	}
	ac.routeCreatedSession = true
	return sess, ac, nil
}

func (d *Daemon) remoteTargetMatchesSession(sess *session, target domain.RemoteSessionTarget) bool {
	if sess == nil || sess.incarnation != target.LifecycleID || sess.name != target.SessionName {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sessions[sess.id] != sess {
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

func stoppedTabMetadata(stopped stoppedSession) []domain.TabSelectorTab {
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

func remoteTargetTabIndexStopped(stopped stoppedSession, target domain.RemoteSessionTarget) (int, bool) {
	return target.StoppedTab.Resolve(stoppedTabMetadata(stopped))
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
	if target.Stopped {
		return target.StoppedTab.Resolve(metadata)
	}
	if target.LiveTabID == "" {
		return 0, false
	}
	found := -1
	for i, tab := range metadata {
		if tab.ID != target.LiveTabID {
			continue
		}
		if found >= 0 {
			return 0, false
		}
		found = i
	}
	return found, found >= 0
}
