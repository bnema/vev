package daemon

import (
	"context"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

var errRemoteViewTabUnavailable = errors.New("remote view: selected tab is unavailable")

func runningRemoteTarget(target domain.RemoteSessionTarget, active domain.TabStableID) domain.RemoteSessionTarget {
	target.Stopped = false
	target.StoppedTab = domain.TabSelector{}
	target.LiveTabID = active
	return target
}

// activateRemoteViewTab moves an already-handshaken remote view to the exact
// stable tab selected in the local picker. The remote protocol presently
// exposes relative tab commands, so each command waits for the metadata
// revision it causes and recalculates from the authoritative remote order.
// The caller remains locally attached until this succeeds.
func (d *Daemon) activateRemoteViewTab(ctx context.Context, view *remoteView, target domain.RemoteSessionTarget) error {
	if d == nil || view == nil {
		return errRemoteViewUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := target.Validate(); err != nil {
		return fmt.Errorf("remote view: invalid tab target: %w", err)
	}
	view.tabActivationMu.Lock()
	defer view.tabActivationMu.Unlock()

	for attempts := 0; ; attempts++ {
		link, generation, revision, active, selected, tabCount, err := remoteViewTabActivationSnapshot(view, target)
		if err != nil {
			return err
		}
		if active == selected {
			view.mu.Lock()
			if !view.closed && view.link == link && view.linkGeneration == generation && view.metadata.ActiveTabID == selected {
				view.reconnectTarget = runningRemoteTarget(target, selected)
			}
			view.mu.Unlock()
			return nil
		}
		// Each successful relative command must advance the metadata revision.
		// A remote peer that continually races us cannot make this selection
		// converge indefinitely.
		if attempts >= tabCount {
			return errRemoteViewTabUnavailable
		}
		slug, err := remoteViewRelativeTabCommand(view, link, generation, selected)
		if err != nil {
			return err
		}
		if slug == "" {
			return nil
		}
		if err := d.sendRemoteViewCommand(ctx, link, generation, slug); err != nil {
			return err
		}
		if err := d.waitRemoteViewMetadataRevision(ctx, view, link, generation, revision); err != nil {
			return err
		}
	}
}

func remoteViewTabActivationSnapshot(view *remoteView, target domain.RemoteSessionTarget) (link *remoteLink, generation, revision uint64, active, selected domain.TabStableID, tabCount int, err error) {
	view.mu.Lock()
	defer view.mu.Unlock()
	if !remoteViewLinkReusableLocked(view) {
		return nil, 0, 0, "", "", 0, errRemoteViewUnavailable
	}
	selected, err = remoteTargetTabID(target, view.metadata)
	if err != nil {
		return nil, 0, 0, "", "", 0, err
	}
	if _, ok := remoteSessionTabIndex(view.metadata.Tabs, selected); !ok {
		return nil, 0, 0, "", "", 0, errRemoteViewTabUnavailable
	}
	return view.link, view.linkGeneration, view.metadata.Revision, view.metadata.ActiveTabID, selected, len(view.metadata.Tabs), nil
}

func remoteTargetTabID(target domain.RemoteSessionTarget, metadata ports.SessionMeta) (domain.TabStableID, error) {
	if !target.Stopped {
		return target.LiveTabID, nil
	}
	tabs := make([]domain.TabSelectorTab, len(metadata.Tabs))
	for i, tab := range metadata.Tabs {
		tabs[i] = domain.TabSelectorTab{ID: tab.ID, Name: tab.Name}
	}
	index, ok := target.StoppedTab.Resolve(tabs)
	if !ok {
		return "", errRemoteViewTabUnavailable
	}
	return tabs[index].ID, nil
}

func remoteSessionTabIndex(tabs []ports.SessionTabMeta, id domain.TabStableID) (int, bool) {
	found := -1
	for index, tab := range tabs {
		if tab.ID != id {
			continue
		}
		if found != -1 {
			return 0, false
		}
		found = index
	}
	return found, found >= 0
}

func remoteViewRelativeTabCommand(view *remoteView, link *remoteLink, generation uint64, selected domain.TabStableID) (string, error) {
	view.mu.Lock()
	defer view.mu.Unlock()
	if view.closed || view.link != link || view.linkGeneration != generation || !link.active {
		return "", errRemoteViewStale
	}
	activeIndex, activeOK := remoteSessionTabIndex(view.metadata.Tabs, view.metadata.ActiveTabID)
	selectedIndex, selectedOK := remoteSessionTabIndex(view.metadata.Tabs, selected)
	if !activeOK || !selectedOK || len(view.metadata.Tabs) < 2 {
		return "", errRemoteViewTabUnavailable
	}
	forward := (selectedIndex - activeIndex + len(view.metadata.Tabs)) % len(view.metadata.Tabs)
	backward := (activeIndex - selectedIndex + len(view.metadata.Tabs)) % len(view.metadata.Tabs)
	if forward == 0 || backward == 0 {
		return "", nil
	}
	if forward <= backward {
		return "next-tab", nil
	}
	return "previous-tab", nil
}

// sendRemoteViewCommand is intentionally narrower than the remote daemon's
// attached-command executor. It is not palette forwarding: stable remote
// metadata only supports the two no-argument relative tab operations.
func (d *Daemon) sendRemoteViewCommand(ctx context.Context, link *remoteLink, generation uint64, slug string) error {
	if d == nil || link == nil || link.commands == nil {
		return errRemoteViewUnavailable
	}
	if slug != "next-tab" && slug != "previous-tab" {
		return fmt.Errorf("remote view: command %q is not allowed", slug)
	}
	requestID, outcome := link.commands.Publish(generation)
	payload, err := ports.MarshalCommandRequest(ports.CommandRequest{
		Version: ports.ProtocolVersion, RequestID: requestID, Attached: true, Slug: slug,
	})
	if err != nil {
		link.commands.Fail(requestID, generation, err)
		return fmt.Errorf("remote view: encode command: %w", err)
	}
	if err := link.send(ports.Frame{Type: ports.MsgCommand, Payload: payload}); err != nil {
		link.commands.Fail(requestID, generation, err)
		d.markRemoteLinkUnavailable(link)
		return fmt.Errorf("remote view: send command: %w", err)
	}
	clock := d.clock
	if clock == nil {
		clock = systemClock{}
	}
	result, err := link.commands.Wait(ctx, clock, requestID, generation, outcome)
	if err != nil {
		return fmt.Errorf("remote view: wait for command: %w", err)
	}
	if !result.OK {
		return fmt.Errorf("remote view: command %q failed (%d): %s", slug, result.Code, result.Text)
	}
	return nil
}

func (d *Daemon) waitRemoteViewMetadataRevision(ctx context.Context, view *remoteView, link *remoteLink, generation, revision uint64) error {
	if d == nil || view == nil || link == nil {
		return errRemoteViewUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	clock := d.clock
	if clock == nil {
		clock = systemClock{}
	}
	timer := clock.NewTimer(CommandRequestTimeout)
	if timer == nil {
		return ErrCommandRequestTimeout
	}
	defer timer.Stop()

	for {
		view.mu.Lock()
		current := !view.closed && view.link == link && view.linkGeneration == generation && link.active
		if !current {
			view.mu.Unlock()
			return errRemoteViewStale
		}
		if view.metadata.Revision > revision {
			view.mu.Unlock()
			return nil
		}
		changed := view.metadataChanged
		if changed == nil {
			changed = make(chan struct{})
			view.metadataChanged = changed
		}
		view.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C():
			return ErrCommandRequestTimeout
		case <-changed:
		}
	}
}
