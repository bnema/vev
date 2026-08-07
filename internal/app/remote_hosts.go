package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"text/tabwriter"

	remoteadapter "github.com/bnema/vev/internal/adapters/remote"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/platform"
	"github.com/bnema/vev/internal/ports"
)

const (
	hostActionAdd  = "add"
	hostActionRm   = "rm"
	hostActionList = "list"
)

// remoteHostDeps holds injectable seams for host management and remote listing.
type remoteHostDeps struct {
	stateDir  func() string
	store     ports.RemoteHostStore
	catalog   ports.RemoteCatalogClient
	localList func(context.Context) ([]ports.SessionInfo, error)
	stdout    io.Writer
}

func defaultRemoteHostDeps() remoteHostDeps {
	return remoteHostDeps{
		stateDir:  platform.StateDir,
		catalog:   remoteadapter.NewCatalogClient(),
		localList: listLocalSessions,
		stdout:    os.Stdout,
	}
}

func (d remoteHostDeps) withDefaults() remoteHostDeps {
	if d.stateDir == nil {
		d.stateDir = platform.StateDir
	}
	if d.store == nil {
		d.store = remoteadapter.NewFileHostStore(remoteadapter.HostStorePath(d.stateDir()))
	}
	if d.catalog == nil {
		d.catalog = remoteadapter.NewCatalogClient()
	}
	if d.localList == nil {
		d.localList = listLocalSessions
	}
	if d.stdout == nil {
		d.stdout = os.Stdout
	}
	return d
}

func (d remoteHostDeps) hostStore() ports.RemoteHostStore {
	return d.store
}

func runHostCommand(ctx context.Context, cmd command, deps remoteHostDeps) error {
	_ = ctx
	deps = deps.withDefaults()

	switch cmd.hostAction {
	case hostActionAdd:
		return hostAdd(deps, cmd.hostTarget)
	case hostActionRm:
		return hostRm(deps, cmd.hostTarget)
	case hostActionList:
		return hostList(deps)
	default:
		return usagef("unknown host action %q", cmd.hostAction)
	}
}

func hostAdd(deps remoteHostDeps, target string) error {
	if err := domain.ValidateRemoteHostTarget(target); err != nil {
		return err
	}
	if err := deps.hostStore().AddPinned(target); err != nil {
		return fmt.Errorf("vev: adding pinned host %q: %w", target, err)
	}
	return nil
}

func hostRm(deps remoteHostDeps, target string) error {
	if err := domain.ValidateRemoteHostTarget(target); err != nil {
		return err
	}
	deleted, err := deps.hostStore().Remove(target)
	if err != nil {
		return fmt.Errorf("vev: removing host %q: %w", target, err)
	}
	if !deleted {
		return fmt.Errorf("vev: unknown host %q", target)
	}
	return nil
}

func hostList(deps remoteHostDeps) error {
	hosts, err := mergeKnownHosts(deps)
	if err != nil {
		return err
	}
	if len(hosts) == 0 {
		_, _ = fmt.Fprintln(deps.stdout, "no hosts")
		return nil
	}
	tw := tabwriter.NewWriter(deps.stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "TARGET\tSOURCE")
	for _, host := range hosts {
		_, _ = fmt.Fprintf(tw, "%s\t%s\n", host.Target, remoteHostSource(host))
	}
	return tw.Flush()
}

func remoteHostSource(host domain.RemoteHost) string {
	switch {
	case host.Pinned && host.Learned:
		return "pinned,learned"
	case host.Pinned:
		return "pinned"
	case host.Learned:
		return "learned"
	default:
		return ""
	}
}

func runRemoteList(ctx context.Context, cmd command, deps remoteHostDeps) error {
	deps = deps.withDefaults()

	hosts, err := mergeKnownHosts(deps)
	if err != nil {
		return err
	}
	if cmd.listHost != "" {
		return listOneRemoteHost(ctx, deps, hosts, cmd.listHost)
	}
	return listAllSessions(ctx, deps, hosts)
}

func listOneRemoteHost(ctx context.Context, deps remoteHostDeps, hosts []domain.RemoteHost, target string) error {
	known := false
	for _, host := range hosts {
		if host.Target == target {
			known = true
			break
		}
	}
	if !known {
		return fmt.Errorf("vev: unknown host %q", target)
	}
	catalog, err := deps.catalog.List(ctx, target)
	if err != nil {
		return fmt.Errorf("vev: listing sessions on %q: %w", target, err)
	}
	printSessions(deps.stdout, catalogSessionsAsInfo(target, catalog.Sessions))
	return nil
}

// listAllSessions queries hosts sequentially for deterministic output ordering.
func listAllSessions(ctx context.Context, deps remoteHostDeps, hosts []domain.RemoteHost) error {
	local, err := deps.localList(ctx)
	if err != nil {
		if local != nil {
			printSessions(deps.stdout, local)
		}
		return err
	}
	all := append([]ports.SessionInfo(nil), local...)
	var hostErrs []error
	for _, host := range hosts {
		if err := ctx.Err(); err != nil {
			printSessions(deps.stdout, all)
			return err
		}
		catalog, listErr := deps.catalog.List(ctx, host.Target)
		if listErr != nil {
			hostErrs = append(hostErrs, fmt.Errorf("host %s: %w", host.Target, listErr))
			continue
		}
		all = append(all, catalogSessionsAsInfo(host.Target, catalog.Sessions)...)
	}
	printSessions(deps.stdout, all)
	if len(hostErrs) > 0 {
		return errors.Join(hostErrs...)
	}
	return nil
}

func catalogSessionsAsInfo(host string, sessions []ports.RemoteCatalogSession) []ports.SessionInfo {
	out := make([]ports.SessionInfo, 0, len(sessions))
	for _, session := range sessions {
		info := ports.SessionInfo{
			Name:      session.Name + "@" + host,
			Ephemeral: session.Ephemeral,
			Tabs:      ports.SaturateUint16(ports.CatalogTabCount(session)),
			Attached:  session.Attached,
		}
		switch session.State {
		case "up":
			info.State = ports.SessionUp
		case "down":
			info.State = ports.SessionDown
		case "broken":
			info.State = ports.SessionBroken
		default:
			slog.Debug("remote catalog session has unknown state", "host", host, "session", session.Name, "state", session.State)
			info.State = ports.SessionBroken
		}
		out = append(out, info)
	}
	return out
}

func mergeKnownHosts(deps remoteHostDeps) ([]domain.RemoteHost, error) {
	pinned, learned, err := deps.hostStore().Hosts()
	if err != nil {
		return nil, fmt.Errorf("vev: reading hosts: %w", err)
	}
	return mergeRemoteHosts(pinned, learned), nil
}

// mergeRemoteHosts returns pinned hosts in stored order, then learned-only
// hosts in lexical order. Duplicates keep first occurrence and mark both sources.
func mergeRemoteHosts(pinned, learned []string) []domain.RemoteHost {
	pinned = domain.UniqueRemoteHostTargets(pinned)
	learnedSet := make(map[string]struct{}, len(learned))
	for _, target := range learned {
		learnedSet[target] = struct{}{}
	}
	pinnedSet := make(map[string]struct{}, len(pinned))
	out := make([]domain.RemoteHost, 0, len(pinned)+len(learned))
	for _, target := range pinned {
		pinnedSet[target] = struct{}{}
		_, isLearned := learnedSet[target]
		out = append(out, domain.RemoteHost{Target: target, Pinned: true, Learned: isLearned})
	}
	learnedOnly := make([]string, 0, len(learned))
	for _, target := range learned {
		if _, ok := pinnedSet[target]; !ok {
			learnedOnly = append(learnedOnly, target)
		}
	}
	learnedOnly = domain.UniqueRemoteHostTargets(learnedOnly)
	sort.Strings(learnedOnly)
	for _, target := range learnedOnly {
		out = append(out, domain.RemoteHost{Target: target, Learned: true})
	}
	return out
}

type remoteHostLearner struct {
	store  ports.RemoteHostStore
	target string
}

func (l remoteHostLearner) RememberRemoteHost() error {
	return l.store.Remember(l.target)
}

func attachRememberLearner(deps runAttachDeps, remoteTarget string, _ *slog.Logger) ports.RemoteHostLearner {
	if remoteTarget == "" {
		return nil
	}
	store := deps.hostStore
	if store == nil {
		stateDir := deps.stateDir
		if stateDir == nil {
			stateDir = platform.StateDir
		}
		store = remoteadapter.NewFileHostStore(remoteadapter.HostStorePath(stateDir()))
	}
	return remoteHostLearner{store: store, target: remoteTarget}
}
