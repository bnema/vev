package app

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

const (
	hostActionAdd  = "add"
	hostActionRm   = "rm"
	hostActionList = "list"
)

type remoteHostDeps struct {
	connect func(context.Context) (ports.BrokerService, error)
	stdout  io.Writer
}

func defaultRemoteHostDeps() remoteHostDeps {
	return remoteHostDeps{connect: connectProductionBroker, stdout: os.Stdout}
}

func (d remoteHostDeps) withDefaults() remoteHostDeps {
	if d.connect == nil {
		d.connect = connectProductionBroker
	}
	if d.stdout == nil {
		d.stdout = os.Stdout
	}
	return d
}

func runHostCommand(ctx context.Context, cmd command, deps remoteHostDeps) error {
	deps = deps.withDefaults()
	service, err := deps.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = service.Close() }()
	switch cmd.hostAction {
	case hostActionAdd:
		if err := domain.ValidateRemoteHostTarget(cmd.hostTarget); err != nil {
			return err
		}
		transport, err := remoteTransportModeFromEnv(os.Getenv(envRemoteTransport))
		if err != nil {
			return err
		}
		_, err = service.AddHost(ctx, cmd.hostTarget, remoteBrokerPolicy(transport))
		return err
	case hostActionRm:
		if err := domain.ValidateRemoteHostTarget(cmd.hostTarget); err != nil {
			return err
		}
		var registration domain.RemoteRegistration
		for _, daemon := range service.Snapshot().Daemons {
			if !daemon.Local && daemon.Endpoint == cmd.hostTarget {
				registration = daemon.Registration
				break
			}
		}
		if registration.Endpoint == "" {
			return fmt.Errorf("vev: unknown host %q", cmd.hostTarget)
		}
		removed, err := service.RemoveHost(ctx, registration)
		if err != nil {
			return err
		}
		if !removed {
			return fmt.Errorf("vev: unknown host %q", cmd.hostTarget)
		}
		return nil
	case hostActionList:
		return printBrokerHosts(deps.stdout, service.Snapshot())
	default:
		return usagef("unknown host action %q", cmd.hostAction)
	}
}

func printBrokerHosts(w io.Writer, snapshot ports.BrokerSnapshot) error {
	if err := snapshot.Validate(); err != nil {
		return fmt.Errorf("vev: reading broker catalogue: %w", err)
	}
	var endpoints []string
	for _, daemon := range snapshot.Daemons {
		if !daemon.Local {
			endpoints = append(endpoints, daemon.Endpoint)
		}
	}
	sort.Strings(endpoints)
	if len(endpoints) == 0 {
		_, err := fmt.Fprintln(w, "no hosts")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "TARGET\tSOURCE\tSTATUS")
	for _, endpoint := range endpoints {
		status := "configured"
		for _, daemon := range snapshot.Daemons {
			if !daemon.Local && daemon.Endpoint == endpoint && daemon.Availability == domain.RemoteAvailabilityNoDaemon {
				status = "no daemon"
				break
			}
		}
		_, _ = fmt.Fprintf(tw, "%s\tbroker\t%s\n", endpoint, status)
	}
	return tw.Flush()
}

func runBrokerSnapshotList(cmd command, snapshot ports.BrokerSnapshot, stdout io.Writer) error {
	if err := snapshot.Validate(); err != nil {
		return fmt.Errorf("vev: reading broker catalogue: %w", err)
	}
	var sessions []protocol.SessionInfo
	var statuses []string
	found := cmd.listHost == ""
	for _, daemon := range snapshot.Daemons {
		if cmd.listHost != "" && (daemon.Local || daemon.Endpoint != cmd.listHost) {
			continue
		}
		found = true
		if !daemon.Local && daemon.Availability == domain.RemoteAvailabilityNoDaemon {
			statuses = append(statuses, fmt.Sprintf("%s: no vev daemon — Enter in the picker to create a session", daemon.Endpoint))
		}
		for i, session := range catalogSessionsAsInfo(daemon.Endpoint, daemon.Sessions) {
			if daemon.Local {
				session.Name = daemon.Sessions[i].Name
			}
			sessions = append(sessions, session)
		}
	}
	if !found {
		return fmt.Errorf("vev: unknown host %q", cmd.listHost)
	}
	for _, status := range statuses {
		if _, err := fmt.Fprintln(stdout, status); err != nil {
			return err
		}
	}
	if len(sessions) == 0 && len(statuses) > 0 {
		return nil
	}
	printSessions(stdout, sessions)
	return nil
}

func catalogSessionsAsInfo(host string, sessions []catalogue.RemoteCatalogSession) []protocol.SessionInfo {
	out := make([]protocol.SessionInfo, 0, len(sessions))
	for _, session := range sessions {
		info := protocol.SessionInfo{Name: domain.RemoteSessionDisplay(session.Name, host), Ephemeral: session.Ephemeral, Tabs: catalogue.SaturateUint16(catalogue.CatalogTabCount(session)), Attached: session.Attached}
		switch session.State {
		case catalogue.RemoteCatalogSessionUp:
			info.State = protocol.SessionUp
		case catalogue.RemoteCatalogSessionDown:
			info.State = protocol.SessionDown
		default:
			info.State = protocol.SessionBroken
		}
		out = append(out, info)
	}
	return out
}
