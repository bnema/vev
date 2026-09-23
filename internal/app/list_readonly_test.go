package app

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

func TestObservedHostStatus(t *testing.T) {
	now := time.Date(2026, 9, 23, 9, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name         string
		availability domain.RemoteAvailability
		age          time.Duration
		want         string
		show         bool
	}{
		{"fresh", domain.RemoteAvailabilityReachable, time.Second, "reachable", true},
		{"stale", domain.RemoteAvailabilityReachable, 2 * time.Minute, "stale (2m0s since last observation)", false},
		{"unknown success", domain.RemoteAvailabilityReachable, 0, "stale (last observation unknown)", false},
		{"never observed", domain.RemoteAvailabilityUnknown, time.Second, "not yet observed", false},
		{"down", domain.RemoteAvailabilityUnreachable, time.Second, "down (unreachable)", false},
		{"auth", domain.RemoteAvailabilityAuthFailed, time.Second, "down (authentication_failed)", false},
		{"invalid", domain.RemoteAvailabilityInvalidResponse, time.Second, "down (invalid_response)", false},
		{"incompatible", domain.RemoteAvailabilityIncompatible, time.Second, "incompatible (identity changed? use vev host rm/add)", false},
		{"no daemon", domain.RemoteAvailabilityNoDaemon, time.Second, "no vev daemon — Enter in the picker to create a session", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := ports.BrokerDaemonObservation{Availability: tc.availability}
			if tc.age != 0 {
				observed.LastSuccess = now.Add(-tc.age)
			}
			status, show := observedHostStatus(observed, now)
			require.Equal(t, tc.want, status)
			require.Equal(t, tc.show, show)
		})
	}
}

func TestSnapshotListNeverShowsCachedUpWhenHostIsNotFresh(t *testing.T) {
	now := time.Date(2026, 9, 23, 9, 0, 0, 0, time.UTC)
	for _, availability := range []domain.RemoteAvailability{domain.RemoteAvailabilityReachable, domain.RemoteAvailabilityUnreachable, domain.RemoteAvailabilityNoDaemon} {
		t.Run(availability.String(), func(t *testing.T) {
			remote := ports.BrokerDaemonObservation{
				Endpoint: "user@example.test", DisplayOrigin: "user@example.test",
				Registration: domain.RemoteRegistration{Endpoint: "user@example.test", Incarnation: [16]byte{1}, Generation: 1},
				Policy:       seamBrokerTestPolicy(), Availability: availability,
				LastSuccess: now.Add(-time.Minute), InventoryKnown: true,
				Sessions: []catalogue.RemoteCatalogSession{{LifecycleID: domain.SessionLifecycleID{1}, Name: "old", State: catalogue.RemoteCatalogSessionUp}},
			}
			snapshot := ports.BrokerSnapshot{Epoch: seamBrokerTestEpoch, Revision: 1, Daemons: []ports.BrokerDaemonObservation{remote}}
			var output bytes.Buffer
			require.NoError(t, renderBrokerSnapshotList(command{listAll: true}, snapshot, &output, now))
			require.NotContains(t, output.String(), "old@")
			require.NotContains(t, output.String(), " up")
		})
	}
}

func TestRunListWithoutBrokerDoesNotStartEitherProcess(t *testing.T) {
	previous := connectListBroker
	connectListBroker = func(context.Context) (ports.BrokerService, error) { return nil, errBrokerAbsent }
	t.Cleanup(func() { connectListBroker = previous })
	root := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(root, "runtime"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(root, "state"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(root, "config"))
	for _, tc := range []struct {
		name string
		cmd  command
		want string
	}{
		{"local", command{kind: kindList}, "no sessions"},
		{"remote", command{kind: kindList, listHost: "user@example.test"}, "not connected (no broker running)"},
		{"all", command{kind: kindList, listAll: true}, "no hosts"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := captureStdout(t, func() { require.NoError(t, runList(context.Background(), tc.cmd)) })
			require.Contains(t, output, tc.want)
		})
	}
	_, err := os.Stat(filepath.Join(root, "runtime", "vev"))
	require.True(t, errors.Is(err, os.ErrNotExist))
}

func TestHostListUsesExistingConnector(t *testing.T) {
	var output bytes.Buffer
	called := false
	deps := remoteHostDeps{
		connect:         func(context.Context) (ports.BrokerService, error) { t.Fatal("list spawned a broker"); return nil, nil },
		connectExisting: func(context.Context) (ports.BrokerService, error) { called = true; return nil, errBrokerAbsent },
		stdout:          &output,
	}
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	require.NoError(t, runHostCommand(context.Background(), command{hostAction: hostActionList}, deps))
	require.True(t, called)
	require.Equal(t, "no hosts\n", output.String())
}
