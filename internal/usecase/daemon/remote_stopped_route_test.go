package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
)

func TestDaemonOwnedNoExactTargetRejectsNewAndEphemeral(t *testing.T) {
	for _, tt := range []struct {
		name   string
		intent uint8
	}{
		{name: "new", intent: ports.IntentNew},
		{name: "ephemeral", intent: ports.IntentEphemeral},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			hello := ports.Hello{
				Version: ports.ProtocolVersion, Intent: tt.intent, Name: "work",
				Size: domain.Size{Cols: 80, Rows: 24}, Cwd: "/untrusted/cwd",
				Env: []string{"UNTRUSTED=client"}, EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
			}
			_, _, err := d.route(hello, &closeTrackingTransport{})
			var protocolErr *protoErr
			require.ErrorAs(t, err, &protocolErr)
			require.Equal(t, ports.ErrNoSuchTarget, protocolErr.code)
			d.mu.Lock()
			sessions := len(d.sessions)
			d.mu.Unlock()
			require.Zero(t, sessions)
		})
	}
}

func TestDaemonOwnedStoppedAttachUsesDaemonEnvironment(t *testing.T) {
	d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	d.baseEnv = []string{"DAEMON=owned"}
	d.mu.Lock()
	d.inactive["work"] = inactiveSession{name: "work", cwd: "/remote/work", tabNames: []string{"main"}, state: ports.SessionDown}
	d.mu.Unlock()

	hello := ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work",
		Size: domain.Size{Cols: 80, Rows: 24}, Cwd: "/untrusted/cwd",
		Env: []string{"UNTRUSTED=client"}, EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}
	sess, ac, err := d.routeWithContext(context.Background(), hello, &closeTrackingTransport{})
	require.NoError(t, err)
	require.NotNil(t, ac)
	sess.mu.Lock()
	require.Equal(t, []string{"DAEMON=owned"}, sess.env)
	sess.mu.Unlock()
	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, true))
}

func TestRouteRemoteTargetRejectsLiveIntentForInactiveSession(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	lifecycle := remoteLifecycleForTest()
	d.mu.Lock()
	d.inactive["work"] = inactiveSession{
		name: "work", incarnation: lifecycle, state: ports.SessionDown,
		tabNames:   []string{"main"},
		tabRecords: []domain.CatalogueTabRecord{{StableID: "tab-1", Name: "main"}},
	}
	d.mu.Unlock()

	target := domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle,
		SessionName: "work", LiveTabID: "tab-1",
	}
	_, _, err := d.routeWithContext(context.Background(), ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work",
		Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: &target,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}, &closeTrackingTransport{})

	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, ports.ErrNoSuchTarget, protocolErr.code)
	d.mu.Lock()
	defer d.mu.Unlock()
	require.Empty(t, d.sessions)
	require.Contains(t, d.inactive, "work")
}

func TestRouteRemoteTargetMapsUnavailableInactiveStatesToNoSuchTarget(t *testing.T) {
	lifecycle := remoteLifecycleForTest()
	for _, test := range []struct {
		name  string
		entry inactiveSession
	}{
		{name: "broken", entry: inactiveSession{name: "work", incarnation: lifecycle, state: ports.SessionBroken}},
		{name: "degraded", entry: inactiveSession{name: "work", incarnation: lifecycle, state: ports.SessionDown, record: domain.CatalogueRecord{Name: "work", DegradedReason: "checkpoint unavailable"}}},
		{name: "purging", entry: inactiveSession{name: "work", incarnation: lifecycle, state: ports.SessionDown, purging: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			d.inactive["work"] = test.entry
			target := domain.RemoteSessionTarget{
				Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle,
				SessionName: "work", Stopped: true,
			}
			_, _, err := d.routeWithContext(context.Background(), ports.Hello{
				Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work",
				Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: &target,
				EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
			}, &closeTrackingTransport{})

			var protocolErr *protoErr
			require.ErrorAs(t, err, &protocolErr)
			require.Equal(t, ports.ErrNoSuchTarget, protocolErr.code)
		})
	}
}

func TestRouteRemoteTargetMapsConcurrentInactiveResumeToNoSuchTarget(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	lifecycle := remoteLifecycleForTest()
	d.inactive["work"] = inactiveSession{name: "work", incarnation: lifecycle, state: ports.SessionDown}
	d.creating["work"] = struct{}{}
	target := domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle,
		SessionName: "work", Stopped: true,
	}

	_, _, err := d.routeWithContext(context.Background(), ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work",
		Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: &target,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}, &closeTrackingTransport{})

	var protocolErr *protoErr
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, ports.ErrNoSuchTarget, protocolErr.code)
}

func TestRouteRemoteTargetPreservesInactiveResumeFailures(t *testing.T) {
	newTarget := func(record domain.CatalogueRecord) domain.RemoteSessionTarget {
		return domain.RemoteSessionTarget{
			Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: record.IncarnationID,
			SessionName: record.Name, Stopped: true,
		}
	}
	route := func(d *Daemon, target domain.RemoteSessionTarget) error {
		_, _, err := d.routeWithContext(context.Background(), ports.Hello{
			Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: target.SessionName,
			Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: &target,
			EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
		}, &closeTrackingTransport{})
		return err
	}

	t.Run("PTY startup", func(t *testing.T) {
		cause := errors.New("PTY unavailable")
		factory := portsmocks.NewMockPTYFactory(t)
		factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, cause).Once()
		d := newTestDaemon(t, factory, stubClock{})
		record := durableRecoveryRecord(1)
		record.Committed = nil
		record.TabNames = nil
		record.TabRecords = nil
		d.inactive[record.Name] = inactiveSessionFromRecord(record, ports.SessionDown, nil)

		err := route(d, newTarget(record))
		require.ErrorIs(t, err, cause)
		var protocolErr *protoErr
		require.False(t, errors.As(err, &protocolErr))
	})

	t.Run("catalogue read", func(t *testing.T) {
		cause := errors.New("catalogue unavailable")
		record := durableRecoveryRecord(1)
		record.Committed = nil
		record.TabNames = nil
		record.TabRecords = nil
		catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
		d.catalogue = recordErrorCatalogue{durableRecoveryCatalogue: catalogue, err: cause}
		d.persistEnabled = true
		d.recovery = recoveryusecase.NewCoordinator(d.catalogue, noOpSnapshotRepository{}, nil)
		d.inactive[record.Name] = inactiveSessionFromRecord(record, ports.SessionDown, nil)

		err := route(d, newTarget(record))
		require.ErrorIs(t, err, cause)
		var protocolErr *protoErr
		require.False(t, errors.As(err, &protocolErr))
	})
}

func TestRouteRemoteTargetResumesCanonicalPersistedTabMetadata(t *testing.T) {
	for _, test := range []struct {
		name       string
		tabNames   []string
		tabRecords []domain.CatalogueTabRecord
		selector   domain.TabSelector
	}{
		{
			name:       "tab records without compatibility names",
			tabRecords: []domain.CatalogueTabRecord{{StableID: "tab-1"}},
			selector:   domain.NewStableTabSelector("tab-1"),
		},
		{
			name:     "tab names without stable records",
			tabNames: []string{"main"},
			selector: domain.NewOrdinalTabSelector(0, "main", 1),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := durableRecoveryRecord(1)
			record.Committed = nil
			record.TabNames = append([]string(nil), test.tabNames...)
			record.TabRecords = append([]domain.CatalogueTabRecord(nil), test.tabRecords...)
			catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})
			d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
			d.catalogue = catalogue
			d.persistEnabled = true
			d.recovery = recoveryusecase.NewCoordinator(catalogue, noOpSnapshotRepository{}, nil)
			d.inactive[record.Name] = inactiveSessionFromRecord(record, ports.SessionDown, nil)

			target := domain.RemoteSessionTarget{
				Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: record.IncarnationID,
				SessionName: record.Name, Stopped: true, StoppedTab: test.selector,
			}
			transport, _ := newCapturingTransport(t)
			sess, attachment, err := d.routeWithContext(context.Background(), ports.Hello{
				Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: record.Name,
				Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: &target,
				EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
			}, transport)
			require.NoError(t, err)
			t.Cleanup(func() { d.clientGone(sess, attachment, transport, false) })
			require.Len(t, sess.tabs, 1)
			require.Equal(t, sess.tabs[0].stableID, string(attachment.viewSnapshot().tabID))
		})
	}
}

func TestRouteRemoteTargetRestoresStoppedStableTabAndOwnsEnvironment(t *testing.T) {
	d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	lifecycle := remoteLifecycleForTest()
	d.mu.Lock()
	d.inactive["work"] = inactiveSession{
		name: "work", cwd: "/remote/work", incarnation: lifecycle, state: ports.SessionDown,
		tabNames: []string{"alpha", "beta"},
		tabRecords: []domain.CatalogueTabRecord{
			{StableID: "tab-a", Name: "alpha"},
			{StableID: "tab-b", Name: "beta"},
		},
	}
	d.mu.Unlock()

	target := domain.RemoteSessionTarget{
		Endpoint: "arch", DisplayOrigin: "arch", LifecycleID: lifecycle,
		SessionName: "work", Stopped: true,
		StoppedTab: domain.NewStableTabSelector("tab-b"),
	}
	tr, _ := newCapturingTransport(t)
	hello := ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work",
		Size: domain.Size{Cols: 80, Rows: 24}, RemoteTarget: &target,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
		Env:               []string{"VEV_REMOTE_PICKER_SENTINEL=untrusted"},
	}
	sess, ac, err := d.routeWithContext(context.Background(), hello, tr)
	require.NoError(t, err)
	require.NotNil(t, sess)
	require.NotNil(t, ac)
	require.Equal(t, lifecycle, sess.incarnation)
	require.Equal(t, domain.TabStableID("tab-b"), ac.viewSnapshot().tabID)
	require.NotContains(t, sess.env, "VEV_REMOTE_PICKER_SENTINEL=untrusted")
	d.clientGone(sess, ac, tr, false)
}
