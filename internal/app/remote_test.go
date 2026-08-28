package app

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestParseRemoteAttachTarget(t *testing.T) {
	tests := []struct {
		name, input, target, session string
		ok                           bool
	}{
		{name: "local", input: "work"},
		{name: "remote", input: "user@example.com", target: "user@example.com", ok: true},
		{name: "remote session", input: "user@example.com:work", target: "user@example.com", session: "work", ok: true},
		{name: "empty session is ephemeral", input: "user@example.com:", target: "user@example.com", ok: true},
		{name: "ipv6", input: "user@[2001:db8::1]:work", target: "user@[2001:db8::1]", session: "work", ok: true},
		{name: "missing user", input: "@example.com"},
		{name: "missing host", input: "user@"},
		{name: "multiple user separators", input: "user@example@com"},
		{name: "missing bracketed ipv6 host", input: "user@[]:work"},
		{name: "unclosed ipv6", input: "user@[2001:db8::1:work"},
		{name: "unexpected closing ipv6 bracket", input: "user@2001:db8::1]:work"},
		{name: "unbracketed ipv6", input: "user@2001:db8::1:work"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, session, ok := parseRemoteAttachTarget(tt.input)
			require.Equal(t, tt.target, target)
			require.Equal(t, tt.session, session)
			require.Equal(t, tt.ok, ok)
		})
	}
}

func TestRemoteHostDepsDefaultsStore(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	deps := (remoteHostDeps{stateDir: func() string { return stateDir }}).withDefaults()
	require.NotNil(t, deps.store)
	require.Same(t, deps.store, deps.hostStore())
	require.NoError(t, deps.store.AddPinned("arch"))
	require.FileExists(t, filepath.Join(stateDir, "hosts.json"))
}

func TestDecodeSessionListErrorReply(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		_, err := decodeSessionListReply(ports.Frame{
			Type:    ports.MsgError,
			Payload: ports.MarshalErrorMsg(protocol.ErrorMsg{Text: "denied"}),
		})
		require.EqualError(t, err, "vev: denied")
	})

	t.Run("malformed", func(t *testing.T) {
		_, err := decodeSessionListReply(ports.Frame{Type: ports.MsgError, Payload: []byte{0}})
		require.ErrorContains(t, err, "vev: decoding error reply")
	})
}

func TestMergeRemoteHostsOrderAndSource(t *testing.T) {
	got := mergeRemoteHosts([]string{"zebra", "arch"}, []string{"mule", "arch", "beta"})
	require.Equal(t, []domain.RemoteHost{
		{Target: "zebra", Pinned: true},
		{Target: "arch", Pinned: true, Learned: true},
		{Target: "beta", Learned: true},
		{Target: "mule", Learned: true},
	}, got)
}

func TestRemoteHostCommands(t *testing.T) {
	newDeps := func(store ports.RemoteHostStore, out *bytes.Buffer) remoteHostDeps {
		return remoteHostDeps{
			store:  store,
			stdout: out,
		}
	}

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "add pins in state store",
			run: func(t *testing.T) {
				store := portsmocks.NewMockRemoteHostStore(t)
				store.EXPECT().AddPinned("mule").Return(nil).Once()
				err := runHostCommand(context.Background(), command{hostAction: hostActionAdd, hostTarget: "mule"}, newDeps(store, nil))
				require.NoError(t, err)
			},
		},
		{
			name: "remove known host uses one atomic store call",
			run: func(t *testing.T) {
				store := portsmocks.NewMockRemoteHostStore(t)
				store.EXPECT().Remove("mule").Return(true, nil).Once()
				err := runHostCommand(context.Background(), command{hostAction: hostActionRm, hostTarget: "mule"}, newDeps(store, nil))
				require.NoError(t, err)
			},
		},
		{
			name: "remove unknown host uses one atomic store call",
			run: func(t *testing.T) {
				store := portsmocks.NewMockRemoteHostStore(t)
				store.EXPECT().Remove("mule").Return(false, nil).Once()
				err := runHostCommand(context.Background(), command{hostAction: hostActionRm, hostTarget: "mule"}, newDeps(store, nil))
				require.ErrorContains(t, err, `unknown host "mule"`)
			},
		},
		{
			name: "remove atomic store error is surfaced",
			run: func(t *testing.T) {
				store := portsmocks.NewMockRemoteHostStore(t)
				removeErr := errors.New("rename hosts")
				store.EXPECT().Remove("mule").Return(false, removeErr).Once()
				err := runHostCommand(context.Background(), command{hostAction: hostActionRm, hostTarget: "mule"}, newDeps(store, nil))
				require.ErrorIs(t, err, removeErr)
			},
		},
		{
			name: "list preserves source markers and order",
			run: func(t *testing.T) {
				store := portsmocks.NewMockRemoteHostStore(t)
				store.EXPECT().Hosts().Return([]string{"zebra", "arch"}, []string{"mule", "arch", "beta"}, nil).Once()
				var out bytes.Buffer
				err := runHostCommand(context.Background(), command{hostAction: hostActionList}, newDeps(store, &out))
				require.NoError(t, err)
				got := out.String()
				require.Contains(t, got, "pinned,learned")
				require.True(t, strings.Index(got, "zebra") < strings.Index(got, "arch"))
				require.True(t, strings.Index(got, "arch") < strings.Index(got, "beta"))
				require.True(t, strings.Index(got, "beta") < strings.Index(got, "mule"))
			},
		},
		{
			name: "malformed store is surfaced",
			run: func(t *testing.T) {
				store := portsmocks.NewMockRemoteHostStore(t)
				store.EXPECT().Hosts().Return(nil, nil, errors.New("malformed hosts file")).Once()
				err := runHostCommand(context.Background(), command{hostAction: hostActionList}, newDeps(store, nil))
				require.ErrorContains(t, err, "malformed hosts file")
			},
		},
		{
			name: "list is active without config",
			run: func(t *testing.T) {
				store := portsmocks.NewMockRemoteHostStore(t)
				store.EXPECT().Hosts().Return(nil, nil, nil).Once()
				var out bytes.Buffer
				err := runHostCommand(context.Background(), command{hostAction: hostActionList}, newDeps(store, &out))
				require.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestRemoteHostListingUsesUnifiedStore(t *testing.T) {
	store := portsmocks.NewMockRemoteHostStore(t)
	store.EXPECT().Hosts().Return([]string{"arch"}, []string{"beta"}, nil).Once()
	catalog := portsmocks.NewMockRemoteCatalogClient(t)
	catalog.EXPECT().List(mock.Anything, "arch").Return(ports.RemoteCatalog{Sessions: []ports.RemoteCatalogSession{{Name: "build", State: "up"}}}, nil).Once()
	var out bytes.Buffer
	err := runRemoteList(context.Background(), command{listHost: "arch"}, remoteHostDeps{
		store:   store,
		catalog: catalog,
		stdout:  &out,
	})
	require.NoError(t, err)
	require.Contains(t, out.String(), "build@arch")
}

func TestListAllSessionsInvariants(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "prints partial local results on error",
			run: func(t *testing.T) {
				localErr := errors.New("local release failed")
				var out bytes.Buffer
				err := listAllSessions(context.Background(), remoteHostDeps{
					localList: func(context.Context) ([]protocol.SessionInfo, error) {
						return []protocol.SessionInfo{{Name: "local", State: protocol.SessionUp}}, localErr
					},
					stdout: &out,
				}, nil)
				require.ErrorIs(t, err, localErr)
				require.Contains(t, out.String(), "local")
			},
		},
		{
			name: "prints accumulated results and stops before next host on cancellation",
			run: func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				catalog := portsmocks.NewMockRemoteCatalogClient(t)
				catalog.EXPECT().List(mock.Anything, "arch").RunAndReturn(func(context.Context, string) (ports.RemoteCatalog, error) {
					cancel()
					return ports.RemoteCatalog{Sessions: []ports.RemoteCatalogSession{{Name: "dev", State: "up"}}}, nil
				}).Once()
				var out bytes.Buffer
				err := listAllSessions(ctx, remoteHostDeps{
					catalog: catalog,
					localList: func(context.Context) ([]protocol.SessionInfo, error) {
						return []protocol.SessionInfo{{Name: "local", State: protocol.SessionUp}}, nil
					},
					stdout: &out,
				}, []domain.RemoteHost{{Target: "arch"}, {Target: "mule"}})
				require.ErrorIs(t, err, context.Canceled)
				require.Contains(t, out.String(), "local")
				require.Contains(t, out.String(), "dev@arch")
				require.NotContains(t, out.String(), "@mule")
			},
		},
		{
			name: "lists hosts sequentially in supplied order",
			run: func(t *testing.T) {
				catalog := portsmocks.NewMockRemoteCatalogClient(t)
				var calls []string
				catalog.EXPECT().List(mock.Anything, "arch").Run(func(_ context.Context, target string) {
					calls = append(calls, target)
				}).Return(ports.RemoteCatalog{Sessions: []ports.RemoteCatalogSession{{Name: "dev", State: "up"}}}, nil).Once()
				catalog.EXPECT().List(mock.Anything, "mule").Run(func(_ context.Context, target string) {
					calls = append(calls, target)
				}).Return(ports.RemoteCatalog{Sessions: []ports.RemoteCatalogSession{{Name: "ops", State: "up"}}}, nil).Once()
				var out bytes.Buffer
				err := listAllSessions(context.Background(), remoteHostDeps{
					catalog: catalog,
					localList: func(context.Context) ([]protocol.SessionInfo, error) {
						return []protocol.SessionInfo{{Name: "local", State: protocol.SessionUp}}, nil
					},
					stdout: &out,
				}, []domain.RemoteHost{{Target: "arch"}, {Target: "mule"}})
				require.NoError(t, err)
				require.Equal(t, []string{"arch", "mule"}, calls)
				got := out.String()
				require.Less(t, strings.Index(got, "local"), strings.Index(got, "dev@arch"))
				require.Less(t, strings.Index(got, "dev@arch"), strings.Index(got, "ops@mule"))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestCatalogSessionsAsInfoInvariants(t *testing.T) {
	tests := []struct {
		name      string
		session   ports.RemoteCatalogSession
		wantState protocol.SessionState
		wantTabs  uint16
	}{
		{name: "up", session: ports.RemoteCatalogSession{Name: "dev", State: "up", Tabs: []ports.RemoteCatalogTab{{}, {}}}, wantState: protocol.SessionUp, wantTabs: 2},
		{name: "down", session: ports.RemoteCatalogSession{Name: "dev", State: "down", Tabs: []ports.RemoteCatalogTab{{}, {}}}, wantState: protocol.SessionDown, wantTabs: 2},
		{name: "broken", session: ports.RemoteCatalogSession{Name: "dev", State: "broken", Tabs: []ports.RemoteCatalogTab{{}, {}}}, wantState: protocol.SessionBroken, wantTabs: 2},
		{name: "unknown fails closed", session: ports.RemoteCatalogSession{Name: "dev", State: "unknown", Tabs: []ports.RemoteCatalogTab{{}, {}}}, wantState: protocol.SessionBroken, wantTabs: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			infos := catalogSessionsAsInfo("user@arch", []ports.RemoteCatalogSession{tt.session})
			require.Len(t, infos, 1)
			require.Equal(t, "dev@arch", infos[0].Name)
			require.Equal(t, tt.wantState, infos[0].State)
			require.Equal(t, tt.wantTabs, infos[0].Tabs)
		})
	}
}

func TestRunAttachWithDepsRemoteLearning(t *testing.T) {
	store := portsmocks.NewMockRemoteHostStore(t)
	store.EXPECT().Remember("build@mule").Return(nil).Once()
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	factory.EXPECT().DialerForRemote("build@mule", "work", ports.RemoteTransportUDP, mock.Anything).Return(namedDialer{name: "remote"}, nil).Once()

	var learner ports.RemoteHostLearner
	err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "build@mule", "", nil, runAttachDeps{
		remoteDialerFactory: factory,
		hostStore:           store,
		runClient: func(_ context.Context, deps client.Dependencies, _ client.AttachRequest) error {
			learner = deps.RemoteHostLearner
			return nil
		},
	})
	require.NoError(t, err)
	require.NotNil(t, learner)
	require.NoError(t, learner.RememberRemoteHost())
}

func TestRunAttachWithDepsAlwaysLearnsRemoteHost(t *testing.T) {
	store := portsmocks.NewMockRemoteHostStore(t)
	store.EXPECT().Remember("arch").Return(nil).Once()
	factory := portsmocks.NewMockRemoteDialerFactory(t)
	factory.EXPECT().DialerForRemote("arch", "work", ports.RemoteTransportUDP, mock.Anything).Return(namedDialer{name: "remote"}, nil).Once()
	var learner ports.RemoteHostLearner
	err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "work", "arch", "", nil, runAttachDeps{
		remoteDialerFactory: factory,
		hostStore:           store,
		runClient: func(_ context.Context, deps client.Dependencies, _ client.AttachRequest) error {
			learner = deps.RemoteHostLearner
			return nil
		},
	})
	require.NoError(t, err)
	require.NotNil(t, learner)
	require.NoError(t, learner.RememberRemoteHost())
}
