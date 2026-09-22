package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

func previewTestSession(name string, seed byte, tab string) catalogue.RemoteCatalogSession {
	session := pickerTestSession(name, seed, catalogue_Up)
	session.Tabs = []catalogue.RemoteCatalogTab{{ID: tab, Name: tab}}
	session.ActiveTabID = tab
	return session
}

func previewTestController(t *testing.T, observations ...ports.BrokerDaemonObservation) *pickerController {
	t.Helper()
	controller, clock := pickerTestController(t)
	controller.ApplySnapshot(ports.BrokerSnapshot{Epoch: 7, Revision: 1, Daemons: observations})
	require.NotEmpty(t, controller.Render(domain.Size{Cols: 100, Rows: 40}))
	_ = clock
	return controller
}

func expectPreviewSubscription(t *testing.T, service *portsmocks.MockBrokerService, connection ports.BrokerConnectionID, stream ports.BrokerStreamID, expected ports.BrokerPreviewRequest) *portsmocks.MockBrokerPreviewSubscription {
	t.Helper()
	sub := portsmocks.NewMockBrokerPreviewSubscription(t)
	service.EXPECT().ConnectionID().Return(connection).Times(3)
	service.EXPECT().NextStreamID().Return(stream, nil).Once()
	service.EXPECT().SubscribePreview(expected).Return(sub, nil).Once()
	return sub
}

func expectedPreviewRequest(t *testing.T, controller *pickerController, connection ports.BrokerConnectionID, stream ports.BrokerStreamID, generation ports.BrokerPreviewGeneration, size domain.Size) ports.BrokerPreviewRequest {
	t.Helper()
	route, preview, ok := controller.PreviewRequest(connection, stream, size)
	if !ok {
		key, selected := controller.CursorKey()
		t.Fatalf("preview request refused: selected=%v key=%q connection=%v stream=%v size=%+v", selected, key, connection, stream, size)
	}
	return ports.BrokerPreviewRequest{Epoch: route.Epoch, Connection: connection, Generation: generation, Route: route, Preview: preview}
}

func TestPreviewManagerInitialSelectionUsesExactLocalOrRemoteRequestAndDimensions(t *testing.T) {
	now := time.Unix(100, 0)
	cases := []struct {
		name         string
		observations []ports.BrokerDaemonObservation
		wantEndpoint string
	}{
		{name: "local", observations: []ports.BrokerDaemonObservation{pickerTestLocalObservation(now, previewTestSession("local-session", 1, "tab-local"))}, wantEndpoint: "local"},
		{name: "remote", observations: []ports.BrokerDaemonObservation{pickerTestRemoteObservation("ssh://example.test", 2, 3, now, previewTestSession("remote-session", 4, "tab-remote"))}, wantEndpoint: "ssh://example.test"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			controller := previewTestController(t, tc.observations...)
			service := portsmocks.NewMockBrokerService(t)
			connection := ports.BrokerConnectionID{11}
			size := domain.Size{Cols: 100, Rows: 40}
			expected := expectedPreviewRequest(t, controller, connection, 21, 2, size)
			require.Equal(t, tc.wantEndpoint, expected.Preview.Target.Endpoint)
			require.Equal(t, pickerPreviewSize(size), domain.Size{Cols: int(expected.Preview.Width), Rows: int(expected.Preview.Height)})
			expectPreviewSubscription(t, service, connection, 21, expected)

			var manager previewManager
			manager.refresh(service, controller, size)
			require.Equal(t, expected, manager.request)
		})
	}
}

func TestPreviewManagerSelectionAndResizeReplaceAndCloseOld(t *testing.T) {
	now := time.Unix(100, 0)
	controller := previewTestController(t, pickerTestLocalObservation(now,
		previewTestSession("alpha", 1, "tab-alpha"),
		previewTestSession("beta", 2, "tab-beta"),
	))
	service := portsmocks.NewMockBrokerService(t)
	connection := ports.BrokerConnectionID{11}
	size := domain.Size{Cols: 100, Rows: 40}

	firstExpected := expectedPreviewRequest(t, controller, connection, 21, 2, size)
	first := expectPreviewSubscription(t, service, connection, 21, firstExpected)
	first.EXPECT().Close().Once()
	var manager previewManager
	manager.refresh(service, controller, size)

	require.True(t, controller.ConsumeTerminalRead([]byte("j")))
	secondExpected := expectedPreviewRequest(t, controller, connection, 22, 4, size)
	second := expectPreviewSubscription(t, service, connection, 22, secondExpected)
	second.EXPECT().Close().Once()
	manager.refresh(service, controller, size)
	require.NotEqual(t, firstExpected.Preview.Target, secondExpected.Preview.Target)

	resized := domain.Size{Cols: 120, Rows: 50}
	thirdExpected := expectedPreviewRequest(t, controller, connection, 23, 6, resized)
	third := expectPreviewSubscription(t, service, connection, 23, thirdExpected)
	manager.refresh(service, controller, resized)
	require.NotEqual(t, secondExpected.Preview.Width, thirdExpected.Preview.Width)
	require.NotEqual(t, secondExpected.Preview.Height, thirdExpected.Preview.Height)

	third.EXPECT().Close().Once()
	manager.close(controller)
}

func TestPreviewManagerIgnoresOldLatePublicationAfterReplacement(t *testing.T) {
	now := time.Unix(100, 0)
	controller := previewTestController(t, pickerTestLocalObservation(now,
		previewTestSession("alpha", 1, "tab-alpha"),
		previewTestSession("beta", 2, "tab-beta"),
	))
	service := portsmocks.NewMockBrokerService(t)
	connection := ports.BrokerConnectionID{11}
	size := domain.Size{Cols: 100, Rows: 40}
	firstExpected := expectedPreviewRequest(t, controller, connection, 21, 2, size)
	first := expectPreviewSubscription(t, service, connection, 21, firstExpected)
	first.EXPECT().Close().Once()
	var manager previewManager
	manager.refresh(service, controller, size)

	require.True(t, controller.ConsumeTerminalRead([]byte("j")))
	secondExpected := expectedPreviewRequest(t, controller, connection, 22, 4, size)
	second := expectPreviewSubscription(t, service, connection, 22, secondExpected)
	manager.refresh(service, controller, size)

	// A wake already selected from the retired subscription is fenced by all
	// repeated authority fields and must not replace the current preview.
	late := ports.BrokerPreviewPublication{Epoch: firstExpected.Epoch, Connection: connection, Generation: firstExpected.Generation, Target: firstExpected.Preview.Target, Width: firstExpected.Preview.Width, Height: firstExpected.Preview.Height, Preview: protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewUnavailable}}
	second.EXPECT().Latest().Return(late).Once()
	require.False(t, manager.publish(controller))

	second.EXPECT().Close().Once()
	manager.close(controller)
}

func TestSupervisorRefreshPreviewWiresTerminalGeometryToPreviewManager(t *testing.T) {
	now := time.Unix(100, 0)
	controller := previewTestController(t, pickerTestLocalObservation(now, previewTestSession("alpha", 1, "tab-alpha")))
	service := portsmocks.NewMockBrokerService(t)
	connection := ports.BrokerConnectionID{11}
	size := domain.Size{Cols: 80, Rows: 24}
	expected := expectedPreviewRequest(t, controller, connection, 21, 2, size)
	sub := expectPreviewSubscription(t, service, connection, 21, expected)

	supervisor := &Supervisor{cfg: SupervisorConfig{Picker: controller, Terminal: &pickerRecordingTerminal{}}}
	supervisor.refreshPreview(service)
	require.Equal(t, expected, supervisor.preview.request)

	sub.EXPECT().Close().Once()
	supervisor.preview.close(controller)
}

func TestPreviewManagerBrokerRetirementAndCommitClearAndClose(t *testing.T) {
	for _, reason := range []string{"broker retirement", "commit"} {
		t.Run(reason, func(t *testing.T) {
			now := time.Unix(100, 0)
			controller := previewTestController(t, pickerTestLocalObservation(now, previewTestSession("alpha", 1, "tab-alpha")))
			service := portsmocks.NewMockBrokerService(t)
			connection := ports.BrokerConnectionID{11}
			size := domain.Size{Cols: 100, Rows: 40}
			expected := expectedPreviewRequest(t, controller, connection, 21, 2, size)
			sub := expectPreviewSubscription(t, service, connection, 21, expected)
			sub.EXPECT().Close().Once()
			var manager previewManager
			manager.refresh(service, controller, size)
			manager.close(controller)
			require.Nil(t, manager.sub)
			require.Equal(t, ports.BrokerPreviewRequest{}, manager.request)
		})
	}
}
