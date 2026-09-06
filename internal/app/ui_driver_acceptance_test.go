//go:build linux

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/uiterm"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/stretchr/testify/require"
)

type driverTestEnvelope struct {
	Version int             `json:"version"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code     ports.UIErrorCode `json:"code"`
		Accepted bool              `json:"accepted"`
		ActionID uint64            `json:"action_id,omitempty"`
	} `json:"error,omitempty"`
}

func TestHeadlessDriverUsesRealRunnerAndDaemon(t *testing.T) {
	dir, _ := startDaemon(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clk := clock.New()
	terminal, err := uiterm.New(ctx, domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, "")
	require.NoError(t, err)
	defer terminal.Close()
	ui := client.NewUI(terminal, clk)
	deps := runAttachDeps{
		localDialer:         func() wire.Dialer { return localDaemonDialer{dir: dir} },
		remoteDialerFactory: defaultRemoteDialerFactory(),
		runClient:           runClientWithDeps,
		createDetached:      createDetachedLocalSession,
		ui:                  ui,
		terminal:            func() ports.Terminal { return terminal },
		clock:               func() ports.Clock { return clk },
		stateDir:            func() string { return dir },
	}
	clientSide, serverSide := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- runHeadlessClientWithStream(ctx, protocol.IntentEphemeral, "", "", discardLog(), deps, ui, terminal, serverSide)
	}()
	defer clientSide.Close()
	decoder := json.NewDecoder(clientSide)
	ready := readDriverTestEnvelope(t, decoder)
	require.Equal(t, uint64(0), ready.ID)
	var readyResult struct {
		Attachment string `json:"attachment"`
		Generation uint64 `json:"generation"`
	}
	require.NoError(t, json.Unmarshal(ready.Result, &readyResult))
	require.NotEmpty(t, readyResult.Attachment)
	require.Equal(t, uint64(1), readyResult.Generation)

	writeDriverTestRequest(t, clientSide, map[string]any{
		"version": 1, "id": 1, "op": "keys", "attachment": readyResult.Attachment,
		"generation": readyResult.Generation, "keys": []string{"Alt+Space"},
	})
	paletteAction := readDriverTestEnvelope(t, decoder)
	require.Nil(t, paletteAction.Error)
	var paletteResult struct {
		ActionID uint64 `json:"action_id"`
	}
	require.NoError(t, json.Unmarshal(paletteAction.Result, &paletteResult))
	require.NotZero(t, paletteResult.ActionID)
	writeDriverTestRequest(t, clientSide, map[string]any{
		"version": 1, "id": 2, "op": "wait", "attachment": readyResult.Attachment,
		"after_action": paletteResult.ActionID, "expect": map[string]string{"text_contains": "Commands"},
	})
	paletteWait := readDriverTestEnvelope(t, decoder)
	require.Nil(t, paletteWait.Error)
	writeDriverTestRequest(t, clientSide, map[string]any{
		"version": 1, "id": 3, "op": "keys", "attachment": readyResult.Attachment,
		"generation": readyResult.Generation, "keys": []string{"Escape"},
	})
	paletteClose := readDriverTestEnvelope(t, decoder)
	require.Nil(t, paletteClose.Error)

	writeDriverTestRequest(t, clientSide, map[string]any{
		"version": 1, "id": 4, "op": "text", "attachment": readyResult.Attachment,
		"generation": readyResult.Generation, "text": "printf 'HEADLESS_%s' OK",
	})
	textAction := readDriverTestEnvelope(t, decoder)
	require.Nil(t, textAction.Error)
	var actionResult struct {
		ActionID uint64               `json:"action_id"`
		Status   ports.UIActionStatus `json:"status"`
	}
	require.NoError(t, json.Unmarshal(textAction.Result, &actionResult))
	require.Equal(t, ports.UIActionProcessed, actionResult.Status)

	writeDriverTestRequest(t, clientSide, map[string]any{
		"version": 1, "id": 5, "op": "keys", "attachment": readyResult.Attachment,
		"generation": readyResult.Generation, "keys": []string{"Enter"},
	})
	enterAction := readDriverTestEnvelope(t, decoder)
	require.Nil(t, enterAction.Error)
	var enterResult struct {
		ActionID uint64 `json:"action_id"`
	}
	require.NoError(t, json.Unmarshal(enterAction.Result, &enterResult))
	require.NotZero(t, enterResult.ActionID)

	writeDriverTestRequest(t, clientSide, map[string]any{
		"version": 1, "id": 6, "op": "wait", "attachment": readyResult.Attachment,
		"after_action": enterResult.ActionID, "expect": map[string]string{"text_contains": "HEADLESS_OK"},
	})
	wait := readDriverTestEnvelope(t, decoder)
	require.Nil(t, wait.Error)

	writeDriverTestRequest(t, clientSide, map[string]any{
		"version": 1, "id": 7, "op": "capture", "attachment": readyResult.Attachment,
	})
	capture := readDriverTestEnvelope(t, decoder)
	require.Nil(t, capture.Error)
	var captureResult struct {
		Text string `json:"text"`
	}
	require.NoError(t, json.Unmarshal(capture.Result, &captureResult))
	require.Contains(t, captureResult.Text, "HEADLESS_OK")

	require.NoError(t, clientSide.Close())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("headless runner did not stop after driver EOF")
	}
}

func readDriverTestEnvelope(t *testing.T, decoder *json.Decoder) driverTestEnvelope {
	t.Helper()
	var envelope driverTestEnvelope
	require.NoError(t, decoder.Decode(&envelope))
	require.Equal(t, 1, envelope.Version)
	return envelope
}

func writeDriverTestRequest(t *testing.T, conn net.Conn, request map[string]any) {
	t.Helper()
	data, err := json.Marshal(request)
	require.NoError(t, err)
	_, err = fmt.Fprintf(conn, "%s\n", data)
	require.NoError(t, err)
}
