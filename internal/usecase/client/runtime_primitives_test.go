package client

import (
	"testing"
	"testing/synctest"

	portsmocks "github.com/bnema/vev/internal/ports/mocks"

	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestRequestedOutputWindow(t *testing.T) {
	tests := []struct {
		name      string
		preferred uint8
		want      uint8
	}{
		{name: "no carriage preference pipelines the full window", preferred: 0, want: protocol.MaxOutputWindow},
		{name: "a carriage that prefers one frame keeps it", preferred: 1, want: 1},
		{name: "a carriage preference is kept as declared", preferred: 4, want: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stream := newSessionStream(t, func(m *portsmocks.MockBrokerLogicalConnection, _ *streamLog) {
					m.EXPECT().Capabilities().Return(protocol.ConnectionCapabilities{PreferredOutputWindow: tt.preferred}).Maybe()
				})
				require.Equal(t, tt.want, requestedOutputWindow(stream.conn))
			})
		})
	}
}

// TestHelloRequestsPipelinedOutput pins the fallback window an attachment
// Hello claims when its carriage states no preference. A window of one would
// make every remote frame wait a round trip.
func TestHelloRequestsPipelinedOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		worker, err := newSessionAttachmentWorker(sessionAttachmentConfig{Request: sessionTestRequest(false), SessionEnvironment: SessionEnvironment{Provenance: SessionEnvironmentRemote}})
		require.NoError(t, err)
		hello := worker.hello(newSessionStream(t).conn)
		require.Equal(t, uint8(protocol.MaxOutputWindow), hello.MaxOutputInFlight)
	})
}
