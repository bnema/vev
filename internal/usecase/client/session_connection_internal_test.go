package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloneAttachRequestPreservesEnvironmentShape(t *testing.T) {
	tests := []struct {
		name        string
		environment []string
	}{
		{name: "nil", environment: nil},
		{name: "empty", environment: []string{}},
		{name: "non-empty", environment: []string{"TERM=xterm"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := cloneAttachRequest(AttachRequest{Environment: test.environment})
			if test.environment == nil {
				require.Nil(t, request.Environment)
				return
			}
			require.NotNil(t, request.Environment)
			require.Equal(t, test.environment, request.Environment)
			if len(test.environment) > 0 {
				test.environment[0] = "changed"
				require.Equal(t, "TERM=xterm", request.Environment[0])
			}
		})
	}
}
