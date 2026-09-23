package app

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestClassifyBootstrapFailureKeepsKindThroughTheDialChain(t *testing.T) {
	cause := errors.New("exit status 255")
	tests := []struct {
		name   string
		stderr string
		nilErr bool
		want   domain.RemoteFailureKind
	}{
		{name: "auth failure is typed", stderr: "user@example.test: Permission denied (publickey).", want: domain.RemoteFailureAuthentication},
		{name: "unclassified stderr stays untyped", stderr: "connect: connection refused", want: domain.RemoteFailureNone},
		{name: "nil sink stays untyped", nilErr: true, want: domain.RemoteFailureNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sink *sshstdio.DiagnosticSink
			if !tt.nilErr {
				sink = sshstdio.NewDiagnosticSink(0)
				_, _ = sink.Write([]byte(tt.stderr))
			}
			err := classifyBootstrapFailure(sink, cause)
			// The pool wraps dial failures as a BrokerError cause.
			wrapped := ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: brokerQUICUnavailable("read bootstrap readiness", err)}

			require.ErrorIs(t, wrapped, cause, "the original cause stays in the chain")
			var typed domain.RemoteFailure
			if tt.want == domain.RemoteFailureNone {
				require.False(t, errors.As(wrapped, &typed))
				return
			}
			require.True(t, errors.As(wrapped, &typed))
			require.Equal(t, tt.want, typed.Kind)
			require.Contains(t, err.Error(), cause.Error(), "the error text keeps the cause")
		})
	}
}
