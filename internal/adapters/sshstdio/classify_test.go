package sshstdio

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestClassifyStderr(t *testing.T) {
	tests := []struct {
		name   string
		stderr string
		want   domain.RemoteFailureKind
	}{
		{name: "openssh key refused", stderr: "user@example.test: Permission denied (publickey).", want: domain.RemoteFailureAuthentication},
		{name: "go ssh no methods", stderr: "ssh: handshake failed: ssh: unable to authenticate, attempted methods [none password], no supported methods remain", want: domain.RemoteFailureAuthentication},
		{name: "browser login required", stderr: "SSH authentication required.\nPlease do the SSO login in your browser.", want: domain.RemoteFailureAuthentication},
		{name: "unknown host key", stderr: "Host key verification failed.", want: domain.RemoteFailureTrust},
		{name: "changed host key", stderr: "WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!", want: domain.RemoteFailureTrust},
		{name: "timeout", stderr: "ssh: connect to host example.test port 22: Connection timed out", want: domain.RemoteFailureTimeout},
		{name: "remote shell permission stays generic", stderr: "bash: /usr/local/bin/vev: Permission denied", want: domain.RemoteFailureNone},
		{name: "refused stays generic", stderr: "connect: connection refused", want: domain.RemoteFailureNone},
		{name: "empty", stderr: "", want: domain.RemoteFailureNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ClassifyStderr(tt.stderr))
		})
	}
}
