package sshstdio

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildCommandForRemoteLaunchQuotesBinaryAndEnvironment(t *testing.T) {
	spec := BuildCommandForRemoteLaunch("test@example.com", "/opt/vev with-space", []string{"HOME=/tmp/home", "VALUE=a;b'c"}, "_stdio")
	require.Equal(t, "ssh", spec.Path)
	require.Equal(t, []string{
		"--",
		"test@example.com",
		"'env' '-i' 'HOME=/tmp/home' 'VALUE=a;b'\\''c' '/opt/vev with-space' '_stdio'",
	}, spec.Args)
}
