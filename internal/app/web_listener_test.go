package app

import (
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWebListenerInheritancePreservesFamily(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "0.0.0.0", "::1", "::"} {
		t.Run(host, func(t *testing.T) {
			listener, err := listenWeb(net.JoinHostPort(host, "0"))
			if strings.Contains(host, ":") && (errors.Is(err, syscall.EADDRNOTAVAIL) || errors.Is(err, syscall.EAFNOSUPPORT)) {
				t.Skip("IPv6 address unavailable on this host")
			}
			require.NoError(t, err)
			defer listener.Close()
			gotHost, _, err := net.SplitHostPort(listener.Addr().String())
			require.NoError(t, err)
			require.Equal(t, host, gotHost)
			file, err := listener.(*net.TCPListener).File()
			require.NoError(t, err)
			defer file.Close()
			inherited, err := net.FileListener(file)
			require.NoError(t, err)
			defer inherited.Close()
			require.Equal(t, listener.Addr().String(), inherited.Addr().String())
		})
	}
}
