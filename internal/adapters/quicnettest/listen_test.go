package quicnettest

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExplicitListenSurvivesRebind(t *testing.T) {
	for _, ip := range []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback} {
		t.Run(ip.String(), func(t *testing.T) {
			server, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip})
			if err != nil && ip.To4() == nil {
				t.Skipf("IPv6 loopback unavailable: %v", err)
			}
			require.NoError(t, err)
			defer server.Close()
			proxy, err := New(Config{ServerAddr: server.LocalAddr().(*net.UDPAddr), ListenAddr: &net.UDPAddr{IP: ip}})
			require.NoError(t, err)
			defer proxy.Close()
			require.True(t, proxy.Addr().IP.Equal(ip), "requested listener address")
			client, err := net.ListenUDP("udp", &net.UDPAddr{IP: ip})
			require.NoError(t, err)
			defer client.Close()
			for attempt := 0; attempt < 2; attempt++ {
				if attempt == 1 {
					require.NoError(t, proxy.Rebind())
				}
				_, err = client.WriteToUDP([]byte("fixture"), proxy.Addr())
				require.NoError(t, err)
				require.NoError(t, server.SetReadDeadline(time.Now().Add(time.Second)))
				data := make([]byte, 32)
				n, from, err := server.ReadFromUDP(data)
				require.NoError(t, err)
				require.Equal(t, "fixture", string(data[:n]))
				_, err = server.WriteToUDP(data[:n], from)
				require.NoError(t, err)
				require.NoError(t, client.SetReadDeadline(time.Now().Add(time.Second)))
				n, _, err = client.ReadFromUDP(data)
				require.NoError(t, err)
				require.Equal(t, "fixture", string(data[:n]))
			}
		})
	}
}
