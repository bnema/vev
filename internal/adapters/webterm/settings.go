package webterm

import (
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

// Settings separates the local HTTP listener from the browser-facing origin.
// HTTPS, when selected, is terminated by a proxy preserving Host and Origin.
type Settings struct {
	Listen string `json:"listen"`
	Origin string `json:"origin"`
}

func ParseSettings(listen, origin string) (Settings, error) {
	if listen == "" {
		listen = Address
	}
	addr, err := netip.ParseAddrPort(listen)
	if err != nil || addr.Port() == 0 || addr.Addr().Zone() != "" || addr.Addr().IsMulticast() {
		return Settings{}, errors.New("webterm: web.listen requires an IP address and port (1–65535)")
	}
	listen = addr.String()
	if origin == "" {
		if !addr.Addr().IsLoopback() {
			return Settings{}, errors.New("webterm: web.origin is required for a non-loopback listener")
		}
		origin = "http://" + listen
	}
	u, err := url.Parse(origin)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Opaque != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(origin, "#") {
		return Settings{}, errors.New("webterm: web.origin requires an HTTP(S) origin without credentials, path, query or fragment")
	}
	host := strings.ToLower(u.Hostname())
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.IsUnspecified() || ip.IsMulticast() || ip.Zone() != "" {
			return Settings{}, errors.New("webterm: web.origin requires a concrete host")
		}
		host = ip.String()
	} else {
		if len(host) > 253 {
			return Settings{}, errors.New("webterm: invalid web.origin hostname")
		}
		for _, label := range strings.Split(host, ".") {
			if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
				return Settings{}, errors.New("webterm: invalid web.origin hostname")
			}
			for _, c := range label {
				if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
					return Settings{}, errors.New("webterm: invalid web.origin hostname")
				}
			}
		}
	}
	port := u.Port()
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return Settings{}, errors.New("webterm: invalid web.origin port")
		}
		port = strconv.Itoa(n)
		if u.Scheme == "http" && port == "80" || u.Scheme == "https" && port == "443" {
			port = ""
		}
	} else if strings.HasSuffix(u.Host, ":") {
		return Settings{}, errors.New("webterm: invalid web.origin port")
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return Settings{Listen: listen, Origin: u.Scheme + "://" + host}, nil
}

// ProbeAddress never resolves or contacts the public origin. Wildcard listeners
// are reached through the matching loopback family on this machine.
func (s Settings) ProbeAddress() string {
	addr := netip.MustParseAddrPort(s.Listen)
	if addr.Addr().IsUnspecified() {
		ip := netip.IPv6Loopback()
		if addr.Addr().Is4() {
			ip = netip.MustParseAddr("127.0.0.1")
		}
		addr = netip.AddrPortFrom(ip, addr.Port())
	}
	return addr.String()
}

func (s Settings) Host() string {
	_, host, _ := strings.Cut(s.Origin, "://")
	return host
}
