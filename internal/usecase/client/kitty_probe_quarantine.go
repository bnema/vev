package client

import "github.com/bnema/vev-vt/protocol/terminalquery"

// terminalProbeQuarantine removes delayed replies to the Kitty capability
// probe before they can be forwarded as PTY input.
type terminalProbeQuarantine struct {
	probe terminalquery.Probe
}

func (q *terminalProbeQuarantine) filter(data []byte) []byte {
	if q == nil || len(data) == 0 {
		return append([]byte(nil), data...)
	}
	return q.probe.Feed(data)
}
